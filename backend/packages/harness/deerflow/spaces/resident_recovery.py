"""Private host-authorized recovery; normal fenced retries retain observation-only authority."""

import asyncio
import logging
import math
from contextlib import asynccontextmanager

from sqlalchemy import select

from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.sandbox.lease import run_sync_lifecycle_operation
from deerflow.spaces.attachments import AttachmentPending
from deerflow.spaces.native_resident_evidence import NativeResidentRecord
from deerflow.spaces.resident_containment import _BINDING, _Binding, _RootAdmission
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class NativeResidentRecoveryScopes:
    """Gateway supplies eligibility under its Home fence; no human principal or synthesized row."""

    def __init__(self, containment):
        self.containment = containment

    @asynccontextmanager
    async def __call__(self, record, *, eligible, deadline):
        from deerflow.community.aio_sandbox.resident_containment import ResidentContainmentWorker

        factory = self.containment
        bridge = factory.provider._native_creation
        if not isinstance(record, NativeResidentRecord) or not callable(eligible) or not math.isfinite(deadline):
            raise AttachmentPending("Resident recovery requires exact evidence, eligibility and deadline")
        await await_drained(run_sync_lifecycle_operation(bridge.assert_current, teardown=True))
        if record.daemon_id != bridge.evidence.daemon_id or bridge.residents.for_attachment(record.attachment_id) != record:
            raise AttachmentPending("Resident recovery lost its current original graph")
        handles = {root.backing_handle for root in record.roots}
        async with asyncio.timeout_at(deadline), factory.files.registry._sf() as session:
            rows = (await session.scalars(select(SpaceBackingRow).where(SpaceBackingRow.backing_handle.in_(handles)))).all()
            bindings = {row.backing_handle: _Binding(row.space_id, tuple(getattr(row, key) for key in _BINDING)) for row in rows}
        if set(bindings) != handles:
            raise AttachmentPending("Resident recovery lacks an original backing parent")
        gate = factory.provider._owner_admission_gate()
        token = gate.reserve_resident_batch((("resident:" + record.attachment_id, tuple(root.slot_id for root in record.roots if root.writable), tuple(root.slot_id for root in record.roots if not root.writable)),))
        scope = _RecoveryScope(factory, record, bindings, eligible, deadline)
        scope.worker = ResidentContainmentWorker(
            provider=factory.provider,
            adapter=factory.adapter,
            gate=gate,
            reservation=token,
            records=(record,),
            roots_guard=scope.guard_roots,
            deadline=deadline,
            recovery=True,
            eligibility_guard=scope.guard_eligibility,
        )
        try:
            yield scope
        finally:
            scope.worker.close()
            gate.end_resident_batch(token)


class _RecoveryScope(_RootAdmission):
    def __init__(self, factory, record, bindings, eligible, deadline):
        super().__init__(factory, (record,), bindings, {binding.parent_id for binding in bindings.values()})
        self.record, self.eligible = record, eligible
        self.committed = False
        self.deadline = deadline
        self.loop = asyncio.get_running_loop()
        self.session = None

    @property
    def terminal_completed(self):
        """Only the owned worker's post-fsync/index/qualification receipt counts."""
        return self.committed and self.worker is not None and self.record.attachment_id not in self.worker.records

    def guard_eligibility(self):
        """Worker borrows sequentially while the owning loop awaits its drainage."""
        if self.session is None:
            raise AttachmentPending("Original recovery lacks its borrowed eligibility transaction")

        async def check():
            async with asyncio.timeout_at(self.deadline):
                await self.eligible(self.session, self.record)

        # No Future timeout: the coroutine owns the deadline, and its SQL
        # cancellation must drain before the worker or parent can be released.
        asyncio.run_coroutine_threadsafe(check(), self.loop).result()

    async def admit(self, session):
        await self.eligible(session, self.record)
        await self.admit_roots(session)
        await self.eligible(session, self.record)

    async def contain(self):
        async def perform():
            async with asyncio.timeout_at(self.deadline), self.factory.files.registry.host_locked(self.parent_ids) as (session, _parents):
                self.session = session
                try:
                    await self.admit(session)
                    await await_drained(run_sync_lifecycle_operation(self.worker.recover, self.record.attachment_id))
                    await self.eligible(session, self.record)
                finally:
                    self.session = None
            self.committed = True

        await await_drained(perform())

    async def finalize(self):
        if not self.committed:
            raise AttachmentPending("Resident recovery lacks its confirmed eligibility commit")

        async def perform():
            # This second phase validates only. Its release cannot undo the
            # confirmed first outcome on which durable completion depends.
            async with asyncio.timeout_at(self.deadline), self.factory.files.registry.host_locked(self.parent_ids) as (session, _parents):
                self.session = session
                try:
                    await self.eligible(session, self.record)
                    await self.revalidate_roots(session)
                    await await_drained(run_sync_lifecycle_operation(self.worker.confirm_absent, self.record.attachment_id))
                    await self.eligible(session, self.record)
                    await await_drained(run_sync_lifecycle_operation(self.worker.finalize, self.record.attachment_id))
                finally:
                    self.session = None

        try:
            await await_drained(perform())
        except Exception as exc:
            if not self.terminal_completed:
                raise
            logger.warning("Original resident completion confirmed before validation-only transaction release failed (%s)", type(exc).__name__)
