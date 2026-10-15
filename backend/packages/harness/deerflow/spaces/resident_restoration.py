"""Gateway-owned completion of exact restored originals by absence alone."""

import asyncio
import logging
import math
import time
from contextlib import asynccontextmanager

from sqlalchemy import select

from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.sandbox.lease import run_sync_lifecycle_operation
from deerflow.spaces.attachments import AttachmentPending
from deerflow.spaces.native_resident_evidence import NativeResidentRecord, NativeResidentRoot
from deerflow.spaces.readonly_roots import _readonly_revision
from deerflow.spaces.resident_containment import _BINDING, _Binding, _RootAdmission
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class _CompletionPermit:
    """Private exact-owner/batch receipt; validation under an index lock is memory-only."""

    def __init__(self, worker):
        self.worker = worker
        self._finished = None

    def _require(self, record):
        worker, scope = self.worker, self.worker.scope
        worker.lifetime._assert_live()
        owner = worker.owner
        if (
            not scope.committed
            or not scope.finalizing
            or scope.session is None
            or worker.record != record
            or owner is not worker.provider._pool_runtime_owner
            or not owner.accepts(worker.provider)
            or owner.native_create_evidence is not worker.bridge.evidence
        ):
            raise AttachmentPending("Restored completion lost its confirmed Gateway scope")

    def require_journal(self, evidence, record):
        self._require(record)
        if evidence is not self.worker.bridge.evidence:
            raise AttachmentPending("Restored completion belongs to another journal")

    def require_index(self, index, record):
        worker, lifetime = self.worker, self.worker.lifetime
        # The durable outcome already happened. This consumes that exact
        # memory receipt, without authorizing another resource effect.
        if (
            self._finished != (worker.bridge.evidence, record)
            or index is not worker.bridge.residents
            or not lifetime.active
            or worker.provider._native_creation is not worker.bridge
            or worker.provider._backend is not lifetime.backend
            or lifetime.gate.lock is not index.lock
            or not lifetime.gate.current_resident_batch_locked(lifetime.reservation)
            or worker.owner is not worker.provider._pool_runtime_owner
            or worker.owner.native_create_evidence is not worker.bridge.evidence
        ):
            raise AttachmentPending("Restored completion lacks its durable original outcome")

    def seal_journal(self, evidence, record):
        if evidence is not self.worker.bridge.evidence or record != self.worker.record:
            raise AttachmentPending("Restored terminal receipt changed its original journal")
        self._finished = (evidence, record)


class RestoredResidentCompletionScopes:
    """An explicit per-call factory; it never installs recovery on an adapter."""

    def __init__(self, containment, record, *, eligible, deadline):
        if not isinstance(record, NativeResidentRecord) or not callable(eligible) or not math.isfinite(deadline):
            raise AttachmentPending("Restored completion requires exact evidence, eligibility and deadline")
        self.containment, self.record = containment, record
        self.eligible, self.deadline = eligible, deadline
        self.completed_scope = None

    @property
    def terminal_completed(self):
        return self.completed_scope is not None and self.completed_scope.terminal_completed

    @asynccontextmanager
    async def __call__(self, attachments, captured):
        factory, record = self.containment, self.record
        if attachments.files is not factory.files or attachments.provider is not factory.adapter or set(captured) != {record.attachment_id}:
            raise AttachmentPending("Restored completion changed its original storage scope")
        async with self._scope(attachments, captured) as scope:
            yield scope

    @asynccontextmanager
    async def missing(self, attachments):
        """Missing SQL remains distinct; no attachment is synthesized."""
        async with self._scope(attachments, None) as scope:
            yield scope

    @asynccontextmanager
    async def _scope(self, attachments, captured):
        # This task owns the timeout, including both caller SQL transactions.
        # Worker draining remains inside the scope before its batch expires.
        async with asyncio.timeout_at(self.deadline):
            async with self._owned_scope(attachments, captured) as scope:
                yield scope

    @asynccontextmanager
    async def _owned_scope(self, attachments, captured):
        factory, record = self.containment, self.record
        bridge = factory.provider._native_creation
        await await_drained(run_sync_lifecycle_operation(bridge.assert_current, teardown=True))
        if bridge.residents.for_attachment(record.attachment_id) != record:
            raise AttachmentPending("Restored completion lost its retained original")
        handles = {root.backing_handle for root in record.roots}
        async with asyncio.timeout_at(self.deadline), factory.files.registry._sf() as session:
            rows = (await session.scalars(select(SpaceBackingRow).where(SpaceBackingRow.backing_handle.in_(handles)))).all()
            bindings = {row.backing_handle: _Binding(row.space_id, tuple(getattr(row, key) for key in _BINDING)) for row in rows}
        if set(bindings) != handles:
            raise AttachmentPending("Restored completion lacks its original backing parents")
        gate = factory.provider._owner_admission_gate()
        token = gate.reserve_resident_batch((("resident:" + record.attachment_id, tuple(root.slot_id for root in record.roots if root.writable), tuple(root.slot_id for root in record.roots if not root.writable)),))
        scope = _RestoredScope(self, attachments, captured, bindings)
        self.completed_scope = scope
        try:
            scope.worker = _RestoredWorker(scope, gate, token)
            yield scope
        finally:
            if scope.worker is not None:
                scope.worker.close()
            gate.end_resident_batch(token)


class _RestoredWorker:
    def __init__(self, scope, gate, token):
        from deerflow.community.aio_sandbox.resident_containment import ResidentContainmentWorker

        self.scope, self.record = scope, scope.record
        self.provider = scope.factory.provider
        self.bridge, self.backend = self.provider._native_creation, self.provider._backend
        self.owner = self.provider._pool_runtime_owner
        self.lifetime = ResidentContainmentWorker(provider=self.provider, adapter=scope.factory.adapter, gate=gate, reservation=token, records=(self.record,), roots_guard=scope.guard_roots, deadline=scope.deadline, recovery=True)
        self.permit = _CompletionPermit(self)
        self.completed = False

    def assert_current(self):
        self.lifetime.assert_current()
        if self.bridge.residents.for_attachment(self.record.attachment_id) != self.record or self.owner is not self.provider._pool_runtime_owner or self.owner.native_create_evidence is not self.bridge.evidence:
            raise AttachmentPending("Restored completion lost its exact retained Gateway original")

    def observe(self):
        from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet

        self.assert_current()
        self.scope.guard_roots(self.record)
        self.scope.guard_eligibility()
        reader = NativeResourceSet(self.backend, guard=self.assert_current)
        reader.deadline = min(self.scope.deadline, time.monotonic() + 30)
        if not reader.prove_resident_absent(self.record):
            raise AttachmentPending("Restored completion retains a surviving original ID or name")
        self.assert_current()
        self.scope.guard_eligibility()

    def finalize(self):
        self.observe()
        try:
            if self.record.phase == "prepared":
                retiring = self.bridge.evidence._retire_restored_resident(self.record, self.permit)
                self.bridge.qualify(teardown=True)
                self.record = retiring
                self.lifetime.records[retiring.attachment_id] = retiring
                self.assert_current()
            self.scope.guard_eligibility()
            try:
                self.bridge.evidence._finish_restored_resident(self.record, self.permit)
            finally:
                if self.permit._finished is not None:
                    self.bridge.residents._remove_restored(self.record, self.permit)
                    self.completed = True
            self.bridge.qualify(teardown=True)
        except BaseException:
            self.bridge._check_failure_evidence(teardown=True)
            raise

    def close(self):
        self.lifetime.close()


class _RestoredScope(_RootAdmission):
    def __init__(self, operation, attachments, captured, bindings):
        factory, record = operation.containment, operation.record
        super().__init__(factory, (record,), bindings, {binding.parent_id for binding in bindings.values()})
        self.record, self.attachments, self.captured = record, attachments, captured
        self.eligible, self.deadline = operation.eligible, operation.deadline
        self.loop = asyncio.get_running_loop()
        self.session = None
        self.committed = self.finalizing = False
        self._devices, self._projections = {}, {}

    def _check_volume(self, root, volume):
        if (volume.spec.slot_id, volume.spec.filesystem_uuid, volume.root_inode, volume.control_inode, volume.backing_handle) != (root.slot_id, root.filesystem_uuid, root.root_inode, root.control_inode, root.backing_handle):
            raise AttachmentPending("Restored completion changed its stable physical roots")
        admitted_device = self._devices.setdefault(root.backing_handle, volume.device)
        if volume.device != admitted_device:
            raise AttachmentPending("Restored completion changed its current admitted device")

    def _check_projection(self, root, projection):
        original_revision = _readonly_revision(root.backing_handle, root.slot_id, root.filesystem_uuid, root.source, root.root_inode, root.control_inode, (root.device, root.source_inode))
        if root.revision != original_revision or (projection.source, projection.identity[1]) != (root.source, root.source_inode):
            raise AttachmentPending("Restored completion changed its original read-only projection")
        current = NativeResidentRoot.from_readonly(projection)
        key = root.backing_handle, root.destination
        if self._projections.setdefault(key, current) != current:
            raise AttachmentPending("Restored completion changed its current read-only projection")

    @property
    def terminal_completed(self):
        return self.committed and self.worker is not None and self.worker.completed

    def guard_eligibility(self):
        if self.session is None:
            raise AttachmentPending("Restored completion lacks its borrowed parent transaction")

        async def check():
            async with asyncio.timeout_at(self.deadline):
                await self.eligible(self.session, self.record)

        asyncio.run_coroutine_threadsafe(check(), self.loop).result()

    async def admit(self, session, records=None):
        await self.eligible(session, self.record)
        if self.captured is not None:
            if records is None or set(records) != {self.record.attachment_id} or set(self.captured[self.record.attachment_id]) != set(self.record.correlation[4]):
                raise AttachmentPending("Restored completion changed its original mount scope")
        if self.volumes:
            await self.revalidate_roots(session)
        else:
            await self.admit_roots(session)
        self.session = session
        await self.eligible(session, self.record)

    async def contain(self, row):
        if row.id != self.record.attachment_id:
            raise AttachmentPending("Restored completion changed its original attachment")
        await await_drained(run_sync_lifecycle_operation(self.worker.observe))

    async def recover_missing(self):
        async with asyncio.timeout_at(self.deadline), self.factory.files.registry.host_locked(self.parent_ids) as (session, parents):
            try:
                await self._fenced(session, parents)
                await self.admit(session)
                await await_drained(run_sync_lifecycle_operation(self.worker.observe))
            finally:
                self.session = None
        self.committed = True

    async def _fenced(self, session, parents):
        if set(parents) != set(self.parent_ids):
            raise AttachmentPending("Restored completion lost an original parent")
        await self.eligible(session, self.record)
        if self.captured is not None:
            rows = await self.attachments._locked_records(session, self.captured)
            if any(row.phase != "fenced" for row in rows.values()):
                raise AttachmentPending("Restored completion lacks confirmed fenced SQL")

    async def finalize(self):
        async with asyncio.timeout_at(self.deadline), self.factory.files.registry.host_locked(self.parent_ids) as (session, parents):
            await self._fenced(session, parents)
        self.committed = True
        try:
            async with asyncio.timeout_at(self.deadline), self.factory.files.registry.host_locked(self.parent_ids) as (session, parents):
                try:
                    await self._fenced(session, parents)
                    self.session = session
                    await self.revalidate_roots(session)
                    self.finalizing = True
                    await await_drained(run_sync_lifecycle_operation(self.worker.finalize))
                    await self.eligible(session, self.record)
                finally:
                    self.session = None
                    self.finalizing = False
        except Exception as exc:
            if not self.terminal_completed:
                raise
            logger.warning("Restored original completion confirmed before validation-only release failed (%s)", type(exc).__name__)
