"""Operator containment borrows SQL exclusion and reads original v5 evidence.

No Gateway ownership, capacity release, journal transition or Work callback is
installed. A subsequent Gateway pass owns those outcomes after physical fencing.
"""

import time
from contextlib import asynccontextmanager
from dataclasses import replace

from sqlalchemy import or_, select

from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet
from deerflow.community.aio_sandbox.resident_containment import remove_original_resources
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.sandbox.lease import run_sync_lifecycle_operation
from deerflow.spaces.attachments import AttachmentPending
from deerflow.spaces.resident_containment import _BINDING, _Binding, _Fenced, _fenced_facts, _Scope
from deerflow.utils.file_io import await_drained


class OperatorResidentContainmentScopes:
    def __init__(self, *, files, adapter, evidence):
        self.files, self.adapter, self.evidence = files, adapter, evidence

    @asynccontextmanager
    async def __call__(self, attachments, captured):
        if attachments.files is not self.files or attachments.provider is not self.adapter or not getattr(self.adapter, "_operator_only", False) or attachments.on_contained is not None:
            raise AttachmentPending("Operator containment requires its isolated storage scope")
        snapshot = await await_drained(run_sync_lifecycle_operation(self.evidence.snapshot))
        records = tuple(record for record in snapshot.residents if record.attachment_id in captured)
        missing = set(captured) - {record.attachment_id for record in records}
        bindings, finished = {}, {}
        handles = {root.backing_handle for record in records for root in record.roots}
        finished_parents = {fact[0] for identifier in missing for fact in captured[identifier]}
        async with self.files.registry._sf() as session:
            for identifier in sorted(missing):
                row = await session.get(SpaceAttachmentRow, identifier)
                if row is None or row.phase != "fenced" or row.host_id != self.adapter.host_id or row.container_id is None:
                    raise AttachmentPending("Operator containment lacks its durable original graph")
                attachments._identifier(row.incarnation)
                attachments._closure_reason(row, None)
                finished[identifier] = _Fenced(row.incarnation, row.host_id, row.container_id, _fenced_facts(row))
            rows = (await session.scalars(select(SpaceBackingRow).where(or_(SpaceBackingRow.backing_handle.in_(handles), SpaceBackingRow.space_id.in_(finished_parents))))).all()
            for row in rows:
                bindings[row.backing_handle] = _Binding(row.space_id, tuple(getattr(row, key) for key in _BINDING))
        if not handles.issubset(bindings) or not finished_parents.issubset({binding.parent_id for binding in bindings.values()}):
            raise AttachmentPending("Operator original backing scope is unavailable")
        scope = _OperatorScope(self, attachments, captured, records, bindings, finished)
        scope.worker = _OperatorWorker(self, records, scope.guard_roots)
        try:
            yield scope
        finally:
            scope.worker.close()


class _OperatorWorker:
    def __init__(self, factory, records, roots_guard):
        self.factory, self.roots_guard = factory, roots_guard
        self.records = {record.attachment_id: record for record in records}
        self.backend = factory.adapter._backend
        self.active = True

    def assert_current(self):
        if not self.active or self.factory.adapter._backend is not self.backend:
            raise AttachmentPending("Operator containment lost its original storage scope")
        self.factory.adapter._same_host()
        self.factory.evidence.assert_current()

    def _current_record(self, identifier):
        self.assert_current()
        original = self.records[identifier]
        snapshot = self.factory.evidence.snapshot()
        current = next((record for record in snapshot.residents if record.attachment_id == identifier), None)
        if current is None or replace(current, phase=original.phase) != original or (original.phase == "retiring" and current.phase != "retiring"):
            raise AttachmentPending("Operator containment lost its exact retained original graph")
        writable = {root.slot_id for root in original.roots if root.writable}
        if any(attempt.name == original.name or attempt.owner_id == "resident:" + identifier or writable.intersection(attempt.slots) for attempt in snapshot.pending_creates(self.factory.adapter.host_id)):
            raise AttachmentPending("Operator containment retains an unresolved original preparation")
        self.roots_guard(original)
        return original

    def _reader(self):
        reader = NativeResourceSet(self.backend, guard=self.assert_current)
        reader.deadline = time.monotonic() + 60
        return reader

    def contain(self, identifier):
        original = self._current_record(identifier)
        reader = self._reader()
        reader.qualify_resident_record(original)
        if original.daemon_id != self.factory.adapter.host_id:
            # SQL admission separately requires the recorded host replacement.
            # An old host never authorizes removing an object on this daemon.
            if not reader.prove_resident_absent(original):
                raise AttachmentPending("Replaced host retains a surviving original resource here")
            return

        def command(arguments):
            self._current_record(identifier)
            remaining = reader.deadline - time.monotonic()
            if remaining <= 0:
                raise AttachmentPending("Operator containment exhausted its inspection budget")
            reply = self.backend._run(arguments, capture_output=True, text=True, timeout=min(30, remaining))
            self._current_record(identifier)
            if reply.returncode:
                raise AttachmentPending("Operator original resource effect remains unconfirmed")

        remove_original_resources(reader, original, guard=lambda: self._current_record(identifier), command=command)

    def confirm_absent(self, identifier):
        original = self._current_record(identifier)
        if not self._reader().prove_resident_absent(original):
            raise AttachmentPending("Operator fenced retry retains original resources")

    def close(self):
        self.active = False


class _OperatorScope(_Scope):
    def _check_finished_journal(self, identifier):
        original = self.finished[identifier]
        name = "hartmesh-space-" + identifier
        snapshot = self.factory.evidence.snapshot()
        if any(record.name == name or getattr(record, "container_id", None) == original.container_id for record in (*snapshot.creates, *snapshot.teardowns, *snapshot.residents)):
            raise AttachmentPending("Completed operator scope retains journal uncertainty")

    def _observe_finished(self, identifier):
        self.worker.assert_current()
        original = self.finished[identifier]
        name = "hartmesh-space-" + identifier
        self._check_finished_journal(identifier)
        reader = self.worker._reader()
        if reader.inspect("container", original.container_id) is not None or reader.inspect("container", name) is not None:
            raise AttachmentPending("Completed operator scope has a surviving primary identity")
        paired = reader.inventory_paired()
        residents = self.factory.adapter.bootstrap_residents(known_slots=tuple(self.factory.files.catalog.volumes), paired=paired)
        if any(item.sandbox_id == identifier for item in paired) or any(item.attachment_id == identifier for item in residents):
            raise AttachmentPending("Completed operator scope retains a resource correlation")
        self.worker.assert_current()
        self._check_finished_journal(identifier)

    async def finalize(self):
        # Keep the journal and capacity claims. Only Gateway finalizes them.
        await self._confirmed_fenced()
        for identifier in sorted(self.originals):
            await await_drained(run_sync_lifecycle_operation(self.worker.confirm_absent, identifier))
        for identifier in sorted(self.finished):
            await await_drained(run_sync_lifecycle_operation(self._observe_finished, identifier))
        await self._confirmed_fenced()
