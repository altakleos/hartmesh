"""Private Gateway-owned backing admission for original resident containment."""

from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import PurePosixPath

from sqlalchemy import or_, select

from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.sandbox.lease import run_sync_lifecycle_operation
from deerflow.spaces.attachments import AttachmentPending
from deerflow.spaces.native_resident_evidence import NativeResidentRoot
from deerflow.spaces.readonly_roots import ReadOnlyRoot
from deerflow.utils.file_io import await_drained

_BINDING = ("backing_handle", "space_id", "slot_id", "filesystem_uuid", "root_inode", "control_inode")


@dataclass(frozen=True)
class _Binding:
    parent_id: str
    facts: tuple


@dataclass(frozen=True)
class _Fenced:
    incarnation: str
    daemon_id: str
    container_id: str
    facts: tuple


def _fenced_facts(row):
    return (row.phase, row.incarnation, row.host_id, row.container_id, row.actor_kind, row.actor_id, row.fenced_by_kind, row.fenced_by_id, row.fence_reason, row.fenced_at)


class NativeResidentContainmentScopes:
    """Installed once on the exact qualified adapter; constructing it performs no IO."""

    def __init__(self, *, provider, files, adapter):
        self.provider, self.files, self.adapter = provider, files, adapter

    def blocks_admin(self, backing_handle):
        """Memory-only guard; SQL closure cannot bypass unfinished durable work."""
        bridge = self.provider._native_creation
        return bridge is None or not bridge.residents.ready or any(record.phase == "retiring" and any(root.backing_handle == backing_handle for root in record.roots) for record in bridge.residents.records())

    @asynccontextmanager
    async def __call__(self, attachments, captured):
        from deerflow.community.aio_sandbox.resident_containment import ResidentContainmentWorker

        if attachments.files is not self.files or attachments.provider is not self.adapter:
            raise AttachmentPending("Resident containment belongs to another storage lifetime")
        bridge = self.provider._native_creation
        await await_drained(run_sync_lifecycle_operation(bridge.assert_current, teardown=True))
        records = tuple(record for identifier in sorted(captured) if (record := bridge.residents.for_attachment(identifier)) is not None)
        missing = set(captured) - {record.attachment_id for record in records}
        bindings, finished = {}, {}
        async with self.files.registry._sf() as session:
            for identifier in sorted(missing):
                row = await session.get(SpaceAttachmentRow, identifier)
                if row is None or row.phase != "fenced" or row.host_id != bridge.evidence.daemon_id or row.container_id is None:
                    raise AttachmentPending("Resident containment lacks its durable original graph")
                attachments._identifier(row.incarnation)
                attachments._closure_reason(row, None)
                finished[identifier] = _Fenced(row.incarnation, row.host_id, row.container_id, _fenced_facts(row))
            handles = {root.backing_handle for record in records for root in record.roots}
            finished_parents = {fact[0] for identifier in missing for fact in captured[identifier]}
            rows = (await session.scalars(select(SpaceBackingRow).where(or_(SpaceBackingRow.backing_handle.in_(handles), SpaceBackingRow.space_id.in_(finished_parents))))).all()
            for row in rows:
                bindings[row.backing_handle] = _Binding(row.space_id, tuple(getattr(row, key) for key in _BINDING))
        if not handles.issubset(bindings) or not finished_parents.issubset({binding.parent_id for binding in bindings.values()}):
            raise AttachmentPending("Resident original backing scope is unavailable")
        gate = self.provider._owner_admission_gate()
        finished_slots = {binding.parent_id: binding.facts[2] for binding in bindings.values()}
        reservation = gate.reserve_resident_batch(
            tuple(("resident:" + record.attachment_id, tuple(root.slot_id for root in record.roots if root.writable), tuple(root.slot_id for root in record.roots if not root.writable)) for record in records)
            + tuple(
                (
                    "resident:" + identifier,
                    tuple(finished_slots[parent] for parent, _generation, _alias, writable in captured[identifier] if writable),
                    tuple(finished_slots[parent] for parent, _generation, _alias, writable in captured[identifier] if not writable),
                )
                for identifier in sorted(missing)
            )
        )
        scope = _Scope(self, attachments, captured, records, bindings, finished)
        scope.worker = ResidentContainmentWorker(provider=self.provider, adapter=self.adapter, gate=gate, reservation=reservation, records=records, roots_guard=scope.guard_roots)
        try:
            yield scope
        finally:
            scope.worker.close()
            gate.end_resident_batch(reservation)


class _RootAdmission:
    """Shared original-root proof; callers separately establish SQL eligibility."""

    def __init__(self, factory, records, bindings, parent_ids):
        self.factory, self.bindings = factory, bindings
        self.originals = {record.attachment_id: record for record in records}
        self.parent_ids = tuple(sorted(parent_ids))
        self.volumes = {}
        self.worker = None

    async def admit_roots(self, session):
        await await_drained(run_sync_lifecycle_operation(self.worker.assert_current))
        await self._check_bindings(session)
        volumes = {}
        for original in self.originals.values():
            for root in original.roots:
                captured = self.bindings[root.backing_handle]
                binding = await session.get(SpaceBackingRow, root.backing_handle)
                if binding is None or tuple(getattr(binding, key) for key in _BINDING) != captured.facts:
                    raise AttachmentPending("Resident backing changed while acquiring lifecycle exclusion")
                parent = await session.get(SpaceRow, captured.parent_id)
                if parent is None or parent.backing_handle != root.backing_handle or (root.space_id is not None and parent.id != root.space_id):
                    raise AttachmentPending("Resident backing no longer belongs to its captured parent")
                volume = await self.factory.files._volume(session, parent)
                self._check_volume(root, volume)
                volumes[root.backing_handle] = volume
        self.volumes = volumes
        await await_drained(run_sync_lifecycle_operation(self._guard_all_roots))

    async def _check_bindings(self, session):
        for handle, captured in self.bindings.items():
            binding = await session.get(SpaceBackingRow, handle)
            parent = await session.get(SpaceRow, captured.parent_id)
            if binding is None or tuple(getattr(binding, key) for key in _BINDING) != captured.facts or parent is None or parent.backing_handle != handle:
                raise AttachmentPending("Resident backing changed while acquiring lifecycle exclusion")

    async def revalidate_roots(self, session):
        """Validation-only final phase: refresh physical proof without SQL geometry writes."""
        from deerflow.spaces.incarnations import verify_bound

        await self._check_bindings(session)
        volumes = {}
        for original in self.originals.values():
            for root in original.roots:
                previous = self.volumes.get(root.backing_handle)
                if previous is None:
                    raise AttachmentPending("Resident recovery lost its admitted bound root")

                def fresh(root=root, previous=previous):
                    self.worker.assert_current()
                    physical = self.factory.files.catalog.verify(root.slot_id, previous=previous.physical)
                    bound = verify_bound(physical, root.backing_handle)
                    self._check_volume(root, bound)
                    self.worker.assert_current()
                    return bound

                volumes[root.backing_handle] = await await_drained(run_sync_lifecycle_operation(fresh))
        self.volumes = volumes
        await await_drained(run_sync_lifecycle_operation(self._guard_all_roots))

    def _guard_all_roots(self):
        self.worker.assert_current()
        for record in self.originals.values():
            self.guard_roots(record)

    def _check_volume(self, root, volume):
        if (volume.spec.slot_id, volume.spec.filesystem_uuid, volume.device, volume.root_inode, volume.control_inode, volume.backing_handle) != (
            root.slot_id,
            root.filesystem_uuid,
            root.device,
            root.root_inode,
            root.control_inode,
            root.backing_handle,
        ):
            raise AttachmentPending("Resident qualified physical roots changed")

    def _check_projection(self, root, projection):
        if NativeResidentRoot.from_readonly(projection) != root:
            raise AttachmentPending("Resident read-only projection changed")

    def guard_roots(self, record):
        for root in record.roots:
            volume = self.volumes.get(root.backing_handle)
            if volume is None:
                raise AttachmentPending("Resident containment has no current admitted root proof")
            with volume.filesystem():
                pass
            if root.space_id is None:
                relative = str(PurePosixPath(root.source).relative_to(PurePosixPath(str(volume.data_path))))
                projection = ReadOnlyRoot.capture(volume, relative, root.destination)
                self._check_projection(root, projection)
            elif root.source != str(volume.data_path) or root.source_inode != volume.root_inode:
                raise AttachmentPending("Resident original writable source changed")


class _Scope(_RootAdmission):
    def __init__(self, factory, attachments, captured, records, bindings, finished):
        super().__init__(factory, records, bindings, {binding.parent_id for binding in bindings.values()} | {fact[0] for facts in captured.values() for fact in facts})
        self.attachments, self.captured, self.finished = attachments, captured, finished

    async def admit(self, session, records):
        if set(records) != set(self.captured):
            raise AttachmentPending("Resident containment changed its original attachment batch")
        for identifier, row in records.items():
            if identifier in self.finished:
                if _fenced_facts(row) != self.finished[identifier].facts:
                    raise AttachmentPending("Completed resident changed its historical fenced identity")
                self.attachments._closure_reason(row, None)
                continue
            original = self.originals[identifier]
            if (row.host_id, row.incarnation, row.container_id) != (original.daemon_id, original.incarnation, original.container_id):
                raise AttachmentPending("Resident original attachment identity changed")
            expected = {(root.space_id, root.generation, root.destination.removeprefix("/mnt/spaces/"), root.writable) for root in original.roots if root.space_id is not None}
            if expected != set(self.captured[identifier]):
                raise AttachmentPending("Resident original resource scope changed")
        await self.admit_roots(session)
        for identifier in sorted(self.finished):
            await await_drained(run_sync_lifecycle_operation(self._observe_finished, identifier))

    def _check_finished_journal(self, identifier):
        bridge, original = self.worker.bridge, self.finished[identifier]
        name = "hartmesh-space-" + identifier
        snapshot = bridge.evidence.snapshot()
        if (
            bridge.residents.for_attachment(identifier) is not None
            or self._retained_handoff(name, original.container_id)
            or any(record.name == name or getattr(record, "container_id", None) == original.container_id for record in (*snapshot.creates, *snapshot.teardowns, *snapshot.residents))
        ):
            raise AttachmentPending("Completed resident retains journal uncertainty")

    def _retained_handoff(self, name, container_id):
        handoffs = self.worker.bridge.handoffs
        with handoffs.lock:
            return any(handoff.attempt.name == name or (handoff.verified is not None and handoff.verified.container_id == container_id) for handoff in handoffs._items.values())

    def _observe_finished(self, identifier):
        """Current absence only; never reconstruct an erased original graph."""
        from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet

        self.worker.assert_current()
        original = self.finished[identifier]
        name = "hartmesh-space-" + identifier
        self._check_finished_journal(identifier)
        reader = NativeResourceSet(self.worker.backend, guard=self.worker.assert_current)
        if reader.inspect("container", original.container_id) is not None or reader.inspect("container", name) is not None:
            raise AttachmentPending("Completed resident has a surviving primary identity")
        paired = reader.inventory_paired()
        residents = self.factory.adapter.bootstrap_residents(known_slots=tuple(self.factory.files.catalog.volumes), paired=paired)
        if any(item.sandbox_id == identifier for item in paired) or any(item.attachment_id == identifier for item in residents):
            raise AttachmentPending("Completed resident retains a surviving resource correlation")
        self.worker.assert_current()
        self._check_finished_journal(identifier)

    async def contain(self, row):
        if row.id in self.finished:
            await await_drained(run_sync_lifecycle_operation(self._observe_finished, row.id))
            return
        operation = self.worker.confirm_absent if row.phase == "fenced" else self.worker.contain
        await await_drained(run_sync_lifecycle_operation(operation, row.id))

    async def finalize(self):
        await self._confirmed_fenced()
        for identifier in sorted(self.originals):
            await await_drained(run_sync_lifecycle_operation(self.worker.finalize, identifier, allow_prepared=True))
        for identifier in sorted(self.finished):
            await await_drained(run_sync_lifecycle_operation(self._observe_finished, identifier))
        await self._confirmed_fenced()

    async def _confirmed_fenced(self):
        async with self.factory.files.registry.host_locked(self.parent_ids) as (session, _parents):
            await self._check_bindings(session)
            records = await self.attachments._locked_records(session, self.captured)
            for identifier, original in {**self.originals, **self.finished}.items():
                row = records[identifier]
                if (row.phase, row.host_id, row.incarnation, row.container_id) != ("fenced", original.daemon_id, original.incarnation, original.container_id) or (
                    identifier in self.finished and _fenced_facts(row) != self.finished[identifier].facts
                ):
                    raise AttachmentPending("Resident finalization lacks its confirmed fenced SQL outcome")
