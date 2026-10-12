"""Host resource files with durable admission; no paths or actors from plugins.

Prepared fixed filesystems are the production adapter. Native containers and
host writes share ordinary filesystem semantics; durable SQL operation records
preserve uncertain outcomes across restart. Lifecycle effects require fencing.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections import defaultdict
from collections.abc import Callable
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path

from deerflow_extension_api.storage import StorageOperationPending
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
from deerflow.spaces.backings import BackingUnavailable, PreparedVolumeCatalog
from deerflow.spaces.contract import Custody, FeatureBinding, InvalidPrincipal, MutationMode, Permission, PrincipalRef, Space, SpaceConflict, SpaceDenied
from deerflow.spaces.filesystem import FileConflict, UnsafeSpacePath, _path
from deerflow.spaces.incarnations import prepare_marker, verify_bound
from deerflow.spaces.provision import LAYOUT
from deerflow.spaces.registry import SpaceRegistry, _stored_permissions
from deerflow.utils.file_io import await_drained, run_file_io

MAX_TRANSFER_BYTES = 64 * 1024 * 1024
_reservation = ContextVar("storage_lifecycle_reservation", default=None)


class SpaceCapacityExhausted(SpaceConflict):
    """A definite effect-free refusal: no empty qualified backing slot remains."""


class SpaceOperationPending(StorageOperationPending, SpaceConflict):
    """An interrupted operation needs recovery before conflicting access."""

    def __init__(self, detail, *, cause="workflow"):
        super().__init__(detail)
        self.cause = cause


@dataclass(frozen=True, eq=False)
class _SlotClaim:
    """Identity token for cache publication; never resource authority."""

    handle: str | None = None
    phase: str = "unknown"


class SpaceFiles:
    def __init__(self, registry: SpaceRegistry, catalog: PreparedVolumeCatalog, *, inventory_path: Path | None = None, user_spaces: bool = True) -> None:
        self.registry = registry
        self.catalog = catalog
        self.inventory_path, self.user_spaces = inventory_path, user_spaces
        self.bound_slots = defaultdict(set)
        self.unclaimed_slots = defaultdict(set)
        self._verified = {}
        self._claims = {}
        # All resource metadata mutations use this same pending-operation gate.
        registry.operation_guard = self._guard
        registry.files = self
        self.attachment_guard = None
        self.workflow_settlers = {}

    async def _guard(self, session, admitted, requests, operation_id):
        from deerflow.spaces.workflows import _owner, probe

        owner = _owner.get()
        for key, (row, _grant) in admitted.items():
            if requests[key][0] & (Permission.ADMIN | Permission.OPERATE) and (owner is None or owner[0] is not self or key not in owner[1]):
                await run_file_io(probe, await self._volume(session, row))
        pending = (await session.execute(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id.in_(admitted), SpaceFileOperationRow.phase == "pending"))).scalars().all()
        for operation in pending:
            if (operation.space_id, operation.operation_id) == operation_id:
                continue
            action = operation.request.get("action") if isinstance(operation.request, dict) else None
            file_action = isinstance(action, str) and action in {"write", "put", "mkdir", "rename", "remove", "feature-workflow"}
            if not file_action or requests[operation.space_id][0] & (Permission.WRITE | Permission.OPERATE | Permission.ADMIN):
                cause = "lifecycle" if action in ("backup", "restore", "archive", "delete", "accept") else "workflow"
                raise SpaceOperationPending("A file operation has an unconfirmed outcome; recovery is required", cause=cause)
        if self.attachment_guard is not None:
            await self.attachment_guard(session, admitted, requests)
        else:
            # Adapter loss retains unconfirmed containment for administrative
            # admission. Ordinary file access does not require writer exclusion.
            from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow

            attachments = (
                await session.execute(select(SpaceAttachmentRow, SpaceMountRow).join(SpaceMountRow, SpaceMountRow.attachment_id == SpaceAttachmentRow.id).where(SpaceMountRow.space_id.in_(admitted), SpaceAttachmentRow.phase != "fenced"))
            ).all()
            for attachment, mount in attachments:
                if attachment.phase not in ("active", "pending", "fence_pending"):
                    raise SpaceOperationPending("Invalid persisted attachment state", cause="lifecycle")
                if attachment.phase != "active" and requests[mount.space_id][0] & Permission.ADMIN:
                    raise SpaceOperationPending("Storage attachment provider is required to confirm containment", cause="lifecycle")

    @asynccontextmanager
    async def provisioning(self, *, actor, name, custody, mode, feature=None, kind=None, admission=None):
        """Host-only atomic provisioning boundary for feature-owned SQL links."""
        if kind is not None and kind not in LAYOUT:
            raise ValueError("Unknown storage kind")
        creation = (
            self.registry._host_creating(name=name, custody=custody, mode=mode, feature=feature, _allocation=self._serialize_allocation)
            if actor is None
            else self.registry.creating(actor=actor, name=name, custody=custody, mode=mode, feature=feature, _allocation=self._serialize_allocation)
        )
        claimed = None
        try:
            async with creation as (session, row, permissions):
                if admission is not None:
                    await admission(session)
                used = set((await session.execute(select(SpaceBackingRow.slot_id))).scalars())
                volume = None
                for slot_id, candidate_spec in self.catalog.volumes.items():
                    if slot_id in used or getattr(candidate_spec, "kind", None) not in (kind, None):
                        continue
                    expected = self._claim(slot_id)
                    try:
                        candidate = await run_file_io(self.catalog.verify, slot_id)
                        empty = await run_file_io(self._empty, candidate)
                    except (BackingUnavailable, OSError):
                        empty = False
                    if empty:
                        volume = candidate
                        break
                    self._publish_claim(slot_id, expected, expected.handle, "unclaimed")
                if volume is None:
                    raise SpaceCapacityExhausted("No empty qualified backing capacity remains")
                spec = volume.spec
                session.add(
                    SpaceBackingRow(
                        backing_handle=row.backing_handle,
                        space_id=row.id,
                        slot_id=spec.slot_id,
                        filesystem_uuid=spec.filesystem_uuid,
                        max_bytes=spec.max_bytes,
                        max_inodes=spec.max_inodes,
                        root_inode=volume.root_inode,
                        control_inode=volume.control_inode,
                    )
                )
                await session.flush()
                claimed = self._publish_claim(spec.slot_id, self._claim(spec.slot_id), row.backing_handle, "allocating", force=True)
                await await_drained(run_file_io(prepare_marker, volume, row.backing_handle))
                if kind is not None:
                    # SQL uniqueness is established before making filesystem effects.
                    # A rolled-back or uncertain publication retains these folders.
                    await await_drained(run_file_io(self._layout, volume, kind))
                yield session, self.registry._view(row, int(permissions), allow_empty=actor is None)
        except BaseException:
            if claimed is not None:
                self._publish_claim(spec.slot_id, claimed, claimed.handle, "unclaimed")
            raise
        self._publish_claim(spec.slot_id, claimed, claimed.handle, "bound", volume=volume)

    @staticmethod
    async def _serialize_allocation(session):
        # SQLite already owns its writer. PostgreSQL must retain shared pool
        # ownership through filesystem effects and the caller's commit/rollback.
        if session.get_bind().dialect.name == "postgresql":
            await session.execute(text("SELECT pg_advisory_xact_lock(hashtext(current_schema() || '/hartmesh-space-allocation'))"))

    @staticmethod
    def _layout(volume, kind):
        with volume.filesystem() as filesystem:
            for name in LAYOUT[kind][0]:
                try:
                    filesystem.mkdir(name)
                except FileConflict:
                    filesystem.list_directory(name, limit=1)
            fd = os.dup(filesystem._control_fd)
            try:
                owner = os.fstat(fd)
                if owner.st_ino != volume.control_inode:
                    raise BackingUnavailable("Private control incarnation changed")
                for name in LAYOUT[kind][1]:
                    try:
                        os.mkdir(name, mode=0o700, dir_fd=fd)
                    except FileExistsError:
                        existing = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=fd)
                        os.close(existing)
                    else:
                        os.chown(name, owner.st_uid, owner.st_gid, dir_fd=fd, follow_symlinks=False)
                    os.fsync(fd)
            finally:
                os.close(fd)

    async def assign(self, *, kind, actor, name, custody, mode, feature=None, admission=None):
        from deerflow.features.resources import StorageFeatureLinkRow

        if kind == "user" and (not isinstance(actor, PrincipalRef) or actor.kind != "human"):
            raise SpaceDenied("User storage assignment requires a human identity")

        async def perform():
            async with self.provisioning(actor=actor, name=name, custody=custody, mode=mode, feature=feature, kind=kind, admission=admission) as (session, space):
                role = ("hm.user-space", "human:" + actor.subject_id) if kind == "user" else ("hm.company-space", "company") if kind == "company" else None
                if role is not None:
                    session.add(StorageFeatureLinkRow(namespace=role[0], key=role[1], space_id=space.id))
                    await session.flush()
            return space.id

        return await await_drained(perform())

    async def backfill_binding_markers(self):
        from deerflow.spaces.incarnation_backfill import backfill_binding_markers

        return await await_drained(backfill_binding_markers(self))

    def _claim(self, slot):
        # Keep free/unknown tombstones: deleting them would reintroduce ABA.
        return self._claims.setdefault(slot, _SlotClaim())

    def _claim_snapshot(self):
        for slot in self.catalog.volumes:
            self._claim(slot)
        return dict(self._claims)

    def _binding_claim(self, claims, slot):
        # A SQL-only slot cannot be allocated from this inventory. Register its
        # first observation only if no competing observer already claimed it.
        if slot not in claims and slot not in self._claims:
            claims[slot] = self._claim(slot)
        return claims.get(slot)

    def _publish_claim(self, slot, expected, handle, phase, *, volume=None, force=False):
        """Synchronous compare-and-swap of one claim and all derived caches."""
        if self._claim(slot) is not expected:
            return None
        claim = expected if not force and (expected.handle, expected.phase) == (handle, phase) else _SlotClaim(handle, phase)
        if claim is not expected:
            self._verified.pop(slot, None)
        self._claims[slot] = claim
        kind = getattr(self.catalog.volumes.get(slot), "kind", None)
        for slots in self.bound_slots.values():
            slots.discard(slot)
        for slots in self.unclaimed_slots.values():
            slots.discard(slot)
        if phase == "bound":
            self.bound_slots[kind].add(slot)
        elif phase in ("unknown", "allocating", "unclaimed"):
            self.unclaimed_slots[kind].add(slot)
        if volume is not None:
            self._verified[slot] = volume
        return claim

    def _cache_bound(self, slot, expected, handle, volume):
        if expected is None or expected.phase == "allocating" or expected.handle not in (None, handle):
            return False
        return self._publish_claim(slot, expected, handle, "bound", volume=volume) is not None

    async def load_bound_slots(self):
        claims = self._claim_snapshot()
        async with self.registry._sf() as session:
            bindings = (await session.execute(select(SpaceBackingRow))).scalars().all()
        used = {binding.slot_id: binding.backing_handle for binding in bindings}
        for slot in used:
            self._binding_claim(claims, slot)
        for slot, expected in claims.items():
            if expected.phase == "allocating":
                continue
            handle = used.get(slot)
            self._publish_claim(slot, expected, handle or expected.handle, "bound" if handle is not None else "unclaimed")

    async def refresh_bound_volumes(self):
        """Prove mounts before a short parent-locked publication of operator growth."""
        claims = self._claim_snapshot()
        errors = {}
        async with self.registry._sf() as session:
            bindings = (await session.execute(select(SpaceBackingRow))).scalars().all()
        proven = {}
        for binding in bindings:
            expected = self._binding_claim(claims, binding.slot_id)
            if expected is None or expected.phase == "allocating" or self._claim(binding.slot_id) is not expected:
                continue
            try:
                self._binding_spec(binding)
                volume = await run_file_io(self.catalog.verify, binding.slot_id, previous=self._verified.get(binding.slot_id))
                self._check_incarnation(binding, volume)
                await run_file_io(verify_bound, volume, binding.backing_handle)
                proven[binding.backing_handle] = volume
            except (BackingUnavailable, OSError) as exc:
                if self._claim(binding.slot_id) is expected:
                    errors[binding.slot_id] = str(exc)
        # No mount/extent I/O is awaited while holding SQLite's application
        # writer. Current binding identity is checked again under parent locks.
        publications = []
        async with self.registry._sf() as session, session.begin():
            if session.bind.dialect.name == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            rows = (await session.execute(select(SpaceRow, SpaceBackingRow.slot_id).join(SpaceBackingRow, SpaceBackingRow.space_id == SpaceRow.id).order_by(SpaceRow.id).with_for_update(of=SpaceRow))).all()
            for row, slot in rows:
                volume = proven.get(row.backing_handle)
                expected = claims.get(slot)
                if volume is None or self._claim(slot) is not expected or expected.phase == "allocating":
                    continue
                try:
                    binding = await session.get(SpaceBackingRow, row.backing_handle)
                    if binding is None or binding.space_id != row.id:
                        raise BackingUnavailable("The resource has no qualified filesystem binding")
                    self._binding_spec(binding)
                    if binding.slot_id != volume.spec.slot_id or binding.filesystem_uuid != volume.spec.filesystem_uuid:
                        raise BackingUnavailable("The resource binding changed during verification")
                    self._check_incarnation(binding, volume)
                    binding.max_bytes, binding.max_inodes = volume.spec.max_bytes, volume.spec.max_inodes
                    publications.append((slot, expected, binding.backing_handle, volume))
                except (BackingUnavailable, OSError) as exc:
                    errors[slot] = str(exc)
        # A failed or uncertain SQL commit must never publish cache facts.
        for slot, expected, handle, volume in publications:
            self._cache_bound(slot, expected, handle, volume)
        return {slot: error for slot, error in errors.items() if self._claim(slot) is claims[slot]}

    async def load_unclaimed_slots(self):
        claims = self._claim_snapshot()
        for slot, expected in claims.items():
            if expected.phase in ("bound", "allocating") or slot not in self.catalog.volumes:
                continue
            try:
                empty = await run_file_io(self._empty, await run_file_io(self.catalog.verify, slot))
            except (BackingUnavailable, OSError):
                empty = False
            self._publish_claim(slot, expected, None if empty else expected.handle, "free" if empty else "unclaimed")

    async def create(self, *, actor: PrincipalRef, name: str, custody: Custody, mode: MutationMode, feature: FeatureBinding | None = None) -> Space:
        if not self.user_spaces:
            raise SpaceDenied("Creating Spaces is not enabled on this deployment")
        if not isinstance(actor, PrincipalRef):
            raise InvalidPrincipal("Storage requires a host-validated principal")
        for _attempt in range(len(self.catalog.volumes) + 1):
            try:
                async with self.provisioning(actor=actor, name=name, custody=custody, mode=mode, feature=feature) as (_session, space):
                    pass
                return space
            except IntegrityError:
                continue
        raise SpaceConflict("Concurrent allocation exhausted qualified backing capacity")

    @staticmethod
    def _empty(volume) -> bool:
        # Provider roots are host-controlled and must be entirely unclaimed,
        # including private recovery state; dotfiles are ordinary existing data.
        return not any(volume.data_path.iterdir()) and not any(volume.control_path.iterdir())

    async def _volume(self, session, row):
        claims = self._claim_snapshot()
        binding = await session.get(SpaceBackingRow, row.backing_handle)
        if binding is None or binding.space_id != row.id:
            raise BackingUnavailable("The resource has no qualified filesystem binding")
        spec = self._binding_spec(binding)
        volume = await run_file_io(self.catalog.verify, binding.slot_id, previous=self._verified.get(binding.slot_id))
        self._check_incarnation(binding, volume)
        binding.max_bytes, binding.max_inodes = spec.max_bytes, spec.max_inodes
        bound = await run_file_io(verify_bound, volume, binding.backing_handle)
        self._cache_bound(binding.slot_id, self._binding_claim(claims, binding.slot_id), binding.backing_handle, volume)
        return bound

    def _binding_spec(self, binding):
        spec = self.catalog.volumes.get(binding.slot_id)
        if spec is None or spec.filesystem_uuid != binding.filesystem_uuid or spec.max_bytes < binding.max_bytes or spec.max_inodes < binding.max_inodes:
            raise BackingUnavailable("Provider inventory no longer matches the durable resource binding")
        return spec

    @staticmethod
    def _check_incarnation(binding, volume):
        if (volume.root_inode, volume.control_inode) != (binding.root_inode, binding.control_inode):
            raise BackingUnavailable("Durable resource root incarnation changed; preserve provider recovery state")

    def assignable_slots(self, kind, *, unverified=()):
        """A host snapshot of unbound, empty qualified capacity; never an admission."""
        excluded = set(unverified).union(*self.bound_slots.values(), *self.unclaimed_slots.values())
        return {slot for slot, spec in self.catalog.volumes.items() if getattr(spec, "kind", None) in (kind, None) and slot not in excluded}

    async def binding_verified(self, space_id):
        """Project backing facts without holding SQL across filesystem proof."""
        async with self.registry._sf() as session:
            row = await session.get(SpaceRow, space_id)
            binding = await session.get(SpaceBackingRow, row.backing_handle) if row is not None else None
            if binding is None or binding.space_id != space_id:
                return False
        try:
            self._binding_spec(binding)
            volume = await run_file_io(self.catalog.verify, binding.slot_id, previous=self._verified.get(binding.slot_id))
            self._check_incarnation(binding, volume)
            await run_file_io(verify_bound, volume, binding.backing_handle)
        except (BackingUnavailable, OSError):
            return False
        return True

    @staticmethod
    def _io(volume, call: Callable):
        with volume.filesystem() as filesystem:
            return call(filesystem)

    async def _read(self, *, actor, space_id, permission, call):
        async def perform():
            async with self.registry.admitted(actor=actor, requests={space_id: (permission, None)}) as (session, rows):
                volume = await self._volume(session, rows[space_id][0])
                return await run_file_io(self._io, volume, call)

        return await await_drained(perform())

    async def read(self, *, actor: PrincipalRef, space_id: str, path: str, max_bytes: int) -> bytes:
        return await self._bounded_read(actor=actor, space_id=space_id, permission=Permission.READ, path=path, max_bytes=max_bytes)

    async def export_file(self, *, actor: PrincipalRef, space_id: str, path: str, max_bytes: int = MAX_TRANSFER_BYTES) -> bytes:
        return await self._bounded_read(actor=actor, space_id=space_id, permission=Permission.READ | Permission.EXPORT, path=path, max_bytes=max_bytes)

    async def _bounded_read(self, *, actor, space_id, permission, path, max_bytes):
        for attempt in range(2):
            try:
                return await self._read(actor=actor, space_id=space_id, permission=permission, call=lambda fs: fs.read_bytes(path, max_bytes=max_bytes))
            except FileConflict as exc:
                if attempt or str(exc) != "File changed during its bounded read; retry":
                    raise

    async def list_directory(self, *, actor: PrincipalRef, space_id: str, path: str = "", limit: int = 200):
        return await self._read(actor=actor, space_id=space_id, permission=Permission.READ, call=lambda fs: fs.list_directory(path, limit=limit))

    async def quota(self, *, actor: PrincipalRef, space_id: str):
        async with self.registry.admitted(actor=actor, requests={space_id: (Permission.READ, None)}) as (session, rows):
            volume = await self._volume(session, rows[space_id][0])
            return {
                "max_bytes": volume.spec.max_bytes,
                "max_inodes": volume.spec.max_inodes,
                "available_bytes": volume.available_bytes,
                "available_inodes": volume.available_inodes,
                "backend": "fixed-ext4",
                "editor": "shared",
            }

    @staticmethod
    def _operation_id(value):
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
            raise ValueError("A new UUID-hex operation ID is required")

    @staticmethod
    def _same_operation(operation, actor, generation, request):
        if (operation.actor_kind, operation.actor_id, operation.generation, operation.request) != (actor.kind, actor.subject_id, generation, request):
            raise SpaceConflict("The operation ID already identifies a different request")

    async def _mutate(self, *, actor, requests, destination_id, operation_id, request, call, controller_admission=None):
        self._operation_id(operation_id)
        generation = requests[destination_id][1]
        if generation is None:
            raise ValueError("Mutations require the current lifecycle generation")

        from deerflow.spaces.workflows import _owner, _read_only, acquire, close

        descriptors = []
        token = None

        async def own(volumes):
            nonlocal descriptors, token
            owner = _owner.get()
            if owner is not None:
                if owner[0] is not self or not set(volumes).issubset(owner[1]) or _read_only.get():
                    raise SpaceDenied("Mutation requires compatible exclusive host ownership")
                return
            descriptors = await run_file_io(acquire, volumes)
            token = _owner.set((self, frozenset(volumes)))

        async def mutate():
            async with self.registry.admitted(actor=actor, requests=requests, operation_id=(destination_id, operation_id)) as (session, rows):
                if controller_admission is not None:
                    await controller_admission(session, rows)
                existing = await session.get(SpaceFileOperationRow, (destination_id, operation_id))
                if existing is not None:
                    self._same_operation(existing, actor, generation, request)
                    if existing.phase == "complete":
                        return existing.result["value"]
                    raise SpaceOperationPending("This operation has an unconfirmed or failed outcome; inspect recovery status")
                # Validate provider and audience before writing a durable intent.
                volumes = {key: await self._volume(session, row) for key, (row, _) in rows.items()}
                await own(volumes)
                if request["action"] == "copy":
                    await self._transfer_audience(session, rows, actor, request)
                session.add(SpaceFileOperationRow(space_id=destination_id, operation_id=operation_id, actor_kind=actor.kind, actor_id=actor.subject_id, generation=generation, phase="pending", request=request))
            try:
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=(destination_id, operation_id)) as (session, rows):
                    operation = await session.get(SpaceFileOperationRow, (destination_id, operation_id))
                    self._same_operation(operation, actor, generation, request)
                    if operation.phase != "pending":
                        raise SpaceOperationPending("The operation was settled before publication")
                    volumes = {key: await self._volume(session, row) for key, (row, _) in rows.items()}
                    if controller_admission is not None:
                        await controller_admission(session, rows)
                    if request["action"] == "copy":
                        await self._transfer_audience(session, rows, actor, request)
                    result = await run_file_io(call, volumes)
                    operation.phase = "complete"
                    operation.result = {"value": result}
                return result
            except (FileConflict, FileNotFoundError, UnsafeSpacePath):
                # These primitive failures occur before publishing a mutation.
                # Keep the failed ID immutable, but allow a corrected new request.
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=(destination_id, operation_id)) as (session, _rows):
                    operation = await session.get(SpaceFileOperationRow, (destination_id, operation_id))
                    operation.phase = "failed"
                raise
            except Exception as exc:
                # Filesystem publication and database commit cannot be one atomic
                # transaction. Never erase an intent or automatically replay it
                # when the publication/commit result is uncertain.
                raise SpaceOperationPending("File operation outcome is pending; retain data and request provider recovery") from exc

        async def perform():
            try:
                return await mutate()
            finally:
                if token is not None:
                    _owner.reset(token)
                await run_file_io(close, descriptors)

        # This drains the entire admitted mutation, including its final SQL
        # commit, before a cancelled caller can release host ownership.
        return await await_drained(perform())

    async def _host_write_admission(self, session, rows, destination_id, operation_id):
        row = rows[destination_id][0]
        if row.status != "active" or row.mode != "native":
            raise SpaceDenied("A host write requires an active native Space")
        # The human's READ/EXPORT grant does not waive the destination's writer
        # guard. Scope the exception only to this workflow's exact own intent.
        pending = select(SpaceFileOperationRow.operation_id).where(SpaceFileOperationRow.space_id == destination_id, SpaceFileOperationRow.phase == "pending")
        if operation_id is not None:
            pending = pending.where(SpaceFileOperationRow.operation_id != operation_id)
        if await session.scalar(pending.limit(1)) is not None:
            raise SpaceOperationPending("A destination file operation has an unconfirmed outcome; recovery is required")

    async def run_read_workflow(self, *, actor, requests, call, controller_admission=None):
        """Wait outside SQL; shared locks serialize readers against host workflows."""
        from deerflow.spaces.workflows import _owner, _read_only, acquire, close

        async def perform():
            descriptors = []
            token = None
            read_token = _read_only.set(True)
            try:
                async with self.registry.admitted(actor=actor, requests=requests) as (session, rows):
                    if controller_admission is not None:
                        await controller_admission(session, rows)
                    volumes = {key: await self._volume(session, row) for key, (row, _) in rows.items()}
                descriptors = await run_file_io(acquire, volumes, shared=True)
                token = _owner.set((self, frozenset(volumes)))
                async with self.registry.admitted(actor=actor, requests=requests) as (session, rows):
                    if controller_admission is not None:
                        await controller_admission(session, rows)
                    for key, (row, _) in rows.items():
                        current = await self._volume(session, row)
                        previous = volumes[key]
                        if (current.spec.slot_id, current.spec.filesystem_uuid, current.root_inode, current.control_inode, current.data_path, current.control_path) != (
                            previous.spec.slot_id,
                            previous.spec.filesystem_uuid,
                            previous.root_inode,
                            previous.control_inode,
                            previous.data_path,
                            previous.control_path,
                        ):
                            raise SpaceConflict("The workflow backing changed while waiting")
                result = await call(volumes)
                async with self.registry.admitted(actor=actor, requests=requests) as (session, rows):
                    if controller_admission is not None:
                        await controller_admission(session, rows)
                return result
            finally:
                _read_only.reset(read_token)
                if token is not None:
                    _owner.reset(token)
                await run_file_io(close, descriptors)

        return await await_drained(perform())

    async def run_workflow(self, *, actor, requests, destination_id, operation_id, request, call, controller_admission=None, reconcile=False, host_write=False):
        """Host-only domain work spanning its own SQL sessions and file publication.

        Private process-held filesystem locks prevent accepting/replaying a live
        writer. Durable intent prevents permissive fallback after process death.
        No SQL writer is held while the domain repository commits its own work.
        """
        from deerflow.spaces.workflows import WorkflowReceipt, WorkflowRejected, _effect, _owner, _receipt, acquire, close

        if type(host_write) is not bool:
            raise ValueError("Host-write authority requires an explicit boolean")
        permission = requests[destination_id][0]
        if host_write:
            if permission != (Permission.READ | Permission.EXPORT):
                raise ValueError("A host write requires destination READ and EXPORT")
            request = {**request, "by": "host"}
        elif not permission & (Permission.WRITE | Permission.OPERATE):
            raise SpaceDenied("A writing workflow requires mutation authority")
        self._operation_id(operation_id)
        generation = requests[destination_id][1]
        if generation is None:
            raise ValueError("Host workflows require a lifecycle generation")

        async def perform():
            descriptors = []
            token = None
            effect_token = _effect.set([False])
            receipt_token = None
            receipt = None
            try:
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=(destination_id, operation_id) if reconcile else None) as (session, rows):
                    if host_write:
                        await self._host_write_admission(session, rows, destination_id, operation_id if reconcile else None)
                    if controller_admission is not None:
                        await controller_admission(session, rows)
                    volumes = {key: await self._volume(session, row) for key, (row, _) in rows.items()}
                    descriptors = await run_file_io(acquire, volumes)
                    token = _owner.set((self, frozenset(rows)))
                    if reconcile:
                        existing = await session.get(SpaceFileOperationRow, (destination_id, operation_id))
                        if existing is None or existing.phase != "pending" or existing.request != request or existing.generation != generation:
                            raise SpaceOperationPending("No matching controller workflow intent exists")
                    else:
                        session.add(SpaceFileOperationRow(space_id=destination_id, operation_id=operation_id, actor_kind=actor.kind, actor_id=actor.subject_id, generation=generation, phase="pending", request=request))
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=(destination_id, operation_id)) as (session, rows):
                    if host_write:
                        await self._host_write_admission(session, rows, destination_id, operation_id)
                    if controller_admission is not None:
                        await controller_admission(session, rows)
                try:
                    receipt = WorkflowReceipt(volumes[destination_id], operation_id, request)
                    if reconcile:
                        await run_file_io(receipt.load)
                    else:
                        await run_file_io(receipt.save)
                    receipt_token = _receipt.set(receipt)
                    result = await call(volumes)
                    async with self.registry.admitted(actor=actor, requests=requests, operation_id=(destination_id, operation_id)) as (session, rows):
                        if host_write:
                            await self._host_write_admission(session, rows, destination_id, operation_id)
                        if controller_admission is not None:
                            await controller_admission(session, rows)
                        intent = await session.get(SpaceFileOperationRow, (destination_id, operation_id))
                        if intent.phase != "pending":
                            raise SpaceOperationPending("The host workflow reservation changed")
                        intent.phase = "complete"
                        intent.result = {"value": None, "facts": receipt.value["facts"], "resolution": "controller-reconciled" if reconcile else "confirmed"}
                    try:
                        await run_file_io(receipt.remove)
                    except OSError:
                        # A confirmed operation is not replayed for private cleanup.
                        pass
                    return result
                except WorkflowRejected as exc:
                    if _effect.get()[0]:
                        raise SpaceOperationPending("The rejected workflow may have published an effect") from exc
                    async with self.registry.admitted(actor=actor, requests=requests, operation_id=(destination_id, operation_id)) as (session, _rows):
                        intent = await session.get(SpaceFileOperationRow, (destination_id, operation_id))
                        intent.phase = "failed"
                        intent.result = {"resolution": "rejected-before-effect"}
                    try:
                        await run_file_io(receipt.remove)
                    except OSError:
                        pass
                    raise exc.error
                except Exception as exc:
                    raise SpaceOperationPending("Host workflow outcome is pending; preserve data and required feature recovery") from exc
            finally:
                if receipt_token is not None:
                    _receipt.reset(receipt_token)
                _effect.reset(effect_token)
                if token is not None:
                    _owner.reset(token)
                await run_file_io(close, descriptors)

        return await await_drained(perform())

    async def settle_interrupted(self) -> int:
        from deerflow.spaces.maintenance import settle_interrupted

        return await await_drained(settle_interrupted(self))

    @staticmethod
    def _provenance(request, on_behalf_of, *, size=None):
        if on_behalf_of is None:
            return request
        if not isinstance(on_behalf_of, str) or not 1 <= len(on_behalf_of) <= 128:
            raise ValueError("Provenance requires a bounded human account ID")
        return {"destination": None, "sha256": None, "size": size, **request, "on_behalf_of": on_behalf_of}

    async def write(self, *, actor: PrincipalRef, space_id: str, expected_generation: int, operation_id: str, path: str, content: bytes, expected_sha256: str | None = None, create: bool = False, on_behalf_of: str | None = None) -> str:
        _path(path)
        if not isinstance(content, bytes) or len(content) > MAX_TRANSFER_BYTES or type(create) is not bool:
            raise ValueError("File import/edit must contain at most64MiB")
        if (create and expected_sha256 is not None) or (not create and (not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256))):
            raise ValueError("Edits require the loaded SHA-256; creates require no previous revision")
        request = {"action": "write", "path": path, "sha256": hashlib.sha256(content).hexdigest(), "expected_sha256": expected_sha256, "create": create}
        request = self._provenance(request, on_behalf_of, size=len(content))
        return await self._mutate(
            actor=actor,
            requests={space_id: (Permission.WRITE, expected_generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request=request,
            call=lambda volumes: self._io(volumes[space_id], lambda fs: fs.write_atomic(path, content, expected_sha256=expected_sha256, create=create)),
        )

    async def mkdir(self, *, actor, space_id, expected_generation, operation_id, path, on_behalf_of: str | None = None):
        _path(path)
        return await self._mutate(
            actor=actor,
            requests={space_id: (Permission.WRITE, expected_generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request=self._provenance({"action": "mkdir", "path": path}, on_behalf_of),
            call=lambda volumes: self._io(volumes[space_id], lambda fs: fs.mkdir(path)),
        )

    async def rename(self, *, actor, space_id, expected_generation, operation_id, path, destination, on_behalf_of: str | None = None):
        _path(path)
        _path(destination)
        return await self._mutate(
            actor=actor,
            requests={space_id: (Permission.WRITE, expected_generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request=self._provenance({"action": "rename", "path": path, "destination": destination}, on_behalf_of),
            call=lambda volumes: self._io(volumes[space_id], lambda fs: fs.rename(path, destination)),
        )

    async def remove(self, *, actor, space_id, expected_generation, operation_id, path, on_behalf_of: str | None = None):
        _path(path)
        return await self._mutate(
            actor=actor,
            requests={space_id: (Permission.WRITE, expected_generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request=self._provenance({"action": "remove", "path": path}, on_behalf_of),
            call=lambda volumes: self._io(volumes[space_id], lambda fs: fs.remove(path)),
        )

    async def _transfer_audience(self, session, rows, actor, request):
        source, source_grant = rows[request["source_id"]]
        destination, _destination_grant = rows[request["destination_id"]]
        audiences = {}
        for row in (source, destination):
            grants = (await session.execute(select(SpaceGrantRow).where(SpaceGrantRow.space_id == row.id))).scalars().all()
            audiences[row.id] = {(grant.principal_kind, grant.subject_id) for grant in grants if _stored_permissions(grant.permissions, MutationMode(row.mode)) & (Permission.READ | Permission.EXPORT)}
        if audiences[destination.id] - audiences[source.id]:
            # ADMIN can admit additional readers to the existing source; this
            # transfer is scoped explicit disclosure, not a new standing grant.
            if not source_grant.permissions & Permission.ADMIN or request["acknowledge_disclosure"] is not True:
                raise SpaceDenied("Broader destination audience requires source disclosure authority and acknowledgement")

    async def copy(self, *, actor, source_id, destination_id, source_generation, destination_generation, source_path, destination_path, operation_id, acknowledge_disclosure=False):
        _path(source_path)
        _path(destination_path)
        if source_id == destination_id:
            raise ValueError("Cross-resource import requires two distinct spaces")
        request = {
            "action": "copy",
            "source_id": source_id,
            "source_generation": source_generation,
            "source_path": source_path,
            "destination_id": destination_id,
            "destination_path": destination_path,
            "acknowledge_disclosure": acknowledge_disclosure is True,
        }

        def transfer(volumes):
            content = self._io(volumes[source_id], lambda fs: fs.read_bytes(source_path, max_bytes=MAX_TRANSFER_BYTES))
            return self._io(volumes[destination_id], lambda fs: fs.write_atomic(destination_path, content, expected_sha256=None, create=True))

        return await self._mutate(
            actor=actor,
            requests={source_id: (Permission.READ | Permission.EXPORT, source_generation), destination_id: (Permission.WRITE, destination_generation)},
            destination_id=destination_id,
            operation_id=operation_id,
            request=request,
            call=transfer,
        )
