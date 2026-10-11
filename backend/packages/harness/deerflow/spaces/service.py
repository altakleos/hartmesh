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
from pathlib import Path

from deerflow_extension_api.storage import StorageOperationPending
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
from deerflow.spaces.backings import BackingUnavailable, PreparedVolumeCatalog
from deerflow.spaces.contract import Custody, FeatureBinding, MutationMode, Permission, PrincipalRef, Space, SpaceConflict, SpaceDenied
from deerflow.spaces.filesystem import FileConflict, UnsafeSpacePath, _path
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


class SpaceFiles:
    def __init__(self, registry: SpaceRegistry, catalog: PreparedVolumeCatalog, *, inventory_path: Path | None = None, user_spaces: bool = True) -> None:
        self.registry = registry
        self.catalog = catalog
        self.inventory_path, self.user_spaces = inventory_path, user_spaces
        self.bound_slots = defaultdict(set)
        self.unclaimed_slots = defaultdict(set)
        self._verified = {}
        # All resource metadata mutations use this same pending-operation gate.
        registry.operation_guard = self._guard
        self.attachment_guard = None

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
    async def provisioning(self, *, actor, name, custody, mode, feature=None, kind=None):
        """Host-only atomic provisioning boundary for feature-owned SQL links."""
        if kind is not None and kind not in LAYOUT:
            raise ValueError("Unknown storage kind")
        async with self.registry.creating(actor=actor, name=name, custody=custody, mode=mode, feature=feature) as (session, row, permissions):
            used = set((await session.execute(select(SpaceBackingRow.slot_id))).scalars())
            volume = None
            for slot_id, candidate_spec in self.catalog.volumes.items():
                if slot_id in used or getattr(candidate_spec, "kind", None) not in (kind, None):
                    continue
                try:
                    candidate = await run_file_io(self.catalog.verify, slot_id)
                    empty = await run_file_io(self._empty, candidate)
                except (BackingUnavailable, OSError):
                    empty = False
                if empty:
                    volume = candidate
                    break
                self.unclaimed_slots[getattr(candidate_spec, "kind", None)].add(slot_id)
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
            if kind is not None:
                # SQL uniqueness is established before making filesystem effects.
                # A rolled-back or uncertain publication retains these folders.
                self.unclaimed_slots[getattr(spec, "kind", None)].add(spec.slot_id)
                await await_drained(run_file_io(self._layout, volume, kind))
            yield session, self.registry._view(row, int(permissions))
        self._verified[spec.slot_id] = volume
        self.bound_slots[getattr(spec, "kind", None)].add(spec.slot_id)
        self.unclaimed_slots[getattr(spec, "kind", None)].discard(spec.slot_id)

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

    async def assign(self, *, kind, actor, name, custody, mode):
        from deerflow.features.resources import StorageFeatureLinkRow

        if kind == "user" and actor.kind != "human":
            raise SpaceDenied("User storage assignment requires a human identity")

        async def perform():
            async with self.provisioning(actor=actor, name=name, custody=custody, mode=mode, kind=kind) as (session, space):
                role = {"user": ("hm.user-space", "human:" + actor.subject_id), "company": ("hm.company-space", "company")}.get(kind)
                if role is not None:
                    session.add(StorageFeatureLinkRow(namespace=role[0], key=role[1], space_id=space.id))
                    await session.flush()
            return space.id

        return await await_drained(perform())

    async def load_bound_slots(self):
        async with self.registry._sf() as session:
            used = set((await session.execute(select(SpaceBackingRow.slot_id))).scalars())
        self.bound_slots.clear()
        for slot in used:
            spec = self.catalog.volumes.get(slot)
            self.bound_slots[getattr(spec, "kind", None)].add(slot)

    async def refresh_bound_volumes(self):
        """Prove mounts before a short parent-locked publication of operator growth."""
        errors = {}
        async with self.registry._sf() as session:
            bindings = (await session.execute(select(SpaceBackingRow))).scalars().all()
        proven = {}
        for binding in bindings:
            try:
                self._binding_spec(binding)
                volume = await run_file_io(self.catalog.verify, binding.slot_id, previous=self._verified.get(binding.slot_id))
                self._check_incarnation(binding, volume)
                proven[binding.backing_handle] = volume
            except (BackingUnavailable, OSError) as exc:
                errors[binding.slot_id] = str(exc)
        # No mount/extent I/O is awaited while holding SQLite's application
        # writer. Current binding identity is checked again under parent locks.
        async with self.registry._sf() as session, session.begin():
            if session.bind.dialect.name == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            rows = (await session.execute(select(SpaceRow, SpaceBackingRow.slot_id).join(SpaceBackingRow, SpaceBackingRow.space_id == SpaceRow.id).order_by(SpaceRow.id).with_for_update(of=SpaceRow))).all()
            for row, slot in rows:
                volume = proven.get(row.backing_handle)
                if volume is None:
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
                    self._verified[binding.slot_id] = volume
                except (BackingUnavailable, OSError) as exc:
                    errors[slot] = str(exc)
        return errors

    async def load_unclaimed_slots(self):
        bound = set().union(*self.bound_slots.values())
        self.unclaimed_slots.clear()
        for slot, spec in self.catalog.volumes.items():
            if slot in bound:
                continue
            try:
                empty = await run_file_io(self._empty, await run_file_io(self.catalog.verify, slot))
            except (BackingUnavailable, OSError):
                empty = False
            if not empty:
                self.unclaimed_slots[getattr(spec, "kind", None)].add(slot)

    async def create(self, *, actor: PrincipalRef, name: str, custody: Custody, mode: MutationMode, feature: FeatureBinding | None = None) -> Space:
        if not self.user_spaces:
            raise SpaceDenied("Creating Spaces is not enabled on this deployment")
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
        binding = await session.get(SpaceBackingRow, row.backing_handle)
        if binding is None or binding.space_id != row.id:
            raise BackingUnavailable("The resource has no qualified filesystem binding")
        spec = self._binding_spec(binding)
        volume = await run_file_io(self.catalog.verify, binding.slot_id, previous=self._verified.get(binding.slot_id))
        self._check_incarnation(binding, volume)
        binding.max_bytes, binding.max_inodes = spec.max_bytes, spec.max_inodes
        self._verified[binding.slot_id] = volume
        return volume

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

        async def perform():
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
                for row, _grant in rows.values():
                    await self._volume(session, row)
                if request["action"] == "copy":
                    await self._transfer_audience(session, rows, actor, request)
                session.add(SpaceFileOperationRow(space_id=destination_id, operation_id=operation_id, actor_kind=actor.kind, actor_id=actor.subject_id, generation=generation, phase="pending", request=request))
            try:
                async with self.registry.admitted(actor=actor, requests=requests, operation_id=(destination_id, operation_id)) as (session, rows):
                    operation = await session.get(SpaceFileOperationRow, (destination_id, operation_id))
                    self._same_operation(operation, actor, generation, request)
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

        # This drains the entire admitted mutation, including its final SQL
        # commit, before a cancelled caller can release host ownership.
        return await await_drained(perform())

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

    async def run_workflow(self, *, actor, requests, destination_id, operation_id, request, call, controller_admission=None, reconcile=False):
        """Host-only domain work spanning its own SQL sessions and file publication.

        Private process-held filesystem locks prevent accepting/replaying a live
        writer. Durable intent prevents permissive fallback after process death.
        No SQL writer is held while the domain repository commits its own work.
        """
        from deerflow.spaces.workflows import WorkflowReceipt, WorkflowRejected, _effect, _owner, _receipt, acquire, close

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

    async def write(self, *, actor: PrincipalRef, space_id: str, expected_generation: int, operation_id: str, path: str, content: bytes, expected_sha256: str | None = None, create: bool = False) -> str:
        _path(path)
        if not isinstance(content, bytes) or len(content) > MAX_TRANSFER_BYTES or type(create) is not bool:
            raise ValueError("File import/edit must contain at most64MiB")
        if (create and expected_sha256 is not None) or (not create and (not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256))):
            raise ValueError("Edits require the loaded SHA-256; creates require no previous revision")
        request = {"action": "write", "path": path, "sha256": hashlib.sha256(content).hexdigest(), "expected_sha256": expected_sha256, "create": create}
        return await self._mutate(
            actor=actor,
            requests={space_id: (Permission.WRITE, expected_generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request=request,
            call=lambda volumes: self._io(volumes[space_id], lambda fs: fs.write_atomic(path, content, expected_sha256=expected_sha256, create=create)),
        )

    async def mkdir(self, *, actor, space_id, expected_generation, operation_id, path):
        _path(path)
        return await self._mutate(
            actor=actor,
            requests={space_id: (Permission.WRITE, expected_generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request={"action": "mkdir", "path": path},
            call=lambda volumes: self._io(volumes[space_id], lambda fs: fs.mkdir(path)),
        )

    async def rename(self, *, actor, space_id, expected_generation, operation_id, path, destination):
        _path(path)
        _path(destination)
        return await self._mutate(
            actor=actor,
            requests={space_id: (Permission.WRITE, expected_generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request={"action": "rename", "path": path, "destination": destination},
            call=lambda volumes: self._io(volumes[space_id], lambda fs: fs.rename(path, destination)),
        )

    async def remove(self, *, actor, space_id, expected_generation, operation_id, path):
        _path(path)
        return await self._mutate(
            actor=actor,
            requests={space_id: (Permission.WRITE, expected_generation)},
            destination_id=space_id,
            operation_id=operation_id,
            request={"action": "remove", "path": path},
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
