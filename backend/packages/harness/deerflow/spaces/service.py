"""Host resource files with durable admission; no paths or actors from plugins.

Prepared fixed filesystems are the production adapter. Until a qualified native
attachment provider is installed, new roots admit only this serialized host
writer. Their SQL operation records preserve uncertain outcomes across restart.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable
from contextvars import ContextVar

from deerflow_extension_api.storage import StorageOperationPending
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.backings import BackingUnavailable, PreparedVolumeCatalog
from deerflow.spaces.contract import Custody, FeatureBinding, MutationMode, Permission, PrincipalRef, Space, SpaceConflict, SpaceDenied
from deerflow.spaces.filesystem import FileConflict, UnsafeSpacePath, _path
from deerflow.spaces.registry import SpaceRegistry, _stored_permissions
from deerflow.utils.file_io import await_drained, run_file_io

MAX_TRANSFER_BYTES = 64 * 1024 * 1024
_reservation = ContextVar("storage_lifecycle_reservation", default=None)


class SpaceOperationPending(StorageOperationPending, SpaceConflict):
    """An interrupted operation needs explicit recovery before further access."""


class SpaceFiles:
    def __init__(self, registry: SpaceRegistry, catalog: PreparedVolumeCatalog) -> None:
        self.registry = registry
        self.catalog = catalog
        self._verified = {}
        # All resource metadata mutations use this same pending-operation gate.
        registry.operation_guard = self._guard
        self.attachment_guard = None

    async def _guard(self, session, admitted, requests, operation_id):
        pending = (await session.execute(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id.in_(admitted), SpaceFileOperationRow.phase == "pending"))).scalars().all()
        if any((row.space_id, row.operation_id) != operation_id for row in pending):
            raise SpaceOperationPending("A file operation has an unconfirmed outcome; recovery is required")
        if self.attachment_guard is not None:
            await self.attachment_guard(session, admitted, requests)
        else:
            # Adapter loss/restart cannot turn persisted mounts into an
            # exclusive host edit window. Retain facts until real containment.
            from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow, SpaceMountRow

            attachments = (
                await session.execute(select(SpaceAttachmentRow, SpaceMountRow).join(SpaceMountRow, SpaceMountRow.attachment_id == SpaceAttachmentRow.id).where(SpaceMountRow.space_id.in_(admitted), SpaceAttachmentRow.phase != "fenced"))
            ).all()
            for attachment, mount in attachments:
                if attachment.phase != "active" or requests[mount.space_id][0] & (Permission.WRITE | Permission.ADMIN):
                    raise SpaceOperationPending("Storage attachment provider is required to confirm containment")

    async def create(self, *, actor: PrincipalRef, name: str, custody: Custody, mode: MutationMode, feature: FeatureBinding | None = None) -> Space:
        # Unique slot/filesystem constraints arbitrate simultaneous PostgreSQL
        # allocators. A conflict rolls back the entire resource and retries the
        # remaining inventory; no orphan resource or recycled tombstone root.
        for _attempt in range(len(self.catalog.volumes) + 1):
            try:
                async with self.registry.creating(actor=actor, name=name, custody=custody, mode=mode, feature=feature) as (session, row, permissions):
                    used = set((await session.execute(select(SpaceBackingRow.slot_id))).scalars())
                    volume = None
                    for slot_id in sorted(set(self.catalog.volumes) - used):
                        candidate = await run_file_io(self.catalog.verify, slot_id)
                        if await run_file_io(self._empty, candidate):
                            volume = candidate
                            break
                    if volume is None:
                        raise SpaceConflict("No empty qualified backing capacity remains")
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
                    space = self.registry._view(row, int(permissions))
                self._verified[spec.slot_id] = volume
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
        spec = self.catalog.volumes.get(binding.slot_id)
        if spec is None or (spec.filesystem_uuid, spec.max_bytes, spec.max_inodes) != (binding.filesystem_uuid, binding.max_bytes, binding.max_inodes):
            raise BackingUnavailable("Provider inventory no longer matches the durable resource binding")
        volume = await run_file_io(self.catalog.verify, binding.slot_id, previous=self._verified.get(binding.slot_id))
        if (volume.root_inode, volume.control_inode) != (binding.root_inode, binding.control_inode):
            raise BackingUnavailable("Durable resource root incarnation changed; preserve provider recovery state")
        self._verified[binding.slot_id] = volume
        return volume

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
        return await self._read(actor=actor, space_id=space_id, permission=Permission.READ, call=lambda fs: fs.read_bytes(path, max_bytes=max_bytes))

    async def export_file(self, *, actor: PrincipalRef, space_id: str, path: str, max_bytes: int = MAX_TRANSFER_BYTES) -> bytes:
        return await self._read(actor=actor, space_id=space_id, permission=Permission.READ | Permission.EXPORT, call=lambda fs: fs.read_bytes(path, max_bytes=max_bytes))

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
                "editor": "exclusive-host-window",
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
