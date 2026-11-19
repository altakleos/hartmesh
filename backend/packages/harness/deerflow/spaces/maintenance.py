"""Host recovery of exact abandoned intents, independent of domain workflows."""

import errno
import hashlib
import logging
import os
import re
import stat
from copy import deepcopy
from dataclasses import dataclass

from sqlalchemy import select

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces import directory_creation
from deerflow.spaces.attachments import RetainedAttachment, SpaceAttachments
from deerflow.spaces.filesystem import FileConflict, UnsafeSpacePath, _path
from deerflow.spaces.incarnations import BoundVolume, validate_marker
from deerflow.spaces.settlement import host_settlement_result
from deerflow.spaces.workflows import WorkflowLockMissing, WorkflowReceipt, _owner, _read_only, _receipt, acquire, close
from deerflow.utils.file_io import run_file_io

logger = logging.getLogger(__name__)
_DIRECT_ACTIONS = frozenset(("write", "put", "mkdir", "rename", "remove"))
_ACTIONS = _DIRECT_ACTIONS | {"feature-workflow"}
_BINDING = ("backing_handle", "space_id", "slot_id", "filesystem_uuid", "root_inode", "control_inode", "max_bytes", "max_inodes")
_INTENT = ("space_id", "operation_id", "actor_kind", "actor_id", "generation", "request")
_OBSERVED_INTENT = (*_INTENT, "created_at", "result")


def _facts(row, attributes):
    return tuple(deepcopy(getattr(row, name)) for name in attributes)


def _owned(volume, binding, *, managed_home=False):
    filesystem, descriptors = None, []
    try:
        filesystem = BoundVolume(volume, binding.backing_handle, managed_home=managed_home).filesystem()
        if (os.fstat(filesystem._data_fd).st_ino, os.fstat(filesystem._control_fd).st_ino) != (binding.root_inode, binding.control_inode):
            raise OSError("Durable binding root changed before ownership")
        try:
            descriptors = acquire({binding.space_id: volume}, create=False, control_fds={binding.space_id: filesystem._control_fd})
        except WorkflowLockMissing:
            pass  # Creation is permitted only after the fresh parent check.
        return filesystem, descriptors
    except BaseException:
        try:
            close(descriptors)
        finally:
            if filesystem is not None:
                filesystem.close()
        raise


class _NameUnconfirmed(Exception):
    """The bounded proof could not inspect the operation's named entry."""


def _metadata(filesystem, path):
    _path(path)
    try:
        fd = filesystem._open(path, os.O_PATH | os.O_NOFOLLOW, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except (PermissionError, UnsafeSpacePath) as exc:
        if isinstance(exc, PermissionError) and exc.errno not in (errno.EACCES, errno.EPERM):
            raise
        raise _NameUnconfirmed from exc
    try:
        return os.fstat(fd)
    finally:
        os.close(fd)


def _current_roots(filesystem, volume):
    for path, fd in ((volume.data_path, filesystem._data_fd), (volume.control_path, filesystem._control_fd)):
        current, owned = os.stat(path, follow_symlinks=False), os.fstat(fd)
        if (current.st_dev, current.st_ino) != (owned.st_dev, owned.st_ino):
            raise OSError("Qualified resource root changed during settlement")


def _revision(metadata):
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns


def _require_same_entry(filesystem, path, observed):
    try:
        current = _metadata(filesystem, path)
    except _NameUnconfirmed as exc:
        raise FileConflict("File changed during settlement") from exc
    if current is None or _revision(current) != _revision(observed):
        raise FileConflict("File changed during settlement")


def _direct_path(request):
    path, action = _path(request["path"]), request["action"]
    # Malformed intents cannot be interpreted as a refusal to inspect a name.
    if action in ("write", "put") and (not isinstance(request["sha256"], str) or re.fullmatch(r"[0-9a-f]{64}", request["sha256"]) is None):
        raise ValueError("The intended digest is invalid")
    if action == "rename":
        _path(request["destination"])
    return path


def _direct(filesystem, request):
    path = _direct_path(request)
    try:
        return _observed_direct(filesystem, request, request["action"], path)
    except _NameUnconfirmed:
        return "failed", {"resolution": "host-unconfirmed"}


def _observed_direct(filesystem, request, action, path):
    metadata = _metadata(filesystem, path)
    value = None
    if action in ("write", "put"):
        expected = request["sha256"]
        complete = False
        if metadata is not None and stat.S_ISREG(metadata.st_mode):
            try:
                fd = filesystem.open_regular(path, follow_symlinks=False)
            except (PermissionError, UnsafeSpacePath) as exc:
                if isinstance(exc, PermissionError) and exc.errno not in (errno.EACCES, errno.EPERM):
                    raise
                _require_same_entry(filesystem, path, metadata)
                raise _NameUnconfirmed from exc
            try:
                before = os.fstat(fd)
                if _revision(before) != _revision(metadata):
                    raise FileConflict("File changed during settlement")
                if before.st_size > 64 * 1024 * 1024:
                    _require_same_entry(filesystem, path, before)
                    raise _NameUnconfirmed
                digest, size = hashlib.sha256(), 0
                while chunk := os.read(fd, 64 * 1024):
                    size += len(chunk)
                    if size > before.st_size:
                        raise FileConflict("File grew during settlement")
                    digest.update(chunk)
                after = os.fstat(fd)
                if _revision(before) != _revision(after) or size != after.st_size:
                    raise FileConflict("File changed during settlement")
                _require_same_entry(filesystem, path, after)
                complete = digest.hexdigest() == expected
                value = expected if action == "write" else {"path": path, "size": size, "sha256": expected, "deduplicated": False}
            finally:
                os.close(fd)
        elif metadata is not None and stat.S_ISLNK(metadata.st_mode):
            raise _NameUnconfirmed
    elif action == "mkdir":
        complete = metadata is not None and stat.S_ISDIR(metadata.st_mode)
    elif action == "rename":
        complete = metadata is None and _metadata(filesystem, request["destination"]) is not None
    else:
        complete = metadata is None
    return ("complete", {"resolution": "host-settled", "value": value}) if complete else ("failed", {"resolution": "host-settled"})


def _cleanup(filesystem):
    root = filesystem._control_fd
    for name in os.listdir(root):
        if name == ".mkdir-stage":
            metadata = os.stat(name, dir_fd=root, follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == os.getuid() and not metadata.st_mode & 0o077:
                try:
                    os.rmdir(name, dir_fd=root)
                except OSError as exc:
                    if exc.errno not in (errno.ENOTEMPTY, errno.EEXIST):
                        raise
            continue
        if re.fullmatch(r"stage-[0-9a-f]{32}", name) is None:
            continue
        metadata = os.stat(name, dir_fd=root, follow_symlinks=False)
        if stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1 and metadata.st_uid == os.getuid():
            os.unlink(name, dir_fd=root)
    os.fsync(root)


@dataclass(frozen=True)
class BlockedFileOperation:
    """A pending direct operation observed under writer exclusion with one active Home mount."""

    space_id: str
    operation_id: str
    attachment: RetainedAttachment
    intent: tuple
    parent: tuple

    @classmethod
    async def capture(cls, session, parent, operation, mounts):
        if parent is None or parent.status != "active" or parent.mode != "native" or operation is None or operation.phase != "pending" or operation.generation != parent.generation:
            return None
        if len(mounts) != 1 or not isinstance(operation.request, dict) or operation.request.get("action") not in _DIRECT_ACTIONS:
            return None
        if operation.request["action"] == "mkdir" and operation.result is not None and not directory_creation.valid_identity(operation.result):
            return None
        attachment, mount = mounts[0]
        if attachment.phase != "active" or attachment.actor_kind != "nonhuman" or (mount.generation, mount.alias, mount.writable) != (parent.generation, "home", 1):
            return None
        scope = (await SpaceAttachments._mount_scope(session, [attachment.id]))[attachment.id]
        if len(scope) != 1:
            return None
        other = await session.scalar(
            select(SpaceFileOperationRow.operation_id).where(SpaceFileOperationRow.space_id == parent.id, SpaceFileOperationRow.phase == "pending", SpaceFileOperationRow.operation_id != operation.operation_id).limit(1)
        )
        if other is not None:
            return None
        _direct_path(operation.request)
        claim = SpaceAttachments._retained_claim(attachment, scope)
        return cls(parent.id, operation.operation_id, claim, _facts(operation, _OBSERVED_INTENT), _facts(parent, ("backing_handle", "custody_kind", "custodian_kind", "custodian_id")))

    async def matches_locked(self, session, parent):
        operation = await session.get(SpaceFileOperationRow, (self.space_id, self.operation_id), with_for_update=True)
        if operation is None or _facts(operation, _OBSERVED_INTENT) != self.intent:
            return False
        mounts = await SpaceAttachments._current(session, [self.space_id], lock=True)
        return await self.capture(session, parent, operation, mounts) == self

    async def acquire_if_current(self, files, session):
        """Borrow the containment transaction. Caller holds returned locks through intent commit."""
        from deerflow.spaces.service import SpaceOperationPending

        parent = await session.get(SpaceRow, self.space_id)
        if not await self.matches_locked(session, parent):
            return []
        volume = await files._volume(session, parent)
        try:
            return await run_file_io(acquire, {self.space_id: volume}, create=False)
        except (SpaceOperationPending, WorkflowLockMissing):
            return []


async def _settle(files, candidate, *, discard=False, raise_errors=False, operator_host_id=None, discard_admission=None, observe=False):
    if discard and (not isinstance(operator_host_id, str) or not 1 <= len(operator_host_id) <= 128):
        raise ValueError("Operator discard requires its Docker host identity")
    log = getattr(files, "maintenance_logger", logger)
    filesystem, descriptors, receipt = None, [], None
    tokens = []
    settled = False
    async with files.registry._sf() as session:
        row = await session.get(SpaceRow, candidate.space_id)
        binding = await session.get(SpaceBackingRow, row.backing_handle) if row is not None else None
        if binding is None or binding.space_id != candidate.space_id or (not discard and row.status != "active") or row.mode != "native":
            return False
        binding_facts = _facts(binding, _BINDING)
        managed_homes = await files._untyped_homes(session, [binding])
        row_facts = _facts(row, ("backing_handle", "generation", "status", "mode"))
    try:
        files._operation_id(candidate.operation_id)
        files._binding_spec(binding)
        volume = await run_file_io(files.catalog.verify, binding.slot_id, previous=files._verified.get(binding.slot_id))
        files._check_incarnation(binding, volume)
        # Ownership is acquired before the SQL writer and retained through commit.
        filesystem, descriptors = await run_file_io(_owned, volume, binding, managed_home=binding.space_id in managed_homes)
        await run_file_io(_current_roots, filesystem, volume)
        async with files.registry.host_locked([candidate.space_id]) as (session, rows):
            row = rows.get(candidate.space_id)
            current = await session.get(SpaceFileOperationRow, (candidate.space_id, candidate.operation_id))
            current_binding = await session.get(SpaceBackingRow, row.backing_handle) if row is not None else None
            if row is None or current_binding is None or _facts(current_binding, _BINDING) != binding_facts or _facts(row, ("backing_handle", "generation", "status", "mode")) != row_facts:
                return False
            if current is None or current.phase != "pending" or _facts(current, _INTENT) != _facts(candidate, _INTENT) or (not discard and row.status != "active") or row.mode != "native":
                return False
            mounts = await SpaceAttachments._current(session, [row.id], lock=True)
            if mounts and not observe:
                return False
            await run_file_io(validate_marker, filesystem, binding.backing_handle)
            if not descriptors:
                descriptors = await run_file_io(acquire, {row.id: volume}, control_fds={row.id: filesystem._control_fd})
            if mounts:
                return await BlockedFileOperation.capture(session, row, current, mounts)
            tokens.append((_owner, _owner.set((files, frozenset((candidate.space_id,))))))
            tokens.append((_read_only, _read_only.set(False)))
            request = current.request
            if discard:
                if discard_admission is not None:
                    await discard_admission()
                current.phase, current.result = "failed", {"resolution": "host-discarded", "by": "operator", "host_id": operator_host_id}
            elif request["action"] == "feature-workflow":
                handler = files.workflow_settlers.get(request.get("namespace"))
                if handler is None:
                    return False
                receipt = WorkflowReceipt(volume, current.operation_id, request, control_fd=filesystem._control_fd)
                await run_file_io(receipt.load)
                tokens.append((_receipt, _receipt.set(receipt)))
                resolution = await handler(current, session=session)
                if resolution not in ("host-settled", "host-reconciled"):
                    raise ValueError("Workflow settlement did not confirm an outcome")
                current.phase, current.result = "complete", {"resolution": resolution, "value": None, "facts": receipt.value["facts"]}
            elif request["action"] == "mkdir" and current.result is not None:
                current.phase, current.result = await run_file_io(directory_creation.settle, filesystem, _path(request["path"]), current.result)
            else:
                current.phase, current.result = await run_file_io(_direct, filesystem, request)
            current.result = host_settlement_result(current.result)
            await run_file_io(_current_roots, filesystem, volume)
            phase = current.phase
            kind = getattr(volume.spec, "kind", None) or row.custody_kind
            owner = row.custodian_id if row.custody_kind == "personal" else "company"
            cleanup = (
                await session.scalar(select(SpaceFileOperationRow.operation_id).where(SpaceFileOperationRow.space_id == row.id, SpaceFileOperationRow.phase == "pending", SpaceFileOperationRow.operation_id != current.operation_id).limit(1))
                is None
            )
        # Exiting host_locked confirms commit. Never remove evidence before it.
        settled = True
        log.info("storage: file operation %s of %s %s settled %s", candidate.operation_id, kind, owner, phase)
        if receipt is not None and not discard:
            await run_file_io(receipt.remove)
        if cleanup and not discard:
            await run_file_io(_cleanup, filesystem)
    except Exception as exc:
        label = "publication" if candidate.request.get("action") == "feature-workflow" else "file operation"
        log.warning("storage: %s %s in space %s not settled: %s", label, candidate.operation_id, candidate.space_id, exc)
        if raise_errors:
            raise
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)
        try:
            if filesystem is not None:
                await run_file_io(filesystem.close)
        finally:
            await run_file_io(close, descriptors)
    return settled


async def settle_file_operation(files, candidate, *, discard=False, raise_errors=False, operator_host_id=None, discard_admission=None):
    """Settle one captured operation through its workflow's normal lock and checks."""
    return await _settle(files, candidate, discard=discard, raise_errors=raise_errors, operator_host_id=operator_host_id, discard_admission=discard_admission)


async def discard_interrupted(files, *, space_id, operation_id, host_id):
    """Discard only one exact abandoned intent; do no replay or file cleanup."""
    files._operation_id(space_id)
    files._operation_id(operation_id)
    async with files.registry._sf() as session:
        candidate = await session.get(SpaceFileOperationRow, (space_id, operation_id))
    if candidate is None or candidate.phase != "pending":
        return False
    from deerflow.utils.file_io import await_drained

    return await await_drained(_settle(files, candidate, discard=True, raise_errors=True, operator_host_id=host_id))


async def settle_interrupted(files, *, space_id=None, operation_id=None, blocked_residents=None):
    log = getattr(files, "maintenance_logger", logger)
    statement = select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending")
    if space_id is not None:
        statement = statement.where(SpaceFileOperationRow.space_id == space_id, SpaceFileOperationRow.operation_id == operation_id)
    statement = statement.order_by(SpaceFileOperationRow.created_at, SpaceFileOperationRow.space_id, SpaceFileOperationRow.operation_id)
    async with files.registry._sf() as session:
        candidates = (await session.scalars(statement)).all()
    count = 0
    for candidate in candidates:
        action = candidate.request.get("action") if isinstance(candidate.request, dict) else None
        if isinstance(action, str) and action in _ACTIONS:
            try:
                result = await _settle(files, candidate, observe=blocked_residents is not None and space_id is None and action in _DIRECT_ACTIONS)
                if isinstance(result, BlockedFileOperation):
                    blocked_residents[result.attachment.id] = result
                elif result:
                    count += 1
            except Exception as exc:
                log.warning("storage: file operation %s in space %s not settled: %s", candidate.operation_id, candidate.space_id, exc)
    return count
