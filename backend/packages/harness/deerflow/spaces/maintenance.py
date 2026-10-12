"""Host recovery of exact abandoned intents, independent of domain workflows."""

import hashlib
import logging
import os
import re
import stat
from copy import deepcopy

from sqlalchemy import select

from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.attachments import SpaceAttachments
from deerflow.spaces.filesystem import FileConflict, _path
from deerflow.spaces.incarnations import BoundVolume, validate_marker
from deerflow.spaces.workflows import WorkflowLockMissing, WorkflowReceipt, _owner, _read_only, _receipt, acquire, close
from deerflow.utils.file_io import run_file_io

logger = logging.getLogger(__name__)
_ACTIONS = frozenset(("write", "put", "mkdir", "rename", "remove", "feature-workflow"))
_BINDING = ("backing_handle", "space_id", "slot_id", "filesystem_uuid", "root_inode", "control_inode", "max_bytes", "max_inodes")
_INTENT = ("space_id", "operation_id", "actor_kind", "actor_id", "generation", "request")


def _facts(row, attributes):
    return tuple(deepcopy(getattr(row, name)) for name in attributes)


def _owned(volume, binding):
    filesystem, descriptors = None, []
    try:
        filesystem = BoundVolume(volume, binding.backing_handle).filesystem()
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


def _metadata(filesystem, path):
    _path(path)
    try:
        fd = filesystem._open(path, os.O_PATH | os.O_NOFOLLOW, follow_symlinks=False)
    except FileNotFoundError:
        return None
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


def _direct(filesystem, request):
    path, action = _path(request["path"]), request["action"]
    metadata = _metadata(filesystem, path)
    value = None
    if action in ("write", "put"):
        expected = request["sha256"]
        if not isinstance(expected, str) or re.fullmatch(r"[0-9a-f]{64}", expected) is None:
            raise ValueError("The intended digest is invalid")
        complete = False
        if metadata is not None and stat.S_ISREG(metadata.st_mode):
            fd = filesystem.open_regular(path, follow_symlinks=False)
            try:
                before = os.fstat(fd)
                if before.st_size > 64 * 1024 * 1024:
                    raise ValueError("File exceeds bounded settlement proof")
                digest, size = hashlib.sha256(), 0
                while chunk := os.read(fd, 64 * 1024):
                    size += len(chunk)
                    if size > 64 * 1024 * 1024:
                        raise ValueError("File exceeds bounded settlement proof")
                    digest.update(chunk)
                after, current = os.fstat(fd), _metadata(filesystem, path)
                if current is None or _revision(before) != _revision(after) or _revision(after) != _revision(current) or size != after.st_size:
                    raise FileConflict("File changed during settlement")
                complete = digest.hexdigest() == expected
                value = expected if action == "write" else {"path": path, "size": size, "sha256": expected, "deduplicated": False}
            finally:
                os.close(fd)
        elif metadata is not None and stat.S_ISLNK(metadata.st_mode):
            raise ValueError("Digest proof cannot follow a link")
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
        if re.fullmatch(r"stage-[0-9a-f]{32}", name) is None:
            continue
        metadata = os.stat(name, dir_fd=root, follow_symlinks=False)
        if stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1 and metadata.st_uid == os.getuid():
            os.unlink(name, dir_fd=root)
    os.fsync(root)


async def _settle(files, candidate):
    log = getattr(files, "maintenance_logger", logger)
    filesystem, descriptors, receipt = None, [], None
    tokens = []
    settled = False
    async with files.registry._sf() as session:
        row = await session.get(SpaceRow, candidate.space_id)
        binding = await session.get(SpaceBackingRow, row.backing_handle) if row is not None else None
        if binding is None or binding.space_id != candidate.space_id or row.status != "active" or row.mode != "native":
            return False
        binding_facts = _facts(binding, _BINDING)
        row_facts = _facts(row, ("backing_handle", "generation", "status", "mode"))
    try:
        files._operation_id(candidate.operation_id)
        files._binding_spec(binding)
        volume = await run_file_io(files.catalog.verify, binding.slot_id, previous=files._verified.get(binding.slot_id))
        files._check_incarnation(binding, volume)
        # Ownership is acquired before the SQL writer and retained through commit.
        filesystem, descriptors = await run_file_io(_owned, volume, binding)
        await run_file_io(_current_roots, filesystem, volume)
        async with files.registry.host_locked([candidate.space_id]) as (session, rows):
            row = rows.get(candidate.space_id)
            current = await session.get(SpaceFileOperationRow, (candidate.space_id, candidate.operation_id))
            current_binding = await session.get(SpaceBackingRow, row.backing_handle) if row is not None else None
            if row is None or current_binding is None or _facts(current_binding, _BINDING) != binding_facts or _facts(row, ("backing_handle", "generation", "status", "mode")) != row_facts:
                return False
            if current is None or current.phase != "pending" or _facts(current, _INTENT) != _facts(candidate, _INTENT) or row.status != "active" or row.mode != "native":
                return False
            if await SpaceAttachments._current(session, [row.id], lock=True):
                return False
            await run_file_io(validate_marker, filesystem, binding.backing_handle)
            if not descriptors:
                descriptors = await run_file_io(acquire, {row.id: volume}, control_fds={row.id: filesystem._control_fd})
            tokens.append((_owner, _owner.set((files, frozenset((candidate.space_id,))))))
            tokens.append((_read_only, _read_only.set(False)))
            request = current.request
            if request["action"] == "feature-workflow":
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
            else:
                current.phase, current.result = await run_file_io(_direct, filesystem, request)
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
        if receipt is not None:
            await run_file_io(receipt.remove)
        if cleanup:
            await run_file_io(_cleanup, filesystem)
    except Exception as exc:
        label = "publication" if candidate.request.get("action") == "feature-workflow" else "file operation"
        log.warning("storage: %s %s not settled: %s", label, candidate.operation_id, exc)
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)
        try:
            if filesystem is not None:
                await run_file_io(filesystem.close)
        finally:
            await run_file_io(close, descriptors)
    return settled


async def settle_interrupted(files):
    log = getattr(files, "maintenance_logger", logger)
    async with files.registry._sf() as session:
        candidates = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending").order_by(SpaceFileOperationRow.created_at, SpaceFileOperationRow.space_id, SpaceFileOperationRow.operation_id))).all()
    count = 0
    for candidate in candidates:
        action = candidate.request.get("action") if isinstance(candidate.request, dict) else None
        if isinstance(action, str) and action in _ACTIONS:
            try:
                count += await _settle(files, candidate)
            except Exception as exc:
                log.warning("storage: file operation %s not settled: %s", candidate.operation_id, exc)
    return count
