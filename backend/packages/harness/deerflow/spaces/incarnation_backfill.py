"""Dedicated host startup boundary for existing, durably bound physical roots."""

import os

from sqlalchemy import select

from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.spaces.filesystem import ConfinedFilesystem
from deerflow.spaces.incarnations import BINDING_MARKER, publish_marker, validate_marker
from deerflow.spaces.maintenance import _BINDING, _current_roots, _facts
from deerflow.spaces.workflows import WorkflowLockMissing, acquire, close
from deerflow.utils.file_io import run_file_io


def _inspect(volume, binding):
    filesystem = ConfinedFilesystem(volume.data_path, volume.control_path, expected_data=(volume.device, binding.root_inode), expected_control=(volume.device, binding.control_inode))
    try:
        try:
            os.stat(BINDING_MARKER, dir_fd=filesystem._control_fd, follow_symlinks=False)
        except FileNotFoundError:
            return filesystem, False
        validate_marker(filesystem, binding.backing_handle)
        return filesystem, True
    except BaseException:
        filesystem.close()
        raise


async def _backfill(files, candidate):
    filesystem, descriptors = None, []
    try:
        files._binding_spec(candidate)
        volume = await run_file_io(files.catalog.verify, candidate.slot_id, previous=files._verified.get(candidate.slot_id))
        files._check_incarnation(candidate, volume)
        filesystem, present = await run_file_io(_inspect, volume, candidate)
        if present:
            return
        try:
            descriptors = await run_file_io(acquire, {candidate.space_id: volume}, create=False, control_fds={candidate.space_id: filesystem._control_fd})
        except WorkflowLockMissing:
            pass
        async with files.registry.host_locked([candidate.space_id]) as (session, rows):
            row = rows.get(candidate.space_id)
            current = await session.get(SpaceBackingRow, candidate.backing_handle)
            if row is None or row.backing_handle != candidate.backing_handle or current is None or _facts(current, _BINDING) != _facts(candidate, _BINDING):
                raise ValueError("The durable binding changed before marker backfill")
            await run_file_io(_current_roots, filesystem, volume)
            if not descriptors:
                # No lock file is created until the exact binding is locked.
                descriptors = await run_file_io(acquire, {row.id: volume}, control_fds={row.id: filesystem._control_fd})
            await run_file_io(publish_marker, filesystem, current.backing_handle)
            await run_file_io(_current_roots, filesystem, volume)
    finally:
        try:
            if filesystem is not None:
                await run_file_io(filesystem.close)
        finally:
            await run_file_io(close, descriptors)


async def backfill_binding_markers(files):
    async with files.registry._sf() as session:
        candidates = (await session.scalars(select(SpaceBackingRow).order_by(SpaceBackingRow.space_id))).all()
    errors = {}
    for candidate in candidates:
        try:
            await _backfill(files, candidate)
        except Exception as exc:
            errors[candidate.slot_id] = str(exc)
    return errors
