"""Restore access to a managed Home only under its current lifecycle ownership."""

import logging
import os
import stat

from sqlalchemy import select

from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.backings import PreparedVolumeCatalog
from deerflow.spaces.incarnations import verify_bound
from deerflow.utils.file_io import await_drained, run_file_io

logger = logging.getLogger(__name__)


def _restricted_slots(catalog):
    slots = []
    for slot, spec in catalog.volumes.items():
        if spec.kind not in (None, "assistant", "employee"):
            continue
        try:
            metadata = (spec.mount_path / "data").lstat()
        except OSError:
            continue
        if stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == os.geteuid() and metadata.st_mode & 0o700 != 0o700:
            slots.append(slot)
    return slots


async def repair_home_directories(app):
    files = app.state.storage_spaces
    catalog = getattr(files, "catalog", None)
    if not isinstance(catalog, PreparedVolumeCatalog):
        return
    slots = await run_file_io(_restricted_slots, catalog)
    if not slots:
        return
    homes = (
        select(AgentInstanceRow.id, AgentInstanceRow.generation, AgentInstanceRow.custody, SpaceRow.id, SpaceRow.generation, SpaceBackingRow.backing_handle, SpaceBackingRow.slot_id)
        .join(SpaceRow, SpaceRow.id == AgentInstanceRow.home_id)
        .join(SpaceBackingRow, (SpaceBackingRow.space_id == SpaceRow.id) & (SpaceBackingRow.backing_handle == SpaceRow.backing_handle))
        .where(
            AgentInstanceRow.status.in_(("active", "suspended")),
            SpaceRow.mode == "native",
            SpaceRow.status == "active",
            SpaceRow.custody_kind == AgentInstanceRow.custody,
            SpaceRow.custodian_id.is_not_distinct_from(AgentInstanceRow.owner_id),
            (SpaceRow.custodian_kind.is_(None) | (SpaceRow.custodian_kind == "human")),
            SpaceBackingRow.slot_id.in_(slots),
        )
    )
    async with files.registry._sf() as session:
        candidates = (await session.execute(homes)).all()
    for candidate in candidates:
        instance_id, _, custody, home, _, handle, slot = candidate
        kind = "assistant" if custody == "personal" else "employee"
        try:
            # Use the same parent and allocation locks as release and reassignment.
            async with files.registry.host_locked([home], _before_lock=files._serialize_allocation) as (session, rows):
                if home not in rows:
                    continue
                current = (await session.execute(homes.where(AgentInstanceRow.id == instance_id).with_for_update())).one_or_none()
                if current != candidate or files.catalog is not catalog or catalog.volumes[slot].kind not in (None, kind):
                    continue
                pending = await session.scalars(select(SpaceFileOperationRow.request).where(SpaceFileOperationRow.space_id == home, SpaceFileOperationRow.phase == "pending"))
                if any(not isinstance(request, dict) or request.get("action") not in ("put", "write", "rename", "mkdir", "remove", "copy") for request in pending):
                    continue
                binding = await session.get(SpaceBackingRow, handle)
                files._binding_spec(binding)
                volume = await await_drained(run_file_io(catalog.repair_bound_home, slot, previous=files._verified.get(slot), backing_handle=handle, inodes=(binding.root_inode, binding.control_inode)))
                await run_file_io(verify_bound, volume, handle, managed_home=True)
                app.state.agent_execution.unverified.pop(slot, None)
                logger.info("storage: restored access to the bound Home directory on slot %s", slot)
        except Exception as exc:
            logger.warning("storage: could not confirm Home directory repair on slot %s (%s)", slot, type(exc).__name__)
