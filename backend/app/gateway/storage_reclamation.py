"""Gateway reclamation borrows the canonical native lifecycle and release proof."""

import asyncio
import json
import logging
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass

from sqlalchemy import select

from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from deerflow.community.aio_sandbox.owner_admission import NativeRetirementBusy
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.contract import SpaceConflict
from deerflow.spaces.recovery import SpaceRecovery

logger = logging.getLogger(__name__)
_BINDING = ("backing_handle", "space_id", "slot_id", "filesystem_uuid", "max_bytes", "max_inodes", "root_inode", "control_inode")
_SPACE = ("id", "generation", "backing_handle", "status", "mode", "custody_kind", "custodian_kind", "custodian_id", "feature_namespace", "feature_controller", "feature_metadata_version")
RETIREMENT_WAIT_SECONDS = 5.0


@asynccontextmanager
async def _retirement_scope(retire, owner_id, slot_id):
    """Retry only a pre-effect busy reservation; keep one bounded loop-owned wait."""
    deadline = asyncio.get_running_loop().time() + RETIREMENT_WAIT_SECONDS
    async with AsyncExitStack() as stack:
        while True:
            try:
                scope = await stack.enter_async_context(retire(owner_id, slot_id))
                break
            except NativeRetirementBusy:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise
                await asyncio.sleep(min(0.1, remaining))
        yield scope


@dataclass(frozen=True)
class _ReleasedSpace:
    account_id: str
    released_at: object
    space_id: str
    slot_id: str
    space: tuple
    binding: tuple
    deletion: tuple


async def _snapshot(files, session, account, link, row, binding):
    if (
        account is None
        or account.released_at is None
        or link is None
        or row is None
        or binding is None
        or (link.namespace, link.key, link.space_id) != ("hm.user-space", "human:" + account.id, row.id)
        or (row.status, row.mode, row.custody_kind, row.custodian_kind, row.custodian_id, row.feature_namespace, row.feature_controller, row.feature_metadata_version)
        != ("deleted", "native", "personal", "human", account.id, None, None, None)
        or (binding.backing_handle, binding.space_id) != (row.backing_handle, row.id)
    ):
        return None
    # Inventory metadata is enough here. Root rotation intentionally makes
    # ordinary bound-volume proof unavailable during an interrupted release.
    spec = files._binding_spec(binding)
    if getattr(spec, "kind", None) != "user":
        return None
    event = await session.scalar(select(SpaceEventRow).where(SpaceEventRow.space_id == row.id, SpaceEventRow.action == "delete").order_by(SpaceEventRow.generation.desc()).limit(1))
    if event is None or not isinstance(event.details, dict) or event.details.get("retained_backups") is not False:
        return None
    return _ReleasedSpace(
        account.id,
        account.released_at,
        row.id,
        binding.slot_id,
        tuple(getattr(row, name) for name in _SPACE),
        tuple(getattr(binding, name) for name in _BINDING),
        (event.generation, event.actor_kind, event.actor_id, json.dumps(event.details, sort_keys=True, allow_nan=False)),
    )


async def _candidates(files):
    selected = []
    async with files.registry._sf() as session:
        rows = await session.execute(
            select(UserRow, StorageFeatureLinkRow, SpaceRow, SpaceBackingRow)
            .select_from(UserRow)
            .join(StorageFeatureLinkRow, StorageFeatureLinkRow.key == "human:" + UserRow.id)
            .join(SpaceRow, SpaceRow.id == StorageFeatureLinkRow.space_id)
            .join(SpaceBackingRow, SpaceBackingRow.backing_handle == SpaceRow.backing_handle)
            .where(UserRow.released_at.is_not(None), StorageFeatureLinkRow.namespace == "hm.user-space", SpaceRow.status == "deleted")
            .order_by(SpaceRow.id)
        )
        for account, link, row, binding in rows:
            try:
                candidate = await _snapshot(files, session, account, link, row, binding)
            except (BackingUnavailable, ValueError, TypeError):
                logger.warning("storage: released User Space %s retains unverified metadata", row.id)
                continue
            if candidate is not None:
                selected.append(candidate)
    return selected


async def _admitted(app, files, session, candidate, provider, scope):
    status = app.state.agent_execution
    if status.qualified_provider is not provider or status.storage_reason is not None:
        raise SpaceConflict("The qualified containment provider changed")
    scope.assert_current()
    row = await session.get(SpaceRow, candidate.space_id)
    account = await session.get(UserRow, candidate.account_id, with_for_update=True)
    link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + candidate.account_id))
    binding = await session.get(SpaceBackingRow, candidate.binding[0])
    current = await _snapshot(files, session, account, link, row, binding)
    if current != candidate:
        raise SpaceConflict("Released account, User role or backing changed; preserve its evidence")
    return link


async def reclaim_released_user_spaces(app):
    from app.gateway.storage_spaces import serialize_role_assignment, storage_state

    files = getattr(app.state, "storage_spaces", None)
    status = getattr(app.state, "agent_execution", None)
    provider = getattr(status, "qualified_provider", None)
    retire = getattr(provider, "_native_retirement_scope", None)
    if files is None or not callable(retire) or (await storage_state(app)).state in ("off", "unavailable"):
        return 0
    candidates = await _candidates(files)
    reclaimed = 0
    repository = SQLiteUserRepository(files.registry._sf)

    async def prerequisite(session):
        dialect = session.get_bind().dialect.name
        if dialect == "postgresql":
            await repository.serialize_seats(session)
        elif dialect != "sqlite":
            raise SpaceConflict("Released-account reclamation requires a qualified SQL writer")
        # host_locked already owns SQLite BEGIN IMMEDIATE. Re-entering the
        # repository's writer reservation would start a nested transaction.
        await serialize_role_assignment(session)

    for candidate in candidates:
        try:
            # Preflight SQL is closed. Keep this exact provider and slot
            # reservation through all drained kernel workers and commit.
            async with _retirement_scope(retire, candidate.account_id, candidate.slot_id) as scope:

                async def admission(session):
                    await _admitted(app, files, session, candidate, provider, scope)

                async def publication(session):
                    link = await _admitted(app, files, session, candidate, provider, scope)
                    await session.delete(link)

                result = await SpaceRecovery(files).host_release(
                    candidate.space_id,
                    admission=admission,
                    publication=publication,
                    lock_admission=prerequisite,
                    retain_unverified=True,
                    before_publication=scope.prove_absent,
                )
            if all(result.get(key) is True for key in ("complete", "slot_released", "newly_reclaimed")):
                reclaimed += 1
                logger.info("storage: user space reclaimed (%s)", candidate.slot_id)
        except Exception:
            logger.exception("storage: released User Space %s remains pending", candidate.space_id)
    return reclaimed


async def finish_releases(app):
    """Release every kind without bypassing the released-account User policy."""
    from app.gateway.storage_spaces import storage_state
    from deerflow.persistence.agent_instances.model import AgentInstanceRow

    files = getattr(app.state, "storage_spaces", None)
    status = getattr(app.state, "agent_execution", None)
    provider = getattr(status, "qualified_provider", None)
    retire = getattr(provider, "_native_retirement_scope", None)
    if files is None or not callable(retire) or (await storage_state(app)).state in ("off", "unavailable"):
        return 0
    reclaimed = await reclaim_released_user_spaces(app)
    runtime = getattr(app.state, "resident_runtime", None)
    if runtime is not None:
        await runtime.inventory_runs()
        if runtime.foreign_run_blocks:
            return reclaimed
    candidates = []
    async with files.registry._sf() as session:
        rows = (
            await session.execute(select(SpaceRow, SpaceBackingRow).join(SpaceBackingRow, SpaceBackingRow.backing_handle == SpaceRow.backing_handle).where(SpaceRow.status == "deleted", SpaceRow.mode == "native").order_by(SpaceRow.id))
        ).all()
        for row, binding in rows:
            try:
                kind = getattr(files._binding_spec(binding), "kind", None)
            except BackingUnavailable:
                continue
            if kind in {"assistant", "employee", "company"}:
                candidates.append((row.id, binding.slot_id, kind, tuple(getattr(row, name) for name in _SPACE), tuple(getattr(binding, name) for name in _BINDING)))
    for space_id, slot, kind, expected_space, expected_binding in candidates:
        try:
            # The opaque host resource key reserves the slot; it supplies no
            # human identity and cannot authorize a foreign native holder.
            async with _retirement_scope(retire, "space:" + space_id, slot) as scope:

                async def admission(session):
                    if status.qualified_provider is not provider or status.storage_reason is not None:
                        raise SpaceConflict("The release lost its qualified provider")
                    scope.assert_current()
                    row = await session.get(SpaceRow, space_id)
                    binding = await session.get(SpaceBackingRow, expected_binding[0])
                    if (
                        row is None
                        or binding is None
                        or tuple(getattr(row, name) for name in _SPACE) != expected_space
                        or tuple(getattr(binding, name) for name in _BINDING) != expected_binding
                        or getattr(files._binding_spec(binding), "kind", None) != kind
                    ):
                        raise SpaceConflict("Deleted Space or backing changed before release")
                    if runtime is not None:
                        if runtime.foreign_run_blocks:
                            raise SpaceConflict("A foreign durable owner remains live")
                        owners = (await session.scalars(select(AgentInstanceRow).where(AgentInstanceRow.home_id == space_id).with_for_update())).all()
                        if len(owners) > 1 or any(owner.status != "deleted" or runtime.registry.in_flight(owner.id) or runtime.registry._has_reservation(space_id) for owner in owners):
                            raise SpaceConflict("The agent's Home is still owned")

                result = await SpaceRecovery(files).host_release(space_id, admission=admission, retain_unverified=True, before_publication=scope.prove_absent)
            if all(result.get(key) is True for key in ("complete", "slot_released", "newly_reclaimed")):
                reclaimed += 1
                logger.info("storage: %s space reclaimed (%s)", kind, slot)
        except Exception:
            logger.exception("storage: deleted %s Space %s remains pending", kind, space_id)
    return reclaimed
