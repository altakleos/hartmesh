"""Host identity and startup wiring for mandatory, chat-independent storage."""

import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Literal

from sqlalchemy import func, select

from app.gateway.auth.mode import account_refusal
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.gateway.auth_disabled import AUTH_DISABLED_USER_ID, AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_INTERNAL, AUTH_SOURCE_SESSION, is_auth_disabled
from app.gateway.internal_auth import get_trusted_internal_owner_user_id
from deerflow.agent_instances.contract import ExecutionUnavailable
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.agent_instances.runtime import qualified_attachments
from deerflow.agent_instances.service import AgentInstances
from deerflow.config.agents_api_config import get_agents_api_config
from deerflow.config.storage_spaces_config import StorageSpacesConfig
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.backings import RESERVE_BYTES, RESERVE_INODES, PreparedVolumeCatalog
from deerflow.spaces.contract import PrincipalRef, ResolvedPrincipal
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.provision import KINDS, StorageFit, plan_counts, storage_fit
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles
from deerflow.utils.file_io import await_drained, run_file_io

logger = logging.getLogger(__name__)
STARTUP_BUDGET_SECONDS = 120
ADAPTER_LIMIT_SECONDS = 30
RECHECK_SECONDS = 60
STORAGE_REASONS = ("disk-too-small", "storage-pool-missing", "inventory-unreadable", "host-unqualified", "admission-failed")
EXECUTION_REASONS = ("storage-unavailable", "agents-none", "operator-hold", "host-replaced", "space-unverified", "space-not-assigned")


@dataclass(frozen=True)
class StorageState:
    state: Literal["off", "ready", "incomplete", "unavailable"]
    reason: str | None = None


@dataclass(frozen=True)
class ExecutionState:
    enabled: bool
    execution: Literal["ready", "unavailable"]
    reason: str | None = None


@dataclass(frozen=True)
class Allowance:
    allowed: int
    allowed_key: int
    prepared: int
    in_use: int
    free: int


@dataclass
class ExecutionStatus:
    storage_reason: str | None = None
    storage_text: str | None = None
    fit: StorageFit | None = None
    host_id: str | None = None
    unverified: dict[str, str] = field(default_factory=dict)
    hold_since: datetime | None = None
    mount_pending: bool = False
    adapter_task: asyncio.Future | None = None
    metadata_task: asyncio.Future | None = None
    pending_reconcile: list[str] = field(default_factory=list)
    attachments_fenced: int = 0
    attempts_settled: int = 0
    file_operations_settled: int = 0


async def storage_state(app) -> StorageState:
    if not getattr(app.state, "storage_spaces_enabled", False):
        return StorageState("off")
    status = app.state.agent_execution
    if status.storage_reason is not None:
        return StorageState("unavailable", status.storage_reason)
    if status.fit is not None and any(status.fit.missing.values()):
        return StorageState("incomplete")
    return StorageState("ready")


async def allowance(app, kind: str) -> Allowance:
    if kind not in KINDS:
        raise ValueError("Unknown storage kind")
    files = app.state.storage_spaces
    plan = app.state.storage_spaces_config.plan
    key = plan_counts(plan)[kind] if plan is not None else 0
    prepared = sum(getattr(spec, "kind", None) == kind for spec in files.catalog.volumes.values())
    if kind == "user":
        query = select(func.count()).select_from(UserRow).where(UserRow.released_at.is_(None))
    elif kind == "company":
        query = select(func.count()).select_from(StorageFeatureLinkRow).where(StorageFeatureLinkRow.namespace == "hm.company-space", StorageFeatureLinkRow.key == "company")
    else:
        query = select(func.count()).select_from(AgentInstanceRow).where(AgentInstanceRow.custody == ("personal" if kind == "assistant" else "company"), AgentInstanceRow.status.in_(("provisioning", "active", "suspended", "archived")))
    async with files.registry._sf() as session:
        in_use = await session.scalar(query)
    allowed = min(prepared, max(key, in_use))
    return Allowance(allowed, key, prepared, in_use, allowed - in_use)


async def execution_state(app, instance_id: str | None = None) -> ExecutionState:
    config = await run_file_io(get_agents_api_config)
    reason = None
    if (await storage_state(app)).state == "unavailable":
        reason = "storage-unavailable"
    elif config.ai_assistants_per_user == config.ai_employees == 0:
        reason = "agents-none"
    elif instance_id is not None and getattr(app.state, "agent_instances", None) is not None:
        async with app.state.agent_instances._sf() as session:
            row = (await session.execute(select(AgentInstanceRow.home_id, SpaceBackingRow.slot_id).outerjoin(SpaceBackingRow, SpaceBackingRow.space_id == AgentInstanceRow.home_id).where(AgentInstanceRow.id == instance_id))).one_or_none()
        if row is not None:
            home, slot = row
            if slot is not None and (slot in app.state.agent_execution.unverified or slot not in app.state.storage_spaces.catalog.volumes):
                reason = "space-unverified"
            elif home is None:
                reason = "space-not-assigned"
    return ExecutionState(config.enabled, "unavailable" if reason is not None else "ready", reason)


async def require_execution(app, instance_id: str | None) -> None:
    state = await execution_state(app, instance_id)
    if state.execution == "unavailable":
        raise ExecutionUnavailable(state.reason)


async def storage_health(app):
    storage = await storage_state(app)
    if storage.state == "off":
        return {"storage": "off", "agents": "off"}
    execution = await execution_state(app)
    agents = {"agents-none": "none", "operator-hold": "held"}.get(execution.reason, "unavailable" if execution.execution == "unavailable" else "ready")
    return {"storage": storage.state, "agents": agents}


def _read_pool(config):
    path = Path(config.inventory_path)
    reason, text = None, None
    try:
        catalog = PreparedVolumeCatalog.from_manifest(path)
    except Exception as exc:
        catalog = PreparedVolumeCatalog([], data_disk=path.parent, reserve_bytes=RESERVE_BYTES, reserve_inodes=RESERVE_INODES)
        reason = "storage-pool-missing" if isinstance(exc, FileNotFoundError) else "inventory-unreadable"
        text = f"no inventory at {path}" if isinstance(exc, FileNotFoundError) else str(exc)
    unverified = {}
    for slot in catalog.volumes:
        try:
            catalog.verify(slot)
        except Exception as exc:
            unverified[slot] = str(exc)
    fit = None
    if config.plan is not None:
        try:
            fit = storage_fit(path.parent, config.plan, catalog)
        except Exception:
            pass
        if reason is None and fit is not None and not fit.fits and not any(fit.present[k] for k, count in fit.required.items() if count):
            reason = "disk-too-small"
            text = (
                f"the keys call for {(fit.missing_bytes + fit.headroom_bytes) / (1 << 30):.1f} GiB free, "
                f"{fit.missing_bytes / (1 << 30):.1f} GiB of spaces and {fit.headroom_bytes / (1 << 30):.1f} GiB kept free, "
                f"and the data disk has {fit.free_bytes / (1 << 30):.1f} GiB free; nothing was prepared"
            )
    return catalog, unverified, fit, reason, text


def _observe(task):
    if not task.cancelled():
        task.exception()


async def _adapter_step(app, *, timeout=ADAPTER_LIMIT_SECONDS):
    status = app.state.agent_execution
    if status.adapter_task is None:
        status.adapter_task = asyncio.ensure_future(qualified_attachments(app.state.storage_spaces))
        status.adapter_task.add_done_callback(_observe)
    task = status.adapter_task
    done, _pending = await asyncio.wait({task}, timeout=timeout)
    if not done:
        status.storage_reason, status.storage_text = "admission-failed", "the storage adapter did not answer in 30 s"
        return False
    status.adapter_task = None
    try:
        _baseline, attachments = task.result()
    except Exception as exc:
        status.storage_reason = "host-unqualified" if isinstance(exc, StorageHostUnqualified) else "admission-failed"
        status.storage_text = str(exc)
        return False
    status.host_id = attachments.provider.host_id
    status.storage_reason, status.storage_text = None, None
    return True


async def _load_metadata(app):
    files = app.state.storage_spaces
    await files.load_bound_slots()
    errors = await files.refresh_bound_volumes()
    await files.load_unclaimed_slots()
    counts = {kind: await allowance(app, kind) for kind in KINDS}
    return errors, counts


async def _metadata_step(app, *, timeout):
    status = app.state.agent_execution
    if status.metadata_task is None:
        status.metadata_task = asyncio.ensure_future(_load_metadata(app))
        status.metadata_task.add_done_callback(_observe)
    task = status.metadata_task
    done, _pending = await asyncio.wait({task}, timeout=timeout)
    if not done:
        if status.storage_reason is None:
            status.storage_reason, status.storage_text = "admission-failed", "storage verification did not finish within the startup budget"
        return None
    status.metadata_task = None
    try:
        errors, counts = task.result()
    except Exception as exc:
        if status.storage_reason in (None, "admission-failed"):
            status.storage_reason, status.storage_text = "inventory-unreadable", f"storage binding facts could not be loaded: {exc}"
        return None
    status.unverified.update(errors)
    return counts


async def _startup_lines(app, *, counts=None):
    status, files = app.state.agent_execution, app.state.storage_spaces
    verified = sum(slot not in status.unverified for slot in files.catalog.volumes)
    state = await storage_state(app)
    if state.state == "unavailable":
        ending = "chats run without files" if state.reason == "disk-too-small" else "existing spaces are kept; chats run without files"
        logger.warning("storage: unavailable (%s: %s; %s)", state.reason, status.storage_text, ending)
        logger.warning("agents: unavailable (storage-unavailable)")
    else:
        if counts is None:
            counts = {kind: await allowance(app, kind) for kind in KINDS}
        n = len(files.catalog.volumes)
        noun = "space" if n == 1 else "spaces"
        details = (
            f"users {counts['user'].in_use} of {counts['user'].allowed} seats, "
            f"AI assistants {counts['assistant'].in_use} of {counts['assistant'].allowed}, "
            f"AI employees {counts['employee'].in_use} of {counts['employee'].allowed}, "
            f"company {counts['company'].in_use} of {counts['company'].allowed}"
        )
        reserved = sum(spec.max_bytes for spec in files.catalog.volumes.values()) / (1 << 30)
        fit = status.fit
        if state.state == "incomplete":
            missing = sum(fit.missing.values())
            prepared = "; ".join(f"{kind} {fit.present[kind]} of {fit.required[kind]} prepared" for kind in KINDS)
            explanation = (
                "the plan fits the disk and was not finished; run docker compose up -d --wait again, or storage-init prepare"
                if fit.fits
                else (
                    f"{fit.missing_bytes / (1 << 30):.1f} GiB more needed and {fit.headroom_bytes / (1 << 30):.1f} GiB must stay free, "
                    f"{fit.free_bytes / (1 << 30):.1f} GiB free; existing spaces work; "
                    "grow the data disk or lower the counts, then docker compose up -d --wait"
                )
            )
            logger.info("storage: ready, %s %s short (%s; %s)", missing, "space" if missing == 1 else "spaces", prepared, explanation)
        else:
            capacity = f"{fit.free_bytes / (1 << 30):.1f} GiB free of {fit.disk_bytes / (1 << 30):.1f}" if fit is not None else "disk figures unavailable"
            logger.info("storage: ready (%s %s prepared, %s verified; %s; %.1f GiB reserved; %s; runtime %s)", n, noun, verified, details, reserved, capacity, os.environ.get("DEER_FLOW_SANDBOX_RUNTIME", "default"))
        config = await run_file_io(get_agents_api_config)
        if config.ai_assistants_per_user == config.ai_employees == 0:
            logger.info("agents: none (HARTMESH_AI_ASSISTANTS_PER_USER=0, HARTMESH_AI_EMPLOYEES=0)")
        else:
            logger.info("agents: ready (%s attachments fenced at start; %s work attempts settled)", status.attachments_fenced, status.attempts_settled)
        keys = {"user": "HARTMESH_USERS", "assistant": "HARTMESH_USERS x HARTMESH_AI_ASSISTANTS_PER_USER", "employee": "HARTMESH_AI_EMPLOYEES", "company": "company allowance"}
        for kind, count in counts.items():
            if count.allowed_key < count.in_use and kind in ("assistant", "employee"):
                logger.warning(
                    "storage: %s=%s is below the %s AI %s in use; %s stays in force until the key is raised or %s are deleted",
                    keys[kind],
                    count.allowed_key,
                    count.in_use,
                    "assistants" if kind == "assistant" else "employees",
                    count.allowed,
                    count.in_use - count.allowed_key,
                )
            if count.prepared > count.allowed_key:
                logger.info("storage: %s %s spaces beyond %s=%s are kept; remove one with forget <slot> when it is free", count.prepared - count.allowed_key, kind, keys[kind], count.allowed_key)
        if counts["user"].in_use > counts["user"].prepared:
            logger.warning("storage: accounts %s, seats %s; %s accounts have no files until HARTMESH_USERS is raised", counts["user"].in_use, counts["user"].prepared, counts["user"].in_use - counts["user"].prepared)
    for slot, text in status.unverified.items():
        spec = files.catalog.volumes.get(slot)
        logger.warning("storage: space %s (%s) unverified (%s); %s of %s spaces verified", slot, getattr(spec, "kind", None), text, verified, len(files.catalog.volumes))


async def storage_recheck(app):
    status = app.state.agent_execution
    if status.storage_reason != "admission-failed":
        return
    counts = None
    if status.metadata_task is not None:
        if not status.metadata_task.done():
            return
        counts = await _metadata_step(app, timeout=0)
        if counts is None:
            return
    if status.adapter_task is not None and not status.adapter_task.done():
        return
    if await _adapter_step(app, timeout=ADAPTER_LIMIT_SECONDS):
        await _startup_lines(app, counts=counts)


async def _recheck_loop(app):
    stop = app.state.storage_recheck_stop
    if app.state.agent_execution.storage_reason == "admission-failed":
        try:
            await storage_recheck(app)
        except Exception:
            logger.exception("storage: availability recheck failed; existing facts are kept")
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=RECHECK_SECONDS)
        except TimeoutError:
            try:
                await storage_recheck(app)
            except Exception:
                logger.exception("storage: availability recheck failed; existing facts are kept")


async def close_storage_spaces(app):
    async def drain():
        stop = getattr(app.state, "storage_recheck_stop", None)
        if stop is not None:
            stop.set()
        task = getattr(app.state, "storage_recheck_task", None)
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        status = getattr(app.state, "agent_execution", None)
        owned = [getattr(app.state, "storage_inventory_task", None)]
        if status is not None:
            owned.extend((status.adapter_task, status.metadata_task))
        owned = [task for task in owned if task is not None]
        if owned:
            await asyncio.gather(*owned, return_exceptions=True)

    await await_drained(drain())


@asynccontextmanager
async def storage_spaces_lifecycle(app):
    """Own startup tasks through later initialization failures and runtime teardown."""
    try:
        yield
    finally:
        await close_storage_spaces(app)


def supports_storage_credentials(request) -> bool:
    """Legacy route scope/default identity is never resource authentication."""
    source = getattr(getattr(request, "state", None), "auth_source", None)
    if source == AUTH_SOURCE_INTERNAL:
        return get_trusted_internal_owner_user_id(request) is not None
    return source in (AUTH_SOURCE_SESSION, AUTH_SOURCE_AUTH_DISABLED)


def human_lookup(repository):
    async def lookup(reference: PrincipalRef) -> ResolvedPrincipal | None:
        if reference.kind != "human":
            return None
        # The one synthetic identity is supported only by the explicit local
        # development adapter, never by missing request context or a missing row.
        if reference.subject_id == AUTH_DISABLED_USER_ID and is_auth_disabled():
            return ResolvedPrincipal(reference, can_provision_company=True)
        user = await repository.get_user_by_id(reference.subject_id)
        if user is None or str(user.id) != reference.subject_id or account_refusal(user) is not None:
            return None
        return ResolvedPrincipal(reference, can_provision_company=user.system_role == "admin" and getattr(user, "role_limit", None) != "user")

    return lookup


async def initialize_storage_spaces(app, config: StorageSpacesConfig, *, session_factory) -> None:
    entered = time.monotonic()
    app.state.agent_execution = ExecutionStatus()
    app.state.storage_inventory_task = None
    app.state.storage_spaces_config = config
    app.state.storage_spaces_enabled = config.enabled
    app.state.storage_spaces = None
    app.state.agent_instances = None
    app.state.agent_conversations = AgentConversations(None, session_factory=session_factory) if session_factory is not None else None
    thread_store = getattr(app.state, "thread_store", None)
    if thread_store is not None and hasattr(thread_store, "instance_authority"):
        thread_store.instance_authority = app.state.agent_conversations
    if not config.enabled:
        logger.info("storage: off (HARTMESH_USERS absent)")
        return
    if session_factory is None:
        raise RuntimeError("Storage spaces require a durable database; memory mode is unsupported")
    read = asyncio.ensure_future(run_file_io(_read_pool, config))
    app.state.storage_inventory_task = read
    read.add_done_callback(_observe)
    done, _pending = await asyncio.wait({read}, timeout=max(0, entered + STARTUP_BUDGET_SECONDS - time.monotonic()))
    status = app.state.agent_execution
    if done:
        catalog, status.unverified, status.fit, status.storage_reason, status.storage_text = read.result()
    else:
        catalog = PreparedVolumeCatalog([], data_disk=Path(config.inventory_path).parent, reserve_bytes=RESERVE_BYTES, reserve_inodes=RESERVE_INODES)
        status.storage_reason, status.storage_text = "inventory-unreadable", "inventory validation did not finish within the startup budget"
        app.state.storage_inventory_task = read
    human = human_lookup(SQLiteUserRepository(session_factory))
    directory = InstanceDirectory(session_factory, human=human)
    registry = SpaceRegistry(session_factory, HostPrincipalResolver(human=human, nonhuman=directory.lookup))
    app.state.storage_spaces = SpaceFiles(registry, catalog, inventory_path=Path(config.inventory_path), user_spaces=config.user_spaces)
    app.state.agent_instances = AgentInstances(app.state.storage_spaces, directory)
    app.state.agent_conversations = AgentConversations(app.state.agent_instances)
    if thread_store is not None and hasattr(thread_store, "instance_authority"):
        thread_store.instance_authority = app.state.agent_conversations
    counts = await _metadata_step(app, timeout=max(0, entered + STARTUP_BUDGET_SECONDS - time.monotonic()))
    app.state.agent_instances.execution_probe = lambda instance_id=None: execution_state(app, instance_id)
    app.state.agent_instances.allowance_of = lambda kind: allowance(app, kind)
    if status.storage_reason is None and status.metadata_task is None:
        await _adapter_step(app, timeout=min(ADAPTER_LIMIT_SECONDS, max(0, entered + STARTUP_BUDGET_SECONDS - time.monotonic())))
    await _startup_lines(app, counts=counts)
    app.state.storage_recheck_stop = asyncio.Event()
    app.state.storage_recheck_task = asyncio.create_task(_recheck_loop(app))
