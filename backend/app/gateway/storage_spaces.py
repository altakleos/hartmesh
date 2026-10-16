"""Host identity and startup wiring for mandatory, chat-independent storage."""

import asyncio
import json
import logging
import os
import subprocess
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from sqlalchemy import func, select
from sqlalchemy import text as sql_text

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
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.user.model import UserRow
from deerflow.sandbox.lease import run_sync_lifecycle_operation
from deerflow.spaces.backings import RESERVE_BYTES, RESERVE_INODES, BackingUnavailable, PreparedVolumeCatalog
from deerflow.spaces.contract import Custody, InvalidPrincipal, MutationMode, Permission, PrincipalRef, ResolvedPrincipal, SpaceConflict
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.principals import HostPrincipalResolver
from deerflow.spaces.provision import KINDS, StorageFit, plan_counts, storage_fit
from deerflow.spaces.readonly_roots import ReadOnlyRoot
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable
from deerflow.spaces.service import SpaceFiles
from deerflow.utils.file_io import await_drained, run_file_io

logger = logging.getLogger(__name__)
STARTUP_BUDGET_SECONDS = 120
ADAPTER_LIMIT_SECONDS = 30
RECHECK_SECONDS = 60
UNFINISHED_CREATION_SECONDS = 86400
AGENTS_HOLD_PATH = Path(os.environ.get("HARTMESH_OPERATOR_DIR", "/srv/hartmesh/operator")) / "agents-hold"
STORAGE_REASONS = ("disk-too-small", "storage-pool-missing", "inventory-unreadable", "host-unqualified", "admission-failed")
EXECUTION_REASONS = ("storage-unavailable", "agents-none", "operator-hold", "host-replaced", "space-unverified", "space-not-assigned")


def _bootstrap_error_locations(error: BaseException) -> str:
    """Bounded operator diagnostics without exception values or source text."""

    def token(value):
        return "".join(char if char.isascii() and (char.isalnum() or char in "._-") else "_" for char in value[:80])

    chain, seen = [], set()
    for _ in range(3):
        if error is None or id(error) in seen:
            break
        seen.add(id(error))
        frames, traceback = [], error.__traceback__
        for _ in range(64):
            if traceback is None:
                break
            code = traceback.tb_frame.f_code
            frames.append(f"{token(os.path.basename(code.co_filename))}:{token(code.co_name)}:{traceback.tb_lineno}")
            traceback = traceback.tb_next
        chain.append(f"{token(type(error).__name__)}@{'/'.join(frames[-3:]) or 'unknown'}")
        error = error.__cause__ or (None if error.__suppress_context__ else error.__context__)
    return " <- ".join(chain)


def hold_present():
    """One stat per admission; an unreadable operator path cannot lift a hold."""
    try:
        AGENTS_HOLD_PATH.stat()
    except FileNotFoundError:
        return False
    except OSError:
        logger.warning("agents: operator hold status is unreadable; admission remains held")
    return True


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
    runtime_retry: bool = False
    fit: StorageFit | None = None
    host_id: str | None = None
    unverified: dict[str, str] = field(default_factory=dict)
    hold_since: datetime | None = None
    mount_pending: bool = False
    mount_task: asyncio.Future | None = None
    mount_cleanup: str | None = None
    mount_cleanup_task: asyncio.Future | None = None
    mount_verify_task: asyncio.Future | None = None
    mount_reverify: bool = False
    mount_retry_at: float = 0
    adapter_task: asyncio.Future | None = None
    metadata_task: asyncio.Future | None = None
    marker_backfill_task: asyncio.Future | None = None
    marker_backfill_started: bool = False
    company_task: asyncio.Future | None = None
    maintenance_task: asyncio.Future | None = None
    company_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pending_reconcile: list[str] = field(default_factory=list)
    attachments_fenced: int = 0
    attempts_settled: int = 0
    agents_ready_reported: bool = False
    file_operations_settled: int = 0
    qualified_provider: object | None = field(default=None, repr=False)
    user_spaces_reclaimed: int = 0
    spaces_reclaimed: int = 0


async def storage_state(app) -> StorageState:
    if not getattr(app.state, "storage_spaces_enabled", False):
        return StorageState("off")
    status = app.state.agent_execution
    if status.storage_reason is not None:
        return StorageState("unavailable", status.storage_reason)
    if status.fit is not None and any(status.fit.missing.values()):
        return StorageState("incomplete")
    return StorageState("ready")


async def allowance(app, kind: str, *, session=None) -> Allowance:
    files = app.state.storage_spaces
    return await _allowance(app.state.storage_spaces_config, files.catalog, files.registry._sf, kind, session=session)


async def _allowance(config, catalog, session_factory, kind: str, *, session=None) -> Allowance:
    """One calculation for live metadata and offline account creation."""
    if kind not in KINDS:
        raise ValueError("Unknown storage kind")
    plan = config.plan
    key = plan_counts(plan)[kind] if plan is not None else 0
    prepared = sum(getattr(spec, "kind", None) == kind for spec in catalog.volumes.values())
    if kind == "company":
        query = select(func.count()).select_from(StorageFeatureLinkRow).where(StorageFeatureLinkRow.namespace == "hm.company-space", StorageFeatureLinkRow.key == "company")
    elif kind != "user":
        query = select(func.count()).select_from(AgentInstanceRow).where(AgentInstanceRow.custody == ("personal" if kind == "assistant" else "company"), AgentInstanceRow.status.in_(("provisioning", "active", "suspended", "archived")))
    if kind == "user":
        in_use = await SQLiteUserRepository(session_factory).count_seated_users(session=session)
    elif session is not None:
        if not session.in_transaction() or not session.is_active:
            raise ValueError("Allowance requires an active borrowed transaction")
        in_use = await session.scalar(query)
    else:
        async with session_factory() as reader:
            in_use = await reader.scalar(query)
    allowed = min(prepared, max(key, in_use))
    return Allowance(allowed, key, prepared, in_use, allowed - in_use)


async def install_operator_seats(repository, config: StorageSpacesConfig) -> None:
    """Offline commands inspect inventory only; they never start or qualify Docker."""
    if not config.enabled or config.plan is None:
        return

    def read():
        try:
            return PreparedVolumeCatalog.from_manifest(Path(config.inventory_path))
        except Exception:
            return PreparedVolumeCatalog([], data_disk=Path(config.inventory_path).parent, reserve_bytes=RESERVE_BYTES, reserve_inodes=RESERVE_INODES)

    catalog = await run_file_io(read)

    async def seats_allowed(session):
        return (await _allowance(config, catalog, repository._sf, "user", session=session)).allowed

    repository.seats_allowed = seats_allowed


def _execution_refusal(app, source):
    runtime = getattr(app.state, "resident_runtime", None)
    coordinator = getattr(runtime, "coordinator", None)
    logger.warning(
        "agents: execution unavailable (%s; initializer=%s; result=%s; workspace_qualified=%s; accounting_retry=%s; revision=%s; pending=%s; foreign=%s)",
        source,
        getattr(coordinator, "task", None) is not None,
        getattr(coordinator, "result", None) is not None,
        bool(getattr(runtime, "workspace_qualified", False)),
        bool(getattr(runtime, "accounting_retry", False)),
        getattr(runtime, "_qualification_revision", 0),
        len(getattr(getattr(app.state, "agent_execution", None), "pending_reconcile", ())),
        len(getattr(runtime, "foreign_run_blocks", ())),
    )


async def execution_state(app, instance_id: str | None = None) -> ExecutionState:
    config = await run_file_io(get_agents_api_config)
    reason = None
    if (await storage_state(app)).state == "unavailable":
        reason = "storage-unavailable"
        _execution_refusal(app, "storage-state")
    elif config.ai_assistants_per_user == config.ai_employees == 0:
        reason = "agents-none"
    elif await run_file_io(hold_present):
        reason = "operator-hold"
    elif getattr(getattr(app.state, "chat_storage_host", None), "admission", None) is not None and (
        getattr(app.state, "resident_runtime", None) is None or app.state.resident_runtime.coordinator.result is None or not app.state.resident_runtime.workspace_qualified
    ):
        reason = "storage-unavailable"
        runtime = getattr(app.state, "resident_runtime", None)
        _execution_refusal(app, "runtime-missing" if runtime is None else "bootstrap-pending" if runtime.coordinator.result is None else "workspace-unqualified")
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
    probe = getattr(getattr(app.state, "agent_instances", None), "execution_probe", None)
    state = await probe(instance_id) if probe is not None else await execution_state(app, instance_id)
    if state.execution == "unavailable":
        raise ExecutionUnavailable(state.reason)


async def storage_health(app):
    storage = await storage_state(app)
    if storage.state == "off":
        return {"storage": "off", "agents": "off"}
    execution = await execution_state(app)
    agents = {"agents-none": "none", "operator-hold": "held"}.get(execution.reason, "unavailable" if execution.execution == "unavailable" else "ready")
    return {"storage": storage.state, "agents": agents}


STORAGE_KEYS = ("HARTMESH_USERS", "HARTMESH_USER_STORAGE_GIB", "HARTMESH_AI_ASSISTANTS_PER_USER", "HARTMESH_AI_ASSISTANT_STORAGE_GIB", "HARTMESH_AI_EMPLOYEES", "HARTMESH_AI_EMPLOYEE_STORAGE_GIB", "HARTMESH_COMPANY_STORAGE_GIB")
STORAGE_INIT_VERBS = {"mount-all": 60}
STORAGE_INIT_CLEANUP_SECONDS = 5


def _remove_storage_init(name: str) -> bool:
    try:
        result = subprocess.run(["docker", "rm", "--force", name], capture_output=True, text=True, timeout=STORAGE_INIT_CLEANUP_SECONDS, check=False)
        if result.returncode == 0:
            return True
        # The timed-out CLI may have left an in-flight daemon create request.
        # A point-in-time empty listing cannot prove it will never appear.
        # Keep the owned name uncertain until a removal is acknowledged.
        return False
    except (OSError, subprocess.TimeoutExpired):
        return False


def _storage_init_call(command: list[str], *, name: str, verb: str, limit: int) -> dict:
    entered = time.monotonic()
    document = {"command": verb, "returncode": 2, "note": "storage-init printed no result"}
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=limit, check=False)
        try:
            candidate = json.loads(result.stdout)
        except (ValueError, TypeError):
            candidate = None
        if isinstance(candidate, dict) and type(candidate.get("returncode")) is int and candidate["returncode"] in (0, 1, 2) and isinstance(candidate.get("note"), str):
            document = candidate
    except subprocess.TimeoutExpired:
        document = {"command": verb, "returncode": 2, "timed_out": True, "note": f"storage-init {verb} did not finish in {limit} s"}
        if not _remove_storage_init(name):
            document["cleanup_pending"] = name
    except OSError as exc:
        document = {"command": verb, "returncode": 2, "note": f"storage-init {verb} could not be started", "error": type(exc).__name__}
    note = document["note"]
    if document.get("error"):
        note += f": {document['error']}"
    logger.info("storage: storage-init %s exit %s in %.1f s (%s)", verb, document["returncode"], time.monotonic() - entered, note)
    return document


async def run_storage_init(verb: str) -> dict:
    if verb not in STORAGE_INIT_VERBS:
        raise ValueError("The Gateway may run storage-init mount-all only")
    image = os.environ.get("HARTMESH_STORAGE_INIT_IMAGE", "").strip()
    if not image:
        return {"command": verb, "returncode": 0, "skipped": True, "note": "storage-init image unset"}
    name = f"hartmesh-storage-init-{verb}-{uuid.uuid4().hex[:8]}"
    data_dir = os.environ.get("HARTMESH_DATA_DIR", "/srv/hartmesh")
    command = [
        "docker",
        "run",
        "--rm",
        "--name",
        name,
        "--label",
        f"hartmesh.storage.init={verb}",
        "--privileged",
        "--pid",
        "host",
        "--network",
        "none",
        "--memory",
        "128m",
        "--memory-swap",
        "128m",
        "--pids-limit",
        "64",
        "--workdir",
        "/app/backend",
        "--env",
        "PYTHONPATH=.",
        "--env",
        "UV_CACHE_DIR=/tmp/uv-cache",
        "--env",
        f"HARTMESH_DATA_DIR={data_dir}",
    ]
    for key in STORAGE_KEYS:
        command.extend(("--env", f"{key}={os.environ.get(key, '')}"))
    command.extend(("--mount", f"type=bind,src={data_dir}/spaces,dst={data_dir}/spaces,bind-propagation=rslave", "--entrypoint", "uv", image, "run", "--no-sync", "python", "-m", "app.gateway.storage_init", verb))
    return await await_drained(run_file_io(_storage_init_call, command, name=name, verb=verb, limit=STORAGE_INIT_VERBS[verb]))


async def _mount_step(app, *, timeout: float) -> bool:
    status = app.state.agent_execution
    deadline = time.monotonic() + timeout
    if status.mount_cleanup is not None:
        if status.mount_cleanup_task is None:
            status.mount_cleanup_task = asyncio.ensure_future(run_file_io(_remove_storage_init, status.mount_cleanup))
            status.mount_cleanup_task.add_done_callback(_observe)
        task = status.mount_cleanup_task
        done, _ = await asyncio.wait({task}, timeout=max(0, deadline - time.monotonic()))
        if not done:
            return False
        status.mount_cleanup_task = None
        if not task.result():
            return False
        status.mount_cleanup = None
    if status.mount_task is None:
        if time.monotonic() < status.mount_retry_at:
            return False
        status.mount_task = asyncio.ensure_future(run_storage_init("mount-all"))
        status.mount_task.add_done_callback(_observe)
    task = status.mount_task
    done, _ = await asyncio.wait({task}, timeout=max(0, deadline - time.monotonic()))
    if not done:
        status.mount_pending = True
        status.storage_reason, status.storage_text = "admission-failed", "storage-init mount-all did not finish in 60 s"
        return False
    status.mount_task = None
    try:
        result = task.result()
    except Exception:
        result = {"returncode": 2, "timed_out": True, "note": "storage-init mount-all did not complete"}
    if result.get("timed_out") or result.get("cleanup_pending"):
        status.mount_pending = True
        status.mount_cleanup = result.get("cleanup_pending")
        status.mount_retry_at = time.monotonic() + RECHECK_SECONDS
        status.storage_reason, status.storage_text = "admission-failed", result["note"]
        return False
    if status.mount_pending and result["returncode"] != 0:
        status.mount_retry_at = time.monotonic() + RECHECK_SECONDS
        return False
    status.mount_pending = False
    return True


def _verify_catalog(catalog):
    errors = {}
    for slot in catalog.volumes:
        try:
            catalog.verify(slot)
        except Exception as exc:
            errors[slot] = str(exc)
    return errors


async def _mount_verification_step(app, *, timeout: float) -> bool:
    status = app.state.agent_execution
    if status.mount_verify_task is None:
        status.mount_verify_task = asyncio.ensure_future(run_file_io(_verify_catalog, app.state.storage_spaces.catalog))
        status.mount_verify_task.add_done_callback(_observe)
    task = status.mount_verify_task
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if not done:
        return False
    status.unverified = task.result()
    status.mount_verify_task = None
    return True


def _read_pool(config):
    path = Path(config.inventory_path)
    reason, text = None, None
    try:
        catalog = PreparedVolumeCatalog.from_manifest(path)
    except Exception as exc:
        catalog = PreparedVolumeCatalog([], data_disk=path.parent, reserve_bytes=RESERVE_BYTES, reserve_inodes=RESERVE_INODES)
        reason = "storage-pool-missing" if isinstance(exc, FileNotFoundError) else "inventory-unreadable"
        text = f"no inventory at {path}" if isinstance(exc, FileNotFoundError) else str(exc)
    unverified = _verify_catalog(catalog)
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
    runtime = getattr(app.state, "resident_runtime", None)
    if runtime is not None and status.adapter_task is None:
        runtime.invalidate_accounting()
        if runtime.coordinator.task is not None:
            await runtime.coordinator.poll(timeout=0)
            if runtime.coordinator.task is not None:
                return False
        runtime.coordinator.invalidate()
        runtime.accounting_retry = False
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
        baseline, attachments = task.result()
    except Exception as exc:
        status.runtime_retry = isinstance(exc, StorageRuntimeUnavailable)
        status.storage_reason = "host-unqualified" if isinstance(exc, StorageHostUnqualified) else "admission-failed"
        status.storage_text = str(exc)
        return False
    status.host_id = attachments.provider.host_id
    attachments.provider._readonly_roots = lambda: resident_readonly_roots(app)
    status.qualified_provider = baseline
    status.runtime_retry = False
    status.storage_reason, status.storage_text = None, None
    from app.gateway.resident_runtime import install_resident_runtime

    try:
        install_resident_runtime(app, provider=baseline, attachments=attachments, entered=getattr(app.state, "storage_startup_entered", time.monotonic()))
    except Exception as exc:
        host = getattr(app.state, "chat_storage_host", None)
        admission = getattr(host, "admission", None)
        if admission is not None:
            baseline.external_changed(admission.close)
        status.runtime_retry = True
        status.storage_reason, status.storage_text = "admission-failed", str(exc)
        return False
    return True


async def _load_metadata(app):
    files = app.state.storage_spaces
    await files.load_bound_slots()
    errors = {}
    status = app.state.agent_execution
    if status.mount_pending or status.mount_reverify:
        raise BackingUnavailable("Initial mount verification is still pending")
    if not status.marker_backfill_started:
        status.marker_backfill_started = True
        status.marker_backfill_task = asyncio.create_task(files.backfill_binding_markers())
        status.marker_backfill_task.add_done_callback(_observe)
    backfill = getattr(status, "marker_backfill_task", None)
    if backfill is not None:
        errors.update(await asyncio.shield(backfill))
        status.marker_backfill_task = None
    errors.update(await files.refresh_bound_volumes())
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


def _agents_ready_line(app):
    status = app.state.agent_execution
    if status.agents_ready_reported:
        return
    logger.info(
        "agents: ready (%s %s fenced at start; %s work %s settled)",
        status.attachments_fenced,
        "attachment" if status.attachments_fenced == 1 else "attachments",
        status.attempts_settled,
        "attempt" if status.attempts_settled == 1 else "attempts",
    )
    status.agents_ready_reported = True


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
        logger.info("storage: file operations settled %s", status.file_operations_settled)
        config = await run_file_io(get_agents_api_config)
        if config.ai_assistants_per_user == config.ai_employees == 0:
            logger.info("agents: none (HARTMESH_AI_ASSISTANTS_PER_USER=0, HARTMESH_AI_EMPLOYEES=0)")
        else:
            execution = await execution_state(app)
            if execution.execution == "unavailable":
                logger.warning("agents: unavailable (%s)", execution.reason)
            else:
                _agents_ready_line(app)
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


async def _company_role(session):
    link = await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))
    if link is None:
        return None
    row = await session.get(SpaceRow, link.space_id)
    if row is None or (row.status, row.custody_kind, row.mode, row.feature_namespace, row.feature_controller, row.feature_metadata_version) != ("active", "company", "native", None, None, None):
        raise SpaceConflict("The assigned company Space is unavailable; its identity is retained")
    return row


async def resident_readonly_roots(app):
    """Project the current Company binding without borrowing actor authority."""
    return await storage_readonly_roots(app.state.storage_spaces)


async def storage_readonly_roots(files):
    """Shared observation for resident admission and isolated operator commands."""
    from deerflow.spaces.incarnations import verify_bound

    async def snapshot(session):
        row = await _company_role(session)
        binding = await session.get(SpaceBackingRow, row.backing_handle) if row is not None else None
        if row is None or binding is None or binding.space_id != row.id:
            raise SpaceConflict("The current Company read-only binding is unavailable")
        facts = (row.id, row.backing_handle, binding.slot_id, binding.filesystem_uuid, binding.root_inode, binding.control_inode)
        return facts, binding

    async with files.registry._sf() as session:
        facts, binding = await snapshot(session)
    files._binding_spec(binding)
    volume = await await_drained(run_file_io(files.catalog.verify, binding.slot_id, previous=files._verified.get(binding.slot_id)))
    files._check_incarnation(binding, volume)
    if getattr(volume.spec, "kind", None) not in (None, "company"):
        raise SpaceConflict("The Company read-only binding is not a company slot")
    bound = await await_drained(run_file_io(verify_bound, volume, binding.backing_handle))
    root = await await_drained(run_file_io(ReadOnlyRoot.capture, bound, "shared", "/mnt/user-data/shared"))
    async with files.registry._sf() as session:
        current, _binding = await snapshot(session)
        if current != facts:
            raise SpaceConflict("The Company read-only binding changed during qualification")
    return (root,)


class _AssignedRole(Exception):
    def __init__(self, space_id):
        self.space_id = space_id


async def serialize_role_assignment(session):
    """Application role creation and account release share the caller's transaction."""
    if not session.in_transaction() or not session.is_active:
        raise ValueError("Role assignment requires an active transaction")
    if session.get_bind().dialect.name == "postgresql":
        await session.execute(sql_text("SELECT pg_advisory_xact_lock(hashtext('hartmesh-space-assignment'))"))


async def _company_assignment_admission(session):
    await serialize_role_assignment(session)
    existing = await _company_role(session)
    if existing is not None:
        raise _AssignedRole(existing.id)


async def ensure_company_space(app) -> None:
    """Create the native company role only after this pool has qualified."""
    status = app.state.agent_execution
    files = app.state.storage_spaces
    if files is None or status.host_id is None or (await storage_state(app)).state in ("off", "unavailable"):
        return

    async def perform():
        async with status.company_lock:
            async with files.registry._sf() as session:
                row = await _company_role(session)
                identifier = row.id if row is not None else None
            created = identifier is None
            if created:
                try:
                    identifier = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company", admission=_company_assignment_admission)
                except _AssignedRole as existing:
                    identifier, created = existing.space_id, False
            async with files.registry._sf() as session:
                row = await _company_role(session)
                if row is None or row.id != identifier:
                    raise SpaceConflict("The company role changed during assignment")
                volume = await files._volume(session, row)
                if getattr(volume.spec, "kind", None) not in (None, "company"):
                    raise SpaceConflict("The company role is not bound to a company slot")
                backing = await session.get(SpaceBackingRow, row.backing_handle)
                slot = backing.slot_id
            if created:
                logger.info("storage: company space created (%s)", slot)

    await await_drained(perform())


async def _user_assignment_admission(session, account_id, repository):
    await serialize_role_assignment(session)
    link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + account_id))
    space = await session.get(SpaceRow, link.space_id, with_for_update=True) if link is not None else None
    account = await session.get(UserRow, account_id, with_for_update=True)
    if account is None:
        raise InvalidPrincipal("User storage requires an existing account")
    user = await repository._load(session, account)
    if await run_file_io(account_refusal, user) is not None:
        raise InvalidPrincipal("This account cannot receive user storage")
    if link is not None:
        if space is None or (space.status, space.mode, space.custody_kind, space.custodian_kind, space.custodian_id, space.feature_namespace, space.feature_controller, space.feature_metadata_version) != (
            "active",
            "native",
            "personal",
            "human",
            account_id,
            None,
            None,
            None,
        ):
            raise SpaceConflict("The assigned user Space is unavailable; its identity is retained")
        raise _AssignedRole(space.id)


async def _assign_user_space(app, account) -> tuple[int, int]:
    """Post-commit best effort: personal allocation and company grant repair independently."""
    files = getattr(app.state, "storage_spaces", None)
    config = getattr(app.state, "storage_spaces_config", None)
    if files is None or config is None or config.plan is None or (await storage_state(app)).state in ("off", "unavailable"):
        return 0, 0
    account_id = str(account.id)
    actor = PrincipalRef("human", account_id)

    async def admitted(session):
        await _user_assignment_admission(session, account_id, SQLiteUserRepository(files.registry._sf))

    async def perform():
        assigned = granted = 0
        try:
            identifier = await files.assign(kind="user", actor=actor, name="Files", custody=Custody.personal(actor), mode=MutationMode.NATIVE, admission=admitted)
            assigned = 1
            async with files.registry._sf() as session:
                row = await session.get(SpaceRow, identifier)
                backing = await session.get(SpaceBackingRow, row.backing_handle)
                logger.info("storage: user space of %s assigned (%s)", account_id, backing.slot_id)
        except _AssignedRole:
            pass
        except Exception as exc:
            logger.warning("storage: user assignment remains pending for %s (%s)", account_id, exc)
        try:
            granted = int(await _grant_company_space(app, account_id))
        except Exception as exc:
            logger.warning("storage: company grant remains pending for %s (%s)", account_id, exc)

        return assigned, granted

    return await await_drained(perform())


async def assign_user_space(app, account) -> None:
    await _assign_user_space(app, account)


async def assign_spaces(app) -> tuple[int, int]:
    """Repair oldest current seated accounts; count only committed changes."""
    files = getattr(app.state, "storage_spaces", None)
    config = getattr(app.state, "storage_spaces_config", None)
    if files is None or config is None or config.plan is None or (await storage_state(app)).state in ("off", "unavailable"):
        return 0, 0
    async with files.registry._sf() as session:
        if await _company_role(session) is None:
            return 0, 0
        identifiers = (await session.scalars(select(UserRow.id).where(UserRow.released_at.is_(None)).order_by(UserRow.created_at, UserRow.id))).all()
    repository = SQLiteUserRepository(files.registry._sf)
    assigned = granted = 0
    for identifier in identifiers:
        try:
            account = await repository.get_user_by_id(identifier)
            if account is None or await run_file_io(account_refusal, account) is not None:
                continue
            changes = await _assign_user_space(app, account)
            assigned += changes[0]
            granted += changes[1]
        except Exception as exc:
            logger.warning("storage: account storage remains pending for %s (%s)", identifier, exc)
    return assigned, granted


async def assign_created_account(app, account) -> None:
    """Creation has committed; storage faults cannot turn it into a failed request."""
    try:
        await await_drained(assign_user_space(app, account))
    except Exception as exc:
        logger.warning("storage: account storage remains pending for %s (%s)", account.id, exc)


async def _grant_company_space(app, account_id: str) -> bool:
    """Grant one currently resolving human READ/EXPORT, never mutation rights."""
    files = app.state.storage_spaces
    if files is None or (await storage_state(app)).state in ("off", "unavailable"):
        return False
    subject = PrincipalRef("human", account_id)
    async with files.registry._sf() as session:
        account = await session.get(UserRow, account_id)
        if account is not None and account.released_at is not None:
            raise InvalidPrincipal("Released accounts cannot receive company storage")
        if account is None and not (account_id == AUTH_DISABLED_USER_ID and is_auth_disabled()):
            raise InvalidPrincipal("Company storage requires an existing account")
        row = await _company_role(session)
        if row is None:
            raise SpaceConflict("The company Space is not assigned")
        identifier = row.id
    changed = await files.registry.host_grant(identifier, subject, Permission.READ | Permission.EXPORT)
    if changed:
        logger.info("storage: company grant written for %s", account_id)

    return changed


async def grant_company_space(app, account_id: str) -> None:
    await _grant_company_space(app, account_id)


async def reclaim_released_user_spaces(app) -> int:
    from app.gateway.storage_reclamation import reclaim_released_user_spaces as reclaim

    return await reclaim(app)


async def finish_releases(app) -> int:
    from app.gateway.storage_reclamation import finish_releases as finish

    return await finish(app)


async def reconcile_residents(app, *, attachment_ids=None, deadline):
    runtime = getattr(app.state, "resident_runtime", None)
    if runtime is None:
        raise StorageRuntimeUnavailable("Resident reconciliation has no owning runtime")
    return await await_drained(runtime.reconcile_rows(attachment_ids=attachment_ids, deadline=deadline))


def _wake_after_resident_success(app, runtime):
    coordinator = runtime.coordinator
    task = coordinator.task
    wake = getattr(app.state, "storage_recheck_wake", None)
    if task is None:
        runtime._bootstrap_wake_task = None
        return
    if wake is None or getattr(runtime, "_bootstrap_wake_task", None) is task:
        return
    runtime._bootstrap_wake_task = task

    def wake_if_current(_completed):
        stop = getattr(app.state, "storage_recheck_stop", None)
        if (
            task.cancelled()
            or task.exception() is not None
            or getattr(app.state, "resident_runtime", None) is not runtime
            or runtime.coordinator is not coordinator
            or coordinator.task is not task
            or coordinator._closing
            or (stop is not None and stop.is_set())
        ):
            return
        wake.set()

    def completed(owned):
        if owned.cancelled() or owned.exception() is not None:
            return
        maintenance = app.state.agent_execution.maintenance_task
        if maintenance is not None and not maintenance.done():
            maintenance.add_done_callback(wake_if_current)
        else:
            wake_if_current(owned)

    task.add_done_callback(completed)


async def _resident_step(app, *, timeout):
    runtime = getattr(app.state, "resident_runtime", None)
    if runtime is None:
        return False
    if runtime.accounting_retry and runtime.coordinator.task is None:
        runtime.coordinator.invalidate()
        runtime.accounting_retry = False
    result = await runtime.coordinator.poll(timeout=timeout)
    _wake_after_resident_success(app, runtime)
    failed = result is None and runtime.coordinator.task is None and runtime.coordinator.last_error is not None
    if failed:
        logger.warning(
            "agents: bootstrap unavailable (%s; initializer=%s; result=%s; workspace_qualified=%s; pending=%s)",
            _bootstrap_error_locations(runtime.coordinator.last_error),
            runtime.coordinator.task is not None,
            runtime.coordinator.result is not None,
            runtime.workspace_qualified,
            len(app.state.agent_execution.pending_reconcile),
        )
    if failed or runtime.accounting_retry:
        if runtime.coordinator.task is None:
            runtime.coordinator.invalidate()
            runtime.accounting_retry = False
            await runtime.coordinator.poll(timeout=0)
            _wake_after_resident_success(app, runtime)
        return False
    if result is None:
        return False
    # Producer drainage stopped this existing producer; resume it only after
    # complete accounting and both callbacks have published with admission.
    await await_drained(run_sync_lifecycle_operation(runtime.provider._start_prewarm_reaper))
    return True


async def abandon_unfinished(app) -> int:
    """Discard expired setup intents before this pass releases their Spaces."""
    agents = getattr(app.state, "agent_instances", None)
    if agents is None:
        return 0
    cutoff = datetime.now(UTC) - timedelta(seconds=UNFINISHED_CREATION_SECONDS)
    async with agents._sf() as session:
        rows = (await session.execute(select(AgentInstanceRow.id, AgentInstanceRow.custody).where(AgentInstanceRow.status == "provisioning", AgentInstanceRow.created_at < cutoff).order_by(AgentInstanceRow.id))).all()
    removed = 0
    for instance_id, custody in rows:
        try:
            await agents.abandon_creation(instance_id)
        except Exception as error:
            logger.warning("agents: unfinished creation cleanup remains pending (%s)", type(error).__name__)
            continue
        removed += 1
        logger.info("agents: unfinished %s %s removed after 24 hours", "assistant" if custody == "personal" else "employee", instance_id)
    return removed


async def _maintenance_step(app, *, timeout: float) -> bool:
    status = app.state.agent_execution
    if getattr(app.state, "storage_spaces", None) is None:
        return False

    async def perform():
        try:
            await _resident_step(app, timeout=0)
        except Exception:
            logger.exception("agents: startup accounting retry remains pending")
        try:
            await abandon_unfinished(app)
        except Exception as error:
            logger.warning("agents: unfinished creation cleanup remains pending (%s)", type(error).__name__)
        if (await storage_state(app)).state in ("off", "unavailable"):
            return
        runtime = getattr(app.state, "resident_runtime", None)
        if runtime is not None and runtime.coordinator.result is not None:
            try:
                await runtime.reconcile()
            except Exception:
                logger.exception("agents: workspace qualification remains pending")
            if not runtime.foreign_run_blocks and not runtime.accounting_retry:
                try:
                    await runtime.registry.reap()
                except Exception:
                    logger.exception("agents: workspace maintenance remains pending")
                try:
                    await runtime.settle_pending()
                except Exception:
                    logger.exception("agents: Work settlement remains pending")
        try:
            status.spaces_reclaimed += await finish_releases(app)
        except Exception:
            logger.exception("storage: released-account reclamation failed; existing facts are kept")
        try:
            await assign_spaces(app)
        except Exception:
            logger.exception("storage: assignment repair failed; existing facts are kept")
        try:
            status.file_operations_settled += await app.state.storage_spaces.settle_interrupted()
        except Exception:
            logger.exception("storage: operation settlement failed; existing facts are kept")
        if not status.agents_ready_reported and (await execution_state(app)).execution == "ready":
            _agents_ready_line(app)

    if status.maintenance_task is not None and status.maintenance_task.done():
        status.maintenance_task.result()
        status.maintenance_task = None
    if status.maintenance_task is None:
        status.maintenance_task = asyncio.create_task(perform())
        status.maintenance_task.add_done_callback(_observe)
    task = status.maintenance_task
    done, _pending = await asyncio.wait({task}, timeout=max(0, timeout))
    if not done:
        return False
    status.maintenance_task = None
    task.result()
    return True


async def _company_step(app, *, timeout: float) -> bool:
    status = app.state.agent_execution
    if status.host_id is None or (await storage_state(app)).state in ("off", "unavailable"):
        return False
    if status.company_task is None:
        status.company_task = asyncio.create_task(ensure_company_space(app))
        status.company_task.add_done_callback(_observe)
    task = status.company_task
    done, _pending = await asyncio.wait({task}, timeout=max(0, timeout))
    if not done:
        return False
    status.company_task = None
    try:
        task.result()
    except Exception as exc:
        logger.warning("storage: company assignment remains pending (%s)", exc)
        return False
    return True


async def storage_recheck(app):
    try:
        await _admission_recheck(app)
    except Exception:
        logger.exception("storage: availability recheck failed; existing facts are kept")
    status = app.state.agent_execution
    if status.host_id is not None and (await storage_state(app)).state not in ("off", "unavailable"):
        await _company_step(app, timeout=0)
    await _maintenance_step(app, timeout=0)


async def _admission_recheck(app):
    status = app.state.agent_execution
    if status.storage_reason != "admission-failed" and not (status.storage_reason == "host-unqualified" and status.runtime_retry):
        return
    counts = None
    if status.metadata_task is not None:
        if not status.metadata_task.done():
            return
        counts = await _metadata_step(app, timeout=0)
        if counts is None:
            return
    if status.mount_pending:
        if status.mount_task is not None and not status.mount_task.done():
            return
        if status.mount_cleanup_task is not None and not status.mount_cleanup_task.done():
            return
        if not await _mount_step(app, timeout=STORAGE_INIT_VERBS["mount-all"]):
            return
        status.mount_reverify = True
    if status.mount_reverify:
        if not await _mount_verification_step(app, timeout=ADAPTER_LIMIT_SECONDS):
            return
        status.mount_reverify = False
        counts = await _metadata_step(app, timeout=ADAPTER_LIMIT_SECONDS)
        if counts is None:
            return
    if status.adapter_task is not None and not status.adapter_task.done():
        return
    if await _adapter_step(app, timeout=ADAPTER_LIMIT_SECONDS):
        await _startup_lines(app, counts=counts)


async def _recheck_loop(app):
    stop = app.state.storage_recheck_stop
    wake = getattr(app.state, "storage_recheck_wake", None)
    try:
        await storage_recheck(app)
    except Exception:
        logger.exception("storage: availability recheck failed; existing facts are kept")
    while not stop.is_set():
        async with asyncio.TaskGroup() as group:
            waits = [group.create_task(stop.wait())]
            if wake is not None:
                waits.append(group.create_task(wake.wait()))
            await asyncio.wait(waits, timeout=RECHECK_SECONDS, return_when=asyncio.FIRST_COMPLETED)
            for task in waits:
                task.cancel()
        if wake is not None:
            wake.clear()
        if stop.is_set():
            break
        try:
            await storage_recheck(app)
        except Exception:
            logger.exception("storage: availability recheck failed; existing facts are kept")


async def drain_storage_spaces(app):
    """Join owned maintenance without withdrawing host storage callbacks."""

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
            owned.extend((status.adapter_task, status.metadata_task, getattr(status, "marker_backfill_task", None), status.company_task, status.maintenance_task, status.mount_task, status.mount_cleanup_task, status.mount_verify_task))
        owned = [task for task in owned if task is not None]
        if owned:
            await asyncio.gather(*owned, return_exceptions=True)
        if status is not None:
            if status.mount_task is not None and not status.mount_task.cancelled():
                try:
                    result = status.mount_task.result()
                except Exception:
                    result = {}
                status.mount_cleanup = result.get("cleanup_pending") or status.mount_cleanup
            if status.mount_cleanup_task is not None and not status.mount_cleanup_task.cancelled():
                try:
                    removed = status.mount_cleanup_task.result()
                except Exception:
                    removed = False
                if removed:
                    status.mount_cleanup = None
            if status.mount_cleanup is not None:
                status.mount_cleanup_task = asyncio.ensure_future(run_file_io(_remove_storage_init, status.mount_cleanup))
                status.mount_cleanup_task.add_done_callback(_observe)
                if await status.mount_cleanup_task:
                    status.mount_cleanup = None
                else:
                    logger.warning("storage: storage-init cleanup remains unconfirmed (%s)", status.mount_cleanup)

    task = getattr(app.state, "storage_shutdown_task", None)
    if task is None:
        task = asyncio.create_task(drain())
        app.state.storage_shutdown_task = task
    await await_drained(task)


async def close_storage_spaces(app):
    try:
        await drain_storage_spaces(app)
    finally:
        _clear_owned_seats(app)


def _clear_owned_seats(app):
    owned = getattr(app.state, "storage_seat_callback", None)
    if owned is not None:
        repository, callback = owned
        if repository.seats_allowed is callback:
            repository.seats_allowed = None
        app.state.storage_seat_callback = None


def _install_seats(app, session_factory, repository):
    """Use the creation repository, including when the typed bundle pool is unavailable."""
    if app.state.storage_spaces_config.plan is None:
        return
    if repository._sf.kw.get("bind") is not session_factory.kw.get("bind"):
        raise RuntimeError("Account creation and bundle storage must share one engine")

    async def seats_allowed(session):
        return (await allowance(app, "user", session=session)).allowed

    repository.seats_allowed = seats_allowed
    app.state.storage_seat_callback = (repository, seats_allowed)


@asynccontextmanager
async def storage_spaces_lifecycle(app):
    """Own startup tasks through later initialization failures and runtime teardown."""
    app.state.storage_shutdown_task = None
    try:
        yield
    finally:
        from app.gateway.deps import _close_runtime_storage

        await _close_runtime_storage(app)


def supports_storage_credentials(request) -> bool:
    """Legacy route scope/default identity is never resource authentication."""
    source = getattr(getattr(request, "state", None), "auth_source", None)
    if source == AUTH_SOURCE_INTERNAL:
        return get_trusted_internal_owner_user_id(request) is not None
    return source in (AUTH_SOURCE_SESSION, AUTH_SOURCE_AUTH_DISABLED)


def human_lookup(repository):
    async def lookup(reference: PrincipalRef, *, session=None) -> ResolvedPrincipal | None:
        if reference.kind != "human":
            return None
        # The one synthetic identity is supported only by the explicit local
        # development adapter, never by missing request context or a missing row.
        if reference.subject_id == AUTH_DISABLED_USER_ID and is_auth_disabled():
            return ResolvedPrincipal(reference, can_provision_company=True)
        user = await repository.get_user_by_id(reference.subject_id, session=session) if session is not None else await repository.get_user_by_id(reference.subject_id)
        if user is None or str(user.id) != reference.subject_id or account_refusal(user) is not None:
            return None
        return ResolvedPrincipal(reference, can_provision_company=user.system_role == "admin" and getattr(user, "role_limit", None) != "user")

    async def in_session(reference, session):
        return await lookup(reference, session=session)

    lookup.in_session = in_session
    return lookup


def read_storage_pool(config):
    """Read and verify the configured pool without starting Gateway services."""
    return _read_pool(config)


async def storage_allowance(config, catalog, session_factory, kind: str, *, session=None) -> Allowance:
    """Read a typed allowance with an optional borrowed transaction."""
    return await _allowance(config, catalog, session_factory, kind, session=session)


def build_storage_services(config, catalog, session_factory, repository):
    """Construct policy objects only; callers separately own all startup effects."""
    human = human_lookup(repository)
    directory = InstanceDirectory(session_factory, human=human)
    registry = SpaceRegistry(session_factory, HostPrincipalResolver(human=human, nonhuman=directory.lookup, nonhuman_in_session=directory.lookup_in_session))
    files = SpaceFiles(registry, catalog, inventory_path=Path(config.inventory_path), user_spaces=config.user_spaces)
    return files, AgentInstances(files, directory)


async def initialize_storage_spaces(app, config: StorageSpacesConfig, *, session_factory, creation_repository=None) -> None:
    entered = time.monotonic()
    app.state.storage_startup_entered = entered
    _clear_owned_seats(app)
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
    status = app.state.agent_execution
    if os.environ.get("HARTMESH_STORAGE_INIT_IMAGE", "").strip():
        await _mount_step(app, timeout=min(STORAGE_INIT_VERBS["mount-all"], max(0, entered + STARTUP_BUDGET_SECONDS - time.monotonic())))
    mount_reason, mount_text = status.storage_reason, status.storage_text
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
    if status.mount_pending and status.storage_reason is None:
        status.storage_reason, status.storage_text = mount_reason, mount_text
    if creation_repository is None:
        from deerflow.persistence.engine import get_session_factory

        # Isolated harnesses own their repository without changing app singletons.
        active_factory = get_session_factory()
        if active_factory is not None and active_factory.kw.get("bind") is session_factory.kw.get("bind"):
            from app.gateway.deps import bind_user_repo

            creation_repository = bind_user_repo(session_factory)
        else:
            creation_repository = SQLiteUserRepository(session_factory)
    app.state.storage_spaces, app.state.agent_instances = build_storage_services(config, catalog, session_factory, creation_repository)
    app.state.storage_spaces._managed_runtime_required = config.plan is not None
    app.state.storage_spaces._pool_runtime_owner = getattr(app.state, "pool_runtime_owner", None)
    app.state.storage_spaces.maintenance_logger = logger
    from deerflow.features.plugins import FeatureServices

    extensions = getattr(app.state, "extensions", None)
    feature_services = extensions.app_store.get(FeatureServices) if extensions is not None else None
    if feature_services is not None:
        for namespace, service in feature_services.services.items():
            if service.healthy:
                app.state.storage_spaces.workflow_settlers[namespace] = service.settle_workflow
    _install_seats(app, session_factory, creation_repository)
    app.state.agent_conversations = AgentConversations(app.state.agent_instances)
    if thread_store is not None and hasattr(thread_store, "instance_authority"):
        thread_store.instance_authority = app.state.agent_conversations
    counts = None if status.mount_pending else await _metadata_step(app, timeout=max(0, entered + STARTUP_BUDGET_SECONDS - time.monotonic()))
    app.state.agent_instances.execution_probe = lambda instance_id=None: execution_state(app, instance_id)
    app.state.agent_instances.allowance_of = lambda kind, *, session=None: allowance(app, kind, session=session)
    if status.storage_reason is None and status.metadata_task is None and not status.mount_pending:
        await _adapter_step(app, timeout=min(ADAPTER_LIMIT_SECONDS, max(0, entered + STARTUP_BUDGET_SECONDS - time.monotonic())))
    await _resident_step(app, timeout=None)
    if await _company_step(app, timeout=max(0, entered + STARTUP_BUDGET_SECONDS - time.monotonic())):
        counts = None
    if await _maintenance_step(app, timeout=max(0, entered + STARTUP_BUDGET_SECONDS - time.monotonic())):
        counts = None
    await _startup_lines(app, counts=counts)
    app.state.storage_recheck_stop = asyncio.Event()
    app.state.storage_recheck_wake = asyncio.Event()
    app.state.storage_recheck_task = asyncio.create_task(_recheck_loop(app))
