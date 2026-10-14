"""Bind one existing run to its qualified home environment, without fake users."""

import asyncio
import logging
import shlex
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

import httpx

from deerflow.agent_instances.contract import RESIDENT_INTERRUPTED, AgentDenied, Refusal
from deerflow.agent_instances.conversations import AgentExecution
from deerflow.sandbox.sandbox_provider import SandboxProvider
from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
from deerflow.spaces.facade import storage_actor_scope
from deerflow.utils.file_io import await_drained, run_file_io

logger = logging.getLogger(__name__)


@dataclass
class ExecutionEnvironment:
    execution: AgentExecution
    provider: SandboxProvider | None = None
    attachment_id: str | None = None
    recycle_required: bool = False


_environment = ContextVar("agent_execution_environment", default=None)


def current_environment():
    return _environment.get()


async def admit_storage(agents, instance, call, *, wait=False):
    """Translate run-start storage facts without locks or physical effects."""
    from deerflow.agent_instances.contract import ExecutionUnavailable, ResidentFilesBusy, ResidentStartFailed, ResidentStartingElsewhere
    from deerflow.spaces.attachments import AttachmentPending
    from deerflow.spaces.backings import BackingUnavailable
    from deerflow.spaces.service import SpaceOperationPending

    try:
        return await call()
    except SpaceOperationPending as exc:
        raise ResidentFilesBusy(instance.name, cause=exc.cause) from exc
    except BackingUnavailable as exc:
        state = await agents.execution_probe(instance.id) if agents.execution_probe is not None else None
        raise ExecutionUnavailable(getattr(state, "reason", None) or "space-unverified") from exc
    except AttachmentPending as exc:
        current = await agents.files.attachments.unfenced(instance.home_id)
        refusal = ResidentStartFailed if getattr(current, "phase", None) == "fence_pending" else ResidentStartingElsewhere
        raise refusal(instance.name) from exc


def _workspace_identity(execution, environment):
    if environment is None:
        return None, None
    if environment.execution is not execution:
        raise AgentDenied("The workspace environment belongs to another execution")
    sandbox = getattr(getattr(environment, "provider", None), "sandbox", None)
    captured = getattr(environment, "attachment_id", None)
    client_id = getattr(sandbox, "id", None)
    if captured is not None and client_id is not None and captured != client_id:
        raise AgentDenied("The workspace client does not match its captured attachment")
    return sandbox, captured or client_id


async def require_live_workspace(execution, environment) -> None:
    residents = getattr(execution.authority.instances, "residents", None)
    if residents is None:
        return
    sandbox, attachment_id = _workspace_identity(execution, environment)
    if attachment_id is None:
        return
    reason = await residents.attachments.fence_reason(attachment_id)
    if reason is None and (getattr(sandbox, "execution_outcome_uncertain", False) or getattr(sandbox, "requires_container_recycle", False)):
        if await residents.attachments.container_gone(attachment_id):
            async with residents.entry(execution.instance).lock:
                if await residents.attachments.container_gone(attachment_id):
                    await residents.attachments.contain_host(attachment_id, "container-absent")
            reason = await residents.attachments.fence_reason(attachment_id)
    if reason is not None:
        raise AgentDenied("The workspace of this run was closed (" + reason + ")")


async def run_error(execution, record, exc: BaseException) -> dict:
    """Map host facts without mutating the winning run outcome."""
    plain = {"name": type(exc).__name__, "reason": None, "stop_reason": None}
    if isinstance(exc, Refusal):
        return {"name": type(exc).__name__, "reason": exc.reason, "stop_reason": exc.reason}
    if execution is None:
        return plain
    try:
        residents = getattr(execution.authority.instances, "residents", None)
        if residents is None:
            return plain
        _sandbox, attachment_id = _workspace_identity(execution, current_environment())
        reason = await residents.attachments.fence_reason(attachment_id) if attachment_id is not None else None
        if reason is not None:
            return {"name": RESIDENT_INTERRUPTED, "reason": reason, "stop_reason": None}
    except Exception:
        logger.warning("Workspace error diagnostics unavailable for run %s", record.run_id, exc_info=True)
    from deerflow.spaces.service import SpaceOperationPending

    if isinstance(exc, SpaceOperationPending):
        return {"name": "ResidentFilesBusy", "reason": "storage-busy", "stop_reason": "storage-busy", "cause": exc.cause}
    return plain


async def complete_environment(execution, environment, run_manager, record):
    """Own SDK closure, Work settlement and unavoidable resident completion."""
    from deerflow.agent_instances.work import UNRESOLVED

    residents = execution.authority.instances.residents

    async def perform():
        cleanup_settled = not getattr(environment, "recycle_required", False)
        provider = getattr(environment, "provider", None)
        sandbox = getattr(provider, "sandbox", None)
        attachment_id = getattr(environment, "attachment_id", None) or getattr(sandbox, "id", None)
        attempt_status = None
        try:
            if provider is not None:
                try:
                    cleanup_settled = cleanup_settled and not (getattr(sandbox, "requires_container_recycle", True) or getattr(sandbox, "execution_outcome_uncertain", True))
                    await await_drained(run_file_io(provider.close))
                    cleanup_settled = cleanup_settled and not (getattr(sandbox, "requires_container_recycle", True) or getattr(sandbox, "execution_outcome_uncertain", True))
                except (Exception, asyncio.CancelledError):
                    cleanup_settled = False
                    logger.warning("SDK cleanup remains unconfirmed for run %s", record.run_id, exc_info=True)
        finally:
            try:
                if execution.work is not None:
                    settled = False
                    try:
                        stored = await run_manager._store.get(record.run_id, user_id=record.user_id) if run_manager._store is not None else None
                        fenced = attachment_id is not None and await residents.attachments.fence_reason(attachment_id) is not None
                        settled = bool(cleanup_settled and not fenced and execution.work.owned_cleanup_confirmed and stored and stored.get("status") == record.status.value and stored.get("stop_reason") != "orphan_recovered")
                    except (Exception, asyncio.CancelledError):
                        logger.warning("Work cleanup diagnostics unavailable for run %s", record.run_id, exc_info=True)
                    try:
                        attempt_status = await execution.work.finish(execution, record, settled=settled)
                    except (Exception, asyncio.CancelledError):
                        attempt_status = "uncertain"
                        logger.warning("Work settlement remains pending for run %s", record.run_id, exc_info=True)
            finally:
                await residents.run_ended(
                    execution.instance.id, execution.instance.home_id, record.run_id, attachment_id, recycle=not cleanup_settled or attempt_status in UNRESOLVED, attempt_id=execution.work.attempt_id if execution.work is not None else None
                )

    try:
        await await_drained(perform())
    except (Exception, asyncio.CancelledError):
        # Completion cannot replace the worker's original result or exception.
        logger.warning("Resident completion drained with an error for run %s", record.run_id, exc_info=True)


@contextmanager
def execution_scope(execution):
    if not isinstance(execution, AgentExecution):
        yield None
        return
    environment = ExecutionEnvironment(execution)
    token = _environment.set(environment)
    try:
        from deerflow.agent_instances.public_skills import public_skill_scope
        from deerflow.agents.memory.manager import memory_execution_scope

        with storage_actor_scope(execution.instance.principal), public_skill_scope(), memory_execution_scope(execution):
            yield environment
    finally:
        from deerflow.agent_instances.memory import finish_execution_memory

        finish_execution_memory(execution)
        _environment.reset(token)


class InstanceSandboxProvider(SandboxProvider):
    """One run's client; storage owns the resident container and its retirement."""

    uses_thread_data_mounts = False
    needs_upload_permission_adjustment = False

    def __init__(self, execution, sandbox, *, app_config=None, skill_revision=None):
        self.execution, self.sandbox = execution, sandbox
        self.app_config, self.skill_revision = app_config, skill_revision

    def acquire(self, thread_id=None, *, user_id=None):
        self.execution.validate_sync()
        if thread_id != self.execution.thread_id:
            raise AgentDenied("An instance environment cannot be reused by another conversation")
        return self.sandbox.id

    async def acquire_async(self, thread_id=None, *, user_id=None):
        await self.execution.validate()
        if thread_id != self.execution.thread_id:
            raise AgentDenied("An instance environment cannot be reused by another conversation")
        return self.sandbox.id

    def get(self, sandbox_id):
        return self.sandbox if sandbox_id == self.sandbox.id else None

    def get_scoped(self, sandbox_id, *, thread_id, user_id):
        return self.get(sandbox_id) if thread_id == self.execution.thread_id else None

    def release(self, sandbox_id):
        # Leases close execution-scoped shells through SandboxLeaseManager.
        # Ordinary completion cannot retire or replace a storage writer.
        pass

    def close(self):
        self.sandbox.close()


def instance_tools(tools):
    """Keep supported native/public tools; withhold private integration surfaces."""
    builtin_modules = {
        "deerflow.sandbox.tools",
        "deerflow.tools.conversation",
        "deerflow.tools.builtins.ask_clarification_tool",
        "deerflow.tools.builtins.present_file_tool",
        "deerflow.tools.builtins.task_tool",
        "deerflow.tools.builtins.view_image_tool",
    }
    public_providers = {"tavily", "sofya", "jina_ai", "unbrowse", "firecrawl", "image_search", "brave", "ddg_search", "exa", "searxng", "serper", "serply", "tencent_wsa"}
    retained = []
    for tool in tools:
        function = getattr(tool, "coroutine", None) or getattr(tool, "func", None)
        module = getattr(function, "__module__", "")
        parts = module.split(".")
        if module in builtin_modules or (parts[:2] == ["deerflow", "community"] and len(parts) > 2 and parts[2] in public_providers):
            retained.append(tool)
    return retained


async def qualified_attachments(files, *, baseline_provider=None):
    """Resolve the existing qualified host adapter independently of a first chat."""
    from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
    from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
    from deerflow.spaces.docker import DockerStorageAdapter, StorageAdapterUnsupported, StorageHostUnqualified
    from deerflow.spaces.runtime_owner import StorageRuntimeUnavailable

    owner = getattr(files, "_pool_runtime_owner", None)
    if getattr(files, "_managed_runtime_required", False):
        if owner is None:
            raise StorageHostUnqualified("Managed storage requires its owning Gateway lifetime")
        if baseline_provider is None:
            from deerflow.sandbox.sandbox_provider import get_baseline_sandbox_provider

            baseline_provider = await await_drained(run_file_io(owner.resolve_provider, get_baseline_sandbox_provider))
        else:
            await await_drained(run_file_io(owner.assert_provider, baseline_provider))
    elif baseline_provider is None:
        # Resolve before installing the run provider, so no ambient requester
        # environment can ever become the instance's fallback.
        from deerflow.sandbox.sandbox_provider import get_initialized_sandbox_provider

        baseline_provider = get_initialized_sandbox_provider()
        if baseline_provider is None:
            # The explicit baseline getter skips only this host scope; it
            # still constructs the configured provider through its usual ABI.
            from deerflow.sandbox.sandbox_provider import get_baseline_sandbox_provider

            baseline_provider = await await_drained(run_file_io(get_baseline_sandbox_provider))
    if not isinstance(baseline_provider, AioSandboxProvider) or not isinstance(baseline_provider._backend, LocalContainerBackend):
        raise StorageHostUnqualified("Instance execution requires qualified direct-host Docker AIO storage")
    if owner is not None:
        from deerflow.spaces.chat_bridge import get_chat_storage_host

        host = get_chat_storage_host()
        admission = getattr(host, "admission", None)
        if admission is not None:
            if host.pool_runtime_owner is not owner or baseline_provider._storage_host() is not host:
                raise StorageHostUnqualified("Managed execution lifecycle belongs to another owning host")
            baseline_provider._bind_runtime_admission(admission)
    if getattr(files, "inventory_path", None) is None:
        raise StorageHostUnqualified("Native execution requires a configured inventory path")
    if owner is not None:
        baseline_provider._managed_runtime_required = True
        baseline_provider._pool_runtime_owner = owner
        baseline_provider._backend._runtime_runner = owner.run
        baseline_provider._backend._managed_native_required = True
    lock = getattr(files, "_qualification_lock", None)
    if lock is None:
        lock = files._qualification_lock = asyncio.Lock()
    async with lock:
        previous = getattr(baseline_provider, "_native_creation", None)
        try:
            mounts = getattr(files, "attachments", None)
            adapter = getattr(files, "_qualified_adapter", None)
            if adapter is None:
                adapter = await await_drained(run_file_io(DockerStorageAdapter.from_local_backend, baseline_provider._backend, pool=Path(files.inventory_path).parent, clean_probe=mounts is None))
            else:
                if mounts is None or mounts.provider is not adapter or getattr(files, "_qualified_backend", None) is not baseline_provider._backend:
                    raise StorageHostUnqualified("Cached storage adapter lost its exact provider binding")
                await await_drained(run_file_io(adapter._same_host))
            if mounts is None:
                mounts = SpaceAttachments(files, adapter)
            elif mounts.provider.host_id != adapter.host_id:
                raise StorageAdapterUnsupported("The home attachment belongs to another qualified host")
            if owner is not None and mounts.provider is not adapter:
                raise StorageRuntimeUnavailable("Attachment qualification cannot replace its retained adapter")
            # The adapter's physical probe already succeeded. Retain these
            # exact objects even if the later journal installation fails.
            files.attachments = mounts
            files._qualified_adapter = adapter
            files._qualified_backend = baseline_provider._backend
            if owner is not None:
                evidence = await await_drained(run_file_io(owner.qualify_native_evidence, baseline_provider, daemon_id=adapter.host_id))
                await await_drained(run_file_io(baseline_provider._install_native_creation, evidence, adapter))
                from deerflow.spaces.resident_containment import NativeResidentContainmentScopes

                containment = getattr(adapter, "_containment_scope", None)
                if containment is None:
                    adapter._containment_scope = NativeResidentContainmentScopes(provider=baseline_provider, files=files, adapter=adapter)
                elif not isinstance(containment, NativeResidentContainmentScopes) or (containment.provider is not baseline_provider or containment.files is not files or containment.adapter is not adapter):
                    raise StorageRuntimeUnavailable("Resident containment cannot replace another retained lifetime")
                await await_drained(run_file_io(owner.assert_provider, baseline_provider))
                await await_drained(run_file_io(baseline_provider._reconcile_orphans))
            files.attachments = mounts
            files._qualified_adapter = mounts.provider
            files._qualified_backend = baseline_provider._backend
            return baseline_provider, mounts
        except BaseException as exc:
            from deerflow.community.aio_sandbox.native_creation import NativeCreateBridge

            bridge = getattr(baseline_provider, "_native_creation", None)
            if isinstance(bridge, NativeCreateBridge):
                bridge.invalidate()
            if isinstance(previous, NativeCreateBridge) and isinstance(exc, StorageAdapterUnsupported):
                raise StorageRuntimeUnavailable("Previously qualified native runtime is unavailable; its evidence and provider are retained") from exc
            raise


async def prepare_environment(execution, *, baseline_provider=None, app_config=None, run_id=None):
    residents = getattr(execution.authority.instances, "residents", None)
    if residents is None:
        return await _prepare_environment_legacy(execution, baseline_provider=baseline_provider, app_config=app_config)
    environment = current_environment()
    if environment is None or environment.execution is not execution:
        raise AgentDenied("Instance preparation requires its exact host environment")
    from deerflow.agent_instances.resident_preparation import prepare_resident

    # Publish the client inside the owned operation. Cancellation drains that
    # operation but discards its return value; completion still needs its client.
    return await await_drained(prepare_resident(execution, environment, residents, baseline_provider=baseline_provider, app_config=app_config, run_id=run_id))


async def _prepare_environment_legacy(execution, *, baseline_provider=None, app_config=None):
    await execution.authority.validate(execution)
    from deerflow.agent_instances.public_skills import capture_public_skills
    from deerflow.config.app_config import get_app_config

    app_config = app_config or await await_drained(run_file_io(get_app_config))
    capture = await await_drained(run_file_io(capture_public_skills, execution.definition, app_config))
    baseline_provider, mounts = await qualified_attachments(execution.authority.instances.files, baseline_provider=baseline_provider)
    resources = [ResourceMount(execution.instance.home_id, execution.home_generation, "home", writable=True)]
    attached = await mounts.resume(actor=execution.instance.principal, resources=resources)
    if attached is None:
        attached = await mounts.attach(actor=execution.instance.principal, incarnation=uuid4().hex, resources=resources)
    return await _prepare_client(execution, baseline_provider, attached, capture, app_config)


async def _prepare_client(execution, baseline_provider, attached, capture, app_config, *, validate=True):
    from deerflow.agent_instances.docker_transport import DockerControlTransport
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox
    from deerflow.spaces.docker import StorageAdapterUnsupported

    transport = DockerControlTransport(attached.container_id)
    url = "http://127.0.0.1:8080"

    def ready():
        deadline = time.monotonic() + baseline_provider.sandbox_ready_timeout()
        with httpx.Client(transport=DockerControlTransport(attached.container_id), trust_env=False) as client:
            while time.monotonic() < deadline:
                try:
                    if client.get(url + "/v1/sandbox", timeout=min(5, max(0.01, deadline - time.monotonic()))).status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                time.sleep(min(0.25, max(0, deadline - time.monotonic())))
        raise StorageAdapterUnsupported("Instance readiness is unconfirmed; retain its storage attachment")

    await await_drained(run_file_io(ready))
    sandbox = AioSandbox(attached.id, url, home_dir="/mnt/spaces/home", control_transport=transport, download_roots=("/mnt/spaces/home", "/mnt/user-data/workspace", "/mnt/user-data/uploads", "/mnt/user-data/outputs"))
    try:
        await await_drained(run_file_io(transport.prepare_home_aliases))
        layout = await await_drained(
            run_file_io(
                sandbox.execute_command,
                "mkdir -p /mnt/spaces/home/uploads /mnt/spaces/home/outputs && cd /mnt/user-data/workspace && test \"$(pwd -P)\" = /mnt/spaces/home && printf 'HARTMESH_HOME_READY\\n'",
            )
        )
        if "HARTMESH_HOME_READY" not in layout.splitlines():
            raise AgentDenied("The admitted Home layout is unconfirmed")
        # Per-run references name a content-addressed home cache. This changes
        # no process-global alias or physical attachment; ordinary home edits
        # remain data and cannot change the host definition/tool authority.
        package_root = "/mnt/spaces/home/.agent-skill-packages/" + capture.revision
        for relative, content in capture.files:
            destination = package_root + "/" + relative
            await await_drained(run_file_io(sandbox.execute_command, "mkdir -p " + shlex.quote(str(PurePosixPath(destination).parent))))
            await await_drained(run_file_io(sandbox.update_file, destination, content))
            if await await_drained(run_file_io(sandbox.download_file, destination)) != content:
                raise AgentDenied("The admitted public package copy is unconfirmed")
        scoped_skills = app_config.skills.model_copy(update={"container_path": package_root})
        scoped_config = app_config.model_copy(update={"skills": scoped_skills})
        if validate:
            await execution.authority.validate(execution)
        return InstanceSandboxProvider(execution, sandbox, app_config=scoped_config, skill_revision=capture.revision)
    except BaseException:
        await await_drained(run_file_io(sandbox.close))
        raise
