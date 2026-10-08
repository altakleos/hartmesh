"""Bind one existing run to its qualified home environment, without fake users."""

import shlex
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import PurePosixPath
from uuid import uuid4

import httpx

from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.conversations import AgentExecution
from deerflow.sandbox.sandbox_provider import SandboxProvider
from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
from deerflow.spaces.facade import storage_actor_scope
from deerflow.utils.file_io import await_drained, run_file_io


@dataclass
class ExecutionEnvironment:
    execution: AgentExecution
    provider: SandboxProvider | None = None


_environment = ContextVar("agent_execution_environment", default=None)


def current_environment():
    return _environment.get()


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
    from deerflow.spaces.docker import DockerStorageAdapter, StorageAdapterUnsupported

    if baseline_provider is None:
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
        raise StorageAdapterUnsupported("Instance execution requires qualified direct-host Docker AIO storage")
    adapter = await await_drained(run_file_io(DockerStorageAdapter.from_local_backend, baseline_provider._backend))
    # One host adapter service per configured inventory; its guard applies to
    # every ordinary file/membership/lifecycle operation, including restarts.
    mounts = getattr(files, "attachments", None)
    if mounts is None:
        mounts = SpaceAttachments(files, adapter)
    elif mounts.provider.host_id != adapter.host_id:
        raise StorageAdapterUnsupported("The home attachment belongs to another qualified host")
    return baseline_provider, mounts


async def prepare_environment(execution, *, baseline_provider=None, app_config=None):
    from deerflow.community.aio_sandbox.aio_sandbox import AioSandbox
    from deerflow.spaces.docker import StorageAdapterUnsupported

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

    from deerflow.agent_instances.docker_transport import DockerControlTransport

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
        await execution.authority.validate(execution)
        return InstanceSandboxProvider(execution, sandbox, app_config=scoped_config, skill_revision=capture.revision)
    except BaseException:
        await await_drained(run_file_io(sandbox.close))
        raise
