import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import replace as dc_replace
from typing import NotRequired, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.runtime import Runtime
from langgraph.types import Command, Overwrite

from deerflow.agents.thread_state import SandboxStateField, ThreadDataState
from deerflow.authz.sandbox_authz import (
    authorize_sandbox_execution,
    authorize_sandbox_execution_async,
    safe_app_config,
    safe_app_config_async,
)
from deerflow.runtime.user_context import resolve_runtime_user_id
from deerflow.sandbox import get_sandbox_provider
from deerflow.sandbox.exceptions import SandboxAuthorizationError, SandboxRuntimeError
from deerflow.sandbox.overwrite import unwrap_sandbox
from deerflow.sandbox.sandbox_provider import (
    _NO_BINDING,
    accepted_skill_material_binding_from_runtime,
    accepted_skill_snapshot_id_from_runtime,
    ensure_accepted_skill_binding,
    invalidate_runtime_skill_projection_token,
    release_accepted_skill_consumer,
    require_runtime_accepted_skill_isolation,
)
from deerflow.sandbox.session import declared_sandbox

logger = logging.getLogger(__name__)


class SandboxMiddlewareState(AgentState):
    """Compatible with the `ThreadState` schema."""

    sandbox: SandboxStateField
    thread_data: NotRequired[ThreadDataState | None]


class SandboxMiddleware(AgentMiddleware[SandboxMiddlewareState]):
    """Create a sandbox environment and assign it to an agent.

    Lifecycle Management:
    - With lazy_init=True (default): Sandbox is acquired on first tool call
    - With lazy_init=False: Sandbox is acquired on first agent invocation (before_agent)
    - Sandbox is reused across multiple turns within the same thread
    - Sandbox is NOT released after each agent call to avoid wasteful recreation
    - Cleanup happens at application shutdown via SandboxProvider.shutdown()
    """

    state_schema = SandboxMiddlewareState

    def __init__(
        self,
        lazy_init: bool = True,
        *,
        available_skills: set[str] | None = None,
        owns_agent_skill_projection: bool = True,
    ):
        """Initialize sandbox middleware.

        Args:
            lazy_init: If True, defer sandbox acquisition until first tool call.
                      If False, acquire sandbox eagerly in before_agent().
                      Default is True for optimal performance.
            owns_agent_skill_projection: Whether this middleware may create or
                rebuild the thread's physical skill projection. Delegated
                subagents share the lead thread sandbox and must preserve the
                lead-owned view instead of applying their discovery policy to it.
        """
        super().__init__()
        self._lazy_init = lazy_init
        self._available_skills = set(available_skills) if available_skills is not None else None
        self._owns_agent_skill_projection = owns_agent_skill_projection

    def _prepare_agent_skill_projection(self, thread_id: str, *, user_id: str):
        """Build the run's physical skill view before any sandbox is reused."""
        if not self._owns_agent_skill_projection:
            # Subagents inherit the lead's thread id and sandbox state. Their
            # skill lists scope discovery/activation only; rebuilding here
            # would widen or narrow the shared filesystem for every concurrent
            # agent using this sandbox.
            return None

        from deerflow.config.paths import get_paths

        # Preserve the zero-copy shared view for ordinary threads. A thread
        # that previously used a restricted Agent keeps its stable mount root;
        # an unrestricted run repopulates that root with all enabled skills.
        if self._available_skills is None and not get_paths().thread_skills_view_dir(thread_id, user_id=user_id).exists():
            return None

        provider = get_sandbox_provider()
        if not provider.supports_agent_skill_isolation:
            if self._available_skills is not None:
                raise SandboxRuntimeError(f"Sandbox provider {provider.__class__.__name__} cannot enforce per-Agent skill filesystem isolation")
            # The thread projection may have been created under a different
            # provider. An unrestricted run does not need that policy view and
            # may safely use this provider's ordinary shared skill behavior.
            return None

        from deerflow.config import get_app_config
        from deerflow.skills.projection import ensure_thread_skill_projection
        from deerflow.skills.storage import get_or_new_user_skill_storage

        app_config = get_app_config()
        storage = get_or_new_user_skill_storage(user_id, app_config=app_config)
        return ensure_thread_skill_projection(storage, thread_id, self._available_skills)

    @staticmethod
    def _require_projection_support(provider, projection) -> None:
        if projection is not None and not provider.supports_agent_skill_isolation:
            raise SandboxRuntimeError(f"Sandbox provider {provider.__class__.__name__} cannot enforce per-Agent skill filesystem isolation")

    def _acquire_sandbox(self, thread_id: str, *, user_id: str, accepted_skills_only: bool = False, runtime: Runtime | None = None) -> str:
        provider = get_sandbox_provider()
        if accepted_skills_only:
            binding = accepted_skill_material_binding_from_runtime(runtime, user_id=user_id)
            if binding is None:
                raise RuntimeError("accepted_skill_snapshot_runtime_identity_missing")
            sandbox_id = provider.acquire_bound_accepted_skills(
                thread_id,
                user_id=user_id,
                binding=binding,
            )
        else:
            sandbox_id = provider.acquire(thread_id, user_id=user_id)
        logger.info(f"Acquiring sandbox {sandbox_id}")
        return sandbox_id

    async def _acquire_sandbox_async(self, thread_id: str, *, user_id: str, accepted_skills_only: bool = False, runtime: Runtime | None = None) -> str:
        provider = get_sandbox_provider()
        if accepted_skills_only:
            binding = accepted_skill_material_binding_from_runtime(runtime, user_id=user_id)
            if binding is None:
                raise RuntimeError("accepted_skill_snapshot_runtime_identity_missing")
            sandbox_id = await provider.acquire_bound_accepted_skills_async(
                thread_id,
                user_id=user_id,
                binding=binding,
            )
        else:
            sandbox_id = await provider.acquire_async(thread_id, user_id=user_id)
        logger.info(f"Acquiring sandbox {sandbox_id}")
        return sandbox_id

    async def _release_sandbox_async(self, sandbox_id: str) -> None:
        await asyncio.to_thread(get_sandbox_provider().release, sandbox_id)

    @override
    def before_agent(self, state: SandboxMiddlewareState, runtime: Runtime) -> dict | None:
        has_accepted_binding = accepted_skill_snapshot_id_from_runtime(runtime) is not _NO_BINDING
        thread_id = (runtime.context or {}).get("thread_id")
        if thread_id is None:
            return super().before_agent(state, runtime)
        user_id = resolve_runtime_user_id(runtime)
        declared = declared_sandbox()
        if declared is not None:
            try:
                authorize_sandbox_execution(
                    context=runtime.context or {},
                    app_config=safe_app_config(),
                )
            except SandboxAuthorizationError:
                logger.info(
                    "Accepted sandbox execution denied for this role (thread_id=%s)",
                    thread_id,
                )
                return None
            existing_sandbox_id = self._read_sandbox_id_from_state(state)
            if existing_sandbox_id == declared.id:
                return super().before_agent(state, runtime)
            if existing_sandbox_id is not None:
                return {
                    "sandbox": Overwrite({"sandbox_id": declared.id}),
                }
            return {"sandbox": {"sandbox_id": declared.id}}
        projection = None if has_accepted_binding else self._prepare_agent_skill_projection(thread_id, user_id=user_id)

        # Durable accepted material and policy-scoped legacy views must be
        # projected before the first model. Ordinary shared-view runs keep
        # lazy sandbox initialization.
        if self._lazy_init and not has_accepted_binding and projection is None:
            return super().before_agent(state, runtime)

        existing_sandbox_id = self._read_sandbox_id_from_state(state)
        sandbox_id = existing_sandbox_id
        if isinstance(sandbox_id, str) and not has_accepted_binding and projection is None:
            return super().before_agent(state, runtime)
        try:
            authorize_sandbox_execution(
                context=runtime.context or {},
                app_config=safe_app_config(),
            )
        except SandboxAuthorizationError:
            if projection is not None:
                # An explicit skill policy cannot leave a checkpointed,
                # previously shared sandbox reusable by downstream tools.
                raise
            logger.info("Sandbox execution denied for this role; skipping eager sandbox acquisition (thread_id=%s)", thread_id)
            return None
        provider = get_sandbox_provider()
        self._require_projection_support(provider, projection)
        prebound = False
        runtime_sandbox_id = (runtime.context or {}).get("sandbox_id")
        if has_accepted_binding and not isinstance(sandbox_id, str) and isinstance(runtime_sandbox_id, str) and provider.get(runtime_sandbox_id) is not None:
            sandbox_id = runtime_sandbox_id
            prebound = True
        if has_accepted_binding and isinstance(sandbox_id, str) and provider.get(sandbox_id) is not None and not provider.has_accepted_skill_isolation(sandbox_id):
            provider.release(sandbox_id)
            sandbox_id = None
        acquired = not isinstance(sandbox_id, str) or provider.get(sandbox_id) is None
        if acquired:
            sandbox_id = self._acquire_sandbox(
                thread_id,
                user_id=user_id,
                accepted_skills_only=has_accepted_binding,
                runtime=runtime,
            )
        if has_accepted_binding:
            token = None
            try:
                require_runtime_accepted_skill_isolation(
                    provider,
                    runtime,
                    sandbox_id=sandbox_id,
                )
                binding, token, _created_token = ensure_accepted_skill_binding(
                    runtime,
                    sandbox_id=sandbox_id,
                    user_id=user_id,
                )
                assert binding is not None
                provider.bind_accepted_skill_snapshot(
                    sandbox_id,
                    thread_id=thread_id,
                    user_id=user_id,
                    binding=binding,
                )
            except Exception:
                released = False
                invalidate_runtime_skill_projection_token(runtime, token)
                if token is not None:
                    try:
                        released = release_accepted_skill_consumer(token)
                    except Exception:
                        logger.warning("Failed to clear a rejected accepted-skill projection", exc_info=True)
                if acquired and token is None and not released:
                    provider.release(sandbox_id)
                raise
        elif projection is not None:
            provider.sync_agent_skills(
                sandbox_id,
                thread_id=thread_id,
                user_id=user_id,
                projection=projection,
            )
        if acquired or prebound or projection is not None:
            logger.info(f"Assigned sandbox {sandbox_id} to thread {thread_id}")
            if existing_sandbox_id == sandbox_id:
                return super().before_agent(state, runtime)
            if existing_sandbox_id is not None:
                return {
                    "sandbox": Overwrite({"sandbox_id": sandbox_id}),
                }
            return {"sandbox": {"sandbox_id": sandbox_id}}
        return super().before_agent(state, runtime)

    @override
    async def abefore_agent(self, state: SandboxMiddlewareState, runtime: Runtime) -> dict | None:
        has_accepted_binding = accepted_skill_snapshot_id_from_runtime(runtime) is not _NO_BINDING
        thread_id = (runtime.context or {}).get("thread_id")
        if thread_id is None:
            return await super().abefore_agent(state, runtime)
        user_id = resolve_runtime_user_id(runtime)
        declared = declared_sandbox()
        if declared is not None:
            try:
                await authorize_sandbox_execution_async(
                    context=runtime.context or {},
                    app_config=await safe_app_config_async(),
                )
            except SandboxAuthorizationError:
                logger.info(
                    "Accepted sandbox execution denied for this role (thread_id=%s)",
                    thread_id,
                )
                return None
            existing_sandbox_id = self._read_sandbox_id_from_state(state)
            if existing_sandbox_id == declared.id:
                return await super().abefore_agent(state, runtime)
            if existing_sandbox_id is not None:
                return {
                    "sandbox": Overwrite({"sandbox_id": declared.id}),
                }
            return {"sandbox": {"sandbox_id": declared.id}}
        projection = (
            None
            if has_accepted_binding
            else await asyncio.to_thread(
                self._prepare_agent_skill_projection,
                thread_id,
                user_id=user_id,
            )
        )

        if self._lazy_init and not has_accepted_binding and projection is None:
            return await super().abefore_agent(state, runtime)

        existing_sandbox_id = self._read_sandbox_id_from_state(state)
        sandbox_id = existing_sandbox_id
        if isinstance(sandbox_id, str) and not has_accepted_binding and projection is None:
            return await super().abefore_agent(state, runtime)
        try:
            await authorize_sandbox_execution_async(
                context=runtime.context or {},
                app_config=await safe_app_config_async(),
            )
        except SandboxAuthorizationError:
            if projection is not None:
                raise
            logger.info("Sandbox execution denied for this role; skipping eager sandbox acquisition (thread_id=%s)", thread_id)
            return None
        provider = get_sandbox_provider()
        self._require_projection_support(provider, projection)
        prebound = False
        runtime_sandbox_id = (runtime.context or {}).get("sandbox_id")
        if has_accepted_binding and not isinstance(sandbox_id, str) and isinstance(runtime_sandbox_id, str) and provider.get(runtime_sandbox_id) is not None:
            sandbox_id = runtime_sandbox_id
            prebound = True
        if has_accepted_binding and isinstance(sandbox_id, str) and provider.get(sandbox_id) is not None and not provider.has_accepted_skill_isolation(sandbox_id):
            await self._release_sandbox_async(sandbox_id)
            sandbox_id = None
        acquired = not isinstance(sandbox_id, str) or provider.get(sandbox_id) is None
        if acquired:
            sandbox_id = await self._acquire_sandbox_async(
                thread_id,
                user_id=user_id,
                accepted_skills_only=has_accepted_binding,
                runtime=runtime,
            )
        if has_accepted_binding:
            token = None
            try:
                require_runtime_accepted_skill_isolation(
                    provider,
                    runtime,
                    sandbox_id=sandbox_id,
                )
                binding, token, _created_token = ensure_accepted_skill_binding(
                    runtime,
                    sandbox_id=sandbox_id,
                    user_id=user_id,
                )
                assert binding is not None
                await provider.bind_accepted_skill_snapshot_async(
                    sandbox_id,
                    thread_id=thread_id,
                    user_id=user_id,
                    binding=binding,
                )
            except Exception:
                released = False
                invalidate_runtime_skill_projection_token(runtime, token)
                if token is not None:
                    try:
                        released = await asyncio.to_thread(
                            release_accepted_skill_consumer,
                            token,
                        )
                    except Exception:
                        logger.warning("Failed to clear a rejected accepted-skill projection", exc_info=True)
                if acquired and token is None and not released:
                    await self._release_sandbox_async(sandbox_id)
                raise
        elif projection is not None:
            await provider.sync_agent_skills_async(
                sandbox_id,
                thread_id=thread_id,
                user_id=user_id,
                projection=projection,
            )
        if acquired or prebound or projection is not None:
            logger.info(f"Assigned sandbox {sandbox_id} to thread {thread_id}")
            if existing_sandbox_id == sandbox_id:
                return await super().abefore_agent(state, runtime)
            if existing_sandbox_id is not None:
                return {
                    "sandbox": Overwrite({"sandbox_id": sandbox_id}),
                }
            return {"sandbox": {"sandbox_id": sandbox_id}}
        return await super().abefore_agent(state, runtime)

    @override
    def after_agent(self, state: SandboxMiddlewareState, runtime: Runtime) -> dict | None:
        if declared_sandbox() is not None:
            # The declaring execution owns the terminal; the agent loop is a
            # holder inside it and must not retire the session on its way out.
            return None
        from deerflow.runtime.skill_projection import SKILL_PROJECTION_TOKEN_CONTEXT_KEY, SkillProjectionConsumerToken

        token = (runtime.context or {}).get(SKILL_PROJECTION_TOKEN_CONTEXT_KEY)
        if isinstance(token, SkillProjectionConsumerToken):
            release_accepted_skill_consumer(token)
            return None
        sandbox, fork_restored = unwrap_sandbox(state.get("sandbox"))
        if sandbox is not None:
            sandbox_id = sandbox["sandbox_id"]
            if fork_restored:
                # The wrapped value replays the parent thread's sandbox state;
                # releasing it here would evict the parent's warm sandbox.
                logger.info(f"Not releasing fork-restored sandbox {sandbox_id}")
                return None
            logger.info(f"Releasing sandbox {sandbox_id}")
            get_sandbox_provider().release(sandbox_id)
            return None

        if (runtime.context or {}).get("sandbox_id") is not None:
            sandbox_id = runtime.context.get("sandbox_id")
            logger.info(f"Releasing sandbox {sandbox_id} from context")
            get_sandbox_provider().release(sandbox_id)
            return None

        # No sandbox to release
        return super().after_agent(state, runtime)

    @override
    async def aafter_agent(self, state: SandboxMiddlewareState, runtime: Runtime) -> dict | None:
        if declared_sandbox() is not None:
            # The declaring execution owns the terminal; the agent loop is a
            # holder inside it and must not retire the session on its way out.
            return None
        from deerflow.runtime.skill_projection import SKILL_PROJECTION_TOKEN_CONTEXT_KEY, SkillProjectionConsumerToken

        token = (runtime.context or {}).get(SKILL_PROJECTION_TOKEN_CONTEXT_KEY)
        if isinstance(token, SkillProjectionConsumerToken):
            await asyncio.to_thread(release_accepted_skill_consumer, token)
            return None
        sandbox, fork_restored = unwrap_sandbox(state.get("sandbox"))
        if sandbox is not None:
            sandbox_id = sandbox["sandbox_id"]
            if fork_restored:
                # The wrapped value replays the parent thread's sandbox state;
                # releasing it here would evict the parent's warm sandbox.
                logger.info(f"Not releasing fork-restored sandbox {sandbox_id}")
                return None
            logger.info(f"Releasing sandbox {sandbox_id}")
            await self._release_sandbox_async(sandbox_id)
            return None

        if (runtime.context or {}).get("sandbox_id") is not None:
            sandbox_id = runtime.context.get("sandbox_id")
            logger.info(f"Releasing sandbox {sandbox_id} from context")
            await self._release_sandbox_async(sandbox_id)
            return None

        # No sandbox to release
        return await super().aafter_agent(state, runtime)

    # ------------------------------------------------------------------
    # Tool-call wrappers: persist lazily-acquired sandbox state into the
    # graph state via Command(update=...).
    #
    # Background:
    #   ``ensure_sandbox_initialized*`` in ``deerflow.sandbox.tools`` mutates
    #   ``runtime.state["sandbox"]`` directly. That mutation is local to the
    #   current tool invocation and is NOT picked up by LangGraph's channel
    #   reducer, so subsequent graph steps (and downstream consumers such as
    #   ``ToolOutputBudgetMiddleware`` and the sub-agent ``task_tool``)
    #   cannot observe the sandbox id. Wrapping the tool call lets us detect
    #   a fresh lazy init by diffing the state snapshot before/after the
    #   handler and emit a proper state update via ``Command``.
    # ------------------------------------------------------------------

    @staticmethod
    def _read_sandbox_id_from_state(state: object) -> str | None:
        if not isinstance(state, dict):
            return None
        sandbox_state, _ = unwrap_sandbox(state.get("sandbox"))
        if not isinstance(sandbox_state, dict):
            return None
        sandbox_id = sandbox_state.get("sandbox_id")
        return sandbox_id if isinstance(sandbox_id, str) else None

    @staticmethod
    def _attach_sandbox_update(result: ToolMessage | Command, sandbox_id: str) -> ToolMessage | Command:
        """Wrap or merge ``result`` so that ``sandbox.sandbox_id`` is persisted.

        - ``ToolMessage`` -> ``Command(update={"sandbox": ..., "messages": [msg]})``
        - ``Command`` with dict update -> merge ``sandbox`` key, preserve all
          existing fields (``messages``, ``goto``, ``graph``, ``resume``, ...).
        - ``Command`` with non-dict / None update -> leave it untouched to
          avoid silent data loss on unknown update shapes.
        """
        sandbox_update = {"sandbox": {"sandbox_id": sandbox_id}}

        if isinstance(result, ToolMessage):
            return Command(update={**sandbox_update, "messages": [result]})

        existing_update = result.update
        if isinstance(existing_update, dict):
            merged_update = {**existing_update, **sandbox_update}
            return dc_replace(result, update=merged_update)
        return result

    @staticmethod
    def _read_sandbox_id_from_request(request: ToolCallRequest) -> str | None:
        """Read sandbox_id from runtime.state (where ensure_sandbox_initialized writes)."""
        runtime = request.runtime
        if runtime is None or runtime.state is None:
            return None
        return SandboxMiddleware._read_sandbox_id_from_state(runtime.state)

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        prev_sandbox_id = self._read_sandbox_id_from_request(request)
        result = handler(request)
        if prev_sandbox_id is not None:
            return result
        curr_sandbox_id = self._read_sandbox_id_from_request(request)
        if curr_sandbox_id is None:
            return result
        return self._attach_sandbox_update(result, curr_sandbox_id)

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        prev_sandbox_id = self._read_sandbox_id_from_request(request)
        result = await handler(request)
        if prev_sandbox_id is not None:
            return result
        curr_sandbox_id = self._read_sandbox_id_from_request(request)
        if curr_sandbox_id is None:
            return result
        return self._attach_sandbox_update(result, curr_sandbox_id)
