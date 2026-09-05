from __future__ import annotations

import asyncio
from typing import get_type_hints

import pytest
from langchain.agents.middleware import AgentMiddleware
from langchain.tools import ToolRuntime
from langchain_core.messages import HumanMessage, ToolMessage
from langgraph.graph import END
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.runtime import Runtime
from langgraph.types import Command, Overwrite

from deerflow.agents.thread_state import ThreadState
from deerflow.runtime.accepted_invocation import ResolvedAgentMaterialV1
from deerflow.runtime.agent_revision import RESOLVED_AGENT_MATERIAL_CONTEXT_KEY
from deerflow.sandbox.exceptions import SandboxAuthorizationError, SandboxRuntimeError
from deerflow.sandbox.lease import (
    get_sandbox_lease_manager,
    release_sandbox_execution_lease,
    release_sandbox_execution_lease_async,
)
from deerflow.sandbox.middleware import SandboxMiddleware, SandboxMiddlewareState
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import (
    AcceptedSkillSandboxBindingError,
    SandboxProvider,
    get_sandbox_provider,
    reset_sandbox_provider,
    set_sandbox_provider,
)
from deerflow.sandbox.search import GrepMatch
from deerflow.sandbox.tools import ensure_sandbox_initialized, ls_tool


class _SyncProvider(SandboxProvider):
    def __init__(self) -> None:
        self.thread_ids: list[str | None] = []
        self.user_ids: list[str | None] = []

    def acquire(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        self.thread_ids.append(thread_id)
        self.user_ids.append(user_id)
        return "sync-sandbox"

    def get(self, sandbox_id: str) -> Sandbox | None:
        return None

    def release(self, sandbox_id: str) -> None:
        return None


class _AgentSkillSyncProvider(_SyncProvider):
    supports_agent_skill_isolation = True

    def __init__(self) -> None:
        super().__init__()
        self.skill_syncs: list[tuple[str, str, str, object]] = []

    def sync_agent_skills(
        self,
        sandbox_id: str,
        *,
        thread_id: str,
        user_id: str,
        projection,
    ) -> None:
        self.skill_syncs.append((sandbox_id, thread_id, user_id, projection))


class _NetworkPolicyProvider(_SyncProvider):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[dict[str, object]] = []
        self.decisions: list[tuple[str, str, str]] = []
        self.consume_calls: list[str] = []
        self.deny_pending_calls: list[str] = []

    def sandbox_network_mode(self) -> str:
        return "allowlist"

    def consume_network_policy_events(self, sandbox_id: str) -> list[dict[str, object]]:
        self.consume_calls.append(sandbox_id)
        events, self.events = self.events, []
        return events

    def deny_pending_network_policy_events(self, sandbox_id: str) -> bool:
        self.deny_pending_calls.append(sandbox_id)
        for event in self.events:
            request_id = event.get("request_id")
            if isinstance(request_id, str):
                self.decisions.append((sandbox_id, request_id, "deny"))
        self.events = []
        return True

    def decide_network_policy_request(self, sandbox_id: str, request_id: str, decision: str) -> bool:
        self.decisions.append((sandbox_id, request_id, decision))
        return True


class _SandboxStub(Sandbox):
    def execute_command(
        self,
        command: str,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> str:
        del env, timeout
        return "OK"

    def read_file(
        self,
        path: str,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        return "content"

    def download_file(self, path: str) -> bytes:
        return b"content"

    def list_dir(self, path: str, max_depth: int = 2) -> list[str]:
        return ["/mnt/user-data/workspace/file.txt"]

    def write_file(self, path: str, content: str, append: bool = False) -> None:
        return None

    def glob(self, path: str, pattern: str, *, include_dirs: bool = False, max_results: int = 200) -> tuple[list[str], bool]:
        return [], False

    def grep(
        self,
        path: str,
        pattern: str,
        *,
        glob: str | None = None,
        literal: bool = False,
        case_sensitive: bool = False,
        max_results: int = 100,
    ) -> tuple[list[GrepMatch], bool]:
        return [], False

    def update_file(self, path: str, content: bytes) -> None:
        return None


class _AsyncOnlyProvider(SandboxProvider):
    def __init__(self) -> None:
        self.thread_ids: list[str | None] = []
        self.user_ids: list[str | None] = []
        self.released_ids: list[str] = []
        self.sandbox = _SandboxStub("async-sandbox")

    def acquire(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        del user_id
        raise AssertionError("async middleware should not call sync acquire")

    async def acquire_async(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        self.thread_ids.append(thread_id)
        self.user_ids.append(user_id)
        return "async-sandbox"

    def get(self, sandbox_id: str) -> Sandbox | None:
        if sandbox_id == "async-sandbox":
            return self.sandbox
        return None

    def release(self, sandbox_id: str) -> None:
        self.released_ids.append(sandbox_id)
        return None


class _IncompleteAcceptedProvider(_AsyncOnlyProvider):
    """Claims accepted hooks but omits the required isolation advertisement."""

    async def acquire_accepted_skills_async(
        self,
        thread_id: str,
        *,
        user_id: str,
    ) -> str:
        self.thread_ids.append(thread_id)
        self.user_ids.append(user_id)
        return "async-sandbox"

    async def bind_accepted_skill_snapshot_async(self, *args, **kwargs) -> None:
        del args, kwargs


class _AcceptedNamespaceOnlyProvider(_IncompleteAcceptedProvider):
    def has_accepted_skill_isolation(self, sandbox_id: str) -> bool:
        return sandbox_id == "async-sandbox"


class _CapabilityProbeMustNotRunProvider(_AcceptedNamespaceOnlyProvider):
    def accepted_skill_material_capability(self, sandbox_id: str):
        raise AssertionError(f"capability probe must not run for {sandbox_id}")


class _PrepublicationFailureProvider(_AcceptedNamespaceOnlyProvider):
    async def bind_accepted_skill_snapshot_async(self, *args, **kwargs) -> None:
        del args, kwargs
        raise RuntimeError("bind failed before publication")

    def clear_accepted_skill_snapshot(self, clear) -> bool:
        del clear
        return False

    def ensure_accepted_skill_snapshot_absent(self, clear) -> bool:
        del clear
        return True


class _UnprovenPrepublicationFailureProvider(_AcceptedNamespaceOnlyProvider):
    async def bind_accepted_skill_snapshot_async(self, *args, **kwargs) -> None:
        del args, kwargs
        raise RuntimeError("unproven bind failure")

    def clear_accepted_skill_snapshot(self, clear) -> bool:
        del clear
        return False


def test_sandbox_middleware_state_matches_thread_state_sandbox_field() -> None:
    """Middleware-local schema must not drift from ThreadState.sandbox."""
    middleware_hints = get_type_hints(SandboxMiddlewareState, include_extras=True)
    thread_hints = get_type_hints(ThreadState, include_extras=True)

    assert middleware_hints["sandbox"] == thread_hints["sandbox"]


def test_material_capability_probe_is_skipped_for_legacy_and_accepted_empty_runs() -> None:
    from deerflow.sandbox.sandbox_provider import (
        require_runtime_accepted_skill_isolation,
    )

    provider = _CapabilityProbeMustNotRunProvider()
    require_runtime_accepted_skill_isolation(
        provider,
        Runtime(context={}),
        sandbox_id="async-sandbox",
    )
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
    )
    require_runtime_accepted_skill_isolation(
        provider,
        Runtime(context={RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material}),
        sandbox_id="async-sandbox",
    )


@pytest.mark.anyio
async def test_provider_default_acquire_async_offloads_sync_acquire(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = _SyncProvider()
    calls: list[tuple[object, tuple[object, ...]]] = []

    async def fake_to_thread(func, /, *args, **kwargs):
        calls.append((func, args, kwargs))
        return func(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", fake_to_thread)

    sandbox_id = await provider.acquire_async("thread-1")

    assert sandbox_id == "sync-sandbox"
    assert provider.thread_ids == ["thread-1"]
    assert provider.user_ids == [None]
    assert calls == [(provider.acquire, ("thread-1",), {"user_id": None})]


@pytest.mark.anyio
async def test_abefore_agent_uses_async_provider_acquire() -> None:
    provider = _AsyncOnlyProvider()
    set_sandbox_provider(provider)
    try:
        middleware = SandboxMiddleware(lazy_init=False)

        result = await middleware.abefore_agent({}, Runtime(context={"thread_id": "thread-2", "user_id": "owner-2"}))
    finally:
        reset_sandbox_provider()

    assert result == {"sandbox": {"sandbox_id": "async-sandbox"}}
    assert provider.thread_ids == ["thread-2"]
    assert provider.user_ids == ["owner-2"]


def test_explicit_skill_policy_eagerly_acquires_and_syncs_existing_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _AgentSkillSyncProvider()
    projection = object()
    middleware = SandboxMiddleware(lazy_init=True, available_skills=set())
    monkeypatch.setattr(
        middleware,
        "_prepare_agent_skill_projection",
        lambda *_args, **_kwargs: projection,
    )
    set_sandbox_provider(provider)
    try:
        result = middleware.before_agent(
            {"sandbox": {"sandbox_id": "shared-view-sandbox"}},
            Runtime(context={"thread_id": "thread-policy", "user_id": "owner-policy"}),
        )
    finally:
        reset_sandbox_provider()

    assert result is not None
    assert isinstance(result["sandbox"], Overwrite)
    assert result["sandbox"].value == {"sandbox_id": "sync-sandbox"}
    assert provider.thread_ids == ["thread-policy"]
    assert provider.user_ids == ["owner-policy"]
    assert provider.skill_syncs == [("sync-sandbox", "thread-policy", "owner-policy", projection)]


def test_explicit_skill_policy_fails_closed_for_unsupported_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _SyncProvider()
    middleware = SandboxMiddleware(lazy_init=True, available_skills={"allowed"})
    monkeypatch.setattr(
        middleware,
        "_prepare_agent_skill_projection",
        lambda *_args, **_kwargs: object(),
    )
    set_sandbox_provider(provider)
    try:
        with pytest.raises(
            SandboxRuntimeError,
            match="cannot enforce per-Agent skill filesystem isolation",
        ):
            middleware.before_agent(
                {},
                Runtime(context={"thread_id": "thread-policy", "user_id": "owner-policy"}),
            )
    finally:
        reset_sandbox_provider()

    assert provider.thread_ids == []


def test_non_owner_skill_policy_preserves_lazy_init_without_projection_or_acquire(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _SyncProvider()
    middleware = SandboxMiddleware(
        lazy_init=True,
        available_skills={"bootstrap"},
        owns_agent_skill_projection=False,
    )
    prepare_calls: list[tuple[str, str]] = []
    original_prepare = middleware._prepare_agent_skill_projection

    def _prepare(thread_id: str, *, user_id: str):
        prepare_calls.append((thread_id, user_id))
        return original_prepare(thread_id, user_id=user_id)

    monkeypatch.setattr(middleware, "_prepare_agent_skill_projection", _prepare)
    set_sandbox_provider(provider)
    try:
        result = middleware.before_agent(
            {},
            Runtime(
                context={
                    "thread_id": "thread-bootstrap",
                    "user_id": "owner-bootstrap",
                }
            ),
        )
    finally:
        reset_sandbox_provider()

    assert result is None
    assert prepare_calls == [("thread-bootstrap", "owner-bootstrap")]
    assert provider.thread_ids == []


def test_explicit_skill_policy_does_not_reuse_checkpointed_sandbox_after_auth_denial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    provider = _AgentSkillSyncProvider()
    middleware = SandboxMiddleware(lazy_init=True, available_skills=set())
    monkeypatch.setattr(
        middleware,
        "_prepare_agent_skill_projection",
        lambda *_args, **_kwargs: object(),
    )
    monkeypatch.setattr(
        "deerflow.sandbox.middleware.authorize_sandbox_execution",
        lambda **_kwargs: (_ for _ in ()).throw(SandboxAuthorizationError("denied")),
    )
    set_sandbox_provider(provider)
    try:
        with pytest.raises(SandboxAuthorizationError, match="denied"):
            middleware.before_agent(
                {"sandbox": {"sandbox_id": "shared-view-sandbox"}},
                Runtime(
                    context={
                        "thread_id": "thread-policy",
                        "user_id": "owner-policy",
                    }
                ),
            )
    finally:
        reset_sandbox_provider()

    assert provider.thread_ids == []
    assert provider.skill_syncs == []


@pytest.mark.anyio
async def test_accepted_empty_skill_set_fails_closed_for_unsupported_provider() -> None:
    provider = _AsyncOnlyProvider()
    set_sandbox_provider(provider)
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
    )
    runtime = Runtime(
        context={
            "thread_id": "thread-accepted",
            "run_id": "run-accepted",
            "user_id": "owner-accepted",
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material,
        }
    )
    try:
        with pytest.raises(
            AcceptedSkillSandboxBindingError,
            match="accepted_skill_snapshot_projection_unsupported",
        ):
            await SandboxMiddleware(lazy_init=True).abefore_agent({}, runtime)
    finally:
        reset_sandbox_provider()

    # Durable accepted material selects the explicit accepted-only acquisition
    # profile. Unsupported providers fail before creating a sandbox that might
    # expose mutable live skill mounts.
    assert provider.thread_ids == []
    assert provider.released_ids == []


@pytest.mark.anyio
async def test_accepted_acquisition_requires_provider_isolation_advertisement() -> None:
    provider = _IncompleteAcceptedProvider()
    set_sandbox_provider(provider)
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
    )
    runtime = Runtime(
        context={
            "thread_id": "thread-incomplete",
            "run_id": "run-incomplete",
            "user_id": "owner-incomplete",
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material,
        }
    )
    try:
        with pytest.raises(
            AcceptedSkillSandboxBindingError,
            match="accepted_skill_snapshot_isolation_unverified",
        ):
            await SandboxMiddleware(lazy_init=True).abefore_agent({}, runtime)
    finally:
        reset_sandbox_provider()

    assert provider.thread_ids == ["thread-incomplete"]
    assert provider.released_ids == ["async-sandbox"]


@pytest.mark.anyio
async def test_bind_failure_before_publication_releases_after_absence_proof() -> None:
    from deerflow.runtime.skill_projection import get_skill_projection_coordinator

    provider = _PrepublicationFailureProvider()
    set_sandbox_provider(provider)
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
    )
    runtime = Runtime(
        context={
            "thread_id": "thread-prepublication-failure",
            "run_id": "run-prepublication-failure",
            "user_id": "owner-prepublication-failure",
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material,
        }
    )
    coordinator = get_skill_projection_coordinator()
    try:
        with pytest.raises(RuntimeError, match="bind failed before publication"):
            await SandboxMiddleware(lazy_init=True).abefore_agent({}, runtime)

        assert provider.released_ids == ["async-sandbox"]
        assert not coordinator.is_busy(
            user_id="owner-prepublication-failure",
            thread_id="thread-prepublication-failure",
        )
        assert coordinator.try_claim_committed_run(
            user_id="owner-prepublication-failure",
            thread_id="thread-prepublication-failure",
            run_id="replacement-after-prepublication-failure",
            snapshot_id=None,
        )
    finally:
        coordinator.release_unactivated_run(
            user_id="owner-prepublication-failure",
            thread_id="thread-prepublication-failure",
            run_id="replacement-after-prepublication-failure",
        )
        reset_sandbox_provider()


@pytest.mark.anyio
async def test_bind_failure_without_absence_proof_does_not_release_sandbox() -> None:
    from deerflow.runtime.skill_projection import get_skill_projection_coordinator

    provider = _UnprovenPrepublicationFailureProvider()
    set_sandbox_provider(provider)
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
    )
    runtime = Runtime(
        context={
            "thread_id": "thread-unproven-failure",
            "run_id": "run-unproven-failure",
            "user_id": "owner-unproven-failure",
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material,
        }
    )
    coordinator = get_skill_projection_coordinator()
    try:
        with pytest.raises(RuntimeError, match="unproven bind failure"):
            await SandboxMiddleware(lazy_init=True).abefore_agent({}, runtime)

        assert provider.released_ids == []
        assert coordinator.is_busy(
            user_id="owner-unproven-failure",
            thread_id="thread-unproven-failure",
        )
    finally:
        token = coordinator.token_for_consumer(
            user_id="owner-unproven-failure",
            thread_id="thread-unproven-failure",
            run_id="run-unproven-failure",
            consumer_id="run:run-unproven-failure:lead",
        )
        if token is not None:
            clear = coordinator.release(token)
            if clear is not None:
                coordinator.finalize_release(clear)
        reset_sandbox_provider()


@pytest.mark.anyio
async def test_nonempty_accepted_material_requires_hard_read_only_provider(
    tmp_path,
    monkeypatch,
) -> None:
    from pathlib import Path

    from deerflow.config.paths import Paths
    from deerflow.runtime.skill_snapshot import snapshot_effective_skills
    from deerflow.skills.parser import parse_skill_file
    from deerflow.skills.types import SkillCategory

    source = tmp_path / "source" / "immutable-skill"
    source.mkdir(parents=True)
    skill_file = source / "SKILL.md"
    skill_file.write_text(
        "---\nname: immutable-skill\ndescription: immutable\n---\naccepted bytes\n",
        encoding="utf-8",
    )
    skill = parse_skill_file(
        skill_file,
        SkillCategory.CUSTOM,
        relative_path=Path("immutable-skill"),
    )
    assert skill is not None
    paths = Paths(base_dir=tmp_path / "state")
    monkeypatch.setattr("deerflow.runtime.skill_snapshot.get_paths", lambda: paths)
    snapshot = snapshot_effective_skills((skill,), user_id="owner-read-only")
    assert snapshot is not None
    material = ResolvedAgentMaterialV1(
        agent_id="lead-agent",
        storage_source="test",
        storage_version="1",
        agent_config=None,
        soul="",
        model_profile={},
        skill_snapshot=snapshot,
        enabled_skill_objects=snapshot.skills,
        all_skill_objects=snapshot.skills,
    )
    provider = _AcceptedNamespaceOnlyProvider()
    set_sandbox_provider(provider)
    runtime = Runtime(
        context={
            "thread_id": "thread-read-only",
            "run_id": "run-read-only",
            "user_id": "owner-read-only",
            RESOLVED_AGENT_MATERIAL_CONTEXT_KEY: material,
        }
    )
    try:
        with pytest.raises(
            AcceptedSkillSandboxBindingError,
            match="accepted_skill_snapshot_immutability_unsupported",
        ):
            await SandboxMiddleware(lazy_init=True).abefore_agent({}, runtime)
    finally:
        reset_sandbox_provider()
        snapshot.release()

    assert provider.released_ids == ["async-sandbox"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("middleware", "state", "runtime"),
    [
        (SandboxMiddleware(lazy_init=True), {}, Runtime(context={"thread_id": "thread-lazy"})),
        (SandboxMiddleware(lazy_init=False), {}, Runtime(context={})),
        (SandboxMiddleware(lazy_init=False), {"sandbox": {"sandbox_id": "existing"}}, Runtime(context={"thread_id": "thread-existing"})),
    ],
)
async def test_abefore_agent_delegates_to_super_when_not_acquiring(
    monkeypatch: pytest.MonkeyPatch,
    middleware: SandboxMiddleware,
    state: dict,
    runtime: Runtime,
) -> None:
    calls: list[tuple[dict, Runtime]] = []
    provider = _AsyncOnlyProvider()

    async def fake_super_abefore_agent(self, state_arg, runtime_arg):
        calls.append((state_arg, runtime_arg))
        return {"delegated": True}

    monkeypatch.setattr(AgentMiddleware, "abefore_agent", fake_super_abefore_agent)
    set_sandbox_provider(provider)
    try:
        result = await middleware.abefore_agent(state, runtime)
    finally:
        reset_sandbox_provider()

    assert result == {"delegated": True}
    assert calls == [(state, runtime)]


def test_shared_subagents_release_provider_only_after_last_execution() -> None:
    """A child finishing must not park the sandbox under a running sibling (#5128)."""
    provider = _AsyncOnlyProvider()
    state = {"sandbox": {"sandbox_id": "async-sandbox"}}
    first_runtime = Runtime(
        context={
            "thread_id": "shared-thread",
            "user_id": "shared-user",
            "is_subagent": True,
        }
    )
    second_runtime = Runtime(
        context={
            "thread_id": "shared-thread",
            "user_id": "shared-user",
            "is_subagent": True,
        }
    )
    middleware = SandboxMiddleware()
    set_sandbox_provider(provider)
    try:
        middleware.before_agent(state, first_runtime)
        middleware.before_agent(state, second_runtime)
        for runtime in (first_runtime, second_runtime):
            ensure_sandbox_initialized(
                ToolRuntime(
                    state=state,
                    context=runtime.context,
                    config={"configurable": {}},
                    stream_writer=lambda _: None,
                    tools=[],
                    tool_call_id="call-1",
                    store=None,
                )
            )

        middleware.after_agent(state, first_runtime)
        assert provider.released_ids == []

        middleware.after_agent(state, second_runtime)
        assert provider.released_ids == ["async-sandbox"]
    finally:
        reset_sandbox_provider()


@pytest.mark.anyio
async def test_default_lazy_tool_acquisition_uses_async_provider() -> None:
    provider = _AsyncOnlyProvider()
    set_sandbox_provider(provider)
    try:
        runtime = ToolRuntime(
            state={},
            context={"thread_id": "thread-lazy", "user_id": "owner-lazy"},
            config={"configurable": {}},
            stream_writer=lambda _: None,
            tools=[],
            tool_call_id="call-1",
            store=None,
        )

        result = await ls_tool.ainvoke({"runtime": runtime, "description": "list workspace", "path": "/mnt/user-data/workspace"})
    finally:
        reset_sandbox_provider()

    assert result == "/mnt/user-data/workspace/file.txt"
    assert provider.thread_ids == ["thread-lazy"]
    assert provider.user_ids == ["owner-lazy"]
    assert runtime.state["sandbox"] == {"sandbox_id": "async-sandbox"}
    assert runtime.context["sandbox_id"] == "async-sandbox"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("state", "runtime", "expected_sandbox_id"),
    [
        ({"sandbox": {"sandbox_id": "state-sandbox"}}, Runtime(context={}), "state-sandbox"),
        ({}, Runtime(context={"sandbox_id": "context-sandbox"}), "context-sandbox"),
    ],
)
async def test_aafter_agent_releases_sandbox_off_thread(
    monkeypatch: pytest.MonkeyPatch,
    state: dict,
    runtime: Runtime,
    expected_sandbox_id: str,
) -> None:
    provider = _AsyncOnlyProvider()
    to_thread_calls: list[tuple[object, tuple[object, ...]]] = []

    async def fake_to_thread(func, /, *args):
        to_thread_calls.append((func, args))
        return func(*args)

    monkeypatch.setattr(asyncio, "to_thread", fake_to_thread)
    set_sandbox_provider(provider)
    try:
        installed = get_sandbox_provider()
        result = await SandboxMiddleware().aafter_agent(state, runtime)
    finally:
        reset_sandbox_provider()

    assert result is None
    assert provider.released_ids == [expected_sandbox_id]
    # The installed provider is the session provider in front of the fake; its
    # release forwards to the fake's, and that is the callable handed to to_thread.
    assert to_thread_calls == [(installed.release, (expected_sandbox_id,))]


@pytest.mark.anyio
async def test_aafter_agent_delegates_to_super_when_no_sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[dict, Runtime]] = []

    async def fake_super_aafter_agent(self, state_arg, runtime_arg):
        calls.append((state_arg, runtime_arg))
        return {"delegated": True}

    monkeypatch.setattr(AgentMiddleware, "aafter_agent", fake_super_aafter_agent)

    state = {}
    runtime = Runtime(context={})
    result = await SandboxMiddleware().aafter_agent(state, runtime)

    assert result == {"delegated": True}
    assert calls == [(state, runtime)]


def test_after_agent_unwraps_overwrite_sandbox_state() -> None:
    """Fork-restored state may carry the sandbox channel Overwrite-wrapped."""
    provider = _AsyncOnlyProvider()
    set_sandbox_provider(provider)
    try:
        state = {"sandbox": Overwrite({"sandbox_id": "fork-restored"})}
        result = SandboxMiddleware().after_agent(state, Runtime(context={}))
    finally:
        reset_sandbox_provider()

    assert result is None
    # The wrapped value replays the parent's sandbox; this run must not release it.
    assert provider.released_ids == []


def test_after_agent_releases_own_sandbox_state() -> None:
    provider = _AsyncOnlyProvider()
    set_sandbox_provider(provider)
    try:
        state = {"sandbox": {"sandbox_id": "own-sandbox"}}
        result = SandboxMiddleware().after_agent(state, Runtime(context={}))
    finally:
        reset_sandbox_provider()

    assert result is None
    assert provider.released_ids == ["own-sandbox"]


@pytest.mark.anyio
async def test_aafter_agent_unwraps_overwrite_sandbox_state() -> None:
    provider = _AsyncOnlyProvider()
    set_sandbox_provider(provider)
    try:
        state = {"sandbox": Overwrite({"sandbox_id": "fork-restored"})}
        result = await SandboxMiddleware().aafter_agent(state, Runtime(context={}))
    finally:
        reset_sandbox_provider()

    assert result is None
    assert provider.released_ids == []


@pytest.mark.anyio
async def test_aafter_agent_releases_own_sandbox_state() -> None:
    provider = _AsyncOnlyProvider()
    set_sandbox_provider(provider)
    try:
        state = {"sandbox": {"sandbox_id": "own-sandbox"}}
        result = await SandboxMiddleware().aafter_agent(state, Runtime(context={}))
    finally:
        reset_sandbox_provider()

    assert result is None
    assert provider.released_ids == ["own-sandbox"]


# ---------------------------------------------------------------------------
# wrap_tool_call / awrap_tool_call: persistent sandbox state via Command
# ---------------------------------------------------------------------------


def _make_tool_call_request(state: dict) -> ToolCallRequest:
    """Build a minimal ToolCallRequest backed by a real ToolRuntime."""
    runtime = ToolRuntime(
        state=state,
        context={},
        config={"configurable": {}},
        stream_writer=lambda _: None,
        tools=[],
        tool_call_id="call-1",
        store=None,
    )
    return ToolCallRequest(
        tool_call={"id": "call-1", "name": "bash", "args": {}},
        tool=None,
        state=state,
        runtime=runtime,
    )


def test_wrap_tool_call_emits_command_when_lazy_init_happens() -> None:
    middleware = SandboxMiddleware()
    state: dict = {}
    request = _make_tool_call_request(state)

    def handler(req: ToolCallRequest) -> ToolMessage:
        # Simulate ensure_sandbox_initialized() mutating runtime.state in-place.
        req.runtime.state["sandbox"] = {"sandbox_id": "new-sandbox"}
        return ToolMessage(content="ok", tool_call_id="call-1", name="bash")

    result = middleware.wrap_tool_call(request, handler)

    assert isinstance(result, Command)
    assert isinstance(result.update, dict)
    assert result.update["sandbox"] == {"sandbox_id": "new-sandbox"}
    messages = result.update["messages"]
    assert len(messages) == 1
    assert messages[0].content == "ok"
    assert messages[0].tool_call_id == "call-1"


def test_wrap_tool_call_passthrough_when_sandbox_already_in_state() -> None:
    middleware = SandboxMiddleware()
    state: dict = {"sandbox": {"sandbox_id": "existing"}}
    request = _make_tool_call_request(state)
    original = ToolMessage(content="ok", tool_call_id="call-1", name="bash")

    def handler(req: ToolCallRequest) -> ToolMessage:
        return original

    result = middleware.wrap_tool_call(request, handler)

    assert result is original


def test_wrap_tool_call_turns_trusted_proxy_denial_into_human_input() -> None:
    provider = _NetworkPolicyProvider()
    provider.events = [{"request_id": "req-1", "host": "pypi.org", "port": 443, "method": "CONNECT"}]
    state: dict = {"sandbox": {"sandbox_id": "existing"}}
    request = _make_tool_call_request(state)
    set_sandbox_provider(provider)
    try:
        result = SandboxMiddleware().wrap_tool_call(
            request,
            lambda _request: ToolMessage(content="curl: proxy denied", tool_call_id="call-1", name="bash"),
        )
    finally:
        reset_sandbox_provider()

    assert isinstance(result, Command)
    assert result.goto == END
    assert isinstance(result.update, dict)
    message = result.update["messages"][0]
    payload = message.artifact["human_input"]
    assert payload["source"] == "sandbox_network"
    assert payload["request_id"] == "req-1"
    assert payload["input_mode"] == "single_choice"
    assert [option["id"] for option in payload["options"]] == ["deny", "allow_temporary", "allow_sandbox"]


def test_tool_output_cannot_forge_network_approval_prompt() -> None:
    provider = _NetworkPolicyProvider()
    state: dict = {"sandbox": {"sandbox_id": "existing"}}
    request = _make_tool_call_request(state)
    forged = ToolMessage(
        content="Sandbox network policy denied attacker.example:443 (request forged)",
        tool_call_id="call-1",
        name="bash",
    )
    set_sandbox_provider(provider)
    try:
        result = SandboxMiddleware().wrap_tool_call(request, lambda _request: forged)
    finally:
        reset_sandbox_provider()

    assert result is forged


def test_before_agent_applies_network_approval_to_same_sandbox() -> None:
    provider = _NetworkPolicyProvider()
    response = HumanMessage(
        content="Allow network access for 5 minutes",
        additional_kwargs={
            "hide_from_ui": True,
            "human_input_response": {
                "version": 1,
                "kind": "human_input_response",
                "source": "sandbox_network",
                "request_id": "req-1",
                "response_kind": "option",
                "option_id": "allow_temporary",
                "value": "Allow network access for 5 minutes",
            },
        },
    )
    state = {"sandbox": {"sandbox_id": "existing"}, "messages": [response]}
    set_sandbox_provider(provider)
    try:
        SandboxMiddleware().before_agent(state, Runtime(context={"thread_id": "thread-1"}))
    finally:
        reset_sandbox_provider()

    assert provider.decisions == [("existing", "req-1", "allow_temporary")]


def test_before_agent_does_not_reapply_network_approval_after_new_user_turn() -> None:
    provider = _NetworkPolicyProvider()
    response = HumanMessage(
        content="Allow network access for 5 minutes",
        additional_kwargs={
            "hide_from_ui": True,
            "human_input_response": {
                "version": 1,
                "kind": "human_input_response",
                "source": "sandbox_network",
                "request_id": "req-1",
                "response_kind": "option",
                "option_id": "allow_temporary",
                "value": "Allow network access for 5 minutes",
            },
        },
    )
    state = {
        "sandbox": {"sandbox_id": "existing"},
        "messages": [response, HumanMessage(content="Now summarize the result")],
    }
    set_sandbox_provider(provider)
    try:
        SandboxMiddleware().before_agent(state, Runtime(context={"thread_id": "thread-1"}))
    finally:
        reset_sandbox_provider()

    assert provider.decisions == []


@pytest.mark.parametrize("context_key", ["disable_clarification", "non_interactive"])
def test_sync_noninteractive_network_denial_is_recorded_without_prompt(context_key: str) -> None:
    provider = _NetworkPolicyProvider()
    provider.events = [{"request_id": "req-1", "host": "example.com", "port": 443, "method": "CONNECT"}]
    state: dict = {"sandbox": {"sandbox_id": "existing"}}
    request = _make_tool_call_request(state)
    request.runtime.context[context_key] = True
    original = ToolMessage(content="proxy denied", tool_call_id="call-1", name="bash")
    set_sandbox_provider(provider)
    try:
        result = SandboxMiddleware().wrap_tool_call(request, lambda _request: original)
    finally:
        reset_sandbox_provider()

    assert result is original
    assert provider.decisions == [("existing", "req-1", "deny")]
    assert provider.deny_pending_calls == ["existing"]
    assert provider.consume_calls == []


@pytest.mark.anyio
@pytest.mark.parametrize("context_key", ["disable_clarification", "non_interactive"])
async def test_async_noninteractive_network_denial_is_recorded_without_prompt(context_key: str) -> None:
    provider = _NetworkPolicyProvider()
    provider.events = [{"request_id": "req-1", "host": "example.com", "port": 443, "method": "CONNECT"}]
    state: dict = {"sandbox": {"sandbox_id": "existing"}}
    request = _make_tool_call_request(state)
    request.runtime.context[context_key] = True
    original = ToolMessage(content="proxy denied", tool_call_id="call-1", name="bash")

    async def handler(_request: ToolCallRequest) -> ToolMessage:
        return original

    set_sandbox_provider(provider)
    try:
        result = await SandboxMiddleware().awrap_tool_call(request, handler)
    finally:
        reset_sandbox_provider()

    assert result is original
    assert provider.decisions == [("existing", "req-1", "deny")]
    assert provider.deny_pending_calls == ["existing"]
    assert provider.consume_calls == []


def test_subagent_network_denial_fails_closed_without_prompt() -> None:
    provider = _NetworkPolicyProvider()
    provider.events = [{"request_id": "req-1", "host": "example.com", "port": 443, "method": "CONNECT"}]
    state: dict = {"sandbox": {"sandbox_id": "existing"}}
    request = _make_tool_call_request(state)
    request.runtime.context["is_subagent"] = True
    original = ToolMessage(content="proxy denied", tool_call_id="call-1", name="bash")
    set_sandbox_provider(provider)
    try:
        result = SandboxMiddleware().wrap_tool_call(request, lambda _request: original)
    finally:
        reset_sandbox_provider()

    assert result is original
    assert provider.events == []
    assert provider.decisions == [("existing", "req-1", "deny")]
    assert provider.deny_pending_calls == ["existing"]
    assert provider.consume_calls == []


@pytest.mark.anyio
async def test_async_subagent_network_denial_fails_closed_without_prompt() -> None:
    provider = _NetworkPolicyProvider()
    provider.events = [{"request_id": "req-1", "host": "example.com", "port": 443, "method": "CONNECT"}]
    state: dict = {"sandbox": {"sandbox_id": "existing"}}
    request = _make_tool_call_request(state)
    request.runtime.context["is_subagent"] = True
    original = ToolMessage(content="proxy denied", tool_call_id="call-1", name="bash")

    async def handler(_request: ToolCallRequest) -> ToolMessage:
        return original

    set_sandbox_provider(provider)
    try:
        result = await SandboxMiddleware().awrap_tool_call(request, handler)
    finally:
        reset_sandbox_provider()

    assert result is original
    assert provider.events == []
    assert provider.decisions == [("existing", "req-1", "deny")]
    assert provider.deny_pending_calls == ["existing"]
    assert provider.consume_calls == []


def test_noninteractive_network_denial_atomically_drains_more_than_sixteen_hosts() -> None:
    provider = _NetworkPolicyProvider()
    provider.events = [{"request_id": f"req-{index}", "host": f"host-{index}.example", "port": 443, "method": "CONNECT"} for index in range(17)]
    state: dict = {"sandbox": {"sandbox_id": "existing"}}
    request = _make_tool_call_request(state)
    request.runtime.context["non_interactive"] = True
    original = ToolMessage(content="proxy denied", tool_call_id="call-1", name="bash")
    set_sandbox_provider(provider)
    try:
        result = SandboxMiddleware().wrap_tool_call(request, lambda _request: original)
    finally:
        reset_sandbox_provider()

    assert result is original
    assert provider.events == []
    assert len(provider.decisions) == 17
    assert provider.deny_pending_calls == ["existing"]
    assert provider.consume_calls == []


def test_wrap_tool_call_passthrough_when_handler_did_not_initialize_sandbox() -> None:
    middleware = SandboxMiddleware()
    state: dict = {}
    request = _make_tool_call_request(state)
    original = ToolMessage(content="ok", tool_call_id="call-1", name="bash")

    def handler(req: ToolCallRequest) -> ToolMessage:
        return original

    result = middleware.wrap_tool_call(request, handler)

    assert result is original


def test_wrap_tool_call_merges_with_existing_command_update() -> None:
    middleware = SandboxMiddleware()
    state: dict = {}
    request = _make_tool_call_request(state)
    tool_msg = ToolMessage(content="ok", tool_call_id="call-1", name="bash")

    def handler(req: ToolCallRequest) -> Command:
        req.runtime.state["sandbox"] = {"sandbox_id": "new-sandbox"}
        return Command(
            update={
                "messages": [tool_msg],
                "viewed_images": {"a.png": {"mime_type": "image/png", "size": 1, "actual_path": "/tmp/a.png"}},
            },
            goto="next-node",
        )

    result = middleware.wrap_tool_call(request, handler)

    assert isinstance(result, Command)
    assert result.goto == "next-node"
    assert isinstance(result.update, dict)
    assert result.update["messages"] == [tool_msg]
    assert result.update["viewed_images"] == {"a.png": {"mime_type": "image/png", "size": 1, "actual_path": "/tmp/a.png"}}
    assert result.update["sandbox"] == {"sandbox_id": "new-sandbox"}


def test_wrap_tool_call_does_not_override_non_dict_update() -> None:
    middleware = SandboxMiddleware()
    state: dict = {}
    request = _make_tool_call_request(state)
    cmd = Command(update=[("messages", [ToolMessage(content="x", tool_call_id="c", name="bash")])])

    def handler(req: ToolCallRequest) -> Command:
        req.runtime.state["sandbox"] = {"sandbox_id": "new-sandbox"}
        return cmd

    result = middleware.wrap_tool_call(request, handler)

    # Non-dict update is left untouched to avoid silent data loss.
    assert result is cmd


def test_wrap_tool_call_defers_terminal_lease_release_to_outer_run_fence() -> None:
    provider = _AsyncOnlyProvider()
    owner_id = "agent:terminal"
    state: dict = {"sandbox": {"sandbox_id": "async-sandbox"}}
    request = _make_tool_call_request(state)
    request.runtime.context.update(
        thread_id="thread-1",
        user_id="user-1",
        sandbox_lease_owner_id=owner_id,
        sandbox_id="async-sandbox",
    )
    set_sandbox_provider(provider)
    try:
        get_sandbox_lease_manager(provider).retain(
            owner_id,
            "async-sandbox",
            thread_id="thread-1",
            user_id="user-1",
        )
        result = SandboxMiddleware().wrap_tool_call(
            request,
            lambda _: Command(goto=END),
        )
        assert provider.released_ids == []
        release_sandbox_execution_lease(request.runtime.context)
    finally:
        reset_sandbox_provider()

    assert isinstance(result, Command)
    assert provider.released_ids == ["async-sandbox"]


@pytest.mark.anyio
async def test_awrap_tool_call_emits_command_when_lazy_init_happens() -> None:
    middleware = SandboxMiddleware()
    state: dict = {}
    request = _make_tool_call_request(state)

    async def handler(req: ToolCallRequest) -> ToolMessage:
        req.runtime.state["sandbox"] = {"sandbox_id": "async-new"}
        return ToolMessage(content="ok", tool_call_id="call-1", name="bash")

    result = await middleware.awrap_tool_call(request, handler)

    assert isinstance(result, Command)
    assert isinstance(result.update, dict)
    assert result.update["sandbox"] == {"sandbox_id": "async-new"}
    messages = result.update["messages"]
    assert len(messages) == 1
    assert messages[0].content == "ok"


@pytest.mark.anyio
async def test_awrap_tool_call_passthrough_when_sandbox_already_in_state() -> None:
    middleware = SandboxMiddleware()
    state: dict = {"sandbox": {"sandbox_id": "existing"}}
    request = _make_tool_call_request(state)
    original = ToolMessage(content="ok", tool_call_id="call-1", name="bash")

    async def handler(req: ToolCallRequest) -> ToolMessage:
        return original

    result = await middleware.awrap_tool_call(request, handler)

    assert result is original


@pytest.mark.anyio
async def test_awrap_tool_call_defers_terminal_lease_release_to_outer_run_fence() -> None:
    provider = _AsyncOnlyProvider()
    owner_id = "agent:async-terminal"
    state: dict = {"sandbox": {"sandbox_id": "async-sandbox"}}
    request = _make_tool_call_request(state)
    request.runtime.context.update(
        thread_id="thread-1",
        user_id="user-1",
        sandbox_lease_owner_id=owner_id,
        sandbox_id="async-sandbox",
    )
    set_sandbox_provider(provider)
    try:
        get_sandbox_lease_manager(provider).retain(
            owner_id,
            "async-sandbox",
            thread_id="thread-1",
            user_id="user-1",
        )
        result = await SandboxMiddleware().awrap_tool_call(
            request,
            lambda _: asyncio.sleep(0, result=Command(goto=END)),
        )
        assert provider.released_ids == []
        await release_sandbox_execution_lease_async(request.runtime.context)
    finally:
        reset_sandbox_provider()

    assert isinstance(result, Command)
    assert provider.released_ids == ["async-sandbox"]


@pytest.mark.anyio
async def test_parallel_terminal_command_does_not_release_while_sibling_handler_runs() -> None:
    provider = _AsyncOnlyProvider()
    owner_id = "agent:parallel-terminal"
    state: dict = {"sandbox": {"sandbox_id": "async-sandbox"}}
    request = _make_tool_call_request(state)
    request.runtime.context.update(
        thread_id="thread-1",
        user_id="user-1",
        sandbox_lease_owner_id=owner_id,
        sandbox_id="async-sandbox",
    )
    sibling_started = asyncio.Event()
    allow_sibling_finish = asyncio.Event()

    async def sibling_handler(_: ToolCallRequest) -> ToolMessage:
        sibling_started.set()
        await allow_sibling_finish.wait()
        return ToolMessage(content="done", tool_call_id="call-2", name="bash")

    async def terminal_handler(_: ToolCallRequest) -> Command:
        await sibling_started.wait()
        return Command(goto=END)

    set_sandbox_provider(provider)
    try:
        get_sandbox_lease_manager(provider).retain(
            owner_id,
            "async-sandbox",
            thread_id="thread-1",
            user_id="user-1",
        )
        middleware = SandboxMiddleware()
        sibling_task = asyncio.create_task(middleware.awrap_tool_call(request, sibling_handler))
        terminal_task = asyncio.create_task(middleware.awrap_tool_call(request, terminal_handler))

        terminal_result = await terminal_task

        assert isinstance(terminal_result, Command)
        assert not sibling_task.done()
        assert provider.released_ids == []

        allow_sibling_finish.set()
        sibling_result = await sibling_task
        assert isinstance(sibling_result, ToolMessage)
        assert provider.released_ids == []

        await release_sandbox_execution_lease_async(request.runtime.context)
    finally:
        reset_sandbox_provider()

    assert provider.released_ids == ["async-sandbox"]


def test_wrap_tool_call_preserves_existing_command_fields_when_merging() -> None:
    """Regression: when merging sandbox_update into an existing Command,
    all other Command fields (e.g. graph, goto, resume) must be preserved.
    """
    middleware = SandboxMiddleware()
    state: dict = {}
    request = _make_tool_call_request(state)

    def handler(req: ToolCallRequest) -> Command:
        req.runtime.state["sandbox"] = {"sandbox_id": "sbx-merge"}
        return Command(
            update={"existing_key": "existing_value"},
            graph="parent",
            goto="next_node",
            resume="resume-token",
        )

    result = middleware.wrap_tool_call(request, handler)

    assert isinstance(result, Command)
    assert result.update == {
        "existing_key": "existing_value",
        "sandbox": {"sandbox_id": "sbx-merge"},
    }
    # Critical: other Command fields must NOT be dropped by the merge.
    assert result.graph == "parent"
    assert result.goto == "next_node"
    assert result.resume == "resume-token"
