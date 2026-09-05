"""The session provider is the single resolution point for sandbox handles.

Every path that resolves a sandbox already goes through the provider's
acquire, get, and release. The session provider wraps the configured provider
and dispatches those verbs by the executing session's declared kind: a
declared execution gets its own public ref and handle, a stranger never sees a
public ref, and an ordinary acquire cannot attach to a mount scope held by a
retire-terminal session.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from deerflow.sandbox import sandbox_provider as provider_module
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import (
    SandboxProvider,
    get_sandbox_provider,
    reset_sandbox_provider,
    set_sandbox_provider,
)
from deerflow.sandbox.session import (
    SandboxSessionConflict,
    SandboxSessionDeclaration,
    SandboxSessionKind,
    SandboxSessionRegistry,
    SandboxSessionTerminal,
    SessionProvider,
    bind_sandbox_session,
    current_sandbox_session,
    sandbox_session_provider,
    unwrap_sandbox_provider,
)


class _Handle(Sandbox):
    def execute_command(self, command, env=None, timeout=None):
        return f"ran:{command}"

    def read_file(self, path, start_line=None, end_line=None):
        return ""

    def download_file(self, path):
        return b""

    def list_dir(self, path, max_depth=2):
        return []

    def write_file(self, path, content, append=False):
        return None

    def glob(self, path, pattern, *, include_dirs=False, max_results=200):
        return ([], False)

    def grep(self, path, pattern, *, glob=None, literal=False, case_sensitive=False, max_results=100):
        return ([], False)

    def update_file(self, path, content):
        return None


class _Backing(SandboxProvider):
    uses_thread_data_mounts = True
    supports_agent_skill_isolation = True

    def __init__(self) -> None:
        self.calls: list[tuple[object, ...]] = []
        self.sandboxes: dict[str, Sandbox] = {}

    def _mint(self, thread_id, user_id) -> str:
        sandbox_id = f"box:{user_id}:{thread_id}"
        self.sandboxes[sandbox_id] = _Handle(sandbox_id)
        return sandbox_id

    def acquire(self, thread_id=None, *, user_id=None):
        self.calls.append(("acquire", thread_id, user_id))
        return self._mint(thread_id, user_id)

    async def acquire_async(self, thread_id=None, *, user_id=None):
        self.calls.append(("acquire_async", thread_id, user_id))
        return self._mint(thread_id, user_id)

    def get(self, sandbox_id):
        self.calls.append(("get", sandbox_id))
        return self.sandboxes.get(sandbox_id)

    def release(self, sandbox_id):
        self.calls.append(("release", sandbox_id))

    def reset(self):
        self.calls.append(("reset",))

    def has_accepted_skill_isolation(self, sandbox_id):
        return sandbox_id.endswith("-accepted")

    async def sync_agent_skills_async(self, sandbox_id, *, thread_id, user_id, projection=None):
        self.calls.append(("sync_agent_skills_async", sandbox_id))

    def custom_extra(self, value):
        return ("extra", value)

    def destroy(self, sandbox_id):
        self.calls.append(("destroy", sandbox_id))


def _declaration(*, public_ref="accepted-session-abc", mount_scope=("user-1", "thread-1"), live=True):
    state = {"live": live, "retired": 0}

    def retire() -> None:
        state["retired"] += 1
        state["live"] = False

    declaration = SandboxSessionDeclaration(
        public_ref=public_ref,
        mount_scope=mount_scope,
        kind=SandboxSessionKind.ACCEPTED,
        terminal=SandboxSessionTerminal.RETIRE,
        handle=_Handle(public_ref),
        is_live=lambda: state["live"],
        retire=retire,
    )
    return declaration, state


@pytest.fixture
def stack():
    backing = _Backing()
    registry = SandboxSessionRegistry()
    return backing, registry, SessionProvider(backing, registry=registry)


def test_declared_execution_acquires_its_public_ref_without_provisioning(stack):
    backing, registry, provider = stack
    declaration, _ = _declaration()
    registry.declare(declaration)
    with bind_sandbox_session(declaration):
        assert provider.acquire("thread-1", user_id="user-1") == "accepted-session-abc"
        assert provider.get("accepted-session-abc") is declaration.handle
    assert backing.calls == []


@pytest.mark.asyncio
async def test_declared_execution_acquires_async_without_provisioning(stack):
    backing, registry, provider = stack
    declaration, _ = _declaration()
    registry.declare(declaration)
    with bind_sandbox_session(declaration):
        assert await provider.acquire_async("thread-1", user_id="user-1") == "accepted-session-abc"
    assert backing.calls == []


def test_declared_execution_whose_session_died_fails_typed_never_ordinary(stack):
    backing, registry, provider = stack
    declaration, state = _declaration()
    registry.declare(declaration)
    state["live"] = False
    with bind_sandbox_session(declaration):
        with pytest.raises(SandboxSessionConflict, match="no longer live"):
            provider.acquire("thread-1", user_id="user-1")
        assert provider.get("accepted-session-abc") is None
    assert backing.calls == []


def test_public_ref_resolves_only_for_the_declaring_execution(stack):
    backing, registry, provider = stack
    declaration, _ = _declaration()
    registry.declare(declaration)
    assert provider.get("accepted-session-abc") is None
    other, _ = _declaration(public_ref="accepted-session-other", mount_scope=("user-2", "thread-2"))
    registry.declare(other)
    with bind_sandbox_session(other):
        assert provider.get("accepted-session-abc") is None
        assert provider.get("accepted-session-other") is other.handle
    assert backing.calls == []


def test_undeclared_acquire_on_a_retire_terminal_mount_scope_is_refused(stack):
    backing, registry, provider = stack
    declaration, _ = _declaration()
    registry.declare(declaration)
    with pytest.raises(SandboxSessionConflict) as excinfo:
        provider.acquire("thread-1", user_id="user-1")
    assert excinfo.value.code == "sandbox_session_conflict"
    assert excinfo.value.mount_scope == ("user-1", "thread-1")
    assert backing.calls == []
    assert provider.acquire("thread-2", user_id="user-1") == "box:user-1:thread-2"


@pytest.mark.asyncio
async def test_undeclared_async_acquire_on_a_retire_terminal_mount_scope_is_refused(stack):
    backing, registry, provider = stack
    declaration, _ = _declaration()
    registry.declare(declaration)
    with pytest.raises(SandboxSessionConflict):
        await provider.acquire_async("thread-1", user_id="user-1")
    assert backing.calls == []


def test_dead_or_revoked_declarations_stop_refusing_admission(stack):
    backing, registry, provider = stack
    declaration, state = _declaration()
    registry.declare(declaration)
    state["live"] = False
    assert provider.acquire("thread-1", user_id="user-1") == "box:user-1:thread-1"
    second, _ = _declaration(public_ref="accepted-session-two")
    registry.declare(second)
    registry.revoke("accepted-session-two")
    assert provider.acquire("thread-1", user_id="user-1") == "box:user-1:thread-1"
    with bind_sandbox_session(second):
        assert provider.get("accepted-session-two") is None


def test_release_of_a_public_ref_runs_the_terminal_only_for_the_declaring_execution(stack):
    backing, registry, provider = stack
    declaration, state = _declaration()
    registry.declare(declaration)
    provider.release("accepted-session-abc")
    assert state["retired"] == 0
    with bind_sandbox_session(declaration):
        provider.release("accepted-session-abc")
        provider.release("accepted-session-abc")
    assert state["retired"] == 1
    assert registry.lookup("accepted-session-abc") is None
    assert all(call[0] != "release" for call in backing.calls)
    provider.release("box:user-1:thread-9")
    assert backing.calls[-1] == ("release", "box:user-1:thread-9")


@pytest.mark.asyncio
async def test_ordinary_paths_delegate_to_the_backing_provider(stack):
    backing, _registry, provider = stack
    sandbox_id = provider.acquire("thread-1", user_id="user-1")
    assert provider.get(sandbox_id) is backing.sandboxes[sandbox_id]
    provider.release(sandbox_id)
    provider.reset()
    await provider.sync_agent_skills_async(sandbox_id, thread_id="thread-1", user_id="user-1")
    provider.destroy(sandbox_id)
    assert [call[0] for call in backing.calls] == ["acquire", "get", "release", "reset", "sync_agent_skills_async", "destroy"]
    assert provider.custom_extra(1) == ("extra", 1)
    assert provider.uses_thread_data_mounts is True
    assert provider.supports_agent_skill_isolation is True
    assert provider.has_accepted_skill_isolation("x-accepted") is True
    assert isinstance(provider, SandboxProvider)


def test_declaration_survives_thread_and_task_boundaries(stack):
    _backing, registry, provider = stack
    declaration, _ = _declaration()
    registry.declare(declaration)

    async def main():
        with bind_sandbox_session(declaration):
            in_thread = await asyncio.to_thread(provider.get, "accepted-session-abc")
            in_task = await asyncio.create_task(asyncio.to_thread(provider.get, "accepted-session-abc"))
            return in_thread, in_task

    assert asyncio.run(main()) == (declaration.handle, declaration.handle)
    assert current_sandbox_session() is None


def test_registry_lets_the_newest_declaration_win_and_the_old_one_stops_resolving(stack):
    """Two sessions minting one public ref in a process is a fault, not a
    sharing opportunity: the newest wins and the older execution fails typed."""
    _backing, registry, provider = stack
    declaration, _ = _declaration()
    assert registry.declare(declaration) is None
    assert registry.declare(declaration) is None
    replacement, _ = _declaration()
    assert registry.declare(replacement) is declaration
    assert registry.lookup("accepted-session-abc") is replacement
    with bind_sandbox_session(declaration):
        assert provider.get("accepted-session-abc") is None
    with bind_sandbox_session(replacement):
        assert provider.get("accepted-session-abc") is replacement.handle


def test_wrap_is_idempotent_and_unwrap_returns_the_backing():
    backing = _Backing()
    provider = sandbox_session_provider(backing)
    assert isinstance(provider, SessionProvider)
    assert sandbox_session_provider(provider) is provider
    assert unwrap_sandbox_provider(provider) is backing
    assert unwrap_sandbox_provider(backing) is backing


def test_installed_provider_is_one_session_provider_per_process():
    reset_sandbox_provider()
    backing = _Backing()
    set_sandbox_provider(backing)
    try:
        installed = get_sandbox_provider()
        assert isinstance(installed, SessionProvider)
        assert unwrap_sandbox_provider(installed) is backing
        assert get_sandbox_provider() is installed
        set_sandbox_provider(installed)
        assert get_sandbox_provider() is installed
    finally:
        reset_sandbox_provider()
    assert ("reset",) in backing.calls


def test_cold_start_wraps_the_configured_provider_class(monkeypatch):
    reset_sandbox_provider()
    monkeypatch.setattr(provider_module, "resolve_class", lambda _path, _base: _Backing)
    monkeypatch.setattr(provider_module, "get_app_config", lambda: SimpleNamespace(sandbox=SimpleNamespace(use="tests:_Backing")))
    try:
        installed = get_sandbox_provider()
        assert isinstance(installed, SessionProvider)
        assert isinstance(unwrap_sandbox_provider(installed), _Backing)
    finally:
        reset_sandbox_provider()
