"""The Kind's Observer: a fact recorded from outside the run reaches the session's record."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.gateway import authz
from app.gateway.authz import SandboxRequestLease, record_refused_sandbox_acquire, sandbox_sync_skip_message, try_acquire_sandbox_for_request
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.search import GrepMatch
from deerflow.sandbox.session import (
    SandboxSessionConflict,
    SandboxSessionDeclaration,
    SandboxSessionKind,
    SandboxSessionRegistry,
    SandboxSessionTerminal,
)

PUBLIC_REF = "accepted-execution-" + "a" * 64


class _Handle(Sandbox):
    def execute_command(self, command, env=None, timeout=None):
        return "OK"

    def read_file(self, path, start_line=None, end_line=None):
        return ""

    def download_file(self, path):
        return b""

    def list_dir(self, path, max_depth=2):
        return []

    def write_file(self, path, content, append=False):
        return None

    def glob(self, path, pattern, *, include_dirs=False, max_results=200):
        return [], False

    def grep(self, path, pattern, *, glob=None, literal=False, case_sensitive=False, max_results=100) -> tuple[list[GrepMatch], bool]:
        return [], False

    def update_file(self, path, content):
        return None


def _declaration(observe, *, live=True) -> SandboxSessionDeclaration:
    return SandboxSessionDeclaration(
        public_ref=PUBLIC_REF,
        mount_scope=("user-1", "thread-1"),
        kind=SandboxSessionKind.ACCEPTED,
        terminal=SandboxSessionTerminal.RETIRE,
        handle=_Handle(PUBLIC_REF),
        is_live=lambda: live,
        retire=lambda: None,
        observe=observe,
    )


def test_the_registry_records_through_the_declarations_observer() -> None:
    seen: list[tuple[str, dict]] = []
    registry = SandboxSessionRegistry()
    registry.declare(_declaration(lambda kind, facts: seen.append((kind, dict(facts)))))

    assert registry.observe(PUBLIC_REF, "session.refused", facts={"requester": "gateway:upload"}) is True
    assert seen == [("session.refused", {"requester": "gateway:upload"})]


def test_observation_is_refused_for_unknown_dead_or_silent_sessions() -> None:
    registry = SandboxSessionRegistry()

    assert registry.observe(PUBLIC_REF, "session.refused", facts={}) is False
    registry.declare(_declaration(None))
    assert registry.observe(PUBLIC_REF, "session.refused", facts={}) is False
    registry.declare(_declaration(lambda kind, facts: None, live=False))
    assert registry.observe(PUBLIC_REF, "session.refused", facts={}) is False


def test_a_failing_observer_never_raises_out_of_the_registry() -> None:
    def broken(kind, facts):
        raise RuntimeError("observer bug")

    registry = SandboxSessionRegistry()
    registry.declare(_declaration(broken))

    assert registry.observe(PUBLIC_REF, "session.refused", facts={}) is False


def test_a_declaration_rejects_a_non_callable_observer() -> None:
    with pytest.raises(TypeError, match="observe"):
        _declaration("not callable")


def test_the_request_lease_turns_a_session_conflict_into_a_skipped_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    observed: list[tuple[str, str, dict]] = []
    registry = SimpleNamespace(observe=lambda public_ref, kind, *, facts: observed.append((public_ref, kind, dict(facts))) or True)
    monkeypatch.setattr("deerflow.sandbox.session.get_sandbox_session_registry", lambda: registry)
    provider = MagicMock()
    provider.acquire_async = AsyncMock(side_effect=SandboxSessionConflict("held", mount_scope=("user-1", "thread-1"), public_ref=PUBLIC_REF))
    provider.get = MagicMock(side_effect=AssertionError("nothing to resolve after a refusal"))

    lease = asyncio.run(
        try_acquire_sandbox_for_request(
            None,
            provider,
            "thread-1",
            user_id="user-1",
            app_config=None,
            owner_prefix="gateway:upload",
            release_on_last=False,
        )
    )

    assert lease == SandboxRequestLease(sandbox=None, sandbox_id=None, denied=True, owner_id=None, provider=None, reason="sandbox_session_conflict")
    assert observed == [(PUBLIC_REF, "session.refused", {"requester": "gateway:upload", "reason": "sandbox_session_conflict"})]
    assert sandbox_sync_skip_message(lease.reason) == "an accepted run holds this thread's sandbox until it ends"
    asyncio.run(lease.release())


def test_a_conflict_without_a_session_ref_is_skipped_but_not_recorded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("deerflow.sandbox.session.get_sandbox_session_registry", lambda: SimpleNamespace(observe=lambda *a, **k: pytest.fail("no ref to observe")))

    assert record_refused_sandbox_acquire(SandboxSessionConflict("dead declaration"), requester="gateway:artifact") is False


def test_skip_messages_are_plain_and_stable() -> None:
    assert sandbox_sync_skip_message(None) is None
    assert sandbox_sync_skip_message(authz.SANDBOX_SYNC_SKIPPED_DENIED) == "sandbox execution is not permitted for your role"
    assert sandbox_sync_skip_message("something_else") == "something_else"
