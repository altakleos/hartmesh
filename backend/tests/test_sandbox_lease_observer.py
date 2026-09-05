"""A lease observer sees holder transitions and never changes them."""

from __future__ import annotations

import pytest

from deerflow.sandbox.lease import SandboxLeaseManager, set_sandbox_lease_observer
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import SandboxProvider
from deerflow.sandbox.search import GrepMatch


class _Stub(Sandbox):
    def execute_command(self, command: str, env: dict[str, str] | None = None, timeout: float | None = None) -> str:
        return "OK"

    def read_file(self, path: str, start_line: int | None = None, end_line: int | None = None) -> str:
        return ""

    def download_file(self, path: str) -> bytes:
        return b""

    def list_dir(self, path: str, max_depth: int = 2) -> list[str]:
        return []

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


class _Provider(SandboxProvider):
    def __init__(self) -> None:
        self.released: list[str] = []
        self.sandbox = _Stub("sandbox")

    def acquire(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        return "sandbox"

    async def acquire_async(self, thread_id: str | None = None, *, user_id: str | None = None) -> str:
        return "sandbox"

    def get(self, sandbox_id: str) -> Sandbox | None:
        return self.sandbox if sandbox_id == "sandbox" else None

    def release(self, sandbox_id: str) -> None:
        self.released.append(sandbox_id)


class _Recording:
    def __init__(self) -> None:
        self.events: list[tuple] = []

    def lease_bound(self, *, owner_id, sandbox_id, thread_id, user_id, borrowed) -> None:
        self.events.append(("bound", owner_id, sandbox_id, thread_id, user_id, borrowed))

    def lease_released(self, *, owner_id, sandbox_id, last_holder) -> None:
        self.events.append(("released", owner_id, sandbox_id, last_holder))


class _Failing(_Recording):
    def lease_bound(self, **kwargs) -> None:
        raise RuntimeError("observer bug")


@pytest.fixture
def observer():
    recording = _Recording()
    set_sandbox_lease_observer(recording)
    try:
        yield recording
    finally:
        set_sandbox_lease_observer(None)


def test_binding_and_last_holder_release_are_observed(observer) -> None:
    provider = _Provider()
    manager = SandboxLeaseManager(provider)

    manager.acquire("lead", "thread", user_id="user")
    manager.acquire("lead", "thread", user_id="user")
    manager.release("lead")

    assert observer.events == [
        ("bound", "lead", "sandbox", "thread", "user", False),
        ("released", "lead", "sandbox", True),
    ]
    assert provider.released == ["sandbox"]


def test_a_borrower_is_observed_as_borrowed_and_the_last_owner_parks(observer) -> None:
    provider = _Provider()
    manager = SandboxLeaseManager(provider)

    manager.acquire("lead", "thread", user_id="user")
    manager.retain("fork", "sandbox", thread_id="thread", user_id="user", release_on_last=False)
    manager.release("lead")
    manager.release("fork")

    assert observer.events == [
        ("bound", "lead", "sandbox", "thread", "user", False),
        ("bound", "fork", "sandbox", "thread", "user", True),
        ("released", "lead", "sandbox", False),
        ("released", "fork", "sandbox", True),
    ]
    assert provider.released == ["sandbox"]


def test_a_failing_observer_never_changes_the_lifecycle() -> None:
    provider = _Provider()
    manager = SandboxLeaseManager(provider)
    set_sandbox_lease_observer(_Failing())
    try:
        assert manager.acquire("lead", "thread", user_id="user") == "sandbox"
        manager.release("lead")
    finally:
        set_sandbox_lease_observer(None)

    assert provider.released == ["sandbox"]


def test_removing_the_observer_stops_observation(observer) -> None:
    provider = _Provider()
    manager = SandboxLeaseManager(provider)
    set_sandbox_lease_observer(None)

    manager.acquire("lead", "thread", user_id="user")
    manager.release("lead")

    assert observer.events == []
