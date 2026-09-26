"""What a Gateway process keeps for a person between requests, ended when the person is turned off.

Four subsystems keep state for a person after the run that made it is over:
the sandbox provider (a sandbox, parked with whatever the run left running
in it), the MCP session pool, the browser sessions and the memory-update
queue. Each already records whose it is; the process's holdings ask each one,
at every look, to end what it keeps for the refused owners. A sandbox
provider with no way to end an owner's sandboxes is named as not reached.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.gateway import retained_state
from deerflow.runtime.owner_holdings import Ended, OwnerHoldings
from deerflow.sandbox.capabilities import OwnerSandboxEnding
from deerflow.sandbox.sandbox_provider import SandboxProvider


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _Endable(SandboxProvider, OwnerSandboxEnding):
    def __init__(self) -> None:
        self.asked: list[frozenset[str]] = []

    def acquire(self, thread_id=None, *, user_id=None):  # pragma: no cover - not reached
        raise NotImplementedError

    def get(self, sandbox_id):  # pragma: no cover - not reached
        return None

    def release(self, sandbox_id):  # pragma: no cover - not reached
        return None

    def end_sandboxes_for_owners(self, owners):
        self.asked.append(owners)
        return {"pat": Ended(1)} if "pat" in owners else {}


class _NotEndable(SandboxProvider):
    def acquire(self, thread_id=None, *, user_id=None):  # pragma: no cover - not reached
        raise NotImplementedError

    def get(self, sandbox_id):  # pragma: no cover - not reached
        return None

    def release(self, sandbox_id):  # pragma: no cover - not reached
        return None


def test_the_sandboxes_are_unreached_only_where_the_configured_provider_cannot_end_them(monkeypatch) -> None:
    monkeypatch.setattr(retained_state, "_configured_sandbox_provider_class", lambda: _Endable)
    assert retained_state.unreached_surfaces() == ()
    monkeypatch.setattr(retained_state, "_configured_sandbox_provider_class", lambda: _NotEndable)
    assert retained_state.unreached_surfaces() == ("sandboxes",)
    monkeypatch.setattr(retained_state, "_configured_sandbox_provider_class", lambda: None)
    assert retained_state.unreached_surfaces() == (), "no sandbox configured, none to keep"

    def _cannot_load():
        raise ImportError("no module named the_provider")

    monkeypatch.setattr(retained_state, "_configured_sandbox_provider_class", _cannot_load)
    assert retained_state.unreached_surfaces() == ("sandboxes",), "the Gateway still starts, and says it cannot end them"


@pytest.mark.anyio
async def test_each_subsystem_is_asked_to_end_what_it_keeps_for_a_refused_owner(monkeypatch) -> None:
    provider = _Endable()
    monkeypatch.setattr(retained_state, "_initialized_sandbox_provider", lambda: provider)

    async def _mcp(owners):
        return {"pat": Ended(2)} if "pat" in owners else {}

    async def _browsers(owners, *, owner_of):
        assert await owner_of("thread-pat") == "pat"
        return {"pat": Ended(1)} if "pat" in owners else {}

    monkeypatch.setattr(retained_state, "_mcp_session_pool", lambda: SimpleNamespace(close_for_owners=_mcp))
    monkeypatch.setattr(retained_state, "_browser_session_manager", lambda: SimpleNamespace(close_for_owners=_browsers))
    dropped: list[str] = []

    def _cancel(agent_name=None, *, user_id=None):
        dropped.append(user_id)
        return 3 if user_id == "pat" else 0

    monkeypatch.setattr(retained_state, "_initialized_memory_manager", lambda: SimpleNamespace(cancel_by_agent=_cancel))

    async def _thread_owner(thread_id: str) -> str | None:
        return {"thread-pat": "pat"}.get(thread_id)

    holdings = OwnerHoldings()
    retained_state.add_retained_state_sources(holdings, thread_owner=_thread_owner)

    ended = await holdings.end_owners({"pat", "lee"})

    assert ended == {"pat": {"sandboxes": Ended(1), "mcp_sessions": Ended(2), "browser_sessions": Ended(1), "memory_updates": Ended(3)}}
    assert provider.asked == [frozenset({"pat", "lee"})]
    assert sorted(dropped) == ["lee", "pat"]


@pytest.mark.anyio
async def test_a_subsystem_nothing_has_started_keeps_nothing_and_is_not_started_by_asking(monkeypatch) -> None:
    monkeypatch.setattr(retained_state, "_initialized_sandbox_provider", lambda: None)
    monkeypatch.setattr(retained_state, "_initialized_memory_manager", lambda: None)
    monkeypatch.setattr(retained_state, "_mcp_session_pool", lambda: None)
    monkeypatch.setattr(retained_state, "_browser_session_manager", lambda: None)

    async def _thread_owner(thread_id: str) -> str | None:  # pragma: no cover - nothing to ask about
        return None

    holdings = OwnerHoldings()
    retained_state.add_retained_state_sources(holdings, thread_owner=_thread_owner)
    assert await holdings.end_owners({"pat"}) == {}


@pytest.mark.anyio
async def test_a_provider_that_cannot_end_an_owners_sandboxes_is_asked_nothing(monkeypatch) -> None:
    """It is named unreached at registration; asking it at a look would only read as nothing held."""
    monkeypatch.setattr(retained_state, "_initialized_sandbox_provider", lambda: _NotEndable())
    monkeypatch.setattr(retained_state, "_initialized_memory_manager", lambda: None)
    monkeypatch.setattr(retained_state, "_mcp_session_pool", lambda: None)
    monkeypatch.setattr(retained_state, "_browser_session_manager", lambda: None)

    async def _thread_owner(thread_id: str) -> str | None:  # pragma: no cover
        return None

    holdings = OwnerHoldings()
    retained_state.add_retained_state_sources(holdings, thread_owner=_thread_owner)
    assert await holdings.end_owners({"pat"}) == {}


@pytest.mark.anyio
async def test_whose_a_thread_is_is_read_for_whoever_owns_it_with_nobody_signed_in(tmp_path) -> None:
    """The watch acts for no signed-in person, so the thread store must not be asked as the current user."""
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.thread_meta import make_thread_store

    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/threads.db", sqlite_dir=str(tmp_path))
    try:
        threads = make_thread_store(get_session_factory())
        await threads.create("thread-pat", user_id="pat")
        owner_of = retained_state.thread_owner_from(threads)
        assert await owner_of("thread-pat") == "pat"
        assert await owner_of("thread-gone") is None
        assert await retained_state.thread_owner_from(None)("thread-pat") is None
    finally:
        await close_engine()
