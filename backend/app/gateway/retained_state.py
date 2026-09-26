"""What this Gateway process keeps for a person between requests, ended when the person is turned off.

A run that is over can leave state behind, kept for its owner until
something needs the room:

- ``sandboxes``: the sandbox the run used, parked for the owner's next turn
  with whatever the run left running inside it, and one a turn is still
  using. Stopping the container ends both (``OwnerSandboxEnding``).
- ``mcp_sessions``: a pooled session to each MCP server the run called, kept
  per owner and thread with no idle expiry.
- ``browser_sessions``: a headless browser per thread, kept with its pages
  and cookies until another thread needs the slot.
- ``memory_updates``: a conversation queued to be written to the owner's
  memory after the debounce.

Each subsystem already records whose each entry is, so none is copied into
the process's holdings: each is added as a source
(``OwnerHoldings.add_source``), which the refusal watch asks at every look to
end what it keeps for the refused owners. A subsystem nothing in this
process has started keeps nothing, and asking does not start it.

A sandbox provider that cannot end an owner's sandboxes is named when the
process registers (:func:`unreached_surfaces`), so the account command
reports that surface as not reached rather than read the silence as nothing
held. A memory update already being written when the look comes is not
stopped: it is one model call, already under way.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from deerflow.runtime.owner_holdings import Ended, OwnerHoldings

logger = logging.getLogger(__name__)

#: The surfaces this module adds, in the order the account command names them.
RETAINED_SURFACES = ("sandboxes", "mcp_sessions", "browser_sessions", "memory_updates")

ThreadOwner = Callable[[str], Awaitable[str | None]]


def _configured_sandbox_provider_class() -> type | None:
    from deerflow.config import get_app_config
    from deerflow.reflection import resolve_class
    from deerflow.sandbox.sandbox_provider import SandboxProvider

    use = getattr(getattr(get_app_config(), "sandbox", None), "use", None)
    return resolve_class(use, SandboxProvider) if use else None


def _initialized_sandbox_provider() -> Any:
    from deerflow.sandbox.sandbox_provider import get_initialized_sandbox_provider

    return get_initialized_sandbox_provider()


def _mcp_session_pool() -> Any:
    from deerflow.mcp.session_pool import get_initialized_session_pool

    return get_initialized_session_pool()


def _browser_session_manager() -> Any:
    from deerflow.community.browser_automation.session import get_initialized_browser_session_manager

    return get_initialized_browser_session_manager()


def _initialized_memory_manager() -> Any:
    from deerflow.agents.memory.manager import get_initialized_memory_manager

    return get_initialized_memory_manager()


def thread_owner_from(thread_store: Any) -> ThreadOwner:
    """Whose a thread is, read from ``thread_store`` for whoever owns it -- the watch acts for no signed-in person."""

    async def _thread_owner(thread_id: str) -> str | None:
        if thread_store is None:
            return None
        thread = await thread_store.get(thread_id, user_id=None)
        return thread.get("user_id") if thread else None

    return _thread_owner


def unreached_surfaces() -> tuple[str, ...]:
    """The surfaces this process has no way to end: the sandboxes, when the configured provider cannot end an owner's."""
    from deerflow.sandbox.capabilities import OwnerSandboxEnding

    try:
        provider_class = _configured_sandbox_provider_class()
    except Exception:  # noqa: BLE001 - a provider that cannot be loaded ends nothing; the Gateway still starts and says so
        logger.warning("The configured sandbox provider could not be loaded; a refused owner's sandboxes are reported not reached", exc_info=True)
        return ("sandboxes",)
    if provider_class is None or issubclass(provider_class, OwnerSandboxEnding):
        return ()
    return ("sandboxes",)


def add_retained_state_sources(holdings: OwnerHoldings, *, thread_owner: ThreadOwner) -> None:
    """Add each subsystem that keeps state for a person as a source of ``holdings``.

    ``thread_owner`` answers whose a thread is: a browser is kept per thread
    with no owner of its own.
    """
    from deerflow.sandbox.capabilities import OwnerSandboxEnding, sandbox_capability

    def _sandboxes(owners: frozenset[str]) -> dict[str, Ended]:
        provider = _initialized_sandbox_provider()
        ending = sandbox_capability(provider, OwnerSandboxEnding) if provider is not None else None
        # A provider without the capability was named unreached at registration.
        return ending.end_sandboxes_for_owners(owners) if ending is not None else {}

    async def _mcp_sessions(owners: frozenset[str]) -> dict[str, Ended]:
        pool = _mcp_session_pool()
        return await pool.close_for_owners(owners) if pool is not None else {}

    async def _browser_sessions(owners: frozenset[str]) -> dict[str, Ended]:
        manager = _browser_session_manager()
        return await manager.close_for_owners(owners, owner_of=thread_owner) if manager is not None else {}

    def _memory_updates(owners: frozenset[str]) -> dict[str, Ended]:
        manager = _initialized_memory_manager()
        if manager is None:
            return {}
        # ``agent_name=None``: every agent's pending update for that owner.
        dropped = {owner: manager.cancel_by_agent(None, user_id=owner) for owner in owners}
        return {owner: Ended(count) for owner, count in dropped.items() if count}

    holdings.add_source("sandboxes", _sandboxes, blocking=True)
    holdings.add_source("mcp_sessions", _mcp_sessions)
    holdings.add_source("browser_sessions", _browser_sessions)
    holdings.add_source("memory_updates", _memory_updates)


__all__ = ["RETAINED_SURFACES", "add_retained_state_sources", "thread_owner_from", "unreached_surfaces"]
