"""What this process holds open for each account, so a refusal of that account can end it.

A refusal is derived at every read: a request, a token, a launch all ask
whether the account may act, and are turned away once it may not. Something
that authenticated once and then stays open never asks again -- a browser
WebSocket, an SSE stream, a streaming download. Each registers here under
its owner, with how to end it (``app.gateway.owner_connections``); anything
else a process keeps open for a person between requests belongs here the
same way. The Gateway's refusal watch ends what a refused owner holds, and
the account command waits for the watch's record of having looked.

A holding registered for an owner the last look found refused ends the
moment it registers, so nothing started in the gap between two looks stays
open until the next one -- unless it was authorized after that look read
the refusals: its own read of the account is then the later one, and an
owner enabled again in between is not cut by a look that predates it.
Those endings are kept for the watch to record. An
ending that must be awaited runs on the registering thread's event loop, so
a holding registered from a thread without one must end synchronously.
"""

from __future__ import annotations

import asyncio
import inspect
import itertools
import logging
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

Ending = Callable[[], Awaitable[None] | None]


@dataclass
class Holding:
    """One thing held open for ``owner``; release it when it finishes on its own."""

    owner: str
    surface: str
    end: Ending
    key: int
    registry: OwnerHoldings
    ended: bool = field(default=False)

    def release(self) -> None:
        self.registry._release(self)


class OwnerHoldings:
    """Holdings by owner. Thread-safe: retained state is registered from worker threads too."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._held: dict[str, dict[int, Holding]] = {}
        self._keys = itertools.count()
        self._refused: frozenset[str] = frozenset()
        self._refused_read_at = float("-inf")
        self._late: dict[str, dict[str, int]] = {}

    def hold(self, owner: str, surface: str, end: Ending, *, authorized_at: float | None = None) -> Holding:
        """Hold ``end`` under ``owner``.

        ``authorized_at`` is a ``time.monotonic()`` reading taken no later than
        the read of the account that let it in; without one it is judged by
        the last look.
        """
        holding = Holding(owner=owner, surface=surface, end=end, key=next(self._keys), registry=self)
        with self._lock:
            refused = owner in self._refused and (authorized_at is None or authorized_at < self._refused_read_at)
            if not refused:
                self._held.setdefault(owner, {})[holding.key] = holding
                return holding
            surfaces = self._late.setdefault(owner, {})
            surfaces[surface] = surfaces.get(surface, 0) + 1
        holding.ended = True
        _start_ending(holding)
        return holding

    def _release(self, holding: Holding) -> None:
        with self._lock:
            held = self._held.get(holding.owner)
            if held is None:
                return
            held.pop(holding.key, None)
            if not held:
                del self._held[holding.owner]

    def owners(self) -> set[str]:
        with self._lock:
            return set(self._held)

    def set_refused(self, owners: set[str], *, read_at: float | None = None) -> None:
        """The owners the last look found refused, read by ``read_at`` (``time.monotonic()``, default now).

        A holding they register from now on ends at once, unless it was
        authorized after ``read_at``.
        """
        with self._lock:
            self._refused = frozenset(owners)
            self._refused_read_at = time.monotonic() if read_at is None else read_at

    async def end_owner(self, owner: str) -> dict[str, int]:
        """End everything ``owner`` holds; returns how many of each surface."""
        with self._lock:
            taken = list(self._held.pop(owner, {}).values())
        counts: dict[str, int] = {}
        for holding in taken:
            holding.ended = True
            counts[holding.surface] = counts.get(holding.surface, 0) + 1
            try:
                result = holding.end()
                if inspect.isawaitable(result):
                    await result
            except Exception:  # noqa: BLE001 - one holding that fails to end must not keep the others open
                logger.warning("Ending a %s held for %s failed", holding.surface, owner, exc_info=True)
        return counts

    def drain_late_endings(self) -> dict[str, dict[str, int]]:
        """What ended on registering since the last drain, by owner and surface."""
        with self._lock:
            late, self._late = self._late, {}
        return late


def _start_ending(holding: Holding) -> None:
    try:
        result = holding.end()
    except Exception:  # noqa: BLE001 - the holding is refused either way
        logger.warning("Ending a %s held for %s failed", holding.surface, holding.owner, exc_info=True)
        return
    if not inspect.isawaitable(result):
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        # Registered from a thread with no event loop: an ending that has to
        # be awaited belongs to a loop, so such a holding must end synchronously.
        if inspect.iscoroutine(result):
            result.close()
        logger.warning("A %s held for %s was registered off the event loop with an ending that must be awaited; it was not ended", holding.surface, holding.owner)
        return
    asyncio.ensure_future(result)


_holdings = OwnerHoldings()


def get_owner_holdings() -> OwnerHoldings:
    """This process's registry."""
    return _holdings


__all__ = ["Holding", "OwnerHoldings", "get_owner_holdings"]
