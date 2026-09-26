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

State a subsystem keeps for a person between requests -- a parked sandbox,
a pooled MCP session, a browser, a queued memory update -- already has a
record of its own, keyed by its owner. It is not copied here: the subsystem
adds a *source* (:meth:`OwnerHoldings.add_source`), which each look asks to
end what it keeps for the refused owners, from that record. What a source
could not confirm ended is reported as ``failed``, never as ended. A source
that takes longer than its limit -- a container that is slow to stop -- is
reported failed for that look, is not asked again while it runs, and what it
ended is carried into the next look, so a slow subsystem neither holds up
the others nor reads as done before it is.
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


@dataclass(frozen=True)
class Ended:
    """How many of one surface were ended for one owner, and how many could not be confirmed ended."""

    count: int = 0
    failed: int = 0


#: Ends what a subsystem keeps for any of the given owners; by owner.
Source = Callable[[frozenset[str]], "Awaitable[dict[str, Ended]] | dict[str, Ended]"]

#: The owner a source reports under when it cannot say whose: it failed
#: outright, it is still running past its limit, or it keeps something whose
#: owner it never learned. Read for every refused owner.
ANY_OWNER = "*"

#: How long one look waits on one source before reporting it not ended.
SOURCE_TIME_LIMIT_SECONDS = 15.0


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

    def __init__(self, *, source_time_limit_seconds: float = SOURCE_TIME_LIMIT_SECONDS) -> None:
        self._lock = threading.Lock()
        self._source_time_limit = source_time_limit_seconds
        # A source still running past its limit, by surface: not asked again
        # until it finishes, and its result taken into the next look.
        self._running: dict[str, asyncio.Future[dict[str, Ended]]] = {}
        self._held: dict[str, dict[int, Holding]] = {}
        self._keys = itertools.count()
        self._refused: frozenset[str] = frozenset()
        self._refused_read_at = float("-inf")
        self._late: dict[str, dict[str, Ended]] = {}
        self._sources: dict[str, tuple[Source, bool]] = {}

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
            self._add_late_locked(owner, surface, Ended(1))
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

    def add_source(self, surface: str, end_for: Source, *, blocking: bool = False) -> None:
        """Have each look ask ``end_for`` to end what it keeps for the refused owners.

        ``blocking``: it waits on something slow (a container stop) and runs
        on a worker thread. One source per surface; adding it again replaces it.
        """
        with self._lock:
            self._sources[surface] = (end_for, blocking)

    async def end_owners(self, owners: set[str]) -> dict[str, dict[str, Ended]]:
        """End what ``owners`` hold here and what every source keeps for them, by owner and surface.

        A source that fails outright, or is still running past its limit, has
        confirmed nothing for any of them: it is reported as one not ended
        under :data:`ANY_OWNER`, so none reads as ended. The sources are asked
        side by side.
        """
        ended: dict[str, dict[str, Ended]] = {}
        if not owners:
            return ended
        for owner in sorted(self.owners() & owners):
            for surface, count in (await self.end_owner(owner)).items():
                ended.setdefault(owner, {})[surface] = Ended(count)
        with self._lock:
            sources = list(self._sources.items())
        asked = frozenset(owners)
        outcomes = await asyncio.gather(*(self._ask(surface, end_for, blocking, asked) for surface, (end_for, blocking) in sources))
        for (surface, _), result in zip(sources, outcomes, strict=True):
            for owner, outcome in result.items():
                if outcome.count or outcome.failed:
                    ended.setdefault(owner, {})[surface] = outcome
        return ended

    async def _ask(self, surface: str, end_for: Source, blocking: bool, asked: frozenset[str]) -> dict[str, Ended]:
        # Whose it was is not known: one row read for every refused owner,
        # rather than one per refused owner on every look.
        not_ended = {ANY_OWNER: Ended(0, failed=1)}
        carried: dict[str, Ended] = {}
        running = self._running.pop(surface, None)
        if running is not None:
            if not running.done():
                self._running[surface] = running
                return not_ended
            carried = running.result() if not running.cancelled() and running.exception() is None else {}

        async def _call() -> dict[str, Ended]:
            result = await asyncio.to_thread(end_for, asked) if blocking else end_for(asked)
            result = await result if inspect.isawaitable(result) else result
            if not isinstance(result, dict) or not all(isinstance(outcome, Ended) for outcome in result.values()):
                raise TypeError(f"the {surface} source answered {type(result).__name__}, not a mapping of owner to Ended")
            return result

        call = asyncio.ensure_future(_call())
        try:
            result = await asyncio.wait_for(asyncio.shield(call), self._source_time_limit)
        except TimeoutError:
            logger.warning("Ending the %s kept for refused accounts is taking longer than %.0fs; reported not ended until it finishes", surface, self._source_time_limit)
            self._running[surface] = call
            result = not_ended
        except Exception:  # noqa: BLE001 - one subsystem that cannot end what it keeps must not keep the others from ending theirs
            logger.warning("Ending the %s kept for refused accounts failed", surface, exc_info=True)
            result = not_ended
        merged = dict(carried)
        for owner, outcome in result.items():
            before = merged.get(owner, Ended())
            merged[owner] = Ended(before.count + outcome.count, before.failed + outcome.failed)
        return merged

    def _add_late_locked(self, owner: str, surface: str, ended: Ended) -> None:
        surfaces = self._late.setdefault(owner, {})
        before = surfaces.get(surface, Ended())
        surfaces[surface] = Ended(before.count + ended.count, before.failed + ended.failed)

    def is_refused(self, owner: str) -> bool:
        """Whether the last look found ``owner`` refused: for a subsystem about to keep something for them."""
        with self._lock:
            return owner in self._refused

    def note_late_ending(self, owner: str, surface: str, ended: Ended) -> None:
        """Something kept for a refused owner was ended as it would have been kept; the watch records it."""
        with self._lock:
            self._add_late_locked(owner, surface, ended)

    def drain_late_endings(self) -> dict[str, dict[str, Ended]]:
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


__all__ = ["ANY_OWNER", "Ended", "Holding", "OwnerHoldings", "Source", "get_owner_holdings"]
