"""Each Gateway process ends what it holds for an account nothing may act for, and records that it did.

The refusal is derived at every read; this is where a read reaches what does
not read again. The process's holdings (``deerflow.runtime.owner_holdings``)
name every owner it holds a connection for. A look reads every account a
recorded refusal covers in one query (``list_refused_user_ids``, the match
each account's own read makes), publishes that set to the holdings -- so
anything a refused owner registers from then on ends as it registers -- and
only then ends what those owners already hold. Publishing first leaves no
window in which an owner the look found refused can open something the look
does not end.

It looks when the account command asks it to (a new row in
``refusal_checks``, polled every ``interval_seconds``), and on its own every
``full_look_every_seconds``, so a refusal that came without a request is
taken in. After a complete look it records what it ended and the check it
acted on. It beats on the database clock every tick, whether or not the look
succeeds: a process that is alive but cannot look reads to the command as
alive and unconfirmed, never as gone. Only a process that stopped beating
for :data:`LIVE_WINDOW_SECONDS` counts as gone, and what it held went with
it.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable

from deerflow.persistence.refusal_sweeps import RefusalSweepRepository
from deerflow.runtime.owner_holdings import OwnerHoldings

logger = logging.getLogger(__name__)

#: How often a process looks for a new check and beats. The bound on closing
#: a refused owner's connection is this plus one look.
DEFAULT_INTERVAL_SECONDS = 1.0

#: A look with no check asking for it, for a refusal that came some other way.
DEFAULT_FULL_LOOK_EVERY_SECONDS = 30.0

#: How long the rows of endings and old checks are kept.
PRUNE_AFTER_SECONDS = 24 * 3600.0

#: A process whose heartbeat is older than this is gone, and holds nothing.
#: Generous on purpose: a process beats every tick even when it cannot look,
#: so only one whose loop has stopped -- which streams nothing either -- goes
#: quiet, and until then the command reports it unconfirmed rather than
#: presume its connections closed.
LIVE_WINDOW_SECONDS = 90.0


class RefusalWatch:
    def __init__(
        self,
        sweeps: RefusalSweepRepository,
        holdings: OwnerHoldings,
        *,
        refused_owners: Callable[[], Awaitable[set[str]]],
        process_id: str,
        interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
        full_look_every_seconds: float = DEFAULT_FULL_LOOK_EVERY_SECONDS,
    ) -> None:
        self._sweeps = sweeps
        self._holdings = holdings
        self._refused_owners = refused_owners
        self.process_id = process_id
        self._interval = interval_seconds
        self._full_look_every = full_look_every_seconds
        self._checked = 0
        self._last_look = float("-inf")
        # Ended but not yet recorded: a look whose record fails keeps what it
        # ended for the next one, so the command's count is whole.
        self._unrecorded: dict[str, dict[str, int]] = {}
        self._failing = False
        self._task: asyncio.Task[None] | None = None

    async def register(self) -> None:
        self._checked = await self._sweeps.register(self.process_id)
        # A process that just started holds nothing yet to look at.
        self._last_look = time.monotonic()
        with contextlib.suppress(Exception):
            await self._sweeps.prune(older_than_seconds=PRUNE_AFTER_SECONDS)

    async def tick(self) -> None:
        """Beat, then look if asked or due. The beat never waits on the look."""
        await self._sweeps.beat(self.process_id)
        latest = await self._sweeps.latest_check()
        if latest > self._checked or time.monotonic() - self._last_look >= self._full_look_every:
            await self._look(latest)
            return
        # Endings of holdings a refused owner registered since the last look,
        # recorded against the latest check, which they came after.
        self._keep(self._holdings.drain_late_endings())
        await self._record(latest)

    async def _look(self, check: int) -> None:
        refused = await self._refused_owners()
        # Published before anything is ended: whatever a refused owner
        # registers from here on ends as it registers, unless it was
        # authorized after this read and so read the account later.
        self._holdings.set_refused(refused, read_at=time.monotonic())
        for owner in sorted(self._holdings.owners() & refused):
            self._keep({owner: await self._holdings.end_owner(owner)})
        self._keep(self._holdings.drain_late_endings())
        await self._record(check)
        await self._sweeps.beat(self.process_id, checked_through=check)
        self._checked = check
        self._last_look = time.monotonic()

    def _keep(self, endings: dict[str, dict[str, int]]) -> None:
        for owner, surfaces in endings.items():
            for surface, count in surfaces.items():
                if count:
                    kept = self._unrecorded.setdefault(owner, {})
                    kept[surface] = kept.get(surface, 0) + count

    async def _record(self, check: int) -> None:
        if not self._unrecorded:
            return
        endings = self._unrecorded
        await self._sweeps.record_endings(self.process_id, check, endings)
        self._unrecorded = {}
        logger.info("Closed what refused accounts held in this process: %s", endings)

    async def _run(self) -> None:
        while True:
            try:
                await self.tick()
                if self._failing:
                    logger.info("Refusal watch recovered")
                self._failing = False
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the next tick tries again; the command reports this process unconfirmed meanwhile
                # The traceback once per outage, not once a second.
                logger.warning("Refusal watch tick failed", exc_info=not self._failing)
                self._failing = True
            await asyncio.sleep(self._interval)

    async def start(self) -> None:
        await self.register()
        self._task = asyncio.create_task(self._run(), name="refusal-watch")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        with contextlib.suppress(Exception):
            await self._sweeps.unregister(self.process_id)


__all__ = ["DEFAULT_INTERVAL_SECONDS", "LIVE_WINDOW_SECONDS", "RefusalWatch"]
