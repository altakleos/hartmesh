"""Each Gateway process ends what it holds for an account nothing may act for, and records that it did.

The refusal is derived at every read; this is where a read reaches what does
not read again. The process's holdings (``deerflow.runtime.owner_holdings``)
name every owner it holds a connection for, and ask each subsystem that
keeps state for a person between requests to end it from its own record. A look reads every account a
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
acted on. It beats on the database clock every ``interval_seconds`` in a loop
of its own, whether the look succeeds, fails or is still waiting: a process that is alive but cannot look reads to the command as
alive and unconfirmed, never as gone. Only a process that stopped beating
for :data:`LIVE_WINDOW_SECONDS` counts as gone, and what it held went with
it. What a look tried to end and could not confirm is recorded as failed,
and a surface the process has no way to end at all is named when it
registers (``unreached``), so the command never reads either as ended.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable

from deerflow.persistence.refusal_sweeps import RefusalSweepRepository
from deerflow.runtime.owner_holdings import Ended, OwnerHoldings

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
        unreached: tuple[str, ...] = (),
    ) -> None:
        self._sweeps = sweeps
        self._holdings = holdings
        self._refused_owners = refused_owners
        self.process_id = process_id
        self._interval = interval_seconds
        self._full_look_every = full_look_every_seconds
        self._unreached = tuple(unreached)
        self._checked = 0
        self._last_look = float("-inf")
        # Ended but not yet recorded: a look whose record fails keeps what it
        # ended for the next one, so the command's count is whole.
        self._unrecorded: dict[str, dict[str, Ended]] = {}
        self._failing = False
        self._task: asyncio.Task[None] | None = None
        self._beat_task: asyncio.Task[None] | None = None
        self._beat_failing = False

    async def register(self) -> None:
        self._checked = await self._sweeps.register(self.process_id, unreached=self._unreached)
        # A process that just started holds nothing yet to look at.
        self._last_look = time.monotonic()
        with contextlib.suppress(Exception):
            await self._sweeps.prune(older_than_seconds=PRUNE_AFTER_SECONDS)

    async def beat(self) -> None:
        await self._sweeps.beat(self.process_id, unreached=self._unreached)

    async def tick(self) -> None:
        """Look if asked or due. The heartbeat is its own loop (``_beat_forever``), so it never waits on a look."""
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
        self._keep(await self._holdings.end_owners(refused))
        self._keep(self._holdings.drain_late_endings())
        await self._record(check)
        await self._sweeps.beat(self.process_id, checked_through=check, unreached=self._unreached)
        self._checked = check
        self._last_look = time.monotonic()

    def _keep(self, endings: dict[str, dict[str, Ended]]) -> None:
        for owner, surfaces in endings.items():
            for surface, ended in surfaces.items():
                if ended.count or ended.failed:
                    kept = self._unrecorded.setdefault(owner, {})
                    before = kept.get(surface, Ended())
                    kept[surface] = Ended(before.count + ended.count, before.failed + ended.failed)

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

    async def _beat_forever(self) -> None:
        # Apart from the looks on purpose: a look can wait on a container
        # that is slow to stop, and a process that went quiet meanwhile would
        # read to the command as gone, and what it holds as ended.
        while True:
            try:
                await self.beat()
                self._beat_failing = False
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - the next beat tries again; a process that cannot beat reads as gone once its window passes
                logger.warning("Refusal watch heartbeat failed", exc_info=not self._beat_failing)
                self._beat_failing = True
            await asyncio.sleep(self._interval)

    async def start(self) -> None:
        await self.register()
        self._beat_task = asyncio.create_task(self._beat_forever(), name="refusal-watch-beat")
        self._task = asyncio.create_task(self._run(), name="refusal-watch")

    async def stop(self) -> None:
        for task in (self._task, self._beat_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._task = self._beat_task = None
        with contextlib.suppress(Exception):
            await self._sweeps.unregister(self.process_id)


__all__ = ["DEFAULT_INTERVAL_SECONDS", "LIVE_WINDOW_SECONDS", "RefusalWatch"]
