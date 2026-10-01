"""What a run says about itself while the person waits.

Why
---
Between the moment a message is sent and the model's first tool call the
client had one word, "Working…", for anywhere from seven seconds on a warm
turn to twenty-three on a cold one (tenant-class `.18`), and the only richer
signal was the model writing todos -- five to eight seconds of model time
per update, which is the cost this progress exists to avoid. The turn already
journals its phases (`turn_phases.py`); this module turns the few a person
can act on into one advisory ``custom`` stream frame each, published the
moment the phase begins, before any model token exists.

What it is not
--------------
Not the tool progress: once the model has called a tool the client already
shows that call and its description, so no frame is sent for tools. Not a
promise of timing: a stage says what the run is doing now, never how long it
will take. Not authoritative: a client that never negotiated ``custom`` or
missed the frame loses a label, nothing else, exactly like the delivery
verdict frame this rides beside.

Vocabulary
----------
Three stages, closed: ``preparing`` (from admission -- the workspace and the
accepted skills are being made ready), ``workspace_starting`` (a sandbox is
being created; a cold start, tens of seconds on the tenant class) and
``thinking`` (the first model request has been sent). The stages are ordered
and each is published at most once per run, in that order: a stage behind
one already published is dropped, so a sandbox created after the first model
request (a lazy or re-acquired one, mid-turn) does not put "starting a
workspace" under a turn whose tool cards already cover that stretch, and
later model calls do not re-announce ``thinking``. The stream's first frame
is ``metadata`` (the client learns the run and thread ids from it, and only
then can it place a label), so the publisher holds frames until the worker
opens it right after that frame and then sends the held ones in order. The
worker closes the publisher when the run ends; a phase a stray thread opens
after that is dropped rather than published onto a finished stream.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import Any, Final

from deerflow.runtime.turn_phases import TurnPhase

logger = logging.getLogger(__name__)

#: The ``type`` of the advisory ``custom`` frame.
TURN_PROGRESS_EVENT_TYPE: Final = "turn_progress"


class TurnProgressStage(StrEnum):
    PREPARING = "preparing"
    WORKSPACE_STARTING = "workspace_starting"
    THINKING = "thinking"


#: ``WORKSPACE_STARTING`` no longer covers every cold turn. A new chat's
#: sandbox is built when the chat opens, so a turn that reclaims it warm
#: records no ``SANDBOX_CREATE`` and goes straight from ``PREPARING`` to
#: ``THINKING`` -- and a person who sends while that build is still running
#: waits for it inside ``SANDBOX_LOOKUP``, which is also not this phase. Both
#: are shorter waits than before, under the earlier label; mapping the lookup
#: here instead would put "workspace starting" on every warm follow-up, which
#: is the common case and would be a lie.
_STAGE_BY_PHASE: Final[dict[TurnPhase, TurnProgressStage]] = {
    TurnPhase.ADMISSION: TurnProgressStage.PREPARING,
    TurnPhase.SANDBOX_CREATE: TurnProgressStage.WORKSPACE_STARTING,
    TurnPhase.MODEL_REQUEST: TurnProgressStage.THINKING,
}

#: The order stages are announced in; a stage behind the furthest one already
#: published is dropped.
_STAGE_ORDER: Final[tuple[TurnProgressStage, ...]] = (
    TurnProgressStage.PREPARING,
    TurnProgressStage.WORKSPACE_STARTING,
    TurnProgressStage.THINKING,
)


def stage_for_phase(phase: TurnPhase) -> TurnProgressStage | None:
    """The stage a phase begins, or ``None`` for the phases a person is not told about."""
    return _STAGE_BY_PHASE.get(phase)


def turn_progress_payload(run_id: str, stage: TurnProgressStage, at_ms: float) -> dict[str, Any]:
    """The frame body: the run, the stage, and the journal offset it began at."""
    return {"type": TURN_PROGRESS_EVENT_TYPE, "run_id": run_id, "stage": str(stage), "at_ms": int(at_ms)}


class TurnProgressPublisher:
    """A phase observer that publishes each stage at most once, in order, from any thread.

    Phases begin on whichever thread opens them -- the provider's sandbox
    create runs in a ``to_thread`` worker -- while the stream bridge is an
    asyncio object on the run's loop, so the observer hands the frame to the
    loop with ``call_soon_threadsafe`` and never awaits itself. Publication
    failures are logged and dropped: the journal, the run and the answer do
    not depend on a progress frame reaching anyone.
    """

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        run_id: str,
        publish: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._loop = loop
        self._run_id = run_id
        self._publish = publish
        self._lock = threading.Lock()
        self._furthest = -1
        self._opened = False
        self._closed = False
        self._held: list[dict[str, Any]] = []
        # Strong references: a task only its done-callback knows can be
        # collected mid-flight; the set also lets a test drain publication.
        self._pending: set[asyncio.Task[None]] = set()
        self._handed = 0

    def __call__(self, phase: TurnPhase, at_ms: float) -> None:
        stage = stage_for_phase(phase)
        if stage is None:
            return
        position = _STAGE_ORDER.index(stage)
        payload = turn_progress_payload(self._run_id, stage, at_ms)
        with self._lock:
            if self._closed or position <= self._furthest:
                return
            self._furthest = position
            if not self._opened:
                self._held.append(payload)
                return
            self._handed += 1
        self._hand_off(payload)

    async def open(self) -> None:
        """The stream's ``metadata`` frame is out: send what was held, then publish live.

        Awaited on the run's loop by the worker, so the held frames are on
        the stream before anything the graph publishes. A run that never
        reaches its metadata frame (refused before the graph) publishes no
        progress at all.
        """
        with self._lock:
            if self._opened or self._closed:
                return
            self._opened = True
            held, self._held = self._held, []
        for payload in held:
            try:
                await self._publish(payload)
            except Exception as error:
                logger.warning("turn progress frame for run %s was not published: %s", self._run_id, error)

    def _hand_off(self, payload: dict[str, Any]) -> None:
        try:
            self._loop.call_soon_threadsafe(self._schedule, payload)
        except RuntimeError:
            # The loop is closed: the run is over and nobody is listening.
            with self._lock:
                self._handed -= 1
            logger.debug("turn progress %s dropped for run %s: loop closed", payload["stage"], self._run_id)

    def close(self) -> None:
        """Stop publishing: the run has ended and its stream is finishing.

        A phase opened afterwards by a thread the run abandoned would
        otherwise publish onto a stream that has already ended, or re-create
        one the bridge had cleaned up.
        """
        with self._lock:
            self._closed = True

    def _schedule(self, payload: dict[str, Any]) -> None:
        try:
            task = self._loop.create_task(self._publish(payload))
        except RuntimeError:
            # Closed between the hand-off and this callback; same outcome as
            # above, without asyncio's default handler logging a traceback.
            logger.debug("turn progress frame dropped for run %s: loop closed", self._run_id)
            return
        finally:
            with self._lock:
                self._handed -= 1
        self._pending.add(task)
        task.add_done_callback(self._report)

    def _report(self, task: asyncio.Task[None]) -> None:
        self._pending.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.warning("turn progress frame for run %s was not published: %s", self._run_id, error)

    async def drain(self) -> None:
        """Wait for every frame handed to the loop so far; for tests and shutdown."""
        while True:
            with self._lock:
                handed = self._handed
            if not handed and not self._pending:
                return
            if self._pending:
                await asyncio.gather(*list(self._pending), return_exceptions=True)
            await asyncio.sleep(0)
