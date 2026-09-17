"""Can an operator tell that background memory work has finished?

A tenant-class upgrade took a byte-exact baseline of the data disk while a
turn's *background* memory extraction was still running: the foreground run had
terminated in the browser minutes earlier, the extraction started at 07:14:47
and finished at 07:15:21, inside the first shutdown window, and the post-reboot
comparison then failed on a `memory.json` nobody had touched by hand. The
finding was not data loss -- the drain did its job -- but that nothing the
Gateway publishes said the writer was still working, so "the run is over" was
read as "the disk is settled".

These tests pin the signal that makes the difference observable: what is
buffered, whether a writer is mid-update, and -- separately -- whether the
backend can answer at all, because a backend that cannot observe its own
writers must never be reported idle.
"""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.gateway.routers import memory as memory_router
from deerflow.agents.memory.backends.noop.noop_manager import NoopMemoryManager
from deerflow.agents.memory.manager import MemoryManager, MemoryWriterActivityV1


def _sample_memory() -> dict:
    return {
        "version": "1.0",
        "lastUpdated": "2026-09-17T07:15:21Z",
        "user": {
            "workContext": {"summary": "", "updatedAt": ""},
            "personalContext": {"summary": "", "updatedAt": ""},
            "topOfMind": {"summary": "", "updatedAt": ""},
        },
        "history": {
            "recentMonths": {"summary": "", "updatedAt": ""},
            "earlierContext": {"summary": "", "updatedAt": ""},
            "longTermBackground": {"summary": "", "updatedAt": ""},
        },
        "facts": [],
    }


# ── the record ─────────────────────────────────────────────────────────────


def test_idle_requires_both_an_empty_buffer_and_no_worker():
    assert MemoryWriterActivityV1(buffered=0, in_flight=0).idle is True
    assert MemoryWriterActivityV1(buffered=1, in_flight=0).idle is False
    assert MemoryWriterActivityV1(buffered=0, in_flight=1).idle is False


def test_counts_cannot_be_negative():
    with pytest.raises(ValueError, match="negative"):
        MemoryWriterActivityV1(in_flight=-1)


def test_an_unobservable_backend_is_never_reported_idle():
    """Not knowing is not the same as knowing there is nothing.

    A baseline taken on an inferred idle would be exactly the mistake this
    signal exists to prevent, so the one honest answer for a backend that
    cannot see its own writers is "I cannot say".
    """
    unknown = MemoryWriterActivityV1(observable=False, reason="backend_does_not_track_writers")
    assert unknown.idle is False
    assert unknown.reason == "backend_does_not_track_writers"


def test_an_observable_record_carries_no_reason():
    with pytest.raises(ValueError, match="reason"):
        MemoryWriterActivityV1(buffered=0, in_flight=0, reason="not_applicable")


def test_an_unobservable_record_must_say_why():
    with pytest.raises(ValueError, match="reason"):
        MemoryWriterActivityV1(observable=False)


# ── the contract ───────────────────────────────────────────────────────────


def test_a_backend_that_answers_nothing_is_unknown_rather_than_idle():
    """The one tier-2 default that is deliberately not the convenient answer.

    ``shutdown_flush`` may default to success: a backend with nothing to drain
    loses nothing by being asked. An idle answer is the opposite, because a
    caller acts on it by taking a snapshot. So saying nothing must not read as
    "the writers have finished", and a backend that can account for its workers
    opts in by overriding.
    """

    class Silent(MemoryManager):
        @classmethod
        def from_config(cls, config, **_kwargs):
            return cls()

        def get_context(self, *_args, **_kwargs):
            return ""

        def add(self, *_args, **_kwargs):
            return None

    assert "writer_activity" not in MemoryManager.__abstractmethods__
    activity = Silent().writer_activity()
    assert activity.observable is False and activity.idle is False
    assert activity.reason == "backend_does_not_track_writers"


@pytest.mark.parametrize(
    "backend",
    ["noop", "honcho", "mem0"],
)
def test_every_synchronous_backend_opts_in_to_idle(backend):
    """A backend whose writes finish inside their own call says so explicitly,
    so the honest default costs the deployments it does not apply to nothing."""
    if backend == "noop":
        manager = NoopMemoryManager()
    elif backend == "honcho":
        from deerflow.agents.memory.backends.honcho.honcho_manager import (
            HonchoMemoryManager,
        )

        manager = HonchoMemoryManager.__new__(HonchoMemoryManager)
    else:
        from deerflow.agents.memory.backends.mem0.mem0_manager import Mem0Manager

        manager = Mem0Manager.__new__(Mem0Manager)

    assert manager.writer_activity() == MemoryWriterActivityV1()
    assert manager.writer_activity().idle is True


def test_deermem_reports_its_debounce_buffer(monkeypatch):
    """The buffered updates are the ones a baseline would miss."""
    from deerflow.agents.memory.backends.deermem.deer_mem import DeerMem

    deermem = DeerMem.__new__(DeerMem)
    deermem._queue = SimpleNamespace(pending_count=2, is_processing=False)

    assert deermem.writer_activity() == MemoryWriterActivityV1(buffered=2, in_flight=0)


def test_deermem_reports_a_writer_that_is_mid_update():
    """The extraction in the field report was here: buffer already emptied into
    a worker, the worker still holding an LLM call and the file write."""
    from deerflow.agents.memory.backends.deermem.deer_mem import DeerMem

    deermem = DeerMem.__new__(DeerMem)
    deermem._queue = SimpleNamespace(pending_count=0, is_processing=True)

    activity = deermem.writer_activity()
    assert activity == MemoryWriterActivityV1(buffered=0, in_flight=1)
    assert activity.idle is False


def test_deermem_writer_activity_sees_a_real_queue_through_its_whole_cycle():
    """Against the real queue, not a stand-in: enqueued, then in a worker, then
    idle again once the worker returns."""
    from deerflow.agents.memory.backends.deermem.deer_mem import DeerMem
    from deerflow.agents.memory.backends.deermem.deermem.config import DeerMemConfig
    from deerflow.agents.memory.backends.deermem.deermem.core.queue import MemoryUpdateQueue

    released = threading.Event()
    entered = threading.Event()

    class _SlowUpdater:
        def update_memory(self, *_args, **_kwargs):
            entered.set()
            released.wait(timeout=10)
            return True

    queue = MemoryUpdateQueue(DeerMemConfig(debounce_seconds=60), _SlowUpdater())
    deermem = DeerMem.__new__(DeerMem)
    deermem._queue = queue

    assert deermem.writer_activity().idle is True

    queue.add("thread-1", [{"role": "user", "content": "hello"}], user_id="user-1")
    assert deermem.writer_activity() == MemoryWriterActivityV1(buffered=1, in_flight=0)

    worker = threading.Thread(target=queue.flush, daemon=True)
    worker.start()
    assert entered.wait(timeout=10), "the worker never started"
    assert deermem.writer_activity().in_flight == 1
    assert deermem.writer_activity().idle is False

    released.set()
    worker.join(timeout=10)
    assert deermem.writer_activity().idle is True


def test_openviking_reports_its_accepted_calls_as_writers():
    from deerflow.agents.memory.backends.openviking.openviking_manager import (
        OpenVikingMemoryManager,
    )

    manager = OpenVikingMemoryManager.__new__(OpenVikingMemoryManager)
    manager._lifecycle = threading.Condition(threading.Lock())
    manager._active_operations = 3

    # The counter does not separate a read from a write, so a read counts and
    # `idle` stays conservative -- the direction a caller about to take a
    # snapshot needs it to err in.
    assert manager.writer_activity() == MemoryWriterActivityV1(in_flight=3)

    manager._active_operations = 0
    assert manager.writer_activity().idle is True


# ── the operator's view ────────────────────────────────────────────────────


def test_memory_writers_route_publishes_the_activity():
    """This is what a pre-snapshot check reads."""
    app = FastAPI()
    app.include_router(memory_router.router)
    manager = MagicMock()
    manager.writer_activity.return_value = MemoryWriterActivityV1(buffered=1, in_flight=1)

    with patch("app.gateway.routers.memory.get_memory_manager", return_value=manager):
        with TestClient(app) as client:
            response = client.get("/api/memory/writers")

    assert response.status_code == 200
    assert response.json() == {
        "version": 1,
        "buffered": 1,
        "in_flight": 1,
        "idle": False,
        "observable": True,
        "reason": None,
    }


def test_writers_are_answered_for_a_backend_with_no_memory_document():
    """The reason this is not a field on `/memory/status`.

    That route reads the whole memory document first and answers 501 for a
    backend that does not expose one -- which would hide the writer facts on
    exactly the backends whose workers a caller most needs to ask about.
    """
    app = FastAPI()
    app.include_router(memory_router.router)
    manager = MagicMock()
    manager.get_memory.side_effect = NotImplementedError("no document")
    manager.writer_activity.return_value = MemoryWriterActivityV1()

    with (
        patch("app.gateway.routers.memory.get_memory_manager", return_value=manager),
        patch("app.gateway.routers.memory._resolve_memory_user_id", return_value="user-1"),
    ):
        with TestClient(app) as client:
            assert client.get("/api/memory/status").status_code == 501
            writers = client.get("/api/memory/writers")

    assert writers.status_code == 200 and writers.json()["idle"] is True


def test_a_backend_that_raises_is_reported_unknown_not_a_server_error():
    """A third-party backend may refuse the question; it must not turn it into
    a 500 for the operator who asked."""
    app = FastAPI()
    app.include_router(memory_router.router)
    manager = MagicMock()
    manager.writer_activity.side_effect = NotImplementedError("no writer accounting")

    with patch("app.gateway.routers.memory.get_memory_manager", return_value=manager):
        with TestClient(app) as client:
            response = client.get("/api/memory/writers")

    assert response.status_code == 200
    body = response.json()
    assert body["observable"] is False and body["idle"] is False
    assert body["reason"] == "backend_does_not_track_writers"


def test_memory_writers_never_blocks_the_event_loop_on_the_backend():
    """``writer_activity`` takes the backend's lock, and a worker holds it while
    it works; the route must not take that lock on the loop.

    The loop's thread is recorded from inside the running app, not from the
    test thread: `TestClient` drives the app on its own portal thread, so
    comparing against the test's ident would pass even for a handler that ran
    inline on the loop.
    """
    idents: dict[str, int] = {}

    def writer_activity() -> MemoryWriterActivityV1:
        idents["backend"] = threading.get_ident()
        time.sleep(0.01)
        return MemoryWriterActivityV1()

    manager = MagicMock()
    manager.writer_activity.side_effect = writer_activity

    app = FastAPI()
    app.include_router(memory_router.router)

    @app.get("/loop-thread")
    async def _loop_thread() -> dict[str, int]:
        return {"ident": threading.get_ident()}

    with patch("app.gateway.routers.memory.get_memory_manager", return_value=manager):
        with TestClient(app) as client:
            loop_ident = client.get("/loop-thread").json()["ident"]
            assert client.get("/api/memory/writers").status_code == 200

    assert idents["backend"] != loop_ident
