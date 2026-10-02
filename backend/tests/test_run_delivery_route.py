"""``GET /api/threads/{id}/runs/{run_id}/delivery``: the verdict a reloaded page reads.

The projection itself is pinned in ``test_run_delivery_projection.py``. These
cover the route's own decisions: whose run it reads, what it answers for a run
that is not this thread's, and that the stop reason comes from the run record.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from _router_auth_helpers import call_unwrapped
from fastapi import HTTPException

from app.gateway.routers import run_delivery
from deerflow.runtime.runs.delivery import DELIVERY_INCOMPLETE_ERROR, DELIVERY_INCOMPLETE_STOP_REASON


class _Runs:
    def __init__(self, record) -> None:
        self._record = record
        self.asked: list[tuple[str, str | None]] = []

    async def get(self, run_id: str, *, user_id: str | None = None):
        self.asked.append((run_id, user_id))
        return self._record


class _Events:
    def __init__(self, events: list[dict]) -> None:
        self._events = events

    async def list_events(self, *args, **kwargs) -> list[dict]:
        return self._events


def _wire(monkeypatch, record, events: list[dict] | None = None) -> _Runs:
    runs = _Runs(record)

    async def visible(run_id, thread_id, request) -> None:
        return None

    async def scope(request, thread_id) -> str:
        return "user-7"

    monkeypatch.setattr(run_delivery, "get_run_manager", lambda request: runs)
    monkeypatch.setattr(run_delivery, "get_run_event_store", lambda request: _Events(events or []))
    monkeypatch.setattr(run_delivery, "_require_run_visible_to_scope", visible)
    monkeypatch.setattr(run_delivery, "_run_scope_user_id", scope)
    return runs


def _get(thread_id: str = "thread-1", run_id: str = "run-1") -> dict:
    return asyncio.run(call_unwrapped(run_delivery.get_run_delivery, thread_id, run_id, SimpleNamespace()))


def test_a_run_that_withheld_a_file_answers_with_what_it_withheld(monkeypatch) -> None:
    record = SimpleNamespace(thread_id="thread-1", stop_reason=DELIVERY_INCOMPLETE_STOP_REASON)
    runs = _wire(monkeypatch, record, [{"content": {"produced_paths": ["/mnt/user-data/outputs/report.pdf"], "matched_paths": []}}])

    answer = _get()

    assert answer == {
        "available": True,
        "version": 1,
        "run_id": "run-1",
        "message": DELIVERY_INCOMPLETE_ERROR,
        "undelivered_paths": ["/mnt/user-data/outputs/report.pdf"],
        "undelivered_count": 1,
    }
    assert runs.asked == [("run-1", "user-7")], "the run is read in the caller's own scope"


def test_a_run_that_delivered_answers_that_there_is_nothing_to_correct(monkeypatch) -> None:
    _wire(monkeypatch, SimpleNamespace(thread_id="thread-1", stop_reason=None))

    assert _get() == {"available": False, "version": 1}


@pytest.mark.parametrize("record", [None, SimpleNamespace(thread_id="another-thread", stop_reason=DELIVERY_INCOMPLETE_STOP_REASON)], ids=["no-such-run", "another-threads-run"])
def test_a_run_that_is_not_this_threads_is_not_found(monkeypatch, record) -> None:
    _wire(monkeypatch, record)

    with pytest.raises(HTTPException) as refused:
        _get()

    assert refused.value.status_code == 404


def test_the_gateway_serves_the_route() -> None:
    from app.gateway.app import create_app

    assert "/api/threads/{thread_id}/runs/{run_id}/delivery" in [getattr(route, "path", "") for route in create_app().routes]
