"""The durable delivery verdict a rejoining client reads.

The e2e counterpart in ``test_delivery_failure_gateway_stream_e2e.py`` proves
the happy path against a real Gateway, worker and receipt. These cover the
shapes that path cannot produce on demand: a fenced run whose best-effort
receipt never landed, a receipt that says everything was presented, and a run
holding more paths than one response may disclose.
"""

from __future__ import annotations

from typing import Any

import pytest

from deerflow.runtime.runs.delivery import (
    DELIVERY_INCOMPLETE_ERROR,
    DELIVERY_INCOMPLETE_STOP_REASON,
    MAX_DISCLOSED_UNDELIVERED_PATHS,
    get_run_delivery_response,
    undelivered_paths,
)

UNAVAILABLE = {"available": False, "version": 1}
ERROR = DELIVERY_INCOMPLETE_ERROR


class _Events:
    """An event store that returns what it was handed, and records the query."""

    def __init__(self, events: list[dict[str, Any]]) -> None:
        self._events = events
        self.queries: list[tuple[tuple, dict]] = []

    async def list_events(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        self.queries.append((args, kwargs))
        return self._events


def _receipt(produced: list[str], matched: list[str] | None = None) -> dict[str, Any]:
    return {"content": {"produced_paths": produced, "matched_paths": matched or []}}


async def _project(events: _Events, *, stop_reason: str | None) -> dict[str, Any]:
    return await get_run_delivery_response(events, "thread-1", "run-1", stop_reason=stop_reason)


@pytest.mark.asyncio
async def test_a_run_that_delivered_is_answered_without_reading_a_receipt() -> None:
    """Almost every run takes this path, so it must not cost an event query."""
    events = _Events([_receipt(["/mnt/user-data/outputs/a.pdf"])])

    assert await _project(events, stop_reason=None) == UNAVAILABLE
    assert await _project(events, stop_reason="loop_capped") == UNAVAILABLE
    assert events.queries == []


@pytest.mark.asyncio
async def test_a_fenced_run_reports_what_it_never_presented() -> None:
    events = _Events([_receipt(["/mnt/user-data/outputs/a.pdf", "/mnt/user-data/outputs/b.docx"])])

    assert await _project(events, stop_reason=DELIVERY_INCOMPLETE_STOP_REASON) == {
        "available": True,
        "version": 1,
        "run_id": "run-1",
        "message": ERROR,
        "undelivered_paths": ["/mnt/user-data/outputs/a.pdf", "/mnt/user-data/outputs/b.docx"],
        "undelivered_count": 2,
    }


@pytest.mark.asyncio
async def test_a_fenced_run_without_a_receipt_shows_nothing_rather_than_an_empty_notice() -> None:
    """The receipt is best-effort; a correction naming no file is worse than none."""
    assert await _project(_Events([]), stop_reason=DELIVERY_INCOMPLETE_STOP_REASON) == UNAVAILABLE
    assert await _project(_Events([{"content": None}]), stop_reason=DELIVERY_INCOMPLETE_STOP_REASON) == UNAVAILABLE
    assert await _project(_Events([_receipt([])]), stop_reason=DELIVERY_INCOMPLETE_STOP_REASON) == UNAVAILABLE
    # Two receipts for one run means the terminal one is not identifiable.
    duplicated = _Events([_receipt(["/mnt/user-data/outputs/a.pdf"]), _receipt(["/mnt/user-data/outputs/b.pdf"])])
    assert await _project(duplicated, stop_reason=DELIVERY_INCOMPLETE_STOP_REASON) == UNAVAILABLE


@pytest.mark.asyncio
async def test_the_disclosed_path_list_is_bounded_and_the_count_is_not() -> None:
    produced = [f"/mnt/user-data/outputs/report-{index}.pdf" for index in range(MAX_DISCLOSED_UNDELIVERED_PATHS + 7)]
    response = await _project(_Events([_receipt(produced)]), stop_reason=DELIVERY_INCOMPLETE_STOP_REASON)

    assert response["undelivered_count"] == len(produced)
    assert response["undelivered_paths"] == produced[:MAX_DISCLOSED_UNDELIVERED_PATHS]


@pytest.mark.asyncio
async def test_a_malformed_receipt_reports_nothing_rather_than_raising() -> None:
    """The route is a projection over bytes an earlier process wrote.

    A string where a list belongs would otherwise iterate into characters and
    offer the reader per-character "files"; an unhashable entry would raise out
    of the request.
    """
    for content, expected in (
        # A string would iterate into characters.
        ({"produced_paths": "/mnt/user-data/outputs/a.pdf"}, []),
        ({"produced_paths": [{"path": "/out/a.pdf"}]}, []),
        # An unhashable entry in the subtrahend would raise out of ``set()``.
        ({"produced_paths": ["/out/a.pdf"], "matched_paths": [["/out/a.pdf"]]}, ["/out/a.pdf"]),
    ):
        response = await _project(_Events([{"content": content}]), stop_reason=DELIVERY_INCOMPLETE_STOP_REASON)
        assert response.get("undelivered_paths", []) == expected


def test_presented_outputs_are_subtracted_in_scan_order() -> None:
    content = {
        "produced_paths": ["/out/a.pdf", "/out/b.pdf", "/out/c.pdf"],
        "matched_paths": ["/out/b.pdf"],
    }

    assert undelivered_paths(content) == ["/out/a.pdf", "/out/c.pdf"]
    assert undelivered_paths({}) == []
    assert undelivered_paths({"produced_paths": "/out/a.pdf"}) == []
    assert undelivered_paths({"produced_paths": ["/out/a.pdf", 7, ""]}) == ["/out/a.pdf"]


def test_the_projection_and_the_worker_share_one_sentence() -> None:
    """The frame, the run record and this projection must not drift apart.

    The route emits the constant rather than echoing ``record.error``: the run
    error is otherwise absent from every ``runs:read`` response, and a verdict
    that reworded itself on reload would undo the point of reading it durably.
    """
    from deerflow.runtime.runs import worker

    assert worker._DELIVERY_INCOMPLETE_ERROR is DELIVERY_INCOMPLETE_ERROR
    assert worker._delivery_error({"produced_paths": ["/out/a.pdf"]}) == DELIVERY_INCOMPLETE_ERROR
