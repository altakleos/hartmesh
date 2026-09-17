"""Does the product admit that nothing is going to run these schedules?

A tenant-class upgrade found a task row enabled, its next run dated
September 9 and its last run September 8, on a Gateway whose scheduler was not
running at all. Nothing had gone wrong with the data -- the upgrade neither
activated, deleted, rescheduled nor executed it -- but every surface a person
could read said "enabled, next run September 9", eight days after that date had
passed, and none of them said the one fact that explains it.

These tests pin the fact itself: whether the scheduler is actually running,
answered from the running service rather than from a hot-reloadable config file
that says what it was asked to do at startup. Nothing here starts, resumes,
reschedules or triggers anything.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from _router_auth_helpers import call_unwrapped

from app.gateway.routers import scheduled_tasks


def _request(service: object | None) -> SimpleNamespace:
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(scheduled_task_service=service)),
    )


def _with_config(monkeypatch, *, enabled: bool) -> None:
    monkeypatch.setattr(
        scheduled_tasks,
        "get_config",
        lambda: SimpleNamespace(scheduler=SimpleNamespace(enabled=enabled)),
    )


def test_a_running_scheduler_says_so_and_gives_no_reason(monkeypatch):
    _with_config(monkeypatch, enabled=True)
    request = _request(SimpleNamespace(running=True))

    state = asyncio.run(call_unwrapped(scheduled_tasks.get_scheduler_state, request))

    assert state.model_dump() == {"version": 1, "running": True, "configured": True, "state": "running"}


def test_a_scheduler_turned_off_by_configuration_names_that(monkeypatch):
    """The tenant case: the rows are real, and nothing will run them."""
    _with_config(monkeypatch, enabled=False)
    request = _request(SimpleNamespace(running=False))

    state = asyncio.run(call_unwrapped(scheduled_tasks.get_scheduler_state, request))

    assert state.model_dump() == {
        "version": 1,
        "running": False,
        "configured": False,
        "state": "disabled_by_configuration",
    }


def test_a_scheduler_that_is_configured_on_but_not_running_is_a_distinct_answer(monkeypatch):
    """Config hot-reloads; the scheduler starts once, at startup.

    So "the file says enabled" and "a loop is polling" can disagree, and
    collapsing them would report a healthy scheduler on a Gateway whose start
    failed or whose configuration changed under it. That is the anomaly worth
    surfacing, not the one worth hiding.
    """
    _with_config(monkeypatch, enabled=True)
    request = _request(SimpleNamespace(running=False))

    state = asyncio.run(call_unwrapped(scheduled_tasks.get_scheduler_state, request))

    assert state.model_dump() == {
        "version": 1,
        "running": False,
        "configured": True,
        "state": "not_running",
    }


def test_a_gateway_with_no_scheduler_service_answers_instead_of_failing(monkeypatch):
    """A 503 here would leave the caller with nothing to tell the person.

    The list endpoint still returns the rows, so the question "will these run?"
    has to be answerable on the same Gateway; "no, there is no scheduler here"
    is an answer.
    """
    _with_config(monkeypatch, enabled=False)
    request = _request(None)

    state = asyncio.run(call_unwrapped(scheduled_tasks.get_scheduler_state, request))

    assert state.model_dump() == {
        "version": 1,
        "running": False,
        "configured": False,
        "state": "unavailable",
    }


def test_reading_the_state_starts_nothing(monkeypatch):
    """The route is a read. A schedule overdue on a stopped scheduler is a
    resumption/catch-up decision somebody else owns, and this must not make it."""
    _with_config(monkeypatch, enabled=True)
    calls: list[str] = []

    class _Service:
        running = False

        async def start(self) -> None:
            calls.append("start")

        async def trigger(self, *_args, **_kwargs) -> None:
            calls.append("trigger")

    asyncio.run(call_unwrapped(scheduled_tasks.get_scheduler_state, _request(_Service())))

    assert calls == []
