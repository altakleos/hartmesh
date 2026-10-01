"""Scheduled runs and the tenant's two sandbox slots.

The tenant profile holds two 1 GiB sandboxes, a third acquisition evicts an idle
one, and with both in active use an acquisition waits ``capacity_wait_timeout``
and is then refused. A scheduled run takes a slot at its first sandbox call and
keeps it until the run ends. Two things must hold once the scheduler is on:

- a person's interactive turn is not refused because unattended runs hold the
  slots, so the profile lets the scheduler hold at most one of the two;
- a scheduled occurrence that cannot get a slot ends ``failed`` with the reason
  its owner reads, never silently and never as a third container.

These tests take the scheduler's values from the tenant profile itself
(``deploy/compose/config.yaml``), and drive the production path: a task made
through the Gateway route and claimed by the real ``ScheduledTaskService``
into the real ``InvocationRuntime``, ``RunManager``, ``run_agent`` and
lead-agent graph with the real ``bash`` tool, and the real AIO provider's
admission over a fake container backend (``_FakeBackend``) with the profile's
two slots. Only the model is scripted. Nothing here is a stub that returns a
slot.
"""

from __future__ import annotations

import asyncio
import shutil
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
import yaml
from _agent_e2e_helpers import FakeToolCallingModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from test_accepted_capacity_outcomes import _live
from test_capacity_refusal_terminal_state import _CONFIG_YAML, _stream_turn
from test_runtime_lifecycle_e2e import (
    _create_thread,
    _preserve_process_config_singletons,
    _register_user,
    _reset_process_singletons,
    _wait_for_status,
)
from test_sandbox_warm_reuse_latency import _make_provider

pytestmark = pytest.mark.no_auto_user

REPO_ROOT = Path(__file__).resolve().parents[2]
PROFILE_TEMPLATE = REPO_ROOT / "deploy" / "compose" / "config.yaml"
REFUSED = "no sandbox was free when the task ran, so its tools did not run"


def _profile() -> dict[str, Any]:
    return yaml.safe_load(PROFILE_TEMPLATE.read_text(encoding="utf-8"))


class _Holds:
    """What the scripted model reports to the test, and what the test lets go of.

    ``cancellable`` holds the run in an awaited sleep, which a Stop cancels; otherwise the hold is a
    blocked worker thread, which nothing outside it can interrupt.
    """

    def __init__(self, *, cancellable: bool = False, unwind_seconds: float = 0.0) -> None:
        self.release = threading.Event()
        self.cancellable = cancellable
        self.unwind_seconds = unwind_seconds
        self.holding: list[str] = []
        self._lock = threading.Lock()

    def holds(self, prompt: str) -> None:
        with self._lock:
            self.holding.append(prompt)

    def wait_for(self, count: int, *, timeout: float = 60.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._lock:
                if len(self.holding) >= count:
                    return
            time.sleep(0.05)
        raise AssertionError(f"{count} scheduled runs never held a sandbox; holding {self.holding}")


def _scripted_model(holds: _Holds) -> FakeToolCallingModel:
    """Every turn asks for one bash command. A scheduled turn then holds the run open until the test lets go; a person's turn answers."""

    class _Model(FakeToolCallingModel):
        @staticmethod
        def _reply(messages) -> tuple[AIMessage, str | None]:
            """The reply, and the prompt of a scheduled turn that is to be held open first."""
            prompt = str(next(message for message in messages if isinstance(message, HumanMessage)).content)
            if isinstance(messages[-1], ToolMessage):
                return AIMessage(content="Done."), (prompt if "monthly review" in prompt else None)
            turn = sum(1 for message in messages if isinstance(message, AIMessage))
            call = {"name": "bash", "args": {"description": "list", "command": "ls /mnt/user-data"}, "id": f"bash-{turn}", "type": "tool_call"}
            return AIMessage(content="", tool_calls=[call]), None

        def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
            del stop, run_manager, kwargs
            reply, held = self._reply(messages)
            if held is not None:
                holds.holds(held)
                holds.release.wait(120)
            return ChatResult(generations=[ChatGeneration(message=reply)])

        async def _agenerate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
            if not holds.cancellable:
                return await super()._agenerate(messages, stop=stop, run_manager=run_manager, **kwargs)
            reply, held = self._reply(messages)
            if held is not None:
                holds.holds(held)
                try:
                    while not holds.release.is_set():
                        await asyncio.sleep(0.05)
                except asyncio.CancelledError:
                    # A Stop lands, and the run takes a moment to unwind, holding its sandbox until it has.
                    await asyncio.sleep(holds.unwind_seconds)
                    raise
            return ChatResult(generations=[ChatGeneration(message=reply)])

    return _Model(responses=[AIMessage(content="unused")])


@pytest.fixture
def slot_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """The Gateway on the tenant profile's scheduler and sandbox numbers; ``held_by_others`` slots start held by other people."""

    def _make(*, scheduler: dict[str, Any] | None = None, held_by_others: int = 0):
        profile = _profile()
        blocks = {
            # The poll interval is the one thing the test owns: the poller runs, and the test polls when it wants a poll.
            "scheduler": {**profile["scheduler"], "poll_interval_seconds": 300, **(scheduler or {})},
        }
        home = tmp_path / "deer-flow-home"
        home.mkdir()
        monkeypatch.setenv("DEER_FLOW_HOME", str(home))
        monkeypatch.setenv("OPENAI_API_KEY", "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY")
        monkeypatch.setenv("OPENAI_API_BASE", "https://example.invalid")
        skills = tmp_path / "skills"
        shutil.copytree(REPO_ROOT / "skills" / "public" / "business-report", skills / "public" / "business-report")
        monkeypatch.setenv("DEER_FLOW_SKILLS_PATH", str(skills))
        config = tmp_path / "config.yaml"
        config.write_text(_CONFIG_YAML + yaml.safe_dump(blocks), encoding="utf-8")
        monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(config))
        extensions = tmp_path / "extensions_config.json"
        extensions.write_text('{"mcpServers": {}, "skills": {}}', encoding="utf-8")
        monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(extensions))

        provider, backend = _make_provider(tmp_path, monkeypatch, replicas=profile["sandbox"]["replicas"])
        # The profile waits capacity_wait_timeout (5 s) for a slot; a second keeps the same path and the test short.
        provider._config["capacity_wait_timeout"] = 1
        held = [provider.acquire(f"holder-{index}", user_id="another-person") for index in range(held_by_others)]
        for target in (
            "deerflow.sandbox.get_sandbox_provider",
            "deerflow.sandbox.sandbox_provider.get_sandbox_provider",
            "deerflow.sandbox.tools.get_sandbox_provider",
            "deerflow.sandbox.middleware.get_sandbox_provider",
        ):
            monkeypatch.setattr(target, lambda: provider)

        _preserve_process_config_singletons(monkeypatch)
        _reset_process_singletons(monkeypatch)
        from deerflow.config import app_config as app_config_module

        app_config_module.get_app_config().database.sqlite_dir = str(home / "db")
        from app.gateway.app import create_app

        return create_app(), provider, backend, held

    return _make


def _make_task(client, csrf_token: str, prompt: str) -> str:
    response = client.post(
        "/api/scheduled-tasks",
        json={"title": prompt, "prompt": prompt, "schedule_type": "cron", "schedule_spec": {"cron": "0 9 1 * *"}, "timezone": "UTC"},
        headers={"X-CSRF-Token": csrf_token},
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def _due_now(client, task_ids: list[str]) -> None:
    """Both tasks come due at the same minute."""
    repo = client.app.state.scheduled_task_repo

    async def _move() -> None:
        for task_id in task_ids:
            task = await repo.get_internal(task_id)
            await repo.update(task_id, user_id=task["user_id"], updates={"next_run_at": datetime.now(UTC) - timedelta(minutes=1)})

    client.portal.call(_move)


def _poll(client) -> None:
    client.portal.call(lambda: client.app.state.scheduled_task_service.run_once(now=datetime.now(UTC)))


def _poll_at(client, at: datetime) -> None:
    """One poll on a chosen clock, so a limit that is minutes long can be crossed in a moment."""
    client.portal.call(lambda: client.app.state.scheduled_task_service.run_once(now=at))


def _worker_finished(client, run_id: str) -> bool:
    async def _finished() -> bool:
        record = await client.app.state.run_manager.get(run_id)
        task = getattr(record, "task", None)
        return task is None or task.done()

    return client.portal.call(_finished)


def _started(client, task_id: str) -> datetime:
    began = datetime.fromisoformat(_occurrence(client, task_id)["started_at"])
    return began if began.tzinfo is not None else began.replace(tzinfo=UTC)


def _occurrence(client, task_id: str) -> dict[str, Any]:
    rows = client.get(f"/api/scheduled-tasks/{task_id}/runs").json()
    return rows[0] if rows else {}


def _wait_for_occurrence(client, task_id: str, status: str, *, timeout: float = 60.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        row = _occurrence(client, task_id)
        if row.get("status") == status:
            return row
        time.sleep(0.05)
    raise AssertionError(f"the occurrence never reached {status!r}: {_occurrence(client, task_id)}")


def _wait_for_last_error(client, task_id: str, expected: str, *, timeout: float = 30.0) -> None:
    """The task's own ``last_error`` is written just after its occurrence is ended, by the same completion."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if client.get(f"/api/scheduled-tasks/{task_id}").json()["last_error"] == expected:
            return
        time.sleep(0.05)
    raise AssertionError(f"the task never showed {expected!r}: {client.get(f'/api/scheduled-tasks/{task_id}').json()}")


def _model(holds: _Holds):
    model = _scripted_model(holds)
    return patch("deerflow.agents.lead_agent.agent.create_chat_model", new=lambda *args, **kwargs: model)


def test_two_occurrences_due_together_run_one_after_the_other_and_a_persons_turn_is_accepted(slot_app):
    from starlette.testclient import TestClient

    holds = _Holds()
    app, provider, backend, _ = slot_app()
    with _model(holds), TestClient(app) as client:
        try:
            csrf_token = _register_user(client, email="scheduled-slots-e2e@example.com")
            first, second = _make_task(client, csrf_token, "First monthly review"), _make_task(client, csrf_token, "Second monthly review")
            _due_now(client, [first, second])

            _poll(client)
            holds.wait_for(1)
            _poll(client)
            # One occurrence runs and holds a slot; the other waits, durably, behind it.
            occurrences = {task: _occurrence(client, task) for task in (first, second)}
            running = [task for task, row in occurrences.items() if row.get("status") == "running"]
            queued = [task for task, row in occurrences.items() if row.get("status") == "queued"]
            assert len(running) == 1 and len(queued) == 1, occurrences
            assert len(_live(backend)) == 1

            # A person sends a turn while the scheduled run holds its slot: accepted, and it runs.
            thread_id = _create_thread(client, csrf_token)
            run_id, events = _stream_turn(client, thread_id, csrf_token)
            person = _wait_for_status(client, thread_id, run_id, "success")
            assert person["stop_reason"] is None, person
            assert [event["event"] for event in events][-1] == "end" and "error" not in [event["event"] for event in events]
            assert len(_live(backend)) == 2, "the scheduled run's set and the person's"
            assert _occurrence(client, queued[0])["status"] == "queued", "the waiting occurrence did not take a slot"

            # The running occurrence finishes; the next poll launches the one that waited, and it finishes too.
            holds.release.set()
            _wait_for_occurrence(client, running[0], "success")
            _poll(client)
            _wait_for_occurrence(client, queued[0], "success")
        finally:
            holds.release.set()


def test_with_a_scheduled_run_and_one_person_holding_the_slots_a_second_persons_turn_is_refused_and_nothing_is_evicted(slot_app):
    """The limit of the reserve, stated as a test: it keeps the scheduler off the second slot, not off the first.

    Two slots, one scheduled run and one person's turn: the slots are in use, so another person's turn waits
    ``capacity_wait_timeout`` and is refused, exactly as it is between two people. The scheduled run's set is
    a live turn and is not evicted.
    """
    from starlette.testclient import TestClient

    holds = _Holds()
    app, provider, backend, _ = slot_app()
    with _model(holds), TestClient(app) as client:
        try:
            csrf_token = _register_user(client, email="scheduled-two-people-e2e@example.com")
            task_id = _make_task(client, csrf_token, "First monthly review")
            _due_now(client, [task_id])
            _poll(client)
            holds.wait_for(1)
            [scheduled_set] = _live(backend)
            # Another person's turn takes the other slot and keeps it: an active set, as a person's live turn is.
            holder = provider.acquire("a-person-mid-turn", user_id="another-person")
            assert sorted(_live(backend)) == sorted([scheduled_set, holder])

            thread_id = _create_thread(client, csrf_token)
            run_id, _ = _stream_turn(client, thread_id, csrf_token)
            second_person = _wait_for_status(client, thread_id, run_id, "success")

            assert second_person["stop_reason"] == "sandbox_capacity_exceeded", second_person
            assert sorted(_live(backend)) == sorted([scheduled_set, holder]), "no set was evicted and none was added"
            assert _occurrence(client, task_id)["status"] == "running", "the scheduled run kept its slot"
        finally:
            holds.release.set()


def test_without_the_reserve_two_scheduled_runs_hold_both_slots_and_refuse_a_persons_turn(slot_app):
    """What the profile's one-at-a-time budget prevents: with room for two, two runs hold both slots."""
    from starlette.testclient import TestClient

    holds = _Holds()
    app, provider, backend, _ = slot_app(scheduler={"max_concurrent_runs": 2})
    with _model(holds), TestClient(app) as client:
        try:
            csrf_token = _register_user(client, email="scheduled-no-reserve-e2e@example.com")
            first, second = _make_task(client, csrf_token, "First monthly review"), _make_task(client, csrf_token, "Second monthly review")
            _due_now(client, [first, second])
            _poll(client)
            holds.wait_for(2)
            assert len(_live(backend)) == 2

            thread_id = _create_thread(client, csrf_token)
            run_id, _ = _stream_turn(client, thread_id, csrf_token)
            person = _wait_for_status(client, thread_id, run_id, "success")

            assert person["stop_reason"] == "sandbox_capacity_exceeded", person
        finally:
            holds.release.set()


def test_a_run_stopped_at_the_time_limit_frees_its_slot_before_the_next_scheduled_run_starts(slot_app):
    """Ending the occurrence frees its budget row at once; the run's sandbox is freed when its worker has finished."""
    from starlette.testclient import TestClient

    holds = _Holds(cancellable=True, unwind_seconds=2.0)
    app, provider, backend, _ = slot_app()
    with _model(holds), TestClient(app) as client:
        try:
            csrf_token = _register_user(client, email="scheduled-time-limit-slot-e2e@example.com")
            first, second = _make_task(client, csrf_token, "First monthly review"), _make_task(client, csrf_token, "Second monthly review")
            _due_now(client, [first, second])
            _poll(client)
            holds.wait_for(1)
            _poll(client)
            [running] = [task for task in (first, second) if _occurrence(client, task).get("status") == "running"]
            [waiting] = [task for task in (first, second) if task != running]
            assert _occurrence(client, waiting)["status"] == "queued"
            began = _started(client, running)
            limit = _profile()["scheduler"]["max_run_seconds"]

            # Past the limit the occurrence is ended and the run is stopped; the one that waited is launched only
            # once that run's worker has finished, so at no moment do two scheduled runs hold the slots.
            stopped_run = _occurrence(client, running)["run_id"]
            for step in range(150):
                _poll_at(client, began + timedelta(seconds=limit + 1 + step))
                if _occurrence(client, waiting).get("status") != "queued":
                    assert _worker_finished(client, stopped_run), "the next scheduled run was launched while the stopped one still held its worker and sandbox"
                    break
                time.sleep(0.1)
            assert _occurrence(client, running)["status"] == "failed"
            assert _occurrence(client, running)["error"] == "the task did not finish within 15 minutes, so it was stopped"
            _wait_for_occurrence(client, waiting, "running")
            holds.wait_for(2)
            stopped_thread = _occurrence(client, running)["thread_id"]
            assert provider._thread_sandboxes.get(stopped_thread) is None, "the stopped run no longer holds a set"
            holds.release.set()
            _wait_for_occurrence(client, waiting, "success")
        finally:
            holds.release.set()


def test_an_occurrence_that_meets_both_slots_held_by_people_ends_failed_with_the_reason_and_starts_no_third_set(slot_app):
    from starlette.testclient import TestClient

    holds = _Holds()
    app, provider, backend, held = slot_app(held_by_others=2)
    with _model(holds), TestClient(app) as client:
        try:
            csrf_token = _register_user(client, email="scheduled-refused-e2e@example.com")
            task_id = _make_task(client, csrf_token, "First monthly review")
            _due_now(client, [task_id])
            holds.release.set()

            _poll(client)
            failed = _wait_for_occurrence(client, task_id, "failed")

            assert failed["error"] == REFUSED, failed
            assert _live(backend) == sorted(held), "no third set"
            assert provider._starting == set(), "no reservation left behind"
            _wait_for_last_error(client, task_id, REFUSED)

            # The next occurrence, once a slot is free, is accepted and runs.
            provider.release(held[0])
            assert client.post(f"/api/scheduled-tasks/{task_id}/trigger", headers={"X-CSRF-Token": csrf_token}).status_code == 200
            _wait_for_occurrence(client, task_id, "success")
        finally:
            holds.release.set()
