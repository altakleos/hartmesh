"""What bounds an unattended run, and what its owner reads when it hits the bound.

Nobody watches a scheduled run. A run holds one of ``scheduler.max_concurrent_runs``
slots until it ends, so a run that never ends holds that slot until the Gateway
restarts, and before this change nothing bounded a run's wall time: the
scheduler's ``recursion_limit`` and the execution policy's
``scheduler_max_agent_turns`` bound its model calls, and a run that hit one of
those ended ``success`` for its occurrence.

These tests drive the production path as the tenant does: a scheduled task made
through the Gateway route, dispatched by the real ``ScheduledTaskService`` into
the real ``InvocationRuntime``, ``RunManager`` (SQLite run store, ``run_events:
db``), ``run_agent`` and lead-agent graph. Only the model is scripted.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from _agent_e2e_helpers import FakeToolCallingModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from test_capacity_refusal_terminal_state import _durable_row
from test_runtime_lifecycle_e2e import (
    _preserve_process_config_singletons,
    _register_user,
    _reset_process_singletons,
)

pytestmark = pytest.mark.no_auto_user

_CONFIG_YAML = """\
log_level: info
models:
  - name: fake-test-model
    display_name: Fake Test Model
    use: langchain_openai:ChatOpenAI
    model: gpt-4o-mini
    api_key: $OPENAI_API_KEY
    base_url: $OPENAI_API_BASE
sandbox:
  use: deerflow.sandbox.local:LocalSandboxProvider
deployment:
  profile: local_development
title:
  enabled: false
memory:
  enabled: false
database:
  backend: sqlite
run_events:
  backend: db
scheduler:
  enabled: false
  max_concurrent_runs: 1
  max_run_seconds: 300
execution_policy:
  scheduler_max_agent_turns: 3
"""


def _never_answers(started: threading.Event, release: threading.Event) -> FakeToolCallingModel:
    """A model that takes the call and holds it until the test lets go: a run that is not going to finish."""

    class _NeverAnswers(FakeToolCallingModel):
        def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
            del messages, stop, run_manager, kwargs
            started.set()
            release.wait(60)
            return ChatResult(generations=[ChatGeneration(message=AIMessage(content="too late"))])

    return _NeverAnswers(responses=[AIMessage(content="unused")])


class _KeepsAsking(FakeToolCallingModel):
    """Asks for another tool call every turn, each a different one, and never answers."""

    def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
        del stop, run_manager, kwargs
        turn = sum(1 for message in messages if isinstance(message, AIMessage))
        call = {"name": "present_files", "args": {"filepaths": [f"/mnt/user-data/outputs/report-{turn}.txt"]}, "id": f"present-{turn}", "type": "tool_call"}
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="", tool_calls=[call]))])


@pytest.fixture
def scheduled_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    home = tmp_path / "deer-flow-home"
    home.mkdir()
    monkeypatch.setenv("DEER_FLOW_HOME", str(home))
    monkeypatch.setenv("OPENAI_API_KEY", "FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY")
    monkeypatch.setenv("OPENAI_API_BASE", "https://example.invalid")
    config = tmp_path / "config.yaml"
    config.write_text(_CONFIG_YAML, encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_CONFIG_PATH", str(config))
    extensions = tmp_path / "extensions_config.json"
    extensions.write_text('{"mcpServers": {}, "skills": {}}', encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(extensions))

    _preserve_process_config_singletons(monkeypatch)
    _reset_process_singletons(monkeypatch)
    from deerflow.config import app_config as app_config_module

    app_config_module.get_app_config().database.sqlite_dir = str(home / "db")
    from app.gateway.app import create_app

    return create_app()


def _make_daily_task(client, csrf_token: str) -> str:
    response = client.post(
        "/api/scheduled-tasks",
        json={"title": "Monthly review", "prompt": "Build the monthly review.", "schedule_type": "cron", "schedule_spec": {"cron": "0 9 1 * *"}, "timezone": "UTC"},
        headers={"X-CSRF-Token": csrf_token},
    )
    assert response.status_code == 200, response.text
    return response.json()["id"]


def _occurrences(client, task_id: str) -> list[dict[str, Any]]:
    response = client.get(f"/api/scheduled-tasks/{task_id}/runs")
    assert response.status_code == 200, response.text
    return response.json()


def _wait_for_occurrence(client, task_id: str, status: str, *, timeout: float = 60.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        last = _occurrences(client, task_id)
        if last and last[0]["status"] == status:
            return last[0]
        time.sleep(0.05)
    raise AssertionError(f"the occurrence never reached {status!r}: {last}")


def _wait_for_run_status(client, occurrence: dict[str, Any], status: str, *, timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = client.get(f"/api/threads/{occurrence['thread_id']}/runs/{occurrence['run_id']}").json()
        if last["status"] == status:
            return
        time.sleep(0.05)
    raise AssertionError(f"the run never reached {status!r}: {last}")


def _watch_completions(client) -> threading.Event:
    """Set when the scheduler has been told a run ended: the hook runs after the run's terminal commit."""
    completed = threading.Event()
    service = client.app.state.scheduled_task_service
    hook = service.handle_run_completion

    async def _hook(record):
        try:
            await hook(record)
        finally:
            completed.set()

    service.handle_run_completion = _hook
    return completed


def _poll(client, at: datetime) -> None:
    """One scheduler poll at ``at``: the real service, the real stores, a chosen clock."""
    client.portal.call(lambda: client.app.state.scheduled_task_service.run_once(now=at))


def test_a_scheduled_run_that_never_finishes_is_ended_at_its_time_limit_and_says_so(scheduled_app):
    from starlette.testclient import TestClient

    started, release = threading.Event(), threading.Event()
    model = _never_answers(started, release)

    with patch("deerflow.agents.lead_agent.agent.create_chat_model", new=lambda *args, **kwargs: model), TestClient(scheduled_app) as client:
        try:
            csrf_token = _register_user(client, email="scheduled-bound-e2e@example.com")
            task_id = _make_daily_task(client, csrf_token)
            completed = _watch_completions(client)

            triggered = client.post(f"/api/scheduled-tasks/{task_id}/trigger", headers={"X-CSRF-Token": csrf_token})
            assert triggered.status_code == 200, triggered.text
            assert started.wait(60), "the scheduled run never reached the model"
            running = _wait_for_occurrence(client, task_id, "running")
            began = datetime.fromisoformat(running["started_at"])
            began = began if began.tzinfo is not None else began.replace(tzinfo=UTC)

            # Inside its limit, the poll leaves it alone.
            _poll(client, began + timedelta(seconds=299))
            assert _occurrences(client, task_id)[0]["status"] == "running"

            # Past it, the poll ends the occurrence failed and says how long it had, and stops the run.
            _poll(client, began + timedelta(seconds=301))
            [failed] = _occurrences(client, task_id)
            assert failed["status"] == "failed", failed
            assert failed["error"] == "the task did not finish within 5 minutes, so it was stopped", failed
            assert failed["run_id"] == running["run_id"]
            assert client.get(f"/api/scheduled-tasks/{task_id}").json()["last_error"] == failed["error"]

            # The run unwinds interrupted, and its completion does not rewrite the occurrence.
            _wait_for_run_status(client, failed, "interrupted")
            # A model call cannot be interrupted from outside its thread; once it lets go the run commits its ending.
            release.set()
            assert completed.wait(60), "the run's completion never reached the scheduler"
            assert _durable_row(failed["run_id"])["status"] == "interrupted"
            [after] = _occurrences(client, task_id)
            assert (after["status"], after["error"]) == ("failed", failed["error"])
            assert client.get(f"/api/scheduled-tasks/{task_id}").json()["last_error"] == failed["error"]

            # The slot is free: the next occurrence is accepted and runs.
            assert client.post(f"/api/scheduled-tasks/{task_id}/trigger", headers={"X-CSRF-Token": csrf_token}).status_code == 200
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                runs = _occurrences(client, task_id)
                if runs[0]["id"] != failed["id"] and runs[0]["status"] in {"running", "success"}:
                    break
                time.sleep(0.05)
            else:
                raise AssertionError(f"the next occurrence never ran: {_occurrences(client, task_id)}")
        finally:
            release.set()


def test_a_person_who_stops_a_scheduled_run_still_ends_it_interrupted(scheduled_app):
    from starlette.testclient import TestClient

    started, release = threading.Event(), threading.Event()
    model = _never_answers(started, release)

    with patch("deerflow.agents.lead_agent.agent.create_chat_model", new=lambda *args, **kwargs: model), TestClient(scheduled_app) as client:
        try:
            csrf_token = _register_user(client, email="scheduled-stop-e2e@example.com")
            task_id = _make_daily_task(client, csrf_token)
            assert client.post(f"/api/scheduled-tasks/{task_id}/trigger", headers={"X-CSRF-Token": csrf_token}).status_code == 200
            assert started.wait(60)
            running = _wait_for_occurrence(client, task_id, "running")

            stopped = client.post(f"/api/threads/{running['thread_id']}/runs/{running['run_id']}/cancel?wait=true&action=interrupt", headers={"X-CSRF-Token": csrf_token})

            assert stopped.status_code == 204, stopped.text
            interrupted = _wait_for_occurrence(client, task_id, "interrupted")
            assert interrupted["error"] == "run was interrupted before completion"
            run = client.get(f"/api/threads/{interrupted['thread_id']}/runs/{interrupted['run_id']}").json()
            assert (run["status"], run["stop_reason"]) == ("interrupted", None), run
        finally:
            release.set()


def test_a_scheduled_run_that_hits_its_turn_budget_is_a_failed_occurrence_that_names_the_limit(scheduled_app):
    from starlette.testclient import TestClient

    with patch("deerflow.agents.lead_agent.agent.create_chat_model", new=lambda *args, **kwargs: _KeepsAsking(responses=[AIMessage(content="unused")])), TestClient(scheduled_app) as client:
        csrf_token = _register_user(client, email="scheduled-budget-e2e@example.com")
        task_id = _make_daily_task(client, csrf_token)

        assert client.post(f"/api/scheduled-tasks/{task_id}/trigger", headers={"X-CSRF-Token": csrf_token}).status_code == 200

        failed = _wait_for_occurrence(client, task_id, "failed")
        # The owner reads words about the limit, and the run still records the typed reason.
        assert failed["error"] == "the task used up the number of steps it is allowed, so it stopped before it finished", failed
        assert _durable_row(failed["run_id"])["stop_reason"] == "turn_budget_exhausted"
