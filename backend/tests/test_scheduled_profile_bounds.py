"""What bounds an unattended run on the tenant profile, with the profile's own numbers.

``test_scheduled_run_bounds.py`` drives the mechanisms with small values. These
take the scheduler and execution-policy blocks from ``deploy/compose/config.yaml``
unchanged and check that a scheduled run which never stops asking, and one that
never answers, each end in a terminal state its owner can read.
"""

from __future__ import annotations

import threading
from datetime import UTC, datetime, timedelta

import pytest
import yaml
from _agent_e2e_helpers import FakeToolCallingModel
from langchain_core.messages import AIMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from test_capacity_refusal_terminal_state import _durable_row
from test_runtime_lifecycle_e2e import _register_user
from test_scheduler_on_disable_and_role_limit import (
    REPO_ROOT,
    _deployment,
    live_scheduler_app,  # noqa: F401 -- the fixture
    live_scheduler_app_with,  # noqa: F401 -- the fixture
)

pytestmark = pytest.mark.no_auto_user

PROFILE = yaml.safe_load((REPO_ROOT / "deploy" / "compose" / "config.yaml").read_text(encoding="utf-8"))


def _keeps_asking(calls: list[int]) -> FakeToolCallingModel:
    """Asks for another tool call every turn, each a different one, and never answers."""

    class _Model(FakeToolCallingModel):
        def _generate(self, messages, stop=None, run_manager=None, **kwargs) -> ChatResult:
            del stop, run_manager, kwargs
            turn = sum(1 for message in messages if isinstance(message, AIMessage))
            calls.append(turn)
            call = {"name": "present_files", "args": {"filepaths": [f"/mnt/user-data/outputs/report-{turn}.txt"]}, "id": f"present-{turn}", "type": "tool_call"}
            return ChatResult(generations=[ChatGeneration(message=AIMessage(content="", tool_calls=[call]))])

    return _Model(responses=[AIMessage(content="unused")])


def test_a_scheduled_run_that_never_stops_asking_ends_at_the_configured_step_limit_and_reads_in_words(live_scheduler_app_with):  # noqa: F811
    """``scheduler.recursion_limit`` reaches the run, and what its owner read at the limit was a bare reference.

    Before, the setting never left the scheduler: every scheduled run got the Gateway default of 100 graph steps,
    which the lead-agent graph spends in nine model turns whatever the setting said. The graph spends about eleven steps a model turn, so 300 steps is about 27 turns.
    """
    calls: list[int] = []
    with _deployment(live_scheduler_app_with(recursion_limit=300), model=_keeps_asking(calls)) as gateway:
        task_id = gateway.make_task(gateway.make_account(role="user"), prompt="Build the monthly review.")
        gateway.make_due(task_id)

        gateway.poll()
        failed = gateway.wait_for_occurrence(task_id, "failed", timeout=300)

        assert failed["error"] == "the task used up the number of steps it is allowed, so it stopped before it finished", failed
        run = gateway.state.run_manager
        assert gateway.call(lambda: run.get(failed["run_id"])).stop_reason == "recursion_limit_reached"
        # The typed reason is on the durable row too, and the run's own error is still the bare reference.
        row = _durable_row(failed["run_id"])
        assert (row["status"], row["stop_reason"]) == ("error", "recursion_limit_reached"), row
        assert row["error"].startswith("Runtime operation failed (reference: "), row
        assert 20 < len(calls) < 35, f"{len(calls)} model turns under a 300-step limit (about eleven steps a turn)"
        assert gateway.task(task_id)["status"] == "enabled", "a recurring task stays enabled"


def test_a_scheduled_run_that_never_answers_is_ended_at_the_profiles_time_limit_and_says_so(live_scheduler_app):  # noqa: F811
    hold = threading.Event()
    with _deployment(live_scheduler_app, hold=hold) as gateway:
        task_id = gateway.make_task(gateway.make_account(role="user"), prompt="Build the monthly review.")
        gateway.make_due(task_id)
        gateway.poll()
        running = gateway.wait_for_occurrence(task_id, "running")
        began = datetime.fromisoformat(running["started_at"])
        began = began if began.tzinfo is not None else began.replace(tzinfo=UTC)
        limit = PROFILE["scheduler"]["max_run_seconds"]

        gateway.call(lambda: gateway.service.run_once(now=began + timedelta(seconds=limit - 1)))
        assert gateway.occurrences(task_id)[0]["status"] == "running"
        gateway.call(lambda: gateway.service.run_once(now=began + timedelta(seconds=limit + 1)))

        [ended] = gateway.occurrences(task_id)
        assert (ended["status"], ended["error"]) == ("failed", "the task did not finish within 15 minutes, so it was stopped")
        assert gateway.task(task_id)["last_error"] == ended["error"]
        assert gateway.task(task_id)["status"] == "enabled"
        hold.set()


def test_a_tenant_that_turns_scheduling_off_is_still_told_saved_times_will_not_arrive(live_scheduler_app_with):  # noqa: F811
    with _deployment(live_scheduler_app_with(enabled=False)) as gateway:
        _register_user(gateway.client, email="scheduling-off-e2e@example.com")

        state = gateway.client.get("/api/scheduler").json()

        assert (state["state"], state["running"], state["configured"]) == ("disabled_by_configuration", False, False), state


def test_a_scheduler_that_has_stopped_is_still_reported_as_not_running(live_scheduler_app):  # noqa: F811
    with _deployment(live_scheduler_app) as gateway:
        _register_user(gateway.client, email="scheduler-stopped-e2e@example.com")
        assert gateway.client.get("/api/scheduler").json()["state"] == "running"

        gateway.call(gateway.service.stop)

        state = gateway.client.get("/api/scheduler").json()
        assert (state["state"], state["running"], state["configured"]) == ("not_running", False, True), state
