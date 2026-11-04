"""A turn that could not get a sandbox says so, and a scheduled task that met it ends failed.

The Compose profile holds two sandboxes. With both in use by people, a third
turn's first sandbox call is refused inside the tool. The model is told to
finish with what it has, and the turn ends ``success``. The run row carries
``sandbox_capacity_exceeded`` so a scheduled occurrence, whose answer nobody
reads, ends ``failed`` with words its owner can use. A guard that cuts a
scheduled run short ends its occurrence the same way.

The path below the scheduler's launch is the production one: the AIO
provider's own admission over a fake container backend with two slots held,
the real ``bash`` tool through an async graph (the way the worker runs it),
the sandbox, tool-error and loop-guard middlewares, ``run_agent``, the SQL run
store, and ``ScheduledTaskService.handle_run_completion`` over the SQL
scheduled-task repositories, wired as the Gateway wires it
(``RunContext.on_run_completed``). Only the model is scripted.

The outcome is the store's (``finalize_occurrence``): the completion hook,
the poller's reconcile and startup recovery read the same run row the same way.
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import pytest
import pytest_asyncio
from _sandbox_provider_fakes import _make_provider
from langchain.agents import create_agent
from langchain.tools import ToolRuntime
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.tools import tool
from pydantic import Field
from sqlalchemy import text, update

from app.scheduler.service import ScheduledTaskService
from deerflow.agents.middlewares.loop_detection_middleware import LoopDetectionMiddleware
from deerflow.agents.middlewares.tool_error_handling_middleware import ToolErrorHandlingMiddleware
from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY
from deerflow.config.app_config import AppConfig
from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence.engine import close_engine, get_engine, get_session_factory, init_engine_from_config
from deerflow.persistence.run import RunRepository
from deerflow.persistence.scheduled_task_runs import ScheduledTaskRunRepository
from deerflow.persistence.scheduled_task_runs.finalization import STOPPED_EARLY_ERRORS
from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
from deerflow.runtime.runs import worker
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.user_context import reset_current_user, set_current_user
from deerflow.sandbox.exceptions import SandboxCapacityExceededError
from deerflow.sandbox.middleware import SandboxMiddleware
from deerflow.sandbox.sandbox_provider import reset_sandbox_provider, set_sandbox_provider
from deerflow.sandbox.tools import bash_tool, ensure_sandbox_initialized_async

OWNER = "scheduled-owner"
REFUSED = "no sandbox was free when the task ran, so its tools did not run"
LOOPED = "the task kept repeating the same action, so it was stopped before it finished"
BUSY_ANSWER = "The workspace is busy, so I could not build the report this time."


def _postgres_url(url: str) -> str:
    parts = urlsplit(url)
    query = urlencode([(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key not in {"sslmode", "channel_binding"}])
    return urlunsplit(parts._replace(query=query))


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def stores(tmp_path, request):
    schema = None
    if request.param == "postgres":
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("TEST_POSTGRES_URI is required for PostgreSQL coverage")
        schema = "capacity_" + uuid4().hex
        config = DatabaseConfig(backend="postgres", postgres_url=_postgres_url(uri), postgres_schema=schema)
    else:
        config = DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path / "db"))
    await init_engine_from_config(config)
    session_factory = get_session_factory()
    runs = RunRepository(session_factory)
    try:
        yield SimpleNamespace(
            session_factory=session_factory,
            runs=runs,
            schedules=ScheduledTaskRepository(session_factory, run_repository=runs),
            occurrences=ScheduledTaskRunRepository(session_factory, run_repository=runs),
        )
    finally:
        if schema is not None:
            async with get_engine().begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await close_engine()


@pytest.fixture
def slots_held_by_people(tmp_path, monkeypatch):
    """The profile's two slots, each held by a person's turn mid-command."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=2)
    # The wait before a refusal is the provider's own subject; here it would only cost time.
    provider._config["capacity_wait_timeout"] = 0
    held = sorted([provider.acquire("alice-chat", user_id="alice"), provider.acquire("bob-chat", user_id="bob")])
    set_sandbox_provider(provider)
    try:
        yield SimpleNamespace(provider=provider, backend=backend, held=held)
    finally:
        reset_sandbox_provider()
        provider._acquire_worker_executor.shutdown(wait=False)


def _live(backend) -> list[str]:
    return sorted(sandbox_id for sandbox_id, alive in backend.alive.items() if alive)


class ScriptedModel(FakeMessagesListChatModel):
    """Answers from a script and keeps what each call was sent."""

    received: list = Field(default_factory=list)

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.received.append(list(messages))
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


class RepeatingModel(ScriptedModel):
    """Asks for the same lookup every time, until the loop guard stops it."""

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        self.received.append(list(messages))
        from langchain_core.outputs import ChatGeneration, ChatResult

        call = {"id": f"look-{len(self.received)}", "name": "look_up_sales", "args": {"month": "October"}}
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content="", tool_calls=[call]))])


def _bash_then(answer: str, *more: AIMessage) -> ScriptedModel:
    build = {"id": "build-1", "name": "bash", "args": {"command": "python build_report.py", "description": "Build the report"}}
    return ScriptedModel(responses=[AIMessage(content="", tool_calls=[build]), *more, AIMessage(content=answer)])


def _graph(model, tools, *extra_middleware):
    return create_agent(model=model, tools=tools, middleware=[SandboxMiddleware(), *extra_middleware, ToolErrorHandlingMiddleware()])


async def _scheduled_occurrence(stores, run_id: str, thread_id: str) -> tuple[str, str]:
    """A weekly task whose occurrence was launched as *run_id* and is running."""
    task_id, occurrence_id = str(uuid4()), str(uuid4())
    now = datetime.now(UTC)
    await stores.schedules.create(
        task_id=task_id,
        user_id=OWNER,
        thread_id=thread_id,
        context_mode="reuse_thread",
        assistant_id="lead_agent",
        title="Monday report",
        prompt="Build the weekly sales report",
        schedule_type="cron",
        schedule_spec={"cron": "0 9 * * 1"},
        timezone="UTC",
        next_run_at=now + timedelta(days=7),
    )
    await stores.occurrences.create(run_record_id=occurrence_id, task_id=task_id, thread_id=thread_id, scheduled_for=now, trigger="scheduled", status="queued")
    async with stores.session_factory() as session:
        await session.execute(update(ScheduledTaskRunRow).where(ScheduledTaskRunRow.id == occurrence_id).values(status="running", run_id=run_id, started_at=now))
        await session.commit()
    return task_id, occurrence_id


def _scheduler(stores) -> ScheduledTaskService:
    async def never_launch(**kwargs):
        raise AssertionError("these occurrences are already running")

    return ScheduledTaskService(task_repo=stores.schedules, task_run_repo=stores.occurrences, launch_run=never_launch, poll_interval_seconds=5, lease_seconds=120, max_concurrent_runs=1)


async def _turn(stores, graph, *, scheduled: bool, monkeypatch):
    """One run through the worker; a scheduled one is completed by the real scheduler."""
    run_id, thread_id = str(uuid4()), f"thread-{uuid4().hex}"
    metadata, scheduler, task = {}, None, None
    if scheduled:
        task = await _scheduled_occurrence(stores, run_id, thread_id)
        metadata = {"scheduled_task_id": task[0], "scheduled_task_run_id": task[1], "scheduled_trigger": "scheduled"}
        scheduler = _scheduler(stores)
    manager = RunManager(stores.runs)
    record = await manager.create_or_reject(thread_id, "lead_agent", user_id=OWNER, metadata=metadata, run_id=run_id)
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    housekeeping = []
    original_task = worker._create_contextless_task

    def track(coroutine):
        created = original_task(coroutine)
        housekeeping.append(created)
        return created

    monkeypatch.setattr(worker, "_create_contextless_task", track)
    token = set_current_user(SimpleNamespace(id=OWNER))
    try:
        await run_agent(
            bridge,
            manager,
            record,
            ctx=RunContext(
                checkpointer=None,
                event_store=None,
                app_config=AppConfig(models=[], sandbox={"use": "deerflow.community.aio_sandbox:AioSandboxProvider"}),
                on_run_completed=scheduler.handle_run_completion if scheduler is not None else None,
            ),
            agent_factory=lambda *, config: graph,
            graph_input={"messages": [HumanMessage("Build the weekly sales report.")]},
            config={"configurable": {"thread_id": thread_id}},
        )
        stored = await stores.runs.get(run_id, user_id=OWNER)
        assert stored is not None and record.terminal_confirmed
        bridge.publish_end.assert_awaited_once_with(run_id)
    finally:
        reset_current_user(token)
        for created in housekeeping:
            if "RunManager.cleanup" in created.get_coro().__qualname__ and not created.done():
                created.cancel()  # Only the existing post-terminal eviction delay.
        await asyncio.gather(*housekeeping, return_exceptions=True)
        await manager.shutdown(timeout=0)
    outcome = None
    if task is not None:
        occurrence = next(row for row in await stores.occurrences.list_by_task(task[0]) if row["id"] == task[1])
        schedule = await stores.schedules.get(task[0], user_id=OWNER)
        outcome = SimpleNamespace(status=occurrence["status"], error=occurrence["error"], last_error=schedule["last_error"])
    return record, stored, outcome


def _tool_results(model) -> list[ToolMessage]:
    return [message for message in model.received[-1] if isinstance(message, ToolMessage)]


@pytest.mark.asyncio
async def test_a_scheduled_occurrence_that_meets_both_slots_held_by_people_ends_failed_with_the_reason(stores, slots_held_by_people, monkeypatch):
    model = _bash_then(BUSY_ANSWER)

    record, stored, outcome = await _turn(stores, _graph(model, [bash_tool]), scheduled=True, monkeypatch=monkeypatch)

    # The model was told to wait, as an operational state, not handed the raw refusal.
    [refusal] = _tool_results(model)
    assert refusal.status == "error"
    assert refusal.additional_kwargs[TOOL_META_KEY]["error_type"] == "capacity"
    assert "room for" in refusal.content and "SANDBOX_CAPACITY_EXCEEDED" not in refusal.content, refusal.content
    assert _live(slots_held_by_people.backend) == slots_held_by_people.held, "no third container was started"
    assert (record.status, stored["status"], stored["stop_reason"]) == (RunStatus.success, "success", "sandbox_capacity_exceeded")
    assert (outcome.status, outcome.error, outcome.last_error) == ("failed", REFUSED, REFUSED)


@pytest.mark.asyncio
async def test_a_persons_turn_that_survives_the_refusal_records_it_and_stays_an_ordinary_success(stores, slots_held_by_people, monkeypatch):
    """Not an ``error`` row: nothing that retries refused turns acts on it."""
    model = _bash_then(BUSY_ANSWER)

    record, stored, _ = await _turn(stores, _graph(model, [bash_tool]), scheduled=False, monkeypatch=monkeypatch)

    assert (record.status, stored["status"], stored["stop_reason"], stored["error"]) == (RunStatus.success, "success", "sandbox_capacity_exceeded", None)


@pytest.mark.asyncio
async def test_a_turn_whose_later_call_got_a_sandbox_is_not_recorded_as_refused(stores, slots_held_by_people, monkeypatch):
    """The refusal only describes the turn when its sandboxed work never ran."""
    held = slots_held_by_people

    @tool
    async def check_workspace(runtime: ToolRuntime) -> str:
        """Look at the workspace after a person's turn has finished."""
        held.provider.release(held.held[0])
        await ensure_sandbox_initialized_async(runtime)
        return "The workspace is ready."

    retry = AIMessage(content="", tool_calls=[{"id": "check-1", "name": "check_workspace", "args": {}}])
    model = _bash_then("The report is ready.", retry)

    record, stored, outcome = await _turn(stores, _graph(model, [bash_tool, check_workspace]), scheduled=True, monkeypatch=monkeypatch)

    assert (stored["status"], stored["stop_reason"]) == ("success", None)
    assert (outcome.status, outcome.error, outcome.last_error) == ("success", None, None)


@pytest.mark.asyncio
async def test_a_guard_capped_scheduled_run_ends_failed_with_words_about_the_limit(stores, monkeypatch):
    @tool
    def look_up_sales(month: str) -> str:
        """Look up a month's sales."""
        return "No rows yet."

    model = RepeatingModel(responses=[AIMessage(content="unused")])
    graph = create_agent(model=model, tools=[look_up_sales], middleware=[LoopDetectionMiddleware(warn_threshold=2, hard_limit=3, window_size=20), ToolErrorHandlingMiddleware()])

    _record, stored, outcome = await _turn(stores, graph, scheduled=True, monkeypatch=monkeypatch)

    assert (stored["status"], stored["stop_reason"]) == ("success", "loop_capped")
    assert (outcome.status, outcome.error, outcome.last_error) == ("failed", LOOPED, LOOPED)


@pytest.mark.asyncio
async def test_a_scheduled_run_with_no_reason_still_ends_success(stores, monkeypatch):
    model = ScriptedModel(responses=[AIMessage(content="Here is the report.")])

    _record, stored, outcome = await _turn(stores, _graph(model, []), scheduled=True, monkeypatch=monkeypatch)

    assert (stored["status"], stored["stop_reason"]) == ("success", None)
    assert (outcome.status, outcome.error, outcome.last_error) == ("success", None, None)


# The same run row, however its occurrence is finalized. The completion hook
# can miss (a deploy cancels it after the run committed, the Gateway restarts,
# the hook raises); the poller and startup recovery then finish the occurrence
# from the durable run alone.


async def _finished_run(stores, *, stop_reason: str | None, goal_objective: str | None = None) -> SimpleNamespace:
    run_id, thread_id = str(uuid4()), f"thread-{uuid4().hex}"
    task_id, occurrence_id = await _scheduled_occurrence(stores, run_id, thread_id)
    if goal_objective is not None:
        async with stores.session_factory() as session:
            await session.execute(update(ScheduledTaskRunRow).where(ScheduledTaskRunRow.id == occurrence_id).values(goal_objective=goal_objective))
            await session.commit()
    await stores.runs.put(run_id, thread_id=thread_id, assistant_id="lead_agent", user_id=OWNER, status="success", stop_reason=stop_reason, metadata={"scheduled_task_id": task_id, "scheduled_task_run_id": occurrence_id})
    return SimpleNamespace(task_id=task_id, occurrence_id=occurrence_id, run_id=run_id)


async def _outcome(stores, run) -> SimpleNamespace:
    occurrence = next(row for row in await stores.occurrences.list_by_task(run.task_id) if row["id"] == run.occurrence_id)
    schedule = await stores.schedules.get(run.task_id, user_id=OWNER)
    return SimpleNamespace(status=occurrence["status"], error=occurrence["error"], last_error=schedule["last_error"])


async def _by_the_hook(stores, run, stop_reason, goal_verdict=None):
    kwargs = {"goal_verdict": goal_verdict} if goal_verdict is not None else {}
    await stores.schedules.complete_run(run.task_id, user_id=OWNER, task_run_id=run.occurrence_id, run_id=run.run_id, status="success", error=None, finished_at=datetime.now(UTC), only_if_active=True, stop_reason=stop_reason, **kwargs)


async def _by_the_poller(stores, run, stop_reason):
    assert await stores.schedules.reconcile_terminal_runs(now=datetime.now(UTC)) == 1


async def _at_startup(stores, run, stop_reason):
    assert await stores.occurrences.mark_stale_active_runs(error="restarted") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("finish", [_by_the_hook, _by_the_poller, _at_startup], ids=["completion-hook", "poller-reconcile", "startup-recovery"])
@pytest.mark.parametrize(("stop_reason", "expected"), [("sandbox_capacity_exceeded", ("failed", REFUSED, REFUSED)), (None, ("success", None, None))], ids=["refused", "no-reason"])
async def test_every_way_of_finishing_an_occurrence_reads_the_same_run_the_same_way(stores, finish, stop_reason, expected):
    run = await _finished_run(stores, stop_reason=stop_reason)

    await finish(stores, run, stop_reason)

    outcome = await _outcome(stores, run)
    assert (outcome.status, outcome.error, outcome.last_error) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("stop_reason", "words"),
    [
        ("sandbox_capacity_exceeded", REFUSED),
        ("loop_capped", LOOPED),
        ("token_capped", "the task used up the amount of text it is allowed, so it stopped before it finished"),
        ("safety_capped", "the model declined to go on, so the task stopped before it finished"),
        ("model_length_capped", "the model's answer was cut off at its length limit, so the task stopped before it finished"),
        # The cap drops the extra delegations and the run may go on: the words say only what is true either way.
        ("subagent_limit_capped", "the task asked for more helpers than it is allowed, so the work they would have done was not done"),
        ("extension_guard_capped", "the task stopped before it finished"),
    ],
    ids=["capacity", "loop", "token", "safety", "model-length", "subagent-limit", "unnamed"],
)
async def test_an_occurrence_that_did_not_do_the_task_says_why_in_words(stores, stop_reason, words):
    run = await _finished_run(stores, stop_reason=stop_reason)

    await _by_the_hook(stores, run, stop_reason)

    outcome = await _outcome(stores, run)
    assert (outcome.status, outcome.error, outcome.last_error) == ("failed", words, words)


@pytest.mark.asyncio
@pytest.mark.parametrize(("satisfied", "expected_status"), [(True, "success"), (False, "unmet")])
async def test_a_goal_occurrence_capped_by_a_guard_is_still_judged_by_its_verdict(stores, satisfied, expected_status):
    run = await _finished_run(stores, stop_reason="loop_capped", goal_objective="The report is in outputs")

    await _by_the_hook(stores, run, "loop_capped", goal_verdict={"satisfied": satisfied, "stand_down_reason": None if satisfied else "loop_capped"})

    assert (await _outcome(stores, run)).status == expected_status


def test_the_store_names_the_refusal_by_the_reason_the_run_carries():
    assert SandboxCapacityExceededError.run_stop_reason in STOPPED_EARLY_ERRORS
