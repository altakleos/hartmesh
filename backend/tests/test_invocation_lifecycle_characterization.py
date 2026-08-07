"""Characterize the production entry points into durable run admission.

Durable ``RunRow`` creation inventory discovered from production source:

* Normal graph runs enter through ``app.runtime.InvocationRuntime``. The
  Gateway's five create/stream/wait HTTP variants use the ``start_run``
  compatibility adapter; Scheduled Task occurrences and in-process native
  channels call the runtime directly. A standalone channel manager retains
  the SDK create/wait/stream transport when Gateway is a real process boundary.
* Checkpoint mutations call ``reserve_checkpoint_write`` and create a temporary
  ``operation_kind=checkpoint_write`` row through
  ``RunManager.reserve_thread_operation``.
* Artifact edits call ``reserve_artifact_write`` and create a temporary
  ``operation_kind=artifact_write`` row through the same reservation boundary.
* ``RunManager.create`` and missing-row persistence repair use
  ``RunRepository.put``. No inspected production entry point calls
  ``RunManager.create``; it remains a compatibility/testing primitive, while
  missing-row repair restores an already-admitted record rather than launching
  graph execution.

``RunRepository.put`` and ``create_thread_operation_atomic`` are the only SQL
constructors of ``RunRow``. Production admission uses the latter through
``RunManager``; the former is the idempotent persistence primitive described
above.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from langchain.agents.middleware import AgentMiddleware
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from support.scheduled_task_runtime import CallbackInvocationRuntime

from app.channels.manager import ChannelManager
from app.channels.message_bus import (
    PENDING_CLARIFICATION_METADATA_KEY,
    InboundMessage,
    MessageBus,
)
from app.channels.store import ChannelStore
from app.gateway.routers import runs as stateless_runs
from app.gateway.routers import thread_runs
from app.gateway.run_models import RunCreateRequest
from deerflow.config.app_config import AppConfig, reset_app_config, set_app_config
from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
from deerflow.runtime import (
    ConflictError,
    DisconnectMode,
    RunManager,
    RunStatus,
    ThreadOperationKind,
)
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import CancelOutcome, RunRecord
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge


@pytest.fixture
def runtime_app_config():
    set_app_config(AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}}))
    yield
    reset_app_config()


def _record(run_id: str, thread_id: str, body: RunCreateRequest) -> RunRecord:
    now = datetime.now(UTC).isoformat()
    return RunRecord(
        run_id=run_id,
        thread_id=thread_id,
        assistant_id=body.assistant_id,
        status=RunStatus.pending,
        on_disconnect=DisconnectMode.continue_,
        multitask_strategy=body.multitask_strategy,
        created_at=now,
        updated_at=now,
    )


@pytest.mark.anyio
async def test_gateway_create_stream_wait_routes_share_durable_admission(monkeypatch):
    expected_paths = {
        "/api/runs/stream",
        "/api/runs/wait",
        "/api/threads/{thread_id}/runs",
        "/api/threads/{thread_id}/runs/stream",
        "/api/threads/{thread_id}/runs/wait",
    }
    actual_paths = {route.path for router in (stateless_runs.router, thread_runs.router) for route in router.routes if "POST" in getattr(route, "methods", set()) and route.path in expected_paths}
    assert actual_paths == expected_paths

    admissions: list[tuple[str, str]] = []

    async def durable_start_run(body, thread_id, _request, **_kwargs):
        admissions.append((thread_id, body.multitask_strategy))
        return _record(f"run-{len(admissions)}", thread_id, body)

    monkeypatch.setattr(stateless_runs, "start_run", durable_start_run)
    monkeypatch.setattr(thread_runs, "start_run", durable_start_run)

    runtime_state = SimpleNamespace(stream_bridge=object(), run_manager=object())
    request = SimpleNamespace(
        app=SimpleNamespace(state=runtime_state),
        headers={},
        state=SimpleNamespace(),
    )
    stateless_body = RunCreateRequest(
        input={"messages": [{"role": "user", "content": "hello"}]},
        config={"configurable": {"thread_id": "stateless-thread"}},
        multitask_strategy="rollback",
    )
    thread_body = RunCreateRequest(
        input={"messages": [{"role": "user", "content": "hello"}]},
        multitask_strategy="rollback",
    )

    stream_response = await stateless_runs.stateless_stream(stateless_body, request)
    wait_response = await stateless_runs.stateless_wait(stateless_body, request)
    create_response = await inspect.unwrap(thread_runs.create_run)("thread-route", thread_body, request)
    thread_stream_response = await inspect.unwrap(thread_runs.stream_run)("thread-route", thread_body, request)
    thread_wait_response = await inspect.unwrap(thread_runs.wait_run)("thread-route", thread_body, request)

    assert stream_response.headers["content-location"].endswith("/runs/run-1")
    assert wait_response == {"status": "pending", "error": None}
    assert create_response.status == "pending"
    assert thread_stream_response.headers["content-location"].endswith("/runs/run-4")
    assert thread_wait_response == {"status": "pending", "error": None}
    assert admissions == [
        ("stateless-thread", "rollback"),
        ("stateless-thread", "rollback"),
        ("thread-route", "rollback"),
        ("thread-route", "rollback"),
        ("thread-route", "rollback"),
    ]


def _make_start_request(run_manager: RunManager):
    store = InMemoryStore()
    return SimpleNamespace(
        headers={},
        state=SimpleNamespace(auth_source=None),
        app=SimpleNamespace(
            state=SimpleNamespace(
                stream_bridge=SimpleNamespace(),
                run_manager=run_manager,
                checkpointer=InMemorySaver(),
                store=store,
                run_event_store=MemoryRunEventStore(),
                run_events_config=None,
                thread_store=MemoryThreadMetaStore(store),
                checkpoint_channel_mode="full",
                scheduled_task_service=None,
            )
        ),
    )


@pytest.mark.anyio
async def test_gateway_admits_durably_before_worker_attachment_and_rejects_conflict(
    monkeypatch,
    runtime_app_config,
):
    from app.gateway import services

    run_store = MemoryRunStore()
    run_manager = RunManager(store=run_store)
    request = _make_start_request(run_manager)
    body = RunCreateRequest(
        input={"messages": [{"role": "user", "content": "hello"}]},
        multitask_strategy="reject",
    )
    graph_started = asyncio.Event()
    release_graph = asyncio.Event()
    durable_rows_seen_by_graph: list[dict] = []

    async def fake_run_agent(_bridge, _manager, record, **_kwargs):
        stored = await run_store.get(record.run_id, user_id=record.user_id)
        assert stored is not None
        durable_rows_seen_by_graph.append(dict(stored))
        graph_started.set()
        await release_graph.wait()

    monkeypatch.setattr(services, "resolve_agent_factory", lambda _assistant_id: object())
    monkeypatch.setattr(services, "run_agent", fake_run_agent)

    record = await services.start_run(body, "thread-durable", request)
    assert record.task is not None
    assert await run_store.get(record.run_id, user_id=record.user_id) is not None
    await graph_started.wait()

    with pytest.raises(HTTPException) as exc_info:
        await services.start_run(body, "thread-durable", request)
    assert exc_info.value.status_code == 409
    assert len(durable_rows_seen_by_graph) == 1
    assert durable_rows_seen_by_graph[0]["operation_kind"] == "run"

    release_graph.set()
    await record.task
    assert await run_manager.cancel(record.run_id) is CancelOutcome.cancelled


@pytest.mark.anyio
async def test_multitask_strategies_preserve_current_active_thread_behavior():
    reject_manager = RunManager(store=MemoryRunStore())
    await reject_manager.create_or_reject("thread-reject")
    with pytest.raises(ConflictError, match="already has an active run"):
        await reject_manager.create_or_reject("thread-reject", multitask_strategy="reject")

    for strategy in ("interrupt", "rollback"):
        store = MemoryRunStore()
        manager = RunManager(store=store)
        previous = await manager.create_or_reject(f"thread-{strategy}")
        replacement = await manager.create_or_reject(f"thread-{strategy}", multitask_strategy=strategy)

        assert (await store.get(previous.run_id))["status"] == "interrupted"
        assert (await store.get(replacement.run_id))["status"] == "pending"
        assert replacement.multitask_strategy == strategy


class _ChannelConnectionRepo:
    def __init__(self):
        self.lookups: list[dict] = []

    async def find_connection_by_external_identity(self, **kwargs):
        self.lookups.append(kwargs)
        return {
            "id": "connection-1",
            "owner_user_id": "owner-1",
        }

    async def get_thread_id(self, _connection_id, _chat_id, _topic_id):
        return "channel-thread"


@pytest.mark.anyio
@pytest.mark.parametrize("launch_mode", ["create", "stream", "wait"])
async def test_standalone_native_channel_launch_modes_select_sdk_transport(
    launch_mode,
    tmp_path,
    monkeypatch,
):
    from app.channels.run_policy import CHANNEL_RUN_POLICY, ChannelRunPolicy

    channel_name = f"characterization-{launch_mode}"
    if launch_mode == "create":
        monkeypatch.setitem(
            CHANNEL_RUN_POLICY,
            channel_name,
            ChannelRunPolicy(fire_and_forget=True),
        )

    async def stream_result():
        yield SimpleNamespace(
            event="values",
            data={"messages": [{"type": "ai", "content": "streamed"}]},
        )

    client = SimpleNamespace(
        runs=SimpleNamespace(
            create=AsyncMock(return_value={"run_id": "run-create"}),
            stream=MagicMock(side_effect=lambda *_args, **_kwargs: stream_result()),
            wait=AsyncMock(return_value={"messages": [{"type": "ai", "content": "waited"}]}),
        )
    )
    manager = ChannelManager(
        bus=MessageBus(),
        store=ChannelStore(path=tmp_path / f"{launch_mode}.json"),
    )
    monkeypatch.setattr(
        manager,
        "_channel_supports_streaming",
        lambda _channel_name: launch_mode == "stream",
    )
    message = InboundMessage(
        channel_name=channel_name,
        chat_id="chat-1",
        user_id="provider-user",
        text="hello",
        metadata={"agent_name": "incident-agent"},
    )

    await manager._handle_chat_on_thread(client, message, "channel-thread")

    selected = {
        "create": client.runs.create.call_count,
        "stream": client.runs.stream.call_count,
        "wait": client.runs.wait.call_count,
    }
    assert selected == {
        "create": int(launch_mode == "create"),
        "stream": int(launch_mode == "stream"),
        "wait": int(launch_mode == "wait"),
    }
    selected_call = getattr(client.runs, launch_mode).call_args
    assert selected_call.args == ("channel-thread", "lead_agent")
    assert selected_call.kwargs["multitask_strategy"] == "reject"
    assert selected_call.kwargs["context"]["agent_name"] == "incident-agent"


@pytest.mark.anyio
async def test_native_channel_revalidates_owner_dedupes_and_continues_clarification(
    tmp_path,
    monkeypatch,
):
    monkeypatch.delenv("DEER_FLOW_AUTH_DISABLED", raising=False)
    bus = MessageBus()
    repo = _ChannelConnectionRepo()
    manager = ChannelManager(
        bus=bus,
        store=ChannelStore(path=tmp_path / "channel-store.json"),
        connection_repo=repo,
        require_bound_identity=True,
    )
    outbound = []
    bus.subscribe_outbound(outbound.append)

    clarification = {
        "messages": [
            {"type": "human", "content": "deploy"},
            {
                "type": "ai",
                "content": "",
                "tool_calls": [{"name": "ask_clarification", "args": {}}],
            },
            {
                "type": "tool",
                "name": "ask_clarification",
                "content": "Which environment?",
            },
        ]
    }
    completed = {
        "messages": [
            {"type": "human", "content": "prod"},
            {"type": "ai", "content": "Deploying to prod."},
        ]
    }
    client = SimpleNamespace(
        threads=SimpleNamespace(update=AsyncMock()),
        runs=SimpleNamespace(
            wait=AsyncMock(side_effect=[clarification, completed]),
            create=AsyncMock(),
            stream=MagicMock(),
        ),
    )
    manager._client = client

    forged = InboundMessage(
        channel_name="slack",
        chat_id="C1",
        user_id="U1",
        workspace_id="T1",
        connection_id="forged-connection",
        owner_user_id="forged-owner",
        text="forged",
    )
    await manager._handle_chat(forged)
    client.runs.wait.assert_not_called()

    first = InboundMessage(
        channel_name="slack",
        chat_id="C1",
        user_id="U1",
        workspace_id="T1",
        connection_id="connection-1",
        owner_user_id="owner-1",
        text="deploy",
        metadata={"message_id": "provider-1", "agent_name": "incident-agent"},
    )
    duplicate = InboundMessage(**first.__dict__)
    second = InboundMessage(
        channel_name="slack",
        chat_id="C1",
        user_id="U1",
        workspace_id="T1",
        connection_id="connection-1",
        owner_user_id="owner-1",
        text="prod",
        metadata={"message_id": "provider-2", "agent_name": "incident-agent"},
    )

    for message in (first, duplicate, second):
        if not await manager._is_duplicate_inbound(message):
            await manager._handle_chat(message)

    assert client.runs.wait.call_count == 2
    for call in client.runs.wait.call_args_list:
        assert call.args == ("channel-thread", "lead_agent")
        assert call.kwargs["multitask_strategy"] == "reject"
        assert call.kwargs["context"]["agent_name"] == "incident-agent"
        assert call.kwargs["context"]["user_id"] == "owner-1"
    assert outbound[-2].text == "Which environment?"
    assert outbound[-2].metadata[PENDING_CLARIFICATION_METADATA_KEY] is True
    assert outbound[-1].text == "Deploying to prod."
    assert PENDING_CLARIFICATION_METADATA_KEY not in outbound[-1].metadata
    assert len(repo.lookups) == 3


class _TaskRepo:
    def __init__(self):
        self.updated: list[tuple[tuple, dict]] = []

    async def update_after_launch(self, *args, **kwargs):
        self.updated.append((args, kwargs))


class _TaskRunRepo:
    def __init__(self, events: list[tuple], *, active: bool = False):
        self.events = events
        self.active = active
        self.created: list[dict] = []
        self.updated: list[tuple[str, dict]] = []

    async def has_active_runs(self, _task_id):
        return self.active

    async def create(self, **kwargs):
        self.events.append(("task_run_created", kwargs["run_record_id"]))
        self.created.append(kwargs)
        return {"id": kwargs["run_record_id"]}

    async def update_status(self, run_record_id, **kwargs):
        self.updated.append((run_record_id, kwargs))


def _scheduled_task(*, context_mode: str) -> dict:
    return {
        "id": "task-1",
        "user_id": "owner-1",
        "thread_id": "scheduled-thread",
        "context_mode": context_mode,
        "assistant_id": "report-agent",
        "prompt": "prepare report",
        "schedule_type": "cron",
        "schedule_spec": {"cron": "0 9 * * *"},
        "timezone": "UTC",
        "status": "enabled",
        "overlap_policy": "skip",
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("context_mode", "reuses_thread"),
    [("reuse_thread", True), ("fresh_thread_per_run", False)],
)
async def test_scheduled_task_persists_task_run_before_durable_launch(
    context_mode,
    reuses_thread,
):
    from app.scheduler.service import ScheduledTaskService

    events: list[tuple] = []
    task_repo = _TaskRepo()
    task_run_repo = _TaskRunRepo(events)

    async def launch_run(**kwargs):
        events.append(("launch", kwargs["metadata"]["scheduled_task_run_id"]))
        assert task_run_repo.created
        return {"run_id": "run-scheduled", "thread_id": kwargs["thread_id"]}

    service = ScheduledTaskService(
        task_repo=task_repo,
        task_run_repo=task_run_repo,
        invocation_runtime=CallbackInvocationRuntime(launch_run),
        poll_interval_seconds=5,
        lease_seconds=30,
        max_concurrent_runs=2,
    )
    result = await service.dispatch_task(
        _scheduled_task(context_mode=context_mode),
        now=datetime(2026, 8, 6, tzinfo=UTC),
        trigger="scheduled",
    )

    task_run_id = task_run_repo.created[0]["run_record_id"]
    assert events == [
        ("task_run_created", task_run_id),
        ("launch", task_run_id),
    ]
    assert result["task_run_id"] == task_run_id
    assert result["outcome"] == "launched"
    assert (result["thread_id"] == "scheduled-thread") is reuses_thread
    assert task_run_repo.updated[0][1]["run_id"] == "run-scheduled"


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("trigger", "outcome", "creates_skip_tombstone"),
    [("scheduled", "skipped", True), ("manual", "conflict", False)],
)
async def test_scheduled_task_overlap_never_crosses_launch_boundary(
    trigger,
    outcome,
    creates_skip_tombstone,
):
    from app.scheduler.service import ScheduledTaskService

    events: list[tuple] = []
    task_run_repo = _TaskRunRepo(events, active=True)
    launches = []

    async def launch_run(**kwargs):
        launches.append(kwargs)
        return {"run_id": "unreachable", "thread_id": kwargs["thread_id"]}

    service = ScheduledTaskService(
        task_repo=_TaskRepo(),
        task_run_repo=task_run_repo,
        invocation_runtime=CallbackInvocationRuntime(launch_run),
        poll_interval_seconds=5,
        lease_seconds=30,
        max_concurrent_runs=2,
    )
    result = await service.dispatch_task(
        _scheduled_task(context_mode="reuse_thread"),
        now=datetime(2026, 8, 6, tzinfo=UTC),
        trigger=trigger,
    )

    assert result["outcome"] == outcome
    assert launches == []
    assert bool(task_run_repo.created) is creates_skip_tombstone
    if creates_skip_tombstone:
        assert task_run_repo.created[0]["status"] == "skipped"
    else:
        assert result["task_run_id"] is None


@pytest.mark.anyio
async def test_scheduled_runtime_boundary_marks_noninteractive():
    from app.scheduler.service import ScheduledTaskService

    captured = {}

    async def launch_run(**kwargs):
        captured.update(kwargs)
        return {"run_id": "run-1", "thread_id": kwargs["thread_id"]}

    service = ScheduledTaskService(
        task_repo=_TaskRepo(),
        task_run_repo=_TaskRunRepo([]),
        invocation_runtime=CallbackInvocationRuntime(launch_run),
        poll_interval_seconds=5,
        lease_seconds=30,
        max_concurrent_runs=2,
    )
    result = await service.dispatch_task(
        _scheduled_task(context_mode="reuse_thread"),
        now=datetime(2026, 8, 6, tzinfo=UTC),
        trigger="scheduled",
    )

    assert captured["thread_id"] == "scheduled-thread"
    assert captured["context"] == {"non_interactive": True, "user_id": "owner-1"}
    assert captured["multitask_strategy"] == "reject"
    assert captured["metadata"]["scheduled_task_run_id"] == result["task_run_id"]


@pytest.mark.anyio
async def test_cancellation_terminal_status_leases_and_orphan_recovery():
    local_store = MemoryRunStore()
    local_manager = RunManager(store=local_store)
    cancelled = await local_manager.create_or_reject("thread-cancel")
    assert await local_manager.try_start(cancelled.run_id)
    assert await local_manager.cancel(cancelled.run_id) is CancelOutcome.cancelled
    assert (await local_store.get(cancelled.run_id))["status"] == "interrupted"

    terminal = await local_manager.create_or_reject("thread-terminal")
    await local_manager.try_start(terminal.run_id)
    await local_manager.set_status(terminal.run_id, RunStatus.success)
    assert await local_manager.cancel(terminal.run_id) is CancelOutcome.not_cancellable
    assert (await local_store.get(terminal.run_id))["status"] == "success"

    ownership = RunOwnershipConfig(
        heartbeat_enabled=True,
        lease_seconds=30,
        grace_seconds=0,
    )
    shared_store = MemoryRunStore()
    owner = RunManager(
        store=shared_store,
        worker_id="worker-a",
        run_ownership_config=ownership,
    )
    peer = RunManager(
        store=shared_store,
        worker_id="worker-b",
        run_ownership_config=ownership,
    )
    leased = await owner.create_or_reject("thread-leased")
    assert await peer.cancel(leased.run_id) is CancelOutcome.requested
    assert (await shared_store.get(leased.run_id))["cancel_action"] == "interrupt"

    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    await shared_store.put(
        "orphan-run",
        thread_id="thread-orphan",
        status="running",
        owner_worker_id="dead-worker",
        lease_expires_at=expired,
        created_at=expired,
    )
    recovered = await peer.reconcile_orphaned_inflight_runs(error="worker lost")
    assert [record.run_id for record in recovered] == ["orphan-run"]
    assert (await shared_store.get("orphan-run"))["status"] == "error"


@pytest.mark.anyio
async def test_stream_and_durable_event_cursors_resume_exclusively():
    from app.gateway.services import sse_consumer

    bridge = MemoryStreamBridge()
    await bridge.publish("run-cursor", "values", {"value": "one"})
    await bridge.publish("run-cursor", "values", {"value": "two"})
    first = await anext(bridge.subscribe("run-cursor"))
    await bridge.publish_end("run-cursor")
    record = RunRecord(
        run_id="run-cursor",
        thread_id="thread-cursor",
        assistant_id="lead_agent",
        status=RunStatus.success,
        on_disconnect=DisconnectMode.continue_,
    )
    request = SimpleNamespace(
        headers={"Last-Event-ID": first.id},
        is_disconnected=AsyncMock(return_value=False),
    )
    frames = [
        frame
        async for frame in sse_consumer(
            bridge,
            record,
            request,
            SimpleNamespace(),
        )
    ]
    rendered = "".join(frames)
    assert '"two"' in rendered
    assert '"one"' not in rendered
    assert f"id: {first.id}\n" not in rendered

    event_store = MemoryRunEventStore()
    for value in ("one", "two", "three"):
        await event_store.put(
            thread_id="thread-cursor",
            run_id="run-cursor",
            event_type="custom",
            category="debug",
            content=value,
        )
    event_request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(run_event_store=event_store)))
    events = await inspect.unwrap(thread_runs.list_run_events)(
        "thread-cursor",
        "run-cursor",
        event_request,
        event_types=None,
        task_id=None,
        limit=500,
        after_seq=1,
    )
    assert [event["seq"] for event in events] == [2, 3]


def test_required_plugin_load_is_fail_closed_but_observer_runtime_is_fail_open():
    from deerflow.extensions.isolation import IsolatedMiddleware
    from deerflow.extensions.loader import (
        ExtensionLoadError,
        ExtensionSpec,
        load_extensions,
    )

    with pytest.raises(ExtensionLoadError):
        load_extensions([ExtensionSpec(use="missing_invocation_plugin:install", required=True)])

    class BrokenObserver(AgentMiddleware):
        def wrap_model_call(self, request, handler):
            raise RuntimeError("observation failed")

    diagnostics = []
    downstream_requests = []
    wrapped = IsolatedMiddleware(
        BrokenObserver(),
        "observer:install",
        diagnostics.append,
    )
    result = wrapped.wrap_model_call(
        "request",
        lambda request: downstream_requests.append(request) or "core-result",
    )

    assert result == "core-result"
    assert downstream_requests == ["request"]
    assert len(diagnostics) == 1
    assert diagnostics[0].source == "observer:install"


class _TrackingRunStore(MemoryRunStore):
    def __init__(self):
        super().__init__()
        self.admissions: list[dict] = []

    async def create_thread_operation_atomic(self, run_id, **kwargs):
        self.admissions.append({"run_id": run_id, **kwargs})
        return await super().create_thread_operation_atomic(run_id, **kwargs)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("reservation_name", "expected_kind"),
    [
        ("checkpoint", ThreadOperationKind.checkpoint_write),
        ("artifact", ThreadOperationKind.artifact_write),
    ],
)
async def test_internal_thread_writes_use_temporary_durable_run_rows(
    reservation_name,
    expected_kind,
):
    from app.gateway.routers.artifacts import reserve_artifact_write
    from app.gateway.services import reserve_checkpoint_write

    store = _TrackingRunStore()
    manager = RunManager(store=store)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(run_manager=manager)))
    if reservation_name == "checkpoint":
        reservation = reserve_checkpoint_write(
            request,
            "thread-operation",
            user_id="owner-1",
        )
    else:
        reservation = reserve_artifact_write(
            request,
            "thread-operation",
            user_id="owner-1",
        )

    async with reservation:
        admission = store.admissions[-1]
        assert admission["operation_kind"] == expected_kind.value
        assert await store.get(admission["run_id"], user_id="owner-1") is not None

    assert await store.get(admission["run_id"], user_id="owner-1") is None


def test_sql_runrow_constructors_match_the_documented_inventory():
    sql_source = Path(__file__).resolve().parents[1] / "packages/harness/deerflow/persistence/run/sql.py"
    tree = ast.parse(sql_source.read_text(encoding="utf-8"))
    creators = set()

    for class_node in (node for node in tree.body if isinstance(node, ast.ClassDef)):
        for function_node in (node for node in class_node.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))):
            if any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "RunRow" for node in ast.walk(function_node)):
                creators.add(f"{class_node.name}.{function_node.name}")

    assert creators == {
        "RunRepository.create_thread_operation_atomic",
        "RunRepository.put",
    }
