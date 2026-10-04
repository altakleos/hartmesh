"""Private output evidence reuse through a real compiled graph and worker."""

import asyncio
from contextlib import nullcontext
from contextvars import copy_context
from types import SimpleNamespace
from typing import Annotated
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.runtime import Runtime

from deerflow.agents.middlewares.runtime_delivery_middleware import RuntimeDeliveryMiddleware
from deerflow.config.paths import Paths
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs import worker
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.workspace_changes import recorder
from deerflow.workspace_changes.handoff import bind_output_snapshot, take_output_snapshot
from deerflow.workspace_changes.types import FileSnapshot, WorkspaceSnapshot


def _append_artifacts(left, right):
    return list(dict.fromkeys([*left, *right]))


class DeliveryState(MessagesState):
    artifacts: Annotated[list[str], _append_artifacts]


@pytest.mark.anyio
@pytest.mark.parametrize("continuation", [False, True])
@pytest.mark.parametrize("saturated_scratch", [False, True])
async def test_compiled_worker_reuses_one_scan_with_identical_delivery(tmp_path, monkeypatch, continuation, saturated_scratch):
    real_scan = recorder.scan_workspace_roots
    scans = []

    def count_scan(roots, **kwargs):
        scans.append(tuple(root.name for root in roots))
        return real_scan(roots, **kwargs)

    monkeypatch.setattr(recorder, "scan_workspace_roots", count_scan)

    async def execute(folder):
        paths = Paths(folder)
        monkeypatch.setattr(recorder, "get_paths", lambda: paths)
        manager = RunManager()
        record = await manager.create("thread-evidence")
        outputs = paths.sandbox_outputs_dir(record.thread_id, user_id=get_effective_user_id())
        outputs.mkdir(parents=True, exist_ok=True)
        (outputs / "existing.txt").write_text("old", encoding="utf-8")
        if saturated_scratch:
            scratch = paths.sandbox_work_dir(record.thread_id, user_id=get_effective_user_id())
            scratch.mkdir(parents=True, exist_ok=True)
            for i in range(2001):
                (scratch / f"scratch-{i:04d}.txt").write_text("scratch", encoding="utf-8")

        middleware = RuntimeDeliveryMiddleware(extra_excluded_dir_names=frozenset({".tool-results"}))
        calls = 0

        async def before(state, runtime: Runtime):
            return await middleware.abefore_agent(state, runtime)

        async def write(state):
            nonlocal calls
            calls += 1
            await asyncio.to_thread((outputs / f"result-{calls}.txt").write_text, f"result-{calls}", encoding="utf-8")
            return {"messages": [AIMessage("Done.", id=f"answer-{calls}")]}

        async def after(state, runtime: Runtime):
            return await middleware.aafter_agent(state, runtime)

        builder = StateGraph(DeliveryState)
        builder.add_node("before", before)
        builder.add_node("write", write)
        builder.add_node("after", after)
        builder.add_edge(START, "before")
        builder.add_edge("before", "write")
        builder.add_edge("write", "after")
        builder.add_edge("after", END)
        graph = builder.compile()
        continuation_calls = 0

        async def prepare(**kwargs):
            nonlocal continuation_calls
            continuation_calls += 1
            if continuation and continuation_calls == 1:
                return {"messages": [HumanMessage("continue", id="continuation")]}
            return None

        monkeypatch.setattr(worker, "_prepare_goal_continuation_input", prepare)
        events = MemoryRunEventStore()
        bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
        start = len(scans)
        await worker.run_agent(bridge, manager, record, ctx=worker.RunContext(checkpointer=None, event_store=events), agent_factory=lambda **kwargs: graph, graph_input={"messages": [HumanMessage("go", id="user")]}, config={})
        delivery = await events.list_events(record.thread_id, record.run_id, event_types=["run.delivery"])
        assert record.status == RunStatus.success
        assert len(delivery) == 1 and delivery[0]["content"]["satisfied"]
        return delivery[0]["content"], scans[start:], bridge.publish.call_args_list

    reused, reused_scans, reused_messages = await execute(tmp_path / "reused")
    with monkeypatch.context() as disabled:
        disabled.setattr(worker, "bind_output_snapshot", lambda *args, **kwargs: nullcontext(), raising=False)
        original, original_scans, original_messages = await execute(tmp_path / "independent")
    assert reused == original
    expected_paths = [f"/mnt/user-data/outputs/result-{i}.txt" for i in range(1, 3 if continuation else 2)]
    assert reused["produced_paths"] == expected_paths
    assert reused["matched_paths"] == expected_paths
    assert len(reused_scans) == 6 + 2 * int(continuation)
    assert len(original_scans) == len(reused_scans) + 1
    assert reused_scans.count(("outputs",)) == 3 + 2 * int(continuation)
    for messages in (reused_messages, original_messages):
        delivered = [call.args[2].get("artifacts", []) for call in messages if call.args[1] == "values"]
        assert set(expected_paths) == {path for paths in delivered for path in paths}


def _binding(**updates):
    return {"thread_id": "thread", "run_id": "run", "user_id": "owner", "extra_excluded_dir_names": frozenset({"spill"}), **updates}


@pytest.mark.parametrize("changed", [{"thread_id": "other"}, {"run_id": "other"}, {"user_id": "other"}, {"extra_excluded_dir_names": frozenset({"other"})}])
def test_a_mismatched_claim_invalidates_the_offer(changed):
    snapshot = WorkspaceSnapshot()
    with bind_output_snapshot(snapshot, **_binding()):
        assert take_output_snapshot(**_binding(**changed)) is None
        assert take_output_snapshot(**_binding()) is None


def test_only_one_matching_context_copy_can_claim_the_baseline():
    snapshot = WorkspaceSnapshot()
    with bind_output_snapshot(snapshot, **_binding()):
        copied = copy_context()
        assert copied.run(take_output_snapshot, **_binding()) is snapshot
        assert take_output_snapshot(**_binding()) is None
        assert copied.run(take_output_snapshot, **_binding()) is None


@pytest.mark.parametrize("error", [None, RuntimeError, asyncio.CancelledError])
def test_exiting_the_first_stream_invalidates_copied_contexts(error):
    def invoke():
        nonlocal copied
        with bind_output_snapshot(WorkspaceSnapshot(), **_binding()):
            copied = copy_context()
            if error:
                raise error()

    copied = None
    if error:
        with pytest.raises(error):
            invoke()
    else:
        invoke()
    assert copied.run(take_output_snapshot, **_binding()) is None


@pytest.mark.parametrize(
    "invalid", [None, WorkspaceSnapshot(truncated=True), WorkspaceSnapshot(text_cache_dir="fixture-cache"), WorkspaceSnapshot(files={"scratch": FileSnapshot(path="scratch", root="workspace", size=0, mtime_ns=0, sha256=None)})]
)
def test_an_ineligible_nested_baseline_shadows_outer_evidence(invalid):
    original = WorkspaceSnapshot()
    with bind_output_snapshot(original, **_binding()):
        with bind_output_snapshot(invalid, **_binding()):
            assert take_output_snapshot(**_binding()) is None
        assert take_output_snapshot(**_binding()) is original


@pytest.mark.anyio
async def test_a_delegated_hook_cannot_consume_the_lead_baseline(monkeypatch):
    from deerflow.sandbox.lease import SANDBOX_COMMAND_SCOPE_CONTEXT_KEY

    snapshot = WorkspaceSnapshot()
    middleware = RuntimeDeliveryMiddleware(extra_excluded_dir_names=frozenset({"spill"}))
    with bind_output_snapshot(snapshot, **_binding(user_id=get_effective_user_id())):
        runtime = SimpleNamespace(context={"thread_id": "thread", "run_id": "run", SANDBOX_COMMAND_SCOPE_CONTEXT_KEY: "delegated"})
        await middleware.abefore_agent({}, runtime)
        assert middleware._snapshots.get(("thread", "run")) is None
        runtime.context.pop(SANDBOX_COMMAND_SCOPE_CONTEXT_KEY)
        await middleware.abefore_agent({}, runtime)
        assert middleware._snapshots.get(("thread", "run")) is snapshot


@pytest.mark.anyio
@pytest.mark.parametrize("hook", ["before", "after"])
async def test_delegated_hooks_cannot_discard_a_claimed_lead_baseline(tmp_path, monkeypatch, hook):
    from deerflow.sandbox.lease import SANDBOX_COMMAND_SCOPE_CONTEXT_KEY

    paths = Paths(tmp_path)
    monkeypatch.setattr(recorder, "get_paths", lambda: paths)
    outputs = paths.sandbox_outputs_dir("thread", user_id=get_effective_user_id())
    outputs.mkdir(parents=True, exist_ok=True)
    middleware = RuntimeDeliveryMiddleware(extra_excluded_dir_names=frozenset({"spill"}))
    lead = SimpleNamespace(context={"thread_id": "thread", "run_id": "run"})
    delegated = SimpleNamespace(context={**lead.context, SANDBOX_COMMAND_SCOPE_CONTEXT_KEY: "delegated"})
    with bind_output_snapshot(WorkspaceSnapshot(), **_binding(user_id=get_effective_user_id())):
        await middleware.abefore_agent({}, lead)
        (outputs / "result.txt").write_text("result", encoding="utf-8")
        delegated_hook = middleware.abefore_agent if hook == "before" else middleware.aafter_agent
        assert await delegated_hook({}, delegated) is None
        update = await middleware.aafter_agent({"messages": [HumanMessage("go"), AIMessage("done", id="answer")]}, lead)
    assert update["artifacts"] == ["/mnt/user-data/outputs/result.txt"]


@pytest.mark.anyio
async def test_owned_outputs_created_during_assembly_are_delivered_without_public_snapshot_state(tmp_path, monkeypatch):
    import json

    from deerflow.runtime.serialization import serialize_lc_object

    paths = Paths(tmp_path)
    monkeypatch.setattr(recorder, "get_paths", lambda: paths)
    manager = RunManager()
    record = await manager.create("thread-assembly")
    outputs = paths.sandbox_outputs_dir(record.thread_id, user_id=get_effective_user_id())
    outputs.mkdir(parents=True, exist_ok=True)
    (outputs / "existing.txt").write_text("previous run", encoding="utf-8")
    middleware = RuntimeDeliveryMiddleware(extra_excluded_dir_names=frozenset({".tool-results"}))
    seen_contexts = []

    async def before(state, runtime: Runtime):
        seen_contexts.append(dict(runtime.context))
        return await middleware.abefore_agent(state, runtime)

    async def answer(state):
        return {"messages": [AIMessage("Done.", id="answer")]}

    async def after(state, runtime: Runtime):
        return await middleware.aafter_agent(state, runtime)

    builder = StateGraph(DeliveryState)
    builder.add_node("before", before)
    builder.add_node("answer", answer)
    builder.add_node("after", after)
    builder.add_edge(START, "before")
    builder.add_edge("before", "answer")
    builder.add_edge("answer", "after")
    builder.add_edge("after", END)
    graph = builder.compile()

    def factory(**kwargs):
        (outputs / "setup.txt").write_text("created during this run's assembly", encoding="utf-8")
        return graph

    events = MemoryRunEventStore()
    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())
    await worker.run_agent(bridge, manager, record, ctx=worker.RunContext(checkpointer=None, event_store=events), agent_factory=factory, graph_input={"messages": [HumanMessage("go")]}, config={})
    delivery = await events.list_events(record.thread_id, record.run_id, event_types=["run.delivery"])
    assert record.status == RunStatus.success
    assert delivery[0]["content"]["matched_paths"] == ["/mnt/user-data/outputs/setup.txt"]
    assert all(not any(isinstance(value, WorkspaceSnapshot) for value in context.values()) for context in seen_contexts)
    public = json.dumps(serialize_lc_object([call.args[2] for call in bridge.publish.call_args_list]))
    assert "deerflow_output_baseline" not in public and str(tmp_path) not in public
    assert take_output_snapshot(thread_id=record.thread_id, run_id=record.run_id, user_id=get_effective_user_id(), extra_excluded_dir_names=frozenset({".tool-results"})) is None
