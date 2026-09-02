"""Worker-level regression tests for the terminal run.delivery event (#4272 slice 1)."""

import asyncio
import logging
from contextlib import suppress
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.types import Command
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.config.app_config import AppConfig
from deerflow.config.paths import Paths
from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.config.sandbox_config import SandboxConfig
from deerflow.config.tool_output_config import ToolOutputConfig
from deerflow.persistence.base import Base
from deerflow.persistence.run import RunRepository
from deerflow.runtime.events.appender import RuntimeEventOwnershipLost
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.journal import RunJournal
from deerflow.runtime.runs.manager import (
    ConflictError,
    RunManager,
    _AdmissionTerminalDisposition,
)
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.runs.worker import (
    RunContext,
    _delivery_content_with_outputs,
    _persist_delivery_receipt,
    run_agent,
)
from deerflow.runtime.user_context import get_effective_user_id


def _make_bridge():
    return SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())


async def _delivery_events(store: MemoryRunEventStore, thread_id: str, run_id: str) -> list[dict]:
    events = await store.list_events(thread_id, run_id)
    return [e for e in events if e["event_type"] == "run.delivery"]


@pytest.mark.anyio
async def test_delivery_receipt_does_not_retry_after_runtime_ownership_loss() -> None:
    store = SimpleNamespace(put_if_absent=AsyncMock(side_effect=RuntimeEventOwnershipLost("runtime_event_ownership_lost")))

    with pytest.raises(RuntimeEventOwnershipLost, match="runtime_event_ownership_lost"):
        await _persist_delivery_receipt(
            store,
            thread_id="thread-1",
            run_id="run-1",
            content={"presented": 0, "paths": [], "by_tool": {}},
        )

    store.put_if_absent.assert_awaited_once()


def test_delivery_verification_treats_presented_directory_as_covering_produced_files():
    content = {
        "presented": 1,
        "paths": ["/mnt/user-data/outputs/site"],
        "by_tool": {"present_files": ["/mnt/user-data/outputs/site"]},
    }

    delivery = _delivery_content_with_outputs(
        content,
        [
            "/mnt/user-data/outputs/site/index.html",
            "/mnt/user-data/outputs/site/assets/style.css",
        ],
    )

    assert delivery["matched_paths"] == [
        "/mnt/user-data/outputs/site/index.html",
        "/mnt/user-data/outputs/site/assets/style.css",
    ]
    assert delivery["satisfied"] is True


@pytest.mark.anyio
async def test_delivery_event_records_present_files_paths_on_success():
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()

    class DummyAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            journal = config["context"]["__run_journal"]
            ai = AIMessage(content="", tool_calls=[{"id": "call_1", "name": "present_files", "args": {}}])
            journal._remember_current_run_tool_calls(ai, caller="lead_agent")
            journal.on_tool_end(
                Command(
                    update={
                        "artifacts": ["/mnt/user-data/outputs/report.md"],
                        "messages": [ToolMessage("Successfully presented files", tool_call_id="call_1")],
                    }
                ),
                run_id=uuid4(),
            )
            yield {"messages": []}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store),
        agent_factory=lambda *, config: DummyAgent(),
        graph_input={},
        config={},
    )
    await asyncio.sleep(0)

    delivery = await _delivery_events(store, "thread-1", record.run_id)
    assert len(delivery) == 1
    assert delivery[0]["content"]["presented"] == 1
    assert delivery[0]["content"]["paths"] == ["/mnt/user-data/outputs/report.md"]
    assert delivery[0]["content"]["by_tool"] == {"present_files": ["/mnt/user-data/outputs/report.md"]}
    fetched = await run_manager.get(record.run_id)
    assert fetched.status == RunStatus.success


@pytest.mark.anyio
async def test_delivery_event_presented_zero_without_artifact_production():
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()

    class DummyAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            yield {"messages": []}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store),
        agent_factory=lambda *, config: DummyAgent(),
        graph_input={},
        config={},
    )
    await asyncio.sleep(0)

    delivery = await _delivery_events(store, "thread-1", record.run_id)
    assert len(delivery) == 1
    assert delivery[0]["content"] == {"presented": 0, "paths": [], "by_tool": {}}
    fetched = await run_manager.get(record.run_id)
    assert fetched.status == RunStatus.success


@pytest.mark.anyio
async def test_changed_outputs_succeed_when_a_produced_output_is_presented(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._produced_output_paths",
        AsyncMock(return_value=["/mnt/user-data/outputs/report.md"]),
    )

    class DummyAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            journal = config["context"]["__run_journal"]
            ai = AIMessage(content="", tool_calls=[{"id": "call_1", "name": "present_files", "args": {}}])
            journal._remember_current_run_tool_calls(ai, caller="lead_agent")
            journal.on_tool_end(
                Command(
                    update={
                        "artifacts": ["/mnt/user-data/outputs/report.md"],
                        "messages": [ToolMessage("Successfully presented files", tool_call_id="call_1")],
                    }
                ),
                run_id=uuid4(),
            )
            yield {"messages": []}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store),
        agent_factory=lambda *, config: DummyAgent(),
        graph_input={},
        config={},
    )

    delivery = await _delivery_events(store, "thread-1", record.run_id)
    assert delivery[0]["content"] == {
        "presented": 1,
        "paths": ["/mnt/user-data/outputs/report.md"],
        "by_tool": {"present_files": ["/mnt/user-data/outputs/report.md"]},
        "verification": {
            "source": "outputs_changed",
            "requirement": "present_files_matches_produced_output",
        },
        "produced_paths": ["/mnt/user-data/outputs/report.md"],
        "presented_paths": ["/mnt/user-data/outputs/report.md"],
        "matched_paths": ["/mnt/user-data/outputs/report.md"],
        "stage": "presented",
        "satisfied": True,
    }
    assert record.status == RunStatus.success


@pytest.mark.anyio
async def test_changed_outputs_fail_closed_when_not_presented(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._produced_output_paths",
        AsyncMock(return_value=["/mnt/user-data/outputs/report.md"]),
    )

    class ProseOnlyAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            yield {"messages": [AIMessage(content="SESSION SUMMARY")]}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store),
        agent_factory=lambda *, config: ProseOnlyAgent(),
        graph_input={},
        config={},
    )

    delivery = await _delivery_events(store, "thread-1", record.run_id)
    assert delivery[0]["content"] == {
        "presented": 0,
        "paths": [],
        "by_tool": {},
        "verification": {
            "source": "outputs_changed",
            "requirement": "present_files_matches_produced_output",
        },
        "produced_paths": ["/mnt/user-data/outputs/report.md"],
        "presented_paths": [],
        "matched_paths": [],
        "stage": "not_started",
        "satisfied": False,
    }
    assert record.status == RunStatus.error
    assert record.error == "Artifact delivery incomplete: no produced output artifact was presented"
    assert record.stop_reason is None


@pytest.mark.anyio
async def test_externalized_tool_results_do_not_trigger_delivery_verification(tmp_path, monkeypatch):
    """Oversized tool outputs externalized under outputs/.tool-results/ are
    process feedback for the model, not deliverables: a run that only produced
    those files must succeed without any present_files call."""
    paths = Paths(base_dir=tmp_path)
    monkeypatch.setattr("deerflow.workspace_changes.recorder.get_paths", lambda: paths)
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()

    class ExternalizingAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            # Simulates ToolOutputBudgetMiddleware persisting an oversized tool
            # output mid-run (default storage_subdir is ".tool-results").
            tool_results = paths.sandbox_outputs_dir("thread-1", user_id=get_effective_user_id()) / ".tool-results"
            tool_results.mkdir(parents=True, exist_ok=True)
            (tool_results / "bash-abcdef123456.log").write_text("x" * 20000, encoding="utf-8")
            yield {"messages": [AIMessage(content="Here is the answer.")]}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store),
        agent_factory=lambda *, config: ExternalizingAgent(),
        graph_input={},
        config={},
    )

    delivery = await _delivery_events(store, "thread-1", record.run_id)
    assert len(delivery) == 1
    assert delivery[0]["content"] == {"presented": 0, "paths": [], "by_tool": {}}
    fetched = await run_manager.get(record.run_id)
    assert fetched.status == RunStatus.success


@pytest.mark.anyio
async def test_custom_tool_output_storage_subdir_does_not_trigger_delivery_verification(tmp_path, monkeypatch):
    """A custom tool_output.storage_subdir is honoured by the exclusion, not
    only the default .tool-results name."""
    paths = Paths(base_dir=tmp_path)
    monkeypatch.setattr("deerflow.workspace_changes.recorder.get_paths", lambda: paths)
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()
    app_config = AppConfig(sandbox=SandboxConfig(use="test"), tool_output=ToolOutputConfig(storage_subdir="tool-output-cache"))

    class ExternalizingAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            cache = paths.sandbox_outputs_dir("thread-1", user_id=get_effective_user_id()) / "tool-output-cache"
            cache.mkdir(parents=True, exist_ok=True)
            (cache / "web_fetch-abcdef123456.log").write_text("y" * 20000, encoding="utf-8")
            yield {"messages": [AIMessage(content="Here is the answer.")]}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store, app_config=app_config),
        agent_factory=lambda *, config: ExternalizingAgent(),
        graph_input={},
        config={},
    )

    fetched = await run_manager.get(record.run_id)
    assert fetched.status == RunStatus.success


@pytest.mark.anyio
async def test_changed_outputs_succeed_when_one_of_multiple_outputs_is_presented(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._produced_output_paths",
        AsyncMock(
            return_value=[
                "/mnt/user-data/outputs/report.md",
                "/mnt/user-data/outputs/appendix.md",
            ]
        ),
    )

    class PartiallyPresentingAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            journal = config["context"]["__run_journal"]
            journal._remember_current_run_tool_calls(
                AIMessage(content="", tool_calls=[{"id": "call_1", "name": "present_files", "args": {}}]),
                caller="lead_agent",
            )
            journal.on_tool_end(
                Command(
                    update={
                        "artifacts": ["/mnt/user-data/outputs/report.md"],
                        "messages": [ToolMessage("Successfully presented files", tool_call_id="call_1")],
                    }
                ),
                run_id=uuid4(),
            )
            yield {"messages": []}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store),
        agent_factory=lambda *, config: PartiallyPresentingAgent(),
        graph_input={},
        config={},
    )

    delivery = (await _delivery_events(store, "thread-1", record.run_id))[0]["content"]
    assert delivery["stage"] == "presented"
    assert delivery["presented_paths"] == ["/mnt/user-data/outputs/report.md"]
    assert delivery["matched_paths"] == ["/mnt/user-data/outputs/report.md"]
    assert delivery["satisfied"] is True
    assert record.status == RunStatus.success


@pytest.mark.anyio
async def test_changed_outputs_fail_when_present_files_only_presents_an_unrelated_file(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._produced_output_paths",
        AsyncMock(return_value=["/mnt/user-data/outputs/report.md"]),
    )

    class UnrelatedPresentingAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            journal = config["context"]["__run_journal"]
            journal._remember_current_run_tool_calls(
                AIMessage(content="", tool_calls=[{"id": "call_1", "name": "present_files", "args": {}}]),
                caller="lead_agent",
            )
            journal.on_tool_end(
                Command(
                    update={
                        "artifacts": ["/mnt/user-data/outputs/old-report.md"],
                        "messages": [ToolMessage("Successfully presented files", tool_call_id="call_1")],
                    }
                ),
                run_id=uuid4(),
            )
            yield {"messages": []}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store),
        agent_factory=lambda *, config: UnrelatedPresentingAgent(),
        graph_input={},
        config={},
    )

    delivery = (await _delivery_events(store, "thread-1", record.run_id))[0]["content"]
    assert delivery["stage"] == "mismatched"
    assert delivery["presented_paths"] == ["/mnt/user-data/outputs/old-report.md"]
    assert delivery["matched_paths"] == []
    assert delivery["satisfied"] is False
    assert record.status == RunStatus.error


@pytest.mark.anyio
async def test_fenced_worker_leaves_delivery_receipt_to_peer_recovery():
    """A stale worker must not finalize the singleton delivery receipt."""
    run_manager = RunManager()
    record = await run_manager.create("thread-lease-lost")
    record.ownership_lost = True
    record.abort_event.set()
    record.status = RunStatus.error
    event_store = MemoryRunEventStore()
    thread_store = SimpleNamespace(project_run=AsyncMock())
    on_run_completed = AsyncMock()
    agent_factory = MagicMock(side_effect=AssertionError("fenced worker started the agent"))
    bridge = _make_bridge()

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            event_store=event_store,
            thread_store=thread_store,
            on_run_completed=on_run_completed,
        ),
        agent_factory=agent_factory,
        graph_input={},
        config={},
    )

    assert await _delivery_events(event_store, record.thread_id, record.run_id) == []
    agent_factory.assert_not_called()
    thread_store.project_run.assert_not_awaited()
    on_run_completed.assert_not_awaited()
    bridge.publish_end.assert_not_awaited()
    bridge.cleanup.assert_not_awaited()


@pytest.mark.anyio
async def test_delivery_event_is_singleton_across_goal_continuations(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()
    stream_calls = 0
    continuation_calls = 0

    class ContinuingAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            nonlocal stream_calls
            stream_calls += 1
            journal = config["context"]["__run_journal"]
            tool_call_id = f"call_{stream_calls}"
            journal._remember_current_run_tool_calls(
                AIMessage(content="", tool_calls=[{"id": tool_call_id, "name": "present_files", "args": {}}]),
                caller="lead_agent",
            )
            artifacts = ["/mnt/user-data/outputs/report.md"]
            if stream_calls == 2:
                artifacts.append("/mnt/user-data/outputs/appendix.md")
            journal.on_tool_end(
                Command(
                    update={
                        "artifacts": artifacts,
                        "messages": [ToolMessage("Successfully presented files", tool_call_id=tool_call_id)],
                    }
                ),
                run_id=uuid4(),
            )
            yield {"messages": []}

    async def prepare_continuation(**kwargs):
        nonlocal continuation_calls
        continuation_calls += 1
        if continuation_calls == 1:
            return {"messages": []}
        return None

    monkeypatch.setattr("deerflow.runtime.runs.worker._prepare_goal_continuation_input", prepare_continuation)

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store),
        agent_factory=lambda *, config: ContinuingAgent(),
        graph_input={},
        config={},
    )

    delivery = await _delivery_events(store, "thread-1", record.run_id)
    assert stream_calls == 2
    assert len(delivery) == 1
    assert delivery[0]["content"] == {
        "presented": 2,
        "paths": [
            "/mnt/user-data/outputs/report.md",
            "/mnt/user-data/outputs/appendix.md",
        ],
        "by_tool": {
            "present_files": [
                "/mnt/user-data/outputs/report.md",
                "/mnt/user-data/outputs/appendix.md",
            ]
        },
    }


@pytest.mark.anyio
async def test_delivery_event_emitted_exactly_once_on_error_path():
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()

    class FailingAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            raise RuntimeError("boom")
            yield  # pragma: no cover - make this an async generator

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store),
        agent_factory=lambda *, config: FailingAgent(),
        graph_input={},
        config={},
    )
    await asyncio.sleep(0)

    delivery = await _delivery_events(store, "thread-1", record.run_id)
    assert len(delivery) == 1
    assert delivery[0]["content"]["presented"] == 0
    fetched = await run_manager.get(record.run_id)
    assert fetched.status == RunStatus.error


@pytest.mark.anyio
async def test_unexpected_worker_failure_emits_only_bounded_correlated_evidence(
    caplog,
):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()
    bridge = _make_bridge()
    secret = "provider-secret-token=sk-live-private"

    class FailingAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            raise RuntimeError(secret)
            yield  # pragma: no cover

    with caplog.at_level(logging.ERROR, logger="deerflow.runtime.runs.worker"):
        await run_agent(
            bridge,
            run_manager,
            record,
            ctx=RunContext(checkpointer=None, event_store=store),
            agent_factory=lambda *, config: FailingAgent(),
            graph_input={},
            config={},
        )

    fetched = await run_manager.get(record.run_id)
    assert fetched is not None
    assert fetched.status == RunStatus.error
    assert fetched.error is not None
    assert fetched.error.startswith("Runtime operation failed (reference: ")
    assert secret not in fetched.error
    error_payloads = [call.args[2] for call in bridge.publish.await_args_list if call.args[1] == "error"]
    assert len(error_payloads) == 1
    assert error_payloads[0]["message"] == fetched.error
    assert error_payloads[0]["name"] == "RuntimeFailure"
    terminal = await store.list_events(
        "thread-1",
        record.run_id,
        event_types=["run.terminal.v1"],
    )
    assert len(terminal) == 1
    assert terminal[0]["content"]["status"] == "error"
    assert terminal[0]["content"]["failure"]["code"] == "run_execution_failed"
    assert terminal[0]["content"]["failure"]["correlation_id"] in fetched.error
    assert secret not in caplog.text


@pytest.mark.anyio
async def test_delivery_is_durable_before_terminal_run_status():
    events = MemoryRunEventStore()

    class OrderingRunStore(MemoryRunStore):
        async def update_status(self, run_id, status, *, error=None, stop_reason=None):
            if status not in {"pending", "running"}:
                receipt = await events.list_events("thread-1", run_id, event_types=["run.delivery"])
                assert len(receipt) == 1
            return await super().update_status(run_id, status, error=error, stop_reason=stop_reason)

    run_store = OrderingRunStore()
    run_manager = RunManager(store=run_store)
    record = await run_manager.create("thread-1")

    class DummyAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            yield {"messages": []}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=events),
        agent_factory=lambda *, config: DummyAgent(),
        graph_input={},
        config={},
    )

    assert (await run_store.get(record.run_id))["status"] == "success"


@pytest.mark.anyio
async def test_delivery_write_retries_before_persisting_success():
    class FlakyReceiptStore(MemoryRunEventStore):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        async def put_if_absent(self, **kwargs):
            if kwargs.get("event_type") != "run.delivery":
                return await super().put_if_absent(**kwargs)
            self.attempts += 1
            if self.attempts == 1:
                raise RuntimeError("transient event store outage")
            return await super().put_if_absent(**kwargs)

    event_store = FlakyReceiptStore()
    run_store = MemoryRunStore()
    run_manager = RunManager(store=run_store)
    record = await run_manager.create("thread-1")

    class DummyAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            yield {"messages": []}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=event_store),
        agent_factory=lambda *, config: DummyAgent(),
        graph_input={},
        config={},
    )

    assert event_store.attempts == 2
    assert len(await _delivery_events(event_store, "thread-1", record.run_id)) == 1
    assert (await run_store.get(record.run_id))["status"] == "success"


@pytest.mark.anyio
async def test_delivery_write_failure_preserves_real_durable_terminal_status():
    class FailingReceiptStore(MemoryRunEventStore):
        def __init__(self):
            super().__init__()
            self.attempts = 0

        async def put_if_absent(self, **kwargs):
            if kwargs.get("event_type") != "run.delivery":
                return await super().put_if_absent(**kwargs)
            self.attempts += 1
            raise RuntimeError("event store unavailable")

    run_store = MemoryRunStore()
    run_manager = RunManager(store=run_store)
    record = await run_manager.create("thread-1")
    event_store = FailingReceiptStore()

    class DummyAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            yield {"messages": []}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=event_store),
        agent_factory=lambda *, config: DummyAgent(),
        graph_input={},
        config={},
    )

    # A receipt outage must not let lease recovery rewrite a genuine success
    # as an error. After bounded retries, preserve the worker's real outcome.
    assert event_store.attempts > 1
    assert record.status == RunStatus.success
    assert (await run_store.get(record.run_id))["status"] == "success"


@pytest.mark.anyio
async def test_produced_artifact_delivery_fails_closed_when_receipt_cannot_be_persisted(monkeypatch):
    class FailingReceiptStore(MemoryRunEventStore):
        async def put_if_absent(self, **kwargs):
            if kwargs.get("event_type") == "run.delivery":
                raise RuntimeError("event store unavailable")
            return await super().put_if_absent(**kwargs)

    run_store = MemoryRunStore()
    run_manager = RunManager(store=run_store)
    record = await run_manager.create("thread-1")
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._produced_output_paths",
        AsyncMock(return_value=["/mnt/user-data/outputs/report.md"]),
    )

    class PresentingAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            journal = config["context"]["__run_journal"]
            journal._remember_current_run_tool_calls(
                AIMessage(content="", tool_calls=[{"id": "call_1", "name": "present_files", "args": {}}]),
                caller="lead_agent",
            )
            journal.on_tool_end(
                Command(
                    update={
                        "artifacts": ["/mnt/user-data/outputs/report.md"],
                        "messages": [ToolMessage("Successfully presented files", tool_call_id="call_1")],
                    }
                ),
                run_id=uuid4(),
            )
            yield {"messages": []}

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=FailingReceiptStore()),
        agent_factory=lambda *, config: PresentingAgent(),
        graph_input={},
        config={},
    )

    assert record.status == RunStatus.error
    assert record.error == "Artifact delivery verification failed: terminal delivery receipt could not be persisted"
    assert (await run_store.get(record.run_id))["status"] == "error"


@pytest.mark.anyio
async def test_delivery_event_emitted_when_checkpoint_preflight_fails(monkeypatch):
    run_manager = RunManager()
    run_manager.update_run_completion = AsyncMock(wraps=run_manager.update_run_completion)
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()
    compatibility_check = AsyncMock(side_effect=RuntimeError("incompatible checkpoint"))
    monkeypatch.setattr("deerflow.runtime.runs.worker.aensure_checkpoint_mode_compatible", compatibility_check)

    def unexpected_agent_factory(**kwargs):
        raise AssertionError("agent must not be built after preflight failure")

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=object(), event_store=store),
        agent_factory=unexpected_agent_factory,
        graph_input={},
        config={},
    )

    delivery = await _delivery_events(store, "thread-1", record.run_id)
    assert len(delivery) == 1
    assert delivery[0]["content"] == {"presented": 0, "paths": [], "by_tool": {}}
    fetched = await run_manager.get(record.run_id)
    assert fetched.status == RunStatus.error
    run_manager.update_run_completion.assert_not_awaited()


@pytest.mark.anyio
async def test_delivery_event_emitted_when_cancelled_waiting_for_prior_finalization(monkeypatch):
    run_manager = RunManager()
    run_manager.update_run_completion = AsyncMock(wraps=run_manager.update_run_completion)
    record = await run_manager.create("thread-1")
    store = MemoryRunEventStore()
    monkeypatch.setattr(
        run_manager,
        "wait_for_prior_finalizing",
        AsyncMock(side_effect=asyncio.CancelledError()),
    )

    def unexpected_agent_factory(**kwargs):
        raise AssertionError("agent must not be built after preflight cancellation")

    await run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=store),
        agent_factory=unexpected_agent_factory,
        graph_input={},
        config={},
    )

    delivery = await _delivery_events(store, "thread-1", record.run_id)
    assert len(delivery) == 1
    assert delivery[0]["content"] == {"presented": 0, "paths": [], "by_tool": {}}
    fetched = await run_manager.get(record.run_id)
    assert fetched.status == RunStatus.interrupted
    run_manager.update_run_completion.assert_not_awaited()


@pytest.mark.anyio
async def test_local_cancel_keeps_sql_row_active_until_delivery_receipt(
    tmp_path,
    monkeypatch,
):
    """The API cancellation task cannot outrun worker terminal evidence."""

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'cancel-receipt-race.db'}",
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    run_store = RunRepository(session_factory)
    event_store = DbRunEventStore(session_factory)
    run_manager = RunManager(
        store=run_store,
        event_store=event_store,
        worker_id="worker-cancel-receipt",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await run_manager.create_or_reject("thread-cancel-receipt")
    retry = None
    graph_started = asyncio.Event()
    terminal_flush_entered = asyncio.Event()
    allow_terminal_flush = asyncio.Event()
    original_flush = RunJournal.flush

    async def pause_terminal_flush(journal):
        if record.abort_event.is_set() and not allow_terminal_flush.is_set():
            terminal_flush_entered.set()
            await allow_terminal_flush.wait()
        return await original_flush(journal)

    monkeypatch.setattr(RunJournal, "flush", pause_terminal_flush)

    class BlockingAgent:
        async def astream(self, *_args, **_kwargs):
            graph_started.set()
            await asyncio.Event().wait()
            yield  # pragma: no cover

    worker = asyncio.create_task(
        run_agent(
            _make_bridge(),
            run_manager,
            record,
            ctx=RunContext(
                checkpointer=None,
                event_store=event_store,
            ),
            agent_factory=lambda *, config: BlockingAgent(),
            graph_input={},
            config={},
        ),
    )
    record.task = worker

    try:
        await asyncio.wait_for(graph_started.wait(), timeout=2)
        outcome = asyncio.create_task(
            run_manager.cancel(record.run_id, action="interrupt"),
        )
        await asyncio.wait_for(terminal_flush_entered.wait(), timeout=2)
        assert await asyncio.wait_for(outcome, timeout=2)

        before_receipt = await run_store.get(record.run_id)
        assert before_receipt is not None
        assert before_receipt["status"] == "running"
        assert before_receipt["cancel_action"] == "interrupt"

        allow_terminal_flush.set()
        await asyncio.wait_for(worker, timeout=2)

        terminal = await run_store.get(record.run_id)
        assert terminal is not None
        assert terminal["status"] == "interrupted"
        events = await event_store.list_events(
            record.thread_id,
            record.run_id,
            user_id=None,
        )
        assert [event["event_type"] for event in events].count("run.delivery") == 1
        retry = await run_manager.create_or_reject(
            record.thread_id,
            candidate_run_id=str(uuid4()),
        )
        assert (await run_store.get(retry.run_id))["status"] == RunStatus.pending.value
    finally:
        allow_terminal_flush.set()
        if not worker.done():
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        if retry is not None:
            await run_manager.cancel_start_if_pending(retry.run_id)
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_attached_durable_replacement_fails_closed_without_cancel_side_effect(
    tmp_path,
    strategy,
):
    """A rejected replacement leaves the attached predecessor fully live."""

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / f'replacement-receipt-{strategy}.db'}",
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    run_store = RunRepository(session_factory)
    event_store = DbRunEventStore(session_factory)
    run_manager = RunManager(
        store=run_store,
        event_store=event_store,
        worker_id=f"worker-replacement-receipt-{strategy}",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await run_manager.create_or_reject(
        f"thread-replacement-receipt-{strategy}",
        candidate_run_id=str(uuid4()),
    )
    graph_started = asyncio.Event()

    class BlockingAgent:
        async def astream(self, *_args, **_kwargs):
            graph_started.set()
            await asyncio.Event().wait()
            yield  # pragma: no cover

    worker_coroutine = run_agent(
        _make_bridge(),
        run_manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            event_store=event_store,
        ),
        agent_factory=lambda *, config: BlockingAgent(),
        graph_input={},
        config={},
    )
    worker = await run_manager.attach_worker_once(
        record.run_id,
        worker_coroutine,
        asyncio.create_task,
    )
    candidate_run_id = str(uuid4())

    try:
        await asyncio.wait_for(graph_started.wait(), timeout=2)
        before = await run_store.get(record.run_id)
        assert before is not None
        assert before["status"] == RunStatus.running.value

        with pytest.raises(ConflictError):
            await run_manager.create_or_reject(
                record.thread_id,
                candidate_run_id=candidate_run_id,
                multitask_strategy=strategy,
            )

        assert await run_store.get(record.run_id) == before
        assert await run_store.get(candidate_run_id) is None
        assert record.abort_event.is_set() is False
        assert worker.done() is False
        assert (
            await event_store.list_events(
                record.thread_id,
                record.run_id,
                event_types=["run.delivery"],
                user_id=None,
            )
            == []
        )
    finally:
        if not worker.done():
            await run_manager.cancel(record.run_id, action="interrupt")
        await asyncio.gather(worker, return_exceptions=True)
        await run_manager.cancel_start_if_pending(candidate_run_id)
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_taskless_durable_replacement_fails_closed_without_mutation(
    strategy,
):
    """Separate run/event stores cannot atomically prepare replacement evidence."""

    run_store = MemoryRunStore()
    event_store = MemoryRunEventStore(run_store=run_store)
    run_manager = RunManager(store=run_store, event_store=event_store)
    predecessor = await run_manager.create_or_reject(
        f"thread-taskless-replacement-{strategy}",
        candidate_run_id=str(uuid4()),
    )
    candidate_run_id = str(uuid4())
    before = await run_store.get(predecessor.run_id)

    with pytest.raises(ConflictError):
        await run_manager.create_or_reject(
            predecessor.thread_id,
            candidate_run_id=candidate_run_id,
            multitask_strategy=strategy,
        )

    assert await run_store.get(predecessor.run_id) == before
    assert await run_store.get(candidate_run_id) is None
    assert (
        await _delivery_events(
            event_store,
            predecessor.thread_id,
            predecessor.run_id,
        )
        == []
    )


@pytest.mark.anyio
async def test_attached_replacement_rejects_before_store_or_predecessor_mutation():
    """Fail closed before a non-composite replacement can consume evidence."""

    class FailingReplacementStore(MemoryRunStore):
        durable_lifecycle = True
        fail_replacement = False
        replacement_attempts = 0

        async def create_thread_operation_atomic(self, *args, **kwargs):
            if self.fail_replacement:
                self.replacement_attempts += 1
                raise RuntimeError("replacement store unavailable")
            return await super().create_thread_operation_atomic(*args, **kwargs)

    run_store = FailingReplacementStore()
    event_store = MemoryRunEventStore(run_store=run_store)
    run_manager = RunManager(store=run_store, event_store=event_store)
    predecessor = await run_manager.create_or_reject(
        "thread-failed-attached-replacement",
        candidate_run_id=str(uuid4()),
    )
    worker_started = asyncio.Event()
    release_worker = asyncio.Event()

    async def attached_worker():
        worker_started.set()
        await release_worker.wait()

    worker = await run_manager.attach_worker_once(
        predecessor.run_id,
        attached_worker(),
        asyncio.create_task,
    )
    await worker_started.wait()
    assert predecessor.task is worker
    assert worker.done() is False
    assert predecessor.status is RunStatus.pending
    run_store.fail_replacement = True
    candidate_run_id = str(uuid4())
    before = await run_store.get(predecessor.run_id)
    before_receipts = await _delivery_events(
        event_store,
        predecessor.thread_id,
        predecessor.run_id,
    )

    try:
        with pytest.raises(ConflictError) as raised:
            await run_manager.create_or_reject(
                predecessor.thread_id,
                candidate_run_id=candidate_run_id,
                multitask_strategy="interrupt",
            )

        assert raised.value.active_run_id == predecessor.run_id
        assert run_store.replacement_attempts == 0
        assert await run_store.get(predecessor.run_id) == before
        assert await run_store.get(candidate_run_id) is None
        assert (
            await _delivery_events(
                event_store,
                predecessor.thread_id,
                predecessor.run_id,
            )
            == before_receipts
        )
        assert predecessor.abort_event.is_set() is False
        assert worker.done() is False
    finally:
        run_store.fail_replacement = False
        release_worker.set()
        if not worker.done():
            worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        predecessor.task = None
        predecessor.attachment_supervised = True
        await run_manager.cancel_start_if_pending(predecessor.run_id)


@pytest.mark.anyio
async def test_preentry_attached_durable_replacement_fails_closed_without_cancel_side_effect():
    """The durable guard also covers a worker attached before run_agent entry."""

    run_store = MemoryRunStore()
    event_store = MemoryRunEventStore(run_store=run_store)
    run_manager = RunManager(store=run_store, event_store=event_store)
    record = await run_manager.create_or_reject(
        "thread-pre-entry-replacement",
        candidate_run_id=str(uuid4()),
    )
    wrapper_started = asyncio.Event()
    release_worker = asyncio.Event()

    async def before_run_agent():
        wrapper_started.set()
        await release_worker.wait()

    worker = await run_manager.attach_worker_once(
        record.run_id,
        before_run_agent(),
        asyncio.create_task,
    )
    await wrapper_started.wait()
    candidate_run_id = str(uuid4())
    before = await run_store.get(record.run_id)

    try:
        with pytest.raises(ConflictError):
            await run_manager.create_or_reject(
                record.thread_id,
                candidate_run_id=candidate_run_id,
                multitask_strategy="interrupt",
            )

        assert await run_store.get(record.run_id) == before
        assert await run_store.get(candidate_run_id) is None
        assert record.abort_event.is_set() is False
        assert worker.done() is False
        assert (
            await _delivery_events(
                event_store,
                record.thread_id,
                record.run_id,
            )
            == []
        )
    finally:
        release_worker.set()
        await asyncio.gather(worker, return_exceptions=True)
        record.task = None
        record.attachment_supervised = True
        await run_manager.cancel_start_if_pending(record.run_id)


@pytest.mark.anyio
@pytest.mark.parametrize("keyed", [False, True], ids=["idempotency-key", "external-key"])
async def test_remote_replay_does_not_cancel_local_predecessor(keyed):
    """A persisted replay is resolved before replacement side effects."""

    run_store = MemoryRunStore()
    writer = RunManager(store=run_store)
    thread_id = "thread-remote-idempotency-replay"
    if keyed:
        identity = {
            "external_scope": "service:test",
            "external_key": "remote-replay-key",
            "request_digest": "a" * 64,
            "request_digest_version": "request-v1",
            "caller_intent_json": {"message": "remote replay"},
            "caller_intent_digest": "b" * 64,
            "caller_intent_digest_version": "intent-v1",
        }
        replay_admission = await writer.ensure_or_reject(thread_id, **identity)
        replay = replay_admission.record
    else:
        identity = {"idempotency_key": "remote-replay-key"}
        replay = await writer.create_or_reject(thread_id, **identity)
    await writer.set_status(replay.run_id, RunStatus.success)

    event_store = MemoryRunEventStore(run_store=run_store)
    run_manager = RunManager(store=run_store, event_store=event_store)
    predecessor = await run_manager.create_or_reject(
        replay.thread_id,
        candidate_run_id=str(uuid4()),
    )
    release_worker = asyncio.Event()

    async def blocking_worker():
        await release_worker.wait()

    worker = await run_manager.attach_worker_once(
        predecessor.run_id,
        blocking_worker(),
        asyncio.create_task,
    )
    try:
        replacement = {
            "candidate_run_id": str(uuid4()),
            "multitask_strategy": "interrupt",
            **identity,
        }
        if keyed:
            retained_admission = await run_manager.ensure_or_reject(
                replay.thread_id,
                **replacement,
            )
            retained = retained_admission.record
            assert retained_admission.outcome.value == "known_same"
        else:
            retained = await run_manager.create_or_reject(
                replay.thread_id,
                **replacement,
            )

        assert retained.run_id == replay.run_id
        if not keyed:
            assert retained.idempotency_reused
        active = await run_store.get(predecessor.run_id)
        assert active is not None
        assert active["status"] == RunStatus.pending.value
        assert active["cancel_action"] is None
        assert not predecessor.abort_event.is_set()
        assert not worker.done()
    finally:
        release_worker.set()
        await worker
        predecessor.task = None
        predecessor.attachment_supervised = True
        await run_manager.cancel_start_if_pending(predecessor.run_id)


@pytest.mark.anyio
async def test_recovery_delivery_receipt_is_visible_to_run_owner(tmp_path):
    """Background recovery stamps the durable owner, not ambient context."""

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'recovery-receipt-owner.db'}",
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    run_store = RunRepository(session_factory)
    event_store = DbRunEventStore(session_factory)
    run_manager = RunManager(
        store=run_store,
        event_store=event_store,
        worker_id="worker-recovery-receipt",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await run_manager.create_or_reject(
        "thread-recovery-receipt",
        user_id="receipt-owner",
    )

    try:
        await run_store.update_status(
            record.run_id,
            RunStatus.error.value,
            error="recovered after owner loss",
        )
        terminal = await run_store.get(record.run_id, user_id="receipt-owner")
        assert terminal is not None
        record = run_manager._record_from_store(terminal)
        assert await run_manager._ensure_delivery_receipt(record)
        owner_events = await event_store.list_events(
            record.thread_id,
            record.run_id,
            user_id="receipt-owner",
        )
        assert [event["event_type"] for event in owner_events] == ["run.delivery"]
        assert owner_events[0]["user_id"] == "receipt-owner"
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("existing_user_id", "expected_result", "expected_stored_user_id"),
    [
        pytest.param(None, True, "receipt-owner", id="legacy-null-owner"),
        pytest.param(
            "different-owner",
            False,
            "different-owner",
            id="conflicting-owner",
        ),
    ],
)
async def test_recovery_delivery_receipt_reconciles_existing_owner_identity(
    tmp_path,
    existing_user_id,
    expected_result,
    expected_stored_user_id,
):
    """Legacy NULL owners repair; contradictory owners fail closed."""

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / f'recovery-existing-{existing_user_id}.db'}",
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    run_store = RunRepository(session_factory)
    event_store = DbRunEventStore(session_factory)
    run_manager = RunManager(
        store=run_store,
        event_store=event_store,
        worker_id="worker-recovery-existing-receipt",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await run_manager.create_or_reject(
        "thread-recovery-existing-receipt",
        user_id="receipt-owner",
    )

    try:
        _, created = await event_store.put_if_absent(
            thread_id=record.thread_id,
            run_id=record.run_id,
            event_type="run.delivery",
            category="outputs",
            content={"presented": 0, "paths": [], "by_tool": {}},
            user_id=existing_user_id,
        )
        assert created
        await run_store.update_status(
            record.run_id,
            RunStatus.error.value,
            error="recovered after owner loss",
        )
        terminal = await run_store.get(record.run_id, user_id="receipt-owner")
        assert terminal is not None
        recovered = run_manager._record_from_store(terminal)

        assert await run_manager._ensure_delivery_receipt(recovered) is expected_result

        unscoped_events = await event_store.list_events(
            record.thread_id,
            record.run_id,
            user_id=None,
        )
        assert len(unscoped_events) == 1
        assert unscoped_events[0]["user_id"] == expected_stored_user_id
        owner_events = await event_store.list_events(
            record.thread_id,
            record.run_id,
            user_id="receipt-owner",
        )
        assert len(owner_events) == (1 if expected_result else 0)
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("terminalize", "expected_status"),
    [
        ("failure", RunStatus.error.value),
        ("cancellation", RunStatus.interrupted.value),
        ("rollback-cancellation", RunStatus.error.value),
    ],
)
async def test_taskless_admission_writes_delivery_before_terminal_status(
    tmp_path,
    terminalize,
    expected_status,
):
    """Creator-owned compensation cannot expose a receipt-less terminal row."""

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / f'taskless-{terminalize}.db'}",
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    run_store = RunRepository(session_factory)
    event_store = DbRunEventStore(session_factory)
    run_manager = RunManager(
        store=run_store,
        event_store=event_store,
        worker_id=f"worker-taskless-{terminalize}",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await run_manager.create_or_reject(
        f"thread-taskless-{terminalize}",
        candidate_run_id=str(uuid4()),
        user_id="taskless-owner",
    )

    try:
        assert record.task is None
        assert record.attachment_supervised
        if terminalize == "failure":
            assert await run_manager.fail_start_if_pending(
                record.run_id,
                error="startup failed",
            )
        elif terminalize == "cancellation":
            assert await run_manager.cancel_start_if_pending(record.run_id)
        else:
            assert await run_manager._close_cancelled_admission(
                record,
                action="rollback",
                claim_manager_admission=True,
            )

        terminal = await run_store.get(record.run_id, user_id="taskless-owner")
        assert terminal is not None
        assert terminal["status"] == expected_status
        if terminalize == "rollback-cancellation":
            assert terminal["error"] == "Rolled back by user"
        events = await event_store.list_events(
            record.thread_id,
            record.run_id,
            user_id="taskless-owner",
        )
        assert [event["event_type"] for event in events].count("run.delivery") == 1
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("terminalize", "expected_status"),
    [
        ("attached-failure", RunStatus.error.value),
        ("pregraph-cancellation", RunStatus.interrupted.value),
    ],
)
async def test_pregraph_terminalization_writes_delivery_receipt(
    tmp_path,
    terminalize,
    expected_status,
):
    """Every owner-fenced pregraph terminal path emits delivery first."""

    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / f'pregraph-{terminalize}.db'}",
    )
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)

    run_store = RunRepository(session_factory)
    event_store = DbRunEventStore(session_factory)
    run_manager = RunManager(
        store=run_store,
        event_store=event_store,
        worker_id=f"worker-pregraph-{terminalize}",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await run_manager.create_or_reject(
        f"thread-pregraph-{terminalize}",
        user_id="pregraph-owner",
    )

    try:
        if terminalize == "attached-failure":
            record.task = asyncio.current_task()
            assert await run_manager.fail_start_if_pending(
                record.run_id,
                error="pregraph failed",
            )
        else:
            record.abort_action = "interrupt"
            record.abort_event.set()
            assert await run_manager.finalize_pending_cancellation(record.run_id)

        terminal = await run_store.get(record.run_id, user_id="pregraph-owner")
        assert terminal is not None
        assert terminal["status"] == expected_status
        events = await event_store.list_events(
            record.thread_id,
            record.run_id,
            user_id="pregraph-owner",
        )
        assert [event["event_type"] for event in events].count("run.delivery") == 1
    finally:
        if record.task is asyncio.current_task():
            record.task = None
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize(
    (
        "terminalize",
        "expected_status",
        "expected_disposition",
        "expected_cancellation_action",
    ),
    [
        (
            "attached-failure",
            RunStatus.error.value,
            _AdmissionTerminalDisposition.worker_attachment_failed,
            None,
        ),
        (
            "pregraph-cancellation",
            RunStatus.interrupted.value,
            _AdmissionTerminalDisposition.cancelled,
            "interrupt",
        ),
    ],
)
async def test_pregraph_receipt_outage_retains_exact_compensation_until_receipt_recovers(
    monkeypatch,
    terminalize,
    expected_status,
    expected_disposition,
    expected_cancellation_action,
):
    """A dead pregraph worker cannot strand an active receipt-less row."""

    monkeypatch.setattr(
        "deerflow.runtime.runs.manager._admission_compensation_retry_delay",
        lambda _round: 60.0,
    )
    run_store = MemoryRunStore()

    class RecoveringReceiptStore(MemoryRunEventStore):
        def __init__(self):
            super().__init__(run_store=run_store)
            self.receipt_available = False

        async def append_fenced_if_absent(self, authority, event):
            if event.get("event_type") == "run.delivery" and not self.receipt_available:
                raise RuntimeError("receipt store unavailable")
            return await super().append_fenced_if_absent(authority, event)

    event_store = RecoveringReceiptStore()
    run_manager = RunManager(
        store=run_store,
        event_store=event_store,
        worker_id=f"worker-receipt-outage-{terminalize}",
    )
    record = await run_manager.create_or_reject(
        f"thread-receipt-outage-{terminalize}",
        user_id="pregraph-owner",
    )
    record.task = asyncio.current_task()

    try:
        if terminalize == "attached-failure":
            terminalized = await run_manager.fail_start_if_pending(
                record.run_id,
                error="pregraph failed",
            )
        else:
            record.abort_action = "interrupt"
            record.abort_event.set()
            terminalized = await run_manager.finalize_pending_cancellation(
                record.run_id,
            )

        assert terminalized is False
        active = await run_store.get(record.run_id, user_id="pregraph-owner")
        assert active is not None
        assert active["status"] == RunStatus.pending.value
        assert (
            await _delivery_events(
                event_store,
                record.thread_id,
                record.run_id,
            )
            == []
        )

        retained = run_manager._unresolved_admissions[record.run_id]
        assert retained.run_id == record.run_id
        assert retained.thread_id == record.thread_id
        assert retained.user_id == "pregraph-owner"
        assert retained.owner_worker_id == run_manager.worker_id
        assert retained.terminal_disposition is expected_disposition
        assert retained.cancellation_action == expected_cancellation_action

        event_store.receipt_available = True
        run_manager._wake_admission_compensator(reset_backoff=True)
        assert await run_manager.drain_post_commit_obligations(timeout=1)

        terminal = await run_store.get(record.run_id, user_id="pregraph-owner")
        assert terminal is not None
        assert terminal["status"] == expected_status
        delivery = await _delivery_events(
            event_store,
            record.thread_id,
            record.run_id,
        )
        assert len(delivery) == 1
        assert record.run_id not in run_manager._unresolved_admissions
    finally:
        if record.task is asyncio.current_task():
            record.task = None


@pytest.mark.anyio
async def test_pregraph_cancellation_status_outage_retains_exact_compensation_until_store_recovers(
    monkeypatch,
):
    """A receipt alone cannot discharge an unpersisted terminal transition."""

    monkeypatch.setattr(
        "deerflow.runtime.runs.manager._admission_compensation_retry_delay",
        lambda _round: 60.0,
    )

    class RecoveringRunStore(MemoryRunStore):
        def __init__(self):
            super().__init__()
            self.status_available = True

        async def transition_run_atomic(self, run_id, **kwargs):
            if not self.status_available:
                raise RuntimeError("run status store unavailable")
            return await super().transition_run_atomic(run_id, **kwargs)

    run_store = RecoveringRunStore()
    event_store = MemoryRunEventStore(run_store=run_store)
    run_manager = RunManager(
        store=run_store,
        event_store=event_store,
        worker_id="worker-pregraph-status-outage",
    )
    record = await run_manager.create_or_reject(
        "thread-pregraph-status-outage",
        user_id="pregraph-owner",
    )
    record.task = asyncio.current_task()
    record.abort_action = "interrupt"
    record.abort_event.set()
    run_store.status_available = False

    try:
        assert await run_manager.finalize_pending_cancellation(record.run_id) is False

        active = await run_store.get(record.run_id, user_id="pregraph-owner")
        assert active is not None
        assert active["status"] == RunStatus.pending.value
        assert (
            len(
                await _delivery_events(
                    event_store,
                    record.thread_id,
                    record.run_id,
                )
            )
            == 1
        )
        retained = run_manager._unresolved_admissions[record.run_id]
        assert retained.run_id == record.run_id
        assert retained.thread_id == record.thread_id
        assert retained.user_id == "pregraph-owner"
        assert retained.owner_worker_id == run_manager.worker_id
        assert retained.terminal_disposition is _AdmissionTerminalDisposition.cancelled
        assert retained.cancellation_action == "interrupt"

        run_store.status_available = True
        run_manager._wake_admission_compensator(reset_backoff=True)
        assert await run_manager.drain_post_commit_obligations(timeout=1)

        terminal = await run_store.get(record.run_id, user_id="pregraph-owner")
        assert terminal is not None
        assert terminal["status"] == RunStatus.interrupted.value
        assert (
            len(
                await _delivery_events(
                    event_store,
                    record.thread_id,
                    record.run_id,
                )
            )
            == 1
        )
        assert record.run_id not in run_manager._unresolved_admissions
    finally:
        if record.task is asyncio.current_task():
            record.task = None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("terminalize", "expected_status", "expected_disposition"),
    [
        (
            "attached-failure",
            RunStatus.error.value,
            _AdmissionTerminalDisposition.worker_attachment_failed,
        ),
        (
            "pregraph-cancellation",
            RunStatus.interrupted.value,
            _AdmissionTerminalDisposition.cancelled,
        ),
    ],
)
async def test_pregraph_receipt_cancellation_retains_exact_compensation(
    monkeypatch,
    terminalize,
    expected_status,
    expected_disposition,
):
    """Outer cancellation cannot erase a committed admission obligation."""

    monkeypatch.setattr(
        "deerflow.runtime.runs.manager._admission_compensation_retry_delay",
        lambda _round: 60.0,
    )
    run_store = MemoryRunStore()

    class BlockingReceiptStore(MemoryRunEventStore):
        def __init__(self):
            super().__init__(run_store=run_store)
            self.receipt_started = asyncio.Event()
            self.receipt_available = False
            self.release_receipt = asyncio.Event()

        async def append_fenced_if_absent(self, authority, event):
            if event.get("event_type") == "run.delivery" and not self.receipt_available:
                self.receipt_started.set()
                await self.release_receipt.wait()
            return await super().append_fenced_if_absent(authority, event)

    event_store = BlockingReceiptStore()
    run_manager = RunManager(
        store=run_store,
        event_store=event_store,
        worker_id=f"worker-receipt-cancelled-{terminalize}",
    )
    record = await run_manager.create_or_reject(
        f"thread-receipt-cancelled-{terminalize}",
        user_id="pregraph-owner",
    )

    if terminalize == "attached-failure":
        terminalize_task = asyncio.create_task(
            run_manager.fail_start_if_pending(
                record.run_id,
                error="pregraph failed",
            )
        )
    else:
        record.abort_action = "interrupt"
        record.abort_event.set()
        terminalize_task = asyncio.create_task(run_manager.finalize_pending_cancellation(record.run_id))
    record.task = terminalize_task

    try:
        await asyncio.wait_for(event_store.receipt_started.wait(), timeout=1)
        terminalize_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await terminalize_task

        active = await run_store.get(record.run_id, user_id="pregraph-owner")
        assert active is not None
        assert active["status"] == RunStatus.pending.value
        assert (
            await _delivery_events(
                event_store,
                record.thread_id,
                record.run_id,
            )
            == []
        )
        retained = run_manager._unresolved_admissions[record.run_id]
        assert retained.run_id == record.run_id
        assert retained.thread_id == record.thread_id
        assert retained.user_id == "pregraph-owner"
        assert retained.owner_worker_id == run_manager.worker_id
        assert retained.terminal_disposition is expected_disposition
        assert retained.cancellation_action == ("interrupt" if terminalize == "pregraph-cancellation" else None)

        event_store.receipt_available = True
        event_store.release_receipt.set()
        run_manager._wake_admission_compensator(reset_backoff=True)
        assert await run_manager.drain_post_commit_obligations(timeout=1)

        terminal = await run_store.get(record.run_id, user_id="pregraph-owner")
        assert terminal is not None
        assert terminal["status"] == expected_status
        assert (
            len(
                await _delivery_events(
                    event_store,
                    record.thread_id,
                    record.run_id,
                )
            )
            == 1
        )
        assert record.run_id not in run_manager._unresolved_admissions
    finally:
        event_store.receipt_available = True
        event_store.release_receipt.set()
        if not terminalize_task.done():
            terminalize_task.cancel()
            with suppress(asyncio.CancelledError):
                await terminalize_task
        if record.task is terminalize_task:
            record.task = None
