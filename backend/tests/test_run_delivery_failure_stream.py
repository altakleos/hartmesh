"""The client must hear a delivery failure (hartmesh-tenancy/DF13).

Every other terminal-error branch in ``run_agent`` publishes an ``error`` frame
before the stream's end marker. The delivery fence runs after an ordinary graph
completion, so until these regressions it terminalized the run in SQL and in the
journal while the browser saw confident prose followed by a normal end: two
tenant-class turns produced valid outputs, omitted ``present_files``, and looked
successful on screen.
"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.types import Command

from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.runs.worker import (
    _DELIVERY_INCOMPLETE_ERROR,
    _DELIVERY_RECEIPT_FAILED_ERROR,
    MAX_DISCLOSED_UNDELIVERED_PATHS,
    RunContext,
    run_agent,
)

END_FRAME = "__end__"


def _recording_bridge() -> tuple[Any, list[tuple[str, Any]]]:
    """A bridge that records every frame in publication order.

    The order matters as much as the content: a client stops reading at the
    first ``error`` frame, so the detail frame has to precede it, and both have
    to precede the end marker.
    """
    frames: list[tuple[str, Any]] = []

    async def publish(run_id: str, event: str, data: Any) -> None:
        frames.append((event, data))

    async def publish_end(run_id: str) -> None:
        frames.append((END_FRAME, None))

    return SimpleNamespace(publish=publish, publish_end=publish_end, cleanup=AsyncMock()), frames


def _frames_of(frames: list[tuple[str, Any]], event: str) -> list[Any]:
    return [data for name, data in frames if name == event]


class _ProseOnlyAgent:
    """Finishes normally with prose and never presents what it produced."""

    async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
        yield {"messages": [AIMessage(content="Your report is ready.")]}


class _PresentingAgent:
    def __init__(self, *paths: str) -> None:
        self._paths = list(paths)

    async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
        journal = config["context"]["__run_journal"]
        journal._remember_current_run_tool_calls(
            AIMessage(content="", tool_calls=[{"id": "call_1", "name": "present_files", "args": {}}]),
            caller="lead_agent",
        )
        journal.on_tool_end(
            Command(
                update={
                    "artifacts": self._paths,
                    "messages": [ToolMessage("Successfully presented files", tool_call_id="call_1")],
                }
            ),
            run_id=uuid4(),
        )
        yield {"messages": []}


def _produced(monkeypatch, *paths: str) -> None:
    monkeypatch.setattr(
        "deerflow.runtime.runs.worker._produced_output_paths",
        AsyncMock(return_value=list(paths)),
    )


@pytest.mark.anyio
async def test_an_undelivered_artifact_reaches_the_client_as_an_error(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    _produced(monkeypatch, "/mnt/user-data/outputs/report.md")

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: _ProseOnlyAgent(),
        graph_input={},
        config={},
    )

    assert record.status == RunStatus.error
    errors = _frames_of(frames, "error")
    assert errors == [
        {
            "message": _DELIVERY_INCOMPLETE_ERROR,
            "name": "ArtifactDeliveryIncompleteError",
        }
    ]


@pytest.mark.anyio
async def test_the_error_names_the_files_the_run_produced_but_never_handed_over(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    _produced(monkeypatch, "/mnt/user-data/outputs/report.md", "/mnt/user-data/outputs/data.csv")

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: _ProseOnlyAgent(),
        graph_input={},
        config={},
    )

    details = _frames_of(frames, "custom")
    assert details == [
        {
            "type": "artifact_delivery_incomplete",
            "run_id": record.run_id,
            "message": _DELIVERY_INCOMPLETE_ERROR,
            "undelivered_paths": [
                "/mnt/user-data/outputs/report.md",
                "/mnt/user-data/outputs/data.csv",
            ],
            "undelivered_count": 2,
        }
    ]


@pytest.mark.anyio
async def test_the_detail_frame_precedes_the_error_and_both_precede_the_end(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    _produced(monkeypatch, "/mnt/user-data/outputs/report.md")

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: _ProseOnlyAgent(),
        graph_input={},
        config={},
    )

    names = [name for name, _ in frames]
    assert names.index("custom") < names.index("error") < names.index(END_FRAME)


@pytest.mark.anyio
async def test_a_run_that_presented_only_unrelated_paths_discloses_everything_it_produced(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    _produced(monkeypatch, "/mnt/user-data/outputs/report.md", "/mnt/user-data/outputs/notes.md")

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: _PresentingAgent("/mnt/user-data/outputs/unrelated.md"),
        graph_input={},
        config={},
    )

    assert record.status == RunStatus.error
    detail = _frames_of(frames, "custom")[0]
    assert detail["undelivered_paths"] == [
        "/mnt/user-data/outputs/report.md",
        "/mnt/user-data/outputs/notes.md",
    ]


@pytest.mark.anyio
async def test_a_delivered_run_publishes_no_failure_frame(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    _produced(monkeypatch, "/mnt/user-data/outputs/report.md")

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: _PresentingAgent("/mnt/user-data/outputs/report.md"),
        graph_input={},
        config={},
    )

    assert record.status == RunStatus.success
    assert _frames_of(frames, "error") == []
    assert _frames_of(frames, "custom") == []


@pytest.mark.anyio
async def test_an_ordinary_chat_turn_publishes_no_failure_frame(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    _produced(monkeypatch)

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: _ProseOnlyAgent(),
        graph_input={},
        config={},
    )

    assert record.status == RunStatus.success
    assert _frames_of(frames, "error") == []
    assert _frames_of(frames, "custom") == []


@pytest.mark.anyio
async def test_a_large_undelivered_set_is_bounded_but_still_counted(monkeypatch):
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    produced = [f"/mnt/user-data/outputs/part-{index:03d}.md" for index in range(MAX_DISCLOSED_UNDELIVERED_PATHS + 7)]
    _produced(monkeypatch, *produced)

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: _ProseOnlyAgent(),
        graph_input={},
        config={},
    )

    detail = _frames_of(frames, "custom")[0]
    assert detail["undelivered_paths"] == produced[:MAX_DISCLOSED_UNDELIVERED_PATHS]
    assert detail["undelivered_count"] == len(produced)


@pytest.mark.anyio
async def test_an_unverifiable_receipt_also_reaches_the_client(monkeypatch):
    """A presented run downgraded to error because its receipt could not be
    written is a terminal failure too, and was equally silent on the stream."""

    class FailingReceiptStore(MemoryRunEventStore):
        async def put_if_absent(self, **kwargs):
            if kwargs.get("event_type") == "run.delivery":
                raise RuntimeError("event store unavailable")
            return await super().put_if_absent(**kwargs)

    run_manager = RunManager(store=MemoryRunStore())
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    _produced(monkeypatch, "/mnt/user-data/outputs/report.md")

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=FailingReceiptStore()),
        agent_factory=lambda *, config: _PresentingAgent("/mnt/user-data/outputs/report.md"),
        graph_input={},
        config={},
    )

    assert record.status == RunStatus.error
    assert _frames_of(frames, "error") == [
        {
            "message": _DELIVERY_RECEIPT_FAILED_ERROR,
            "name": "DeliveryReceiptUnverifiedError",
        }
    ]
    names = [name for name, _ in frames]
    assert names.index("error") < names.index(END_FRAME)


@pytest.mark.anyio
async def test_the_error_frame_survives_a_detail_frame_that_cannot_be_published(monkeypatch):
    """The detail frame is decoration; the error frame is the fix.

    A transport failure on the first must not take the second with it, or the
    client is back to confident prose and a clean end.
    """

    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    accepting_publish = bridge.publish

    async def refuse_custom(run_id: str, event: str, data: Any) -> None:
        if event == "custom":
            raise RuntimeError("stream transport rejected the detail frame")
        await accepting_publish(run_id, event, data)

    bridge.publish = refuse_custom
    _produced(monkeypatch, "/mnt/user-data/outputs/report.md")

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: _ProseOnlyAgent(),
        graph_input={},
        config={},
    )

    assert record.status == RunStatus.error
    assert not _frames_of(frames, "custom")
    assert _frames_of(frames, "error")[-1]["name"] == "ArtifactDeliveryIncompleteError"


@pytest.mark.anyio
async def test_a_fenced_worker_narrates_nothing_onto_a_stream_a_peer_owns(monkeypatch):
    """Losing the run mid-terminalization also returns None from the status CAS.

    Publishing then would tell the client a story the surviving owner did not
    author, against the worker's own no-publication-after-loss invariant.
    """

    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    _produced(monkeypatch, "/mnt/user-data/outputs/report.md")

    async def lose_ownership(run_id: str, status: RunStatus, **kwargs: Any) -> None:
        record.ownership_lost = True
        return None

    run_manager.set_status_if_not_cancelled = lose_ownership

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: _ProseOnlyAgent(),
        graph_input={},
        config={},
    )

    assert _frames_of(frames, "error") == []
    assert _frames_of(frames, "custom") == []
