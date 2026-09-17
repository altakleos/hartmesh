"""The client must hear a delivery failure (hartmesh-tenancy/DF13).

The delivery fence runs after an ordinary graph completion, so until these
regressions it terminalized the run in SQL and in the journal while the browser
saw confident prose followed by a normal end: two tenant-class turns produced
valid outputs, omitted ``present_files``, and looked successful on screen.

The verdict deliberately does **not** ride an ``error`` frame. That frame means
"this stream carries no valid assistant turn", which is false here and which the
SDK acts on by discarding the end marker and skipping the settle. It rides one
advisory ``custom`` frame for live clients and ``stop_reason`` on the run record
for everyone else, so these tests pin both halves — and pin that losing the
frame costs a round-trip, never the verdict.
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

INCOMPLETE_STOP_REASON = "artifact_delivery_incomplete"
RECEIPT_STOP_REASON = "delivery_receipt_failed"

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


def _is_delivery_verdict(data: Any) -> bool:
    return isinstance(data, dict) and str(data.get("type", "")).startswith("artifact_delivery_")


def _verdicts(frames: list[tuple[str, Any]]) -> list[Any]:
    """The delivery verdict frames alone.

    The advisory ``custom`` channel also carries ``turn_progress`` frames
    (``preparing`` at admission, ``thinking`` at the model request), published
    while the worker still owns the run; these tests are about the verdict,
    which is the one frame that must never follow an ownership loss.
    """
    return [data for name, data in frames if name == "custom" and _is_delivery_verdict(data)]


def _verdict_index(frames: list[tuple[str, Any]]) -> int:
    return next(index for index, (name, data) in enumerate(frames) if name == "custom" and _is_delivery_verdict(data))


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
                    "messages": [ToolMessage("Successfully presented files", tool_call_id="call_1", additional_kwargs={"presented_files": self._paths})],
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
async def test_an_undelivered_artifact_reaches_the_client_without_failing_the_stream(monkeypatch):
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
    assert record.error == _DELIVERY_INCOMPLETE_ERROR
    assert record.stop_reason == INCOMPLETE_STOP_REASON
    assert _frames_of(frames, "error") == []
    assert len(_verdicts(frames)) == 1


@pytest.mark.anyio
async def test_the_frame_names_the_files_the_run_produced_but_never_handed_over(monkeypatch):
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

    details = _verdicts(frames)
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
async def test_the_delivery_frame_precedes_a_clean_end_and_no_error_frame_is_published(monkeypatch):
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
    assert "error" not in names
    assert _verdict_index(frames) < names.index(END_FRAME)


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
    detail = _verdicts(frames)[0]
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
    assert _verdicts(frames) == []


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
    assert _verdicts(frames) == []


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

    detail = _verdicts(frames)[0]
    assert detail["undelivered_paths"] == produced[:MAX_DISCLOSED_UNDELIVERED_PATHS]
    assert detail["undelivered_count"] == len(produced)


@pytest.mark.anyio
async def test_an_unverifiable_receipt_also_reaches_the_client(monkeypatch):
    """A presented run downgraded to error because its receipt could not be
    written is a terminal failure too, and was equally silent. It carries no
    paths: the files were presented, so there is nothing to offer in place."""

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
    assert record.error == _DELIVERY_RECEIPT_FAILED_ERROR
    assert record.stop_reason == RECEIPT_STOP_REASON
    assert _frames_of(frames, "error") == []
    assert _verdicts(frames) == [
        {
            "type": "artifact_delivery_unverified",
            "run_id": record.run_id,
            "message": _DELIVERY_RECEIPT_FAILED_ERROR,
        }
    ]
    names = [name for name, _ in frames]
    assert _verdict_index(frames) < names.index(END_FRAME)


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
    assert _verdicts(frames) == []
    assert record.stop_reason is None


@pytest.mark.anyio
async def test_a_verdict_that_cannot_be_published_still_stands_on_the_record(monkeypatch):
    """The frame is advisory; the record is the authority.

    Losing the stream frame costs a live client a round-trip. It must not cost
    anyone the verdict, which is the whole difference between this design and
    one where the stream is the only carrier.
    """

    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    accepting_publish = bridge.publish

    async def refuse_custom(run_id: str, event: str, data: Any) -> None:
        if event == "custom":
            raise RuntimeError("stream transport rejected the verdict frame")
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

    assert not _verdicts(frames)
    assert record.status == RunStatus.error
    assert record.error == _DELIVERY_INCOMPLETE_ERROR
    assert record.stop_reason == INCOMPLETE_STOP_REASON


@pytest.mark.anyio
async def test_a_capped_turn_that_also_fails_delivery_reports_the_delivery_reason(monkeypatch):
    """``stop_reason`` explains the status being reported, and that status is
    the delivery error. The guard's cap stays on the journal's middleware
    evidence."""

    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, _frames = _recording_bridge()
    _produced(monkeypatch, "/mnt/user-data/outputs/report.md")

    class CappedProseAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            runtime_context = config["context"]
            runtime_context["stop_reason"] = "token_capped"
            yield {"messages": [AIMessage(content="Your report is ready.")]}

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: CappedProseAgent(),
        graph_input={},
        config={},
    )

    assert record.status == RunStatus.error
    assert record.stop_reason == INCOMPLETE_STOP_REASON


@pytest.mark.anyio
async def test_a_delivered_run_leaves_stop_reason_alone(monkeypatch):
    """The fence must not stamp a reason onto a run it did not fail."""

    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, _frames = _recording_bridge()
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
    assert record.stop_reason is None


def test_both_delivery_reasons_are_registered_lifecycle_evidence():
    """``stop_reason`` is governed, not free text.

    It reaches the durable row through ``LifecycleTransition.reason``, which
    ``build_lifecycle_payload`` validates against a closed vocabulary; an
    unregistered value raises there, the terminal CAS is caught as an
    indeterminate store failure, and the worker marks its own lease lost and
    overwrites the real error. So registering these two is load-bearing, and
    this pins it directly instead of leaving it to a worker-level symptom.
    """
    from deerflow.runtime.runs.store.base import (
        LifecycleTransition,
        build_lifecycle_payload,
        lifecycle_type_for_status,
    )

    for reason in (INCOMPLETE_STOP_REASON, RECEIPT_STOP_REASON):
        payload = build_lifecycle_payload(
            LifecycleTransition(
                lifecycle_type=lifecycle_type_for_status(RunStatus.error.value),
                status=RunStatus.error.value,
                error="boom",
                stop_reason=reason,
                reason=reason,
            )
        )
        assert payload["reason"] == reason
