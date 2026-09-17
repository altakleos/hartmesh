"""The person waiting hears what the run is doing, from admission on.

Tenant-class `.18`: the client showed "Working…" for 7 s on a warm turn and
23 s on a cold one before the first tool card, and the only richer signal was
the model writing todos at 5 to 8 s of model time each. These tests pin the
system-authored alternative: one advisory ``custom`` frame per stage, the
first at admission before any model token, deduplicated, from any thread.
"""

from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from langchain_core.messages import AIMessage

from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.turn_phases import TurnPhase, TurnPhaseJournal, current_turn_phases
from deerflow.runtime.turn_progress import (
    TURN_PROGRESS_EVENT_TYPE,
    TurnProgressPublisher,
    TurnProgressStage,
    stage_for_phase,
    turn_progress_payload,
)

# ---------------------------------------------------------------------------
# The journal tells its observers when a phase begins
# ---------------------------------------------------------------------------


def test_observers_hear_marks_and_span_starts_in_order() -> None:
    journal = TurnPhaseJournal(correlation_id="c")
    heard: list[tuple[TurnPhase, float]] = []
    journal.observe(lambda phase, at_ms: heard.append((phase, at_ms)))

    journal.mark(TurnPhase.ADMISSION)
    with journal.span(TurnPhase.SANDBOX_CREATE):
        heard_inside = list(heard)
    assert journal.mark_once(TurnPhase.MODEL_REQUEST) is True
    assert journal.mark_once(TurnPhase.MODEL_REQUEST) is False

    assert [phase for phase, _ in heard] == [TurnPhase.ADMISSION, TurnPhase.SANDBOX_CREATE, TurnPhase.MODEL_REQUEST]
    assert [phase for phase, _ in heard_inside] == [TurnPhase.ADMISSION, TurnPhase.SANDBOX_CREATE], "a span is announced when it begins, not when it ends"
    assert all(at_ms >= 0 for _, at_ms in heard)


def test_a_failing_observer_does_not_stop_the_journal_or_the_others() -> None:
    journal = TurnPhaseJournal(correlation_id="c")
    heard: list[TurnPhase] = []

    def broken(phase: TurnPhase, at_ms: float) -> None:
        raise RuntimeError("boom")

    journal.observe(broken)
    journal.observe(lambda phase, at_ms: heard.append(phase))
    journal.mark(TurnPhase.ADMISSION)
    with journal.span(TurnPhase.AGENT_BUILD):
        pass

    assert heard == [TurnPhase.ADMISSION, TurnPhase.AGENT_BUILD]
    assert [record.phase for record in journal.snapshot().phases] == [TurnPhase.ADMISSION, TurnPhase.AGENT_BUILD]


# ---------------------------------------------------------------------------
# The stages a person is told about, and the frame that carries them
# ---------------------------------------------------------------------------


def test_only_the_phases_a_person_can_act_on_become_stages() -> None:
    assert stage_for_phase(TurnPhase.ADMISSION) is TurnProgressStage.PREPARING
    assert stage_for_phase(TurnPhase.SANDBOX_CREATE) is TurnProgressStage.WORKSPACE_STARTING
    assert stage_for_phase(TurnPhase.MODEL_REQUEST) is TurnProgressStage.THINKING
    for phase in TurnPhase:
        if phase not in (TurnPhase.ADMISSION, TurnPhase.SANDBOX_CREATE, TurnPhase.MODEL_REQUEST):
            assert stage_for_phase(phase) is None, phase


def test_the_frame_carries_the_run_the_stage_and_a_whole_offset() -> None:
    assert turn_progress_payload("run-1", TurnProgressStage.THINKING, 4242.7) == {"type": TURN_PROGRESS_EVENT_TYPE, "run_id": "run-1", "stage": "thinking", "at_ms": 4242}


@pytest.mark.anyio
async def test_the_publisher_sends_each_stage_once_and_from_any_thread() -> None:
    published: list[dict] = []

    async def publish(payload: dict) -> None:
        published.append(payload)

    publisher = TurnProgressPublisher(loop=asyncio.get_running_loop(), run_id="run-1", publish=publish)
    await publisher.open()
    publisher(TurnPhase.ADMISSION, 0.0)
    publisher(TurnPhase.ASSEMBLY, 5.0)  # no stage
    publisher(TurnPhase.SANDBOX_LOOKUP, 20.0)  # no stage
    worker = threading.Thread(target=publisher, args=(TurnPhase.SANDBOX_CREATE, 150.0))
    worker.start()
    worker.join()
    publisher(TurnPhase.MODEL_REQUEST, 4242.0)
    publisher(TurnPhase.MODEL_REQUEST, 9000.0)  # a second model call is not re-announced
    publisher(TurnPhase.SANDBOX_CREATE, 9500.0)  # a sandbox acquired mid-turn is behind "thinking": dropped
    await publisher.drain()

    assert [(frame["stage"], frame["at_ms"]) for frame in published] == [("preparing", 0), ("workspace_starting", 150), ("thinking", 4242)]
    assert all(frame["type"] == TURN_PROGRESS_EVENT_TYPE and frame["run_id"] == "run-1" for frame in published)


@pytest.mark.anyio
async def test_stages_are_held_until_the_stream_has_its_metadata_frame() -> None:
    """The client places a label only once it knows the run and thread; frames before that are held and sent in order after."""
    published: list[dict] = []

    async def publish(payload: dict) -> None:
        published.append(payload)

    publisher = TurnProgressPublisher(loop=asyncio.get_running_loop(), run_id="run-1", publish=publish)
    publisher(TurnPhase.ADMISSION, 0.0)
    publisher(TurnPhase.SANDBOX_CREATE, 20.0)
    await asyncio.sleep(0)
    assert published == []
    await publisher.open()
    publisher(TurnPhase.MODEL_REQUEST, 400.0)
    await publisher.drain()

    assert [frame["stage"] for frame in published] == ["preparing", "workspace_starting", "thinking"]


@pytest.mark.anyio
async def test_a_run_refused_before_its_metadata_frame_publishes_no_progress() -> None:
    published: list[dict] = []

    async def publish(payload: dict) -> None:
        published.append(payload)

    publisher = TurnProgressPublisher(loop=asyncio.get_running_loop(), run_id="run-1", publish=publish)
    publisher(TurnPhase.ADMISSION, 0.0)
    publisher.close()
    await publisher.open()  # too late: closed
    await publisher.drain()

    assert published == []


@pytest.mark.anyio
async def test_a_closed_publisher_drops_what_a_stray_thread_opens_after_the_run() -> None:
    published: list[dict] = []

    async def publish(payload: dict) -> None:
        published.append(payload)

    publisher = TurnProgressPublisher(loop=asyncio.get_running_loop(), run_id="run-1", publish=publish)
    await publisher.open()
    publisher(TurnPhase.ADMISSION, 0.0)
    publisher.close()
    publisher(TurnPhase.SANDBOX_CREATE, 30.0)
    publisher(TurnPhase.MODEL_REQUEST, 40.0)
    await publisher.drain()

    assert [frame["stage"] for frame in published] == ["preparing"]


def test_a_frame_handed_to_a_loop_that_closed_meanwhile_is_dropped_quietly() -> None:
    loop = asyncio.new_event_loop()

    async def publish(payload: dict) -> None:  # pragma: no cover - never reached
        raise AssertionError("must not run")

    publisher = TurnProgressPublisher(loop=loop, run_id="run-1", publish=publish)
    loop.run_until_complete(publisher.open())
    publisher(TurnPhase.ADMISSION, 0.0)  # handed to the loop, not yet scheduled
    loop.close()
    publisher._schedule(turn_progress_payload("run-1", TurnProgressStage.PREPARING, 0.0))  # the pending callback, on a closed loop

    assert publisher._handed == 0 and not publisher._pending


@pytest.mark.anyio
async def test_a_publication_failure_is_logged_and_the_run_goes_on(caplog) -> None:
    async def publish(payload: dict) -> None:
        raise RuntimeError("bridge down")

    publisher = TurnProgressPublisher(loop=asyncio.get_running_loop(), run_id="run-1", publish=publish)
    await publisher.open()
    with caplog.at_level("WARNING", logger="deerflow.runtime.turn_progress"):
        publisher(TurnPhase.ADMISSION, 0.0)
        await publisher.drain()
        await asyncio.sleep(0)
    assert "was not published" in caplog.text


# ---------------------------------------------------------------------------
# On the real run path: admission is announced before the model, on the stream
# ---------------------------------------------------------------------------


def _recording_bridge():
    frames: list[tuple[str, object]] = []

    async def publish(run_id: str, event: str, data: object) -> None:
        frames.append((event, data))

    async def publish_end(run_id: str) -> None:
        frames.append(("end", None))

    return SimpleNamespace(publish=publish, publish_end=publish_end, cleanup=AsyncMock()), frames


class _ModelCallingAgent:
    """Marks the model request the way the callback handler does, then answers."""

    async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
        journal = current_turn_phases()
        assert journal is not None
        journal.mark_once(TurnPhase.MODEL_REQUEST)
        await asyncio.sleep(0)
        yield {"messages": [AIMessage(content="Here is your report.")]}


@pytest.mark.anyio
async def test_the_stream_hears_preparing_at_admission_and_thinking_at_the_model_request() -> None:
    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()

    await run_agent(
        bridge,
        run_manager,
        record,
        ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()),
        agent_factory=lambda *, config: _ModelCallingAgent(),
        graph_input={},
        config={},
    )

    progress = [(index, data) for index, (event, data) in enumerate(frames) if event == "custom" and isinstance(data, dict) and data.get("type") == TURN_PROGRESS_EVENT_TYPE]
    assert [data["stage"] for _, data in progress] == ["preparing", "thinking"]
    assert all(data["run_id"] == record.run_id for _, data in progress)
    first_progress_index = progress[0][0]
    metadata_index = next(index for index, (event, _data) in enumerate(frames) if event == "metadata")
    first_model_output = next(index for index, (event, data) in enumerate(frames) if event not in ("custom", "metadata") and data is not None)
    assert metadata_index < first_progress_index < first_model_output, (
        f"the person is told the run is preparing after the stream names the run and before any model output exists: {[(event, (data.get('type') or data.get('stage')) if isinstance(data, dict) else data) for event, data in frames]}"
    )
    assert frames[-1] == ("end", None)


@pytest.mark.anyio
async def test_a_turn_without_a_model_call_announces_only_preparing() -> None:
    class SilentAgent:
        async def astream(self, graph_input, config=None, stream_mode=None, subgraphs=False):
            yield {"messages": []}

    run_manager = RunManager()
    record = await run_manager.create("thread-1")
    bridge, frames = _recording_bridge()
    await run_agent(bridge, run_manager, record, ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()), agent_factory=lambda *, config: SilentAgent(), graph_input={}, config={})
    stages = [data["stage"] for event, data in frames if event == "custom" and isinstance(data, dict) and data.get("type") == TURN_PROGRESS_EVENT_TYPE]
    assert stages == ["preparing"]


def test_the_todo_prompt_says_a_skill_workflow_is_not_a_todo_list() -> None:
    """Four `write_todos` calls mirrored the skill's four steps on the tenant (20 s of model time); the progress frames make that visibility redundant."""
    from deerflow.agents.factory import _TODO_SYSTEM_PROMPT
    from deerflow.agents.lead_agent.agent import _create_todo_list_middleware

    lead = _create_todo_list_middleware(True)
    assert lead is not None
    for prompt in (_TODO_SYSTEM_PROMPT, lead.system_prompt):
        assert "A skill's documented workflow is already the plan" in prompt
        assert "do not mirror them into todos" in prompt
