"""Turn timing through the real runtime and the real streaming route.

The model here is deterministic and local: no provider call, no API key, a
controlled delay before its first text and a deliberately slow cleanup after
it. That is enough to establish ordering and to show what the phases would
report; it establishes nothing about restricted-runsc performance.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.gateway.services import sse_consumer
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.stream_bridge import MemoryStreamBridge
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.runtime.turn_phases import (
    TurnPhase,
    current_turn_phases,
    reset_turn_phase_registry,
    turn_phases,
    turn_phases_for_run,
)

TENANT = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()

# Long enough to dominate scheduling noise, short enough to keep the suite fast.
FIRST_TEXT_DELAY_S = 0.15
SLOW_CLEANUP_S = 0.30


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_turn_phase_registry()
    yield
    reset_turn_phase_registry()


class _StubRequest:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}

    async def is_disconnected(self) -> bool:
        return False


def _ai_text_frame(text: str):
    return [{"type": "AIMessageChunk", "content": text}, {"langgraph_node": "agent"}]


async def _collect(consumer) -> list[str]:
    frames: list[str] = []
    async for frame in consumer:
        frames.append(frame)
    return frames


@pytest.mark.asyncio
async def test_first_outgoing_assistant_text_is_timed_through_the_real_sse_route():
    """Lifecycle and progress frames must not stop the first-text clock.

    The run publishes metadata, a progress update and a hidden-reasoning chunk
    before any answer, then waits, then speaks. The recorded first stream text
    must land after the wait, not at the first frame on the wire.
    """
    bridge = MemoryStreamBridge()
    record = SimpleNamespace(
        run_id="run-sse-timing",
        thread_id="thread-sse-timing",
        status=RunStatus.running,
        on_disconnect="continue",
    )

    with turn_phases(correlation_id="trace-sse", run_id=record.run_id) as journal:
        consumer = sse_consumer(bridge, record, _StubRequest(), SimpleNamespace(), apply_on_disconnect=False)
        collector = asyncio.create_task(_collect(consumer))

        await bridge.publish(record.run_id, "metadata", {"run_id": record.run_id})
        await bridge.publish(record.run_id, "custom", {"progress": "acquiring sandbox"})
        await bridge.publish(record.run_id, "messages", _ai_text_frame([{"type": "thinking", "thinking": "hidden"}]))
        await asyncio.sleep(FIRST_TEXT_DELAY_S)
        await bridge.publish(record.run_id, "messages", _ai_text_frame("Hello"))
        await bridge.publish(record.run_id, "messages", _ai_text_frame(" there"))
        # Slow cleanup after the answer: it must not move the first-text mark.
        await asyncio.sleep(SLOW_CLEANUP_S)
        await bridge.publish_end(record.run_id)

        frames = await asyncio.wait_for(collector, timeout=10)
        snapshot = journal.snapshot()

    first_text_ms = snapshot.phase_at_ms(TurnPhase.FIRST_STREAM_TEXT)
    assert first_text_ms is not None
    assert first_text_ms >= FIRST_TEXT_DELAY_S * 1000 * 0.9, "first text was credited to a lifecycle frame"
    assert first_text_ms < (FIRST_TEXT_DELAY_S + SLOW_CLEANUP_S) * 1000, "first text drifted into cleanup"
    assert len([record_ for record_ in snapshot.phases if record_.phase is TurnPhase.FIRST_STREAM_TEXT]) == 1
    assert any("Hello" in frame for frame in frames)


@pytest.mark.asyncio
async def test_a_run_with_no_assistant_text_reports_no_first_text_rather_than_guessing():
    bridge = MemoryStreamBridge()
    record = SimpleNamespace(
        run_id="run-sse-silent",
        thread_id="thread-sse-silent",
        status=RunStatus.running,
        on_disconnect="continue",
    )

    with turn_phases(correlation_id="trace-silent", run_id=record.run_id) as journal:
        consumer = sse_consumer(bridge, record, _StubRequest(), SimpleNamespace(), apply_on_disconnect=False)
        collector = asyncio.create_task(_collect(consumer))
        await bridge.publish(record.run_id, "metadata", {"run_id": record.run_id})
        await bridge.publish(record.run_id, "custom", {"progress": "still working"})
        await bridge.publish_end(record.run_id)
        await asyncio.wait_for(collector, timeout=10)
        snapshot = journal.snapshot()

    assert snapshot.phase_at_ms(TurnPhase.FIRST_STREAM_TEXT) is None


# ── The run path itself ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_agent_opens_a_journal_and_records_admission_through_terminal():
    manager = RunManager(tenant=TENANT)
    record = await manager.create_or_reject("thread-phase-run")
    captured: dict[str, object] = {}

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            captured["journal"] = current_turn_phases()
            captured["registered"] = turn_phases_for_run(record.run_id)
            yield {"messages": []}

    def factory(*, config):
        captured["callbacks"] = list(config.get("callbacks") or ())
        return _Agent()

    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())

    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None, tenant=TENANT),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    journal = captured["journal"]
    assert journal is not None, "the run must execute inside a phase journal"
    assert captured["registered"] is journal, "the journal must be findable by run id while the run is live"
    snapshot = journal.snapshot()
    phases = [record_.phase for record_ in snapshot.phases]
    assert TurnPhase.ADMISSION in phases
    assert TurnPhase.TERMINAL in phases
    assert snapshot.outcome is not None
    assert snapshot.phase_at_ms(TurnPhase.ADMISSION) <= snapshot.phase_at_ms(TurnPhase.TERMINAL)
    # And the journal is released once the turn is over.
    assert turn_phases_for_run(record.run_id) is None


@pytest.mark.asyncio
async def test_the_model_phase_handler_is_attached_to_the_graph_the_agent_receives():
    """Model request, first provider text and completion ride the run's own
    callback seam rather than a new telemetry path."""
    from deerflow.runtime.turn_phases import TurnPhaseCallbackHandler

    manager = RunManager(tenant=TENANT)
    record = await manager.create_or_reject("thread-phase-model")
    captured: dict[str, object] = {}

    class _Agent:
        def __init__(self, handlers) -> None:
            self._handlers = handlers

        async def astream(self, *_args, **_kwargs):
            for handler in self._handlers:
                handler.on_chat_model_start({}, [])
            await asyncio.sleep(FIRST_TEXT_DELAY_S)
            for handler in self._handlers:
                handler.on_llm_new_token("")
                handler.on_llm_new_token("Hello")
                handler.on_llm_end(None)
            captured["journal"] = current_turn_phases()
            yield {"messages": []}

    def factory(*, config):
        handlers = [handler for handler in (config.get("callbacks") or ()) if isinstance(handler, TurnPhaseCallbackHandler)]
        captured["handler_count"] = len(handlers)
        return _Agent(handlers)

    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())

    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None, tenant=TENANT),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert captured["handler_count"] == 1, "exactly one phase handler on the graph root"
    snapshot = captured["journal"].snapshot()
    model_request_ms = snapshot.phase_at_ms(TurnPhase.MODEL_REQUEST)
    first_text_ms = snapshot.phase_at_ms(TurnPhase.FIRST_PROVIDER_TEXT)
    assert model_request_ms is not None and first_text_ms is not None
    assert first_text_ms - model_request_ms >= FIRST_TEXT_DELAY_S * 1000 * 0.9
    assert snapshot.phase_at_ms(TurnPhase.MODEL_COMPLETION) is not None


@pytest.mark.asyncio
async def test_run_agent_records_the_session_kind_and_snapshot_facts():
    """An ordinary run with no pinned material: kind ordinary, no snapshot.

    Presence and count are separate fields; the worker's guard is
    ``skill_snapshot is not None`` and the journal must say exactly that.
    """
    manager = RunManager(tenant=TENANT)
    record = await manager.create_or_reject("thread-phase-kind")
    captured: dict[str, object] = {}

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            captured["journal"] = current_turn_phases()
            yield {"messages": []}

    bridge = SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())

    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(checkpointer=None, tenant=TENANT),
        agent_factory=lambda *, config: _Agent(),
        graph_input={},
        config={},
    )

    snapshot = captured["journal"].snapshot()
    assert snapshot.session_kind == "ordinary"
    assert snapshot.snapshot_present is False
    assert snapshot.snapshot_package_count is None
    assert snapshot.mandatory_materialization is False
