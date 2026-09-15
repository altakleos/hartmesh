"""The turn phase journal: what it measures, what it refuses to claim."""

from __future__ import annotations

import logging
import threading
import time
from types import SimpleNamespace

import pytest

from deerflow.logging_config import DEFAULT_LOG_DATE_FORMAT, DEFAULT_LOG_FORMAT
from deerflow.runtime.turn_phases import (
    MAX_PHASE_RECORDS,
    MAX_TRACKED_RUNS,
    AcquisitionSource,
    TurnPhase,
    TurnPhaseCallbackHandler,
    TurnPhaseJournal,
    current_turn_phases,
    mark_first_stream_text,
    reset_turn_phase_registry,
    sse_frame_carries_assistant_text,
    turn_phases,
    turn_phases_for_run,
)


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_turn_phase_registry()
    yield
    reset_turn_phase_registry()


def _ai_chunk(content):
    return [{"type": "AIMessageChunk", "content": content}, {"langgraph_node": "agent"}]


# ── Timing ───────────────────────────────────────────────────────────────


def test_offsets_are_monotonic_durations_not_wall_clock_differences(monkeypatch):
    """A wall-clock step must not move a phase offset."""
    ticks = iter([100.0, 100.25, 100.5, 100.75])
    monkeypatch.setattr("deerflow.runtime.turn_phases.time.monotonic", lambda: next(ticks))

    journal = TurnPhaseJournal(correlation_id="trace-1")
    with journal.span(TurnPhase.SANDBOX_CREATE):
        pass

    assert journal.snapshot().phase_ms(TurnPhase.SANDBOX_CREATE) == pytest.approx(250.0)


def test_a_span_records_its_phase_even_when_the_body_raises():
    journal = TurnPhaseJournal(correlation_id="trace-raise")

    with pytest.raises(RuntimeError), journal.span(TurnPhase.SANDBOX_READINESS):
        raise RuntimeError("boom")

    assert journal.snapshot().phase_ms(TurnPhase.SANDBOX_READINESS) is not None


def test_phases_ordered_by_observation_expose_the_gap_before_the_model_call():
    """The point of the journal: where a turn's seconds went, in order."""
    journal = TurnPhaseJournal(correlation_id="trace-order")
    journal.mark(TurnPhase.ADMISSION)
    with journal.span(TurnPhase.SANDBOX_ACQUIRE):
        with journal.span(TurnPhase.SANDBOX_CREATE):
            pass
        with journal.span(TurnPhase.SANDBOX_READINESS):
            pass
    journal.mark(TurnPhase.MODEL_REQUEST)
    journal.mark(TurnPhase.FIRST_PROVIDER_TEXT)

    snapshot = journal.snapshot()
    acquire_at = snapshot.phase_at_ms(TurnPhase.SANDBOX_ACQUIRE)
    model_at = snapshot.phase_at_ms(TurnPhase.MODEL_REQUEST)
    assert acquire_at is not None and model_at is not None
    assert acquire_at <= model_at
    assert snapshot.phase_ms(TurnPhase.SANDBOX_ACQUIRE) is not None


def test_first_text_marks_once_and_keeps_the_first_observation(monkeypatch):
    ticks = iter([0.0, 1.0, 2.0, 3.0])
    monkeypatch.setattr("deerflow.runtime.turn_phases.time.monotonic", lambda: next(ticks))
    journal = TurnPhaseJournal(correlation_id="trace-first")

    assert journal.mark_once(TurnPhase.FIRST_PROVIDER_TEXT) is True
    assert journal.mark_once(TurnPhase.FIRST_PROVIDER_TEXT) is False

    records = [record for record in journal.snapshot().phases if record.phase is TurnPhase.FIRST_PROVIDER_TEXT]
    assert len(records) == 1
    assert records[0].started_ms == pytest.approx(1000.0)


# ── Honesty ──────────────────────────────────────────────────────────────


def test_an_unobservable_phase_is_reported_not_inferred():
    journal = TurnPhaseJournal(correlation_id="trace-gap")
    journal.unobservable("browser_first_text", "requires a browser through public ingress")

    snapshot = journal.snapshot()
    assert snapshot.phase_at_ms(TurnPhase.FIRST_STREAM_TEXT) is None
    assert snapshot.unobservable[0][0] == "browser_first_text"


def test_an_unmeasured_phase_reads_as_none_rather_than_zero():
    snapshot = TurnPhaseJournal(correlation_id="trace-empty").snapshot()

    assert snapshot.phase_ms(TurnPhase.MODEL_COMPLETION) is None
    assert snapshot.acquisition_source is None
    assert snapshot.snapshot_present is None


def test_zero_packages_is_recorded_beside_presence_not_instead_of_it():
    """Zero packages alone does not establish a deferrable state."""
    journal = TurnPhaseJournal(correlation_id="trace-snap")
    journal.set_snapshot_facts(present=True, package_count=0, mandatory_materialization=True)

    snapshot = journal.snapshot()
    assert snapshot.snapshot_present is True
    assert snapshot.snapshot_package_count == 0
    assert snapshot.mandatory_materialization is True


# ── Disclosure and bounds ────────────────────────────────────────────────


def test_free_text_is_reduced_to_a_bounded_label():
    journal = TurnPhaseJournal(correlation_id="x" * 400)
    journal.set_outcome("failed because the provider said 'secret token abc'")
    journal.mark(TurnPhase.TERMINAL, detail="y" * 400)

    snapshot = journal.snapshot()
    assert len(snapshot.correlation_id) <= 128
    assert " " not in (snapshot.outcome or "")
    assert len(snapshot.outcome or "") <= 48
    detail = snapshot.phases[0].detail
    assert detail is not None and len(detail) <= 64


def test_the_record_count_is_capped_and_the_overflow_is_declared():
    journal = TurnPhaseJournal(correlation_id="trace-cap")
    for _ in range(MAX_PHASE_RECORDS + 20):
        journal.mark(TurnPhase.MODEL_REQUEST)

    snapshot = journal.snapshot()
    assert len(snapshot.phases) == MAX_PHASE_RECORDS
    assert snapshot.dropped_records == 20


def test_the_run_registry_is_bounded():
    for index in range(MAX_TRACKED_RUNS + 5):
        with turn_phases(correlation_id="c", run_id=f"run-{index}"):
            pass
    # Each context manager unregisters on exit, so nothing is retained at all.
    assert turn_phases_for_run("run-0") is None


def test_a_run_journal_is_unregistered_when_its_turn_ends():
    with turn_phases(correlation_id="c", run_id="run-live") as journal:
        assert turn_phases_for_run("run-live") is journal
    assert turn_phases_for_run("run-live") is None


def test_the_emitted_record_is_one_structured_log_line(caplog):
    journal = TurnPhaseJournal(correlation_id="trace-log", run_id="run-log")
    journal.set_acquisition_source(AcquisitionSource.WARM_RECLAIM)

    with caplog.at_level(logging.INFO, logger="deerflow.runtime.turn_phases"):
        journal.emit()

    records = [record for record in caplog.records if hasattr(record, "turn_phases")]
    assert len(records) == 1
    assert records[0].turn_phases["acquisition_source"] == "warm_reclaim"


def test_instrumentation_overhead_stays_far_below_the_latency_it_measures():
    """A bounded cost, measured, not assumed.

    The budget is deliberately loose: the claim is only that recording a phase
    is orders of magnitude cheaper than the milliseconds it reports, so the
    journal cannot become the latency.
    """
    journal = TurnPhaseJournal(correlation_id="trace-cost")
    iterations = 10_000
    started = time.perf_counter()
    for _ in range(iterations):
        journal.mark(TurnPhase.MODEL_REQUEST)
    elapsed_us = (time.perf_counter() - started) / iterations * 1_000_000

    assert elapsed_us < 50, f"one mark cost {elapsed_us:.1f}us"
    assert journal.snapshot().dropped_records == iterations - MAX_PHASE_RECORDS


def test_recording_is_thread_safe_across_the_thread_hops_acquisition_makes():
    journal = TurnPhaseJournal(correlation_id="trace-threads")

    def _worker() -> None:
        for _ in range(50):
            journal.record_resource_create()
            journal.record_eviction()

    workers = [threading.Thread(target=_worker) for _ in range(8)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=10)

    snapshot = journal.snapshot()
    assert snapshot.resource_creates == 400
    assert snapshot.evictions == 400


# ── Binding ──────────────────────────────────────────────────────────────


def test_nothing_is_bound_outside_a_measured_turn():
    assert current_turn_phases() is None


def test_the_journal_is_visible_to_the_worker_threads_acquisition_uses():
    seen: list[object] = []

    with turn_phases(correlation_id="c", run_id="run-thread") as journal:
        import contextvars

        context = contextvars.copy_context()
        worker = threading.Thread(target=lambda: seen.append(context.run(current_turn_phases)))
        worker.start()
        worker.join(timeout=5)

    assert seen == [journal]


# ── First text is text ───────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("event", "data"),
    [
        ("messages", _ai_chunk("Hello")),
        ("messages", _ai_chunk([{"type": "text", "text": "Hello"}])),
        ("messages", _ai_chunk(["Hello"])),
    ],
)
def test_assistant_text_frames_are_recognised(event, data):
    assert sse_frame_carries_assistant_text(event, data) is True


@pytest.mark.parametrize(
    ("event", "data"),
    [
        ("metadata", {"run_id": "r"}),
        ("values", {"messages": []}),
        ("updates", {"agent": {}}),
        ("custom", {"progress": "thinking"}),
        ("end", None),
        ("gap", {"code": "stream_replay_gap"}),
        ("messages", _ai_chunk("")),
        ("messages", _ai_chunk("   ")),
        ("messages", _ai_chunk([{"type": "thinking", "thinking": "hidden"}])),
        ("messages", _ai_chunk([{"type": "reasoning", "text": "hidden"}])),
        ("messages", [{"type": "ToolMessage", "content": "tool output"}, {}]),
        ("messages", [{"type": "HumanMessage", "content": "hi"}, {}]),
        ("messages", _ai_chunk([{"type": "tool_use", "name": "bash"}])),
    ],
)
def test_lifecycle_progress_and_hidden_reasoning_are_not_answer_text(event, data):
    assert sse_frame_carries_assistant_text(event, data) is False


def test_the_first_stream_text_mark_ignores_everything_before_the_answer():
    with turn_phases(correlation_id="c", run_id="run-sse") as journal:
        for event, data in (
            ("metadata", {"run_id": "run-sse"}),
            ("custom", {"progress": "starting sandbox"}),
            ("messages", _ai_chunk([{"type": "thinking", "thinking": "hidden"}])),
            ("values", {"messages": []}),
            ("messages", _ai_chunk("Hello")),
            ("messages", _ai_chunk(" there")),
        ):
            mark_first_stream_text("run-sse", event, data)

        marks = [record for record in journal.snapshot().phases if record.phase is TurnPhase.FIRST_STREAM_TEXT]

    assert len(marks) == 1


def test_marking_a_run_nothing_is_measuring_is_a_no_op():
    mark_first_stream_text("run-absent", "messages", _ai_chunk("Hello"))


# ── Model-side callback seam ─────────────────────────────────────────────


def test_the_callback_handler_times_request_first_text_and_completion():
    journal = TurnPhaseJournal(correlation_id="trace-model")
    handler = TurnPhaseCallbackHandler(journal)

    handler.on_chat_model_start({}, [])
    handler.on_llm_new_token("")
    handler.on_llm_new_token("   ")
    handler.on_llm_new_token("Hello")
    handler.on_llm_new_token(" world")
    handler.on_llm_end(None)

    snapshot = journal.snapshot()
    assert snapshot.phase_at_ms(TurnPhase.MODEL_REQUEST) is not None
    assert len([r for r in snapshot.phases if r.phase is TurnPhase.FIRST_PROVIDER_TEXT]) == 1
    assert snapshot.phase_at_ms(TurnPhase.MODEL_COMPLETION) is not None


def test_the_callback_handler_records_a_model_error_as_a_failed_attempt():
    journal = TurnPhaseJournal(correlation_id="trace-model-error")
    handler = TurnPhaseCallbackHandler(journal)

    handler.on_llm_start({}, [])
    handler.on_llm_error(RuntimeError("provider said something untrusted"))

    snapshot = journal.snapshot()
    assert snapshot.failed_attempts == 1
    assert snapshot.phase_at_ms(TurnPhase.MODEL_COMPLETION) is not None
    assert all("untrusted" not in str(record.detail) for record in snapshot.phases)


def test_the_callback_handler_ignores_the_hooks_it_does_not_implement():
    handler = TurnPhaseCallbackHandler(TurnPhaseJournal(correlation_id="trace-ignore"))

    handler.on_tool_start({}, "input")
    handler.on_chain_end({})

    with pytest.raises(AttributeError):
        handler.not_a_callback


def test_the_callback_handler_judges_a_structured_chunk_by_its_blocks():
    journal = TurnPhaseJournal(correlation_id="trace-chunk")
    handler = TurnPhaseCallbackHandler(journal)
    thinking = SimpleNamespace(message=SimpleNamespace(content=[{"type": "thinking", "thinking": "hidden"}]))
    text = SimpleNamespace(message=SimpleNamespace(content=[{"type": "text", "text": "Hello"}]))

    handler.on_llm_new_token("hidden", chunk=thinking)
    assert journal.snapshot().phase_at_ms(TurnPhase.FIRST_PROVIDER_TEXT) is None
    handler.on_llm_new_token("", chunk=text)
    assert journal.snapshot().phase_at_ms(TurnPhase.FIRST_PROVIDER_TEXT) is not None


def test_the_emitted_message_carries_the_timings_a_plain_formatter_shows(caplog):
    """The numbers must survive the formatter a deployment actually runs.

    ``extra`` reaches nothing a released deployment prints: the default text
    format renders ``%(message)s`` and the JSON formatter builds a fixed
    payload. A journal whose whole purpose is telling an operator where a
    turn's time went has to put the timings in the message itself.
    """
    journal = TurnPhaseJournal(correlation_id="trace-msg", run_id="run-msg")
    journal.set_acquisition_source(AcquisitionSource.ACCEPTED_WARM_RECLAIM)
    journal.set_session_kind("accepted")
    with journal.span(TurnPhase.SANDBOX_ACQUIRE):
        pass
    journal.mark_once(TurnPhase.FIRST_STREAM_TEXT)
    journal.set_outcome("success")

    with caplog.at_level(logging.INFO, logger="deerflow.runtime.turn_phases"):
        journal.emit()

    message = caplog.records[0].getMessage()
    assert message.startswith("turn phase timings")
    assert "run=run-msg" in message
    assert "outcome=success" in message
    assert "acquisition=accepted_warm_reclaim" in message
    assert "kind=accepted" in message
    assert "first_stream_text@" in message
    assert "sandbox_acquire@" in message
    # The structured record stays for anything that reads fields.
    assert caplog.records[0].turn_phases["run_id"] == "run-msg"
    # And the same reading survives the format a released deployment runs.
    rendered = logging.Formatter(DEFAULT_LOG_FORMAT, datefmt=DEFAULT_LOG_DATE_FORMAT).format(caplog.records[0])
    assert "first_stream_text@" in rendered and "run=run-msg" in rendered


def test_an_unobservable_phase_is_named_in_the_message_with_its_reason(caplog):
    journal = TurnPhaseJournal(correlation_id="trace-unobs", run_id="run-unobs")
    journal.unobservable(TurnPhase.FIRST_STREAM_TEXT, "no SSE consumer in this process marked it")

    with caplog.at_level(logging.INFO, logger="deerflow.runtime.turn_phases"):
        journal.emit()

    message = caplog.records[0].getMessage()
    # The reason keeps the journal's own bounded-label spelling.
    assert "unobservable=first_stream_text(no_SSE_consumer_in_this_process_marked_it)" in message


def test_the_rendered_message_stays_bounded_when_a_turn_records_many_phases(caplog):
    journal = TurnPhaseJournal(correlation_id="trace-many", run_id="run-many")
    for _ in range(MAX_PHASE_RECORDS + 20):
        journal.mark(TurnPhase.SANDBOX_LOOKUP)

    with caplog.at_level(logging.INFO, logger="deerflow.runtime.turn_phases"):
        journal.emit()

    message = caplog.records[0].getMessage()
    assert len(message) <= 2048
    assert "more" in message
    # Nothing is lost from the structured record the message summarises.
    assert len(caplog.records[0].turn_phases["phases"]) == MAX_PHASE_RECORDS
