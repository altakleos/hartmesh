"""The turn phase journal: what it measures, what it refuses to claim."""

from __future__ import annotations

import contextlib
import logging
import threading
import time
from types import SimpleNamespace

import pytest

from deerflow.logging_config import DEFAULT_LOG_DATE_FORMAT, DEFAULT_LOG_FORMAT
from deerflow.runtime.turn_phases import (
    MAX_PHASE_RECORDS,
    MAX_RENDERED_PHASES,
    MAX_TRACKED_RUNS,
    MAX_TRACKED_TOOL_NAMES,
    OTHER_TOOLS_LABEL,
    TOOL_LABEL_LIMIT,
    AcquisitionSource,
    TurnPhase,
    TurnPhaseCallbackHandler,
    TurnPhaseJournal,
    _tool_label,
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

    handler.on_chain_end({})
    handler.on_agent_action(None)
    handler.on_retriever_start({}, "query")

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


def test_a_truncated_render_keeps_the_end_of_the_turn(caplog):
    """Truncation takes the middle, not the end.

    A turn with goal continuations records a graph start and a binding per
    attempt, so a head-only render drops ``model_completion`` and ``terminal``
    -- where the arithmetic this line exists for finishes -- before it drops a
    repeated early span.
    """
    journal = TurnPhaseJournal(correlation_id="trace-tail", run_id="run-tail")
    for _ in range(MAX_RENDERED_PHASES + 10):
        journal.mark(TurnPhase.SANDBOX_BINDING)
    journal.mark(TurnPhase.MODEL_COMPLETION)
    journal.mark(TurnPhase.TERMINAL)

    with caplog.at_level(logging.INFO, logger="deerflow.runtime.turn_phases"):
        journal.emit()

    message = caplog.records[0].getMessage()
    assert "more" in message
    assert "model_completion@" in message, message
    assert "terminal@" in message, message
    # The record behind the message still has every phase.
    assert len(caplog.records[0].turn_phases["phases"]) == MAX_RENDERED_PHASES + 12


# ── The acquisition label must keep the turn's origin ─────────────────────
#
# Tenant-class .15 read `acquisition=accepted_active` on all seven turns,
# including the cold one that also carried `creates=1` and a measured
# `sandbox_create` span, and the warm ones whose provider logged a warm-pool
# reclaim. A turn acquires in stages -- the worker's accepted projection early,
# the sandbox middleware's binding later -- and the later stage only observes
# that the container is already in hand. Last-writer-wins therefore threw away
# the one thing the field exists to disclose.


def test_an_active_reuse_observation_does_not_replace_the_recorded_origin():
    journal = TurnPhaseJournal(correlation_id="trace-origin", run_id="run-origin")

    journal.set_acquisition_source(AcquisitionSource.ACCEPTED_WARM_RECLAIM)
    journal.set_acquisition_source(AcquisitionSource.ACCEPTED_ACTIVE)

    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.ACCEPTED_WARM_RECLAIM
    assert snapshot.acquisition_reuse is AcquisitionSource.ACCEPTED_ACTIVE


def test_a_create_survives_the_binding_stage_that_finds_the_container_active():
    journal = TurnPhaseJournal(correlation_id="trace-cold", run_id="run-cold")

    journal.set_acquisition_source(AcquisitionSource.CREATED)
    journal.record_resource_create()
    journal.set_acquisition_source(AcquisitionSource.ACCEPTED_ACTIVE)
    journal.set_acquisition_source(AcquisitionSource.IN_PROCESS)

    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.CREATED
    # The counter and the label agree, which is what the tenant run could not say.
    assert snapshot.resource_creates == 1
    assert snapshot.acquisition_reuse is AcquisitionSource.IN_PROCESS


def test_a_turn_that_only_observed_reuse_reports_that_reuse():
    journal = TurnPhaseJournal(correlation_id="trace-active", run_id="run-active")

    journal.set_acquisition_source(AcquisitionSource.ACCEPTED_ACTIVE)

    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.ACCEPTED_ACTIVE
    assert snapshot.acquisition_reuse is None
    assert "reused=" not in snapshot.to_log_line()


def test_an_origin_recorded_after_a_reuse_observation_supersedes_it():
    """Order is not precedence: an origin is the answer whenever one is known."""
    journal = TurnPhaseJournal(correlation_id="trace-late", run_id="run-late")

    journal.set_acquisition_source(AcquisitionSource.ACCEPTED_ACTIVE)
    journal.set_acquisition_source(AcquisitionSource.CREATED)

    snapshot = journal.snapshot()
    assert snapshot.acquisition_source is AcquisitionSource.CREATED
    assert snapshot.acquisition_reuse is None


def test_a_second_origin_replaces_the_first():
    """A reclaim that failed into a create is described by the create."""
    journal = TurnPhaseJournal(correlation_id="trace-two", run_id="run-two")

    journal.set_acquisition_source(AcquisitionSource.ACCEPTED_WARM_RECLAIM)
    journal.set_acquisition_source(AcquisitionSource.CREATED)

    assert journal.snapshot().acquisition_source is AcquisitionSource.CREATED


def test_the_line_and_the_record_both_carry_the_origin_and_the_reuse(caplog):
    journal = TurnPhaseJournal(correlation_id="trace-both", run_id="run-both")
    journal.set_acquisition_source(AcquisitionSource.ACCEPTED_WARM_RECLAIM)
    journal.set_acquisition_source(AcquisitionSource.ACCEPTED_ACTIVE)

    with caplog.at_level(logging.INFO, logger="deerflow.runtime.turn_phases"):
        journal.emit()

    message = caplog.records[0].getMessage()
    assert "acquisition=accepted_warm_reclaim" in message
    assert "reused=accepted_active" in message
    wire = caplog.records[0].turn_phases
    assert wire["acquisition_source"] == "accepted_warm_reclaim"
    assert wire["acquisition_reuse"] == "accepted_active"


def test_the_pre_model_phases_name_the_work_between_admission_and_the_model():
    """The .15 run left 2.6 to 3.4 s of every turn unaccounted for.

    Between the sandbox lookup ending and the binding starting no phase said
    anything, so an operator could see that the time was spent but not on what.
    """
    journal = TurnPhaseJournal(correlation_id="trace-pre", run_id="run-pre")

    journal.mark(TurnPhase.ADMISSION)
    with journal.span(TurnPhase.SKILL_MATERIALIZATION), journal.span(TurnPhase.SANDBOX_LOOKUP):
        pass
    with journal.span(TurnPhase.AGENT_BUILD):
        pass
    journal.mark(TurnPhase.CHECKPOINT_PREFLIGHT)
    journal.mark(TurnPhase.GRAPH_START)
    journal.mark(TurnPhase.MODEL_REQUEST)

    snapshot = journal.snapshot()
    assert snapshot.phase_ms(TurnPhase.AGENT_BUILD) is not None
    assert snapshot.phase_at_ms(TurnPhase.SKILL_MATERIALIZATION) is not None
    for phase in (TurnPhase.CHECKPOINT_PREFLIGHT, TurnPhase.GRAPH_START):
        assert snapshot.phase_at_ms(phase) is not None
    line = snapshot.to_log_line()
    for phase in ("skill_materialization@", "agent_build@", "checkpoint_preflight@", "graph_start@"):
        assert phase in line, line


@pytest.mark.parametrize("source", list(AcquisitionSource))
def test_every_source_is_classified_as_an_origin_or_an_observation(source):
    """The partition is the whole repair; a ninth source must not default into it.

    Only the two that say "the container was already in hand" are
    observations. Everything else names how the container came to be, and a
    new member silently taking the origin side is the defect this pins.
    """
    observation = source in {AcquisitionSource.IN_PROCESS, AcquisitionSource.ACCEPTED_ACTIVE}
    assert source.observes_active_reuse is observation
    if observation:
        # An observation is also a reuse in the wider "paid for no creation"
        # sense; the two questions stay separate but must agree here.
        assert source.reuses_existing_resource


def test_a_second_graph_start_is_recorded_rather_than_collapsed():
    """A resumed or retried stream is a second graph start, and the turn paid for both."""
    journal = TurnPhaseJournal(correlation_id="trace-retry", run_id="run-retry")

    journal.mark(TurnPhase.GRAPH_START)
    journal.mark(TurnPhase.GRAPH_START)

    line = journal.snapshot().to_log_line()
    assert line.count("graph_start@") == 2, line


def test_the_launch_interval_is_carried_from_the_route_into_the_journal(caplog):
    """The `.19` run left 1.4 to 3.4 s of every turn before the journal opened.

    The journal starts at worker admission, so the route's own work -- sealing
    the accepted invocation, persisting the row, handing the record to the
    worker -- had no phase and no line to attribute it to. The launch records
    its steps against the request's own monotonic stamp and the worker hands
    them to the journal, which renders them beside the phases.
    """
    from deerflow.runtime.turn_phases import LaunchTimings

    received_at = time.monotonic() - 2.5
    persisted_at = received_at + 2.4
    timings = LaunchTimings(
        received_at=received_at,
        persisted_at=persisted_at,
        steps=(("identify", 3.0), ("seal", 2210.4), ("persist", 11.2)),
    )
    journal = TurnPhaseJournal(correlation_id="trace-launch", run_id="run-launch")
    journal.set_launch(timings)
    journal.mark(TurnPhase.ADMISSION)

    snapshot = journal.snapshot()
    # From the request's stamp to the journal's own start, not to now.
    assert snapshot.launch_ms is not None and 2450 <= snapshot.launch_ms <= 2600
    # From the persisted row to the journal's start: the worker handoff.
    assert snapshot.launch_handoff_ms is not None and 50 <= snapshot.launch_handoff_ms <= 200
    assert snapshot.launch_steps == (("identify", 3.0), ("seal", 2210.4), ("persist", 11.2))

    with caplog.at_level(logging.INFO, logger="deerflow.runtime.turn_phases"):
        journal.emit()
    message = caplog.records[0].getMessage()
    assert "launch=" in message and "(identify=3ms,seal=2210ms,persist=11ms,handoff=" in message, message
    # The launch reads before the phases: it is what happened before them.
    assert message.index("launch=") < message.index("phases="), message
    wire = caplog.records[0].turn_phases
    assert wire["launch"]["total_ms"] == round(snapshot.launch_ms, 3)
    assert wire["launch"]["steps"] == [{"step": "identify", "ms": 3.0}, {"step": "seal", "ms": 2210.4}, {"step": "persist", "ms": 11.2}]


def test_a_turn_with_no_launch_timings_says_nothing_about_a_launch(caplog):
    journal = TurnPhaseJournal(correlation_id="trace-nolaunch", run_id="run-nolaunch")
    journal.mark(TurnPhase.ADMISSION)
    snapshot = journal.snapshot()
    assert snapshot.launch_ms is None and snapshot.launch_steps == ()
    with caplog.at_level(logging.INFO, logger="deerflow.runtime.turn_phases"):
        journal.emit()
    assert "launch=" not in caplog.records[0].getMessage()
    assert caplog.records[0].turn_phases["launch"] is None


def test_launch_step_names_are_bounded_labels_and_a_stamp_behind_the_request_is_clamped():
    from deerflow.runtime.turn_phases import LaunchTimings

    now = time.monotonic()
    timings = LaunchTimings(received_at=now, persisted_at=now, steps=(("seal it now!", -5.0),))
    assert timings.steps == (("seal_it_now_", 0.0),)
    # Diagnostics never fail a run: an impossible ordering is clamped, not raised.
    clamped = LaunchTimings(received_at=now + 1, persisted_at=now, steps=())
    assert clamped.persisted_at == clamped.received_at == now + 1


# ── Where a working turn's time goes ──────────────────────────────────────


def test_tool_work_opens_the_tool_execution_phase_once_and_counts_every_call():
    journal = TurnPhaseJournal(correlation_id="trace-tools")

    for call in ("a", "b"):
        journal.record_tool_start(call)
        journal.record_tool_end(call)

    snapshot = journal.snapshot()
    assert snapshot.tool_calls == 2
    opened = [record for record in snapshot.phases if record.phase is TurnPhase.TOOL_EXECUTION]
    assert len(opened) == 1, "the phase names where tool work began, not each call"


def test_a_turn_that_runs_no_tools_reports_no_tool_time():
    journal = TurnPhaseJournal(correlation_id="trace-no-tools")

    snapshot = journal.snapshot()
    assert snapshot.tool_calls == 0
    assert snapshot.tool_ms == 0.0
    assert snapshot.phase_at_ms(TurnPhase.TOOL_EXECUTION) is None


def test_overlapping_tool_calls_are_counted_once_not_summed():
    """Two tools running together cost the turn one stretch of wall clock.

    Summing durations would report more tool time than the turn took, which
    is how an attribution loses the reader's trust: the named work must stay
    inside the total.
    """
    journal = TurnPhaseJournal(correlation_id="trace-parallel")

    journal.record_tool_start("a")
    journal.record_tool_start("b")
    time.sleep(0.02)
    journal.record_tool_end("a")
    journal.record_tool_end("b")

    snapshot = journal.snapshot()
    assert snapshot.tool_calls == 2
    assert snapshot.tool_ms >= 15.0
    assert snapshot.tool_ms <= snapshot.total_ms
    assert snapshot.tool_ms < 35.0, "two overlapping 20ms calls are one stretch, not two"


def test_a_tool_still_running_is_reported_as_open_not_as_measured_time():
    """An end the journal never saw is not a duration it may claim."""
    journal = TurnPhaseJournal(correlation_id="trace-unfinished")

    journal.record_tool_start("a")
    time.sleep(0.02)

    snapshot = journal.snapshot()
    assert snapshot.tool_calls == 1
    assert snapshot.tool_ms == 0.0, "nothing completed, so nothing is measured"
    assert snapshot.tool_open == 1
    assert snapshot.tool_open_ms >= 15.0
    assert "tools_open=1/" in snapshot.to_log_line()


def test_a_tool_end_without_a_start_invents_no_span():
    journal = TurnPhaseJournal(correlation_id="trace-stray-end")

    journal.record_tool_end("never-started")

    snapshot = journal.snapshot()
    assert snapshot.tool_ms == 0.0
    assert snapshot.tool_calls == 0
    assert snapshot.tool_open == 0


def test_model_calls_after_the_first_are_counted_and_timed():
    """``model_request`` names the first call only; a working turn makes many."""
    journal = TurnPhaseJournal(correlation_id="trace-model-calls")

    for call in range(3):
        journal.record_model_start(call)
        journal.record_model_end(call)

    snapshot = journal.snapshot()
    assert snapshot.model_calls == 3
    assert len([r for r in snapshot.phases if r.phase is TurnPhase.MODEL_REQUEST]) == 1
    assert snapshot.model_ms >= 0.0


def test_tool_and_model_time_reach_the_line_an_operator_reads():
    journal = TurnPhaseJournal(correlation_id="trace-line", run_id="run-line")
    journal.record_model_start("m")
    journal.record_model_end("m")
    journal.record_tool_start("t")
    time.sleep(0.01)
    journal.record_tool_end("t")

    line = journal.snapshot().to_log_line()
    assert "tools=1/" in line
    assert "model=1/" in line


def test_the_wire_record_carries_the_working_turn_fields_at_a_new_version():
    journal = TurnPhaseJournal(correlation_id="trace-wire")
    journal.record_tool_start("t")
    journal.record_tool_end("t")

    wire = journal.snapshot().to_wire()
    assert wire["version"] == 6
    assert wire["tool_calls"] == 1
    assert wire["model_calls"] == 0
    for field in ("tool_ms", "tool_open", "tool_open_ms", "model_ms", "model_open", "model_open_ms", "busy_ms"):
        assert field in wire


def test_the_callback_handler_records_the_tool_name_and_nothing_else_about_the_call():
    """The name is the point; the arguments and the output are never recorded.

    Naming the tool is what turns "it was tools" into something an operator
    can act on. It is also the one thing this module's disclosure contract
    admits about a call, so the rest is pinned here rather than assumed.
    """
    journal = TurnPhaseJournal(correlation_id="trace-tool-callback")
    handler = TurnPhaseCallbackHandler(journal)

    handler.on_tool_start({"name": "execute_command", "description": "Run a command."}, "rm -rf /tenant-secret", run_id="call-1")
    time.sleep(0.01)
    handler.on_tool_end("SECRET-OUTPUT", run_id="call-1")

    snapshot = journal.snapshot()
    assert snapshot.tool_calls == 1
    assert snapshot.tool_ms >= 5.0
    assert [(name, calls) for name, calls, _ms in snapshot.tool_names] == [("execute_command", 1)]
    rendered = snapshot.to_log_line() + repr(snapshot.to_wire())
    assert "execute_command" in rendered
    assert "tenant-secret" not in rendered
    assert "SECRET-OUTPUT" not in rendered
    assert "Run a command" not in rendered


def test_the_tool_breakdown_names_the_call_that_spent_the_turn():
    journal = TurnPhaseJournal(correlation_id="trace-breakdown")

    journal.record_tool_start("slow", name="execute_command")
    time.sleep(0.03)
    journal.record_tool_end("slow")
    journal.record_tool_start("fast", name="read_file")
    journal.record_tool_end("fast")

    snapshot = journal.snapshot()
    names = [name for name, _calls, _ms in snapshot.tool_names]
    assert names == ["execute_command", "read_file"], "largest first, so the guilty call reads first"
    assert "tools=2/" in snapshot.to_log_line()
    assert "(execute_command=1/" in snapshot.to_log_line()


def test_a_tool_plane_that_mints_names_cannot_grow_the_journal():
    """Operator configuration decides these names, so the set has to be capped."""
    journal = TurnPhaseJournal(correlation_id="trace-many-names")

    for index in range(MAX_TRACKED_TOOL_NAMES * 3):
        journal.record_tool_start(index, name=f"mcp_tool_{index}")
        journal.record_tool_end(index)

    snapshot = journal.snapshot()
    assert snapshot.tool_calls == MAX_TRACKED_TOOL_NAMES * 3
    assert len(snapshot.tool_names) <= MAX_TRACKED_TOOL_NAMES + 1
    pooled = {name: calls for name, calls, _ms in snapshot.tool_names}
    assert pooled[OTHER_TOOLS_LABEL] == MAX_TRACKED_TOOL_NAMES * 2
    assert snapshot.to_log_line().count("=") < 40, "the line stays readable"


def test_a_name_that_cannot_be_logged_as_itself_stays_distinguishable():
    """Sanitizing must not merge two tools into one row.

    The log line is built from ``,`` ``=`` ``(`` ``)``, so a name carrying
    them is neutralized -- but neutralizing maps whole families of names onto
    the same string. Every tool named in a non-Latin script becomes one run of
    underscores, and two long MCP names sharing a prefix become one label.
    Merging is exactly what a per-tool breakdown must never do.
    """
    assert _tool_label("execute_command") == "execute_command", "an ordinary name is untouched"
    assert _tool_label("evil=9/99999ms,fake").count("=") == 0, "no forged field"
    distinct = {_tool_label(name) for name in ("\u65e5\u672c\u8a9e\u30c4\u30fc\u30eb", "\u5225\u306e\u30c4\u30fc\u30eb")}
    assert len(distinct) == 2, "two tools, two rows"
    long_prefix = "mcp__" + "x" * 60
    assert _tool_label(f"{long_prefix}__alpha") != _tool_label(f"{long_prefix}__beta")
    assert all(len(label) <= TOOL_LABEL_LIMIT for label in distinct)


def test_calls_pooled_past_the_name_cap_carry_their_time_with_them():
    """The pooled bucket has to hold the milliseconds, not just the count."""
    journal = TurnPhaseJournal(correlation_id="trace-pooled")
    for index in range(MAX_TRACKED_TOOL_NAMES):
        journal.record_tool_start(index, name=f"tool_{index}")
        journal.record_tool_end(index)

    journal.record_tool_start("over", name="one_name_too_many")
    time.sleep(0.03)
    journal.record_tool_end("over")

    pooled = {name: (calls, ms) for name, calls, ms in journal.snapshot().tool_names}
    assert pooled[OTHER_TOOLS_LABEL][0] == 1
    assert pooled[OTHER_TOOLS_LABEL][1] >= 15.0, "the over-cap call's time is pooled, not lost"


def test_an_unnamed_tool_is_counted_under_a_label_rather_than_dropped():
    journal = TurnPhaseJournal(correlation_id="trace-unnamed")

    journal.record_tool_start("a")
    journal.record_tool_end("a")

    snapshot = journal.snapshot()
    assert [name for name, _calls, _ms in snapshot.tool_names] == ["unnamed"]


def test_the_callback_handler_closes_a_tool_that_raised():
    journal = TurnPhaseJournal(correlation_id="trace-tool-error")
    handler = TurnPhaseCallbackHandler(journal)

    handler.on_tool_start({}, "input", run_id="call-1")
    handler.on_tool_error(RuntimeError("provider said something untrusted"), run_id="call-1")
    journal.record_tool_start("call-2")
    journal.record_tool_end("call-2")

    snapshot = journal.snapshot()
    assert snapshot.tool_calls == 2
    assert all("untrusted" not in str(record.detail) for record in snapshot.phases)


def test_the_callback_handler_hears_tool_events_at_all():
    """LangChain gates every tool callback on ``ignore_agent``.

    Left at the handler's earlier ``True`` the tool hooks below are never
    called, and tool time stays the residual this phase exists to name.
    """
    assert TurnPhaseCallbackHandler.ignore_agent is False


def test_a_real_tool_invocation_reaches_the_handler_the_worker_attaches():
    """The flag is only half of it: LangChain has to route the event here.

    This is the wiring the phase depends on -- a tool invoked the way the
    graph invokes one, with the handler in ``config["callbacks"]`` exactly as
    ``run_agent`` puts it there.
    """
    from langchain_core.tools import tool

    @tool
    def slow_thing(argument: str) -> str:
        """A tool that takes a moment."""
        time.sleep(0.02)
        return "done"

    journal = TurnPhaseJournal(correlation_id="trace-real-tool")
    handler = TurnPhaseCallbackHandler(journal)

    slow_thing.invoke({"argument": "x"}, config={"callbacks": [handler]})

    snapshot = journal.snapshot()
    assert snapshot.tool_calls == 1, "LangChain never delivered the tool event"
    assert snapshot.tool_ms >= 15.0
    assert snapshot.phase_at_ms(TurnPhase.TOOL_EXECUTION) is not None


def test_a_full_journal_does_not_count_every_later_tool_call_as_a_dropped_record():
    """The cap loses records; it must not make the loss look worse than it is.

    ``dropped_records`` is what an operator reads to decide whether to trust
    the rest of the line. A turn that ran hundreds of tools after filling the
    cap would have reported hundreds of drops for one record.
    """
    journal = TurnPhaseJournal(correlation_id="trace-full")
    for _ in range(MAX_PHASE_RECORDS):
        journal.mark(TurnPhase.GRAPH_START)
    for call in range(50):
        journal.record_tool_start(call)
        journal.record_tool_end(call)

    snapshot = journal.snapshot()
    assert snapshot.tool_calls == 50
    assert snapshot.dropped_records <= 1


def test_a_cancelled_async_tool_does_not_turn_the_rest_of_the_turn_into_tool_time():
    """The end event LangChain never sends must cost one call, not the turn.

    ``asyncio.CancelledError`` is a ``BaseException`` and LangChain's tool base
    catches ``Exception``, so a cancelled async tool reaches neither
    ``on_tool_end`` nor ``on_tool_error``. Counting depth alone, that one lost
    event reported a measured 100 ms of work as ``tools=1/601ms`` on a 602 ms
    turn -- the residual this phase exists to remove, inverted. Subagent
    timeouts and run aborts both reach it, and the turn carries on afterwards.
    """
    import asyncio

    from langchain_core.tools import tool

    @tool
    async def never_returns(argument: str) -> str:
        """A tool the caller gives up on."""
        await asyncio.sleep(30)
        return "done"

    async def scenario() -> TurnPhaseJournal:
        journal = TurnPhaseJournal(correlation_id="trace-cancelled")
        handler = TurnPhaseCallbackHandler(journal)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(never_returns.ainvoke({"argument": "x"}, config={"callbacks": [handler]}), timeout=0.05)
        await asyncio.sleep(0.2)  # the turn carries on: pure non-tool time
        return journal

    snapshot = asyncio.run(scenario()).snapshot()
    assert snapshot.tool_calls == 1
    assert snapshot.tool_ms == 0.0, "nothing completed, so nothing may be claimed as measured"
    assert snapshot.tool_open == 1
    assert snapshot.busy_ms < snapshot.total_ms / 2, "idle time after the cancellation is not tool time"
    assert "tools_open=1/" in snapshot.to_log_line()


def test_a_lost_end_does_not_stop_later_tool_calls_being_measured():
    """Depth counting lost every later call too; intervals must not."""
    journal = TurnPhaseJournal(correlation_id="trace-after-loss")

    journal.record_tool_start("lost")  # never ends
    journal.record_tool_start("real")
    time.sleep(0.02)
    journal.record_tool_end("real")

    snapshot = journal.snapshot()
    assert snapshot.tool_calls == 2
    assert snapshot.tool_ms >= 15.0, "the call that did finish is still measurable"
    assert snapshot.tool_open == 1


def test_busy_time_merges_both_kinds_so_the_residual_stays_positive():
    """A tool and a model call can overlap; their sum can exceed the turn."""
    journal = TurnPhaseJournal(correlation_id="trace-busy")

    journal.record_tool_start("t")
    journal.record_model_start("m")
    time.sleep(0.02)
    journal.record_tool_end("t")
    journal.record_model_end("m")

    snapshot = journal.snapshot()
    assert snapshot.busy_ms <= snapshot.total_ms
    assert snapshot.busy_ms < snapshot.tool_ms + snapshot.model_ms, "the overlap is counted once"
    assert "busy=" in snapshot.to_log_line()
