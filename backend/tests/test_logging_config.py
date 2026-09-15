import io
import json
import logging
from datetime import UTC, datetime
from types import SimpleNamespace

from deerflow.logging_config import TraceContextFilter, configure_logging
from deerflow.trace_context import request_trace_context


def test_trace_context_filter_injects_current_trace_id() -> None:
    record = logging.LogRecord("deerflow.test", logging.INFO, __file__, 1, "hello", (), None)

    with request_trace_context("trace-log-1"):
        assert TraceContextFilter().filter(record) is True

    assert record.trace_id == "trace-log-1"


def test_configure_logging_enhanced_text_includes_trace_id() -> None:
    root = logging.getLogger()
    old_handlers = root.handlers[:]
    old_level = root.level
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)

    try:
        root.handlers = [handler]
        root.setLevel(logging.INFO)
        config = SimpleNamespace(
            log_level="info",
            logging=SimpleNamespace(enhance=SimpleNamespace(enabled=True, format="text")),
        )
        configure_logging(config)

        with request_trace_context("trace-log-2"):
            logging.getLogger("deerflow.test").info("hello")

        assert "[trace_id=trace-log-2]" in stream.getvalue()
    finally:
        root.handlers = old_handlers
        root.setLevel(old_level)


def test_json_format_carries_the_turn_phase_record() -> None:
    """A JSON log that drops the one structured field it is given is not structured.

    The turn-phase journal's whole payload rides on ``extra``; under the JSON
    formatter it was discarded, so a deployment that chose JSON logging saw
    exactly as little as one that did not.
    """
    root = logging.getLogger()
    old_handlers = root.handlers[:]
    old_level = root.level
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)

    try:
        root.handlers = [handler]
        root.setLevel(logging.INFO)
        config = SimpleNamespace(
            log_level="info",
            logging=SimpleNamespace(enhance=SimpleNamespace(enabled=True, format="json")),
        )
        configure_logging(config)

        wire = {"version": 3, "run_id": "run-json", "total_ms": 12.5}
        logging.getLogger("deerflow.test").info("turn phase timings run=run-json", extra={"turn_phases": wire})

        payload = json.loads(stream.getvalue().strip().splitlines()[-1])
        assert payload["message"] == "turn phase timings run=run-json"
        assert payload["turn_phases"] == wire
    finally:
        root.handlers = old_handlers
        root.setLevel(old_level)


def test_json_format_ignores_a_turn_phase_field_that_is_not_a_record() -> None:
    """Only the journal's own versioned record travels; the name alone earns nothing.

    The field is named for a module with a closed-vocabulary disclosure
    contract. Anything else setting ``turn_phases`` does not inherit it, so the
    journal's own version stamp is what admits the payload.
    """
    root = logging.getLogger()
    old_handlers = root.handlers[:]
    old_level = root.level
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)

    try:
        root.handlers = [handler]
        root.setLevel(logging.INFO)
        configure_logging(
            SimpleNamespace(
                log_level="info",
                logging=SimpleNamespace(enhance=SimpleNamespace(enabled=True, format="json")),
            )
        )

        logging.getLogger("deerflow.test").info("hello", extra={"turn_phases": "not-a-record"})
        logging.getLogger("deerflow.test").info("hello", extra={"turn_phases": {"run_id": "unstamped"}})

        lines = stream.getvalue().strip().splitlines()
        assert len(lines) == 2
        for line in lines:
            assert "turn_phases" not in json.loads(line)
    finally:
        root.handlers = old_handlers
        root.setLevel(old_level)


def test_a_json_record_survives_a_value_json_cannot_serialise() -> None:
    """A formatter that raises loses the record it was formatting."""
    root = logging.getLogger()
    old_handlers = root.handlers[:]
    old_level = root.level
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)

    try:
        root.handlers = [handler]
        root.setLevel(logging.INFO)
        configure_logging(
            SimpleNamespace(
                log_level="info",
                logging=SimpleNamespace(enhance=SimpleNamespace(enabled=True, format="json")),
            )
        )

        logging.getLogger("deerflow.test").info(
            "turn phase timings run=run-odd",
            extra={"turn_phases": {"version": 3, "run_id": "run-odd", "at": datetime(2026, 9, 15, tzinfo=UTC)}},
        )

        payload = json.loads(stream.getvalue().strip().splitlines()[-1])
        assert payload["message"] == "turn phase timings run=run-odd"
        assert payload["turn_phases"]["at"].startswith("2026-09-15")
    finally:
        root.handlers = old_handlers
        root.setLevel(old_level)
