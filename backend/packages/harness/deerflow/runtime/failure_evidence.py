"""Bounded runtime failure and terminal evidence.

Exception messages and tracebacks are provider-controlled data.  This module is
the single conversion boundary from an arbitrary failure into the small shape
that may be persisted in run rows/events, sent over SSE, or written to operator
logs.  Intentional conversation and ``run.end`` output content do not pass
through this mapper.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal
from uuid import uuid4

_SAFE_LABEL = re.compile(r"[^A-Za-z0-9_.:/@-]", re.ASCII)
_TERMINAL_STATUSES = frozenset({"success", "error", "timeout", "interrupted"})


def _bounded_label(value: object, *, fallback: str, limit: int) -> str:
    if not isinstance(value, str):
        return fallback
    sanitized = _SAFE_LABEL.sub("_", value)
    return (sanitized or fallback)[:limit]


@dataclass(frozen=True, slots=True)
class RuntimeFailureV1:
    """Safe, attributable failure facts with no message or traceback."""

    version: Literal[1]
    code: str
    error_class: str
    correlation_id: str

    def to_event_body(self) -> dict[str, str | int]:
        return {
            "version": self.version,
            "code": self.code,
            "error_class": self.error_class,
            "correlation_id": self.correlation_id,
        }

    @property
    def public_message(self) -> str:
        """Bounded client/run-row message suitable for untrusted failures."""

        return f"Runtime operation failed (reference: {self.correlation_id})"


def map_runtime_failure(
    *,
    code: str,
    error: BaseException | None = None,
    error_class: str | None = None,
) -> RuntimeFailureV1:
    """Map exactly one arbitrary failure signal to safe V1 evidence.

    ``error_class`` supports already-classified failure signals such as a model
    fallback marker.  It is mutually exclusive with ``error`` so callers never
    accidentally mix an untrusted exception with a claimed classification.
    """

    if (error is None) == (error_class is None):
        raise ValueError("supply exactly one of error or error_class")
    resolved_class = type(error).__name__ if error is not None else error_class
    return RuntimeFailureV1(
        version=1,
        code=_bounded_label(code, fallback="runtime_failure", limit=64),
        error_class=_bounded_label(resolved_class, fallback="Exception", limit=128),
        correlation_id=uuid4().hex,
    )


@dataclass(frozen=True, slots=True)
class TerminalSummaryV1:
    """Bounded terminal fact independent of opaque graph output content."""

    version: Literal[1]
    status: str
    stop_reason: str | None
    failure: RuntimeFailureV1 | None

    def __post_init__(self) -> None:
        if self.status not in _TERMINAL_STATUSES:
            raise ValueError("terminal status is invalid")
        if self.stop_reason is not None:
            safe_reason = _bounded_label(self.stop_reason, fallback="runtime_failure", limit=64)
            if safe_reason != self.stop_reason:
                raise ValueError("terminal stop reason is not a safe label")
        if self.status == "success" and self.failure is not None:
            raise ValueError("successful terminal summary cannot carry a failure")

    def to_event_body(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "status": self.status,
            "stop_reason": self.stop_reason,
            "failure": None if self.failure is None else self.failure.to_event_body(),
        }


__all__ = [
    "RuntimeFailureV1",
    "TerminalSummaryV1",
    "map_runtime_failure",
]
