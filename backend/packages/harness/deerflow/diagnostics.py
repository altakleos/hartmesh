"""Bounded diagnostics and authoritative-operation contract checks.

Provider exception text and tracebacks are untrusted.  This module is the one
owner for turning such failures into attributable, correlation-safe records.
"""

from __future__ import annotations

import inspect
import logging
import re
from dataclasses import dataclass
from uuid import uuid4

_SAFE_LABEL = re.compile(r"[^A-Za-z0-9_.:/@-]", re.ASCII)


def _bounded_label(value: object, *, fallback: str, limit: int = 160) -> str:
    if not isinstance(value, str):
        return fallback
    sanitized = _SAFE_LABEL.sub("_", value)
    return (sanitized or fallback)[:limit]


@dataclass(frozen=True, slots=True)
class BoundedDiagnostic:
    """Safe attribution for one provider-controlled failure."""

    code: str
    operation: str
    error_class: str
    correlation_id: str
    capability_id: str | None = None
    contribution_id: str | None = None


def bounded_diagnostic(
    *,
    code: str,
    operation: str,
    error: BaseException,
    capability_id: str | None = None,
    contribution_id: str | None = None,
) -> BoundedDiagnostic:
    """Snapshot safe failure facts without retaining message or traceback."""

    return BoundedDiagnostic(
        code=_bounded_label(code, fallback="extension_failure", limit=64),
        operation=_bounded_label(operation, fallback="extension_operation", limit=64),
        error_class=_bounded_label(type(error).__name__, fallback="Exception", limit=128),
        correlation_id=uuid4().hex,
        capability_id=(None if capability_id is None else _bounded_label(capability_id, fallback="invalid_capability")),
        contribution_id=(None if contribution_id is None else _bounded_label(contribution_id, fallback="invalid_contribution")),
    )


def log_bounded_failure(
    target: logging.Logger,
    diagnostic: BoundedDiagnostic,
    *,
    level: int = logging.WARNING,
) -> None:
    """Emit one structured record with no exception text or traceback."""

    target.log(
        level,
        "extension operation failed code=%s operation=%s error_class=%s capability_id=%s contribution_id=%s correlation_id=%s",
        diagnostic.code,
        diagnostic.operation,
        diagnostic.error_class,
        diagnostic.capability_id,
        diagnostic.contribution_id,
        diagnostic.correlation_id,
        extra={
            "diagnostic_code": diagnostic.code,
            "extension_operation": diagnostic.operation,
            "exception_class": diagnostic.error_class,
            "capability_id": diagnostic.capability_id,
            "contribution_id": diagnostic.contribution_id,
            "correlation_id": diagnostic.correlation_id,
        },
    )


def require_async_authoritative_operation(provider: object, operation: str) -> None:
    """Require an async contract, including transparent ``@wraps`` decorators.

    A synchronous wrapper is accepted only when ``inspect.unwrap`` proves that
    it transparently wraps a coroutine function.  A merely callable method
    returning an arbitrary value is never accepted as an async authority.
    """

    candidate = getattr(provider, operation, None)
    if not callable(candidate):
        raise TypeError("authoritative_operation_missing")
    try:
        unwrapped = inspect.unwrap(candidate)
    except (ValueError, TypeError):
        raise TypeError("authoritative_operation_not_async") from None
    if not inspect.iscoroutinefunction(unwrapped):
        raise TypeError("authoritative_operation_not_async")


__all__ = [
    "BoundedDiagnostic",
    "bounded_diagnostic",
    "log_bounded_failure",
    "require_async_authoritative_operation",
]
