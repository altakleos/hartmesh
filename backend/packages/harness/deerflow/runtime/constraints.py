"""Run-scoped invocation-constraint evidence and exact reservations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Any

from deerflow_extension_api import ConstraintProjectionV1

from deerflow.runtime.accepted_invocation import AcceptedInvocation, canonical_digest

INVOCATION_CONSTRAINTS_CONTEXT_KEY = "__deerflow_invocation_constraints_v1"
SUBAGENT_RESERVATION_CONTEXT_KEY = "__deerflow_subagent_reservation_v1"
_MAX_FUTURE_SKEW = timedelta(seconds=30)
_PROJECTION_KEYS = frozenset(
    {
        "version",
        "request_digest",
        "agent_revision_digest",
        "projection_revision",
        "issued_at",
        "valid_until",
        "evidence_id",
        "evidence_digest",
        "max_total_subagents",
        "projection_digest",
    }
)


class ConstraintFenceError(RuntimeError):
    """Accepted constraint evidence cannot safely cross a worker fence."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _aware_now(clock: Callable[[], datetime] | None) -> datetime:
    now = clock() if clock is not None else datetime.now(UTC)
    if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ConstraintFenceError("constraint_evidence_mismatch")
    return now


def validate_constraint_fence(
    accepted: AcceptedInvocation,
    *,
    request_digest: str | None,
    clock: Callable[[], datetime] | None,
) -> ConstraintProjectionV1 | None:
    """Validate persisted binding/evidence and current freshness."""

    raw = accepted.decision_evidence.get("constraints")
    if raw is None:
        return None
    try:
        if not isinstance(raw, Mapping) or set(raw) != _PROJECTION_KEYS:
            raise ValueError("invalid constraint evidence shape")
        payload: dict[str, Any] = {key: raw[key] for key in _PROJECTION_KEYS if key != "projection_digest"}
        if raw["projection_digest"] != canonical_digest(payload):
            raise ValueError("constraint projection digest mismatch")
        if payload["version"] != 1:
            raise ValueError("unsupported constraint projection version")
        projection = ConstraintProjectionV1(
            request_digest=payload["request_digest"],
            agent_revision_digest=payload["agent_revision_digest"],
            projection_revision=payload["projection_revision"],
            issued_at=datetime.fromisoformat(payload["issued_at"]),
            valid_until=datetime.fromisoformat(payload["valid_until"]),
            evidence_id=payload["evidence_id"],
            evidence_digest=payload["evidence_digest"],
            max_total_subagents=payload["max_total_subagents"],
        )
        if projection.agent_revision_digest != accepted.agent_revision.digest:
            raise ValueError("constraint agent revision binding mismatch")
        if request_digest is not None and projection.request_digest != request_digest:
            raise ValueError("constraint request binding mismatch")
        now = _aware_now(clock)
        if projection.issued_at > now + _MAX_FUTURE_SKEW:
            raise ValueError("constraint issued_at exceeds future skew")
    except ConstraintFenceError:
        raise
    except Exception as exc:
        raise ConstraintFenceError("constraint_evidence_mismatch") from exc
    if projection.valid_until <= now:
        raise ConstraintFenceError("constraint_expired_before_start")
    return projection


class InvocationSubagentReservation:
    """One concurrency-safe, retry-idempotent reservation counter per run."""

    __slots__ = ("_limit", "_lock", "_reserved_ids")

    def __init__(self, limit: int) -> None:
        if type(limit) is not int or limit <= 0:
            raise ValueError("subagent reservation limit must be a positive integer")
        self._limit = limit
        self._lock = Lock()
        self._reserved_ids: set[str] = set()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def reserved(self) -> int:
        with self._lock:
            return len(self._reserved_ids)

    def reserve(self, dispatch_id: str) -> bool:
        if not isinstance(dispatch_id, str) or not dispatch_id:
            raise ValueError("subagent dispatch_id must be a non-empty string")
        with self._lock:
            if dispatch_id in self._reserved_ids:
                return True
            if len(self._reserved_ids) >= self._limit:
                return False
            self._reserved_ids.add(dispatch_id)
            return True


__all__ = [
    "ConstraintFenceError",
    "INVOCATION_CONSTRAINTS_CONTEXT_KEY",
    "SUBAGENT_RESERVATION_CONTEXT_KEY",
    "InvocationSubagentReservation",
    "validate_constraint_fence",
]
