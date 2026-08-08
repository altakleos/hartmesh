"""Run-scoped invocation-constraint evidence and exact reservations."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from threading import Lock
from typing import Any

from deerflow_extension_api import (
    INVOCATION_CONSTRAINTS_V2_SUPPORTED_OBLIGATIONS,
    ConstraintProjectionV1,
    ConstraintProjectionV2,
)

from deerflow.runtime.accepted_invocation import AcceptedInvocation, canonical_digest

INVOCATION_CONSTRAINTS_CONTEXT_KEY = "__deerflow_invocation_constraints"
SUBAGENT_RESERVATION_CONTEXT_KEY = "__deerflow_subagent_reservation_v1"
_MAX_FUTURE_SKEW = timedelta(seconds=30)
_PROJECTION_V1_KEYS = frozenset(
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
_PROJECTION_V2_KEYS = frozenset(
    {
        "version",
        "request_digest",
        "trusted_context_digest",
        "thread_id",
        "agent_revision_digest",
        "profile_revision_digest",
        "extension_manifest_digest",
        "extension_generation",
        "projection_revision",
        "issued_at",
        "valid_until",
        "evidence_id",
        "evidence_digest",
        "mandatory_obligations",
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
) -> ConstraintProjectionV1 | ConstraintProjectionV2 | None:
    """Validate persisted binding/evidence and current freshness."""

    raw = accepted.decision_evidence.get("constraints")
    if raw is None:
        return None
    try:
        if not isinstance(raw, Mapping):
            raise ValueError("invalid constraint evidence shape")
        version = raw.get("version")
        projection_keys = _PROJECTION_V1_KEYS if version == 1 else _PROJECTION_V2_KEYS if version == 2 else frozenset()
        if not projection_keys or set(raw) != projection_keys:
            raise ValueError("invalid constraint evidence shape")
        payload: dict[str, Any] = {key: raw[key] for key in projection_keys if key != "projection_digest"}
        if raw["projection_digest"] != canonical_digest(payload):
            raise ValueError("constraint projection digest mismatch")
        if version == 1:
            projection: ConstraintProjectionV1 | ConstraintProjectionV2 = ConstraintProjectionV1(
                request_digest=payload["request_digest"],
                agent_revision_digest=payload["agent_revision_digest"],
                projection_revision=payload["projection_revision"],
                issued_at=datetime.fromisoformat(payload["issued_at"]),
                valid_until=datetime.fromisoformat(payload["valid_until"]),
                evidence_id=payload["evidence_id"],
                evidence_digest=payload["evidence_digest"],
                max_total_subagents=payload["max_total_subagents"],
            )
        else:
            projection = ConstraintProjectionV2(
                request_digest=payload["request_digest"],
                trusted_context_digest=payload["trusted_context_digest"],
                thread_id=payload["thread_id"],
                agent_revision_digest=payload["agent_revision_digest"],
                profile_revision_digest=payload["profile_revision_digest"],
                extension_manifest_digest=payload["extension_manifest_digest"],
                extension_generation=payload["extension_generation"],
                projection_revision=payload["projection_revision"],
                issued_at=datetime.fromisoformat(payload["issued_at"]),
                valid_until=datetime.fromisoformat(payload["valid_until"]),
                evidence_id=payload["evidence_id"],
                evidence_digest=payload["evidence_digest"],
                mandatory_obligations=payload["mandatory_obligations"],
                max_total_subagents=payload["max_total_subagents"],
            )
        if projection.agent_revision_digest != accepted.agent_revision.digest:
            raise ValueError("constraint agent revision binding mismatch")
        if request_digest is not None and projection.request_digest != request_digest:
            raise ValueError("constraint request binding mismatch")
        if isinstance(projection, ConstraintProjectionV2):
            trusted_context = accepted.trusted_context
            if trusted_context is None:
                raise ValueError("constraint v2 requires accepted trusted context")
            bindings = (
                (projection.trusted_context_digest, trusted_context.digest),
                (projection.thread_id, accepted.thread_id),
                (projection.profile_revision_digest, trusted_context.profile_revision.digest),
                (projection.extension_manifest_digest, accepted.extension_manifest_digest),
                (projection.extension_generation, accepted.extension_generation),
            )
            if any(actual != expected for actual, expected in bindings):
                raise ValueError("constraint v2 execution binding mismatch")
            if not set(projection.mandatory_obligations).issubset(INVOCATION_CONSTRAINTS_V2_SUPPORTED_OBLIGATIONS):
                raise ValueError("constraint v2 contains an unsupported mandatory obligation")
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
        if type(limit) is not int or limit < 0:
            raise ValueError("subagent reservation limit must be a non-negative integer")
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
