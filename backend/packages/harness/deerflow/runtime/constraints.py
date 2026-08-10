"""Run-scoped invocation-constraint evidence and exact reservations."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable, Mapping
from concurrent.futures import Future
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
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


class SubagentDispatchOutcome(StrEnum):
    """Finite arbitration outcomes for one invocation-scoped dispatch ID."""

    new = "new"
    replay = "replay"
    conflict = "conflict"
    exhausted = "exhausted"


@dataclass(frozen=True)
class SubagentDispatchTicket:
    """Opaque handle returned by one dispatch-ledger arbitration."""

    dispatch_id: str
    intent_digest: str
    outcome: SubagentDispatchOutcome
    _future: Future[Any] | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class _DispatchFailed:
    error_class: str


@dataclass(frozen=True)
class _DispatchCancelled:
    pass


class InvocationSubagentDispatchLedger:
    """Concurrency-safe physical-start ledger shared by a run and its children.

    The ledger retains only the canonical intent digest and a bounded terminal
    tool result. It never retains the mutable prompt/options used to compute the
    digest. Identical in-flight and completed retries share one physical start.
    """

    __slots__ = ("_closed", "_entries", "_limit", "_lock")

    def __init__(self, limit: int) -> None:
        if type(limit) is not int or limit < 0:
            raise ValueError("subagent dispatch limit must be a non-negative integer")
        self._limit = limit
        self._lock = Lock()
        self._entries: dict[str, tuple[str, Future[Any]]] = {}
        self._closed = False

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def reserved(self) -> int:
        with self._lock:
            return len(self._entries)

    @staticmethod
    def _validate_identity(dispatch_id: str, intent_digest: str) -> None:
        if not isinstance(dispatch_id, str) or not dispatch_id or len(dispatch_id.encode("utf-8")) > 256 or any(ord(character) < 32 or ord(character) == 127 for character in dispatch_id):
            raise ValueError("subagent dispatch_id must be a bounded non-empty string")
        if not isinstance(intent_digest, str) or len(intent_digest) != 64 or any(character not in "0123456789abcdef" for character in intent_digest):
            raise ValueError("subagent intent_digest must be a lowercase SHA-256 digest")

    def acquire(
        self,
        dispatch_id: str,
        intent_digest: str,
    ) -> SubagentDispatchTicket:
        """Arbitrate one dispatch without starting execution."""

        self._validate_identity(dispatch_id, intent_digest)
        with self._lock:
            if self._closed:
                return SubagentDispatchTicket(
                    dispatch_id,
                    intent_digest,
                    SubagentDispatchOutcome.exhausted,
                )
            existing = self._entries.get(dispatch_id)
            if existing is not None:
                existing_digest, future = existing
                return SubagentDispatchTicket(
                    dispatch_id,
                    intent_digest,
                    (SubagentDispatchOutcome.replay if existing_digest == intent_digest else SubagentDispatchOutcome.conflict),
                    future,
                )
            if len(self._entries) >= self._limit:
                return SubagentDispatchTicket(
                    dispatch_id,
                    intent_digest,
                    SubagentDispatchOutcome.exhausted,
                )
            future: Future[Any] = Future()
            self._entries[dispatch_id] = (intent_digest, future)
            return SubagentDispatchTicket(
                dispatch_id,
                intent_digest,
                SubagentDispatchOutcome.new,
                future,
            )

    def complete(self, ticket: SubagentDispatchTicket, result: Any) -> None:
        """Publish one immutable terminal result for equal replays."""

        future = self._owned_future(ticket)
        if not future.done():
            future.set_result(copy.deepcopy(result))

    def fail(self, ticket: SubagentDispatchTicket, error: BaseException) -> None:
        """Publish a bounded failure without retaining provider exception text."""

        future = self._owned_future(ticket)
        if not future.done():
            future.set_result(_DispatchFailed(type(error).__name__))

    def cancel(self, ticket: SubagentDispatchTicket) -> None:
        """Publish invocation cancellation to any identical waiter."""

        future = self._owned_future(ticket)
        if not future.done():
            future.set_result(_DispatchCancelled())

    def _owned_future(self, ticket: SubagentDispatchTicket) -> Future[Any]:
        if ticket.outcome is not SubagentDispatchOutcome.new or ticket._future is None:
            raise ValueError("only the winning dispatch ticket may publish a result")
        with self._lock:
            existing = self._entries.get(ticket.dispatch_id)
            if existing != (ticket.intent_digest, ticket._future):
                raise ValueError("subagent dispatch ticket is no longer owned by this ledger")
        return ticket._future

    async def replay_result(self, ticket: SubagentDispatchTicket) -> Any:
        """Await and defensively copy the winning in-flight or terminal result."""

        if ticket.outcome is not SubagentDispatchOutcome.replay or ticket._future is None:
            raise ValueError("only an equal replay ticket has a reusable result")
        value = await asyncio.shield(asyncio.wrap_future(ticket._future))
        if isinstance(value, _DispatchCancelled):
            raise asyncio.CancelledError
        if isinstance(value, _DispatchFailed):
            raise RuntimeError(f"subagent dispatch failed ({value.error_class})") from None
        return copy.deepcopy(value)

    def close(self) -> None:
        """Idempotently release terminal results and cancel incomplete waiters."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            entries = tuple(self._entries.values())
            self._entries.clear()
        for _digest, future in entries:
            if not future.done():
                future.set_result(_DispatchCancelled())

    def reserve(self, dispatch_id: str) -> bool:
        """Compatibility adapter for pre-ledger internal callers.

        It reserves a stable legacy intent but cannot publish/reuse results;
        production task dispatch uses :meth:`acquire` directly.
        """

        ticket = self.acquire(
            dispatch_id,
            canonical_digest({"version": 1, "legacy_subagent_dispatch": dispatch_id}),
        )
        return ticket.outcome in {
            SubagentDispatchOutcome.new,
            SubagentDispatchOutcome.replay,
        }


# Backward source compatibility for internal integrations while the runtime
# context now supplies the deeper dispatch-ledger semantics.
InvocationSubagentReservation = InvocationSubagentDispatchLedger


__all__ = [
    "ConstraintFenceError",
    "INVOCATION_CONSTRAINTS_CONTEXT_KEY",
    "SUBAGENT_RESERVATION_CONTEXT_KEY",
    "InvocationSubagentDispatchLedger",
    "InvocationSubagentReservation",
    "SubagentDispatchOutcome",
    "SubagentDispatchTicket",
    "validate_constraint_fence",
]
