"""Bounded administrator operations for durable inbound receipts.

This module deliberately exposes no list or bulk-mutation surface. Operators
may inspect one exact dead-letter identity, view capped aggregate health, and
requeue or logically discard that exact row through a fenced compare-and-set.
"""

from __future__ import annotations

import hashlib
import inspect
import logging
import unicodedata
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

logger = logging.getLogger(__name__)

INBOUND_RECEIPT_STATES: tuple[str, ...] = (
    "received",
    "claimed",
    "admitted",
    "deferred",
    "completed",
    "dead_letter",
)
MAX_RECEIPT_STATE_COUNT = 1_000
_MAX_RECEIPT_COUNTER = 2_147_483_647
_OPERATOR_REF_DOMAIN = b"inbound-receipt-operator:v1\0"


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _wire_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def validate_inbound_receipt_id(value: str) -> None:
    """Reject any receipt identity that is not a canonical lowercase UUID."""

    if not isinstance(value, str):
        raise ValueError("receipt_id must be a canonical lowercase UUID")
    try:
        parsed = UUID(value)
    except (AttributeError, ValueError):
        raise ValueError("receipt_id must be a canonical lowercase UUID") from None
    if str(parsed) != value:
        raise ValueError("receipt_id must be a canonical lowercase UUID")


def _validate_sha256(value: str, *, field_name: str) -> None:
    if not isinstance(value, str) or len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")


def _validate_bounded_text(
    value: str,
    *,
    field_name: str,
    max_bytes: int,
    optional: bool = False,
) -> None:
    if value is None and optional:
        return
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    if len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{field_name} exceeds its UTF-8 byte limit")
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise ValueError(f"{field_name} must not contain control characters")


def _validate_non_negative_integer(
    value: int,
    *,
    field_name: str,
    max_value: int = _MAX_RECEIPT_COUNTER,
) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    if value > max_value:
        raise ValueError(f"{field_name} exceeds its integer bound")


def _canonical_utc_datetime(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        # SQLite does not retain timezone metadata. Hartmesh persistence timestamps
        # are UTC, so restore that declared storage convention at this boundary.
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _validate_correlation_id(value: str) -> None:
    if not isinstance(value, str):
        raise ValueError("correlation_id must be a canonical lowercase UUID")
    try:
        parsed = UUID(value)
    except (AttributeError, ValueError):
        raise ValueError("correlation_id must be a canonical lowercase UUID") from None
    if str(parsed) != value:
        raise ValueError("correlation_id must be a canonical lowercase UUID")


def inbound_receipt_operator_ref(user_id: UUID | str) -> str:
    """Return a stable pseudonymous audit reference for one validated user."""

    if isinstance(user_id, UUID):
        material = user_id.bytes
    elif isinstance(user_id, str):
        encoded = user_id.encode("utf-8")
        if not encoded or len(encoded) > 128 or any(ord(character) < 32 or ord(character) == 127 for character in user_id):
            raise ValueError("operator user_id must be bounded and control-free")
        material = b"host-id\0" + encoded
    else:
        raise TypeError("operator user_id must be a UUID or host identifier")
    return hashlib.sha256(_OPERATOR_REF_DOMAIN + material).hexdigest()


@dataclass(frozen=True, slots=True)
class InboundReceiptStateSummary:
    """One bounded state count and indexed due age when it is meaningful."""

    state: str
    count: int
    capped: bool
    oldest_due_age_seconds: int | None

    def __post_init__(self) -> None:
        if self.state not in INBOUND_RECEIPT_STATES:
            raise ValueError("state is not a supported inbound receipt state")
        _validate_non_negative_integer(
            self.count,
            field_name="count",
            max_value=MAX_RECEIPT_STATE_COUNT,
        )
        if not isinstance(self.capped, bool):
            raise ValueError("capped must be a boolean")
        if self.oldest_due_age_seconds is not None:
            _validate_non_negative_integer(
                self.oldest_due_age_seconds,
                field_name="oldest_due_age_seconds",
            )
            if self.state not in {"received", "deferred"}:
                raise ValueError("oldest due age is meaningful only for received or deferred receipts")

    def to_dict(self) -> dict[str, Any]:
        """Return a fresh JSON-safe projection of this state aggregate."""

        return {
            "state": self.state,
            "count": self.count,
            "capped": self.capped,
            "oldest_due_age_seconds": self.oldest_due_age_seconds,
        }


@dataclass(frozen=True, slots=True)
class InboundReceiptOperationsSummary:
    """Deterministic aggregate receipt health without row enumeration."""

    generated_at: datetime
    per_state_cap: int
    states: tuple[InboundReceiptStateSummary, ...]

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "generated_at",
            _canonical_utc_datetime(self.generated_at, field_name="generated_at"),
        )
        _validate_non_negative_integer(self.per_state_cap, field_name="per_state_cap")
        if not 1 <= self.per_state_cap <= MAX_RECEIPT_STATE_COUNT:
            raise ValueError(f"per_state_cap must be between 1 and {MAX_RECEIPT_STATE_COUNT}")
        if not isinstance(self.states, tuple) or not self.states or len(self.states) > len(INBOUND_RECEIPT_STATES):
            raise ValueError("states must be a non-empty bounded tuple")
        seen: set[str] = set()
        for item in self.states:
            if not isinstance(item, InboundReceiptStateSummary):
                raise TypeError("states must contain InboundReceiptStateSummary values")
            if item.state in seen:
                raise ValueError("states must not contain duplicate receipt states")
            if item.count > self.per_state_cap:
                raise ValueError("state count exceeds per_state_cap")
            if item.capped and item.count != self.per_state_cap:
                raise ValueError("a capped state count must equal per_state_cap")
            seen.add(item.state)

    def by_state(self, state: str) -> InboundReceiptStateSummary:
        """Return the aggregate for one supported state or raise ``KeyError``."""

        for item in self.states:
            if item.state == state:
                return item
        raise KeyError(state)

    def to_dict(self) -> dict[str, Any]:
        """Return a fresh JSON-safe aggregate projection."""

        return {
            "generated_at": _wire_datetime(self.generated_at),
            "per_state_cap": self.per_state_cap,
            "states": [item.to_dict() for item in self.states],
        }


@dataclass(frozen=True, slots=True)
class InboundDeadLetterInspection:
    """Safe exact-ID evidence for one unresolved dead-letter receipt."""

    receipt_id: str
    provider: str
    binding_kind: str
    thread_id: str
    payload_digest: str
    provider_event_digest: str | None
    fencing_token: int
    attempt_count: int
    failure_count: int
    outcome_code: str | None
    received_at: datetime
    updated_at: datetime
    completed_at: datetime | None
    state: str = "dead_letter"

    def __post_init__(self) -> None:
        validate_inbound_receipt_id(self.receipt_id)
        if self.state != "dead_letter":
            raise ValueError("dead-letter inspection state must be dead_letter")
        _validate_bounded_text(self.provider, field_name="provider", max_bytes=64)
        _validate_bounded_text(self.binding_kind, field_name="binding_kind", max_bytes=32)
        _validate_bounded_text(self.thread_id, field_name="thread_id", max_bytes=64)
        _validate_sha256(self.payload_digest, field_name="payload_digest")
        if self.provider_event_digest is not None:
            _validate_sha256(
                self.provider_event_digest,
                field_name="provider_event_digest",
            )
        for field_name in ("fencing_token", "attempt_count", "failure_count"):
            _validate_non_negative_integer(getattr(self, field_name), field_name=field_name)
        if self.outcome_code is not None:
            _validate_bounded_text(
                self.outcome_code,
                field_name="outcome_code",
                max_bytes=64,
                optional=True,
            )
        object.__setattr__(
            self,
            "received_at",
            _canonical_utc_datetime(self.received_at, field_name="received_at"),
        )
        object.__setattr__(
            self,
            "updated_at",
            _canonical_utc_datetime(self.updated_at, field_name="updated_at"),
        )
        if self.completed_at is not None:
            object.__setattr__(
                self,
                "completed_at",
                _canonical_utc_datetime(self.completed_at, field_name="completed_at"),
            )

    def to_dict(self) -> dict[str, Any]:
        """Return a fresh JSON-safe projection with no retained envelope."""

        return {
            "receipt_id": self.receipt_id,
            "state": self.state,
            "provider": self.provider,
            "binding_kind": self.binding_kind,
            "thread_id": self.thread_id,
            "payload_digest": self.payload_digest,
            "provider_event_digest": self.provider_event_digest,
            "fencing_token": self.fencing_token,
            "attempt_count": self.attempt_count,
            "failure_count": self.failure_count,
            "outcome_code": self.outcome_code,
            "received_at": _wire_datetime(self.received_at),
            "updated_at": _wire_datetime(self.updated_at),
            "completed_at": _wire_datetime(self.completed_at),
        }


@dataclass(frozen=True, slots=True)
class InboundDeadLetterRequeueRequest:
    """Exact optimistic fence required for one dead-letter requeue."""

    receipt_id: str
    expected_fencing_token: int
    expected_payload_digest: str
    expected_provider_event_digest: str | None

    def __post_init__(self) -> None:
        validate_inbound_receipt_id(self.receipt_id)
        _validate_non_negative_integer(
            self.expected_fencing_token,
            field_name="expected_fencing_token",
        )
        _validate_sha256(
            self.expected_payload_digest,
            field_name="expected_payload_digest",
        )
        if self.expected_provider_event_digest is not None:
            _validate_sha256(
                self.expected_provider_event_digest,
                field_name="expected_provider_event_digest",
            )


class InboundDeadLetterRequeueDisposition(StrEnum):
    """Outcome vocabulary for one exact dead-letter requeue attempt."""

    requeued = "requeued"
    not_requeued = "not_requeued"


@dataclass(frozen=True, slots=True)
class InboundDeadLetterRequeueResult:
    """Bounded outcome of one exact compare-and-set recovery request."""

    receipt_id: str
    disposition: InboundDeadLetterRequeueDisposition
    correlation_id: str
    fencing_token: int | None
    wakeup_published: bool

    def __post_init__(self) -> None:
        validate_inbound_receipt_id(self.receipt_id)
        if not isinstance(self.disposition, InboundDeadLetterRequeueDisposition):
            raise TypeError("disposition must be an InboundDeadLetterRequeueDisposition")
        _validate_correlation_id(self.correlation_id)
        if self.fencing_token is not None:
            _validate_non_negative_integer(self.fencing_token, field_name="fencing_token")
        if (self.disposition is InboundDeadLetterRequeueDisposition.requeued) != (self.fencing_token is not None):
            raise ValueError("requeue disposition and fencing token are inconsistent")
        if not isinstance(self.wakeup_published, bool):
            raise ValueError("wakeup_published must be a boolean")
        if self.disposition is InboundDeadLetterRequeueDisposition.not_requeued and self.wakeup_published:
            raise ValueError("a failed requeue cannot publish a wakeup")

    def to_dict(self) -> dict[str, Any]:
        """Return a fresh JSON-safe requeue result."""

        return {
            "receipt_id": self.receipt_id,
            "disposition": self.disposition.value,
            "correlation_id": self.correlation_id,
            "fencing_token": self.fencing_token,
            "wakeup_published": self.wakeup_published,
        }


@dataclass(frozen=True, slots=True)
class InboundDeadLetterDiscardRequest:
    """Exact optimistic fence required for one logical dead-letter discard."""

    receipt_id: str
    expected_fencing_token: int
    expected_payload_digest: str
    expected_provider_event_digest: str | None

    def __post_init__(self) -> None:
        validate_inbound_receipt_id(self.receipt_id)
        _validate_non_negative_integer(
            self.expected_fencing_token,
            field_name="expected_fencing_token",
        )
        _validate_sha256(
            self.expected_payload_digest,
            field_name="expected_payload_digest",
        )
        if self.expected_provider_event_digest is not None:
            _validate_sha256(
                self.expected_provider_event_digest,
                field_name="expected_provider_event_digest",
            )


class InboundDeadLetterDiscardDisposition(StrEnum):
    """Outcome vocabulary for one exact logical-discard attempt."""

    discarded = "discarded"
    not_discarded = "not_discarded"


@dataclass(frozen=True, slots=True)
class InboundDeadLetterDiscardResult:
    """Bounded outcome of one exact logical-discard request."""

    receipt_id: str
    disposition: InboundDeadLetterDiscardDisposition
    correlation_id: str
    fencing_token: int | None

    def __post_init__(self) -> None:
        validate_inbound_receipt_id(self.receipt_id)
        if not isinstance(self.disposition, InboundDeadLetterDiscardDisposition):
            raise TypeError("disposition must be an InboundDeadLetterDiscardDisposition")
        _validate_correlation_id(self.correlation_id)
        if self.fencing_token is not None:
            _validate_non_negative_integer(self.fencing_token, field_name="fencing_token")
        if (self.disposition is InboundDeadLetterDiscardDisposition.discarded) != (self.fencing_token is not None):
            raise ValueError("discard disposition and fencing token are inconsistent")

    def to_dict(self) -> dict[str, Any]:
        """Return a fresh JSON-safe discard result."""

        return {
            "receipt_id": self.receipt_id,
            "disposition": self.disposition.value,
            "correlation_id": self.correlation_id,
            "fencing_token": self.fencing_token,
        }


class InboundReceiptOperationsStore(Protocol):
    """Persistence seam used only by the bounded operator surface."""

    async def summarize_states(
        self,
        *,
        per_state_cap: int,
        observed_at: datetime,
    ) -> InboundReceiptOperationsSummary: ...

    async def inspect_dead_letter(
        self,
        receipt_id: str,
    ) -> InboundDeadLetterInspection | None: ...

    async def requeue_dead_letter(
        self,
        request: InboundDeadLetterRequeueRequest,
        *,
        requeued_at: datetime,
    ) -> int | None: ...

    async def discard_dead_letter(
        self,
        request: InboundDeadLetterDiscardRequest,
        *,
        discarded_at: datetime,
    ) -> int | None: ...


class InboundReceiptOperations:
    """Host-owned exact-ID receipt inspection and recovery operations."""

    def __init__(
        self,
        *,
        store: InboundReceiptOperationsStore,
        publish_wakeup: Callable[[str], Any],
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._store = store
        self._publish_wakeup = publish_wakeup
        self._clock = clock

    async def summary(
        self,
        *,
        per_state_cap: int = MAX_RECEIPT_STATE_COUNT,
    ) -> InboundReceiptOperationsSummary:
        """Return capped counts with bounded row materialization and response size."""

        if not isinstance(per_state_cap, int) or isinstance(per_state_cap, bool) or not 1 <= per_state_cap <= MAX_RECEIPT_STATE_COUNT:
            raise ValueError(f"per_state_cap must be between 1 and {MAX_RECEIPT_STATE_COUNT}")
        return await self._store.summarize_states(
            per_state_cap=per_state_cap,
            observed_at=self._clock(),
        )

    async def inspect_dead_letter(
        self,
        receipt_id: str,
    ) -> InboundDeadLetterInspection | None:
        """Return one safe dead-letter projection, never an envelope or list."""

        validate_inbound_receipt_id(receipt_id)
        return await self._store.inspect_dead_letter(receipt_id)

    async def requeue_dead_letter(
        self,
        request: InboundDeadLetterRequeueRequest,
        *,
        actor_ref: str,
    ) -> InboundDeadLetterRequeueResult:
        """Atomically requeue one still-matching unresolved dead letter."""

        if not isinstance(request, InboundDeadLetterRequeueRequest):
            raise TypeError("requeue requires an InboundDeadLetterRequeueRequest")
        _validate_sha256(actor_ref, field_name="actor_ref")
        correlation_id = str(uuid.uuid4())
        fencing_token = await self._store.requeue_dead_letter(
            request,
            requeued_at=self._clock(),
        )
        if fencing_token is None:
            logger.warning(
                "inbound receipt operator action code=receipt_operation_requeue outcome=not_requeued receipt_id=%s correlation_id=%s actor_ref=%s",
                request.receipt_id,
                correlation_id,
                actor_ref,
            )
            return InboundDeadLetterRequeueResult(
                receipt_id=request.receipt_id,
                disposition=InboundDeadLetterRequeueDisposition.not_requeued,
                correlation_id=correlation_id,
                fencing_token=None,
                wakeup_published=False,
            )

        wakeup_published = True
        try:
            result = self._publish_wakeup(request.receipt_id)
            if inspect.isawaitable(result):
                await result
        except Exception as exc:
            wakeup_published = False
            logger.warning(
                "inbound receipt wakeup failed code=receipt_operation_wakeup_failed receipt_id=%s correlation_id=%s actor_ref=%s exception_class=%s",
                request.receipt_id,
                correlation_id,
                actor_ref,
                exc.__class__.__name__,
            )
        logger.info(
            "inbound receipt operator action code=receipt_operation_requeue outcome=requeued receipt_id=%s correlation_id=%s actor_ref=%s fencing_token=%s wakeup_published=%s",
            request.receipt_id,
            correlation_id,
            actor_ref,
            fencing_token,
            wakeup_published,
        )
        return InboundDeadLetterRequeueResult(
            receipt_id=request.receipt_id,
            disposition=InboundDeadLetterRequeueDisposition.requeued,
            correlation_id=correlation_id,
            fencing_token=fencing_token,
            wakeup_published=wakeup_published,
        )

    async def discard_dead_letter(
        self,
        request: InboundDeadLetterDiscardRequest,
        *,
        actor_ref: str,
    ) -> InboundDeadLetterDiscardResult:
        """Logically discard one still-matching unresolved dead letter."""

        if not isinstance(request, InboundDeadLetterDiscardRequest):
            raise TypeError("discard requires an InboundDeadLetterDiscardRequest")
        _validate_sha256(actor_ref, field_name="actor_ref")
        correlation_id = str(uuid.uuid4())
        fencing_token = await self._store.discard_dead_letter(
            request,
            discarded_at=self._clock(),
        )
        if fencing_token is None:
            logger.warning(
                "inbound receipt operator action code=receipt_operation_discard outcome=not_discarded receipt_id=%s correlation_id=%s actor_ref=%s",
                request.receipt_id,
                correlation_id,
                actor_ref,
            )
            return InboundDeadLetterDiscardResult(
                receipt_id=request.receipt_id,
                disposition=InboundDeadLetterDiscardDisposition.not_discarded,
                correlation_id=correlation_id,
                fencing_token=None,
            )
        logger.info(
            "inbound receipt operator action code=receipt_operation_discard outcome=discarded receipt_id=%s correlation_id=%s actor_ref=%s fencing_token=%s",
            request.receipt_id,
            correlation_id,
            actor_ref,
            fencing_token,
        )
        return InboundDeadLetterDiscardResult(
            receipt_id=request.receipt_id,
            disposition=InboundDeadLetterDiscardDisposition.discarded,
            correlation_id=correlation_id,
            fencing_token=fencing_token,
        )


__all__ = [
    "INBOUND_RECEIPT_STATES",
    "MAX_RECEIPT_STATE_COUNT",
    "InboundReceiptOperations",
    "InboundReceiptOperationsStore",
    "InboundReceiptOperationsSummary",
    "InboundReceiptStateSummary",
    "InboundDeadLetterInspection",
    "InboundDeadLetterDiscardDisposition",
    "InboundDeadLetterDiscardRequest",
    "InboundDeadLetterDiscardResult",
    "InboundDeadLetterRequeueDisposition",
    "InboundDeadLetterRequeueRequest",
    "InboundDeadLetterRequeueResult",
    "inbound_receipt_operator_ref",
    "validate_inbound_receipt_id",
]
