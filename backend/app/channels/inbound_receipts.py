"""Bounded, leased receipts for keyed native-channel ingress.

The receipt is the durable pre-admission copy.  The MessageBus carries only a
wake-up; a claimant reconstructs the launch from this host-validated envelope.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import unicodedata
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import Any, Protocol

from sqlalchemy import and_, case, exists, or_, select, update
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import aliased

from app.channels.inbound_receipt_operations import (
    INBOUND_RECEIPT_STATES,
    InboundDeadLetterDiscardRequest,
    InboundDeadLetterInspection,
    InboundDeadLetterRequeueRequest,
    InboundReceiptOperationsSummary,
    InboundReceiptStateSummary,
    validate_inbound_receipt_id,
)
from app.channels.message_bus import InboundMessage, InboundMessageType
from app.runtime.native_binding import (
    InternalVerifiedNativeBinding,
    InternalVerifiedNativeBindingKind,
)
from deerflow.persistence.inbound_receipt.model import InboundReceiptRow

logger = logging.getLogger(__name__)

_MAX_TEXT_BYTES = 64 * 1024
_MAX_ENVELOPE_BYTES = 96 * 1024
_MAX_REFERENCE_BYTES = 512
_MAX_POLICY_METADATA_BYTES = 8 * 1024
_SOURCE_IDENTITY_COLUMNS = (
    "provider",
    "binding_kind",
    "binding_reference",
    "provider_delivery_id",
)
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _bounded_text(
    value: Any,
    *,
    field_name: str,
    max_bytes: int = _MAX_REFERENCE_BYTES,
    optional: bool = False,
) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be a non-empty string")
    if len(value.encode("utf-8")) > max_bytes:
        raise ValueError(f"{field_name} exceeds its UTF-8 byte limit")
    if any(unicodedata.category(character) == "Cc" for character in value):
        raise ValueError(f"{field_name} must not contain control characters")
    return value


def _freeze_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        frozen: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("receipt mappings require string keys")
            frozen[key] = _freeze_json(item)
        return MappingProxyType(frozen)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_json(item) for item in value)
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise ValueError(f"receipt data cannot retain {type(value).__name__}")


def _thaw_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _thaw_json(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_json(item) for item in value]
    return value


def _canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        _thaw_json(value),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def _policy_metadata(message: InboundMessage, provider_delivery_id: str) -> Mapping[str, Any]:
    """Project only host-selected fields needed to rebuild channel policy."""

    source = message.metadata if isinstance(message.metadata, Mapping) else {}
    projected: dict[str, Any] = {"message_id": source.get("message_id") or provider_delivery_id}
    for key in ("agent_name", "preferred_thread_id"):
        if key in source:
            projected[key] = source[key]
    channel = source.get(message.channel_name)
    if isinstance(channel, Mapping):
        allowed = {
            key: channel[key]
            for key in (
                "repo",
                "number",
                "event",
                "delivery_id",
                "installation_id",
                "recursion_limit",
                "thread_id",
            )
            if key in channel
        }
        if allowed:
            projected[message.channel_name] = allowed
    frozen = _freeze_json(projected)
    if len(_canonical_json(frozen)) > _MAX_POLICY_METADATA_BYTES:
        raise ValueError("receipt policy metadata exceeds its byte limit")
    return frozen


@dataclass(frozen=True, slots=True)
class InboundReceiptEnvelope:
    """Immutable replay input derived only from verified host source facts."""

    provider: str
    binding: InternalVerifiedNativeBinding
    provider_delivery_id: str
    thread_id: str
    chat_id: str
    sender_id: str
    text: str
    message_type: InboundMessageType
    owner_user_id: str
    workspace_id: str | None
    topic_id: str | None
    thread_ts: str | None
    policy_metadata: Mapping[str, Any]
    created_at: float

    def __post_init__(self) -> None:
        for field_name in (
            "provider",
            "provider_delivery_id",
            "thread_id",
            "chat_id",
            "sender_id",
            "owner_user_id",
        ):
            if field_name in {"provider", "thread_id"}:
                limit = 64
            elif field_name == "provider_delivery_id":
                limit = 320
            else:
                limit = _MAX_REFERENCE_BYTES
            object.__setattr__(
                self,
                field_name,
                _bounded_text(getattr(self, field_name), field_name=field_name, max_bytes=limit),
            )
        for field_name in ("workspace_id", "topic_id", "thread_ts"):
            object.__setattr__(
                self,
                field_name,
                _bounded_text(
                    getattr(self, field_name),
                    field_name=field_name,
                    optional=True,
                ),
            )
        if not isinstance(self.binding, InternalVerifiedNativeBinding):
            raise TypeError("receipt binding must be host verified")
        try:
            message_type = InboundMessageType(self.message_type)
        except (TypeError, ValueError) as exc:
            raise ValueError("unsupported receipt message type") from exc
        object.__setattr__(self, "message_type", message_type)
        if not isinstance(self.text, str) or len(self.text.encode("utf-8")) > _MAX_TEXT_BYTES:
            raise ValueError("receipt text exceeds its UTF-8 byte limit")
        if not isinstance(self.created_at, (int, float)) or isinstance(self.created_at, bool):
            raise ValueError("receipt created_at must be a timestamp")
        if not isinstance(self.policy_metadata, Mapping):
            raise TypeError("receipt policy_metadata must be a mapping")
        object.__setattr__(self, "policy_metadata", _freeze_json(self.policy_metadata))
        if len(_canonical_json(self.to_dict())) > _MAX_ENVELOPE_BYTES:
            raise ValueError("receipt envelope exceeds its byte limit")

    @classmethod
    def from_message(cls, message: InboundMessage) -> InboundReceiptEnvelope:
        """Snapshot a keyed verified message without raw payloads or credentials."""

        if message.verified_source_binding is None:
            raise ValueError("durable receipt requires a verified source binding")
        if message.files:
            raise ValueError("durable receipt does not accept transient attachment data")
        source = message.metadata if isinstance(message.metadata, Mapping) else {}
        channel = source.get(message.channel_name)
        nested_delivery_id = channel.get("delivery_id") if isinstance(channel, Mapping) else None
        provider_delivery_id = nested_delivery_id or source.get("message_id")
        thread_id = source.get("preferred_thread_id")
        return cls(
            provider=message.channel_name,
            binding=message.verified_source_binding,
            provider_delivery_id=provider_delivery_id,
            thread_id=thread_id,
            chat_id=message.chat_id,
            sender_id=message.user_id,
            text=message.text,
            message_type=message.msg_type,
            owner_user_id=message.owner_user_id,
            workspace_id=message.workspace_id,
            topic_id=message.topic_id,
            thread_ts=message.thread_ts,
            policy_metadata=_policy_metadata(message, provider_delivery_id),
            # The carrier's local enqueue timestamp is not provider identity and
            # changes on redelivery. ReceiptRow.received_at owns durable arrival
            # ordering; use a stable neutral value in replay input.
            created_at=0.0,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": 1,
            "provider": self.provider,
            "binding": {
                "kind": self.binding.kind.value,
                "reference": self.binding.reference,
            },
            "provider_delivery_id": self.provider_delivery_id,
            "thread_id": self.thread_id,
            "chat_id": self.chat_id,
            "sender_id": self.sender_id,
            "text": self.text,
            "message_type": self.message_type.value,
            "owner_user_id": self.owner_user_id,
            "workspace_id": self.workspace_id,
            "topic_id": self.topic_id,
            "thread_ts": self.thread_ts,
            "policy_metadata": _thaw_json(self.policy_metadata),
            "created_at": self.created_at,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, Any]) -> InboundReceiptEnvelope:
        expected = {
            "version",
            "provider",
            "binding",
            "provider_delivery_id",
            "thread_id",
            "chat_id",
            "sender_id",
            "text",
            "message_type",
            "owner_user_id",
            "workspace_id",
            "topic_id",
            "thread_ts",
            "policy_metadata",
            "created_at",
        }
        if not isinstance(value, Mapping) or set(value) != expected or value.get("version") != 1:
            raise ValueError("invalid inbound receipt envelope")
        binding = value["binding"]
        if not isinstance(binding, Mapping) or set(binding) != {"kind", "reference"}:
            raise ValueError("invalid receipt binding")
        return cls(
            provider=value["provider"],
            binding=InternalVerifiedNativeBinding(
                kind=InternalVerifiedNativeBindingKind(binding["kind"]),
                reference=binding["reference"],
            ),
            provider_delivery_id=value["provider_delivery_id"],
            thread_id=value["thread_id"],
            chat_id=value["chat_id"],
            sender_id=value["sender_id"],
            text=value["text"],
            message_type=InboundMessageType(value["message_type"]),
            owner_user_id=value["owner_user_id"],
            workspace_id=value["workspace_id"],
            topic_id=value["topic_id"],
            thread_ts=value["thread_ts"],
            policy_metadata=value["policy_metadata"],
            created_at=value["created_at"],
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256(_canonical_json(self.to_dict())).hexdigest()

    def to_message(self) -> InboundMessage:
        """Return a fresh carrier reconstructed from accepted receipt facts."""

        return InboundMessage(
            channel_name=self.provider,
            chat_id=self.chat_id,
            user_id=self.sender_id,
            text=self.text,
            msg_type=self.message_type,
            thread_ts=self.thread_ts,
            topic_id=self.topic_id,
            owner_user_id=self.owner_user_id,
            workspace_id=self.workspace_id,
            verified_source_binding=self.binding,
            files=[],
            metadata=_thaw_json(self.policy_metadata),
            created_at=self.created_at,
        )


@dataclass(frozen=True, slots=True)
class InboundReceiptCandidate:
    """One authenticated provider event paired with proposed execution input.

    The provider-event digest binds the exact request body authenticated by the
    ingress route plus its bounded routing-event value. The envelope is the
    mutable-configuration-dependent launch projection accepted only on the
    first receipt for that source identity.
    """

    envelope: InboundReceiptEnvelope
    provider_event_digest: str

    def __post_init__(self) -> None:
        if not isinstance(self.envelope, InboundReceiptEnvelope):
            raise TypeError("receipt candidate requires an immutable envelope")
        if not isinstance(self.provider_event_digest, str) or _SHA256_PATTERN.fullmatch(self.provider_event_digest) is None:
            raise ValueError("provider event digest must be lowercase SHA-256")

    @classmethod
    def from_message(cls, message: InboundMessage) -> InboundReceiptCandidate:
        """Snapshot one message after its provider event was authenticated."""

        digest = message.verified_provider_event_digest
        if digest is None:
            raise ValueError("durable receipt requires authenticated provider event evidence")
        return cls(
            envelope=InboundReceiptEnvelope.from_message(message),
            provider_event_digest=digest,
        )


class InboundReceiptReplayConflict(ValueError):
    """The same verified source identity carried a different provider event."""


class InboundReceiptState(StrEnum):
    received = "received"
    claimed = "claimed"
    admitted = "admitted"
    deferred = "deferred"
    completed = "completed"
    dead_letter = "dead_letter"


@dataclass(frozen=True, slots=True)
class InboundReceipt:
    """Immutable receipt state returned by the fenced store."""

    receipt_id: str
    envelope: InboundReceiptEnvelope
    state: InboundReceiptState
    fencing_token: int
    attempt_count: int
    failure_count: int
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    run_id: str | None = None
    outcome_code: str | None = None
    provider_event_digest: str | None = None


class InboundReceiptStore(Protocol):
    """Deep state-and-fencing contract for inbound receipt persistence."""

    durable: bool

    async def receive_batch(
        self,
        candidates: Sequence[InboundReceiptCandidate],
    ) -> tuple[InboundReceipt, ...]: ...

    async def list_due(self, *, limit: int) -> tuple[InboundReceipt, ...]: ...

    async def claim(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        lease_seconds: int,
    ) -> InboundReceipt | None: ...

    async def bind_admitted(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        fencing_token: int,
        run_id: str,
    ) -> bool: ...

    async def complete(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        fencing_token: int,
        outcome_code: str,
    ) -> bool: ...

    async def defer_contention(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        fencing_token: int,
        delay_seconds: int,
        outcome_code: str,
    ) -> bool: ...

    async def defer_failure(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        fencing_token: int,
        delay_seconds: int,
        outcome_code: str,
        max_failures: int,
    ) -> InboundReceiptState | None: ...

    async def dead_letter(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        fencing_token: int,
        outcome_code: str,
    ) -> bool: ...

    async def cleanup_completed(self, *, older_than: datetime, limit: int) -> int: ...


def _row_to_receipt(row: InboundReceiptRow) -> InboundReceipt:
    envelope = InboundReceiptEnvelope.from_dict(row.payload_json)
    if envelope.digest != row.payload_digest:
        raise ValueError("persisted inbound receipt envelope digest is inconsistent")
    provider_event_digest = row.provider_event_digest
    if provider_event_digest is not None and _SHA256_PATTERN.fullmatch(provider_event_digest) is None:
        raise ValueError("persisted provider event digest is malformed")
    return InboundReceipt(
        receipt_id=row.receipt_id,
        envelope=envelope,
        state=InboundReceiptState(row.state),
        fencing_token=row.fencing_token,
        attempt_count=row.attempt_count,
        failure_count=row.failure_count,
        lease_owner=row.lease_owner,
        lease_expires_at=row.lease_expires_at,
        run_id=row.run_id,
        outcome_code=row.outcome_code,
        provider_event_digest=provider_event_digest,
    )


class SqlInboundReceiptStore:
    """SQL-backed receipt arbitration for SQLite tests and PostgreSQL runtime."""

    durable = True

    def __init__(
        self,
        session_factory: Any,
        *,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._session_factory = session_factory
        self._clock = clock

    async def receive_batch(
        self,
        candidates: Sequence[InboundReceiptCandidate],
    ) -> tuple[InboundReceipt, ...]:
        """Atomically retain every fan-out row or replay its first envelope."""

        if not candidates:
            return ()
        identities = [
            (
                candidate.envelope.provider,
                candidate.envelope.binding.kind.value,
                candidate.envelope.binding.reference,
                candidate.envelope.provider_delivery_id,
            )
            for candidate in candidates
        ]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate source delivery identity in receipt batch")
        now = self._clock()
        async with self._session_factory() as session:
            async with session.begin():
                dialect = session.bind.dialect.name
                insert = postgresql_insert if dialect == "postgresql" else sqlite_insert
                for position, candidate in enumerate(candidates):
                    envelope = candidate.envelope
                    received_at = now + timedelta(microseconds=position)
                    statement = (
                        insert(InboundReceiptRow)
                        .values(
                            receipt_id=str(uuid.uuid4()),
                            provider=envelope.provider,
                            binding_kind=envelope.binding.kind.value,
                            binding_reference=envelope.binding.reference,
                            provider_delivery_id=envelope.provider_delivery_id,
                            thread_id=envelope.thread_id,
                            payload_json=envelope.to_dict(),
                            payload_digest=envelope.digest,
                            provider_event_digest=candidate.provider_event_digest,
                            state=InboundReceiptState.received.value,
                            fencing_token=0,
                            attempt_count=0,
                            failure_count=0,
                            next_attempt_at=received_at,
                            received_at=received_at,
                            updated_at=now,
                        )
                        .on_conflict_do_nothing(index_elements=list(_SOURCE_IDENTITY_COLUMNS))
                    )
                    await session.execute(statement)
                receipts: list[InboundReceipt] = []
                for candidate in candidates:
                    envelope = candidate.envelope
                    row = await session.scalar(
                        select(InboundReceiptRow).where(
                            InboundReceiptRow.provider == envelope.provider,
                            InboundReceiptRow.binding_kind == envelope.binding.kind.value,
                            InboundReceiptRow.binding_reference == envelope.binding.reference,
                            InboundReceiptRow.provider_delivery_id == envelope.provider_delivery_id,
                        )
                    )
                    if row is None:
                        raise RuntimeError("inbound receipt insert did not retain a row")
                    if row.provider_event_digest != candidate.provider_event_digest:
                        raise InboundReceiptReplayConflict("source delivery identity has a conflicting authenticated event")
                    receipts.append(_row_to_receipt(row))
        return tuple(receipts)

    async def summarize_states(
        self,
        *,
        per_state_cap: int,
        observed_at: datetime,
    ) -> InboundReceiptOperationsSummary:
        """Return capped state counts and indexed due ages with bounded reads."""

        if not isinstance(per_state_cap, int) or isinstance(per_state_cap, bool) or not 1 <= per_state_cap <= 1_000:
            raise ValueError("per_state_cap must be between 1 and 1000")
        if observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise ValueError("observed_at must be timezone-aware")
        summaries: list[InboundReceiptStateSummary] = []
        async with self._session_factory() as session:
            for state in INBOUND_RECEIPT_STATES:
                receipt_ids = (
                    await session.scalars(
                        select(InboundReceiptRow.receipt_id)
                        .where(InboundReceiptRow.state == state)
                        .order_by(
                            InboundReceiptRow.next_attempt_at,
                            InboundReceiptRow.received_at,
                            InboundReceiptRow.receipt_id,
                        )
                        .limit(per_state_cap + 1)
                    )
                ).all()
                oldest_due_age: int | None = None
                if state in {
                    InboundReceiptState.received.value,
                    InboundReceiptState.deferred.value,
                }:
                    oldest_due = await session.scalar(
                        select(InboundReceiptRow.next_attempt_at)
                        .where(
                            InboundReceiptRow.state == state,
                            InboundReceiptRow.next_attempt_at <= observed_at,
                        )
                        .order_by(
                            InboundReceiptRow.next_attempt_at,
                            InboundReceiptRow.received_at,
                            InboundReceiptRow.receipt_id,
                        )
                        .limit(1)
                    )
                    if oldest_due is not None:
                        if oldest_due.tzinfo is None or oldest_due.utcoffset() is None:
                            oldest_due = oldest_due.replace(tzinfo=UTC)
                        oldest_due_age = max(
                            0,
                            int((observed_at - oldest_due).total_seconds()),
                        )
                summaries.append(
                    InboundReceiptStateSummary(
                        state=state,
                        count=min(len(receipt_ids), per_state_cap),
                        capped=len(receipt_ids) > per_state_cap,
                        oldest_due_age_seconds=oldest_due_age,
                    )
                )
        return InboundReceiptOperationsSummary(
            generated_at=observed_at,
            per_state_cap=per_state_cap,
            states=tuple(summaries),
        )

    async def inspect_dead_letter(
        self,
        receipt_id: str,
    ) -> InboundDeadLetterInspection | None:
        """Read one exact dead letter without loading its retained envelope."""

        validate_inbound_receipt_id(receipt_id)
        async with self._session_factory() as session:
            row = (
                await session.execute(
                    select(
                        InboundReceiptRow.receipt_id,
                        InboundReceiptRow.provider,
                        InboundReceiptRow.binding_kind,
                        InboundReceiptRow.thread_id,
                        InboundReceiptRow.payload_digest,
                        InboundReceiptRow.provider_event_digest,
                        InboundReceiptRow.fencing_token,
                        InboundReceiptRow.attempt_count,
                        InboundReceiptRow.failure_count,
                        InboundReceiptRow.outcome_code,
                        InboundReceiptRow.received_at,
                        InboundReceiptRow.updated_at,
                        InboundReceiptRow.completed_at,
                    ).where(
                        InboundReceiptRow.receipt_id == receipt_id,
                        InboundReceiptRow.state == InboundReceiptState.dead_letter.value,
                        InboundReceiptRow.run_id.is_(None),
                    )
                )
            ).one_or_none()
        if row is None:
            return None
        return InboundDeadLetterInspection(
            receipt_id=row.receipt_id,
            provider=row.provider,
            binding_kind=row.binding_kind,
            thread_id=row.thread_id,
            payload_digest=row.payload_digest,
            provider_event_digest=row.provider_event_digest,
            fencing_token=row.fencing_token,
            attempt_count=row.attempt_count,
            failure_count=row.failure_count,
            outcome_code=row.outcome_code,
            received_at=row.received_at,
            updated_at=row.updated_at,
            completed_at=row.completed_at,
        )

    async def requeue_dead_letter(
        self,
        request: InboundDeadLetterRequeueRequest,
        *,
        requeued_at: datetime,
    ) -> int | None:
        """CAS one unresolved dead letter back to pending receipt processing."""

        if not isinstance(request, InboundDeadLetterRequeueRequest):
            raise TypeError("requeue requires an InboundDeadLetterRequeueRequest")
        if requeued_at.tzinfo is None or requeued_at.utcoffset() is None:
            raise ValueError("requeued_at must be timezone-aware")
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(InboundReceiptRow)
                    .where(
                        InboundReceiptRow.receipt_id == request.receipt_id,
                        InboundReceiptRow.state == InboundReceiptState.dead_letter.value,
                        InboundReceiptRow.run_id.is_(None),
                        InboundReceiptRow.fencing_token == request.expected_fencing_token,
                        InboundReceiptRow.payload_digest == request.expected_payload_digest,
                        (InboundReceiptRow.provider_event_digest.is_(None) if request.expected_provider_event_digest is None else InboundReceiptRow.provider_event_digest == request.expected_provider_event_digest),
                    )
                    .values(
                        state=InboundReceiptState.deferred.value,
                        lease_owner=None,
                        lease_expires_at=None,
                        fencing_token=InboundReceiptRow.fencing_token + 1,
                        failure_count=0,
                        next_attempt_at=requeued_at,
                        outcome_code="operator_requeued",
                        updated_at=requeued_at,
                        completed_at=None,
                    )
                    .returning(InboundReceiptRow.fencing_token)
                )
                return result.scalar_one_or_none()

    async def discard_dead_letter(
        self,
        request: InboundDeadLetterDiscardRequest,
        *,
        discarded_at: datetime,
    ) -> int | None:
        """CAS one unresolved dead letter into ordinary completed retention."""

        if not isinstance(request, InboundDeadLetterDiscardRequest):
            raise TypeError("discard requires an InboundDeadLetterDiscardRequest")
        if discarded_at.tzinfo is None or discarded_at.utcoffset() is None:
            raise ValueError("discarded_at must be timezone-aware")
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(InboundReceiptRow)
                    .where(
                        InboundReceiptRow.receipt_id == request.receipt_id,
                        InboundReceiptRow.state == InboundReceiptState.dead_letter.value,
                        InboundReceiptRow.run_id.is_(None),
                        InboundReceiptRow.fencing_token == request.expected_fencing_token,
                        InboundReceiptRow.payload_digest == request.expected_payload_digest,
                        (InboundReceiptRow.provider_event_digest.is_(None) if request.expected_provider_event_digest is None else InboundReceiptRow.provider_event_digest == request.expected_provider_event_digest),
                    )
                    .values(
                        state=InboundReceiptState.completed.value,
                        lease_owner=None,
                        lease_expires_at=None,
                        fencing_token=InboundReceiptRow.fencing_token + 1,
                        # The existing schema keeps this column non-null for all
                        # states. Completed rows are excluded from due scans, so
                        # pinning it to completion time carries no retry meaning.
                        next_attempt_at=discarded_at,
                        outcome_code="operator_discarded",
                        updated_at=discarded_at,
                        completed_at=discarded_at,
                    )
                    .returning(InboundReceiptRow.fencing_token)
                )
                return result.scalar_one_or_none()

    async def list_due(self, *, limit: int) -> tuple[InboundReceipt, ...]:
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 500:
            raise ValueError("receipt page limit must be between 1 and 500")
        now = self._clock()
        earlier = aliased(InboundReceiptRow)
        unfinished = (
            InboundReceiptState.received.value,
            InboundReceiptState.claimed.value,
            InboundReceiptState.admitted.value,
            InboundReceiptState.deferred.value,
        )
        no_earlier_thread_receipt = ~exists(
            select(1)
            .select_from(earlier)
            .where(
                earlier.thread_id == InboundReceiptRow.thread_id,
                earlier.state.in_(unfinished),
                or_(
                    earlier.received_at < InboundReceiptRow.received_at,
                    and_(
                        earlier.received_at == InboundReceiptRow.received_at,
                        earlier.receipt_id < InboundReceiptRow.receipt_id,
                    ),
                ),
            )
        )
        async with self._session_factory() as session:
            rows = (
                await session.scalars(
                    select(InboundReceiptRow)
                    .where(
                        or_(
                            (
                                InboundReceiptRow.state.in_(
                                    (
                                        InboundReceiptState.received.value,
                                        InboundReceiptState.deferred.value,
                                    )
                                )
                                & (InboundReceiptRow.next_attempt_at <= now)
                            ),
                            (
                                InboundReceiptRow.state.in_(
                                    (
                                        InboundReceiptState.claimed.value,
                                        InboundReceiptState.admitted.value,
                                    )
                                )
                                & (InboundReceiptRow.lease_expires_at <= now)
                            ),
                        ),
                        no_earlier_thread_receipt,
                    )
                    .order_by(
                        InboundReceiptRow.received_at,
                        InboundReceiptRow.receipt_id,
                    )
                    .limit(limit)
                )
            ).all()
        return tuple(_row_to_receipt(row) for row in rows)

    async def claim(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        lease_seconds: int,
    ) -> InboundReceipt | None:
        _bounded_text(receipt_id, field_name="receipt_id", max_bytes=36)
        _bounded_text(lease_owner, field_name="lease_owner", max_bytes=96)
        if not isinstance(lease_seconds, int) or isinstance(lease_seconds, bool) or not 1 <= lease_seconds <= 300:
            raise ValueError("lease_seconds must be between 1 and 300")
        now = self._clock()
        eligible = or_(
            (
                InboundReceiptRow.state.in_(
                    (
                        InboundReceiptState.received.value,
                        InboundReceiptState.deferred.value,
                    )
                )
                & (InboundReceiptRow.next_attempt_at <= now)
            ),
            (
                InboundReceiptRow.state.in_(
                    (
                        InboundReceiptState.claimed.value,
                        InboundReceiptState.admitted.value,
                    )
                )
                & (InboundReceiptRow.lease_expires_at <= now)
            ),
        )
        earlier = aliased(InboundReceiptRow)
        unfinished = (
            InboundReceiptState.received.value,
            InboundReceiptState.claimed.value,
            InboundReceiptState.admitted.value,
            InboundReceiptState.deferred.value,
        )
        no_earlier_thread_receipt = ~exists(
            select(1)
            .select_from(earlier)
            .where(
                earlier.thread_id == InboundReceiptRow.thread_id,
                earlier.state.in_(unfinished),
                or_(
                    earlier.received_at < InboundReceiptRow.received_at,
                    and_(
                        earlier.received_at == InboundReceiptRow.received_at,
                        earlier.receipt_id < InboundReceiptRow.receipt_id,
                    ),
                ),
            )
        )
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(InboundReceiptRow)
                    .where(
                        InboundReceiptRow.receipt_id == receipt_id,
                        eligible,
                        no_earlier_thread_receipt,
                    )
                    .values(
                        state=InboundReceiptState.claimed.value,
                        lease_owner=lease_owner,
                        lease_expires_at=now + timedelta(seconds=lease_seconds),
                        fencing_token=InboundReceiptRow.fencing_token + 1,
                        attempt_count=InboundReceiptRow.attempt_count + 1,
                        updated_at=now,
                    )
                    .returning(InboundReceiptRow)
                )
                row = result.scalar_one_or_none()
        return None if row is None else _row_to_receipt(row)

    async def bind_admitted(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        fencing_token: int,
        run_id: str,
    ) -> bool:
        """Fence-bind the admitted run; a reclaimed worker makes old tokens stale."""

        _bounded_text(receipt_id, field_name="receipt_id", max_bytes=36)
        _bounded_text(lease_owner, field_name="lease_owner", max_bytes=96)
        _bounded_text(run_id, field_name="run_id", max_bytes=64)
        if not isinstance(fencing_token, int) or isinstance(fencing_token, bool) or fencing_token < 1:
            raise ValueError("fencing_token must be a positive integer")
        now = self._clock()
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(InboundReceiptRow)
                    .where(
                        InboundReceiptRow.receipt_id == receipt_id,
                        InboundReceiptRow.state == InboundReceiptState.claimed.value,
                        InboundReceiptRow.lease_owner == lease_owner,
                        InboundReceiptRow.fencing_token == fencing_token,
                    )
                    .values(
                        state=InboundReceiptState.admitted.value,
                        run_id=run_id,
                        outcome_code="admitted",
                        updated_at=now,
                    )
                    .returning(InboundReceiptRow.receipt_id)
                )
                return result.scalar_one_or_none() is not None

    async def complete(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        fencing_token: int,
        outcome_code: str,
    ) -> bool:
        """Fence-complete claimed command/rejection work or admitted work."""

        _bounded_text(receipt_id, field_name="receipt_id", max_bytes=36)
        _bounded_text(lease_owner, field_name="lease_owner", max_bytes=96)
        _bounded_text(outcome_code, field_name="outcome_code", max_bytes=64)
        if not isinstance(fencing_token, int) or isinstance(fencing_token, bool) or fencing_token < 1:
            raise ValueError("fencing_token must be a positive integer")
        now = self._clock()
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(InboundReceiptRow)
                    .where(
                        InboundReceiptRow.receipt_id == receipt_id,
                        InboundReceiptRow.state.in_(
                            (
                                InboundReceiptState.claimed.value,
                                InboundReceiptState.admitted.value,
                            )
                        ),
                        InboundReceiptRow.lease_owner == lease_owner,
                        InboundReceiptRow.fencing_token == fencing_token,
                    )
                    .values(
                        state=InboundReceiptState.completed.value,
                        lease_owner=None,
                        lease_expires_at=None,
                        outcome_code=outcome_code,
                        completed_at=now,
                        updated_at=now,
                    )
                    .returning(InboundReceiptRow.receipt_id)
                )
                return result.scalar_one_or_none() is not None

    async def defer_contention(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        fencing_token: int,
        delay_seconds: int,
        outcome_code: str,
    ) -> bool:
        """Fence-release schedulable contention without spending failure budget."""

        _bounded_text(receipt_id, field_name="receipt_id", max_bytes=36)
        _bounded_text(lease_owner, field_name="lease_owner", max_bytes=96)
        _bounded_text(outcome_code, field_name="outcome_code", max_bytes=64)
        if not isinstance(fencing_token, int) or isinstance(fencing_token, bool) or fencing_token < 1:
            raise ValueError("fencing_token must be a positive integer")
        if not isinstance(delay_seconds, int) or isinstance(delay_seconds, bool) or not 1 <= delay_seconds <= 3600:
            raise ValueError("delay_seconds must be between 1 and 3600")
        now = self._clock()
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(InboundReceiptRow)
                    .where(
                        InboundReceiptRow.receipt_id == receipt_id,
                        InboundReceiptRow.state == InboundReceiptState.claimed.value,
                        InboundReceiptRow.lease_owner == lease_owner,
                        InboundReceiptRow.fencing_token == fencing_token,
                    )
                    .values(
                        state=InboundReceiptState.deferred.value,
                        lease_owner=None,
                        lease_expires_at=None,
                        next_attempt_at=now + timedelta(seconds=delay_seconds),
                        outcome_code=outcome_code,
                        updated_at=now,
                    )
                    .returning(InboundReceiptRow.receipt_id)
                )
                return result.scalar_one_or_none() is not None

    async def defer_failure(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        fencing_token: int,
        delay_seconds: int,
        outcome_code: str,
        max_failures: int,
    ) -> InboundReceiptState | None:
        """Atomically count a poison failure and defer or terminalize it."""

        _bounded_text(receipt_id, field_name="receipt_id", max_bytes=36)
        _bounded_text(lease_owner, field_name="lease_owner", max_bytes=96)
        _bounded_text(outcome_code, field_name="outcome_code", max_bytes=64)
        if not isinstance(fencing_token, int) or isinstance(fencing_token, bool) or fencing_token < 1:
            raise ValueError("fencing_token must be a positive integer")
        if not isinstance(delay_seconds, int) or isinstance(delay_seconds, bool) or not 1 <= delay_seconds <= 3600:
            raise ValueError("delay_seconds must be between 1 and 3600")
        if not isinstance(max_failures, int) or isinstance(max_failures, bool) or not 1 <= max_failures <= 100:
            raise ValueError("max_failures must be between 1 and 100")
        now = self._clock()
        exhausted = InboundReceiptRow.failure_count + 1 >= max_failures
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(InboundReceiptRow)
                    .where(
                        InboundReceiptRow.receipt_id == receipt_id,
                        InboundReceiptRow.state == InboundReceiptState.claimed.value,
                        InboundReceiptRow.lease_owner == lease_owner,
                        InboundReceiptRow.fencing_token == fencing_token,
                    )
                    .values(
                        state=case(
                            (exhausted, InboundReceiptState.dead_letter.value),
                            else_=InboundReceiptState.deferred.value,
                        ),
                        failure_count=InboundReceiptRow.failure_count + 1,
                        lease_owner=None,
                        lease_expires_at=None,
                        next_attempt_at=now + timedelta(seconds=delay_seconds),
                        outcome_code=case(
                            (exhausted, "attempts_exhausted"),
                            else_=outcome_code,
                        ),
                        completed_at=case((exhausted, now), else_=None),
                        updated_at=now,
                    )
                    .returning(InboundReceiptRow.state)
                )
                state = result.scalar_one_or_none()
        return None if state is None else InboundReceiptState(state)

    async def dead_letter(
        self,
        receipt_id: str,
        *,
        lease_owner: str,
        fencing_token: int,
        outcome_code: str,
    ) -> bool:
        """Fence-terminalize a poison receipt without retaining exception text."""

        _bounded_text(receipt_id, field_name="receipt_id", max_bytes=36)
        _bounded_text(lease_owner, field_name="lease_owner", max_bytes=96)
        _bounded_text(outcome_code, field_name="outcome_code", max_bytes=64)
        if not isinstance(fencing_token, int) or isinstance(fencing_token, bool) or fencing_token < 1:
            raise ValueError("fencing_token must be a positive integer")
        now = self._clock()
        async with self._session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    update(InboundReceiptRow)
                    .where(
                        InboundReceiptRow.receipt_id == receipt_id,
                        InboundReceiptRow.state == InboundReceiptState.claimed.value,
                        InboundReceiptRow.lease_owner == lease_owner,
                        InboundReceiptRow.fencing_token == fencing_token,
                    )
                    .values(
                        state=InboundReceiptState.dead_letter.value,
                        lease_owner=None,
                        lease_expires_at=None,
                        outcome_code=outcome_code,
                        completed_at=now,
                        updated_at=now,
                    )
                    .returning(InboundReceiptRow.receipt_id)
                )
                return result.scalar_one_or_none() is not None

    async def cleanup_completed(self, *, older_than: datetime, limit: int) -> int:
        """Delete old completed rows; unresolved dead letters require review."""

        if older_than.tzinfo is None or older_than.utcoffset() is None:
            raise ValueError("cleanup cutoff must be timezone-aware")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 500:
            raise ValueError("cleanup limit must be between 1 and 500")
        async with self._session_factory() as session:
            async with session.begin():
                ids = (
                    await session.scalars(
                        select(InboundReceiptRow.receipt_id)
                        .where(
                            InboundReceiptRow.state == InboundReceiptState.completed.value,
                            InboundReceiptRow.completed_at < older_than,
                        )
                        .order_by(InboundReceiptRow.completed_at, InboundReceiptRow.receipt_id)
                        .limit(limit)
                    )
                ).all()
                if not ids:
                    return 0
                from sqlalchemy import delete

                result = await session.execute(
                    delete(InboundReceiptRow).where(
                        InboundReceiptRow.receipt_id.in_(ids),
                        # Repeat the eligibility predicates so a row changed
                        # between the bounded candidate read and this delete
                        # cannot be removed under stale assumptions.
                        InboundReceiptRow.state == InboundReceiptState.completed.value,
                        InboundReceiptRow.completed_at < older_than,
                    )
                )
                return int(result.rowcount or 0)


class InboundProcessingDisposition(StrEnum):
    admitted = "admitted"
    completed = "completed"
    deferred = "deferred"


@dataclass(frozen=True, slots=True)
class InboundProcessingResult:
    """Bounded manager result used by receipt state transitions."""

    disposition: InboundProcessingDisposition
    run_id: str | None = None
    outcome_code: str = "completed"

    def __post_init__(self) -> None:
        disposition = InboundProcessingDisposition(self.disposition)
        object.__setattr__(self, "disposition", disposition)
        if disposition is InboundProcessingDisposition.admitted:
            _bounded_text(self.run_id, field_name="run_id", max_bytes=64)
        elif self.run_id is not None:
            raise ValueError("only admitted receipt processing may return run_id")
        _bounded_text(self.outcome_code, field_name="outcome_code", max_bytes=64)


@dataclass(frozen=True, slots=True)
class InboundReceiptWakeup:
    """Loss-tolerant MessageBus notification carrying no launch data."""

    receipt_id: str

    def __post_init__(self) -> None:
        _bounded_text(self.receipt_id, field_name="receipt_id", max_bytes=36)


class InboundReceiptProcessor:
    """Own bounded recovery, leasing, and durable receipt state transitions."""

    def __init__(
        self,
        *,
        store: InboundReceiptStore,
        publish_wakeup: Callable[[InboundReceiptWakeup], Any],
        process_message: Callable[[InboundMessage], Any],
        lease_owner: str | None = None,
        lease_seconds: int = 30,
        retry_delay_seconds: int = 2,
        contention_delay_seconds: int = 15,
        max_attempts: int = 8,
        recovery_page_size: int = 100,
        recovery_interval_seconds: float = 1.0,
        retention: timedelta = timedelta(days=7),
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self.store = store
        self._publish_wakeup = publish_wakeup
        self._process_message = process_message
        self._lease_owner = lease_owner or f"gateway-{uuid.uuid4()}"
        self._lease_seconds = lease_seconds
        self._retry_delay_seconds = retry_delay_seconds
        self._contention_delay_seconds = contention_delay_seconds
        self._max_attempts = max_attempts
        self._recovery_page_size = recovery_page_size
        self._recovery_interval_seconds = recovery_interval_seconds
        self._retention = retention
        self._clock = clock
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._processing_tasks: dict[str, asyncio.Task[None]] = {}

    @property
    def durable(self) -> bool:
        return self.store.durable

    async def receive_batch(
        self,
        messages: Sequence[InboundMessage],
    ) -> tuple[InboundReceipt, ...]:
        """Commit the entire verified fan-out before emitting lossy wake-ups."""

        candidates = tuple(InboundReceiptCandidate.from_message(message) for message in messages)
        receipts = await self.store.receive_batch(candidates)
        for receipt in receipts:
            if receipt.state not in {
                InboundReceiptState.completed,
                InboundReceiptState.dead_letter,
            }:
                await self._publish_wakeup(InboundReceiptWakeup(receipt.receipt_id))
        return receipts

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._recovery_loop())

    async def stop(self) -> None:
        if not self._running and self._task is None:
            return
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        processing = tuple(self._processing_tasks.values())
        for task in processing:
            task.cancel()
        for task in processing:
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                logger.warning(
                    "inbound receipt shutdown join failed code=processing_join_error exception_class=%s",
                    exc.__class__.__name__,
                )
        self._processing_tasks.clear()

    def schedule(self, receipt_id: str) -> bool:
        """Start one owned claim attempt; duplicate/lost wake-ups remain harmless."""

        _bounded_text(receipt_id, field_name="receipt_id", max_bytes=36)
        if not self._running or receipt_id in self._processing_tasks:
            return False
        task = asyncio.create_task(self.process(receipt_id))
        self._processing_tasks[receipt_id] = task

        def finished(completed: asyncio.Task[None]) -> None:
            if self._processing_tasks.get(receipt_id) is completed:
                self._processing_tasks.pop(receipt_id, None)
            if completed.cancelled():
                return
            exception = completed.exception()
            if exception is not None:
                logger.warning(
                    "inbound receipt task failed code=processing_task_error receipt_id=%s exception_class=%s",
                    receipt_id,
                    exception.__class__.__name__,
                )

        task.add_done_callback(finished)
        return True

    async def recover_once(self) -> int:
        due = await self.store.list_due(limit=self._recovery_page_size)
        for receipt in due:
            await self._publish_wakeup(InboundReceiptWakeup(receipt.receipt_id))
        await self.store.cleanup_completed(
            older_than=self._clock() - self._retention,
            limit=self._recovery_page_size,
        )
        return len(due)

    async def process(self, receipt_id: str) -> None:
        claim = await self.store.claim(
            receipt_id,
            lease_owner=self._lease_owner,
            lease_seconds=self._lease_seconds,
        )
        if claim is None:
            return
        if claim.run_id is not None:
            await self.store.complete(
                claim.receipt_id,
                lease_owner=self._lease_owner,
                fencing_token=claim.fencing_token,
                outcome_code="admitted_recovered",
            )
            return
        try:
            result = await self._process_message(claim.envelope.to_message())
            if not isinstance(result, InboundProcessingResult):
                raise TypeError("receipt processor callback returned an invalid result")
        except Exception as exc:
            logger.warning(
                "inbound receipt processing failed code=processing_error receipt_id=%s exception_class=%s",
                claim.receipt_id,
                exc.__class__.__name__,
            )
            await self._retry_or_dead_letter(claim, "processing_error")
            return
        if result.disposition is InboundProcessingDisposition.deferred:
            await self.store.defer_contention(
                claim.receipt_id,
                lease_owner=self._lease_owner,
                fencing_token=claim.fencing_token,
                delay_seconds=self._contention_delay_seconds,
                outcome_code=result.outcome_code,
            )
            return
        if result.disposition is InboundProcessingDisposition.admitted:
            bound = await self.store.bind_admitted(
                claim.receipt_id,
                lease_owner=self._lease_owner,
                fencing_token=claim.fencing_token,
                run_id=result.run_id or "",
            )
            if not bound:
                return
        await self.store.complete(
            claim.receipt_id,
            lease_owner=self._lease_owner,
            fencing_token=claim.fencing_token,
            outcome_code=result.outcome_code,
        )

    async def _retry_or_dead_letter(
        self,
        claim: InboundReceipt,
        outcome_code: str,
    ) -> None:
        delay = min(
            self._retry_delay_seconds * (2 ** max(0, claim.failure_count)),
            60,
        )
        state = await self.store.defer_failure(
            claim.receipt_id,
            lease_owner=self._lease_owner,
            fencing_token=claim.fencing_token,
            delay_seconds=delay,
            outcome_code=outcome_code,
            max_failures=self._max_attempts,
        )
        if state is InboundReceiptState.dead_letter:
            logger.warning(
                "inbound receipt terminalized code=inbound_receipt_dead_letter receipt_id=%s fencing_token=%s outcome_code=attempts_exhausted",
                claim.receipt_id,
                claim.fencing_token,
            )

    async def _recovery_loop(self) -> None:
        while self._running:
            try:
                await self.recover_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "inbound receipt recovery failed code=recovery_error exception_class=%s",
                    exc.__class__.__name__,
                )
            await asyncio.sleep(self._recovery_interval_seconds)


__all__ = [
    "InboundReceipt",
    "InboundReceiptCandidate",
    "InboundReceiptEnvelope",
    "InboundReceiptProcessor",
    "InboundReceiptReplayConflict",
    "InboundReceiptStore",
    "InboundReceiptState",
    "InboundReceiptWakeup",
    "InboundProcessingDisposition",
    "InboundProcessingResult",
    "SqlInboundReceiptStore",
]
