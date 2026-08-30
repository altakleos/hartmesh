"""Abstract interface for run event storage.

RunEventStore is the unified storage interface for run event streams.
Messages (frontend display) and execution traces (debugging/audit) go
through the same interface, distinguished by the ``category`` field.

Implementations:
- MemoryRunEventStore: in-memory dict (development, tests)
- DbRunEventStore: SQLAlchemy ORM-backed persistence
- JsonlRunEventStore: JSONL file persistence for local/debug use
"""

from __future__ import annotations

import abc
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from deerflow.runtime.user_context import AUTO, _AutoSentinel

_RECEIPT_EVENT_TYPES = frozenset({"tool_receipt.started.v1", "tool_receipt.outcome.v1"})
_MAX_IDEMPOTENCY_KEY_BYTES = 128
_MAX_IDEMPOTENT_BODY_BYTES = 8 * 1024


@dataclass(frozen=True, slots=True)
class AppendOutcome:
    """Result of one fenced idempotent event append."""

    event: dict
    created: bool


async def resolve_owned_run(
    run_store: object | None,
    run_id: str,
    *,
    owner_id: str,
    lease_epoch: int,
) -> dict:
    """Resolve a normal running row only while its execution fence is live."""

    from deerflow.runtime.tool_evidence import ToolReceiptOwnershipLost

    if run_store is None:
        raise ToolReceiptOwnershipLost("tool_receipt_store_unfenced")
    getter = getattr(run_store, "authoritative_get", None)
    if callable(getter):
        row = await getter(run_id)
    else:
        getter = getattr(run_store, "get", None)
        row = await getter(run_id, user_id=None) if callable(getter) else None
    if not isinstance(row, dict):
        raise ToolReceiptOwnershipLost("tool_receipt_ownership_lost")
    if row.get("operation_kind", "run") != "run" or row.get("status") != "running" or row.get("owner_worker_id") != owner_id or row.get("state_version") != lease_epoch:
        raise ToolReceiptOwnershipLost("tool_receipt_ownership_lost")
    deadline = row.get("lease_expires_at")
    if deadline is not None:
        if isinstance(deadline, str):
            try:
                deadline = datetime.fromisoformat(deadline)
            except ValueError as exc:
                raise ToolReceiptOwnershipLost("tool_receipt_ownership_lost") from exc
        if not isinstance(deadline, datetime):
            raise ToolReceiptOwnershipLost("tool_receipt_ownership_lost")
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=UTC)
        if deadline <= datetime.now(UTC):
            raise ToolReceiptOwnershipLost("tool_receipt_ownership_lost")
    thread_id = row.get("thread_id")
    if not isinstance(thread_id, str) or not thread_id:
        raise ToolReceiptOwnershipLost("tool_receipt_ownership_lost")
    return row


def validate_idempotent_append(
    *,
    event_type: str,
    idempotency_key: str,
    body: Mapping[str, object],
) -> dict[str, object]:
    """Validate and detach one receipt event body before persistence."""

    from deerflow.runtime.tool_evidence import (
        TOOL_RECEIPT_STARTED_EVENT,
        DurableToolReceiptV1,
        ToolEvidenceError,
    )

    if event_type not in _RECEIPT_EVENT_TYPES:
        raise ToolEvidenceError("receipt_event_type_invalid")
    if not isinstance(idempotency_key, str) or not idempotency_key or len(idempotency_key.encode("utf-8")) > _MAX_IDEMPOTENCY_KEY_BYTES:
        raise ToolEvidenceError("receipt_idempotency_key_invalid")
    if not isinstance(body, Mapping):
        raise ToolEvidenceError("receipt_body_invalid")
    try:
        encoded = json.dumps(body, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
        detached = json.loads(encoded)
    except (TypeError, ValueError) as exc:
        raise ToolEvidenceError("receipt_body_invalid") from exc
    if len(encoded) > _MAX_IDEMPOTENT_BODY_BYTES or not isinstance(detached, dict):
        raise ToolEvidenceError("receipt_body_too_large")
    body_key = detached.get("idempotency_key")
    if body_key != idempotency_key:
        raise ToolEvidenceError("receipt_body_key_mismatch")
    receipt = DurableToolReceiptV1.from_event_body(detached, occurred_at=datetime.now(UTC))
    expected_type = TOOL_RECEIPT_STARTED_EVENT if receipt.phase == "started" else "tool_receipt.outcome.v1"
    if event_type != expected_type:
        raise ToolEvidenceError("receipt_event_phase_mismatch")
    return receipt.to_event_body()


class RunEventStore(abc.ABC):
    """Run event stream storage interface.

    All implementations must guarantee:
    1. put() events are retrievable in subsequent queries
    2. seq is strictly increasing within the same thread
    3. list_messages() only returns category="message" events
    4. list_events() returns all events for the specified run
    5. Returned dicts contain the required RunEvent envelope fields; backends
       may add documented fields such as DbRunEventStore.user_id
    """

    @abc.abstractmethod
    async def put(
        self,
        *,
        thread_id: str,
        run_id: str,
        event_type: str,
        category: str,
        content: str | dict = "",
        metadata: dict | None = None,
        created_at: str | None = None,
    ) -> dict:
        """Write an event, auto-assign seq, return the complete record."""

    @abc.abstractmethod
    async def put_batch(self, events: list[dict]) -> list[dict]:
        """Batch-write events. Used by RunJournal flush buffer.

        Each dict's keys match put()'s keyword arguments.
        Returns complete records with seq assigned.
        """

    @abc.abstractmethod
    async def put_if_absent(
        self,
        *,
        thread_id: str,
        run_id: str,
        event_type: str,
        category: str,
        content: str | dict = "",
        metadata: dict | None = None,
        created_at: str | None = None,
    ) -> tuple[dict, bool]:
        """Write one event unless this run already has the same event type.

        The check and write must be serialized with ordinary writers for the
        thread. Returns ``(record, created)``. This is the durability primitive
        used by terminal run receipts, whose recovery path may safely retry
        after a worker crash.
        """

    async def append_idempotent(
        self,
        run_id: str,
        *,
        event_type: str,
        idempotency_key: str,
        body: Mapping[str, object],
        owner_id: str,
        lease_epoch: int,
    ) -> AppendOutcome:
        """Append one logical event under the active run execution fence.

        Implementations compare the canonical body without the store-owned
        timestamp. An identical duplicate returns the original event; a
        conflicting duplicate fails with ``ToolReceiptIntegrityError``.
        Compatibility stores fail closed until they implement this boundary.
        """

        del run_id, event_type, idempotency_key, body, owner_id, lease_epoch
        from deerflow.runtime.tool_evidence import ToolReceiptOwnershipLost

        raise ToolReceiptOwnershipLost("tool_receipt_store_unfenced")

    async def reserve_tool_attempt(
        self,
        run_id: str,
        *,
        binding: object,
        tool_call_id: str,
        tool_name: str,
        request_projection_digest: str,
        owner_id: str,
        lease_epoch: int,
    ) -> AppendOutcome:
        """Atomically reserve and append the durable start for one attempt.

        An unmatched prior start is a recovery replay and is returned as-is.
        A prior terminal causes the next attempt number to be reserved. Stores
        must choose and append while holding the same ownership/write fence.
        """

        del run_id, binding, tool_call_id, tool_name, request_projection_digest, owner_id, lease_epoch
        from deerflow.runtime.tool_evidence import ToolReceiptOwnershipLost

        raise ToolReceiptOwnershipLost("tool_receipt_store_unfenced")

    @abc.abstractmethod
    async def list_messages(
        self,
        thread_id: str,
        *,
        limit: int = 50,
        before_seq: int | None = None,
        after_seq: int | None = None,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> list[dict]:
        """Return displayable messages (category=message) for a thread, ordered by seq ascending.

        Supports bidirectional cursor pagination:
        - before_seq: return the last ``limit`` records with seq < before_seq (ascending)
        - after_seq: return the first ``limit`` records with seq > after_seq (ascending)
        - neither: return the latest ``limit`` records (ascending)

        ``user_id`` may be passed explicitly by request-independent callers;
        user-scoped backends must apply it according to their isolation model.
        """

    @abc.abstractmethod
    async def list_events(
        self,
        thread_id: str,
        run_id: str,
        *,
        event_types: list[str] | None = None,
        task_id: str | None = None,
        limit: int = 500,
        after_seq: int | None = None,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> list[dict]:
        """Return the full event stream for a run, ordered by seq ascending.

        Optionally filter by ``event_types`` and/or ``task_id`` (matched against
        ``metadata["task_id"]``). ``after_seq`` is a forward cursor returning the
        first ``limit`` records with seq > after_seq, so callers can page through
        a single subagent task's events without the run-wide ``limit`` truncating
        the tail (#3779).
        """

    @abc.abstractmethod
    async def list_messages_by_run(
        self,
        thread_id: str,
        run_id: str,
        *,
        limit: int = 50,
        before_seq: int | None = None,
        after_seq: int | None = None,
    ) -> list[dict]:
        """Return displayable messages (category=message) for a specific run, ordered by seq ascending.

        Supports bidirectional cursor pagination:
        - after_seq: return the first ``limit`` records with seq > after_seq (ascending)
        - before_seq: return the last ``limit`` records with seq < before_seq (ascending)
        - neither: return the latest ``limit`` records (ascending)
        """

    @abc.abstractmethod
    async def get_last_visible_ai_seq_by_run(
        self,
        thread_id: str,
        run_ids: set[str],
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> dict[str, int]:
        """Return each run's last non-middleware AI message sequence.

        ``user_id`` follows the same explicit-caller semantics as
        :meth:`list_messages`.
        """

    @abc.abstractmethod
    async def count_messages(self, thread_id: str) -> int:
        """Count displayable messages (category=message) in a thread."""

    @abc.abstractmethod
    async def delete_by_thread(self, thread_id: str) -> int:
        """Delete all events for a thread. Return the number of deleted events."""

    @abc.abstractmethod
    async def delete_by_run(self, thread_id: str, run_id: str) -> int:
        """Delete all events for a specific run. Return the number of deleted events."""
