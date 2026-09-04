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
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from deerflow.runtime.events.catalog import TOOL_RECEIPT_RUN_EVENT_DEFINITIONS
from deerflow.runtime.user_context import AUTO, _AutoSentinel

if TYPE_CHECKING:
    from deerflow.retrieval import RetrievalObservationV1
    from deerflow.runtime.events.appender import RuntimeEventAuthority
    from deerflow.runtime.tool_evidence import DurableToolReceiptV1

_RECEIPT_EVENT_TYPES = frozenset(definition.event_type for definition in TOOL_RECEIPT_RUN_EVENT_DEFINITIONS)
_MAX_IDEMPOTENCY_KEY_BYTES = 128
_MAX_IDEMPOTENT_BODY_BYTES = 8 * 1024


@dataclass(frozen=True, slots=True)
class AppendOutcome:
    """Result of one fenced idempotent event append."""

    event: dict
    created: bool
    terminal_event: dict | None = None
    retrieval_observation_event: dict | None = None


@dataclass(frozen=True, slots=True)
class RetrievalPairAppendOutcome:
    """Result of one atomic receipt-outcome/retrieval-observation append."""

    receipt_event: dict
    observation_event: dict
    receipt_created: bool
    observation_created: bool


@dataclass(frozen=True, slots=True)
class PreparedRetrievalPair:
    """Validated immutable pair shared by every persistence backend."""

    receipt_body: dict[str, object]
    observation_body: dict[str, object]
    receipt: DurableToolReceiptV1
    observation: RetrievalObservationV1


def prepare_retrieval_pair(
    receipt_body: Mapping[str, object],
    observation_body: Mapping[str, object],
) -> PreparedRetrievalPair:
    """Validate, detach, and parse an incoming terminal retrieval pair once."""

    from deerflow.retrieval import RetrievalObservationV1, validate_retrieval_pair
    from deerflow.runtime.tool_evidence import DurableToolReceiptV1

    detached_receipt, detached_observation = validate_retrieval_pair(
        receipt_body,
        observation_body,
    )
    return PreparedRetrievalPair(
        receipt_body=detached_receipt,
        observation_body=detached_observation,
        receipt=DurableToolReceiptV1.from_event_body(
            detached_receipt,
            occurred_at=datetime.now(UTC),
        ),
        observation=RetrievalObservationV1.from_event_body(detached_observation),
    )


def reconcile_existing_retrieval_pair(
    prepared: PreparedRetrievalPair,
    *,
    existing_receipt: Mapping[str, object] | None,
    existing_observation: Mapping[str, object] | None,
) -> tuple[bool, bool]:
    """Validate idempotent persisted halves and return their create flags."""

    from deerflow.retrieval import RetrievalObservationV1
    from deerflow.runtime.tool_evidence import (
        ToolReceiptIntegrityError,
        canonical_digest,
        parse_tool_receipt_event,
    )

    if existing_receipt is not None:
        if canonical_digest(existing_receipt.get("content")) != canonical_digest(prepared.receipt_body):
            raise ToolReceiptIntegrityError("receipt_idempotency_conflict")
        parse_tool_receipt_event(existing_receipt)
    if existing_observation is not None:
        if canonical_digest(existing_observation.get("content")) != canonical_digest(prepared.observation_body):
            raise ToolReceiptIntegrityError("retrieval_observation_idempotency_conflict")
        RetrievalObservationV1.from_event_body(existing_observation.get("content"))
    if existing_observation is not None and existing_receipt is None:
        raise ToolReceiptIntegrityError("retrieval_pair_incomplete")
    return existing_receipt is None, existing_observation is None


def find_paired_retrieval_observation(
    events: Sequence[Mapping[str, object]],
    terminal_event: Mapping[str, object] | None,
) -> dict | None:
    """Return the uniquely validated observation paired to a terminal receipt."""

    if terminal_event is None:
        return None
    from deerflow.constants import RETRIEVAL_OBSERVATION_EVENT_TYPE
    from deerflow.retrieval import RetrievalObservationV1, validate_retrieval_pair
    from deerflow.runtime.tool_evidence import (
        ToolReceiptIntegrityError,
        parse_tool_receipt_event,
    )

    receipt = parse_tool_receipt_event(terminal_event).receipt
    expected_key = f"{receipt.receipt_id}:retrieval"
    candidates = [event for event in events if event.get("event_type") == RETRIEVAL_OBSERVATION_EVENT_TYPE and event.get("idempotency_key") == expected_key]
    if len(candidates) > 1:
        raise ToolReceiptIntegrityError("retrieval_observation_duplicate")
    if not candidates:
        return None
    candidate = dict(candidates[0])
    observation = RetrievalObservationV1.from_event_body(candidate.get("content"))
    validate_retrieval_pair(
        receipt.to_event_body(),
        observation.to_event_body(),
    )
    return candidate


async def resolve_owned_run(
    run_store: object | None,
    run_id: str,
    *,
    owner_id: str,
    lease_epoch: int,
    allowed_statuses: tuple[str, ...] = ("running",),
) -> dict:
    """Resolve a normal active row only while its execution fence is live.

    Tool-receipt callers retain the running-only default. Runtime-event
    appenders explicitly include ``pending`` so pre-start failures are fenced
    by the same owner/epoch capability as the rest of execution.
    """

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
    if not allowed_statuses or any(status not in {"pending", "running"} for status in allowed_statuses):
        raise ValueError("allowed_statuses must contain active run states")
    if row.get("operation_kind", "run") != "run" or row.get("status") not in allowed_statuses or row.get("owner_worker_id") != owner_id or row.get("state_version") != lease_epoch:
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


_AI_MESSAGE_RUN_LOOKUP_PAGE_SIZE = 1000


class IncompleteMessageRunLookupError(RuntimeError):
    """Raised when a store cannot prove that a targeted lookup is complete."""


def normalize_message_ids(message_ids: set[str]) -> set[str]:
    """Return the non-empty string IDs that can participate in a lookup."""
    return {message_id for message_id in message_ids if isinstance(message_id, str) and message_id}


def match_ai_message_run_id(event: object, message_ids: set[str]) -> tuple[str, str] | None:
    """Return a target AI message ID and its valid run ID, if present."""
    if not isinstance(event, dict) or event.get("category") != "message":
        return None
    content = event.get("content")
    run_id = event.get("run_id")
    if not isinstance(content, dict) or content.get("type") != "ai" or not isinstance(run_id, str) or not run_id:
        return None
    message_id = content.get("id")
    if not isinstance(message_id, str) or message_id not in message_ids:
        return None
    return message_id, run_id


class RunEventStore(abc.ABC):
    """Run event stream storage interface.

    All implementations must guarantee:
    1. put() events are retrievable in subsequent queries
    2. seq is strictly increasing within the same thread
    3. list_messages() only returns category="message" events
    4. list_events() returns all events for the specified run
    5. Returned dicts contain the required RunEvent envelope fields; backends
       may add documented fields such as DbRunEventStore.user_id
    6. find_latest_ai_message_run_ids() returns the newest valid AI message
       event for each requested ID and performs no storage work for empty input
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
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> tuple[dict, bool]:
        """Write one event unless this run already has the same event type.

        The check and write must be serialized with ordinary writers for the
        thread. Returns ``(record, created)``. This is the durability primitive
        used by terminal run receipts, whose recovery path may safely retry
        after a worker crash. Administrative background writers must pass the
        authoritative run owner explicitly because no request user context is
        available during recovery.
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
        observed_node_attempt: int,
        expected_attempt: int | None,
        owner_id: str,
        lease_epoch: int,
        capability_kind: str | None = None,
    ) -> AppendOutcome:
        """Atomically reserve and append the durable start for one attempt.

        The store reconciles a process-local retry observation against its
        contiguous durable history, returning an existing start and terminal
        on recovery or reserving the next attempt. Stores must decide and
        append while holding the same ownership/write fence.
        """

        del run_id, binding, tool_call_id, tool_name, request_projection_digest, observed_node_attempt, expected_attempt, owner_id, lease_epoch, capability_kind
        from deerflow.runtime.tool_evidence import ToolReceiptOwnershipLost

        raise ToolReceiptOwnershipLost("tool_receipt_store_unfenced")

    async def append_retrieval_pair(
        self,
        run_id: str,
        *,
        receipt_body: Mapping[str, object],
        observation_body: Mapping[str, object],
        owner_id: str,
        lease_epoch: int,
    ) -> RetrievalPairAppendOutcome:
        """Atomically append one terminal receipt and its observation.

        Compatibility stores fail closed. A supported retrieval result must
        never publish a terminal success through two independent mutations.
        """

        del run_id, receipt_body, observation_body, owner_id, lease_epoch
        from deerflow.runtime.tool_evidence import ToolReceiptOwnershipLost

        raise ToolReceiptOwnershipLost("retrieval_observation_store_unfenced")

    async def append_fenced_batch(
        self,
        authority: RuntimeEventAuthority,
        events: list[dict],
    ) -> list[dict]:
        """Append a live-worker batch under one tenant/run/owner/epoch fence.

        Compatibility stores fail closed. Administrative callers use
        :meth:`put_batch` explicitly and never receive this capability.
        """

        del authority, events
        from deerflow.runtime.events.appender import RuntimeEventOwnershipLost

        raise RuntimeEventOwnershipLost("runtime_event_store_unfenced")

    async def append_fenced_if_absent(
        self,
        authority: RuntimeEventAuthority,
        event: dict,
    ) -> tuple[dict, bool]:
        """Append one run-scoped singleton under the live execution fence."""

        del authority, event
        from deerflow.runtime.events.appender import RuntimeEventOwnershipLost

        raise RuntimeEventOwnershipLost("runtime_event_store_unfenced")

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

    async def find_latest_ai_message_run_ids(
        self,
        thread_id: str,
        message_ids: set[str],
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> dict[str, str]:
        """Map target message IDs to their newest valid AI event's run ID.

        Only ``category="message"`` events whose structured content has
        ``type="ai"`` and whose ``run_id`` is a non-empty string qualify. An
        empty target set must return immediately without storage work. The
        default implementation pages backward in bounded windows. It raises
        :class:`IncompleteMessageRunLookupError` instead of returning a
        partial result when a full page lacks a safe, progressing ``seq``
        cursor; callers may only treat an ordinary return as an exhaustive
        lookup for unresolved IDs.

        ``user_id`` follows the same explicit-caller semantics as
        :meth:`list_messages`.
        """
        pending = normalize_message_ids(message_ids)
        if not pending:
            return {}

        result: dict[str, str] = {}
        before_seq: int | None = None
        while pending:
            page = await self.list_messages(
                thread_id,
                limit=_AI_MESSAGE_RUN_LOOKUP_PAGE_SIZE,
                before_seq=before_seq,
                user_id=user_id,
            )
            if not page:
                break

            for event in reversed(page):
                match = match_ai_message_run_id(event, pending)
                if match is None:
                    continue
                message_id, run_id = match
                result[message_id] = run_id
                pending.remove(message_id)
                if not pending:
                    break

            if not pending or len(page) < _AI_MESSAGE_RUN_LOOKUP_PAGE_SIZE:
                break

            seqs: list[int] = []
            for event in page:
                seq = event.get("seq") if isinstance(event, dict) else None
                if not isinstance(seq, int) or isinstance(seq, bool):
                    raise IncompleteMessageRunLookupError("Run event lookup could not form a safe backward cursor from a full page")
                seqs.append(seq)

            next_before_seq = min(seqs)
            if before_seq is not None and next_before_seq >= before_seq:
                raise IncompleteMessageRunLookupError("Run event lookup could not form a safe backward cursor because seq did not progress")
            before_seq = next_before_seq

        return result

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
    async def get_message_seqs(
        self,
        thread_id: str,
        identities: Sequence[str],
        *,
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> dict[str, int]:
        """Return ``{identity: seq}`` for messages already persisted in this thread.

        A checkpoint carries no seq of its own and loses messages to
        summarization, so a client merging a checkpoint frame with this
        seq-ordered feed cannot place a surviving old message once the feed's
        loaded page window no longer reaches back to it (#4666). The seq already
        exists here; this exposes it without paging the whole feed.

        *identities* are the values produced by
        ``deerflow.runtime.events.message_identity.message_identity`` — the same
        rule the frontend applies — so both sides agree on what "same message"
        means. Identities that are not persisted (or not `category="message"`)
        are simply absent from the result: callers degrade to their own
        placement rule rather than treating a miss as an error. When one
        identity resolves to several rows, the earliest seq wins, so a message
        re-persisted later keeps the position it first occupied.
        """

    @abc.abstractmethod
    async def delete_by_thread(self, thread_id: str) -> int:
        """Delete all events for a thread. Return the number of deleted events."""

    @abc.abstractmethod
    async def delete_by_run(self, thread_id: str, run_id: str) -> int:
        """Delete all events for a specific run. Return the number of deleted events."""
