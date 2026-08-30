"""In-memory RunEventStore. Used when run_events.backend=memory (default) and in tests.

Thread-safe for single-process async usage (no threading locks needed
since all mutations happen within the same event loop).
"""

from __future__ import annotations

import bisect
import copy
from datetime import UTC, datetime

from deerflow.runtime.events.store.base import AppendOutcome, RunEventStore, resolve_owned_run, validate_idempotent_append
from deerflow.runtime.tool_evidence import (
    TOOL_RECEIPT_CATEGORY,
    TOOL_RECEIPT_STARTED_EVENT,
    DurableToolReceiptV1,
    ToolEvidenceRuntimeBinding,
    ToolReceiptIntegrityError,
    canonical_digest,
    receipt_event_metadata,
    require_started_transition,
    reserve_attempt_from_events,
)
from deerflow.runtime.user_context import AUTO, _AutoSentinel


class MemoryRunEventStore(RunEventStore):
    def __init__(self, *, run_store: object | None = None) -> None:
        self._run_store = run_store
        self._events: dict[str, list[dict]] = {}  # thread_id -> seq-sorted event list
        # Messages-only projection of ``_events`` (same dict objects, no copies),
        # kept in seq order so message pagination is O(log m + page) via bisect
        # instead of re-scanning every event on each request.
        self._messages: dict[str, list[dict]] = {}  # thread_id -> seq-sorted message list
        # Run-keyed projections of the two lists above (same dict objects, no
        # copies), kept in seq order. Per-run reads then cost O(events-in-run)
        # instead of O(events-in-thread): without these, ``list_events`` and
        # ``list_messages_by_run`` re-scan the whole thread's event log on every
        # request even though one run holds only a handful of events. This is
        # the per-run analogue of the thread-wide ``_messages`` projection.
        self._events_by_run: dict[str, dict[str, list[dict]]] = {}  # thread_id -> run_id -> seq-sorted events
        self._messages_by_run: dict[str, dict[str, list[dict]]] = {}  # thread_id -> run_id -> seq-sorted messages
        self._seq_counters: dict[str, int] = {}  # thread_id -> last assigned seq
        self._idempotency: dict[tuple[str, str, str], dict] = {}

    def _next_seq(self, thread_id: str) -> int:
        current = self._seq_counters.get(thread_id, 0)
        next_val = current + 1
        self._seq_counters[thread_id] = next_val
        return next_val

    def _put_one(
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
        seq = self._next_seq(thread_id)
        record = {
            "thread_id": thread_id,
            "run_id": run_id,
            "event_type": event_type,
            "category": category,
            "content": content,
            "metadata": metadata or {},
            "seq": seq,
            "created_at": created_at or datetime.now(UTC).isoformat(),
        }
        self._events.setdefault(thread_id, []).append(record)
        self._events_by_run.setdefault(thread_id, {}).setdefault(run_id, []).append(record)
        if category == "message":
            self._messages.setdefault(thread_id, []).append(record)
            self._messages_by_run.setdefault(thread_id, {}).setdefault(run_id, []).append(record)
        return record

    async def put(
        self,
        *,
        thread_id,
        run_id,
        event_type,
        category,
        content="",
        metadata=None,
        created_at=None,
    ):
        return self._put_one(
            thread_id=thread_id,
            run_id=run_id,
            event_type=event_type,
            category=category,
            content=content,
            metadata=metadata,
            created_at=created_at,
        )

    async def put_batch(self, events):
        results = []
        for ev in events:
            record = self._put_one(**ev)
            results.append(record)
        return results

    async def put_if_absent(
        self,
        *,
        thread_id,
        run_id,
        event_type,
        category,
        content="",
        metadata=None,
        created_at=None,
    ):
        # No await occurs between the lookup and append, so this is atomic for
        # the backend's documented single-event-loop concurrency model.
        for event in self._events_by_run.get(thread_id, {}).get(run_id, []):
            if event["event_type"] == event_type:
                return event, False
        return (
            self._put_one(
                thread_id=thread_id,
                run_id=run_id,
                event_type=event_type,
                category=category,
                content=content,
                metadata=metadata,
                created_at=created_at,
            ),
            True,
        )

    async def append_idempotent(
        self,
        run_id,
        *,
        event_type,
        idempotency_key,
        body,
        owner_id,
        lease_epoch,
    ) -> AppendOutcome:
        detached = validate_idempotent_append(
            event_type=event_type,
            idempotency_key=idempotency_key,
            body=body,
        )
        # The memory run-store lookup does not yield internally. After it
        # returns, comparison and append form one single-loop critical section.
        run = await resolve_owned_run(
            self._run_store,
            run_id,
            owner_id=owner_id,
            lease_epoch=lease_epoch,
        )
        key = (run_id, event_type, idempotency_key)
        existing = self._idempotency.get(key)
        if existing is not None:
            if canonical_digest(existing["content"]) != canonical_digest(detached):
                raise ToolReceiptIntegrityError("receipt_idempotency_conflict")
            return AppendOutcome(event=copy.deepcopy(existing), created=False)
        receipt = DurableToolReceiptV1.from_event_body(detached, occurred_at=datetime.now(UTC))
        if receipt.phase != "started":
            require_started_transition(self._events_by_run.get(run["thread_id"], {}).get(run_id, []), receipt)
        record = self._put_one(
            thread_id=run["thread_id"],
            run_id=run_id,
            event_type=event_type,
            category=TOOL_RECEIPT_CATEGORY,
            content=detached,
            metadata=receipt_event_metadata(receipt),
        )
        record["idempotency_key"] = idempotency_key
        self._idempotency[key] = record
        return AppendOutcome(event=copy.deepcopy(record), created=True)

    async def reserve_tool_attempt(
        self,
        run_id,
        *,
        binding,
        tool_call_id,
        tool_name,
        request_projection_digest,
        owner_id,
        lease_epoch,
    ) -> AppendOutcome:
        if not isinstance(binding, ToolEvidenceRuntimeBinding) or binding.run_id != run_id:
            raise ToolReceiptIntegrityError("receipt_attempt_binding_invalid")
        run = await resolve_owned_run(
            self._run_store,
            run_id,
            owner_id=owner_id,
            lease_epoch=lease_epoch,
        )
        events = self._events_by_run.get(run["thread_id"], {}).get(run_id, [])
        receipt, existing = reserve_attempt_from_events(
            events,
            binding=binding,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            request_projection_digest=request_projection_digest,
        )
        if existing is not None:
            return AppendOutcome(event=copy.deepcopy(dict(existing)), created=False)
        body = validate_idempotent_append(
            event_type=TOOL_RECEIPT_STARTED_EVENT,
            idempotency_key=receipt.idempotency_key,
            body=receipt.to_event_body(),
        )
        record = self._put_one(
            thread_id=run["thread_id"],
            run_id=run_id,
            event_type=TOOL_RECEIPT_STARTED_EVENT,
            category=TOOL_RECEIPT_CATEGORY,
            content=body,
            metadata=receipt_event_metadata(receipt),
        )
        record["idempotency_key"] = receipt.idempotency_key
        self._idempotency[(run_id, TOOL_RECEIPT_STARTED_EVENT, receipt.idempotency_key)] = record
        return AppendOutcome(event=copy.deepcopy(record), created=True)

    async def list_messages(self, thread_id, *, limit=50, before_seq=None, after_seq=None, user_id: str | None | _AutoSentinel = AUTO):
        # ``messages`` is messages-only and seq-sorted, so the seq window is a
        # contiguous slice located with bisect (O(log m)) rather than a full scan.
        messages = self._messages.get(thread_id, [])

        if before_seq is not None:
            # Records with seq < before_seq, then the last `limit` of them.
            hi = bisect.bisect_left(messages, before_seq, key=lambda e: e["seq"])
            return messages[max(0, hi - limit) : hi]
        elif after_seq is not None:
            # Records with seq > after_seq, then the first `limit` of them.
            lo = bisect.bisect_right(messages, after_seq, key=lambda e: e["seq"])
            return messages[lo : lo + limit]
        else:
            # Return the latest `limit` records, ascending.
            return messages[-limit:]

    async def list_events(self, thread_id, run_id, *, event_types=None, task_id=None, limit=500, after_seq=None, user_id: str | None | _AutoSentinel = AUTO):
        # ``_events_by_run`` is already scoped to this run and seq-ordered, so we
        # touch only this run's events instead of scanning the whole thread.
        run_events = self._events_by_run.get(thread_id, {}).get(run_id, [])
        if event_types is not None:
            run_events = [e for e in run_events if e["event_type"] in event_types]
        if task_id is not None:
            run_events = [e for e in run_events if (e.get("metadata") or {}).get("task_id") == task_id]
        if after_seq is not None:
            run_events = [e for e in run_events if e.get("seq", 0) > after_seq]
        return run_events[:limit]

    async def list_messages_by_run(self, thread_id, run_id, *, limit=50, before_seq=None, after_seq=None):
        # Per-run, messages-only, seq-sorted: the seq window is a contiguous
        # slice located with bisect (O(log m_run)) over only this run's
        # messages, instead of re-scanning the whole thread's event log.
        messages = self._messages_by_run.get(thread_id, {}).get(run_id, [])
        lo = 0 if after_seq is None else bisect.bisect_right(messages, after_seq, key=lambda e: e["seq"])
        hi = len(messages) if before_seq is None else bisect.bisect_left(messages, before_seq, key=lambda e: e["seq"])
        window = messages[lo:hi]
        # An ``after_seq`` cursor pages forward (first ``limit``); otherwise
        # return the last ``limit`` (the latest page, or the page ending just
        # before ``before_seq``). Matches the prior filter-based semantics.
        if after_seq is not None:
            return window[:limit]
        return window[-limit:]

    async def get_last_visible_ai_seq_by_run(self, thread_id, run_ids, *, user_id: str | None | _AutoSentinel = AUTO):
        result: dict[str, int] = {}
        messages_by_run = self._messages_by_run.get(thread_id, {})
        for run_id in run_ids:
            for event in reversed(messages_by_run.get(run_id, [])):
                caller = str((event.get("metadata") or {}).get("caller", ""))
                if event.get("category") == "message" and event.get("event_type") in {"llm.ai.response", "ai_message"} and not caller.startswith("middleware:"):
                    result[run_id] = event["seq"]
                    break
        return result

    async def count_messages(self, thread_id):
        return len(self._messages.get(thread_id, []))

    async def delete_by_thread(self, thread_id):
        events = self._events.pop(thread_id, [])
        self._messages.pop(thread_id, None)
        self._events_by_run.pop(thread_id, None)
        self._messages_by_run.pop(thread_id, None)
        self._seq_counters.pop(thread_id, None)
        run_ids = {event["run_id"] for event in events}
        self._idempotency = {key: value for key, value in self._idempotency.items() if key[0] not in run_ids}
        return len(events)

    async def delete_by_run(self, thread_id, run_id):
        all_events = self._events.get(thread_id, [])
        if not all_events:
            return 0
        remaining = [e for e in all_events if e["run_id"] != run_id]
        removed = len(all_events) - len(remaining)
        self._events[thread_id] = remaining
        # Keep the message projection in lockstep (same surviving dict objects).
        self._messages[thread_id] = [e for e in remaining if e["category"] == "message"]
        # Drop the deleted run from the run-keyed projections.
        self._events_by_run.get(thread_id, {}).pop(run_id, None)
        self._messages_by_run.get(thread_id, {}).pop(run_id, None)
        self._idempotency = {key: value for key, value in self._idempotency.items() if key[0] != run_id}
        return removed
