"""In-memory RunEventStore. Used when run_events.backend=memory (default) and in tests.

Thread-safe for single-process async usage (no threading locks needed
since all mutations happen within the same event loop).
"""

from __future__ import annotations

import bisect
import copy
from datetime import UTC, datetime

from deerflow_extension_api import TenantReferenceV1

from deerflow.constants import RETRIEVAL_OBSERVATION_EVENT_CATEGORY, RETRIEVAL_OBSERVATION_EVENT_TYPE
from deerflow.retrieval import retrieval_observation_event_metadata
from deerflow.runtime.events.appender import RuntimeEventAuthority, RuntimeEventOwnershipLost
from deerflow.runtime.events.message_identity import message_identity
from deerflow.runtime.events.store.base import (
    AppendOutcome,
    RetrievalPairAppendOutcome,
    RunEventStore,
    find_paired_retrieval_observation,
    prepare_retrieval_pair,
    reconcile_existing_retrieval_pair,
    resolve_owned_run,
    validate_idempotent_append,
)
from deerflow.runtime.tool_evidence import (
    TOOL_RECEIPT_CATEGORY,
    TOOL_RECEIPT_STARTED_EVENT,
    DurableToolReceiptV1,
    ToolReceiptIntegrityError,
    canonical_digest,
    parse_tool_receipt_event,
    receipt_event_metadata,
    require_started_transition,
    require_tool_attempt_binding_fence,
    reserve_attempt_from_events,
)
from deerflow.runtime.user_context import AUTO, _AutoSentinel


class MemoryRunEventStore(RunEventStore):
    def __init__(
        self,
        *,
        run_store: object | None = None,
        tenant: TenantReferenceV1 | None = None,
    ) -> None:
        if tenant is not None and not isinstance(tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1 or None")
        self._run_store = run_store
        self._tenant = tenant
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

    def _tenant_visible(self, event: dict) -> bool:
        return self._tenant is None or event.get("tenant_digest") == self._tenant.digest

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
        user_id: str | None | _AutoSentinel = AUTO,
    ) -> dict:
        seq = self._next_seq(thread_id)
        record = {
            "thread_id": thread_id,
            "run_id": run_id,
            "tenant_ref": None if self._tenant is None else self._tenant.public_ref,
            "tenant_digest": None if self._tenant is None else self._tenant.digest,
            "event_type": event_type,
            "category": category,
            "content": content,
            "metadata": metadata or {},
            "seq": seq,
            "created_at": created_at or datetime.now(UTC).isoformat(),
        }
        if not isinstance(user_id, _AutoSentinel):
            record["user_id"] = user_id
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
        user_id: str | None | _AutoSentinel = AUTO,
    ):
        # No await occurs between the lookup and append, so this is atomic for
        # the backend's documented single-event-loop concurrency model.
        for event in self._events_by_run.get(thread_id, {}).get(run_id, []):
            if self._tenant_visible(event) and event["event_type"] == event_type:
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
                user_id=user_id,
            ),
            True,
        )

    async def _require_runtime_authority(
        self,
        authority: RuntimeEventAuthority,
    ) -> dict:
        configured_digest = None if self._tenant is None else self._tenant.digest
        if authority.tenant_digest != configured_digest:
            raise RuntimeEventOwnershipLost("runtime_event_ownership_lost")
        try:
            run = await resolve_owned_run(
                self._run_store,
                authority.run_id,
                owner_id=authority.owner_id,
                lease_epoch=authority.lease_epoch,
                allowed_statuses=("pending", "running"),
            )
        except Exception:
            raise RuntimeEventOwnershipLost("runtime_event_ownership_lost") from None
        if run.get("thread_id") != authority.thread_id or run.get("tenant_digest") != authority.tenant_digest:
            raise RuntimeEventOwnershipLost("runtime_event_ownership_lost")
        return run

    async def append_fenced_batch(
        self,
        authority: RuntimeEventAuthority,
        events: list[dict],
    ) -> list[dict]:
        await self._require_runtime_authority(authority)
        results: list[dict] = []
        # No await occurs after validation, so validation and append are one
        # critical section in this backend's single-event-loop model.
        for event in events:
            authority.require_event_identity(event)
            results.append(self._put_one(**event))
        return results

    async def append_fenced_if_absent(
        self,
        authority: RuntimeEventAuthority,
        event: dict,
    ) -> tuple[dict, bool]:
        await self._require_runtime_authority(authority)
        authority.require_event_identity(event)
        for existing in self._events_by_run.get(authority.thread_id, {}).get(authority.run_id, []):
            if self._tenant_visible(existing) and existing["event_type"] == event["event_type"]:
                return existing, False
        return self._put_one(**event), True

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
        if existing is not None and self._tenant_visible(existing):
            if canonical_digest(existing["content"]) != canonical_digest(detached):
                raise ToolReceiptIntegrityError("receipt_idempotency_conflict")
            parse_tool_receipt_event(existing)
            return AppendOutcome(event=copy.deepcopy(existing), created=False)
        receipt = DurableToolReceiptV1.from_event_body(detached, occurred_at=datetime.now(UTC))
        if receipt.phase != "started":
            require_started_transition(
                [event for event in self._events_by_run.get(run["thread_id"], {}).get(run_id, []) if self._tenant_visible(event)],
                receipt,
            )
        record = self._put_one(
            thread_id=run["thread_id"],
            run_id=run_id,
            event_type=event_type,
            category=TOOL_RECEIPT_CATEGORY,
            content=detached,
            metadata=receipt_event_metadata(
                receipt,
                writer_owner_id=owner_id,
                writer_lease_epoch=lease_epoch,
            ),
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
        observed_node_attempt,
        expected_attempt,
        owner_id,
        lease_epoch,
        capability_kind=None,
    ) -> AppendOutcome:
        binding = require_tool_attempt_binding_fence(
            binding,
            run_id=run_id,
            owner_id=owner_id,
            lease_epoch=lease_epoch,
        )
        run = await resolve_owned_run(
            self._run_store,
            run_id,
            owner_id=owner_id,
            lease_epoch=lease_epoch,
        )
        events = [event for event in self._events_by_run.get(run["thread_id"], {}).get(run_id, []) if self._tenant_visible(event)]
        receipt, existing, terminal = reserve_attempt_from_events(
            events,
            binding=binding,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
            request_projection_digest=request_projection_digest,
            observed_node_attempt=observed_node_attempt,
            expected_attempt=expected_attempt,
            capability_kind=capability_kind,
        )
        if existing is not None:
            return AppendOutcome(
                event=copy.deepcopy(dict(existing)),
                created=False,
                terminal_event=(copy.deepcopy(dict(terminal)) if terminal is not None else None),
                retrieval_observation_event=(copy.deepcopy(find_paired_retrieval_observation(events, terminal)) if terminal is not None else None),
            )
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
            metadata=receipt_event_metadata(
                receipt,
                writer_owner_id=owner_id,
                writer_lease_epoch=lease_epoch,
                include_capability_marker=True,
                capability_kind=capability_kind,
            ),
        )
        record["idempotency_key"] = receipt.idempotency_key
        self._idempotency[(run_id, TOOL_RECEIPT_STARTED_EVENT, receipt.idempotency_key)] = record
        return AppendOutcome(event=copy.deepcopy(record), created=True)

    async def append_retrieval_pair(
        self,
        run_id,
        *,
        receipt_body,
        observation_body,
        owner_id,
        lease_epoch,
    ) -> RetrievalPairAppendOutcome:
        prepared = prepare_retrieval_pair(
            receipt_body,
            observation_body,
        )
        receipt_body = prepared.receipt_body
        observation_body = prepared.observation_body
        receipt = prepared.receipt
        observation = prepared.observation
        run = await resolve_owned_run(
            self._run_store,
            run_id,
            owner_id=owner_id,
            lease_epoch=lease_epoch,
        )
        events = [event for event in self._events_by_run.get(run["thread_id"], {}).get(run_id, []) if self._tenant_visible(event)]
        require_started_transition(events, receipt)
        receipt_key = (run_id, "tool_receipt.outcome.v1", receipt.idempotency_key)
        observation_key = (
            run_id,
            RETRIEVAL_OBSERVATION_EVENT_TYPE,
            observation.idempotency_key,
        )
        existing_receipt = self._idempotency.get(receipt_key)
        existing_observation = self._idempotency.get(observation_key)
        receipt_created, observation_created = reconcile_existing_retrieval_pair(
            prepared,
            existing_receipt=existing_receipt,
            existing_observation=existing_observation,
        )
        thread_id = run["thread_id"]
        thread_length = len(self._events.get(thread_id, ()))
        run_length = len(self._events_by_run.get(thread_id, {}).get(run_id, ()))
        previous_seq = self._seq_counters.get(thread_id)
        try:
            if existing_receipt is None:
                existing_receipt = self._put_one(
                    thread_id=thread_id,
                    run_id=run_id,
                    event_type="tool_receipt.outcome.v1",
                    category=TOOL_RECEIPT_CATEGORY,
                    content=receipt_body,
                    metadata=receipt_event_metadata(
                        receipt,
                        writer_owner_id=owner_id,
                        writer_lease_epoch=lease_epoch,
                    ),
                )
                existing_receipt["idempotency_key"] = receipt.idempotency_key
                self._idempotency[receipt_key] = existing_receipt
            if existing_observation is None:
                existing_observation = self._put_one(
                    thread_id=thread_id,
                    run_id=run_id,
                    event_type=RETRIEVAL_OBSERVATION_EVENT_TYPE,
                    category=RETRIEVAL_OBSERVATION_EVENT_CATEGORY,
                    content=observation_body,
                    metadata=retrieval_observation_event_metadata(
                        observation,
                        task_id=receipt.context.execution_task_id,
                        writer_fence_digest=receipt_event_metadata(
                            receipt,
                            writer_owner_id=owner_id,
                            writer_lease_epoch=lease_epoch,
                        )["writer_fence_digest"],
                    ),
                )
                existing_observation["idempotency_key"] = observation.idempotency_key
                self._idempotency[observation_key] = existing_observation
        except BaseException:
            del self._events.get(thread_id, [])[thread_length:]
            del self._events_by_run.get(thread_id, {}).get(run_id, [])[run_length:]
            if previous_seq is None:
                self._seq_counters.pop(thread_id, None)
            else:
                self._seq_counters[thread_id] = previous_seq
            if receipt_created:
                self._idempotency.pop(receipt_key, None)
            if observation_created:
                self._idempotency.pop(observation_key, None)
            raise
        return RetrievalPairAppendOutcome(
            receipt_event=copy.deepcopy(existing_receipt),
            observation_event=copy.deepcopy(existing_observation),
            receipt_created=receipt_created,
            observation_created=observation_created,
        )

    async def list_messages(self, thread_id, *, limit=50, before_seq=None, after_seq=None, user_id: str | None | _AutoSentinel = AUTO):
        # ``messages`` is messages-only and seq-sorted, so the seq window is a
        # contiguous slice located with bisect (O(log m)) rather than a full scan.
        messages = [event for event in self._messages.get(thread_id, []) if self._tenant_visible(event)]

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
        run_events = [event for event in self._events_by_run.get(thread_id, {}).get(run_id, []) if self._tenant_visible(event)]
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
        messages = [event for event in self._messages_by_run.get(thread_id, {}).get(run_id, []) if self._tenant_visible(event)]
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
                if not self._tenant_visible(event):
                    continue
                caller = str((event.get("metadata") or {}).get("caller", ""))
                if event.get("category") == "message" and event.get("event_type") in {"llm.ai.response", "ai_message"} and not caller.startswith("middleware:"):
                    result[run_id] = event["seq"]
                    break
        return result

    async def count_messages(self, thread_id):
        return sum(1 for event in self._messages.get(thread_id, []) if self._tenant_visible(event))

    async def get_message_seqs(self, thread_id, identities, *, user_id: str | None | _AutoSentinel = AUTO):
        wanted = set(identities)
        if not wanted:
            return {}
        found: dict[str, int] = {}
        for record in self._messages.get(thread_id, []):
            content = record.get("content")
            if not isinstance(content, dict):
                continue
            identity = message_identity(content)
            # Earliest seq wins: a message replaced later in the same thread
            # keeps the position it first occupied in the feed.
            if identity in wanted and identity not in found:
                found[identity] = record["seq"]
                # Later rows can only be re-persisted copies that already lose
                # that tiebreak, so the scan ends with the last wanted seq.
                if len(found) == len(wanted):
                    break
        return found

    async def delete_by_thread(self, thread_id):
        events = self._events.get(thread_id, [])
        removed = [event for event in events if self._tenant_visible(event)]
        remaining = [event for event in events if not self._tenant_visible(event)]
        if remaining:
            self._events[thread_id] = remaining
            self._messages[thread_id] = [event for event in remaining if event["category"] == "message"]
            by_run: dict[str, list[dict]] = {}
            messages_by_run: dict[str, list[dict]] = {}
            for event in remaining:
                by_run.setdefault(event["run_id"], []).append(event)
                if event["category"] == "message":
                    messages_by_run.setdefault(event["run_id"], []).append(event)
            self._events_by_run[thread_id] = by_run
            self._messages_by_run[thread_id] = messages_by_run
        else:
            self._events.pop(thread_id, None)
            self._messages.pop(thread_id, None)
            self._events_by_run.pop(thread_id, None)
            self._messages_by_run.pop(thread_id, None)
            self._seq_counters.pop(thread_id, None)
        self._idempotency = {key: value for key, value in self._idempotency.items() if not (value.get("thread_id") == thread_id and self._tenant_visible(value))}
        return len(removed)

    async def delete_by_run(self, thread_id, run_id):
        all_events = self._events.get(thread_id, [])
        if not all_events:
            return 0
        remaining = [event for event in all_events if event["run_id"] != run_id or not self._tenant_visible(event)]
        removed = len(all_events) - len(remaining)
        self._events[thread_id] = remaining
        # Keep the message projection in lockstep (same surviving dict objects).
        self._messages[thread_id] = [e for e in remaining if e["category"] == "message"]
        # Rebuild the run projection because another tenant can legitimately
        # retain events for the same public run identifier in migration tests.
        retained_for_run = [event for event in remaining if event["run_id"] == run_id]
        if retained_for_run:
            self._events_by_run.setdefault(thread_id, {})[run_id] = retained_for_run
            self._messages_by_run.setdefault(thread_id, {})[run_id] = [event for event in retained_for_run if event["category"] == "message"]
        else:
            self._events_by_run.get(thread_id, {}).pop(run_id, None)
            self._messages_by_run.get(thread_id, {}).pop(run_id, None)
        self._idempotency = {key: value for key, value in self._idempotency.items() if not (key[0] == run_id and self._tenant_visible(value))}
        return removed
