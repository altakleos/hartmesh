"""Gateway-owned exact-two execution recovery coordination.

The harness owns admission, owner/epoch compare-and-set, and terminal writes.
Gateway owns reconstruction because only the application can rebind the
checkpointer, accepted assembly, event store, and worker dependencies without
request-scoped state.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

from deerflow.agents.assembly_descriptor import TOOL_RECOVERY_POLICY_KEY
from deerflow.runtime import (
    ExecutionRecoveryDecision,
    ExecutionRecoveryDisposition,
    ExecutionRecoveryPayloadV1,
    ReconciledToolRecoveryProofV1,
    RunRecord,
    RunStatus,
)
from deerflow.runtime.events.catalog import RUN_EXECUTION_STARTED_EVENT
from deerflow.runtime.runs.store.base import RecoveryPolicy
from deerflow.runtime.tool_evidence import (
    TOOL_RECEIPT_OUTCOME_EVENT,
    TOOL_RECEIPT_STARTED_EVENT,
    ParsedToolReceiptEventV1,
    parse_tool_receipt_event,
)

_EVENT_PAGE_SIZE = 500
_MAX_RECOVERY_EVENTS = 10_000
_RECONCILABLE_KIND = "receipt_idempotent_reconcile_v1"


RecoveryDecisionGate = Callable[
    [RunRecord, object],
    Awaitable[ExecutionRecoveryDecision],
]


class RecoveryWorkerLauncher(Protocol):
    async def __call__(
        self,
        record: RunRecord,
        decision_gate: RecoveryDecisionGate,
    ) -> None: ...


def _checkpoint_id(checkpoint_tuple: object | None) -> str | None:
    if checkpoint_tuple is None:
        return None
    config = getattr(checkpoint_tuple, "config", None)
    if isinstance(config, Mapping):
        configurable = config.get("configurable")
        if isinstance(configurable, Mapping):
            value = configurable.get("checkpoint_id")
            if isinstance(value, str) and value:
                return value
    checkpoint = getattr(checkpoint_tuple, "checkpoint", None)
    if isinstance(checkpoint, Mapping):
        value = checkpoint.get("id")
        if isinstance(value, str) and value:
            return value
    return None


class GatewayExecutionRecoveryCoordinator:
    """Reconstruct and classify one won exact-two takeover.

    ``recover`` performs only request-independent reconstruction and attaches a
    worker that is expected to wait on ``record.execution_recovery_release_event``.
    The manager validates that attachment and releases it. The worker then
    rebuilds and verifies accepted material/assembly before calling ``decide``
    with the actual host-sealed assembly descriptor. This split prevents any
    graph/model/tool work before manager approval while keeping tool recovery
    eligibility out of persisted names and extension-authored metadata.
    """

    def __init__(
        self,
        *,
        run_store: object,
        event_store: object,
        checkpointer: object,
        worker_launcher: RecoveryWorkerLauncher,
    ) -> None:
        if not callable(worker_launcher):
            raise TypeError("worker_launcher must be callable")
        self._run_store = run_store
        self._event_store = event_store
        self._checkpointer = checkpointer
        self._worker_launcher = worker_launcher

    async def recover(self, record: RunRecord) -> ExecutionRecoveryDecision:
        """Attach a release-gated replacement worker after bounded validation."""

        if record.recovery_payload_json is None:
            raise ValueError("recovery_payload_unavailable")
        ExecutionRecoveryPayloadV1.from_persisted(
            record.recovery_payload_json,
        )
        await self._require_current_takeover(record)
        await self._worker_launcher(record, self.decide)
        # This is manager-level attachment approval, not the worker's final
        # safe-point decision. The final decision is made after immutable
        # assembly/material revalidation inside the released worker.
        return ExecutionRecoveryDecision(
            ExecutionRecoveryDisposition.resumed,
        )

    async def decide(
        self,
        record: RunRecord,
        assembly_descriptor: object,
    ) -> ExecutionRecoveryDecision:
        """Classify the durable resume point after accepted revalidation."""

        row = await self._require_current_takeover(record)
        policies = getattr(assembly_descriptor, "effective_policies", None)
        if not isinstance(policies, Mapping):
            raise ValueError("recovery_assembly_policy_unavailable")
        recovery_policy = policies.get(TOOL_RECOVERY_POLICY_KEY, {})
        if not isinstance(recovery_policy, Mapping) or any(not isinstance(name, str) or not isinstance(kind, str) or kind != _RECONCILABLE_KIND for name, kind in recovery_policy.items()):
            raise ValueError("recovery_assembly_policy_invalid")

        events = await self._list_recovery_events(record)
        marker_events = [event for event in events if event.get("event_type") == RUN_EXECUTION_STARTED_EVENT.event_type]
        try:
            open_receipts = self._open_receipts(events)
        except Exception:
            return ExecutionRecoveryDecision(
                ExecutionRecoveryDisposition.terminalize_tool_attempt_indeterminate,
            )

        receipt_events_present = any(event.get("event_type") in {TOOL_RECEIPT_STARTED_EVENT, TOOL_RECEIPT_OUTCOME_EVENT} for event in events)
        if not marker_events:
            if receipt_events_present:
                return ExecutionRecoveryDecision(
                    ExecutionRecoveryDisposition.terminalize_tool_attempt_indeterminate,
                )
            return ExecutionRecoveryDecision(
                ExecutionRecoveryDisposition.restart_pre_graph,
            )
        if len(marker_events) != 1:
            return ExecutionRecoveryDecision(
                ExecutionRecoveryDisposition.terminalize_checkpoint_unavailable,
            )
        pre_graph_checkpoint_id = self._marker_checkpoint_id(
            marker_events[0],
        )

        checkpoint_tuple = await self._checkpointer.aget_tuple(
            {
                "configurable": {
                    "thread_id": record.thread_id,
                    "checkpoint_ns": "",
                }
            }
        )
        current_checkpoint_id = _checkpoint_id(checkpoint_tuple)
        has_safe_checkpoint = current_checkpoint_id is not None and current_checkpoint_id != pre_graph_checkpoint_id

        if open_receipts:
            if len(open_receipts) != 1 or not has_safe_checkpoint:
                return ExecutionRecoveryDecision(
                    ExecutionRecoveryDisposition.terminalize_tool_attempt_indeterminate,
                )
            parsed = open_receipts[0]
            receipt = parsed.receipt
            if recovery_policy.get(receipt.tool_name) != _RECONCILABLE_KIND:
                return ExecutionRecoveryDecision(
                    ExecutionRecoveryDisposition.terminalize_tool_attempt_indeterminate,
                )
            assembly_json = row.get("assembly_evidence_json")
            accepted_fingerprint = assembly_json.get("fingerprint") if isinstance(assembly_json, Mapping) else None
            if not isinstance(accepted_fingerprint, str) or receipt.context.assembly_fingerprint != accepted_fingerprint:
                return ExecutionRecoveryDecision(
                    ExecutionRecoveryDisposition.terminalize_tool_attempt_indeterminate,
                )
            owner = record.owner_worker_id
            if not isinstance(owner, str) or not owner:
                raise ValueError("recovery_takeover_owner_unavailable")
            proof = ReconciledToolRecoveryProofV1(
                receipt_id=receipt.receipt_id,
                tool_name=receipt.tool_name,
                recovery_kind=_RECONCILABLE_KIND,
                assembly_evidence_digest=record.assembly_evidence_digest,
                dispatch_generation_digest=(parsed.dispatch_generation_digest),
                takeover_owner_worker_id=owner,
                takeover_state_version=record.state_version,
            )
            return ExecutionRecoveryDecision(
                ExecutionRecoveryDisposition.resume_reconciled_tool,
                reconciled_tool=proof,
            )

        if not has_safe_checkpoint:
            return ExecutionRecoveryDecision(
                ExecutionRecoveryDisposition.terminalize_checkpoint_unavailable,
            )
        return ExecutionRecoveryDecision(
            ExecutionRecoveryDisposition.resume_checkpoint,
        )

    async def _require_current_takeover(
        self,
        record: RunRecord,
    ) -> Mapping[str, Any]:
        getter = getattr(self._run_store, "authoritative_get", None)
        if callable(getter):
            row = await getter(record.run_id)
        else:
            getter = getattr(self._run_store, "get", None)
            if not callable(getter):
                raise RuntimeError("recovery_run_store_unavailable")
            try:
                row = await getter(record.run_id, user_id=None)
            except TypeError:
                row = await getter(record.run_id)
        if not isinstance(row, Mapping):
            raise ValueError("recovery_takeover_lost")
        expected_status = record.status.value if isinstance(record.status, RunStatus) else str(record.status)
        if (
            row.get("run_id") != record.run_id
            or row.get("thread_id") != record.thread_id
            or row.get("operation_kind", "run") != "run"
            or row.get("status") != expected_status
            or row.get("status") not in {"pending", "running"}
            or row.get("owner_worker_id") != record.owner_worker_id
            or row.get("state_version") != record.state_version
            or row.get("recovery_policy") != RecoveryPolicy.exact_two_takeover_v1.value
            or row.get("recovery_payload_json") != record.recovery_payload_json
            or row.get("assembly_evidence_digest") != record.assembly_evidence_digest
        ):
            raise ValueError("recovery_takeover_lost")
        authorizer = getattr(
            self._run_store,
            "execution_owner_authorized",
            None,
        )
        if not callable(authorizer):
            raise RuntimeError("recovery_run_store_authority_unavailable")
        authorized = await authorizer(
            record.run_id,
            owner_worker_id=record.owner_worker_id,
            state_version=record.state_version,
        )
        if authorized is not True:
            raise ValueError("recovery_takeover_lost")
        return row

    async def _list_recovery_events(
        self,
        record: RunRecord,
    ) -> list[Mapping[str, Any]]:
        event_types = [
            RUN_EXECUTION_STARTED_EVENT.event_type,
            TOOL_RECEIPT_STARTED_EVENT,
            TOOL_RECEIPT_OUTCOME_EVENT,
        ]
        events: list[Mapping[str, Any]] = []
        after_seq: int | None = None
        while len(events) < _MAX_RECOVERY_EVENTS:
            page = await self._event_store.list_events(
                record.thread_id,
                record.run_id,
                event_types=event_types,
                limit=min(
                    _EVENT_PAGE_SIZE,
                    _MAX_RECOVERY_EVENTS - len(events),
                ),
                after_seq=after_seq,
                user_id=None,
            )
            if not isinstance(page, list):
                raise ValueError("recovery_event_page_invalid")
            if not page:
                return events
            for event in page:
                if not isinstance(event, Mapping):
                    raise ValueError("recovery_event_invalid")
                seq = event.get("seq")
                if type(seq) is not int or (after_seq is not None and seq <= after_seq):
                    raise ValueError("recovery_event_sequence_invalid")
                events.append(event)
                after_seq = seq
            if len(page) < _EVENT_PAGE_SIZE:
                return events
        probe = await self._event_store.list_events(
            record.thread_id,
            record.run_id,
            event_types=event_types,
            limit=1,
            after_seq=after_seq,
            user_id=None,
        )
        if probe:
            raise ValueError("recovery_event_limit_exceeded")
        return events

    @staticmethod
    def _marker_checkpoint_id(event: Mapping[str, Any]) -> str | None:
        if event.get("category") != RUN_EXECUTION_STARTED_EVENT.category or event.get("run_id") is None:
            raise ValueError("recovery_execution_marker_invalid")
        content = event.get("content")
        if not isinstance(content, Mapping) or set(content) != {
            "version",
            "pre_graph_checkpoint_id",
        }:
            raise ValueError("recovery_execution_marker_invalid")
        if content.get("version") != 1:
            raise ValueError("recovery_execution_marker_invalid")
        checkpoint_id = content.get("pre_graph_checkpoint_id")
        if checkpoint_id is not None and (not isinstance(checkpoint_id, str) or not checkpoint_id or len(checkpoint_id.encode("utf-8")) > 256):
            raise ValueError("recovery_execution_marker_invalid")
        return checkpoint_id

    @staticmethod
    def _open_receipts(
        events: list[Mapping[str, Any]],
    ) -> list[ParsedToolReceiptEventV1]:
        starts: dict[str, ParsedToolReceiptEventV1] = {}
        outcomes: set[str] = set()
        for event in events:
            if event.get("event_type") not in {
                TOOL_RECEIPT_STARTED_EVENT,
                TOOL_RECEIPT_OUTCOME_EVENT,
            }:
                continue
            parsed = parse_tool_receipt_event(event)
            receipt_id = parsed.receipt.receipt_id
            if parsed.receipt.phase == "started":
                if receipt_id in starts:
                    raise ValueError("recovery_tool_receipt_duplicate_start")
                starts[receipt_id] = parsed
            else:
                if receipt_id not in starts or receipt_id in outcomes:
                    raise ValueError("recovery_tool_receipt_invalid_outcome")
                outcomes.add(receipt_id)
        return [parsed for receipt_id, parsed in starts.items() if receipt_id not in outcomes]


__all__ = [
    "GatewayExecutionRecoveryCoordinator",
    "RecoveryDecisionGate",
    "RecoveryWorkerLauncher",
]
