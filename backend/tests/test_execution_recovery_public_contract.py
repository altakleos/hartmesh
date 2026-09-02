"""Black-box contracts for the reversible exact-two execution policy."""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from deerflow_runtime_api import (
    GraphInputV1,
    InvocationEnsureRequest,
    InvocationOptionsV1,
)
from langgraph.checkpoint.base import BaseCheckpointSaver

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime import (
    ORPHAN_RECOVERY_STOP_REASON,
    ExecutionRecoveryDecision,
    ExecutionRecoveryDisposition,
    ExecutionRecoveryPayloadV1,
    ReconciledToolRecoveryProofV1,
    RunManager,
)
from deerflow.runtime.assembly_evidence import (
    AssemblyEvidenceV1,
    assembly_evidence_digest,
)
from deerflow.runtime.checkpointer.fenced_saver import (
    CheckpointOwnershipLost,
    FencedCheckpointSaver,
)
from deerflow.runtime.events.appender import (
    FencedRunEventAppender,
    RuntimeEventAuthority,
    RuntimeEventOwnershipLost,
)
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.store.base import (
    ExecutionTakeoverOutcome,
    RecoveryPolicy,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.tool_evidence import (
    DurableToolReceiptV1,
    RunEventToolReceiptSink,
    ToolAttemptContextV1,
    ToolReceiptOwnershipLost,
)


def _future_lease() -> str:
    return (datetime.now(UTC) + timedelta(minutes=5)).isoformat()


def _expired_lease() -> str:
    return (datetime.now(UTC) - timedelta(minutes=5)).isoformat()


def _ownership_config() -> RunOwnershipConfig:
    return RunOwnershipConfig(
        heartbeat_enabled=True,
        lease_seconds=30,
        grace_seconds=0,
    )


def _recovery_payload(thread_id: str) -> dict[str, object]:
    return ExecutionRecoveryPayloadV1(
        input_kind="graph",
        input_value={"messages": []},
        config={
            "recursion_limit": 100,
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
            },
        },
        stream_modes=("values",),
        stream_subgraphs=False,
    ).to_persisted()


def _assembly_evidence() -> AssemblyEvidenceV1:
    return AssemblyEvidenceV1(
        version=1,
        fingerprint="1" * 64,
        descriptor_version=1,
        namespace="deerflow",
        agent_name="lead-agent",
        effective_model="qualification-model",
        prompt_digest="2" * 64,
        toolset_digest="3" * 64,
        middleware_digest="4" * 64,
        skillset_digest="5" * 64,
        policy_digest="6" * 64,
        accepted_agent_revision_digest="7" * 64,
        extension_generation=1,
    )


class _RecordingSaver(BaseCheckpointSaver):
    def __init__(self) -> None:
        super().__init__()
        self.puts = 0
        self.writes = 0

    async def aput(self, config, checkpoint, metadata, new_versions):
        del checkpoint, metadata, new_versions
        self.puts += 1
        return config

    async def aput_writes(
        self,
        config,
        writes,
        task_id,
        task_path="",
    ) -> None:
        del config, writes, task_id, task_path
        self.writes += 1


def test_client_wire_cannot_select_or_retrofit_recovery_policy() -> None:
    request = InvocationEnsureRequest(
        external_key="delivery-client-policy",
        thread_id="thread-client-policy",
        agent_hint=None,
        input=GraphInputV1(value={"messages": []}),
        options=InvocationOptionsV1(),
    )
    forged = request.to_dict()
    forged["recovery_policy"] = "exact_two_takeover_v1"

    with pytest.raises(ValueError, match="unknown fields.*recovery_policy"):
        InvocationEnsureRequest.from_dict(forged)


def _started_receipt(
    *,
    run_id: str,
    owner_id: str,
    lease_epoch: int,
) -> DurableToolReceiptV1:
    return DurableToolReceiptV1.started(
        context=ToolAttemptContextV1(
            run_id=run_id,
            execution_task_id=run_id,
            execution_kind="lead",
            subagent_name=None,
            tool_call_id="qualification-call-1",
            attempt=1,
            owner_id=owner_id,
            lease_epoch=lease_epoch,
            agent_revision_digest="a" * 64,
            assembly_fingerprint="b" * 64,
            extension_generation=1,
            subagent_catalog_digest="c" * 64,
            subagent_definition_digest=None,
        ),
        tool_name="ordinary_external_operation",
        request_projection_digest="d" * 64,
    )


@pytest.mark.anyio
async def test_default_policy_still_terminalizes_and_never_calls_recovery() -> None:
    store = MemoryRunStore()
    await store.put(
        "run-default-terminalize",
        thread_id="thread-default-terminalize",
        status="pending",
        owner_worker_id="dead-owner",
        lease_expires_at=_expired_lease(),
    )
    callback_calls = 0

    async def forbidden_recovery(_record):
        nonlocal callback_calls
        callback_calls += 1
        return ExecutionRecoveryDisposition.resumed

    manager = RunManager(
        store=store,
        worker_id="survivor",
        run_ownership_config=_ownership_config(),
        on_execution_takeover=forbidden_recovery,
    )

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="legacy orphan recovery",
        stop_reason=ORPHAN_RECOVERY_STOP_REASON,
    )

    persisted = await store.get("run-default-terminalize")
    assert callback_calls == 0
    assert [record.run_id for record in recovered] == [
        "run-default-terminalize",
    ]
    assert persisted is not None
    assert persisted["status"] == "error"
    assert persisted["stop_reason"] == ORPHAN_RECOVERY_STOP_REASON
    assert persisted["recovery_policy"] == RecoveryPolicy.terminalize_v1


@pytest.mark.anyio
async def test_takeover_preserves_canonical_accepted_evidence_bytes() -> None:
    store = MemoryRunStore()
    run_id = "run-immutable-evidence"
    await store.put(
        run_id,
        thread_id="thread-immutable-evidence",
        status="pending",
        owner_worker_id="dead-owner",
        lease_expires_at=_expired_lease(),
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload(
            "thread-immutable-evidence",
        ),
        origin_json={"source_kind": "http", "version": 1},
        principal_projection_json={"role": "member", "version": 1},
        principal_projection_digest="1" * 64,
        base_origin_digest="2" * 64,
        accepted_context_digest="3" * 64,
        agent_revision_json={"agent_id": "lead", "version": 1},
        agent_revision_digest="4" * 64,
        extension_generation=7,
        decision_evidence_json={"decisions": [], "version": 1},
        external_scope="service:qualification",
        external_key="delivery-immutable",
        request_digest="5" * 64,
        request_digest_version="sha256-canonical-json-v1",
        caller_intent_json={
            "input": {"messages": [{"content": "accepted", "role": "user"}]},
            "version": 1,
        },
        caller_intent_digest="6" * 64,
        caller_intent_digest_version="caller-intent-canonical-json-v1",
    )
    before = await store.get(run_id)
    assert before is not None
    before_epoch = before["state_version"]
    immutable_fields = (
        "run_id",
        "thread_id",
        "recovery_policy",
        "admission_cursor",
        "origin_json",
        "principal_projection_json",
        "principal_projection_digest",
        "base_origin_digest",
        "accepted_context_digest",
        "agent_revision_json",
        "agent_revision_digest",
        "extension_generation",
        "decision_evidence_json",
        "external_scope",
        "external_key",
        "request_digest",
        "request_digest_version",
        "caller_intent_json",
        "caller_intent_digest",
        "caller_intent_digest_version",
    )
    accepted_before = json.dumps(
        {field: before[field] for field in immutable_fields},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()

    claim = await store.claim_for_execution_takeover(
        run_id,
        new_owner_worker_id="survivor",
        lease_expires_at=_future_lease(),
        grace_seconds=0,
        expected_state_version=before_epoch,
    )

    assert claim.outcome is ExecutionTakeoverOutcome.claimed
    assert claim.row is not None
    accepted_after = json.dumps(
        {field: claim.row[field] for field in immutable_fields},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert accepted_after == accepted_before
    assert claim.row["owner_worker_id"] == "survivor"
    assert claim.row["state_version"] == before_epoch + 1


@pytest.mark.anyio
async def test_safe_pre_model_takeover_attaches_one_replacement_worker() -> None:
    store = MemoryRunStore()
    run_id = "run-safe-pre-model"
    await store.put(
        run_id,
        thread_id="thread-safe-pre-model",
        status="pending",
        owner_worker_id="dead-owner",
        lease_expires_at=_expired_lease(),
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload("thread-safe-pre-model"),
        caller_intent_json={"input": {"messages": []}, "version": 1},
        caller_intent_digest="7" * 64,
        caller_intent_digest_version="caller-intent-canonical-json-v1",
    )
    worker_started = asyncio.Event()
    release_worker = asyncio.Event()
    attached_tasks: list[asyncio.Task[None]] = []
    manager: RunManager

    async def replacement_worker() -> None:
        worker_started.set()
        await release_worker.wait()

    async def recover(record):
        task = await manager.attach_worker_once(
            record.run_id,
            replacement_worker(),
            asyncio.create_task,
        )
        attached_tasks.append(task)
        await worker_started.wait()
        return ExecutionRecoveryDisposition.restart_pre_graph

    manager = RunManager(
        store=store,
        worker_id="survivor",
        run_ownership_config=_ownership_config(),
        on_execution_takeover=recover,
        execution_takeover_eligibility=lambda _record: True,
        execution_recovery_claims_enabled=True,
    )

    assert await manager.reconcile_orphaned_inflight_runs(error="orphan") == []
    persisted = await store.get(run_id)
    assert worker_started.is_set()
    assert len(attached_tasks) == 1
    assert persisted is not None
    assert persisted["status"] == "pending"
    assert persisted["owner_worker_id"] == "survivor"
    assert persisted["recovery_policy"] == RecoveryPolicy.exact_two_takeover_v1

    release_worker.set()
    await attached_tasks[0]


@pytest.mark.anyio
async def test_indeterminate_tool_terminalizes_without_new_attempt_or_outcome() -> None:
    store = MemoryRunStore()
    run_id = "run-indeterminate-tool"
    old_owner = "dead-owner"
    await store.put(
        run_id,
        thread_id="thread-indeterminate-tool",
        status="running",
        owner_worker_id=old_owner,
        lease_expires_at=_future_lease(),
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload(
            "thread-indeterminate-tool",
        ),
    )
    row = await store.get(run_id)
    assert row is not None
    events = MemoryRunEventStore(run_store=store)
    sink = RunEventToolReceiptSink(events)
    started = _started_receipt(
        run_id=run_id,
        owner_id=old_owner,
        lease_epoch=row["state_version"],
    )
    await sink.record_started(started)
    renewal = await store.renew_lease(
        run_id,
        owner_worker_id=old_owner,
        lease_expires_at=_expired_lease(),
    )
    assert renewal.renewed is True
    callback_calls = 0

    async def fail_closed(_record):
        nonlocal callback_calls
        callback_calls += 1
        return ExecutionRecoveryDisposition.terminalize_tool_attempt_indeterminate

    manager = RunManager(
        store=store,
        event_store=events,
        worker_id="survivor",
        run_ownership_config=_ownership_config(),
        on_execution_takeover=fail_closed,
        execution_takeover_eligibility=lambda _record: True,
        execution_recovery_claims_enabled=True,
    )

    recovered = await manager.reconcile_orphaned_inflight_runs(error="orphan")

    persisted = await store.get(run_id)
    receipt_events = [
        event
        for event in await events.list_events(
            "thread-indeterminate-tool",
            run_id,
        )
        if event["event_type"].startswith("tool_receipt.")
    ]
    assert callback_calls == 1
    assert [record.run_id for record in recovered] == [run_id]
    assert persisted is not None
    assert persisted["status"] == "error"
    assert persisted["stop_reason"] == "recovery_tool_attempt_indeterminate"
    assert [event["event_type"] for event in receipt_events] == [
        "tool_receipt.started.v1",
    ]


@pytest.mark.anyio
async def test_takeover_terminalization_stays_recoverable_when_delivery_fails() -> None:
    store = MemoryRunStore()
    run_id = "run-terminal-delivery-failure"
    await store.put(
        run_id,
        thread_id="thread-terminal-delivery-failure",
        status="running",
        owner_worker_id="dead-owner",
        lease_expires_at=_expired_lease(),
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload(
            "thread-terminal-delivery-failure",
        ),
    )

    class _FailingDeliveryStore(MemoryRunEventStore):
        async def append_fenced_if_absent(self, authority, event):
            if event.get("event_type") == "run.delivery":
                raise RuntimeError("injected delivery outage")
            return await super().append_fenced_if_absent(authority, event)

    events = _FailingDeliveryStore(run_store=store)

    async def fail_closed(_record):
        return ExecutionRecoveryDisposition.terminalize_checkpoint_unavailable

    manager = RunManager(
        store=store,
        event_store=events,
        worker_id="survivor",
        run_ownership_config=_ownership_config(),
        on_execution_takeover=fail_closed,
        execution_takeover_eligibility=lambda _record: True,
        execution_recovery_claims_enabled=True,
    )

    recovered = await manager.reconcile_orphaned_inflight_runs(
        error="orphan",
    )

    persisted = await store.get(run_id)
    assert recovered == []
    assert persisted is not None
    assert persisted["status"] == "running"
    assert persisted["owner_worker_id"] == "survivor"
    assert (
        await events.list_events(
            "thread-terminal-delivery-failure",
            run_id,
            event_types=["run.delivery"],
        )
        == []
    )


@pytest.mark.anyio
async def test_epoch_transfer_rejects_stale_status_event_checkpoint_and_receipt_writes() -> None:
    store = MemoryRunStore()
    run_id = "run-stale-writers"
    thread_id = "thread-stale-writers"
    old_owner = "owner-a"
    await store.put(
        run_id,
        thread_id=thread_id,
        status="running",
        owner_worker_id=old_owner,
        lease_expires_at=_future_lease(),
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload(thread_id),
    )
    before = await store.get(run_id)
    assert before is not None
    old_epoch = before["state_version"]
    event_store = MemoryRunEventStore(run_store=store)
    appender = FencedRunEventAppender(
        event_store,
        RuntimeEventAuthority(
            tenant=None,
            thread_id=thread_id,
            run_id=run_id,
            owner_id=old_owner,
            lease_epoch=old_epoch,
        ),
    )
    await appender.put(
        event_type="run.start",
        category="trace",
        content={"owner": old_owner},
    )
    receipt_sink = RunEventToolReceiptSink(event_store)
    started = _started_receipt(
        run_id=run_id,
        owner_id=old_owner,
        lease_epoch=old_epoch,
    )
    await receipt_sink.record_started(started)

    @asynccontextmanager
    async def old_execution_fence():
        current = await store.authoritative_get(run_id)
        yield bool(current is not None and current.get("status") == "running" and current.get("owner_worker_id") == old_owner and current.get("state_version") == old_epoch)

    inner_saver = _RecordingSaver()
    checkpoint = FencedCheckpointSaver(
        inner_saver,
        fence=old_execution_fence,
    )
    checkpoint_config = {"configurable": {"thread_id": thread_id}}
    await checkpoint.aput(checkpoint_config, {}, {}, {})
    renewal = await store.renew_lease(
        run_id,
        owner_worker_id=old_owner,
        lease_expires_at=_expired_lease(),
    )
    assert renewal.renewed is True

    takeover = await store.claim_for_execution_takeover(
        run_id,
        new_owner_worker_id="owner-b",
        lease_expires_at=_future_lease(),
        grace_seconds=0,
        expected_state_version=old_epoch,
    )
    assert takeover.outcome is ExecutionTakeoverOutcome.claimed

    with pytest.raises(
        RuntimeEventOwnershipLost,
        match="runtime_event_ownership_lost",
    ):
        await appender.put(
            event_type="run.end",
            category="outputs",
            content={"stale": True},
        )
    with pytest.raises(CheckpointOwnershipLost):
        await checkpoint.aput_writes(
            checkpoint_config,
            [("channel", "stale")],
            "task-stale",
        )
    with pytest.raises(
        ToolReceiptOwnershipLost,
        match="tool_receipt_ownership_lost",
    ):
        await receipt_sink.record_outcome(
            started.outcome(
                phase="succeeded",
                result_projection_digest="e" * 64,
                result_kind="tool_message",
                safe_error_code=None,
            )
        )
    stale_status = await store.finalize_if_not_cancelled(
        run_id,
        status="success",
        expected_owner_worker_id=old_owner,
        expected_state_version=old_epoch,
        require_unexpired_lease=True,
    )

    persisted = await store.get(run_id)
    durable_events = await event_store.list_events(thread_id, run_id)
    assert stale_status.finalized is False
    assert inner_saver.puts == 1
    assert inner_saver.writes == 0
    assert [event["event_type"] for event in durable_events] == [
        "run.start",
        "tool_receipt.started.v1",
    ]
    assert persisted is not None
    assert persisted["status"] == "running"
    assert persisted["owner_worker_id"] == "owner-b"
    assert persisted["state_version"] == old_epoch + 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("proof_fence", "expect_release"),
    [
        ("matching", True),
        ("stale_owner", False),
        ("stale_epoch", False),
    ],
)
async def test_reconciled_tool_proof_is_bound_to_the_won_takeover_fence(
    proof_fence: str,
    expect_release: bool,
) -> None:
    store = MemoryRunStore()
    run_id = f"run-proof-{proof_fence}"
    thread_id = f"thread-proof-{proof_fence}"
    await store.put(
        run_id,
        thread_id=thread_id,
        status="running",
        owner_worker_id="dead-owner",
        lease_expires_at=_future_lease(),
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload(thread_id),
    )
    row = await store.get(run_id)
    assert row is not None
    evidence = _assembly_evidence()
    evidence_digest = assembly_evidence_digest(evidence)
    assert (
        await store.bind_assembly_evidence(
            run_id,
            owner_id="dead-owner",
            lease_epoch=row["state_version"],
            evidence_json=evidence.to_persisted_json(),
            evidence_digest=evidence_digest,
        )
    ).value == "bound"
    renewal = await store.renew_lease(
        run_id,
        owner_worker_id="dead-owner",
        lease_expires_at=_expired_lease(),
    )
    assert renewal.renewed is True

    worker_released = asyncio.Event()
    worker_attached = asyncio.Event()
    attached_tasks: list[asyncio.Task[None]] = []
    manager: RunManager

    async def replacement_worker(record) -> None:
        worker_attached.set()
        await record.execution_recovery_release_event.wait()
        worker_released.set()

    async def recover(record):
        task = await manager.attach_worker_once(
            record.run_id,
            replacement_worker(record),
            asyncio.create_task,
        )
        attached_tasks.append(task)
        await worker_attached.wait()
        proof_owner = "previous-takeover-owner" if proof_fence == "stale_owner" else record.owner_worker_id
        proof_epoch = record.state_version - 1 if proof_fence == "stale_epoch" else record.state_version
        return ExecutionRecoveryDecision(
            ExecutionRecoveryDisposition.resume_reconciled_tool,
            reconciled_tool=ReconciledToolRecoveryProofV1(
                receipt_id="tr_" + ("a" * 64),
                tool_name="qualification_sandbox_operation",
                recovery_kind="receipt_idempotent_reconcile_v1",
                assembly_evidence_digest=evidence_digest,
                dispatch_generation_digest="b" * 64,
                takeover_owner_worker_id=proof_owner,
                takeover_state_version=proof_epoch,
            ),
        )

    manager = RunManager(
        store=store,
        worker_id="survivor",
        run_ownership_config=_ownership_config(),
        on_execution_takeover=recover,
        execution_takeover_eligibility=lambda _record: True,
        execution_recovery_claims_enabled=True,
    )

    assert await manager.reconcile_orphaned_inflight_runs(error="orphan") == []
    await asyncio.sleep(0)
    assert worker_released.is_set() is expect_release
    if not expect_release:
        assert attached_tasks[0].cancelled()
    await asyncio.gather(*attached_tasks, return_exceptions=True)
