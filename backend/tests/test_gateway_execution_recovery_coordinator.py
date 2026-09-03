from __future__ import annotations

import asyncio
from dataclasses import dataclass
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import (
    ActingServiceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    VerifiedActorContextV1,
)
from fastapi import FastAPI
from langchain_core.messages import HumanMessage
from langgraph.types import Command

import deerflow.runtime as runtime_module
from app.gateway import deps as gateway_deps
from app.gateway import services as gateway_services
from app.gateway.run_recovery import GatewayExecutionRecoveryCoordinator
from deerflow.runtime import (
    DisconnectMode,
    ExecutionRecoveryDecision,
    ExecutionRecoveryDisposition,
    ExecutionRecoveryPayloadV1,
    RunContext,
    RunRecord,
    RunStatus,
)
from deerflow.runtime.events.catalog import RUN_EXECUTION_STARTED_EVENT
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.store.base import RecoveryPolicy
from deerflow.runtime.tenant_identity import TenantIdentityV1
from deerflow.runtime.tool_evidence import (
    TOOL_RECEIPT_CATEGORY,
    TOOL_RECEIPT_OUTCOME_EVENT,
    TOOL_RECEIPT_STARTED_EVENT,
    DurableToolReceiptV1,
    ToolAttemptContextV1,
    parse_tool_receipt_event,
    receipt_event_metadata,
)


def _payload(thread_id: str) -> dict[str, object]:
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


def _record(*, run_id: str = "run-recovery", thread_id: str = "thread-recovery") -> RunRecord:
    return RunRecord(
        run_id=run_id,
        thread_id=thread_id,
        assistant_id="lead_agent",
        status=RunStatus.running,
        on_disconnect=DisconnectMode.continue_,
        owner_worker_id="survivor",
        lease_expires_at="2999-01-01T00:00:00+00:00",
        state_version=4,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_payload(thread_id),
        assembly_evidence_digest="a" * 64,
        execution_takeover=True,
    )


class _RunStore:
    def __init__(self, record: RunRecord) -> None:
        self.authorized = True
        self.row = {
            "run_id": record.run_id,
            "thread_id": record.thread_id,
            "operation_kind": "run",
            "status": record.status.value,
            "owner_worker_id": record.owner_worker_id,
            "lease_expires_at": record.lease_expires_at,
            "state_version": record.state_version,
            "recovery_policy": record.recovery_policy.value,
            "recovery_payload_json": record.recovery_payload_json,
            "assembly_evidence_digest": record.assembly_evidence_digest,
        }

    async def authoritative_get(self, run_id: str):
        return dict(self.row) if run_id == self.row["run_id"] else None

    async def execution_owner_authorized(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        state_version: int,
    ) -> bool:
        return bool(self.authorized and run_id == self.row["run_id"] and owner_worker_id == self.row["owner_worker_id"] and state_version == self.row["state_version"])


@dataclass
class _CheckpointTuple:
    checkpoint_id: str

    @property
    def config(self):
        return {"configurable": {"checkpoint_id": self.checkpoint_id}}

    checkpoint = None


class _Checkpointer:
    def __init__(self, checkpoint_id: str | None) -> None:
        self.checkpoint_id = checkpoint_id
        self.calls = 0

    async def aget_tuple(self, _config):
        self.calls += 1
        if self.checkpoint_id is None:
            return None
        return _CheckpointTuple(self.checkpoint_id)


async def _recover(
    record: RunRecord,
    *,
    event_store: MemoryRunEventStore,
    checkpoint_id: str | None,
):
    gate_tasks: list[asyncio.Task] = []
    checkpointer = _Checkpointer(checkpoint_id)

    async def launch(claimed, gate):
        descriptor = SimpleNamespace(effective_policies={})

        async def released_worker():
            await claimed.execution_recovery_release_event.wait()
            return await gate(claimed, descriptor)

        gate_tasks.append(asyncio.create_task(released_worker()))

    coordinator = GatewayExecutionRecoveryCoordinator(
        run_store=_RunStore(record),
        event_store=event_store,
        checkpointer=checkpointer,
        worker_launcher=launch,
    )
    preliminary = await coordinator.recover(record)
    assert preliminary.disposition is ExecutionRecoveryDisposition.resumed
    assert len(gate_tasks) == 1
    await asyncio.sleep(0)
    assert not gate_tasks[0].done()
    assert checkpointer.calls == 0
    record.execution_recovery_release_event.set()
    decision = await gate_tasks[0]
    return decision


@pytest.mark.anyio
async def test_absent_execution_marker_restarts_only_original_accepted_intent() -> None:
    record = _record()

    decision = await _recover(
        record,
        event_store=MemoryRunEventStore(),
        checkpoint_id="previous-run-head",
    )

    assert decision.disposition is ExecutionRecoveryDisposition.restart_pre_graph


@pytest.mark.anyio
async def test_absent_execution_marker_with_closed_receipt_fails_closed() -> None:
    record = _record()
    events = MemoryRunEventStore()
    started = DurableToolReceiptV1.started(
        context=ToolAttemptContextV1(
            run_id=record.run_id,
            execution_task_id=record.run_id,
            execution_kind="lead",
            subagent_name=None,
            tool_call_id="call-1",
            attempt=1,
            owner_id="dead-worker",
            lease_epoch=3,
            agent_revision_digest="b" * 64,
            assembly_fingerprint="c" * 64,
            extension_generation=1,
            subagent_catalog_digest="d" * 64,
            subagent_definition_digest=None,
        ),
        tool_name="qualification_tool",
        request_projection_digest="e" * 64,
    )
    outcome = started.outcome(
        phase="succeeded",
        result_projection_digest="f" * 64,
        result_kind="tool_message",
        safe_error_code=None,
    )
    for event_type, receipt in (
        (TOOL_RECEIPT_STARTED_EVENT, started),
        (TOOL_RECEIPT_OUTCOME_EVENT, outcome),
    ):
        event = await events.put(
            thread_id=record.thread_id,
            run_id=record.run_id,
            event_type=event_type,
            category=TOOL_RECEIPT_CATEGORY,
            content=receipt.to_event_body(),
            metadata=receipt_event_metadata(
                receipt,
                writer_owner_id="dead-worker",
                writer_lease_epoch=3,
            ),
        )
        event["idempotency_key"] = receipt.idempotency_key
        parse_tool_receipt_event(event)

    decision = await _recover(
        record,
        event_store=events,
        checkpoint_id="previous-run-head",
    )

    assert decision.disposition is ExecutionRecoveryDisposition.terminalize_tool_attempt_indeterminate


@pytest.mark.anyio
async def test_marker_without_new_checkpoint_fails_closed() -> None:
    record = _record()
    events = MemoryRunEventStore()
    await events.put(
        thread_id=record.thread_id,
        run_id=record.run_id,
        event_type=RUN_EXECUTION_STARTED_EVENT.event_type,
        category=RUN_EXECUTION_STARTED_EVENT.category,
        content={"version": 1, "pre_graph_checkpoint_id": "before"},
    )

    decision = await _recover(
        record,
        event_store=events,
        checkpoint_id="before",
    )

    assert decision.disposition is ExecutionRecoveryDisposition.terminalize_checkpoint_unavailable


@pytest.mark.anyio
async def test_marker_with_new_durable_checkpoint_resumes_without_original_input() -> None:
    record = _record()
    events = MemoryRunEventStore()
    await events.put(
        thread_id=record.thread_id,
        run_id=record.run_id,
        event_type=RUN_EXECUTION_STARTED_EVENT.event_type,
        category=RUN_EXECUTION_STARTED_EVENT.category,
        content={"version": 1, "pre_graph_checkpoint_id": "before"},
    )

    decision = await _recover(
        record,
        event_store=events,
        checkpoint_id="after-model",
    )

    assert decision.disposition is ExecutionRecoveryDisposition.resume_checkpoint


@pytest.mark.anyio
async def test_takeover_validation_uses_store_clock_authority_not_pod_timestamp() -> None:
    """Persisted deadlines are evidence; the store decides current authority."""

    record = _record()
    record.lease_expires_at = "1970-01-01T00:00:00+00:00"
    store = _RunStore(record)
    coordinator = GatewayExecutionRecoveryCoordinator(
        run_store=store,
        event_store=MemoryRunEventStore(),
        checkpointer=_Checkpointer(None),
        worker_launcher=lambda *_args: None,
    )

    row = await coordinator._require_current_takeover(record)

    assert row["lease_expires_at"] == record.lease_expires_at


@pytest.mark.anyio
async def test_takeover_validation_rejects_database_authority_loss() -> None:
    """A future-looking evidence timestamp cannot override the store fence."""

    record = _record()
    store = _RunStore(record)
    store.authorized = False
    coordinator = GatewayExecutionRecoveryCoordinator(
        run_store=store,
        event_store=MemoryRunEventStore(),
        checkpointer=_Checkpointer(None),
        worker_launcher=lambda *_args: None,
    )

    with pytest.raises(ValueError, match="recovery_takeover_lost"):
        await coordinator._require_current_takeover(record)


class _AttachmentManager:
    def __init__(self) -> None:
        self.task: asyncio.Task[None] | None = None
        self.validated: list[ExecutionRecoveryDecision] = []

    async def attach_worker_once(self, _run_id, worker, task_factory):
        self.task = task_factory(worker)
        return self.task

    async def validate_execution_recovery_decision(self, _record, decision):
        self.validated.append(decision)
        return True


@pytest.mark.anyio
@pytest.mark.parametrize("input_kind", ["graph", "command_resume"])
async def test_production_launcher_reconstructs_only_after_manager_release(
    monkeypatch: pytest.MonkeyPatch,
    input_kind: str,
) -> None:
    record = _record()
    original_identity = InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(
            kind="human",
            subject_id="user-1",
            role="user",
        ),
        acting_service=ActingServiceV1(
            service_id="channel:slack",
        ),
    )
    tenant = TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference()
    record.accepted_invocation = SimpleNamespace(
        principal=SimpleNamespace(
            is_internal=False,
            identity=original_identity,
        ),
        tenant=tenant,
    )
    input_value: object = {"messages": [{"role": "user", "content": "recover once"}]} if input_kind == "graph" else {"approved": True}
    record.recovery_payload_json = ExecutionRecoveryPayloadV1(
        input_kind=input_kind,  # type: ignore[arg-type]
        input_value=input_value,
        config={
            "recursion_limit": 100,
            "configurable": {
                "thread_id": record.thread_id,
                "checkpoint_ns": "",
            },
        },
        stream_modes=("messages-tuple",),
        stream_subgraphs=True,
        interrupt_before=("tools",),
        interrupt_after="*",
    ).to_persisted()
    manager = _AttachmentManager()
    app = FastAPI()
    app.state.stream_bridge = object()
    events: list[str] = []

    async def record_audit(**_values):
        events.append("audit")

    audit = AsyncMock()
    audit.record.side_effect = record_audit
    app.state.credential_audit_repo = audit
    context_calls = 0
    factory_calls = 0
    worker_calls: list[dict[str, object]] = []

    def get_context(_app):
        nonlocal context_calls
        context_calls += 1
        return RunContext(checkpointer=object())

    def resolve_factory(_assistant_id):
        nonlocal factory_calls
        factory_calls += 1
        return object()

    async def fake_run_agent(*_args, **kwargs):
        events.append("run")
        worker_calls.append(kwargs)
        executor = kwargs["ctx"].recovery_executor
        assert isinstance(executor, VerifiedActorContextV1)
        assert executor.identity.effective_subject == original_identity.effective_subject
        assert executor.identity.acting_service is not None
        assert executor.identity.acting_service.service_id == "gateway:execution-recovery"
        assert executor.identity != original_identity
        assert executor.credential.method == "internal_service"
        assert executor.credential.credential_ref is None
        assert executor.credential.authority_categories == ("runs",)
        assert executor.tenant == tenant
        assert record.accepted_invocation.principal.identity is original_identity
        decision = await kwargs["ctx"].execution_recovery_gate(
            record,
            SimpleNamespace(effective_policies={}),
        )
        assert decision.disposition is ExecutionRecoveryDisposition.restart_pre_graph

    async def decision_gate(_record, _descriptor):
        return ExecutionRecoveryDecision(
            ExecutionRecoveryDisposition.restart_pre_graph,
        )

    monkeypatch.setattr(gateway_deps, "get_app_run_context", get_context)
    monkeypatch.setattr(
        gateway_services,
        "resolve_agent_factory",
        resolve_factory,
    )
    monkeypatch.setattr(runtime_module, "run_agent", fake_run_agent)

    await gateway_deps._launch_execution_recovery_worker(
        app,
        manager,  # type: ignore[arg-type]
        record,
        decision_gate,
    )
    await asyncio.sleep(0)

    assert context_calls == 0
    assert factory_calls == 0
    assert worker_calls == []
    assert manager.task is not None

    record.execution_recovery_release_event.set()
    await manager.task

    assert context_calls == 1
    assert factory_calls == 1
    assert len(worker_calls) == 1
    assert events == ["audit", "run"]
    audit.record.assert_awaited_once()
    audit_values = audit.record.await_args.kwargs
    assert audit_values["method"] == "internal_service"
    assert audit_values["action"] == "control"
    assert audit_values["route_category"] == "runtime_recovery"
    assert audit_values["actor_digest"] == worker_calls[0]["ctx"].recovery_executor.digest
    kwargs = worker_calls[0]
    if input_kind == "graph":
        messages = kwargs["graph_input"]["messages"]  # type: ignore[index]
        assert len(messages) == 1
        assert isinstance(messages[0], HumanMessage)
        assert messages[0].content == "recover once"
    else:
        assert isinstance(kwargs["graph_input"], Command)
        assert kwargs["graph_input"].resume == {"approved": True}
    assert kwargs["stream_modes"] == ["messages-tuple"]
    assert kwargs["stream_subgraphs"] is True
    assert kwargs["interrupt_before"] == ["tools"]
    assert kwargs["interrupt_after"] == "*"
    assert len(manager.validated) == 1


def test_profile_manager_options_are_exact_two_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(
        "HARTMESH_EXECUTION_RECOVERY_CLAIMS_ENABLED",
        raising=False,
    )

    async def callback(_record):
        return None

    assert (
        gateway_deps._execution_recovery_manager_options(
            exact_two_profile=False,
            takeover_callback=callback,
        )
        == {}
    )
    exact = gateway_deps._execution_recovery_manager_options(
        exact_two_profile=True,
        takeover_callback=callback,
    )
    assert exact == {
        "on_execution_takeover": callback,
        "admission_recovery_policy": (RecoveryPolicy.exact_two_takeover_v1),
        "execution_recovery_claims_enabled": False,
        "execution_takeover_eligibility": (gateway_deps._exact_two_execution_takeover_eligible),
    }


def test_exact_two_takeover_eligibility_is_unavailable_for_all_runs() -> None:
    assert (
        gateway_deps._exact_two_execution_takeover_eligible(
            SimpleNamespace(
                execution_evidence_json={"provider_kind": "aio_kubernetes"},
            )
        )
        is False
    )
    assert (
        gateway_deps._exact_two_execution_takeover_eligible(
            SimpleNamespace(execution_evidence_json=None),
        )
        is False
    )


def test_exact_two_takeover_eligibility_rejects_unmaterialized_nonempty_skills() -> None:
    record = SimpleNamespace(
        execution_evidence_json=None,
        accepted_invocation=SimpleNamespace(
            agent_revision=SimpleNamespace(
                skill_scopes=SimpleNamespace(
                    scopes={"lead": ("a" * 64,)},
                ),
            ),
        ),
    )

    assert gateway_deps._exact_two_execution_takeover_eligible(record) is False
