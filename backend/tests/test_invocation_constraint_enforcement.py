"""Admission, worker-fence, and exact subagent constraint enforcement."""

from __future__ import annotations

import asyncio
import importlib
from contextlib import asynccontextmanager, contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from types import SimpleNamespace

import pytest
from deerflow_extension_api import (
    ConstraintIndeterminate,
    ConstraintProjectionV1,
    ConstraintRejected,
)

from app.runtime.constraints import ProviderInvocationConstraints
from app.runtime.invocation import (
    DurableAdmission,
    InternalAuthorizationDecision,
    InternalConstraintDecision,
    InternalLaunchIntent,
    InvocationAuthorizationOutcome,
    InvocationRuntime,
    PreparedLaunch,
)
from deerflow.runtime import DisconnectMode, RunRecord, RunStatus
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.constraints import (
    InvocationSubagentDispatchLedger,
    InvocationSubagentReservation,
    SubagentDispatchOutcome,
)
from deerflow.runtime.runs.manager import RunManager
from deerflow.runtime.runs.store.base import (
    AdmissionOutcome,
    LifecycleTransition,
    LifecycleType,
    build_lifecycle_payload,
)
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.tenant_identity import TenantIdentityV1

_TEST_TENANT = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()


def _material() -> ResolvedAgentMaterialV1:
    return ResolvedAgentMaterialV1(
        agent_id="default",
        storage_source="config",
        storage_version="v1",
        agent_config=None,
        soul="steady",
        model_profile={"name": "default"},
        runtime_defaults={
            "subagent_enabled": True,
            "max_concurrent_subagents": 3,
            "max_total_subagents": 6,
        },
    )


def _accepted() -> AcceptedInvocation:
    return AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="owner-1"),
        origin=InvocationOrigin(source_kind="http"),
        thread_id="thread-1",
        context_references={"max_total_subagents": 6},
        agent_revision=ResolvedAgentRevision.from_material(_material()),
        normalized_input={"messages": [{"role": "user", "content": "hello"}]},
        execution_options={},
        extension_generation=4,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
        tenant=_TEST_TENANT,
    )


def _record(*, accepted: AcceptedInvocation | None = None) -> RunRecord:
    now = datetime.now(UTC).isoformat()
    return RunRecord(
        run_id="run-1",
        thread_id="thread-1",
        assistant_id="default",
        status=RunStatus.pending,
        on_disconnect=DisconnectMode.cancel,
        user_id="owner-1",
        created_at=now,
        updated_at=now,
        state_version=1,
        request_digest="a" * 64,
        request_digest_version="sha256-canonical-json-v1",
        accepted_invocation=accepted,
    )


class _Normalizer:
    def __init__(self, events: list[str], *, existing: bool = False) -> None:
        self.events = events
        self.existing = existing

    @contextmanager
    def scope(self, _intent):
        yield

    async def identify(self, _intent):
        if not self.existing:
            return None
        from app.runtime.invocation import InternalAdmissionIdentity, InvocationPrincipal

        return InternalAdmissionIdentity(
            external_scope="http:v1:sha256:scope",
            external_key="raw:key",
            principal_digest="b" * 64,
            base_origin_digest="c" * 64,
            thread_id="thread-1",
            requested_agent_id="default",
            user_id="owner-1",
            principal=InvocationPrincipal(user_id="owner-1"),
        )

    async def validate_replay(self, *_args):
        self.events.append("replay")

    async def normalize(self, _intent):
        self.events.append("normalize")

        async def worker(_record):
            self.events.append("worker")

        return PreparedLaunch(
            thread_id="thread-1",
            assistant_id="default",
            on_disconnect=DisconnectMode.cancel,
            metadata={},
            kwargs={},
            multitask_strategy="reject",
            model_name=None,
            user_id="owner-1",
            worker=worker,
            accepted_invocation=_accepted(),
            request_digest="a" * 64,
            request_digest_version="sha256-canonical-json-v1",
        )


class _Runs:
    def __init__(self, events: list[str], *, existing: bool = False) -> None:
        self.events = events
        self.record = _record(accepted=_accepted())
        self.existing = existing
        self.admitted_launch = None

    @asynccontextmanager
    async def admission_scope(self, _thread_id):
        yield

    async def prepare_admission(self, _launch):
        self.events.append("prepare")

    async def admit(self, launch, *, candidate_run_id):
        self.events.append("admit")
        self.admitted_launch = launch
        self.record.run_id = candidate_run_id
        return DurableAdmission(self.record, AdmissionOutcome.created)

    async def attach_worker(self, record, worker, task_factory):
        record.task = task_factory(worker)
        return record.task

    async def find_by_external_identity(self, _identity):
        self.events.append("lookup")
        return self.record if self.existing else None

    async def observe(self, *_args):
        return self.record

    async def fail_start(self, *_args):
        raise AssertionError("not expected")

    async def cancel(self, *_args):
        raise AssertionError("not expected")


class _Authorization:
    async def authorize_start(self, _launch):
        return InternalAuthorizationDecision.allowed(evidence={"version": 1, "decisions": [{"policy_id": "allow"}]})

    async def authorize_observe(self, *_args, **_kwargs):
        return InternalAuthorizationDecision.allowed()

    async def authorize_cancel(self, *_args):
        return InternalAuthorizationDecision.allowed()


class _Constraints:
    def __init__(self, events: list[str], decision: InternalConstraintDecision) -> None:
        self.events = events
        self.decision = decision

    async def project(self, _launch):
        self.events.append("constraints")
        return self.decision


def _constraint_evidence(limit: int = 2) -> dict:
    now = datetime(2026, 8, 7, 12, tzinfo=UTC)
    projection = ConstraintProjectionV1(
        request_digest="a" * 64,
        agent_revision_digest=_accepted().agent_revision.digest,
        projection_revision="policy-7",
        issued_at=now,
        valid_until=now + timedelta(minutes=5),
        evidence_id="evidence-7",
        evidence_digest="d" * 64,
        max_total_subagents=limit,
    )
    return InternalConstraintDecision.projected(projection).evidence


@pytest.mark.asyncio
async def test_constraints_run_after_authorization_and_before_atomic_admission() -> None:
    events: list[str] = []
    runs = _Runs(events)

    def attach_discarded(worker):
        worker.close()
        return asyncio.create_task(asyncio.sleep(0))

    runtime = InvocationRuntime(
        normalizer=_Normalizer(events),
        runs=runs,
        authorization=_Authorization(),
        constraints=_Constraints(events, InternalConstraintDecision.projected_evidence(_constraint_evidence())),
        task_factory=attach_discarded,
    )

    receipt = await runtime.launch(InternalLaunchIntent(thread_id="thread-1"))

    assert receipt.created is True
    assert events == ["normalize", "constraints", "prepare", "admit"]
    evidence = runs.admitted_launch.accepted_invocation.decision_evidence
    assert evidence["decisions"] == ({"policy_id": "allow"},)
    assert evidence["constraints"]["max_total_subagents"] == 2
    await receipt.record.task


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "decision",
    [InternalConstraintDecision.denied(), InternalConstraintDecision.indeterminate()],
)
async def test_constraint_rejection_or_uncertainty_stops_before_acceptance(decision) -> None:
    events: list[str] = []
    runs = _Runs(events)
    runtime = InvocationRuntime(
        normalizer=_Normalizer(events),
        runs=runs,
        authorization=_Authorization(),
        constraints=_Constraints(events, decision),
    )

    result = await runtime.launch(InternalLaunchIntent(thread_id="thread-1"))

    expected = InvocationAuthorizationOutcome.denied if decision.outcome.value == "denied" else InvocationAuthorizationOutcome.indeterminate
    assert result is expected
    assert events == ["normalize", "constraints"]
    assert runs.admitted_launch is None


@pytest.mark.asyncio
async def test_known_replay_bypasses_constraint_projection_even_when_stored_expired() -> None:
    events: list[str] = []
    runs = _Runs(events, existing=True)
    constraints = _Constraints(events, InternalConstraintDecision.indeterminate())
    runtime = InvocationRuntime(
        normalizer=_Normalizer(events, existing=True),
        runs=runs,
        authorization=_Authorization(),
        constraints=constraints,
    )

    receipt = await runtime.launch(InternalLaunchIntent(thread_id="thread-1"))

    assert receipt.created is False
    assert "constraints" not in events
    assert events == ["lookup", "replay"]


@pytest.mark.asyncio
async def test_reservation_is_concurrency_safe_nested_and_retry_idempotent() -> None:
    reservation = InvocationSubagentReservation(2)
    results = await asyncio.gather(*(asyncio.to_thread(reservation.reserve, dispatch_id) for dispatch_id in ("a", "b", "c")))
    assert sum(results) == 2
    accepted_ids = [dispatch_id for dispatch_id, allowed in zip(("a", "b", "c"), results) if allowed]
    assert reservation.reserve(accepted_ids[0]) is True
    nested_reference = reservation
    assert nested_reference is reservation
    assert nested_reference.reserve("nested") is False
    assert reservation.reserved == 2


@pytest.mark.asyncio
async def test_dispatch_ledger_converges_equal_calls_and_freezes_terminal_result() -> None:
    ledger = InvocationSubagentDispatchLedger(1)
    digest = "a" * 64
    tickets = await asyncio.gather(*(asyncio.to_thread(ledger.acquire, "dispatch-1", digest) for _ in range(8)))
    winners = [ticket for ticket in tickets if ticket.outcome is SubagentDispatchOutcome.new]
    replays = [ticket for ticket in tickets if ticket.outcome is SubagentDispatchOutcome.replay]
    assert len(winners) == 1
    assert len(replays) == 7
    assert ledger.reserved == 1

    mutable = {"messages": [{"content": "accepted"}]}
    ledger.complete(winners[0], mutable)
    mutable["messages"][0]["content"] = "mutated"
    first = await ledger.replay_result(replays[0])
    second = await ledger.replay_result(replays[1])
    first["messages"][0]["content"] = "caller-mutated"
    assert second == {"messages": [{"content": "accepted"}]}


def test_dispatch_ledger_rejects_conflicts_exhaustion_and_zero_ceiling() -> None:
    ledger = InvocationSubagentDispatchLedger(1)
    assert ledger.acquire("dispatch-1", "a" * 64).outcome is SubagentDispatchOutcome.new
    assert ledger.acquire("dispatch-1", "b" * 64).outcome is SubagentDispatchOutcome.conflict
    assert ledger.acquire("dispatch-2", "c" * 64).outcome is SubagentDispatchOutcome.exhausted
    assert (
        InvocationSubagentDispatchLedger(0)
        .acquire(
            "dispatch-1",
            "a" * 64,
        )
        .outcome
        is SubagentDispatchOutcome.exhausted
    )


@pytest.mark.asyncio
async def test_dispatch_ledger_cleanup_cancels_inflight_replay_idempotently() -> None:
    ledger = InvocationSubagentDispatchLedger(1)
    ledger.acquire("dispatch-1", "a" * 64)
    replay = ledger.acquire("dispatch-1", "a" * 64)

    ledger.close()
    ledger.close()

    with pytest.raises(asyncio.CancelledError):
        await ledger.replay_result(replay)


def test_caller_cannot_forge_constraint_projection_or_reservation() -> None:
    from deerflow.runtime.constraints import (
        INVOCATION_CONSTRAINTS_CONTEXT_KEY,
        SUBAGENT_RESERVATION_CONTEXT_KEY,
    )
    from deerflow.runtime.runs.worker import _build_runtime_context

    context = _build_runtime_context(
        "thread-1",
        "run-1",
        {
            INVOCATION_CONSTRAINTS_CONTEXT_KEY: "forged",
            SUBAGENT_RESERVATION_CONTEXT_KEY: InvocationSubagentReservation(1),
        },
    )

    assert INVOCATION_CONSTRAINTS_CONTEXT_KEY not in context
    assert SUBAGENT_RESERVATION_CONTEXT_KEY not in context


@pytest.mark.parametrize(
    "reason",
    ["constraint_evidence_mismatch", "constraint_expired_before_start"],
)
def test_constraint_fence_reasons_are_safe_lifecycle_evidence(reason: str) -> None:
    assert build_lifecycle_payload(
        LifecycleTransition(
            lifecycle_type=LifecycleType.failed,
            status="error",
            stop_reason=reason,
            reason=reason,
        )
    ) == {"version": 1, "reason": reason}


@pytest.mark.asyncio
async def test_task_dispatch_reserves_exactly_once_and_rejects_the_excess(
    monkeypatch,
) -> None:
    from deerflow.runtime.constraints import (
        INVOCATION_CONSTRAINTS_CONTEXT_KEY,
        SUBAGENT_RESERVATION_CONTEXT_KEY,
    )
    from deerflow.subagents.config import SubagentConfig

    module = importlib.import_module("deerflow.tools.builtins.task_tool")
    now = datetime(2026, 8, 7, 12, tzinfo=UTC)
    projection = ConstraintProjectionV1(
        request_digest="a" * 64,
        agent_revision_digest=_accepted().agent_revision.digest,
        projection_revision="policy-7",
        issued_at=now,
        valid_until=now + timedelta(minutes=5),
        evidence_id="evidence-7",
        evidence_digest="d" * 64,
        max_total_subagents=1,
    )
    reservation = InvocationSubagentReservation(1)
    runtime = SimpleNamespace(
        state={},
        context={
            "thread_id": "thread-1",
            "accepted_extension_generation": 4,
            INVOCATION_CONSTRAINTS_CONTEXT_KEY: projection,
            SUBAGENT_RESERVATION_CONTEXT_KEY: reservation,
        },
        config={"metadata": {"model_name": "fixed-model"}},
    )
    starts: list[str] = []
    captured: list[dict] = []

    class _Executor:
        def __init__(self, **kwargs):
            captured.append(kwargs)

        def execute_async(self, _prompt, task_id=None):
            starts.append(task_id)
            return task_id

    class _Status(Enum):
        PENDING = "pending"
        RUNNING = "running"
        COMPLETED = "completed"
        FAILED = "failed"
        CANCELLED = "cancelled"
        TIMED_OUT = "timed_out"

    result = SimpleNamespace(
        status=_Status.COMPLETED,
        ai_messages=[],
        result="done",
        error=None,
        stop_reason=None,
        token_usage_records=[],
        usage_reported=False,
    )

    async def _emit(*_args, **_kwargs):
        return None

    monkeypatch.setattr(module, "SubagentExecutor", _Executor)
    monkeypatch.setattr(module, "SubagentStatus", _Status)
    monkeypatch.setattr(
        module,
        "get_subagent_config",
        lambda _name: SubagentConfig(
            name="general-purpose",
            description="helper",
            model="fixed-model",
        ),
    )
    monkeypatch.setattr(module, "get_available_subagent_names", lambda: ["general-purpose"])
    monkeypatch.setattr(module, "get_background_task_result", lambda _task_id: result)
    monkeypatch.setattr(module, "get_stream_writer", lambda: lambda _event: None)
    monkeypatch.setattr(module, "aemit_custom_event", _emit)
    monkeypatch.setattr(module, "_report_subagent_usage", lambda *_args: None)
    monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **_kwargs: [])
    coroutine = module.task_tool.coroutine

    first = await coroutine(
        runtime=runtime,
        description="first",
        prompt="work",
        subagent_type="general-purpose",
        tool_call_id="dispatch-1",
    )
    excess = await coroutine(
        runtime=runtime,
        description="second",
        prompt="work",
        subagent_type="general-purpose",
        tool_call_id="dispatch-2",
    )
    retry = await coroutine(
        runtime=runtime,
        description="retry",
        prompt="work",
        subagent_type="general-purpose",
        tool_call_id="dispatch-1",
    )
    conflict = await coroutine(
        runtime=runtime,
        description="changed",
        prompt="different work",
        subagent_type="general-purpose",
        tool_call_id="dispatch-1",
    )

    assert first.update["messages"][0].content == "Task Succeeded. Result: done"
    assert "limit (1) reached" in excess.update["messages"][0].content
    assert retry.update["messages"][0].content == "Task Succeeded. Result: done"
    assert "different intent" in conflict.update["messages"][0].content
    assert starts == ["dispatch-1"]
    assert reservation.reserved == 1
    assert captured[0]["invocation_constraints"] is projection
    assert captured[0]["subagent_reservation"] is reservation
    assert captured[0]["accepted_extension_generation"] == 4


@pytest.mark.asyncio
async def test_task_dispatch_inflight_equal_replay_waits_for_one_physical_start(
    monkeypatch,
) -> None:
    from deerflow.runtime.constraints import SUBAGENT_RESERVATION_CONTEXT_KEY
    from deerflow.subagents.config import SubagentConfig

    module = importlib.import_module("deerflow.tools.builtins.task_tool")
    ledger = InvocationSubagentDispatchLedger(1)
    runtime = SimpleNamespace(
        state={},
        context={
            "thread_id": "thread-1",
            SUBAGENT_RESERVATION_CONTEXT_KEY: ledger,
        },
        config={"metadata": {"model_name": "fixed-model"}},
    )
    starts: list[str] = []
    started = asyncio.Event()
    release = asyncio.Event()

    class _Executor:
        def __init__(self, **_kwargs):
            pass

        def execute_async(self, _prompt, task_id=None):
            starts.append(task_id)
            started.set()
            return task_id

    class _Status(Enum):
        RUNNING = "running"
        COMPLETED = "completed"
        FAILED = "failed"
        CANCELLED = "cancelled"
        TIMED_OUT = "timed_out"

    result = SimpleNamespace(
        status=_Status.RUNNING,
        ai_messages=[],
        result="done",
        error=None,
        stop_reason=None,
        token_usage_records=[],
        usage_reported=False,
    )

    async def _emit(*_args, **_kwargs):
        return None

    async def _wait_for_release(_seconds):
        await release.wait()
        result.status = _Status.COMPLETED

    monkeypatch.setattr(module, "SubagentExecutor", _Executor)
    monkeypatch.setattr(module, "SubagentStatus", _Status)
    monkeypatch.setattr(
        module,
        "get_subagent_config",
        lambda _name: SubagentConfig(
            name="general-purpose",
            description="helper",
            model="fixed-model",
        ),
    )
    monkeypatch.setattr(
        module,
        "get_available_subagent_names",
        lambda: ["general-purpose"],
    )
    monkeypatch.setattr(
        module,
        "get_background_task_result",
        lambda _task_id: result,
    )
    monkeypatch.setattr(module, "get_stream_writer", lambda: lambda _event: None)
    monkeypatch.setattr(module, "aemit_custom_event", _emit)
    monkeypatch.setattr(module, "_report_subagent_usage", lambda *_args: None)
    monkeypatch.setattr(module.asyncio, "sleep", _wait_for_release)
    monkeypatch.setattr("deerflow.tools.get_available_tools", lambda **_kwargs: [])

    coroutine = module.task_tool.coroutine
    first = asyncio.create_task(
        coroutine(
            runtime=runtime,
            description="first",
            prompt="same work",
            subagent_type="general-purpose",
            tool_call_id="dispatch-1",
        )
    )
    await started.wait()
    replay = asyncio.create_task(
        coroutine(
            runtime=runtime,
            description="redelivery",
            prompt="same work",
            subagent_type="general-purpose",
            tool_call_id="dispatch-1",
        )
    )
    await asyncio.tasks.sleep(0)
    assert starts == ["dispatch-1"]
    assert replay.done() is False

    release.set()
    first_result, replay_result = await asyncio.gather(first, replay)
    assert starts == ["dispatch-1"]
    assert first_result == replay_result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("host_result", "expected"),
    [
        (ConstraintRejected(), "denied"),
        (ConstraintIndeterminate(), "indeterminate"),
        (None, "allowed"),
    ],
)
async def test_production_adapter_maps_the_strict_union_without_binary_authority(
    host_result,
    expected,
) -> None:
    seen = {}

    class _Host:
        async def project(self, request, **kwargs):
            seen["request"] = request
            seen.update(kwargs)
            return host_result

    launch = await _Normalizer([]).normalize(InternalLaunchIntent(thread_id="thread-1"))
    decision = await ProviderInvocationConstraints(_Host()).project(launch)

    assert decision.outcome.value == expected
    assert seen["request"].request_digest == "a" * 64
    assert seen["request"].agent_revision_digest == launch.accepted_invocation.agent_revision.digest
    assert seen["host_max_total_subagents"] == 6


@pytest.mark.asyncio
async def test_production_adapter_persists_only_normalized_effective_projection() -> None:
    now = datetime(2026, 8, 7, 12, tzinfo=UTC)
    projection = ConstraintProjectionV1(
        request_digest="a" * 64,
        agent_revision_digest=_accepted().agent_revision.digest,
        projection_revision="policy-7",
        issued_at=now,
        valid_until=now + timedelta(minutes=5),
        evidence_id="evidence-7",
        evidence_digest="d" * 64,
        max_total_subagents=2,
    )

    class _Host:
        async def project(self, *_args, **_kwargs):
            return projection

    launch = await _Normalizer([]).normalize(InternalLaunchIntent(thread_id="thread-1"))
    decision = await ProviderInvocationConstraints(_Host()).project(launch)

    persisted = decision.evidence["constraints"]
    assert persisted["max_total_subagents"] == 2
    assert persisted["evidence_id"] == "evidence-7"
    assert persisted["evidence_digest"] == "d" * 64
    assert "provider_config" not in persisted
    assert "credentials" not in persisted


def _accepted_with_constraints(
    *,
    now: datetime,
    valid_until: datetime,
    limit: int = 2,
) -> AcceptedInvocation:
    accepted = _accepted()
    projection = ConstraintProjectionV1(
        request_digest="a" * 64,
        agent_revision_digest=accepted.agent_revision.digest,
        projection_revision="policy-7",
        issued_at=now,
        valid_until=valid_until,
        evidence_id="evidence-7",
        evidence_digest="d" * 64,
        max_total_subagents=limit,
    )
    return replace(
        accepted,
        decision_evidence=InternalConstraintDecision.projected(projection).evidence,
    )


def _bridge():
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    return SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_construction_fence_rejects_evidence_mismatch_before_graph_work() -> None:
    now = datetime(2026, 8, 7, 12, tzinfo=UTC)
    accepted = _accepted_with_constraints(
        now=now,
        valid_until=now + timedelta(minutes=5),
    )
    tampered = dict(accepted.decision_evidence)
    tampered_constraints = dict(tampered["constraints"])
    tampered_constraints["evidence_id"] = "forged"
    tampered["constraints"] = tampered_constraints
    accepted = replace(accepted, decision_evidence=tampered)
    manager = RunManager(tenant=_TEST_TENANT)
    record = await manager.create_or_reject("thread-1", accepted_invocation=accepted)
    factory_calls = 0

    def factory(*, config):
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("graph construction must not run")

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            constraint_clock=lambda: now,
            tenant=_TEST_TENANT,
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert factory_calls == 0
    assert record.status is RunStatus.error
    assert record.stop_reason == "constraint_evidence_mismatch"


@pytest.mark.asyncio
async def test_queue_expiry_fails_before_graph_construction() -> None:
    issued = datetime(2026, 8, 7, 12, tzinfo=UTC)
    accepted = _accepted_with_constraints(
        now=issued,
        valid_until=issued + timedelta(seconds=10),
    )
    manager = RunManager(tenant=_TEST_TENANT)
    record = await manager.create_or_reject("thread-1", accepted_invocation=accepted)
    factory_calls = 0

    def factory(*, config):
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("graph construction must not run")

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            constraint_clock=lambda: issued + timedelta(seconds=11),
            tenant=_TEST_TENANT,
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert factory_calls == 0
    assert record.stop_reason == "constraint_expired_before_start"


@pytest.mark.asyncio
async def test_expiry_between_construction_and_astream_starts_no_graph() -> None:
    issued = datetime(2026, 8, 7, 12, tzinfo=UTC)
    current = [issued]
    accepted = _accepted_with_constraints(
        now=issued,
        valid_until=issued + timedelta(seconds=10),
    )
    manager = RunManager(tenant=_TEST_TENANT)
    record = await manager.create_or_reject("thread-1", accepted_invocation=accepted)
    stream_calls = 0

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            nonlocal stream_calls
            stream_calls += 1
            yield {"messages": []}

    def factory(*, config):
        current[0] = issued + timedelta(seconds=11)
        return _Agent()

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            constraint_clock=lambda: current[0],
            tenant=_TEST_TENANT,
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert stream_calls == 0
    assert record.stop_reason == "constraint_expired_before_start"


@pytest.mark.asyncio
async def test_ordinary_projection_installs_effective_limit_and_starts_once() -> None:
    from deerflow.runtime.constraints import (
        INVOCATION_CONSTRAINTS_CONTEXT_KEY,
        SUBAGENT_RESERVATION_CONTEXT_KEY,
    )

    now = datetime(2026, 8, 7, 12, tzinfo=UTC)
    accepted = _accepted_with_constraints(
        now=now,
        valid_until=now + timedelta(minutes=5),
    )
    manager = RunManager(tenant=_TEST_TENANT)
    record = await manager.create_or_reject("thread-1", accepted_invocation=accepted)
    stream_calls = 0
    seen = {}

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            nonlocal stream_calls
            stream_calls += 1
            yield {"messages": []}

    def factory(*, config):
        seen.update(config["context"])
        return _Agent()

    await run_agent(
        _bridge(),
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            constraint_clock=lambda: now,
            tenant=_TEST_TENANT,
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )

    assert stream_calls == 1
    assert record.status is RunStatus.success
    assert seen["max_total_subagents"] == 2
    assert seen[INVOCATION_CONSTRAINTS_CONTEXT_KEY].max_total_subagents == 2
    reservation = seen[SUBAGENT_RESERVATION_CONTEXT_KEY]
    assert reservation.limit == 2
