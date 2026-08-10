"""Durable invocation qualification across process and worker failure seams.

The required PostgreSQL session/interleaving tests are marked
``postgres_contract``. When ``DEERFLOW_TEST_POSTGRES_URL`` is present,
``tests/conftest.py`` turns any marked skip into a failed session. Without the
variable, every skip is an unpassed release gate. Kubernetes pod termination
remains a documented release gate; the offline process-loss tests below do not
operate a cluster.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import ConstraintProjectionV1
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from support.postgres import postgres_async_url

from app.runtime.idempotency import REQUEST_DIGEST_VERSION, CanonicalCallerIntent, canonical_request_digest, normalize_external_key, scope_for_http
from app.runtime.invocation import (
    DurableAdmission,
    InternalAdmissionIdentity,
    InternalConstraintDecision,
    InternalLaunchIntent,
    InvocationPrincipal,
    InvocationRuntime,
    PreparedLaunch,
)
from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.persistence.base import Base
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime import DisconnectMode, RunManager, RunStatus
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentMaterialV1,
    ResolvedAgentRevision,
    canonical_digest,
)
from deerflow.runtime.runs.lifecycle_query import LifecycleQuery
from deerflow.runtime.runs.manager import ORPHAN_RECOVERY_STOP_REASON, ConflictError
from deerflow.runtime.runs.store.base import AdmissionOutcome, LifecycleType, lifecycle_owner_scope
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.runs.worker import RunContext, run_agent

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")


class _TemporarilyUnavailableRunRepository:
    """Fault gate around the production repository's terminal CAS operations."""

    durable_lifecycle = True

    def __init__(self, inner: RunRepository) -> None:
        self._inner = inner
        self.available = True

    def __getattr__(self, name: str):
        return getattr(self._inner, name)

    def _require_available(self) -> None:
        if not self.available:
            raise OSError("test-only PostgreSQL terminalization outage")

    async def get(self, *args, **kwargs):
        self._require_available()
        return await self._inner.get(*args, **kwargs)

    async def request_cancel_compat(self, *args, **kwargs):
        self._require_available()
        return await self._inner.request_cancel_compat(*args, **kwargs)

    async def request_cancel_owned(self, *args, **kwargs):
        self._require_available()
        return await self._inner.request_cancel_owned(*args, **kwargs)

    async def transition_run_atomic(self, *args, **kwargs):
        self._require_available()
        return await self._inner.transition_run_atomic(*args, **kwargs)

    async def transition_owned_run_atomic(self, *args, **kwargs):
        self._require_available()
        return await self._inner.transition_owned_run_atomic(*args, **kwargs)


class _Normalizer:
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts

    def scope(self, _intent):
        return nullcontext()

    async def identify(self, intent: InternalLaunchIntent) -> InternalAdmissionIdentity | None:
        if intent.external_key is None:
            return None
        caller_intent = CanonicalCallerIntent({"input": intent.input})
        return InternalAdmissionIdentity(
            external_scope=scope_for_http("user", "owner-1"),
            external_key=normalize_external_key(intent.external_key),
            principal_digest="a" * 64,
            base_origin_digest="b" * 64,
            thread_id=intent.thread_id,
            requested_agent_id="lead_agent",
            caller_intent=caller_intent,
            user_id="owner-1",
            principal=InvocationPrincipal(user_id="owner-1"),
        )

    async def normalize(self, intent: InternalLaunchIntent) -> PreparedLaunch:
        self.counts["normalizations"] += 1
        caller_intent = CanonicalCallerIntent({"input": intent.input})

        async def worker(_record) -> None:
            self.counts["worker_bodies"] += 1

        return PreparedLaunch(
            thread_id=intent.thread_id,
            assistant_id="lead_agent",
            on_disconnect=DisconnectMode.cancel,
            metadata={},
            kwargs={},
            multitask_strategy=intent.multitask_strategy,
            model_name=None,
            user_id="owner-1",
            worker=worker,
            external_scope=scope_for_http("user", "owner-1"),
            external_key=normalize_external_key(intent.external_key or ""),
            request_digest=canonical_request_digest({"input": intent.input}),
            request_digest_version=REQUEST_DIGEST_VERSION,
            caller_intent_json=caller_intent.to_persisted(),
            caller_intent_digest=caller_intent.digest,
            caller_intent_digest_version=caller_intent.digest_version,
            principal=InvocationPrincipal(user_id="owner-1"),
        )

    async def validate_replay(
        self,
        intent: InternalLaunchIntent,
        _identity: InternalAdmissionIdentity,
        record,
    ) -> None:
        caller_intent = CanonicalCallerIntent({"input": intent.input})
        if record.caller_intent_digest != caller_intent.digest:
            from deerflow.runtime.runs.manager import IdempotencyConflictError

            raise IdempotencyConflictError("request digest differs")


class _DurableRuns:
    def __init__(self, manager: RunManager) -> None:
        self.manager = manager

    @asynccontextmanager
    async def admission_scope(self, _thread_id: str):
        yield

    async def prepare_admission(self, _launch: PreparedLaunch) -> None:
        return None

    async def admit(
        self,
        launch: PreparedLaunch,
        *,
        candidate_run_id: str,
    ) -> DurableAdmission:
        admission = await self.manager.ensure_or_reject(
            launch.thread_id,
            launch.assistant_id,
            candidate_run_id=candidate_run_id,
            external_scope=launch.external_scope or "",
            external_key=launch.external_key or "",
            request_digest=launch.request_digest or "",
            request_digest_version=launch.request_digest_version or "",
            caller_intent_json=launch.caller_intent_json or {},
            caller_intent_digest=launch.caller_intent_digest or "",
            caller_intent_digest_version=launch.caller_intent_digest_version or "",
            on_disconnect=launch.on_disconnect,
            multitask_strategy=launch.multitask_strategy,
            user_id=launch.user_id,
        )
        return DurableAdmission(admission.record, admission.outcome)

    async def find_by_external_identity(self, identity: InternalAdmissionIdentity):
        return await self.manager.get_by_external_identity(
            identity.external_scope,
            identity.external_key,
            user_id=identity.user_id,
        )

    async def observe(self, run_id: str, principal: InvocationPrincipal):
        return await self.manager.get(run_id, user_id=principal.user_id)

    async def fail_start(self, record, error: str) -> None:
        await self.manager.fail_start_if_pending(record.run_id, error=error)

    async def attach_worker(self, record, worker, task_factory):
        return await self.manager.attach_worker_once(
            record.run_id,
            worker,
            task_factory,
        )


class _ProcessLostAfterAdmission(BaseException):
    """Simulate a process-fatal exit that bypasses application cleanup."""


class _LoseAfterAdmissionRuns(_DurableRuns):
    def __init__(self, manager: RunManager) -> None:
        super().__init__(manager)
        self.committed: DurableAdmission | None = None

    async def admit(
        self,
        launch: PreparedLaunch,
        *,
        candidate_run_id: str,
    ) -> DurableAdmission:
        self.committed = await super().admit(
            launch,
            candidate_run_id=candidate_run_id,
        )
        raise _ProcessLostAfterAdmission


class _TwoPartyBarrier:
    def __init__(self) -> None:
        self._arrivals = 0
        self._lock = asyncio.Lock()
        self._ready = asyncio.Event()

    async def arrive(self) -> None:
        async with self._lock:
            self._arrivals += 1
            if self._arrivals == 2:
                self._ready.set()
        await asyncio.wait_for(self._ready.wait(), timeout=10)


class _PreflightBarrierRepository(RunRepository):
    def __init__(self, session_factory, barrier: _TwoPartyBarrier) -> None:
        super().__init__(session_factory)
        self._barrier = barrier
        self.external_identity_lookups = 0

    async def get_by_external_identity(self, external_scope: str, external_key: str):
        self.external_identity_lookups += 1
        row = await super().get_by_external_identity(external_scope, external_key)
        if self.external_identity_lookups == 1:
            assert row is None
            await self._barrier.arrive()
        return row


@pytest.mark.anyio
async def test_response_loss_after_commit_reconstructs_one_row_and_never_reattaches_worker(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'response-loss.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    counts = {"attachments": 0, "worker_bodies": 0, "normalizations": 0}

    def attach(coroutine):
        counts["attachments"] += 1
        return asyncio.create_task(coroutine)

    intent = InternalLaunchIntent(
        thread_id="thread-1",
        input={"messages": [{"role": "user", "content": "hello"}]},
        external_key="delivery-1",
    )
    try:
        first_manager = RunManager(store=RunRepository(factory))
        first_runtime = InvocationRuntime(
            normalizer=_Normalizer(counts),
            runs=_DurableRuns(first_manager),
            task_factory=attach,
        )
        committed = await first_runtime.launch(intent)
        assert committed.created is True
        assert committed.record.task is not None
        await committed.record.task

        # The caller loses the receipt and a reconstructed process retries.
        second_manager = RunManager(store=RunRepository(factory))
        second_runtime = InvocationRuntime(
            normalizer=_Normalizer(counts),
            runs=_DurableRuns(second_manager),
            task_factory=attach,
        )
        replay = await second_runtime.launch(intent)

        assert replay.created is False
        assert replay.record.run_id == committed.record.run_id
        assert counts == {"attachments": 1, "worker_bodies": 1, "normalizations": 1}
        page = await second_manager.query_lifecycle(
            LifecycleQuery(
                run_id=committed.record.run_id,
                owner_scope=lifecycle_owner_scope("owner-1"),
            )
        )
        assert [snapshot["run_id"] for snapshot in page.snapshots] == [committed.record.run_id]
        assert [event["lifecycle_type"] for event in page.events] == [LifecycleType.accepted]
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_process_loss_after_acceptance_reconstructs_and_recovers_without_attachment(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'accepted-loss.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    counts = {"attachments": 0, "worker_bodies": 0, "normalizations": 0}

    def forbidden_attach(_coroutine):
        counts["attachments"] += 1
        raise AssertionError("a worker must not attach after the simulated process loss")

    intent = InternalLaunchIntent(
        thread_id="thread-accepted-loss",
        input={"messages": [{"role": "user", "content": "hello"}]},
        external_key="accepted-loss",
    )
    first_manager = RunManager(store=RunRepository(factory))
    lost_runs = _LoseAfterAdmissionRuns(first_manager)
    runtime = InvocationRuntime(
        normalizer=_Normalizer(counts),
        runs=lost_runs,
        task_factory=forbidden_attach,
    )
    try:
        with pytest.raises(_ProcessLostAfterAdmission):
            await runtime.launch(intent)

        assert lost_runs.committed is not None
        run_id = lost_runs.committed.record.run_id
        assert lost_runs.committed.outcome is AdmissionOutcome.created
        assert lost_runs.committed.record.task is None
        assert counts == {"attachments": 0, "worker_bodies": 0, "normalizations": 1}

        reconstructed = RunManager(store=RunRepository(factory))
        recovered = await reconstructed.reconcile_orphaned_inflight_runs(error="worker disappeared")
        assert [record.run_id for record in recovered] == [run_id]
        assert recovered[0].task is None

        replay_runtime = InvocationRuntime(
            normalizer=_Normalizer(counts),
            runs=_DurableRuns(reconstructed),
            task_factory=forbidden_attach,
        )
        replay = await replay_runtime.launch(intent)
        assert replay.created is False
        assert replay.record.run_id == run_id
        assert replay.record.task is None
        assert counts == {"attachments": 0, "worker_bodies": 0, "normalizations": 1}

        page = await reconstructed.query_lifecycle(
            LifecycleQuery(
                run_id=run_id,
                owner_scope=lifecycle_owner_scope("owner-1"),
            )
        )
        assert [event["lifecycle_type"] for event in page.events] == [
            LifecycleType.accepted,
            LifecycleType.failed,
        ]
        assert page.events[-1]["payload"]["reason"] == ORPHAN_RECOVERY_STOP_REASON
    finally:
        await engine.dispose()


def _material(*, soul: str = "steady") -> ResolvedAgentMaterialV1:
    return ResolvedAgentMaterialV1(
        agent_id="lead_agent",
        storage_source="file",
        storage_version="v1",
        agent_config={"name": "lead_agent"},
        soul=soul,
        model_profile={"name": "default", "model": "offline"},
        runtime_defaults={
            "thinking_enabled": False,
            "reasoning_effort": None,
            "is_plan_mode": False,
            "subagent_enabled": False,
            "max_concurrent_subagents": 1,
            "max_total_subagents": 2,
        },
    )


def _accepted(material: ResolvedAgentMaterialV1) -> AcceptedInvocation:
    return AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="owner-1"),
        origin=InvocationOrigin(source_kind="http"),
        thread_id="thread-worker",
        context_references={},
        agent_revision=ResolvedAgentRevision.from_material(material),
        normalized_input={},
        execution_options={},
        extension_generation=7,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
    )


def _bridge() -> SimpleNamespace:
    return SimpleNamespace(publish=AsyncMock(), publish_end=AsyncMock(), cleanup=AsyncMock())


@pytest.mark.anyio
async def test_process_loss_during_execution_is_fenced_before_stale_completion() -> None:
    store = MemoryRunStore()
    ownership = RunOwnershipConfig(
        heartbeat_enabled=True,
        lease_seconds=30,
        grace_seconds=0,
    )
    owner = RunManager(
        store=store,
        worker_id="worker-owner",
        run_ownership_config=ownership,
    )
    peer = RunManager(
        store=store,
        worker_id="worker-peer",
        run_ownership_config=ownership,
    )
    record = await owner.create_or_reject(
        "thread-execution-loss",
        accepted_invocation=_accepted(_material()),
    )
    stream_started = asyncio.Event()
    release_stream = asyncio.Event()
    counts = {"attachments": 0, "graph": 0, "astream": 0, "model": 0}

    class BlockingAgent:
        async def astream(self, *_args, **_kwargs):
            counts["astream"] += 1
            stream_started.set()
            await release_stream.wait()
            yield {"messages": []}

    def factory(*, config):
        assert config["context"]
        counts["graph"] += 1
        return BlockingAgent()

    counts["attachments"] += 1
    worker = asyncio.create_task(
        run_agent(
            _bridge(),
            owner,
            record,
            ctx=RunContext(checkpointer=None),
            agent_factory=factory,
            graph_input={},
            config={},
        )
    )
    await asyncio.wait_for(stream_started.wait(), timeout=1)
    expired = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    record.lease_expires_at = expired
    store._runs[record.run_id]["lease_expires_at"] = expired

    recovered = await peer.reconcile_orphaned_inflight_runs(error="worker disappeared")
    assert [item.run_id for item in recovered] == [record.run_id]
    release_stream.set()
    await worker

    row = await store.get(record.run_id)
    events = await store.list_lifecycle_events(run_id=record.run_id)
    assert row is not None
    assert (row["status"], row["stop_reason"]) == ("error", ORPHAN_RECOVERY_STOP_REASON)
    assert events[-1]["lifecycle_type"] is LifecycleType.failed
    assert events[-1]["payload"]["reason"] == ORPHAN_RECOVERY_STOP_REASON
    assert counts == {"attachments": 1, "graph": 1, "astream": 1, "model": 0}


@pytest.mark.anyio
async def test_worker_graph_and_first_astream_counts_hold_across_success_drift_and_expiry() -> None:
    now = datetime(2026, 8, 7, 12, tzinfo=UTC)
    counts = {"graph": 0, "astream": 0, "model": 0}

    class Agent:
        async def astream(self, *_args, **_kwargs):
            counts["astream"] += 1
            yield {"messages": []}

    material = _material()
    ordinary = _accepted(material)
    manager = RunManager(store=MemoryRunStore())
    ordinary_record = await manager.create_or_reject("thread-success", accepted_invocation=ordinary)

    def factory(*, config):
        assert config["context"]
        counts["graph"] += 1
        return Agent()

    await run_agent(
        _bridge(),
        manager,
        ordinary_record,
        ctx=RunContext(checkpointer=None, constraint_clock=lambda: now),
        agent_factory=factory,
        graph_input={},
        config={},
    )
    assert ordinary_record.status is RunStatus.success
    assert counts == {"graph": 1, "astream": 1, "model": 0}

    drifted = replace(ordinary, agent_revision=replace(ordinary.agent_revision, material=None))
    drift_manager = RunManager(store=MemoryRunStore())
    drift_record = await drift_manager.create_or_reject("thread-drift", accepted_invocation=drifted)
    await run_agent(
        _bridge(),
        drift_manager,
        drift_record,
        ctx=RunContext(
            checkpointer=None,
            agent_revision_resolver=lambda _record, _config: ResolvedAgentRevision.from_material(_material(soul="changed")),
            constraint_clock=lambda: now,
        ),
        agent_factory=factory,
        graph_input={},
        config={},
    )
    assert drift_record.stop_reason == "agent_revision_drift"
    assert counts == {"graph": 1, "astream": 1, "model": 0}

    constrained = _accepted(material)
    projection = ConstraintProjectionV1(
        request_digest="a" * 64,
        agent_revision_digest=constrained.agent_revision.digest,
        projection_revision="qualification-v1",
        issued_at=now,
        valid_until=now + timedelta(seconds=5),
        evidence_id="evidence-1",
        evidence_digest="d" * 64,
        max_total_subagents=1,
    )
    constrained = replace(
        constrained,
        decision_evidence=InternalConstraintDecision.projected(projection).evidence or {},
    )
    expiry_manager = RunManager(store=MemoryRunStore())
    expiry_record = await expiry_manager.create_or_reject("thread-expired", accepted_invocation=constrained)
    await run_agent(
        _bridge(),
        expiry_manager,
        expiry_record,
        ctx=RunContext(checkpointer=None, constraint_clock=lambda: now + timedelta(seconds=6)),
        agent_factory=factory,
        graph_input={},
        config={},
    )
    assert expiry_record.stop_reason == "constraint_expired_before_start"
    assert counts == {"graph": 1, "astream": 1, "model": 0}


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for independent-session qualification",
)
async def test_postgres_independent_sessions_force_key_and_thread_arbitration() -> None:
    """Both callers miss preflight before PostgreSQL arbitrates contested inserts."""

    assert _POSTGRES_URL is not None
    database_url = postgres_async_url(_POSTGRES_URL)
    left_engine = create_async_engine(database_url)
    right_engine = create_async_engine(database_url)
    async with left_engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    left_factory = async_sessionmaker(left_engine, expire_on_commit=False)
    right_factory = async_sessionmaker(right_engine, expire_on_commit=False)
    unique = uuid.uuid4().hex
    same_thread = f"same-key-thread-{unique}"
    busy_thread = f"two-key-thread-{unique}"
    scope = scope_for_http("user", f"owner-{unique}")
    digest = canonical_request_digest({"input": "qualification"})
    caller_intent = CanonicalCallerIntent({"input": "qualification"})
    common = {
        "thread_id": same_thread,
        "owner_worker_id": "worker-left",
        "lease_expires_at": None,
        "external_scope": scope,
        "external_key": normalize_external_key(f"same-key-{unique}"),
        "request_digest": digest,
        "request_digest_version": REQUEST_DIGEST_VERSION,
        "caller_intent_json": caller_intent.to_persisted(),
        "caller_intent_digest": caller_intent.digest,
        "caller_intent_digest_version": caller_intent.digest_version,
        "user_id": f"owner-{unique}",
    }
    try:
        same_barrier = _TwoPartyBarrier()
        left = _PreflightBarrierRepository(left_factory, same_barrier)
        right = _PreflightBarrierRepository(right_factory, same_barrier)
        same_results = await asyncio.gather(
            left.ensure_run_atomic(f"same-left-{unique}", **common),
            right.ensure_run_atomic(
                f"same-right-{unique}",
                **{**common, "owner_worker_id": "worker-right"},
            ),
        )
        assert {result.outcome for result in same_results} == {
            AdmissionOutcome.created,
            AdmissionOutcome.known_same,
        }
        assert len({result.row["run_id"] for result in same_results}) == 1
        assert sorted((left.external_identity_lookups, right.external_identity_lookups)) == [1, 2]

        busy_barrier = _TwoPartyBarrier()
        busy_left_store = _PreflightBarrierRepository(left_factory, busy_barrier)
        busy_right_store = _PreflightBarrierRepository(right_factory, busy_barrier)
        busy_left = RunManager(store=busy_left_store, worker_id=f"worker-left-{unique}")
        busy_right = RunManager(store=busy_right_store, worker_id=f"worker-right-{unique}")

        async def distinct(manager: RunManager, suffix: str):
            return await manager.ensure_or_reject(
                busy_thread,
                external_scope=scope,
                external_key=normalize_external_key(f"key-{suffix}-{unique}"),
                request_digest=digest,
                request_digest_version=REQUEST_DIGEST_VERSION,
                caller_intent_json=caller_intent.to_persisted(),
                caller_intent_digest=caller_intent.digest,
                caller_intent_digest_version=caller_intent.digest_version,
                user_id=f"owner-{unique}",
            )

        busy_results = await asyncio.gather(
            distinct(busy_left, "left"),
            distinct(busy_right, "right"),
            return_exceptions=True,
        )
        assert sum(not isinstance(result, Exception) for result in busy_results) == 1
        assert sum(isinstance(result, ConflictError) for result in busy_results) == 1
        # The winner needs only its preflight lookup. The loser performs two
        # additional bounded reads: one to classify the unique-index conflict
        # inside ``ensure_run_atomic`` and one to reconcile the candidate run
        # after the failed atomic admission. Which session wins is deliberately
        # nondeterministic.
        assert sorted(
            (
                busy_left_store.external_identity_lookups,
                busy_right_store.external_identity_lookups,
            )
        ) == [1, 3]
    finally:
        async with left_factory() as session:
            await session.execute(delete(RunRow).where(RunRow.thread_id.in_((same_thread, busy_thread))))
            await session.commit()
        await left_engine.dispose()
        await right_engine.dispose()


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for PostgreSQL compensation qualification",
)
async def test_postgres_cancelled_unattached_candidate_stays_supervised_through_store_outage() -> None:
    """A real PostgreSQL row is terminalized once after exact-candidate recovery."""

    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    inner = RunRepository(factory)
    store = _TemporarilyUnavailableRunRepository(inner)
    manager = RunManager(store=store)
    unique = uuid.uuid4().hex
    candidate_run_id = str(uuid.uuid4())
    try:
        record = await manager.create_or_reject(
            f"thread-cancel-compensation-{unique}",
            candidate_run_id=candidate_run_id,
            user_id=f"owner-{unique}",
        )
        store.available = False
        assert await manager.cancel_start_if_pending(record.run_id) is True

        assert record.status is RunStatus.pending
        assert record.task is None
        assert record.attachment_supervised is True
        assert manager.admission_compensations_ready() is False

        store.available = True
        assert await manager.drain_admission_compensations(timeout=5) is True
        durable = await inner.get(candidate_run_id, user_id=f"owner-{unique}")
        assert durable is not None
        assert durable["status"] == RunStatus.interrupted.value
        events = await inner.list_lifecycle_events(run_id=candidate_run_id)
        assert [event["lifecycle_type"] for event in events] == [
            LifecycleType.accepted,
            LifecycleType.cancellation_requested,
            LifecycleType.cancelled,
        ]
    finally:
        await manager.shutdown(timeout=1)
        async with factory() as session:
            await session.execute(delete(RunRow).where(RunRow.run_id == candidate_run_id))
            await session.commit()
        await engine.dispose()
