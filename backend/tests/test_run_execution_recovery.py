from __future__ import annotations

import asyncio
from collections import OrderedDict
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from deerflow_extension_api import (
    ActingServiceV1,
    CredentialEvidenceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    VerifiedActorContextV1,
    effective_authority_digest_v1,
)
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.persistence.base import Base
from deerflow.persistence.run import RunRepository
from deerflow.persistence.run import sql as run_sql
from deerflow.persistence.run.model import RunRow
from deerflow.runtime import (
    ConflictError,
    ExecutionRecoveryDecision,
    ExecutionRecoveryDisposition,
    RunContext,
    RunManager,
    RunStatus,
    run_agent,
)
from deerflow.runtime.assembly_evidence import (
    AssemblyEvidenceV1,
    assembly_evidence_digest,
)
from deerflow.runtime.runs.store.base import (
    BindAssemblyEvidenceOutcome,
    ExecutionTakeoverOutcome,
    LifecycleTransition,
    LifecycleType,
    RecoveryPayloadIntegrityError,
    RecoveryPolicy,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.runs.worker import RECOVERY_EXECUTOR_CONTEXT_KEY
from deerflow.runtime.tenant_identity import TenantIdentityV1


def _recovery_payload(thread_id: str) -> dict[str, object]:
    return {
        "version": 1,
        "input_kind": "graph",
        "input_value": {"messages": []},
        "config": {
            "recursion_limit": 100,
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": "",
            },
        },
        "stream_modes": ["values"],
        "stream_subgraphs": False,
        "interrupt_before": None,
        "interrupt_after": None,
    }


def _assembly_evidence() -> AssemblyEvidenceV1:
    return AssemblyEvidenceV1(
        version=1,
        fingerprint="1" * 64,
        descriptor_version=1,
        namespace="deerflow",
        agent_name="lead-agent",
        effective_model="test-model",
        prompt_digest="2" * 64,
        toolset_digest="3" * 64,
        middleware_digest="4" * 64,
        skillset_digest="5" * 64,
        policy_digest="6" * 64,
        accepted_agent_revision_digest="7" * 64,
        extension_generation=1,
    )


@pytest.fixture
async def sqlite_recovery_store(tmp_path):
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine

    await init_engine(
        "sqlite",
        url=f"sqlite+aiosqlite:///{tmp_path / 'execution-recovery.db'}",
        sqlite_dir=str(tmp_path),
    )
    yield RunRepository(get_session_factory())
    await close_engine()


async def _assert_snapshot_repair_preserves_recovery_payload(store) -> None:
    original = _recovery_payload("thread-immutable-recovery")
    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    await store.put(
        "run-immutable-recovery",
        thread_id="thread-immutable-recovery",
        owner_worker_id="dead-owner",
        lease_expires_at=expired,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=original,
    )
    admitted = await store.get("run-immutable-recovery")
    assert admitted is not None

    takeover = await store.claim_for_execution_takeover(
        "run-immutable-recovery",
        new_owner_worker_id="replacement-owner",
        lease_expires_at=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        grace_seconds=0,
        expected_state_version=admitted["state_version"],
    )
    assert takeover.outcome is ExecutionTakeoverOutcome.claimed

    # Mapping order is not admission meaning. An equivalent canonical payload
    # remains an idempotent snapshot repair after ownership transfer.
    equivalent = OrderedDict(reversed(list(original.items())))
    await store.put(
        "run-immutable-recovery",
        thread_id="thread-immutable-recovery",
        owner_worker_id="replacement-owner",
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=equivalent,
    )

    conflicting = _recovery_payload("thread-immutable-recovery")
    conflicting["input_value"] = {
        "messages": [{"role": "user", "content": "different"}],
    }
    with pytest.raises(
        RecoveryPayloadIntegrityError,
        match="recovery_payload_integrity_conflict",
    ):
        await store.put(
            "run-immutable-recovery",
            thread_id="thread-immutable-recovery",
            owner_worker_id="replacement-owner",
            recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
            recovery_payload_json=conflicting,
        )

    retained = await store.get("run-immutable-recovery")
    assert retained is not None
    assert retained["recovery_payload_json"] == original


async def _assert_generic_replacement_refuses_exact_two_predecessor(
    store,
    *,
    strategy: str,
) -> None:
    thread_id = f"thread-exact-two-replacement-{strategy}"
    predecessor_id = f"run-exact-two-predecessor-{strategy}"
    replacement_id = f"run-generic-replacement-{strategy}"
    await store.put(
        predecessor_id,
        thread_id=thread_id,
        status="running",
        owner_worker_id="expired-exact-two-owner",
        lease_expires_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload(thread_id),
    )
    before = await store.get(predecessor_id)
    assert before is not None

    with pytest.raises(ConflictError):
        await store.create_thread_operation_atomic(
            replacement_id,
            thread_id=thread_id,
            owner_worker_id="generic-replacement-owner",
            lease_expires_at=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
            multitask_strategy=strategy,
        )

    assert await store.get(predecessor_id) == before
    assert await store.get(replacement_id) is None


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_memory_generic_replacement_refuses_exact_two_predecessor(
    strategy: str,
) -> None:
    await _assert_generic_replacement_refuses_exact_two_predecessor(
        MemoryRunStore(),
        strategy=strategy,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("strategy", ["interrupt", "rollback"])
async def test_sql_generic_replacement_refuses_exact_two_predecessor(
    sqlite_recovery_store,
    strategy: str,
) -> None:
    await _assert_generic_replacement_refuses_exact_two_predecessor(
        sqlite_recovery_store,
        strategy=strategy,
    )


@pytest.mark.anyio
async def test_memory_progress_writer_is_fenced_by_takeover_owner_and_epoch() -> None:
    store = MemoryRunStore()
    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    await store.put(
        "run-progress-fence",
        thread_id="thread-progress-fence",
        status="running",
        model_name="model-original",
        owner_worker_id="worker-old",
        lease_expires_at=expired,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload("thread-progress-fence"),
    )
    original = await store.get("run-progress-fence")
    assert original is not None
    takeover = await store.claim_for_execution_takeover(
        "run-progress-fence",
        new_owner_worker_id="worker-current",
        lease_expires_at=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        grace_seconds=0,
        expected_state_version=original["state_version"],
    )
    assert takeover.outcome is ExecutionTakeoverOutcome.claimed
    assert takeover.row is not None

    stale_applied = await store.update_run_progress(
        "run-progress-fence",
        expected_owner_worker_id="worker-old",
        expected_state_version=original["state_version"],
        require_unexpired_lease=True,
        total_tokens=99,
        last_ai_message="stale snapshot",
    )

    assert stale_applied is False
    after_stale = await store.get("run-progress-fence")
    assert after_stale is not None
    assert after_stale.get("total_tokens", 0) == 0
    assert after_stale.get("last_ai_message") is None

    stale_model_applied = await store.update_model_name(
        "run-progress-fence",
        "model-stale",
        expected_owner_worker_id="worker-old",
        expected_state_version=original["state_version"],
        require_unexpired_lease=True,
    )

    assert stale_model_applied is False
    after_stale_model = await store.get("run-progress-fence")
    assert after_stale_model is not None
    assert after_stale_model["model_name"] == "model-original"

    unfenced_progress = await store.update_run_progress(
        "run-progress-fence",
        total_tokens=88,
        last_ai_message="unfenced snapshot",
    )
    unfenced_model = await store.update_model_name(
        "run-progress-fence",
        "model-unfenced",
    )

    assert unfenced_progress is False
    assert unfenced_model is False
    after_unfenced = await store.get("run-progress-fence")
    assert after_unfenced is not None
    assert after_unfenced.get("total_tokens", 0) == 0
    assert after_unfenced.get("last_ai_message") is None
    assert after_unfenced["model_name"] == "model-original"

    owner_epoch_without_lease_proof = await store.update_run_progress(
        "run-progress-fence",
        expected_owner_worker_id="worker-current",
        expected_state_version=takeover.row["state_version"],
        total_tokens=77,
    )
    model_owner_epoch_without_lease_proof = await store.update_model_name(
        "run-progress-fence",
        "model-without-lease-proof",
        expected_owner_worker_id="worker-current",
        expected_state_version=takeover.row["state_version"],
    )

    assert owner_epoch_without_lease_proof is False
    assert model_owner_epoch_without_lease_proof is False

    current_applied = await store.update_run_progress(
        "run-progress-fence",
        expected_owner_worker_id="worker-current",
        expected_state_version=takeover.row["state_version"],
        require_unexpired_lease=True,
        total_tokens=7,
        last_ai_message="current snapshot",
    )

    assert current_applied is True
    current_model_applied = await store.update_model_name(
        "run-progress-fence",
        "model-current",
        expected_owner_worker_id="worker-current",
        expected_state_version=takeover.row["state_version"],
        require_unexpired_lease=True,
    )
    assert current_model_applied is True
    current = await store.get("run-progress-fence")
    assert current is not None
    assert current["total_tokens"] == 7
    assert current["last_ai_message"] == "current snapshot"
    assert current["model_name"] == "model-current"


@pytest.mark.anyio
async def test_sql_observation_writers_are_fenced_by_takeover_owner_and_epoch(
    sqlite_recovery_store,
) -> None:
    store = sqlite_recovery_store
    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    await store.put(
        "run-observation-fence",
        thread_id="thread-observation-fence",
        status="running",
        model_name="model-original",
        owner_worker_id="worker-old",
        lease_expires_at=expired,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload("thread-observation-fence"),
    )
    original = await store.get("run-observation-fence")
    assert original is not None
    takeover = await store.claim_for_execution_takeover(
        "run-observation-fence",
        new_owner_worker_id="worker-current",
        lease_expires_at=(datetime.now(UTC) + timedelta(minutes=1)).isoformat(),
        grace_seconds=0,
        expected_state_version=original["state_version"],
    )
    assert takeover.outcome is ExecutionTakeoverOutcome.claimed
    assert takeover.row is not None

    stale_progress = await store.update_run_progress(
        "run-observation-fence",
        expected_owner_worker_id="worker-old",
        expected_state_version=original["state_version"],
        require_unexpired_lease=True,
        total_tokens=99,
        last_ai_message="stale snapshot",
    )
    stale_model = await store.update_model_name(
        "run-observation-fence",
        "model-stale",
        expected_owner_worker_id="worker-old",
        expected_state_version=original["state_version"],
        require_unexpired_lease=True,
    )

    assert stale_progress is False
    assert stale_model is False
    after_stale = await store.get("run-observation-fence")
    assert after_stale is not None
    assert after_stale["total_tokens"] == 0
    assert after_stale["last_ai_message"] is None
    assert after_stale["model_name"] == "model-original"

    unfenced_progress = await store.update_run_progress(
        "run-observation-fence",
        total_tokens=88,
        last_ai_message="unfenced snapshot",
    )
    unfenced_model = await store.update_model_name(
        "run-observation-fence",
        "model-unfenced",
    )

    assert unfenced_progress is False
    assert unfenced_model is False
    after_unfenced = await store.get("run-observation-fence")
    assert after_unfenced is not None
    assert after_unfenced["total_tokens"] == 0
    assert after_unfenced["last_ai_message"] is None
    assert after_unfenced["model_name"] == "model-original"

    owner_epoch_without_lease_proof = await store.update_run_progress(
        "run-observation-fence",
        expected_owner_worker_id="worker-current",
        expected_state_version=takeover.row["state_version"],
        total_tokens=77,
    )
    model_owner_epoch_without_lease_proof = await store.update_model_name(
        "run-observation-fence",
        "model-without-lease-proof",
        expected_owner_worker_id="worker-current",
        expected_state_version=takeover.row["state_version"],
    )

    assert owner_epoch_without_lease_proof is False
    assert model_owner_epoch_without_lease_proof is False

    current_progress = await store.update_run_progress(
        "run-observation-fence",
        expected_owner_worker_id="worker-current",
        expected_state_version=takeover.row["state_version"],
        require_unexpired_lease=True,
        total_tokens=7,
        last_ai_message="current snapshot",
    )
    current_model = await store.update_model_name(
        "run-observation-fence",
        "model-current",
        expected_owner_worker_id="worker-current",
        expected_state_version=takeover.row["state_version"],
        require_unexpired_lease=True,
    )

    assert current_progress is True
    assert current_model is True
    current = await store.get("run-observation-fence")
    assert current is not None
    assert current["total_tokens"] == 7
    assert current["last_ai_message"] == "current snapshot"
    assert current["model_name"] == "model-current"


@pytest.mark.anyio
async def test_sql_observation_writers_use_database_clock_for_lease_expiry(
    sqlite_recovery_store,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = sqlite_recovery_store
    actual_now = datetime.now(UTC)
    await store.put(
        "run-observation-expired",
        thread_id="thread-observation-expired",
        status="running",
        model_name="model-original",
        owner_worker_id="worker-current",
        lease_expires_at=(actual_now - timedelta(minutes=1)).isoformat(),
    )
    row = await store.get("run-observation-expired")
    assert row is not None

    class SlowProcessDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = actual_now - timedelta(hours=1)
            return value if tz is not None else value.replace(tzinfo=None)

    monkeypatch.setattr(run_sql, "datetime", SlowProcessDateTime)

    progress_applied = await store.update_run_progress(
        "run-observation-expired",
        expected_owner_worker_id="worker-current",
        expected_state_version=row["state_version"],
        require_unexpired_lease=True,
        total_tokens=99,
    )
    model_applied = await store.update_model_name(
        "run-observation-expired",
        "model-stale",
        expected_owner_worker_id="worker-current",
        expected_state_version=row["state_version"],
        require_unexpired_lease=True,
    )

    assert progress_applied is False
    assert model_applied is False
    retained = await store.get("run-observation-expired")
    assert retained is not None
    assert retained["total_tokens"] == 0
    assert retained["model_name"] == "model-original"


class _RejectingObservationMemoryStore(MemoryRunStore):
    durable_lifecycle = True

    def __init__(self) -> None:
        super().__init__()
        self.progress_call: dict[str, object] | None = None
        self.model_call: dict[str, object] | None = None

    async def update_run_progress(self, run_id: str, **kwargs):
        self.progress_call = {"run_id": run_id, **kwargs}
        return False

    async def update_model_name(self, run_id: str, model_name: str | None, **kwargs):
        self.model_call = {
            "run_id": run_id,
            "model_name": model_name,
            **kwargs,
        }
        return False


class _CancellationRacingObservationMemoryStore(MemoryRunStore):
    durable_lifecycle = True

    def __init__(self, writer: str) -> None:
        super().__init__()
        self._writer = writer

    async def update_run_progress(self, run_id: str, **kwargs):
        if self._writer == "progress":
            await self.request_cancel(run_id, action="interrupt")
        return await super().update_run_progress(run_id, **kwargs)

    async def update_model_name(self, run_id: str, model_name: str | None, **kwargs):
        if self._writer == "model":
            await self.request_cancel(run_id, action="interrupt")
        return await super().update_model_name(
            run_id,
            model_name,
            **kwargs,
        )


@pytest.mark.anyio
async def test_manager_progress_waits_for_owned_store_application() -> None:
    store = _RejectingObservationMemoryStore()
    manager = RunManager(
        store=store,
        worker_id="worker-manager",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await manager.create("thread-manager-progress")
    await manager.set_status(record.run_id, RunStatus.running)

    await manager.update_run_progress(
        record.run_id,
        total_tokens=42,
        last_ai_message="must not become locally visible",
    )

    assert store.progress_call is not None
    assert store.progress_call["expected_owner_worker_id"] == "worker-manager"
    assert store.progress_call["expected_state_version"] == record.state_version
    assert store.progress_call["require_unexpired_lease"] is True
    assert record.total_tokens == 0
    assert record.last_ai_message is None
    assert record.ownership_lost is True


@pytest.mark.anyio
async def test_manager_model_name_waits_for_owned_store_application() -> None:
    store = _RejectingObservationMemoryStore()
    manager = RunManager(
        store=store,
        worker_id="worker-manager",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await manager.create("thread-manager-model")
    await manager.set_status(record.run_id, RunStatus.running)

    await manager.update_model_name(record.run_id, "model-rejected")

    assert store.model_call is not None
    assert store.model_call["expected_owner_worker_id"] == "worker-manager"
    assert store.model_call["expected_state_version"] == record.state_version
    assert store.model_call["require_unexpired_lease"] is True
    assert record.model_name is None
    assert record.ownership_lost is True


@pytest.mark.anyio
@pytest.mark.parametrize("writer", ("progress", "model"))
async def test_manager_observation_rejection_refreshes_same_owner_cancellation(
    writer: str,
) -> None:
    store = _CancellationRacingObservationMemoryStore(writer)
    manager = RunManager(
        store=store,
        worker_id="worker-manager",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
    )
    record = await manager.create(f"thread-manager-cancel-{writer}")
    await manager.set_status(record.run_id, RunStatus.running)
    captured_state_version = record.state_version

    if writer == "progress":
        await manager.update_run_progress(
            record.run_id,
            total_tokens=42,
            last_ai_message="cancelled snapshot",
        )
    else:
        await manager.update_model_name(record.run_id, "model-cancelled")

    assert record.ownership_lost is False
    assert record.abort_event.is_set()
    assert record.abort_action == "interrupt"
    assert record.state_version == captured_state_version + 1
    assert record.total_tokens == 0
    assert record.last_ai_message is None
    assert record.model_name is None


async def _assert_recovery_payload_outbound_snapshots_are_detached(store) -> None:
    direct_original = _recovery_payload("thread-detached-direct")
    await store.put(
        "run-detached-direct",
        thread_id="thread-detached-direct",
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=direct_original,
    )
    returned = await store.get("run-detached-direct")
    assert returned is not None
    returned["recovery_payload_json"]["input_value"]["messages"].append(
        {"role": "user", "content": "mutated outside the store"},
    )
    retained = await store.get("run-detached-direct")
    assert retained is not None
    assert retained["recovery_payload_json"] == direct_original

    mutation_result_original = _recovery_payload("thread-detached-result")
    created, _ = await store.create_thread_operation_atomic(
        "run-detached-result",
        thread_id="thread-detached-result",
        owner_worker_id="worker-result",
        lease_expires_at=None,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=mutation_result_original,
    )
    created["recovery_payload_json"]["input_value"]["messages"].append(
        {"role": "user", "content": "mutated admission result"},
    )
    retained = await store.get("run-detached-result")
    assert retained is not None
    assert retained["recovery_payload_json"] == mutation_result_original
    started = await store.transition_owned_run_atomic(
        "run-detached-result",
        expected_state_version=retained["state_version"],
        expected_statuses=("pending",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.started,
            status="running",
        ),
        expected_owner_worker_id="worker-result",
        require_unexpired_lease=False,
    )
    assert started.applied
    assert started.row is not None
    started.row["recovery_payload_json"]["input_value"]["messages"].append(
        {"role": "user", "content": "mutated transition result"},
    )
    retained = await store.get("run-detached-result")
    assert retained is not None
    assert retained["recovery_payload_json"] == mutation_result_original

    manager_original = _recovery_payload("thread-detached-manager")
    manager = RunManager(
        store=store,
        admission_recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
    )
    record = await manager.create_or_reject(
        "thread-detached-manager",
        recovery_payload_json=manager_original,
    )
    assert record.recovery_payload_json is not None
    record.recovery_payload_json["input_value"]["messages"].append(
        {"role": "user", "content": "mutated through the manager"},
    )
    retained = await store.get(record.run_id)
    assert retained is not None
    assert retained["recovery_payload_json"] == manager_original


@pytest.mark.anyio
async def test_memory_snapshot_repair_cannot_rewrite_recovery_payload() -> None:
    await _assert_snapshot_repair_preserves_recovery_payload(MemoryRunStore())


@pytest.mark.anyio
async def test_sql_snapshot_repair_cannot_rewrite_recovery_payload(
    sqlite_recovery_store,
) -> None:
    await _assert_snapshot_repair_preserves_recovery_payload(
        sqlite_recovery_store,
    )


@pytest.mark.anyio
async def test_memory_recovery_payload_outbound_snapshots_are_detached() -> None:
    await _assert_recovery_payload_outbound_snapshots_are_detached(MemoryRunStore())


@pytest.mark.anyio
async def test_sql_recovery_payload_outbound_snapshots_are_detached(
    sqlite_recovery_store,
) -> None:
    await _assert_recovery_payload_outbound_snapshots_are_detached(
        sqlite_recovery_store,
    )


@pytest.mark.anyio
async def test_sql_execution_takeover_uses_database_clock_not_claimant_clock(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'takeover-clock.db'}")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    actual_now = datetime.now(UTC)

    class FastProcessDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = actual_now + timedelta(hours=1)
            return value if tz is not None else value.replace(tzinfo=None)

    try:
        store = RunRepository(session_factory)
        await store.put(
            "run-live-database-lease",
            thread_id="thread-live-database-lease",
            owner_worker_id="worker-live",
            lease_expires_at=(actual_now + timedelta(minutes=5)).isoformat(),
            recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
            recovery_payload_json=_recovery_payload(
                "thread-live-database-lease",
            ),
        )
        admitted = await store.get("run-live-database-lease")
        assert admitted is not None
        monkeypatch.setattr(run_sql, "datetime", FastProcessDateTime)

        claim = await store.claim_for_execution_takeover(
            "run-live-database-lease",
            new_owner_worker_id="worker-fast-clock",
            lease_expires_at=(actual_now + timedelta(hours=2)).isoformat(),
            grace_seconds=0,
            expected_state_version=admitted["state_version"],
        )

        assert claim.outcome is ExecutionTakeoverOutcome.not_eligible
        retained = await store.get("run-live-database-lease")
        assert retained is not None
        assert retained["owner_worker_id"] == "worker-live"
        assert retained["state_version"] == admitted["state_version"]
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_sql_execution_takeover_rechecks_database_clock_after_lock_wait(
    tmp_path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'takeover-lock-wait.db'}")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    store = RunRepository(session_factory)
    lease_deadline = datetime.now(UTC) + timedelta(milliseconds=250)
    try:
        await store.put(
            "run-expiring-during-lock-wait",
            thread_id="thread-expiring-during-lock-wait",
            owner_worker_id="worker-before-wait",
            lease_expires_at=lease_deadline.isoformat(),
            recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
            recovery_payload_json=_recovery_payload(
                "thread-expiring-during-lock-wait",
            ),
        )
        admitted = await store.get("run-expiring-during-lock-wait")
        assert admitted is not None

        async with session_factory() as blocker:
            await blocker.execute(text("BEGIN IMMEDIATE"))
            locked = await blocker.scalar(
                select(RunRow).where(
                    RunRow.run_id == "run-expiring-during-lock-wait",
                )
            )
            assert locked is not None
            claim_task = asyncio.create_task(
                store.claim_for_execution_takeover(
                    "run-expiring-during-lock-wait",
                    new_owner_worker_id="worker-after-wait",
                    lease_expires_at=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
                    grace_seconds=0,
                    expected_state_version=admitted["state_version"],
                )
            )
            await asyncio.sleep(0.4)
            assert claim_task.done() is False
            await blocker.commit()

        claim = await asyncio.wait_for(claim_task, timeout=2)
        assert claim.outcome is ExecutionTakeoverOutcome.claimed
        assert claim.row is not None
        assert claim.row["owner_worker_id"] == "worker-after-wait"
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_sql_owned_mutations_reject_database_expired_lease_despite_slow_process_clock(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'owned-mutation-clock.db'}")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    actual_now = datetime.now(UTC)
    expired = (actual_now - timedelta(minutes=1)).isoformat()

    class SlowProcessDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = actual_now - timedelta(hours=1)
            return value if tz is not None else value.replace(tzinfo=None)

    try:
        store = RunRepository(session_factory)
        rows: dict[str, dict] = {}
        for suffix in ("transition", "assembly", "renewal"):
            run_id = f"run-expired-{suffix}"
            await store.put(
                run_id,
                thread_id=f"thread-expired-{suffix}",
                status="running",
                owner_worker_id="worker-expired",
                lease_expires_at=expired,
            )
            row = await store.get(run_id)
            assert row is not None
            rows[suffix] = row
        monkeypatch.setattr(run_sql, "datetime", SlowProcessDateTime)

        transitioned = await store.transition_owned_run_atomic(
            "run-expired-transition",
            expected_state_version=rows["transition"]["state_version"],
            expected_statuses=("running",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.succeeded,
                status="success",
            ),
            expected_owner_worker_id="worker-expired",
            require_unexpired_lease=True,
        )
        evidence = _assembly_evidence()
        bound = await store.bind_assembly_evidence(
            "run-expired-assembly",
            owner_id="worker-expired",
            lease_epoch=rows["assembly"]["state_version"],
            evidence_json=evidence.to_persisted_json(),
            evidence_digest=assembly_evidence_digest(evidence),
        )
        renewal = await store.renew_lease(
            "run-expired-renewal",
            owner_worker_id="worker-expired",
            lease_expires_at=(actual_now + timedelta(minutes=5)).isoformat(),
        )

        assert transitioned.applied is False
        assert bound is BindAssemblyEvidenceOutcome.ownership_lost
        assert renewal.renewed is False
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_recovery_policy_is_admitted_once_and_takeover_has_one_winner() -> None:
    store = MemoryRunStore()
    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()

    legacy, _ = await store.create_thread_operation_atomic(
        "run-terminalize",
        thread_id="thread-terminalize",
        owner_worker_id="owner-a",
        lease_expires_at=expired,
    )
    candidate, _ = await store.create_thread_operation_atomic(
        "run-takeover",
        thread_id="thread-takeover",
        owner_worker_id="owner-a",
        lease_expires_at=expired,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload("thread-takeover"),
    )

    assert legacy["recovery_policy"] == RecoveryPolicy.terminalize_v1
    assert candidate["recovery_policy"] == RecoveryPolicy.exact_two_takeover_v1

    new_expiry = (datetime.now(UTC) + timedelta(minutes=1)).isoformat()
    left, right = await asyncio.gather(
        store.claim_for_execution_takeover(
            "run-takeover",
            new_owner_worker_id="owner-b",
            lease_expires_at=new_expiry,
            grace_seconds=0,
            expected_state_version=candidate["state_version"],
        ),
        store.claim_for_execution_takeover(
            "run-takeover",
            new_owner_worker_id="owner-c",
            lease_expires_at=new_expiry,
            grace_seconds=0,
            expected_state_version=candidate["state_version"],
        ),
    )

    winner = next(result for result in (left, right) if result.outcome is ExecutionTakeoverOutcome.claimed)
    loser = next(result for result in (left, right) if result.outcome is ExecutionTakeoverOutcome.not_eligible)
    assert winner.row is not None
    assert winner.row["owner_worker_id"] in {"owner-b", "owner-c"}
    assert winner.row["state_version"] == 2
    assert winner.row["status"] == "pending"
    assert winner.row["recovery_policy"] == RecoveryPolicy.exact_two_takeover_v1
    assert loser.row is not None
    assert loser.row["state_version"] == winner.row["state_version"]

    refused = await store.claim_for_execution_takeover(
        "run-terminalize",
        new_owner_worker_id="owner-b",
        lease_expires_at=new_expiry,
        grace_seconds=0,
        expected_state_version=legacy["state_version"],
    )
    assert refused.outcome is ExecutionTakeoverOutcome.not_eligible
    assert refused.row is not None
    assert refused.row["owner_worker_id"] == "owner-a"


@pytest.mark.anyio
async def test_existing_keyed_admission_keeps_original_recovery_policy() -> None:
    store = MemoryRunStore()
    common = {
        "thread_id": "thread-keyed-recovery",
        "owner_worker_id": "owner-a",
        "lease_expires_at": None,
        "external_scope": "service:qualification",
        "external_key": "delivery-1",
        "request_digest": "a" * 64,
        "request_digest_version": "sha256-canonical-json-v1",
        "caller_intent_json": {"version": 1},
        "caller_intent_digest": "b" * 64,
        "caller_intent_digest_version": "caller-intent-canonical-json-v1",
    }
    created = await store.ensure_run_atomic(
        "run-keyed-recovery",
        recovery_policy=RecoveryPolicy.terminalize_v1,
        **common,
    )
    replay = await store.ensure_run_atomic(
        "run-ignored-candidate",
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload("thread-keyed-recovery"),
        **common,
    )

    assert created.row["recovery_policy"] == RecoveryPolicy.terminalize_v1
    assert replay.row["run_id"] == created.row["run_id"]
    assert replay.row["recovery_policy"] == RecoveryPolicy.terminalize_v1


@pytest.mark.anyio
async def test_manager_routes_exact_two_orphan_to_recovery_callback() -> None:
    store = MemoryRunStore()
    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    row, _ = await store.create_thread_operation_atomic(
        "run-manager-takeover",
        thread_id="thread-manager-takeover",
        owner_worker_id="dead-owner",
        lease_expires_at=expired,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload("thread-manager-takeover"),
    )
    observed = []

    async def recover(record):
        observed.append(record)
        return ExecutionRecoveryDisposition.resumed

    manager = RunManager(
        store=store,
        worker_id="survivor",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
        on_execution_takeover=recover,
        execution_takeover_eligibility=lambda _record: True,
        execution_recovery_claims_enabled=True,
    )

    terminalized = await manager.reconcile_orphaned_inflight_runs(
        error="legacy recovery",
    )

    assert terminalized == []
    assert len(observed) == 1
    recovered = observed[0]
    assert recovered.run_id == row["run_id"]
    assert recovered.recovery_policy is RecoveryPolicy.exact_two_takeover_v1
    assert recovered.execution_takeover is True
    assert recovered.owner_worker_id == "survivor"
    assert recovered.state_version == 2
    persisted = await store.get(row["run_id"])
    assert persisted is not None
    assert persisted["status"] == "pending"
    assert persisted["recovery_policy"] == RecoveryPolicy.exact_two_takeover_v1


@pytest.mark.anyio
async def test_manager_defaults_exact_two_recovery_to_fail_closed() -> None:
    store = MemoryRunStore()
    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    row, _ = await store.create_thread_operation_atomic(
        "run-default-deny-takeover",
        thread_id="thread-default-deny-takeover",
        owner_worker_id="dead-owner",
        lease_expires_at=expired,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload("thread-default-deny-takeover"),
    )
    callback_calls = 0

    async def recover(_record):
        nonlocal callback_calls
        callback_calls += 1
        return ExecutionRecoveryDisposition.resumed

    manager = RunManager(
        store=store,
        worker_id="survivor",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
        on_execution_takeover=recover,
    )

    assert await manager.reconcile_orphaned_inflight_runs(error="legacy") == []
    assert callback_calls == 0
    persisted = await store.get(row["run_id"])
    assert persisted is not None
    assert persisted["owner_worker_id"] == "dead-owner"
    assert persisted["state_version"] == 1


@pytest.mark.anyio
async def test_recovery_claim_kill_switch_leaves_accepted_policy_untouched() -> None:
    store = MemoryRunStore()
    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    row, _ = await store.create_thread_operation_atomic(
        "run-kill-switch",
        thread_id="thread-kill-switch",
        owner_worker_id="dead-owner",
        lease_expires_at=expired,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload("thread-kill-switch"),
    )
    calls = 0

    async def recover(_record):
        nonlocal calls
        calls += 1
        return ExecutionRecoveryDisposition.resumed

    manager = RunManager(
        store=store,
        worker_id="survivor",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
        on_execution_takeover=recover,
    )
    manager.set_execution_recovery_claims_enabled(False)

    assert await manager.reconcile_orphaned_inflight_runs(error="legacy") == []
    assert calls == 0
    persisted = await store.get(row["run_id"])
    assert persisted is not None
    assert persisted["owner_worker_id"] == "dead-owner"
    assert persisted["state_version"] == 1
    assert persisted["recovery_policy"] == RecoveryPolicy.exact_two_takeover_v1


@pytest.mark.anyio
async def test_recovery_eligibility_gate_runs_before_owner_epoch_claim() -> None:
    """A provider capability refusal must not create a transient DB owner."""

    store = MemoryRunStore()
    expired = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    row, _ = await store.create_thread_operation_atomic(
        "run-ineligible-takeover",
        thread_id="thread-ineligible-takeover",
        owner_worker_id="dead-owner",
        lease_expires_at=expired,
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_recovery_payload("thread-ineligible-takeover"),
    )
    callback_calls = 0
    claim_calls = 0
    original_claim = store.claim_for_execution_takeover

    async def counted_claim(*args, **kwargs):
        nonlocal claim_calls
        claim_calls += 1
        return await original_claim(*args, **kwargs)

    async def recover(_record):
        nonlocal callback_calls
        callback_calls += 1
        return ExecutionRecoveryDisposition.resumed

    store.claim_for_execution_takeover = counted_claim  # type: ignore[method-assign]
    manager = RunManager(
        store=store,
        worker_id="survivor",
        run_ownership_config=RunOwnershipConfig(
            heartbeat_enabled=True,
            lease_seconds=30,
            grace_seconds=0,
        ),
        on_execution_takeover=recover,
        execution_takeover_eligibility=lambda _record: False,
    )

    assert await manager.reconcile_orphaned_inflight_runs(error="legacy") == []
    assert claim_calls == 0
    assert callback_calls == 0
    persisted = await store.get(row["run_id"])
    assert persisted is not None
    assert persisted["owner_worker_id"] == "dead-owner"
    assert persisted["state_version"] == 1
    assert persisted["recovery_policy"] == RecoveryPolicy.exact_two_takeover_v1


@pytest.mark.anyio
async def test_manager_rejects_exact_two_without_recovery_payload_before_store_write() -> None:
    store = MemoryRunStore()
    manager = RunManager(
        store=store,
        admission_recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
    )

    with pytest.raises(
        ValueError,
        match="exact-two recovery requires a durable recovery payload",
    ):
        await manager.create_or_reject("thread-missing-recovery-payload")

    assert await store.list_by_thread("thread-missing-recovery-payload") == []


@pytest.mark.anyio
async def test_checkpoint_resume_reuses_existing_dispatch_marker() -> None:
    manager = RunManager()
    record = await manager.create("thread-checkpoint-resume")
    record.execution_takeover = True
    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )
    observed_inputs: list[object] = []
    observed_configs: list[dict[str, object]] = []
    tenant = TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference()
    recovery_executor = VerifiedActorContextV1(
        identity=InvocationIdentityV1(
            effective_subject=EffectiveSubjectV1(
                kind="human",
                subject_id="user-1",
            ),
            acting_service=ActingServiceV1(
                service_id="gateway:execution-recovery",
            ),
        ),
        credential=CredentialEvidenceV1(
            method="internal_service",
            credential_ref=None,
            effective_authority_digest=effective_authority_digest_v1(("runs:recover",)),
            authority_categories=("runs",),
        ),
        tenant=tenant,
    )

    class _Agent:
        async def astream(
            self,
            graph_input,
            *,
            config=None,
            stream_mode=None,
            subgraphs=False,
        ):
            del stream_mode, subgraphs
            observed_inputs.append(graph_input)
            observed_configs.append(config)
            yield {"messages": []}

    async def recovery_gate(_record, _descriptor):
        return ExecutionRecoveryDecision(
            ExecutionRecoveryDisposition.resume_checkpoint,
        )

    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            execution_recovery_gate=recovery_gate,
            recovery_executor=recovery_executor,
        ),
        agent_factory=lambda *, config: _Agent(),
        graph_input={"messages": [{"role": "user", "content": "once"}]},
        config={
            "configurable": {
                "thread_id": "thread-checkpoint-resume",
                "checkpoint_ns": "fork",
                "checkpoint_id": "stale-selected-checkpoint",
                "checkpoint_map": {"fork": "stale-selected-checkpoint"},
            }
        },
    )

    assert observed_inputs == [None]
    assert observed_configs[0]["context"][RECOVERY_EXECUTOR_CONTEXT_KEY] is recovery_executor
    configurable = observed_configs[0]["configurable"]
    assert configurable["checkpoint_ns"] == ""
    assert "checkpoint_id" not in configurable
    assert "checkpoint_map" not in configurable
    assert record.status is RunStatus.success


@pytest.mark.anyio
async def test_terminal_recovery_decision_publishes_bounded_error_without_stale_end() -> None:
    manager = RunManager()
    record = await manager.create("thread-terminal-recovery")
    record.execution_takeover = True
    bridge = SimpleNamespace(
        publish=AsyncMock(),
        publish_end=AsyncMock(),
        cleanup=AsyncMock(),
    )

    class _Agent:
        async def astream(self, *_args, **_kwargs):
            raise AssertionError("terminal recovery must not dispatch graph")
            yield  # pragma: no cover

    async def recovery_gate(_record, _descriptor):
        return ExecutionRecoveryDecision(
            ExecutionRecoveryDisposition.terminalize_checkpoint_unavailable,
        )

    await run_agent(
        bridge,
        manager,
        record,
        ctx=RunContext(
            checkpointer=None,
            execution_recovery_gate=recovery_gate,
        ),
        agent_factory=lambda *, config: _Agent(),
        graph_input={"messages": []},
        config={},
    )

    error_calls = [call for call in bridge.publish.await_args_list if call.args[1] == "error"]
    assert len(error_calls) == 1
    assert error_calls[0].args[2] == {
        "message": ("Recovery stopped because no safe durable execution checkpoint is available."),
        "name": "ExecutionRecoveryError",
        "stop_reason": "recovery_checkpoint_unavailable",
    }
    bridge.publish_end.assert_not_awaited()
    bridge.cleanup.assert_not_awaited()
