"""Fenced persistence for compact protected execution-policy state."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.base import Base
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.execution_policy import (
    ExecutionBudgetV1,
    ExecutionPolicyEvaluator,
    ExecutionPolicyObservationV1,
    ExecutionPolicyStateV1,
    PolicyDecision,
)
from deerflow.runtime.runs.store.base import (
    ApplyExecutionPolicyStateOutcome,
    ExecutionTakeoverOutcome,
    RecoveryPolicy,
    RunStore,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore


async def _store(kind: str, tmp_path) -> tuple[RunStore, object | None]:
    if kind == "memory":
        return MemoryRunStore(), None
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'policy.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return RunRepository(async_sessionmaker(engine, expire_on_commit=False)), engine


async def _expire_lease(store: RunStore, engine: object | None, run_id: str) -> None:
    expired = "2000-01-01T00:00:00+00:00"
    if isinstance(store, MemoryRunStore):
        store._runs[run_id]["lease_expires_at"] = expired
        return
    async with engine.begin() as connection:  # type: ignore[union-attr]
        await connection.execute(
            text("UPDATE runs SET lease_expires_at = :expired WHERE run_id = :run_id"),
            {"expired": expired, "run_id": run_id},
        )


def _execution_recovery_payload(thread_id: str) -> dict[str, object]:
    return {
        "version": 1,
        "input_kind": "graph",
        "input_value": {"messages": []},
        "config": {"configurable": {"thread_id": thread_id}},
        "stream_modes": ["values"],
        "stream_subgraphs": False,
        "interrupt_before": None,
        "interrupt_after": None,
    }


async def cross_worker_takeover_yields_one_accepted_stop(
    store: RunStore,
    *,
    expire_lease,
) -> None:
    """Shared scenario body, reused by the real-PostgreSQL contract module."""

    await store.put(
        "run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        lease_expires_at="2999-01-01T00:00:00+00:00",
        recovery_policy=RecoveryPolicy.exact_two_takeover_v1,
        recovery_payload_json=_execution_recovery_payload("thread-1"),
    )
    assert await store.start_run("run-1") is True
    row = await store.get("run-1")
    assert row is not None
    stale_epoch = row["state_version"]

    budget = ExecutionBudgetV1.build(max_agent_turns=1)
    initial = ExecutionPolicyStateV1.initial(budget)
    assert (
        await store.apply_execution_policy_state(
            "run-1",
            owner_id="worker-1",
            lease_epoch=stale_epoch,
            expected_digest=None,
            state_json=initial.to_json(),
            state_digest=initial.digest,
        )
        is ApplyExecutionPolicyStateOutcome.applied
    )

    # Both workers deterministically cross the same threshold from the same
    # durable state; only the fence decides which stop is accepted.
    evaluation = ExecutionPolicyEvaluator().evaluate(
        budget,
        initial,
        ExecutionPolicyObservationV1(kind="turn"),
    )
    assert evaluation.decision is PolicyDecision.stop
    assert evaluation.reason_code == "turn_budget_exhausted"

    await expire_lease(store, "run-1")
    claim = await store.claim_for_execution_takeover(
        "run-1",
        new_owner_worker_id="worker-2",
        lease_duration_seconds=60,
        grace_seconds=0,
        expected_state_version=stale_epoch,
    )
    assert claim.outcome is ExecutionTakeoverOutcome.claimed
    assert claim.row is not None
    new_epoch = claim.row["state_version"]
    assert new_epoch != stale_epoch

    assert (
        await store.apply_execution_policy_state(
            "run-1",
            owner_id="worker-2",
            lease_epoch=new_epoch,
            expected_digest=initial.digest,
            state_json=evaluation.next_state.to_json(),
            state_digest=evaluation.next_state.digest,
        )
        is ApplyExecutionPolicyStateOutcome.applied
    )
    assert (
        await store.apply_execution_policy_state(
            "run-1",
            owner_id="worker-1",
            lease_epoch=stale_epoch,
            expected_digest=initial.digest,
            state_json=evaluation.next_state.to_json(),
            state_digest=evaluation.next_state.digest,
        )
        is ApplyExecutionPolicyStateOutcome.ownership_lost
    )

    row = await store.get("run-1")
    assert row is not None
    restored = ExecutionPolicyStateV1.from_json(row["execution_policy_state_json"])
    assert restored.terminal_reason == "turn_budget_exhausted"
    # A restart that restores terminal policy state deterministically refuses
    # further work without requiring another durable decision event.
    resumed = ExecutionPolicyEvaluator().evaluate(
        budget,
        restored,
        ExecutionPolicyObservationV1(kind="turn"),
    )
    assert resumed.decision is PolicyDecision.stop
    assert resumed.durable_event_required is False
    assert resumed.next_state == restored


async def _running(store: RunStore) -> int:
    await store.put(
        "run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        lease_expires_at="2999-01-01T00:00:00+00:00",
    )
    assert await store.start_run("run-1") is True
    row = await store.get("run-1")
    assert row is not None
    return row["state_version"]


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_policy_state_can_initialize_under_the_owned_pending_fence(
    kind,
    tmp_path,
) -> None:
    store, engine = await _store(kind, tmp_path)
    try:
        await store.put(
            "run-1",
            thread_id="thread-1",
            owner_worker_id="worker-1",
            lease_expires_at="2999-01-01T00:00:00+00:00",
        )
        row = await store.get("run-1")
        assert row is not None
        state = ExecutionPolicyStateV1.initial(ExecutionBudgetV1.build())

        assert (
            await store.apply_execution_policy_state(
                "run-1",
                owner_id="worker-1",
                lease_epoch=row["state_version"],
                expected_digest=None,
                state_json=state.to_json(),
                state_digest=state.digest,
            )
            is ApplyExecutionPolicyStateOutcome.applied
        )
    finally:
        if engine is not None:
            await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_policy_state_cas_is_fenced_and_survives_reload(kind, tmp_path) -> None:
    store, engine = await _store(kind, tmp_path)
    try:
        lease_epoch = await _running(store)
        budget = ExecutionBudgetV1.build()
        initial = ExecutionPolicyStateV1.initial(budget)
        next_state = (
            ExecutionPolicyEvaluator()
            .evaluate(
                budget,
                initial,
                ExecutionPolicyObservationV1(kind="turn"),
            )
            .next_state
        )

        assert (
            await store.apply_execution_policy_state(
                "run-1",
                owner_id="worker-1",
                lease_epoch=lease_epoch,
                expected_digest=None,
                state_json=initial.to_json(),
                state_digest=initial.digest,
            )
            is ApplyExecutionPolicyStateOutcome.applied
        )
        outcomes = await asyncio.gather(
            store.apply_execution_policy_state(
                "run-1",
                owner_id="worker-1",
                lease_epoch=lease_epoch,
                expected_digest=initial.digest,
                state_json=next_state.to_json(),
                state_digest=next_state.digest,
            ),
            store.apply_execution_policy_state(
                "run-1",
                owner_id="worker-1",
                lease_epoch=lease_epoch,
                expected_digest=initial.digest,
                state_json=next_state.to_json(),
                state_digest=next_state.digest,
            ),
        )
        assert sorted(outcomes) == sorted([ApplyExecutionPolicyStateOutcome.applied, ApplyExecutionPolicyStateOutcome.conflict])
        row = await store.get("run-1")
        assert row is not None
        assert row["execution_policy_state_digest"] == next_state.digest
        assert ExecutionPolicyStateV1.from_json(row["execution_policy_state_json"]) == next_state

        assert (
            await store.apply_execution_policy_state(
                "run-1",
                owner_id="stale-worker",
                lease_epoch=lease_epoch,
                expected_digest=next_state.digest,
                state_json=next_state.to_json(),
                state_digest=next_state.digest,
            )
            is ApplyExecutionPolicyStateOutcome.ownership_lost
        )
    finally:
        if engine is not None:
            await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_legacy_run_has_no_invented_policy_state(kind, tmp_path) -> None:
    store, engine = await _store(kind, tmp_path)
    try:
        await store.put("legacy", thread_id="thread-legacy")
        row = await store.get("legacy")
        assert row is not None
        assert row["execution_policy_state_json"] is None
        assert row["execution_policy_state_digest"] is None
    finally:
        if engine is not None:
            await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_cross_worker_takeover_yields_one_accepted_stop(kind, tmp_path) -> None:
    store, engine = await _store(kind, tmp_path)
    try:
        await cross_worker_takeover_yields_one_accepted_stop(
            store,
            expire_lease=lambda target, run_id: _expire_lease(target, engine, run_id),
        )
    finally:
        if engine is not None:
            await engine.dispose()
