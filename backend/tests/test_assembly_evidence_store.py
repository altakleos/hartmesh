"""Fenced compare-and-set contract for durable agent assembly evidence."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.base import Base
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.assembly_evidence import AssemblyEvidenceV1, assembly_evidence_digest
from deerflow.runtime.runs.store.base import (
    BindAssemblyEvidenceOutcome,
    LifecycleTransition,
    LifecycleType,
    RunStore,
)
from deerflow.runtime.runs.store.memory import MemoryRunStore


def _evidence(*, fingerprint: str = "1" * 64) -> AssemblyEvidenceV1:
    return AssemblyEvidenceV1(
        version=1,
        fingerprint=fingerprint,
        descriptor_version=1,
        namespace="deerflow",
        agent_name="lead-agent",
        effective_model="gpt-5",
        prompt_digest="2" * 64,
        toolset_digest="3" * 64,
        middleware_digest="4" * 64,
        skillset_digest="5" * 64,
        policy_digest="6" * 64,
        accepted_agent_revision_digest="7" * 64,
        extension_generation=3,
    )


async def _make_store(kind: str, tmp_path) -> tuple[RunStore, object | None]:
    if kind == "memory":
        return MemoryRunStore(), None
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / f'assembly-{kind}.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return RunRepository(async_sessionmaker(engine, expire_on_commit=False)), engine


async def _put_running(store: RunStore, run_id: str = "run-1") -> int:
    await store.put(
        run_id,
        thread_id=f"thread-{run_id}",
        owner_worker_id="worker-1",
        lease_expires_at="2999-01-01T00:00:00+00:00",
    )
    assert await store.start_run(run_id) is True
    row = await store.get(run_id)
    assert row is not None
    return row["state_version"]


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_first_bind_repeat_mismatch_and_stale_fences(kind, tmp_path):
    store, engine = await _make_store(kind, tmp_path)
    try:
        lease_epoch = await _put_running(store)
        original = _evidence()
        original_json = original.to_persisted_json()
        original_digest = assembly_evidence_digest(original)

        first = await store.bind_assembly_evidence(
            "run-1",
            owner_id="worker-1",
            lease_epoch=lease_epoch,
            evidence_json=original_json,
            evidence_digest=original_digest,
        )
        repeat = await store.bind_assembly_evidence(
            "run-1",
            owner_id="worker-1",
            lease_epoch=lease_epoch,
            evidence_json=dict(reversed(list(original_json.items()))),
            evidence_digest=original_digest,
        )

        changed = replace(original, fingerprint="8" * 64)
        mismatch = await store.bind_assembly_evidence(
            "run-1",
            owner_id="worker-1",
            lease_epoch=lease_epoch,
            evidence_json=changed.to_persisted_json(),
            evidence_digest=assembly_evidence_digest(changed),
        )
        wrong_owner = await store.bind_assembly_evidence(
            "run-1",
            owner_id="worker-2",
            lease_epoch=lease_epoch,
            evidence_json=original_json,
            evidence_digest=original_digest,
        )
        stale_epoch = await store.bind_assembly_evidence(
            "run-1",
            owner_id="worker-1",
            lease_epoch=lease_epoch - 1,
            evidence_json=original_json,
            evidence_digest=original_digest,
        )
        missing = await store.bind_assembly_evidence(
            "missing",
            owner_id="worker-1",
            lease_epoch=lease_epoch,
            evidence_json=original_json,
            evidence_digest=original_digest,
        )

        assert first is BindAssemblyEvidenceOutcome.bound
        assert repeat is BindAssemblyEvidenceOutcome.already_matching
        assert mismatch is BindAssemblyEvidenceOutcome.mismatch
        assert wrong_owner is BindAssemblyEvidenceOutcome.ownership_lost
        assert stale_epoch is BindAssemblyEvidenceOutcome.ownership_lost
        assert missing is BindAssemblyEvidenceOutcome.not_found
        row = await store.get("run-1")
        assert row is not None
        assert row["assembly_evidence_json"] == original_json
        assert row["assembly_evidence_digest"] == original_digest

        # Idempotent snapshot repair must never erase the already-bound fact.
        await store.put("run-1", thread_id="thread-run-1", owner_worker_id="worker-1")
        repaired = await store.get("run-1")
        assert repaired is not None
        assert repaired["assembly_evidence_json"] == original_json
        assert repaired["assembly_evidence_digest"] == original_digest
    finally:
        if engine is not None:
            await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_nullable_legacy_rows_decode_without_inventing_evidence(kind, tmp_path):
    store, engine = await _make_store(kind, tmp_path)
    try:
        await store.put("legacy", thread_id="legacy-thread")
        row = await store.get("legacy")
        assert row is not None
        assert row["assembly_evidence_json"] is None
        assert row["assembly_evidence_digest"] is None
    finally:
        if engine is not None:
            await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_expired_execution_lease_cannot_bind(kind, tmp_path):
    store, engine = await _make_store(kind, tmp_path)
    try:
        await store.put(
            "expired-run",
            thread_id="thread-expired-run",
            owner_worker_id="worker-1",
            lease_expires_at="2000-01-01T00:00:00+00:00",
        )
        assert await store.start_run("expired-run") is True
        row = await store.get("expired-run")
        assert row is not None
        evidence = _evidence()

        outcome = await store.bind_assembly_evidence(
            "expired-run",
            owner_id="worker-1",
            lease_epoch=row["state_version"],
            evidence_json=evidence.to_persisted_json(),
            evidence_digest=assembly_evidence_digest(evidence),
        )

        assert outcome is BindAssemblyEvidenceOutcome.ownership_lost
    finally:
        if engine is not None:
            await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
async def test_concurrent_different_binds_have_exactly_one_winner(kind, tmp_path):
    store, engine = await _make_store(kind, tmp_path)
    try:
        lease_epoch = await _put_running(store)
        first = _evidence(fingerprint="a" * 64)
        second = _evidence(fingerprint="b" * 64)

        outcomes = await asyncio.gather(
            store.bind_assembly_evidence(
                "run-1",
                owner_id="worker-1",
                lease_epoch=lease_epoch,
                evidence_json=first.to_persisted_json(),
                evidence_digest=assembly_evidence_digest(first),
            ),
            store.bind_assembly_evidence(
                "run-1",
                owner_id="worker-1",
                lease_epoch=lease_epoch,
                evidence_json=second.to_persisted_json(),
                evidence_digest=assembly_evidence_digest(second),
            ),
        )

        assert sorted(outcomes) == sorted([BindAssemblyEvidenceOutcome.bound, BindAssemblyEvidenceOutcome.mismatch])
        row = await store.get("run-1")
        assert row is not None
        assert row["assembly_evidence_digest"] in {
            assembly_evidence_digest(first),
            assembly_evidence_digest(second),
        }
    finally:
        if engine is not None:
            await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["memory", "sqlite"])
@pytest.mark.parametrize(
    ("status", "lifecycle_type"),
    [
        ("success", LifecycleType.succeeded),
        ("error", LifecycleType.failed),
        ("interrupted", LifecycleType.cancelled),
        ("timeout", LifecycleType.timed_out),
    ],
)
async def test_bound_evidence_survives_every_terminal_outcome(
    kind,
    status,
    lifecycle_type,
    tmp_path,
):
    store, engine = await _make_store(kind, tmp_path)
    try:
        lease_epoch = await _put_running(store)
        evidence = _evidence()
        evidence_json = evidence.to_persisted_json()
        evidence_digest = assembly_evidence_digest(evidence)
        assert (
            await store.bind_assembly_evidence(
                "run-1",
                owner_id="worker-1",
                lease_epoch=lease_epoch,
                evidence_json=evidence_json,
                evidence_digest=evidence_digest,
            )
            is BindAssemblyEvidenceOutcome.bound
        )

        result = await store.transition_owned_run_atomic(
            "run-1",
            expected_state_version=lease_epoch,
            expected_statuses=("running",),
            transition=LifecycleTransition(
                lifecycle_type=lifecycle_type,
                status=status,
            ),
            expected_owner_worker_id="worker-1",
            require_unexpired_lease=False,
        )

        assert result.applied is True
        row = await store.get("run-1")
        assert row is not None
        assert row["assembly_evidence_json"] == evidence_json
        assert row["assembly_evidence_digest"] == evidence_digest
    finally:
        if engine is not None:
            await engine.dispose()
