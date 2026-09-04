from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from _subagent_batch_helpers import make_parent_batch_request
from sqlalchemy import select, update

from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config
from deerflow.persistence.subagent_batches import (
    SubagentBatchAttemptRow,
    SubagentBatchItemRow,
    SubagentBatchRepository,
)
from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV2,
    AcceptedMaterialLeaseV1,
    AcceptedMaterialRequestV2,
    AcceptedSandboxCapabilityProfileV1,
    AcceptedSandboxIsolationFactsV1,
    AcceptedSandboxLifecycleKind,
    AcceptedSandboxLifecycleObservationV1,
    AcceptedSandboxQualificationV1,
    accepted_scope_reference,
)
from deerflow.subagents.batch_acceptance import (
    AcceptedBatchV1,
    ParentBoundBatchExecutionV1,
)


@pytest_asyncio.fixture(autouse=True)
async def _close_engine() -> None:
    yield
    await close_engine()


async def _repo(tmp_path) -> SubagentBatchRepository:
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    sf = get_session_factory()
    assert sf is not None
    return SubagentBatchRepository(sf)


async def _create(
    repo: SubagentBatchRepository,
    *,
    count: int = 4,
    max_live: int = 2,
    max_running: int = 1,
    max_attempts: int = 2,
) -> dict:
    return await repo.create_batch(
        batch_id="batch-1",
        user_id="user-1",
        thread_id="thread-1",
        run_id="run-1",
        tool_call_id="call-1",
        submission_key="run-1:call-1",
        title="Research records",
        subagent_type="general-purpose",
        items=[{"key": f"item-{i}", "prompt": f"Process {i}"} for i in range(count)],
        max_live_items=max_live,
        max_running_items=max_running,
        max_attempts=max_attempts,
        execution_spec={
            "subagent_config": {
                "name": "general-purpose",
                "description": "test",
                "system_prompt": "private instructions",
            },
            "authz_attributes": {"tenant": "private-tenant"},
        },
    )


@pytest.mark.asyncio
async def test_claim_separates_total_live_leased_and_running(tmp_path) -> None:
    repo = await _repo(tmp_path)
    created = await _create(repo)
    assert created["counts"]["pending"] == 4

    now = datetime.now(UTC)
    claimed = await repo.claim_items(now=now, lease_owner="worker-1", lease_seconds=60, limit=10)
    assert len(claimed) == 1
    assert claimed[0]["status"] == "leased"

    batch = await repo.get_batch("batch-1", user_id="user-1")
    assert batch is not None
    assert batch["counts"] == {
        "pending": 2,
        "queued": 1,
        "leased": 1,
        "running": 0,
        "succeeded": 0,
        "failed": 0,
        "cancelled": 0,
    }
    assert await repo.mark_item_running(claimed[0]["id"], lease_owner="worker-1", now=now)

    while_full = await repo.claim_items(
        now=now + timedelta(seconds=1),
        lease_owner="worker-2",
        lease_seconds=60,
        limit=10,
    )
    assert while_full == []


@pytest.mark.asyncio
async def test_expired_lease_is_recovered_with_stable_item_identity(tmp_path) -> None:
    repo = await _repo(tmp_path)
    await _create(repo, count=1, max_live=1, max_running=1)
    now = datetime.now(UTC)
    first = await repo.claim_items(now=now, lease_owner="worker-1", lease_seconds=30, limit=1)

    reclaimed = await repo.claim_items(
        now=now + timedelta(seconds=31),
        lease_owner="worker-2",
        lease_seconds=30,
        limit=1,
    )
    assert len(reclaimed) == 1
    assert reclaimed[0]["id"] == first[0]["id"]
    assert reclaimed[0]["item_key"] == "item-0"
    assert reclaimed[0]["attempt"] == 2


@pytest.mark.asyncio
async def test_finalize_retries_then_terminalizes_and_completes_batch(tmp_path) -> None:
    repo = await _repo(tmp_path)
    await _create(repo, count=1, max_live=1, max_running=1)
    now = datetime.now(UTC)
    first = (await repo.claim_items(now=now, lease_owner="worker-1", lease_seconds=60, limit=1))[0]
    await repo.finalize_item(
        first["id"],
        lease_owner="worker-1",
        succeeded=False,
        result=None,
        result_preview=None,
        result_truncated=False,
        error="temporary",
        stop_reason=None,
        token_usage=None,
        model_name="model-a",
        completed_at=now,
    )
    item = (await repo.list_items("batch-1", user_id="user-1"))[0]
    assert item["status"] == "queued"

    second = (await repo.claim_items(now=now + timedelta(seconds=1), lease_owner="worker-2", lease_seconds=60, limit=1))[0]
    await repo.finalize_item(
        second["id"],
        lease_owner="worker-2",
        succeeded=True,
        result="done",
        result_preview="done",
        result_truncated=False,
        error=None,
        stop_reason=None,
        token_usage={"total_tokens": 12},
        model_name="model-a",
        completed_at=now + timedelta(seconds=2),
    )
    batch = await repo.get_batch("batch-1", user_id="user-1")
    assert batch is not None
    assert batch["status"] == "completed"
    assert batch["counts"]["succeeded"] == 1


@pytest.mark.asyncio
async def test_pause_resume_cancel_and_owner_scope(tmp_path) -> None:
    repo = await _repo(tmp_path)
    await _create(repo, count=2, max_live=2, max_running=1)
    paused = await repo.pause_batch("batch-1", user_id="user-1")
    assert paused is not None and paused["status"] == "paused"
    assert await repo.claim_items(now=datetime.now(UTC), lease_owner="worker", lease_seconds=60, limit=1) == []
    resumed = await repo.resume_batch("batch-1", user_id="user-1")
    assert resumed is not None and resumed["status"] == "queued"
    cancelled = await repo.cancel_batch("batch-1", user_id="user-1")
    assert cancelled is not None and cancelled["status"] == "cancelled"
    assert cancelled["counts"]["cancelled"] == 2
    assert await repo.get_batch("batch-1", user_id="other") is None


@pytest.mark.asyncio
async def test_cancel_terminalizes_in_flight_items_and_fences_stale_completion(tmp_path) -> None:
    repo = await _repo(tmp_path)
    await _create(repo, count=1, max_live=1, max_running=1)
    now = datetime.now(UTC)
    claimed = (await repo.claim_items(now=now, lease_owner="worker-1", lease_seconds=60, limit=1))[0]
    assert await repo.mark_item_running(claimed["id"], lease_owner="worker-1", now=now)

    cancelled = await repo.cancel_batch("batch-1", user_id="user-1")

    assert cancelled is not None
    assert cancelled["counts"]["cancelled"] == 1
    item = (await repo.list_items("batch-1", user_id="user-1"))[0]
    assert item["status"] == "cancelled"
    assert not await repo.finalize_item(
        claimed["id"],
        lease_owner="worker-1",
        succeeded=True,
        result="late result",
        result_preview="late result",
        result_truncated=False,
        error=None,
        stop_reason=None,
        token_usage=None,
        model_name="model-a",
        completed_at=now + timedelta(seconds=1),
    )


@pytest.mark.asyncio
async def test_executor_admission_failure_requeues_without_consuming_attempt(tmp_path) -> None:
    repo = await _repo(tmp_path)
    await _create(repo, count=1, max_live=1, max_running=1)
    now = datetime.now(UTC)
    first = (await repo.claim_items(now=now, lease_owner="worker-1", lease_seconds=60, limit=1))[0]
    assert first["attempt"] == 1

    assert await repo.requeue_item_after_admission_failure(
        first["id"],
        lease_owner="worker-1",
        error="Process-wide subagent capacity is full",
        now=now,
    )
    queued = (await repo.list_items("batch-1", user_id="user-1"))[0]
    assert queued["status"] == "queued"
    assert queued["attempt"] == 0

    second = (await repo.claim_items(now=now + timedelta(seconds=1), lease_owner="worker-2", lease_seconds=60, limit=1))[0]
    assert second["attempt"] == 1


@pytest.mark.asyncio
async def test_all_failed_items_mark_batch_failed(tmp_path) -> None:
    repo = await _repo(tmp_path)
    await _create(repo, count=1, max_live=1, max_running=1, max_attempts=1)
    now = datetime.now(UTC)
    item = (await repo.claim_items(now=now, lease_owner="worker-1", lease_seconds=60, limit=1))[0]

    assert await repo.finalize_item(
        item["id"],
        lease_owner="worker-1",
        succeeded=False,
        result=None,
        result_preview=None,
        result_truncated=False,
        error="permanent",
        stop_reason=None,
        token_usage=None,
        model_name="model-a",
        completed_at=now,
    )
    batch = await repo.get_batch("batch-1", user_id="user-1")
    assert batch is not None
    assert batch["status"] == "failed"


@pytest.mark.asyncio
async def test_manual_retry_cannot_reset_an_accepted_attempt_limit(tmp_path) -> None:
    request = make_parent_batch_request()
    request = replace(
        request,
        limits=replace(request.limits, max_attempts=1),
    )
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    sf = get_session_factory()
    assert sf is not None
    repo = SubagentBatchRepository(sf, tenant=request.tenant)
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="accepted-batch",
    )
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    await repo.accept_batch(
        accepted=accepted,
        execution=execution,
        item_requests=request.items,
        user_id=request.user_id,
        submission_key=request.submission_key,
        title=request.title,
        subagent_type=request.subagent_name,
    )

    claimed = (
        await repo.claim_items(
            now=datetime.now(UTC),
            lease_owner="worker-1",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    assert await repo.mark_item_running(
        claimed["id"],
        lease_owner="worker-1",
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        now=datetime.now(UTC),
    )
    assert await repo.finalize_item(
        claimed["id"],
        lease_owner="worker-1",
        attempt_id=claimed["attempt_id"],
        lease_epoch=claimed["lease_epoch"],
        succeeded=False,
        result=None,
        result_preview=None,
        result_truncated=False,
        error="permanent",
        stop_reason=None,
        token_usage=None,
        model_name="model-a",
        completed_at=datetime.now(UTC),
    )

    assert (
        await repo.retry_item(
            accepted.batch_id,
            claimed["id"],
            user_id=request.user_id,
        )
        is None
    )
    items = await repo.list_items(accepted.batch_id, user_id=request.user_id)
    attempts = await repo.list_attempts(accepted.batch_id, user_id=request.user_id)
    assert items is not None
    assert items[0]["status"] == "failed"
    assert items[0]["attempt"] == 1
    assert attempts is not None
    assert [(attempt["attempt_number"], attempt["consumed"]) for attempt in attempts] == [(1, True)]
    assert (
        await repo.claim_items(
            now=datetime.now(UTC),
            lease_owner="worker-2",
            lease_seconds=60,
            limit=1,
        )
        == []
    )


@pytest.mark.asyncio
async def test_item_attempt_authority_is_checked_without_renewing_the_lease(
    tmp_path,
) -> None:
    request = make_parent_batch_request()
    await init_engine_from_config(DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    sf = get_session_factory()
    assert sf is not None
    repo = SubagentBatchRepository(sf, tenant=request.tenant)
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="accepted-batch-authority",
    )
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    await repo.accept_batch(
        accepted=accepted,
        execution=execution,
        item_requests=request.items,
        user_id=request.user_id,
        submission_key=request.submission_key,
        title=request.title,
        subagent_type=request.subagent_name,
    )
    item = (
        await repo.claim_items(
            now=datetime.now(UTC),
            lease_owner="worker-1",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    authority = {
        "item_id": item["id"],
        "attempt_id": item["attempt_id"],
        "lease_epoch": item["lease_epoch"],
        "lease_owner": "worker-1",
    }

    assert await repo.item_attempt_authorized(**authority)
    assert await repo.mark_item_running(
        item["id"],
        attempt_id=item["attempt_id"],
        lease_epoch=item["lease_epoch"],
        lease_owner="worker-1",
        now=None,
    )
    assert await repo.item_attempt_authorized(**authority)
    assert not await repo.item_attempt_authorized(**{**authority, "lease_epoch": item["lease_epoch"] + 1})

    await repo.cancel_batch(accepted.batch_id, user_id=request.user_id)

    assert not await repo.item_attempt_authorized(**authority)


@pytest.mark.asyncio
async def test_attempt_persists_accepted_sandbox_evidence_and_lifecycle_after_terminal(
    tmp_path,
) -> None:
    request = make_parent_batch_request()
    await init_engine_from_config(
        DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)),
    )
    sf = get_session_factory()
    assert sf is not None
    repo = SubagentBatchRepository(sf, tenant=request.tenant)
    accepted = AcceptedBatchV1.from_parent_request(
        request,
        batch_id="accepted-batch-sandbox-evidence",
    )
    execution = ParentBoundBatchExecutionV1.from_parent_request(
        request,
        accepted=accepted,
    )
    await repo.accept_batch(
        accepted=accepted,
        execution=execution,
        item_requests=request.items,
        user_id=request.user_id,
        submission_key=request.submission_key,
        title=request.title,
        subagent_type=request.subagent_name,
    )
    claimed = (
        await repo.claim_items(
            now=None,
            lease_owner="worker-1",
            lease_seconds=60,
            limit=1,
        )
    )[0]
    now = datetime.now(UTC)
    child_identity = f"{accepted.batch_id}:{claimed['id']}:{claimed['attempt_id']}:{claimed['lease_epoch']}:{claimed['request_digest']}"
    attempt_ref = accepted_scope_reference(
        request.tenant,
        kind="attempt",
        value=child_identity,
    )
    batch_child_ref = accepted_scope_reference(
        request.tenant,
        kind="batch-child",
        value=child_identity,
    )
    profile = AcceptedSandboxCapabilityProfileV1.build(
        material_capability="immutable_read_only",
        atomic_provider_ownership_fencing=True,
        atomic_provider_operation_fencing=False,
        authoritative_shared_expiry=True,
        resolved_immutable_image=True,
        restricted_non_root_isolation=True,
        recoverable_resource_lookup=True,
        durable_one_replica=True,
        exact_two=False,
    )
    material_request = AcceptedMaterialRequestV2.build(
        run_id=accepted.parent_run_id,
        attempt_id=attempt_ref,
        tenant=request.tenant,
        user_ref=accepted_scope_reference(
            request.tenant,
            kind="user",
            value=request.user_id,
        ),
        thread_ref=accepted_scope_reference(
            request.tenant,
            kind="thread",
            value=request.thread_id,
        ),
        agent_revision_digest=accepted.parent_agent_revision_digest,
        skill_snapshot_digest="1" * 64,
        skill_scope_digest=accepted.skill_scope_digest,
        file_manifest=(),
        runtime_image_digest="2" * 64,
        lease_expires_at=now + timedelta(minutes=5),
        accepted_invocation_ref=accepted_scope_reference(
            request.tenant,
            kind="invocation",
            value=(f"{accepted.parent_run_id}:{accepted.parent_invocation_digest}"),
        ),
        accepted_invocation_digest=accepted.parent_invocation_digest,
        tool_plane_base_revision_digest="3" * 64,
        tool_plane_user_overlay_digest="4" * 64,
        tool_plane_projection_digest="5" * 64,
        tool_plane_effective_digest="6" * 64,
        batch_child_attempt_ref=batch_child_ref,
        capability_profile_digest=profile.digest,
    )
    lease = AcceptedMaterialLeaseV1(
        version=1,
        provider_kind="qualified-test",
        provider_instance_ref="raw-provider-resource",
        ownership_epoch=9,
        lease_expires_at=now + timedelta(minutes=5),
        opaque_renewal_handle=object(),
    )
    qualification = AcceptedSandboxQualificationV1.build(
        capability_profile_digest=profile.digest,
        qualification_scope="contract_test_only",
        artifact_digest="8" * 64,
        topology_digest="9" * 64,
        verified_at=now,
        expires_at=now + timedelta(hours=1),
    )
    evidence = AcceptedExecutionEvidenceV2.build(
        request=material_request,
        lease=lease,
        materialization_digest="a" * 64,
        verifier_image_digest="b" * 64,
        verifier_contract_version="contract-test-v1",
        read_only_proof_digest="c" * 64,
        qualification=qualification,
        isolation=AcceptedSandboxIsolationFactsV1.build(
            restricted_non_root=True,
            read_only_accepted_material=True,
            privilege_escalation_disabled=True,
            runtime_class_digest="d" * 64,
            network_policy_digest="e" * 64,
        ),
    )
    acquired = AcceptedSandboxLifecycleObservationV1.build(
        evidence=evidence,
        kind=AcceptedSandboxLifecycleKind.ACQUIRED,
        observed_at=now,
    )
    authority = {
        "item_id": claimed["id"],
        "attempt_id": claimed["attempt_id"],
        "lease_epoch": claimed["lease_epoch"],
        "lease_owner": "worker-1",
    }

    assert not await repo.attach_item_sandbox_evidence(
        **{**authority, "lease_epoch": claimed["lease_epoch"] + 1},
        request=material_request,
        evidence=evidence,
        observations=(acquired,),
    )
    assert await repo.attach_item_sandbox_evidence(
        **authority,
        request=material_request,
        evidence=evidence,
        observations=(acquired,),
    )
    conflicting_evidence = AcceptedExecutionEvidenceV2.build(
        request=material_request,
        lease=lease,
        materialization_digest="f" * 64,
        verifier_image_digest="b" * 64,
        verifier_contract_version="contract-test-v1",
        read_only_proof_digest="c" * 64,
        qualification=qualification,
        isolation=evidence.isolation,
    )
    conflicting_acquired = AcceptedSandboxLifecycleObservationV1.build(
        evidence=conflicting_evidence,
        kind=AcceptedSandboxLifecycleKind.ACQUIRED,
        observed_at=now,
    )
    assert not await repo.attach_item_sandbox_evidence(
        **authority,
        request=material_request,
        evidence=conflicting_evidence,
        observations=(conflicting_acquired,),
    )
    async with sf() as session:
        await session.execute(
            update(SubagentBatchItemRow).where(SubagentBatchItemRow.id == claimed["id"]).values(lease_expires_at=now - timedelta(seconds=1)),
        )
        await session.commit()
    replacement = await repo.claim_items(
        now=None,
        lease_owner="worker-2",
        lease_seconds=60,
        limit=1,
    )
    assert len(replacement) == 1
    assert replacement[0]["attempt_id"] != claimed["attempt_id"]
    released = AcceptedSandboxLifecycleObservationV1.build(
        evidence=evidence,
        kind=AcceptedSandboxLifecycleKind.RELEASED,
        observed_at=now + timedelta(seconds=1),
    )
    assert not await repo.append_item_sandbox_lifecycle(
        **{**authority, "lease_owner": "other-worker"},
        execution_evidence_digest=evidence.digest,
        observations=(acquired, released),
    )
    assert await repo.append_item_sandbox_lifecycle(
        **authority,
        execution_evidence_digest=evidence.digest,
        observations=(acquired, released),
    )
    overflow = tuple(
        AcceptedSandboxLifecycleObservationV1.build(
            evidence=evidence,
            kind=AcceptedSandboxLifecycleKind.RELEASED,
            observed_at=now + timedelta(seconds=offset),
        )
        for offset in range(2, 10)
    )
    assert not await repo.append_item_sandbox_lifecycle(
        **authority,
        execution_evidence_digest=evidence.digest,
        observations=overflow,
    )

    attempts = await repo.list_attempts(
        accepted.batch_id,
        user_id=request.user_id,
    )
    assert attempts is not None
    assert attempts[0]["accepted_material_request_digest"] == (material_request.digest)
    assert attempts[0]["accepted_execution_evidence_digest"] == evidence.digest
    assert attempts[0]["accepted_sandbox_lifecycle_count"] == 3
    serialized = str(attempts)
    assert "raw-provider-resource" not in serialized

    observations = await repo.list_observations(
        accepted.batch_id,
        user_id=request.user_id,
    )
    assert observations is not None
    sandbox_events = [row for row in observations if row["event"] == "sandbox.lifecycle"]
    assert {row["state"] for row in sandbox_events} == {
        "acquired",
        "orphaned",
        "released",
    }
    assert len(sandbox_events) == 3
    assert all(row["evidence_digest"] == evidence.digest for row in sandbox_events)

    async with sf() as session:
        attempt_row = (
            await session.execute(
                select(SubagentBatchAttemptRow).where(
                    SubagentBatchAttemptRow.id == claimed["attempt_id"],
                ),
            )
        ).scalar_one()
        assert attempt_row.accepted_material_request_json == (material_request.to_persisted())
        assert attempt_row.accepted_execution_evidence_json == (evidence.to_persisted())
        assert [row["kind"] for row in attempt_row.accepted_sandbox_lifecycle_json] == ["acquired", "orphaned", "released"]
        persisted = str(
            (
                attempt_row.accepted_material_request_json,
                attempt_row.accepted_execution_evidence_json,
                attempt_row.accepted_sandbox_lifecycle_json,
            ),
        )
        assert "raw-provider-resource" not in persisted


@pytest.mark.asyncio
async def test_public_projections_omit_execution_context_and_full_results(tmp_path) -> None:
    repo = await _repo(tmp_path)
    created = await _create(repo, count=1, max_live=1, max_running=1)
    assert "execution_spec" not in created
    assert "user_id" not in created
    assert "submission_key" not in created
    assert "run_id" not in created
    assert "tool_call_id" not in created

    now = datetime.now(UTC)
    item = (await repo.claim_items(now=now, lease_owner="worker-1", lease_seconds=60, limit=1))[0]
    assert await repo.finalize_item(
        item["id"],
        lease_owner="worker-1",
        succeeded=True,
        result="full private result",
        result_preview="preview",
        result_truncated=False,
        error=None,
        stop_reason=None,
        token_usage=None,
        model_name="model-a",
        completed_at=now,
    )

    public_batch = await repo.get_batch("batch-1", user_id="user-1")
    assert public_batch is not None
    assert "execution_spec" not in public_batch
    public_item = (await repo.list_items("batch-1", user_id="user-1"))[0]
    assert "result_preview" not in public_item
    assert "result" not in public_item
    assert "lease_owner" not in public_item
    export_item = (await repo.list_items("batch-1", user_id="user-1", include_result=True))[0]
    assert export_item["result_preview"] == "preview"
    assert export_item["result"] == "full private result"


@pytest.mark.asyncio
async def test_duplicate_submission_key_returns_original_batch(tmp_path) -> None:
    repo = await _repo(tmp_path)
    original = await _create(repo, count=1)
    duplicate = await repo.create_batch(
        batch_id="batch-2",
        user_id="user-1",
        thread_id="thread-1",
        run_id="run-1",
        tool_call_id="call-1",
        submission_key="run-1:call-1",
        title="Duplicate retry",
        subagent_type="general-purpose",
        items=[{"key": "different", "prompt": "Must not be inserted"}],
        max_live_items=1,
        max_running_items=1,
        max_attempts=2,
        execution_spec={"subagent_config": {"name": "general-purpose", "description": "test"}},
    )

    assert duplicate["id"] == original["id"] == "batch-1"
    items = await repo.list_items("batch-1", user_id="user-1", include_prompt=True)
    assert items is not None
    assert [item["item_key"] for item in items] == ["item-0"]
