from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from deerflow_extension_api import TenantReferenceV1

from deerflow.community.aio_sandbox.accepted_materializer import (
    AioAcceptedMaterializer,
)
from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV2,
    AcceptedMaterialCapability,
    AcceptedMaterialError,
    AcceptedMaterialExecutionClaimV1,
    AcceptedMaterialRequestV1,
    AcceptedMaterialRequestV2,
    AcceptedSandboxCapabilityProfileV1,
    AcceptedSandboxIsolationFactsV1,
    AcceptedSandboxQualificationV1,
    AcceptedSkillExecutionEvidenceV2,
    AcceptedSkillSandboxBindingV1,
)


def _request(now: datetime) -> AcceptedMaterialRequestV1:
    tenant_digest = "1" * 64
    return AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=TenantReferenceV1(
            version=1,
            public_ref=f"tenant-{tenant_digest[:16]}",
            digest=tenant_digest,
        ),
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=now + timedelta(minutes=5),
    )


def _legacy_evidence() -> AcceptedSkillExecutionEvidenceV2:
    wire: dict[str, object] = {
        "profile": "rwx_verified_copy_v2",
        "attempt_id": "provider-attempt-1",
        "snapshot_id": "3" * 64,
        "run_id": "run-1",
        "generation": 7,
        "pod_uid": "pod-uid",
        "pod_isolation_digest": "6" * 64,
        "lease_uid": "lease-uid",
        "network_policy_uid": "network-policy-uid",
        "network_policy_spec_digest": "7" * 64,
        "evidence_secret_uid": "evidence-secret-uid",
        "evidence_secret_digest": "8" * 64,
        "capability_secret_uid": "capability-secret-uid",
        "capability_secret_digest": "9" * 64,
        "sandbox_image_digest": "5" * 64,
        "accepted_skill_runtime_image_digest": "a" * 64,
        "runtime_image_ids_digest": "b" * 64,
        "verifier_receipt_digest": "c" * 64,
    }
    wire["materialization_evidence_digest"] = hashlib.sha256(
        json.dumps(
            {"version": 2, **wire, "content_digest": "3" * 64},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"),
    ).hexdigest()
    return AcceptedSkillExecutionEvidenceV2(**wire)  # type: ignore[arg-type]


def _profile() -> AcceptedSandboxCapabilityProfileV1:
    return AcceptedSandboxCapabilityProfileV1.build(
        material_capability=AcceptedMaterialCapability.IMMUTABLE_READ_ONLY,
        atomic_provider_ownership_fencing=True,
        atomic_provider_operation_fencing=False,
        authoritative_shared_expiry=True,
        resolved_immutable_image=True,
        restricted_non_root_isolation=True,
        recoverable_resource_lookup=True,
        durable_one_replica=True,
        exact_two=False,
    )


def _qualification(
    profile: AcceptedSandboxCapabilityProfileV1,
    now: datetime,
) -> AcceptedSandboxQualificationV1:
    return AcceptedSandboxQualificationV1.build(
        capability_profile_digest=profile.digest,
        qualification_scope="durable_one_replica_rwx_verified_copy_v2_nonempty_skill",
        artifact_digest="d" * 64,
        topology_digest="e" * 64,
        verified_at=now - timedelta(minutes=1),
        expires_at=now + timedelta(days=1),
    )


def _isolation() -> AcceptedSandboxIsolationFactsV1:
    return AcceptedSandboxIsolationFactsV1.build(
        restricted_non_root=True,
        read_only_accepted_material=True,
        privilege_escalation_disabled=True,
        runtime_class_digest="6" * 64,
        network_policy_digest="7" * 64,
    )


class _AioProviderPort:
    def __init__(self, evidence: AcceptedSkillExecutionEvidenceV2) -> None:
        self.evidence = evidence
        self.sandbox = SimpleNamespace(id="sandbox-1")
        self.destroyed: list[str] = []
        self.acquire_calls = 0

    async def acquire_bound_accepted_skills_async(
        self,
        thread_id: str,
        *,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
        execution_claim: object | None = None,
    ) -> str:
        self.acquire_calls += 1
        assert (thread_id, user_id) == ("thread-ref", "user-ref")
        assert binding.snapshot_id == "3" * 64
        return self.sandbox.id

    def get(self, sandbox_id: str):
        return self.sandbox if sandbox_id == self.sandbox.id else None

    def accepted_skill_execution_evidence(self, sandbox_id: str):
        return self.evidence if sandbox_id == self.sandbox.id else None

    async def validate_accepted_skill_execution_async(self, sandbox_id, evidence):
        return sandbox_id == self.sandbox.id and evidence == self.evidence

    async def renew_accepted_skill_execution_async(self, sandbox_id, evidence):
        return sandbox_id == self.sandbox.id and evidence == self.evidence

    def destroy(self, sandbox_id: str) -> None:
        self.destroyed.append(sandbox_id)


class _FreshTakeoverProviderPort(_AioProviderPort):
    """A new Gateway process with no process-local accepted-attempt secret."""

    def __init__(self, evidence: AcceptedSkillExecutionEvidenceV2) -> None:
        super().__init__(evidence)
        self.takeover_claims: list[object] = []

    async def recover_bound_accepted_skills_async(
        self,
        thread_id: str,
        *,
        user_id: str,
        binding: AcceptedSkillSandboxBindingV1,
        execution_claim: object,
    ) -> str:
        self.takeover_claims.append(execution_claim)
        assert (thread_id, user_id) == ("thread-ref", "user-ref")
        assert binding.snapshot_id == "3" * 64
        return self.sandbox.id


@pytest.mark.asyncio
async def test_aio_adapter_preserves_v2_evidence_and_fences_release() -> None:
    now = datetime(2026, 8, 31, 12, tzinfo=UTC)
    request = _request(now)
    legacy = _legacy_evidence()
    provider = _AioProviderPort(legacy)
    adapter = AioAcceptedMaterializer(
        provider=provider,
        binding_resolver=lambda _request: AcceptedSkillSandboxBindingV1(
            snapshot_id=legacy.snapshot_id,
            run_id=legacy.run_id,
            generation=legacy.generation,
            evidence=object(),
        ),
        clock=lambda: now,
        lease_duration=timedelta(minutes=5),
    )

    assert adapter.capability() is AcceptedMaterialCapability.IMMUTABLE_READ_ONLY
    sandbox, lease, evidence = await adapter.acquire_and_materialize(request)
    assert sandbox is provider.sandbox
    assert lease.provider_instance_ref == "sandbox-1"
    assert lease.ownership_epoch == legacy.generation
    assert evidence.materialization_digest == legacy.materialization_evidence_digest
    assert evidence.verifier_image_digest == legacy.accepted_skill_runtime_image_digest
    assert evidence.runtime_image_digest == legacy.sandbox_image_digest
    assert await adapter.validate(lease, evidence)

    renewed = await adapter.renew(lease)
    assert not await adapter.validate(lease, evidence)
    assert await adapter.validate(renewed, evidence)
    now += timedelta(minutes=6)
    assert not await adapter.validate(renewed, evidence)
    await adapter.release(lease)
    assert provider.destroyed == []
    await adapter.release(renewed)
    assert provider.destroyed == ["sandbox-1"]


@pytest.mark.asyncio
async def test_aio_adapter_emits_handle_free_v2_evidence_for_v2_request() -> None:
    now = datetime(2026, 8, 31, 12, tzinfo=UTC)
    legacy = _legacy_evidence()
    provider = _AioProviderPort(legacy)
    profile = _profile()
    qualification = _qualification(profile, now)
    v1 = _request(now)
    request = AcceptedMaterialRequestV2.build(
        run_id=v1.run_id,
        attempt_id=v1.attempt_id,
        tenant=v1.tenant,
        user_ref=v1.user_ref,
        thread_ref=v1.thread_ref,
        agent_revision_digest=v1.agent_revision_digest,
        skill_snapshot_digest=v1.skill_snapshot_digest,
        skill_scope_digest=v1.skill_scope_digest,
        file_manifest=v1.file_manifest,
        runtime_image_digest=v1.runtime_image_digest,
        lease_expires_at=v1.lease_expires_at,
        accepted_invocation_ref="invocation-safe-ref",
        accepted_invocation_digest="8" * 64,
        tool_plane_base_revision_digest="9" * 64,
        tool_plane_user_overlay_digest="a" * 64,
        tool_plane_projection_digest="b" * 64,
        tool_plane_effective_digest="c" * 64,
        batch_child_attempt_ref=None,
        capability_profile_digest=profile.digest,
    )
    adapter = AioAcceptedMaterializer(
        provider=provider,
        binding_resolver=lambda _request: AcceptedSkillSandboxBindingV1(
            snapshot_id=legacy.snapshot_id,
            run_id=legacy.run_id,
            generation=legacy.generation,
        ),
        clock=lambda: now,
        qualification=qualification,
        isolation=_isolation(),
    )

    _sandbox, lease, evidence = await adapter.acquire_and_materialize(request)

    assert isinstance(evidence, AcceptedExecutionEvidenceV2)
    assert evidence.binds(request, lease)
    assert evidence.qualification_evidence_digest == qualification.digest
    assert evidence.verifier_contract_version == "rwx_verified_copy_v2:accepted_execution_claim_v2"
    assert "sandbox-1" not in json.dumps(evidence.to_persisted(), sort_keys=True)
    assert await adapter.validate(lease, evidence)
    await adapter.release(lease)


@pytest.mark.asyncio
async def test_aio_adapter_rejects_expired_request_before_provider_work() -> None:
    now = datetime(2026, 8, 31, 12, tzinfo=UTC)
    legacy = _legacy_evidence()
    provider = _AioProviderPort(legacy)
    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=TenantReferenceV1(
            version=1,
            public_ref="tenant-" + "1" * 16,
            digest="1" * 64,
        ),
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=now - timedelta(seconds=1),
    )
    adapter = AioAcceptedMaterializer(
        provider=provider,
        binding_resolver=lambda _request: AcceptedSkillSandboxBindingV1(
            snapshot_id=legacy.snapshot_id,
            run_id=legacy.run_id,
            generation=legacy.generation,
        ),
        clock=lambda: now,
    )

    with pytest.raises(
        AcceptedMaterialError,
        match="accepted_material_lease_expired",
    ):
        await adapter.acquire_and_materialize(request)

    assert provider.acquire_calls == 0


@pytest.mark.asyncio
async def test_aio_adapter_destroys_acquired_sandbox_when_evidence_is_missing() -> None:
    now = datetime(2026, 8, 31, 12, tzinfo=UTC)
    request = _request(now)
    legacy = _legacy_evidence()
    provider = _AioProviderPort(legacy)
    provider.evidence = None  # type: ignore[assignment]
    adapter = AioAcceptedMaterializer(
        provider=provider,
        binding_resolver=lambda _request: AcceptedSkillSandboxBindingV1(
            snapshot_id=legacy.snapshot_id,
            run_id=legacy.run_id,
            generation=legacy.generation,
        ),
        clock=lambda: now,
    )

    with pytest.raises(
        AcceptedMaterialError,
        match="accepted_material_evidence_unavailable",
    ):
        await adapter.acquire_and_materialize(request)

    assert provider.destroyed == ["sandbox-1"]


@pytest.mark.asyncio
async def test_aio_release_for_stale_provider_tuple_forgets_local_active_state() -> None:
    now = datetime(2026, 8, 31, 12, tzinfo=UTC)
    request = _request(now)
    legacy = _legacy_evidence()
    provider = _AioProviderPort(legacy)
    adapter = AioAcceptedMaterializer(
        provider=provider,
        binding_resolver=lambda _request: AcceptedSkillSandboxBindingV1(
            snapshot_id=legacy.snapshot_id,
            run_id=legacy.run_id,
            generation=legacy.generation,
        ),
        clock=lambda: now,
    )
    _sandbox, lease, _evidence = await adapter.acquire_and_materialize(request)

    provider.evidence = None  # type: ignore[assignment]
    await adapter.release(lease)
    assert provider.destroyed == []

    provider.evidence = legacy
    _sandbox, recovered_lease, _evidence = await adapter.acquire_and_materialize(
        request,
    )
    assert provider.acquire_calls == 2
    await adapter.release(recovered_lease)
    assert provider.destroyed == ["sandbox-1"]


@pytest.mark.asyncio
async def test_aio_fresh_process_takeover_is_unavailable_without_linearizable_revocation() -> None:
    """Projected Secret rotation cannot authorize a cross-process takeover."""

    now = datetime(2026, 8, 31, 12, tzinfo=UTC)
    legacy = _legacy_evidence()
    binding = AcceptedSkillSandboxBindingV1(
        snapshot_id=legacy.snapshot_id,
        run_id=legacy.run_id,
        generation=legacy.generation,
        evidence=object(),
    )
    first = AioAcceptedMaterializer(
        provider=_AioProviderPort(legacy),
        binding_resolver=lambda _request: binding,
        clock=lambda: now,
    )
    original_request = _request(now)
    original_claim = AcceptedMaterialExecutionClaimV1(
        version=1,
        tenant_digest=original_request.tenant.digest,
        run_id=original_request.run_id,
        owner_worker_id="gateway-a",
        state_version=9,
        execution_takeover=False,
    )
    _sandbox, _lease, original_evidence = await first.acquire_and_materialize(
        original_request,
        execution_claim=original_claim,
    )

    # Discard both the materializer and provider: no in-memory capability survives.
    fresh_provider = _FreshTakeoverProviderPort(legacy)
    recovered = AioAcceptedMaterializer(
        provider=fresh_provider,
        binding_resolver=lambda _request: binding,
        clock=lambda: now + timedelta(seconds=30),
    )
    recovered_request = AcceptedMaterialRequestV1.build(
        run_id=original_request.run_id,
        attempt_id=original_request.attempt_id,
        tenant=original_request.tenant,
        user_ref=original_request.user_ref,
        thread_ref=original_request.thread_ref,
        agent_revision_digest=original_request.agent_revision_digest,
        skill_snapshot_digest=original_request.skill_snapshot_digest,
        skill_scope_digest=original_request.skill_scope_digest,
        file_manifest=original_request.file_manifest,
        runtime_image_digest=original_request.runtime_image_digest,
        lease_expires_at=original_request.lease_expires_at + timedelta(seconds=30),
    )
    claim = AcceptedMaterialExecutionClaimV1(
        version=1,
        tenant_digest=original_request.tenant.digest,
        run_id=original_request.run_id,
        owner_worker_id="gateway-b",
        state_version=12,
        expected_materialization_digest=legacy.materialization_evidence_digest,
        execution_takeover=True,
    )

    with pytest.raises(
        AcceptedMaterialError,
        match="accepted_material_execution_takeover_unavailable",
    ):
        await recovered.acquire_and_materialize(
            recovered_request,
            execution_claim=claim,
        )

    assert fresh_provider.acquire_calls == 0
    assert fresh_provider.takeover_claims == []
    assert original_evidence.provider_kind == "aio_kubernetes"
