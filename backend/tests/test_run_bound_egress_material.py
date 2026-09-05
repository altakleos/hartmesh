"""The accepted Kind's Material carries the run-bound egress allowance."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import requests
from deerflow_extension_api import TenantReferenceV1

from deerflow.community.aio_sandbox.accepted_materializer import AioAcceptedMaterializer
from deerflow.community.aio_sandbox.aio_sandbox_provider import AioSandboxProvider
from deerflow.community.aio_sandbox.remote_backend import RemoteSandboxBackend
from deerflow.runtime.skill_projection import SkillProjectionEvidence, SkillSnapshotProjection
from deerflow.sandbox.accepted_material import (
    AcceptedMaterialCapability,
    AcceptedMaterialRequestV1,
    AcceptedMaterialRequestV2,
    AcceptedSandboxCapabilityProfileV1,
    AcceptedSandboxQualificationV1,
    AcceptedSkillExecutionEvidenceV2,
    AcceptedSkillSandboxBindingV1,
    decode_accepted_material_request,
)
from deerflow.sandbox.egress import EgressAllowanceV1, EgressRuleV1

_TENANT_DIGEST = "1" * 64


def _tenant() -> TenantReferenceV1:
    return TenantReferenceV1(version=1, public_ref=f"tenant-{_TENANT_DIGEST[:16]}", digest=_TENANT_DIGEST)


def _allowance() -> EgressAllowanceV1:
    return EgressAllowanceV1.build(profile="accepted-egress-v1", dns=True, rules=(EgressRuleV1.build(cidr="140.82.112.0/20", protocol="TCP", port=443),))


def _v2_arguments(now: datetime, *, capability_profile_digest: str = "b" * 64) -> dict[str, object]:
    return {
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "tenant": _tenant(),
        "user_ref": "user-ref",
        "thread_ref": "thread-ref",
        "agent_revision_digest": "2" * 64,
        "skill_snapshot_digest": "3" * 64,
        "skill_scope_digest": "4" * 64,
        "file_manifest": (),
        "runtime_image_digest": "5" * 64,
        "lease_expires_at": now + timedelta(minutes=5),
        "accepted_invocation_ref": "invocation-ref",
        "accepted_invocation_digest": "6" * 64,
        "tool_plane_base_revision_digest": "7" * 64,
        "tool_plane_user_overlay_digest": "8" * 64,
        "tool_plane_projection_digest": "9" * 64,
        "tool_plane_effective_digest": "a" * 64,
        "batch_child_attempt_ref": None,
        "capability_profile_digest": capability_profile_digest,
    }


def test_material_request_binds_the_allowance_into_its_digest() -> None:
    now = datetime(2026, 9, 5, 12, tzinfo=UTC)
    allowance = _allowance()
    bound = AcceptedMaterialRequestV2.build(**_v2_arguments(now), egress_allowance=allowance)
    unbound = AcceptedMaterialRequestV2.build(**_v2_arguments(now))

    assert bound.egress_allowance == allowance
    assert unbound.egress_allowance is None
    assert bound.digest != unbound.digest
    assert "egress_allowance" not in unbound.to_persisted()
    assert bound.to_persisted()["egress_allowance"] == allowance.to_json()
    assert AcceptedMaterialRequestV2.from_persisted(bound.to_persisted()) == bound
    assert decode_accepted_material_request(unbound.to_persisted()) == unbound

    forged = json.loads(json.dumps(bound.to_persisted()))
    forged["egress_allowance"]["rules"][0]["port"] = 80
    with pytest.raises(ValueError, match="egress"):
        AcceptedMaterialRequestV2.from_persisted(forged)
    with pytest.raises(TypeError, match="egress_allowance"):
        AcceptedMaterialRequestV2.build(**_v2_arguments(now), egress_allowance=allowance.to_json())  # type: ignore[arg-type]


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
    wire["materialization_evidence_digest"] = hashlib.sha256(json.dumps({"version": 2, **wire, "content_digest": "3" * 64}, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return AcceptedSkillExecutionEvidenceV2(**wire)  # type: ignore[arg-type]


class _ProviderPort:
    def __init__(self, evidence: AcceptedSkillExecutionEvidenceV2) -> None:
        self.evidence = evidence
        self.sandbox = SimpleNamespace(id="sandbox-1")
        self.allowances: list[object] = []

    async def provision_accepted_skills_async(
        self, thread_id: str, *, user_id: str, binding: AcceptedSkillSandboxBindingV1, execution_claim: object | None = None, resource_scope_ref: str | None = None, egress_allowance: EgressAllowanceV1 | None = None
    ) -> str:
        self.allowances.append(egress_allowance)
        return self.sandbox.id

    def get(self, sandbox_id: str):
        return self.sandbox if sandbox_id == self.sandbox.id else None

    def accepted_skill_execution_evidence(self, sandbox_id: str):
        return self.evidence if sandbox_id == self.sandbox.id else None

    async def validate_accepted_skill_execution_async(self, sandbox_id, evidence):
        return True

    def destroy(self, sandbox_id: str) -> None:
        return None


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


def _qualification(now: datetime) -> AcceptedSandboxQualificationV1:
    return AcceptedSandboxQualificationV1.build(
        capability_profile_digest=_profile().digest,
        qualification_scope="durable_one_replica_rwx_verified_copy_v2_nonempty_skill",
        artifact_digest="d" * 64,
        topology_digest="e" * 64,
        verified_at=now - timedelta(minutes=1),
        expires_at=now + timedelta(days=1),
    )


@pytest.mark.asyncio
async def test_materializer_provisions_the_request_allowance_or_deny_all() -> None:
    now = datetime(2026, 9, 5, 12, tzinfo=UTC)
    legacy = _legacy_evidence()
    provider = _ProviderPort(legacy)
    binding = AcceptedSkillSandboxBindingV1(snapshot_id=legacy.snapshot_id, run_id=legacy.run_id, generation=legacy.generation, evidence=object())
    adapter = AioAcceptedMaterializer(provider=provider, binding_resolver=lambda _request: binding, clock=lambda: now, qualification=_qualification(now))  # type: ignore[arg-type]
    arguments = _v2_arguments(now, capability_profile_digest=_profile().digest)

    allowance = _allowance()
    await adapter.acquire_and_materialize(AcceptedMaterialRequestV2.build(**arguments, egress_allowance=allowance))
    assert provider.allowances == [allowance]

    unbound = AcceptedMaterialRequestV2.build(**{**arguments, "attempt_id": "attempt-2"})
    await adapter.acquire_and_materialize(unbound)
    assert provider.allowances[-1] == EgressAllowanceV1.deny_all()

    v1_arguments = {key: _v2_arguments(now)[key] for key in ("run_id", "tenant", "user_ref", "thread_ref", "agent_revision_digest", "skill_snapshot_digest", "skill_scope_digest", "file_manifest", "runtime_image_digest", "lease_expires_at")}
    await adapter.acquire_and_materialize(AcceptedMaterialRequestV1.build(**v1_arguments, attempt_id="attempt-3"))
    assert provider.allowances[-1] == EgressAllowanceV1.deny_all()


def _projection_evidence() -> SkillProjectionEvidence:
    content = b"# accepted\n"
    header = json.dumps(["public", "demo", "SKILL.md", "regular"], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    tree = hashlib.sha256()
    tree.update(len(header).to_bytes(4, "big"))
    tree.update(header)
    tree.update(len(content).to_bytes(8, "big"))
    tree.update(content)
    projection = SkillSnapshotProjection(name="demo", category="public", relative_path="demo", manifest_digest=hashlib.sha256(content).hexdigest(), content_digest=tree.hexdigest(), file_count=1, total_bytes=len(content))
    snapshot_id = hashlib.sha256(json.dumps({"version": 1, "skills": [projection.to_json()]}, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")).hexdigest()
    return SkillProjectionEvidence(snapshot_id=snapshot_id, content_digest=snapshot_id, projections=(projection,), file_count=1, total_bytes=len(content))


def _receipt_wire(evidence: SkillProjectionEvidence) -> dict[str, object]:
    receipt: dict[str, object] = {
        "version": 2,
        "profile": "rwx_verified_copy_v2",
        "attempt_id": "sandbox-sandbox-1-accepted-attempt",
        "snapshot_id": evidence.snapshot_id,
        "content_digest": evidence.content_digest,
        "run_id": "run-1",
        "generation": 7,
        "pod_uid": "pod-uid-1",
        "pod_isolation_digest": "1" * 64,
        "lease_uid": "lease-uid-1",
        "network_policy_uid": "network-policy-uid-1",
        "network_policy_spec_digest": "2" * 64,
        "evidence_secret_uid": "evidence-secret-uid-1",
        "evidence_secret_digest": "3" * 64,
        "capability_secret_uid": "capability-secret-uid-1",
        "capability_secret_digest": "4" * 64,
        "sandbox_image_digest": "5" * 64,
        "accepted_skill_runtime_image_digest": "6" * 64,
        "runtime_image_ids_digest": "7" * 64,
        "verifier_receipt_digest": "8" * 64,
    }
    receipt["materialization_evidence_digest"] = hashlib.sha256(json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return receipt


class _Response:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload
        self.ok = True
        self.status_code = 200
        self.text = ""

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def _remote_backend(monkeypatch: pytest.MonkeyPatch, *, echo: object, captured: dict[str, object], deleted: list[str]) -> RemoteSandboxBackend:
    evidence = _projection_evidence()
    response: dict[str, object] = {"sandbox_id": "sandbox-1", "sandbox_url": "http://10.0.0.8:8081", "status": "Pending", "accepted_skill_material": _receipt_wire(evidence)}
    if echo is not None:
        response["egress_allowance_digest"] = echo

    def post(_url: str, *, json: dict[str, object], **_kwargs):
        captured.update(json)
        return _Response(response)

    def delete(url: str, **_kwargs):
        deleted.append(url)
        return _Response({})

    monkeypatch.setattr(requests, "post", post)
    monkeypatch.setattr(requests, "delete", delete)
    monkeypatch.setattr("deerflow.community.aio_sandbox.remote_backend.user_should_see_legacy_skills", lambda _user_id: False)
    backend = RemoteSandboxBackend("http://provisioner:8002")
    backend._test_binding = AcceptedSkillSandboxBindingV1(snapshot_id=evidence.snapshot_id, run_id="run-1", generation=7, evidence=evidence)  # type: ignore[attr-defined]
    return backend


def test_remote_backend_sends_the_allowance_and_requires_the_provisioner_to_attest_it(monkeypatch: pytest.MonkeyPatch) -> None:
    allowance = _allowance()
    captured: dict[str, object] = {}
    deleted: list[str] = []
    backend = _remote_backend(monkeypatch, echo=allowance.digest, captured=captured, deleted=deleted)
    info = backend.create("thread-1", "sandbox-1", user_id="owner-1", accepted_skill_binding=backend._test_binding, egress_allowance=allowance)  # type: ignore[attr-defined]
    assert captured["egress_allowance"] == allowance.to_json()
    assert info.accepted_skill_material is not None
    assert deleted == []

    for echo in (None, "0" * 64):
        captured.clear()
        backend = _remote_backend(monkeypatch, echo=echo, captured=captured, deleted=deleted)
        with pytest.raises(RuntimeError, match="accepted_egress_allowance_unattested"):
            backend.create("thread-1", "sandbox-1", user_id="owner-1", accepted_skill_binding=backend._test_binding, egress_allowance=allowance)  # type: ignore[attr-defined]
        assert deleted[-1].endswith("/api/sandboxes/sandbox-1")
        assert "sandbox-1" not in backend._attempt_capabilities

    captured.clear()
    backend = _remote_backend(monkeypatch, echo=None, captured=captured, deleted=deleted)
    backend.create("thread-1", "sandbox-1", user_id="owner-1", accepted_skill_binding=backend._test_binding)  # type: ignore[attr-defined]
    assert "egress_allowance" not in captured


@pytest.mark.asyncio
async def test_provider_forwards_the_allowance_to_the_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = object.__new__(AioSandboxProvider)
    seen: dict[str, object] = {}

    def create_sandbox(thread_id, sandbox_id, *, user_id=None, accepted_skills_only=False, accepted_skill_binding=None, accepted_execution_claim=None, identity_thread_id=None, egress_allowance=None):
        seen.update({"thread_id": thread_id, "sandbox_id": sandbox_id, "egress_allowance": egress_allowance, "binding": accepted_skill_binding, "identity_thread_id": identity_thread_id})
        return sandbox_id

    monkeypatch.setattr(
        provider,
        "_acquire_accepted_skills_internal",
        lambda thread_id, *, user_id, binding, execution_claim=None, resource_scope_ref=None, egress_allowance=None: create_sandbox(
            thread_id, "sandbox-accepted", user_id=user_id, accepted_skill_binding=binding, accepted_execution_claim=execution_claim, egress_allowance=egress_allowance
        ),
    )
    monkeypatch.setattr(provider, "bind_accepted_skill_snapshot", lambda *args, **kwargs: None)
    monkeypatch.setattr(provider, "_accepted_resource_thread_id", lambda thread_id, resource_scope_ref: thread_id)
    allowance = _allowance()
    binding = AcceptedSkillSandboxBindingV1(snapshot_id="3" * 64, run_id="run-1", generation=7, evidence=object())
    sandbox_id = await provider.provision_accepted_skills_async("thread-1", user_id="owner-1", binding=binding, egress_allowance=allowance)
    assert sandbox_id == "sandbox-accepted"
    assert seen["egress_allowance"] == allowance
