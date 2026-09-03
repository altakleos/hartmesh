from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from deerflow_extension_api import TenantReferenceV1

from deerflow.runtime.runs.store.base import (
    LifecycleTransition,
    LifecycleType,
    build_lifecycle_payload,
)
from deerflow.sandbox.accepted_material import (
    AcceptedExecutionEvidenceV1,
    AcceptedExecutionEvidenceV2,
    AcceptedFileType,
    AcceptedFileV1,
    AcceptedMaterialCapability,
    AcceptedMaterialError,
    AcceptedMaterialExecutionClaimV1,
    AcceptedMaterializerSelection,
    AcceptedMaterialLeaseV1,
    AcceptedMaterialRequestV1,
    AcceptedMaterialRequestV2,
    AcceptedSandboxAuthorityLostError,
    AcceptedSandboxCapabilityProfileV1,
    AcceptedSandboxIsolationFactsV1,
    AcceptedSandboxLifecycleKind,
    AcceptedSandboxLifecycleObservationV1,
    AcceptedSandboxOperationV1,
    AcceptedSandboxQualificationV1,
    AcceptedSandboxSession,
    AcceptedSkillSandboxBindingV1,
    InMemoryAcceptedMaterializer,
    InMemoryAcceptedMaterialState,
    accepted_execution_evidence_reference,
    capture_accepted_file_manifest,
    decode_accepted_execution_evidence,
    decode_accepted_material_request,
    resolve_accepted_materializer,
    validate_accepted_materialization,
)
from deerflow.sandbox.sandbox import Sandbox
from deerflow.sandbox.sandbox_provider import AcceptedSkillMaterialCapability


def _tenant() -> TenantReferenceV1:
    digest = "1" * 64
    return TenantReferenceV1(
        version=1,
        public_ref=f"tenant-{digest[:16]}",
        digest=digest,
    )


def _regular_file(path: str, content: bytes = b"content") -> AcceptedFileV1:
    return AcceptedFileV1(
        version=1,
        path=path,
        file_type=AcceptedFileType.REGULAR,
        size=len(content),
        mode=0o444,
        digest=hashlib.sha256(content).hexdigest(),
    )


def _capability_profile() -> AcceptedSandboxCapabilityProfileV1:
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
) -> AcceptedSandboxQualificationV1:
    return AcceptedSandboxQualificationV1.build(
        capability_profile_digest=profile.digest,
        qualification_scope="contract_test_only",
        artifact_digest="a" * 64,
        topology_digest="b" * 64,
        verified_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
        expires_at=datetime(2030, 1, 1, tzinfo=UTC),
    )


def test_accepted_material_request_has_one_canonical_persisted_form() -> None:
    content = b"# demo\n"
    accepted_file = AcceptedFileV1(
        version=1,
        path="SKILL.md",
        file_type=AcceptedFileType.REGULAR,
        size=len(content),
        mode=0o444,
        digest=hashlib.sha256(content).hexdigest(),
    )

    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-abc123",
        thread_ref="thread-def456",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(accepted_file,),
        runtime_image_digest="5" * 64,
        lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )

    assert request.to_persisted() == {
        "version": 1,
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "tenant": {
            "version": 1,
            "public_ref": "tenant-1111111111111111",
            "digest": "1" * 64,
        },
        "user_ref": "user-abc123",
        "thread_ref": "thread-def456",
        "agent_revision_digest": "2" * 64,
        "skill_snapshot_digest": "3" * 64,
        "skill_scope_digest": "4" * 64,
        "file_manifest": [
            {
                "version": 1,
                "path": "SKILL.md",
                "file_type": "regular",
                "size": 7,
                "mode": 0o444,
                "digest": "bc70e26f40b8816eb177813dda1f5f529a27a4641d45aa19cae2348a8c6a5fe9",
                "link_target": None,
            }
        ],
        "runtime_image_digest": "5" * 64,
        "lease_expires_at": "2026-08-31T12:00:00Z",
        "digest": request.digest,
    }
    assert AcceptedMaterialRequestV1.from_persisted(request.to_persisted()) == request


@pytest.mark.parametrize("mode", [0o644, 0o2444, 0o4444, 0o1444])
def test_accepted_material_file_rejects_write_and_special_mode_bits(mode: int) -> None:
    content = b"content"

    with pytest.raises(ValueError, match="read-only without special bits"):
        AcceptedFileV1(
            version=1,
            path="SKILL.md",
            file_type=AcceptedFileType.REGULAR,
            size=len(content),
            mode=mode,
            digest=hashlib.sha256(content).hexdigest(),
        )


@pytest.mark.parametrize(
    "path",
    [
        "../SKILL.md",
        "/SKILL.md",
        "public//SKILL.md",
        "public\\SKILL.md",
        "public/e\u0301.md",
        "/".join(["deep"] * 33),
    ],
)
def test_accepted_material_file_rejects_ambiguous_or_traversing_paths(
    path: str,
) -> None:
    with pytest.raises(ValueError, match="path"):
        _regular_file(path)


@pytest.mark.parametrize("target", ["../outside", "/outside", "safe//target"])
def test_accepted_material_symlink_rejects_escaping_or_ambiguous_target(
    target: str,
) -> None:
    with pytest.raises(ValueError, match="link_target"):
        AcceptedFileV1(
            version=1,
            path="public/link",
            file_type=AcceptedFileType.SYMLINK,
            size=len(target.encode("utf-8")),
            mode=0,
            digest=hashlib.sha256(target.encode("utf-8")).hexdigest(),
            link_target=target,
        )


@pytest.mark.parametrize(
    "manifest",
    [
        (_regular_file("public/Skill.md"), _regular_file("public/skill.md")),
        (_regular_file("public"), _regular_file("public/SKILL.md")),
    ],
)
def test_accepted_material_request_rejects_manifest_aliases_and_non_directory_parents(
    manifest: tuple[AcceptedFileV1, ...],
) -> None:
    with pytest.raises(ValueError, match="duplicate|non-directory parent"):
        AcceptedMaterialRequestV1.build(
            run_id="run-1",
            attempt_id="attempt-1",
            tenant=_tenant(),
            user_ref="user-ref",
            thread_ref="thread-ref",
            agent_revision_digest="2" * 64,
            skill_snapshot_digest="3" * 64,
            skill_scope_digest="4" * 64,
            file_manifest=manifest,
            runtime_image_digest="5" * 64,
            lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
        )


def test_accepted_material_request_rejects_missing_parents_and_dangling_links() -> None:
    target = "missing.md"
    dangling = AcceptedFileV1(
        version=1,
        path="public/link",
        file_type=AcceptedFileType.SYMLINK,
        size=len(target),
        mode=0,
        digest=hashlib.sha256(target.encode()).hexdigest(),
        link_target=target,
    )
    directory = AcceptedFileV1(
        version=1,
        path="public",
        file_type=AcceptedFileType.DIRECTORY,
        size=0,
        mode=0o555,
        digest=None,
    )

    with pytest.raises(ValueError, match="dangling symlink"):
        AcceptedMaterialRequestV1.build(
            run_id="run-1",
            attempt_id="attempt-1",
            tenant=_tenant(),
            user_ref="user-ref",
            thread_ref="thread-ref",
            agent_revision_digest="2" * 64,
            skill_snapshot_digest="3" * 64,
            skill_scope_digest="4" * 64,
            file_manifest=(directory, dangling),
            runtime_image_digest="5" * 64,
            lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
        )


def test_accepted_material_request_accepts_a_closed_safe_symlink() -> None:
    target = "target.md"
    directory = AcceptedFileV1(
        version=1,
        path="public",
        file_type=AcceptedFileType.DIRECTORY,
        size=0,
        mode=0o555,
        digest=None,
    )
    link = AcceptedFileV1(
        version=1,
        path="public/link",
        file_type=AcceptedFileType.SYMLINK,
        size=len(target),
        mode=0,
        digest=hashlib.sha256(target.encode()).hexdigest(),
        link_target=target,
    )

    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(directory, link, _regular_file("public/target.md")),
        runtime_image_digest="5" * 64,
        lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )

    assert request.file_manifest[1] == link


def test_capture_manifest_binds_directories_modes_and_exact_bytes(tmp_path) -> None:
    nested = tmp_path / "public" / "demo"
    nested.mkdir(parents=True)
    skill = nested / "SKILL.md"
    skill.write_bytes(b"# demo\n")
    skill.chmod(0o400)
    nested.chmod(0o500)
    nested.parent.chmod(0o500)

    try:
        manifest = capture_accepted_file_manifest(tmp_path)

        assert [entry.path for entry in manifest] == [
            "public",
            "public/demo",
            "public/demo/SKILL.md",
        ]
        assert manifest[-1].digest == hashlib.sha256(b"# demo\n").hexdigest()
    finally:
        skill.chmod(0o600)
        nested.chmod(0o700)
        nested.parent.chmod(0o700)


@pytest.mark.parametrize(
    ("factory", "message"),
    [
        (
            lambda: AcceptedFileV1(
                version=True,
                path="SKILL.md",
                file_type=AcceptedFileType.REGULAR,
                size=0,
                mode=0o444,
                digest=hashlib.sha256(b"").hexdigest(),
            ),
            "file version",
        ),
    ],
)
def test_accepted_material_versions_reject_booleans(factory, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        factory()


def test_accepted_material_request_rejects_boolean_nested_tenant_version() -> None:
    tenant = TenantReferenceV1(
        version=True,  # type: ignore[arg-type]
        public_ref="tenant-1111111111111111",
        digest="1" * 64,
    )

    with pytest.raises(ValueError, match="tenant version"):
        AcceptedMaterialRequestV1.build(
            run_id="run-1",
            attempt_id="attempt-1",
            tenant=tenant,
            user_ref="user-ref",
            thread_ref="thread-ref",
            agent_revision_digest="2" * 64,
            skill_snapshot_digest="3" * 64,
            skill_scope_digest="4" * 64,
            file_manifest=(),
            runtime_image_digest="5" * 64,
            lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
        )


def test_execution_evidence_binds_request_and_lease_without_persisting_handle() -> None:
    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-abc123",
        thread_ref="thread-def456",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )
    renewal_handle = object()
    lease = AcceptedMaterialLeaseV1(
        version=1,
        provider_kind="aio",
        provider_instance_ref="sandbox-1",
        ownership_epoch=7,
        lease_expires_at=request.lease_expires_at,
        opaque_renewal_handle=renewal_handle,
    )
    evidence = AcceptedExecutionEvidenceV1.build(
        run_id=request.run_id,
        attempt_id=request.attempt_id,
        tenant=request.tenant,
        provider_kind=lease.provider_kind,
        provider_instance_ref=lease.provider_instance_ref,
        ownership_epoch=lease.ownership_epoch,
        runtime_image_digest=request.runtime_image_digest,
        skill_snapshot_digest=request.skill_snapshot_digest,
        skill_scope_digest=request.skill_scope_digest,
        materialization_digest="6" * 64,
        verifier_image_digest="7" * 64,
        verifier_contract_version="rwx_verified_copy_v2",
        read_only_proof_digest="8" * 64,
        qualification_scope="durable_one_replica_rwx_verified_copy_v2_nonempty_skill",
    )

    assert lease.to_persisted() == {
        "version": 1,
        "provider_kind": "aio",
        "provider_instance_ref": "sandbox-1",
        "ownership_epoch": 7,
        "lease_expires_at": "2026-08-31T12:00:00Z",
    }
    assert renewal_handle not in lease.to_persisted().values()
    assert evidence.binds(request, lease)
    assert AcceptedExecutionEvidenceV1.from_persisted(evidence.to_persisted()) == evidence
    tampered = deepcopy(evidence.to_persisted())
    tampered["ownership_epoch"] = 8
    with pytest.raises(ValueError, match="digest"):
        AcceptedExecutionEvidenceV1.from_persisted(tampered)
    lifecycle = build_lifecycle_payload(
        LifecycleTransition(
            lifecycle_type=LifecycleType.started,
            status="running",
            execution_evidence_json=evidence.to_persisted(),
            execution_evidence_digest=evidence.digest,
        ),
    )
    assert lifecycle["execution_evidence_digest"] == evidence.digest


def test_v2_request_and_evidence_bind_admission_without_portable_provider_handles() -> None:
    profile = _capability_profile()
    qualification = _qualification(profile)
    request = AcceptedMaterialRequestV2.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-abc123",
        thread_ref="thread-def456",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=datetime(2030, 1, 1, tzinfo=UTC),
        accepted_invocation_ref="invocation-abc123",
        accepted_invocation_digest="6" * 64,
        tool_plane_base_revision_digest="7" * 64,
        tool_plane_user_overlay_digest="8" * 64,
        tool_plane_projection_digest="9" * 64,
        tool_plane_effective_digest="a" * 64,
        batch_child_attempt_ref="batch-attempt-abc123",
        capability_profile_digest=profile.digest,
    )
    raw_resource = "secret-pod-name-and-provider-url"
    lease = AcceptedMaterialLeaseV1(
        version=1,
        provider_kind="aio_kubernetes",
        provider_instance_ref=raw_resource,
        ownership_epoch=7,
        lease_expires_at=request.lease_expires_at,
        opaque_renewal_handle=object(),
    )
    isolation = AcceptedSandboxIsolationFactsV1.build(
        restricted_non_root=True,
        read_only_accepted_material=True,
        privilege_escalation_disabled=True,
        runtime_class_digest="b" * 64,
        network_policy_digest="c" * 64,
    )
    evidence = AcceptedExecutionEvidenceV2.build(
        request=request,
        lease=lease,
        materialization_digest="d" * 64,
        verifier_image_digest="e" * 64,
        verifier_contract_version="rwx_verified_copy_v2",
        read_only_proof_digest="f" * 64,
        qualification=qualification,
        isolation=isolation,
    )

    request_wire = request.to_persisted()
    evidence_wire = evidence.to_persisted()
    assert decode_accepted_material_request(request_wire) == request
    assert decode_accepted_execution_evidence(evidence_wire) == evidence
    assert evidence.binds(request, lease)
    assert raw_resource not in json.dumps(evidence_wire)
    assert "provider_instance_ref" not in evidence_wire
    assert evidence.provider_resource_commitment != raw_resource
    assert accepted_execution_evidence_reference(evidence) == (f"accepted-execution-{evidence.digest}")

    first_operation = AcceptedSandboxOperationV1.execute_command(
        "credential-that-must-not-enter-a-reference",
    )
    second_operation = AcceptedSandboxOperationV1.execute_command("same shape")
    assert first_operation.operation_ref.startswith("accepted-operation-")
    assert "credential" not in first_operation.operation_ref
    assert first_operation.operation_ref != second_operation.operation_ref
    with pytest.raises(ValueError, match="operation_ref"):
        AcceptedSandboxOperationV1(
            version=1,
            kind=first_operation.kind,
            args=("echo safe",),
            operation_ref="accepted-operation-command-echo-safe",
        )

    observation = AcceptedSandboxLifecycleObservationV1.build(
        evidence=evidence,
        kind=AcceptedSandboxLifecycleKind.AUTHORITY_LOST,
        observed_at=datetime(2028, 1, 1, 0, 1, tzinfo=UTC),
        reason_code="accepted_sandbox_run_fence_lost",
        tool_receipt_ref="tool-receipt-deadbeef",
    )
    observation_wire = observation.to_persisted()
    assert (
        AcceptedSandboxLifecycleObservationV1.from_persisted(
            observation_wire,
        )
        == observation
    )
    assert observation_wire["batch_child_attempt_ref"] == (request.batch_child_attempt_ref)
    assert raw_resource not in json.dumps(observation_wire, sort_keys=True)

    lifecycle = build_lifecycle_payload(
        LifecycleTransition(
            lifecycle_type=LifecycleType.started,
            status="running",
            execution_evidence_json=evidence_wire,
            execution_evidence_digest=evidence.digest,
        ),
    )
    assert lifecycle["execution_evidence_digest"] == evidence.digest

    adapter = InMemoryAcceptedMaterializer(owner="worker-a")
    selection = AcceptedMaterializerSelection(
        materializer=adapter,
        runtime_image_digest=request.runtime_image_digest,
        lease_duration=timedelta(minutes=5),
        capability_profile=profile,
        qualification=qualification,
    )
    validate_accepted_materialization(
        selection=selection,
        request=request,
        lease=lease,
        evidence=evidence,
    )

    different_profile = AcceptedSandboxCapabilityProfileV1.build(
        material_capability=AcceptedMaterialCapability.IMMUTABLE_READ_ONLY,
        atomic_provider_ownership_fencing=True,
        atomic_provider_operation_fencing=False,
        authoritative_shared_expiry=True,
        resolved_immutable_image=True,
        restricted_non_root_isolation=True,
        recoverable_resource_lookup=False,
        durable_one_replica=True,
        exact_two=False,
    )
    with pytest.raises(
        AcceptedMaterialError,
        match="accepted_material_evidence_mismatch",
    ):
        validate_accepted_materialization(
            selection=AcceptedMaterializerSelection(
                materializer=adapter,
                runtime_image_digest=request.runtime_image_digest,
                lease_duration=timedelta(minutes=5),
                capability_profile=different_profile,
                qualification=_qualification(different_profile),
            ),
            request=request,
            lease=lease,
            evidence=evidence,
        )


def test_version_dispatch_is_strict_and_never_reinterprets_v1() -> None:
    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=datetime(2030, 1, 1, tzinfo=UTC),
    )
    assert decode_accepted_material_request(request.to_persisted()) == request

    for invalid_version in (True, 0, 3, "2", None):
        wire = deepcopy(request.to_persisted())
        wire["version"] = invalid_version
        with pytest.raises(ValueError, match="version"):
            decode_accepted_material_request(wire)


def test_v2_decoders_reject_unknown_or_missing_fields() -> None:
    profile = _capability_profile()
    qualification = _qualification(profile)
    request = AcceptedMaterialRequestV2.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=datetime(2030, 1, 1, tzinfo=UTC),
        accepted_invocation_ref="invocation-ref",
        accepted_invocation_digest="6" * 64,
        tool_plane_base_revision_digest="7" * 64,
        tool_plane_user_overlay_digest="8" * 64,
        tool_plane_projection_digest="9" * 64,
        tool_plane_effective_digest="a" * 64,
        batch_child_attempt_ref=None,
        capability_profile_digest=profile.digest,
    )
    lease = AcceptedMaterialLeaseV1(
        version=1,
        provider_kind="aio_kubernetes",
        provider_instance_ref="private-resource",
        ownership_epoch=1,
        lease_expires_at=request.lease_expires_at,
        opaque_renewal_handle=object(),
    )
    evidence = AcceptedExecutionEvidenceV2.build(
        request=request,
        lease=lease,
        materialization_digest="b" * 64,
        verifier_image_digest="c" * 64,
        verifier_contract_version="rwx_verified_copy_v2",
        read_only_proof_digest="d" * 64,
        qualification=qualification,
        isolation=AcceptedSandboxIsolationFactsV1.build(
            restricted_non_root=True,
            read_only_accepted_material=True,
            privilege_escalation_disabled=True,
            runtime_class_digest=None,
            network_policy_digest="e" * 64,
        ),
    )

    for decoder, persisted in (
        (decode_accepted_material_request, request.to_persisted()),
        (decode_accepted_execution_evidence, evidence.to_persisted()),
    ):
        unknown = deepcopy(persisted)
        unknown["unknown"] = True
        with pytest.raises(ValueError, match="unknown or missing"):
            decoder(unknown)
        missing = deepcopy(persisted)
        missing.pop(next(key for key in missing if key != "version"))
        with pytest.raises(ValueError, match="unknown or missing"):
            decoder(missing)


def test_repository_has_no_second_sandbox_execution_authority() -> None:
    """The accepted-material tuple remains the only execution lease seam."""

    backend_root = Path(__file__).resolve().parents[1]
    production_roots = (
        backend_root / "packages" / "harness" / "deerflow",
        backend_root / "app",
    )
    forbidden_names = (
        "SandboxExecutionLeasePort",
        "SandboxExecutionLeaseRow",
        "sandbox_execution_leases",
    )

    for root in production_roots:
        for source in root.rglob("*.py"):
            text = source.read_text(encoding="utf-8")
            assert all(name not in text for name in forbidden_names), source


def test_legacy_provider_capability_name_is_the_neutral_contract() -> None:
    assert AcceptedSkillMaterialCapability is AcceptedMaterialCapability


@pytest.mark.asyncio
async def test_stale_run_claim_prevents_materializer_and_sandbox_calls() -> None:
    class RecordingSandbox(Sandbox):
        def __init__(self) -> None:
            super().__init__("private-sandbox-handle")
            self.calls: list[str] = []

        def execute_command(self, command, env=None, timeout=None):
            del command, env, timeout
            self.calls.append("execute_command")
            return "should-not-run"

        def read_file(self, path, start_line=None, end_line=None):
            raise AssertionError("unexpected read_file")

        def download_file(self, path):
            raise AssertionError("unexpected download_file")

        def list_dir(self, path, max_depth=2):
            raise AssertionError("unexpected list_dir")

        def write_file(self, path, content, append=False):
            raise AssertionError("unexpected write_file")

        def glob(self, path, pattern, *, include_dirs=False, max_results=200):
            raise AssertionError("unexpected glob")

        def grep(
            self,
            path,
            pattern,
            *,
            glob=None,
            literal=False,
            case_sensitive=False,
            max_results=100,
        ):
            raise AssertionError("unexpected grep")

        def update_file(self, path, content):
            raise AssertionError("unexpected update_file")

    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-ref",
        thread_ref="thread-ref",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )
    lease = AcceptedMaterialLeaseV1(
        version=1,
        provider_kind="test",
        provider_instance_ref="private-sandbox-handle",
        ownership_epoch=1,
        lease_expires_at=request.lease_expires_at,
        opaque_renewal_handle=object(),
    )
    evidence = AcceptedExecutionEvidenceV1.build(
        run_id=request.run_id,
        attempt_id=request.attempt_id,
        tenant=request.tenant,
        provider_kind=lease.provider_kind,
        provider_instance_ref=lease.provider_instance_ref,
        ownership_epoch=lease.ownership_epoch,
        runtime_image_digest=request.runtime_image_digest,
        skill_snapshot_digest=request.skill_snapshot_digest,
        skill_scope_digest=request.skill_scope_digest,
        materialization_digest=request.digest,
        verifier_image_digest="6" * 64,
        verifier_contract_version="test_v1",
        read_only_proof_digest="7" * 64,
        qualification_scope="contract_test_only",
    )

    class RecordingMaterializer:
        def __init__(self) -> None:
            self.validate_calls = 0

        def capability(self):
            return AcceptedMaterialCapability.IMMUTABLE_READ_ONLY

        async def validate(self, checked_lease, checked_evidence):
            del checked_lease, checked_evidence
            self.validate_calls += 1
            return True

        async def renew(self, renewed_lease):
            return renewed_lease

        async def release(self, released_lease):
            del released_lease

    sandbox = RecordingSandbox()
    materializer = RecordingMaterializer()
    claim = AcceptedMaterialExecutionClaimV1(
        version=1,
        tenant_digest=request.tenant.digest,
        run_id=request.run_id,
        owner_worker_id="worker-1",
        state_version=4,
        execution_takeover=False,
    )

    async def stale_run_fence(checked_claim):
        assert checked_claim is claim
        return False

    session = AcceptedSandboxSession(
        sandbox=sandbox,
        materializer=materializer,
        lease=lease,
        evidence=evidence,
        execution_claim=claim,
        run_fence_validator=stale_run_fence,
    )

    with pytest.raises(
        AcceptedSandboxAuthorityLostError,
        match="accepted_sandbox_run_fence_lost",
    ):
        await session.execute(
            AcceptedSandboxOperationV1.execute_command("echo forbidden"),
        )

    assert materializer.validate_calls == 0
    assert sandbox.calls == []


@pytest.mark.asyncio
async def test_materializer_resolution_uses_only_an_opt_in_provider_hook() -> None:
    adapter = InMemoryAcceptedMaterializer(owner="worker-a")
    binding = AcceptedSkillSandboxBindingV1(
        snapshot_id="3" * 64,
        run_id="run-1",
        generation=1,
    )
    observed: dict[str, object] = {}

    class ProviderWithAdapter:
        async def accepted_materializer_selection(self, **kwargs):
            observed.update(kwargs)
            profile = _capability_profile()
            return AcceptedMaterializerSelection(
                materializer=adapter,
                runtime_image_digest="5" * 64,
                lease_duration=timedelta(minutes=5),
                capability_profile=profile,
                qualification=_qualification(profile),
            )

    selection = await resolve_accepted_materializer(
        ProviderWithAdapter(),
        binding=binding,
        thread_id="thread-1",
        user_id="user-1",
    )

    assert selection is not None
    assert selection.materializer is adapter
    assert selection.runtime_image_digest == "5" * 64
    assert observed == {
        "binding": binding,
        "thread_id": "thread-1",
        "user_id": "user-1",
    }
    assert (
        await resolve_accepted_materializer(
            object(),
            binding=binding,
            thread_id="thread-1",
            user_id="user-1",
        )
        is None
    )


@pytest.mark.asyncio
async def test_candidate_qualification_is_not_passing_and_requires_explicit_opt_in() -> None:
    adapter = InMemoryAcceptedMaterializer(owner="worker-a")
    profile = _capability_profile()
    now = datetime.now(UTC)
    candidate = AcceptedSandboxQualificationV1.build(
        capability_profile_digest=profile.digest,
        qualification_scope="contract_test_candidate",
        artifact_digest="a" * 64,
        topology_digest="b" * 64,
        verified_at=now - timedelta(minutes=1),
        expires_at=now + timedelta(minutes=10),
        status="candidate",
    )
    binding = AcceptedSkillSandboxBindingV1(
        snapshot_id="3" * 64,
        run_id="run-1",
        generation=1,
    )

    class CandidateProvider:
        async def accepted_materializer_selection(self, **_kwargs):
            return AcceptedMaterializerSelection(
                materializer=adapter,
                runtime_image_digest="5" * 64,
                lease_duration=timedelta(minutes=5),
                capability_profile=profile,
                qualification=candidate,
            )

    assert candidate.is_current(now) is False
    with pytest.raises(AcceptedMaterialError, match="sandbox_provider_unqualified"):
        await resolve_accepted_materializer(
            CandidateProvider(),
            binding=binding,
            thread_id="thread-1",
            user_id="user-1",
            require_durable_one_replica=True,
        )

    selection = await resolve_accepted_materializer(
        CandidateProvider(),
        binding=binding,
        thread_id="thread-1",
        user_id="user-1",
        require_durable_one_replica=True,
        allow_qualification_candidate=True,
    )
    assert selection is not None
    assert selection.qualification.status == "candidate"


@pytest.mark.asyncio
async def test_materializer_resolution_rejects_unqualified_durable_profile() -> None:
    adapter = InMemoryAcceptedMaterializer(owner="worker-a")
    binding = AcceptedSkillSandboxBindingV1(
        snapshot_id="3" * 64,
        run_id="run-1",
        generation=1,
    )
    base = _capability_profile()
    weak_profile = AcceptedSandboxCapabilityProfileV1.build(
        material_capability=base.material_capability,
        atomic_provider_ownership_fencing=base.atomic_provider_ownership_fencing,
        atomic_provider_operation_fencing=False,
        authoritative_shared_expiry=base.authoritative_shared_expiry,
        resolved_immutable_image=base.resolved_immutable_image,
        restricted_non_root_isolation=base.restricted_non_root_isolation,
        recoverable_resource_lookup=base.recoverable_resource_lookup,
        durable_one_replica=True,
        exact_two=False,
    )

    class OneReplicaOnlyProvider:
        async def accepted_materializer_selection(self, **_kwargs):
            return AcceptedMaterializerSelection(
                materializer=adapter,
                runtime_image_digest="5" * 64,
                lease_duration=timedelta(minutes=5),
                capability_profile=weak_profile,
                qualification=_qualification(weak_profile),
            )

    with pytest.raises(AcceptedMaterialError, match="sandbox_capability_missing"):
        await resolve_accepted_materializer(
            OneReplicaOnlyProvider(),
            binding=binding,
            thread_id="thread-1",
            user_id="user-1",
            require_durable_one_replica=True,
            require_exact_two=True,
        )


@pytest.mark.asyncio
async def test_in_memory_adapter_models_idempotency_and_epoch_fences() -> None:
    now = datetime(2026, 8, 31, 12, tzinfo=UTC)

    def clock() -> datetime:
        return now

    request = AcceptedMaterialRequestV1.build(
        run_id="run-1",
        attempt_id="attempt-1",
        tenant=_tenant(),
        user_ref="user-abc123",
        thread_ref="thread-def456",
        agent_revision_digest="2" * 64,
        skill_snapshot_digest="3" * 64,
        skill_scope_digest="4" * 64,
        file_manifest=(),
        runtime_image_digest="5" * 64,
        lease_expires_at=now + timedelta(seconds=30),
    )
    state = InMemoryAcceptedMaterialState()
    first = InMemoryAcceptedMaterializer(
        owner="worker-a",
        state=state,
        clock=clock,
        lease_duration=timedelta(seconds=30),
    )
    second = InMemoryAcceptedMaterializer(
        owner="worker-b",
        state=state,
        clock=clock,
        lease_duration=timedelta(seconds=30),
    )

    sandbox, lease, evidence = await first.acquire_and_materialize(request)
    replay_sandbox, replay_lease, replay_evidence = await first.acquire_and_materialize(
        request,
    )
    assert replay_sandbox is sandbox
    assert replay_lease is lease
    assert replay_evidence is evidence
    assert await first.validate(lease, evidence)

    conflicting = AcceptedMaterialRequestV1.build(
        run_id=request.run_id,
        attempt_id=request.attempt_id,
        tenant=request.tenant,
        user_ref=request.user_ref,
        thread_ref=request.thread_ref,
        agent_revision_digest=request.agent_revision_digest,
        skill_snapshot_digest=request.skill_snapshot_digest,
        skill_scope_digest="9" * 64,
        file_manifest=request.file_manifest,
        runtime_image_digest=request.runtime_image_digest,
        lease_expires_at=request.lease_expires_at,
    )
    with pytest.raises(
        AcceptedMaterialError,
        match="accepted_material_request_conflict",
    ):
        await first.acquire_and_materialize(conflicting)

    with pytest.raises(AcceptedMaterialError, match="accepted_material_claim_conflict"):
        await second.acquire_and_materialize(request)

    now += timedelta(seconds=31)
    recovered_sandbox, recovered_lease, recovered_evidence = await second.acquire_and_materialize(request)
    assert recovered_sandbox is sandbox
    assert recovered_lease.ownership_epoch == lease.ownership_epoch + 1
    assert recovered_evidence.ownership_epoch == recovered_lease.ownership_epoch
    assert not await first.validate(lease, evidence)
    with pytest.raises(AcceptedMaterialError, match="accepted_material_lease_lost"):
        await first.renew(lease)

    await first.release(lease)
    assert await second.validate(recovered_lease, recovered_evidence)
    await second.release(recovered_lease)
    assert not await second.validate(recovered_lease, recovered_evidence)
