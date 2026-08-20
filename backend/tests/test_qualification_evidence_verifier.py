"""Offline verification for operator-declared deployment qualification evidence."""

from __future__ import annotations

import ast
import hashlib
import json
import sys
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from deerflow.qualification_evidence import (
    ACCEPTED_SKILL_QUALIFICATION_SCENARIOS_V2,
    MAX_QUALIFICATION_EVIDENCE_BYTES,
    AcceptedSkillMaterialEvidenceV2,
    AcceptedSkillQualificationEnvironmentV2,
    AcceptedSkillQualificationExpectationV2,
    AcceptedSkillScenarioEvidenceV2,
    KubernetesAcceptedSkillQualificationEvidenceV2,
    KubernetesQualificationEvidence,
    QualificationEvidenceExpectation,
    QualificationVerificationError,
    ScenarioEvidence,
    StoreContinuityEvidence,
    qualification_evidence_digest,
    verify_qualification_evidence,
)
from scripts.verify_qualification_evidence import main as verify_command


def test_shared_evidence_schema_is_standard_library_only() -> None:
    source = (Path(__file__).resolve().parents[1] / "packages" / "harness" / "deerflow" / "qualification_evidence.py").read_text(encoding="utf-8")
    imported_roots: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.partition(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.partition(".")[0])

    assert imported_roots <= sys.stdlib_module_names | {"__future__"}


def _passing_evidence() -> KubernetesQualificationEvidence:
    scenarios = tuple(
        ScenarioEvidence(
            name=name,
            run_id=f"run-{index}",
            worker_attachments=0 if name == "accepted_before_worker_start" else 1,
            graph_starts=0 if name == "accepted_before_worker_start" else 1,
            model_starts=0 if name == "accepted_before_worker_start" else 1,
            terminal_status=("interrupted" if name == "graceful_rollout_termination" else "error"),
            termination_mode=("graceful" if name == "graceful_rollout_termination" else ("forced_deadline" if name == "forced_kill_after_graceful_deadline" else "abrupt")),
            old_pod_termination_millis=500,
        )
        for index, name in enumerate(
            KubernetesQualificationEvidence.REQUIRED_SCENARIOS,
        )
    )
    return KubernetesQualificationEvidence(
        qualification_id="pod-recovery-20260808",
        image_reference="registry.example/hartmesh/gateway@sha256:" + ("a" * 64),
        image_digest="sha256:" + ("a" * 64),
        chart_version="2.1.0",
        chart_digest="sha256:" + ("b" * 64),
        configuration_digest="sha256:" + ("c" * 64),
        migration_head="0020_merge_mcp_task_results",
        stores=(
            StoreContinuityEvidence(
                component="postgres",
                pod_uid="postgres-pod-uid",
                volume_uid="postgres-volume-uid",
                image_id="docker-pullable://postgres@sha256:" + ("d" * 64),
                version="17.5",
            ),
            StoreContinuityEvidence(
                component="redis",
                pod_uid="redis-pod-uid",
                volume_uid="redis-volume-uid",
                image_id="docker-pullable://redis@sha256:" + ("e" * 64),
                version="7.4.3",
            ),
        ),
        kubernetes_server_version="v1.33.1",
        cluster_context="qualification-context",
        cluster_driver="kind",
        namespace="hartmesh-qualification-a1b2c3",
        completed_at=datetime(2026, 8, 8, 12, 0, tzinfo=UTC),
        scenarios=scenarios,
    )


def _expectation() -> QualificationEvidenceExpectation:
    return QualificationEvidenceExpectation(
        qualification_id="pod-recovery-20260808",
        image_digest="sha256:" + ("a" * 64),
        chart_version="2.1.0",
        chart_digest="sha256:" + ("b" * 64),
        configuration_digest="sha256:" + ("c" * 64),
        migration_head="0020_merge_mcp_task_results",
        scope="durable_one_replica_pod_recovery",
        namespace="hartmesh-qualification-a1b2c3",
        required_scenarios=KubernetesQualificationEvidence.REQUIRED_SCENARIOS,
    )


def _accepted_skill_evidence_v2() -> KubernetesAcceptedSkillQualificationEvidenceV2:
    return KubernetesAcceptedSkillQualificationEvidenceV2(
        qualification_id="skill-projection-20260810",
        gateway_image_reference="registry.example/hartmesh/gateway@sha256:" + ("a" * 64),
        gateway_image_digest="sha256:" + ("a" * 64),
        provisioner_image_reference="registry.example/hartmesh/provisioner@sha256:" + ("b" * 64),
        provisioner_image_digest="sha256:" + ("b" * 64),
        verifier_image_reference="registry.example/hartmesh/provisioner@sha256:" + ("b" * 64),
        verifier_image_digest="sha256:" + ("b" * 64),
        sandbox_image_reference="registry.example/hartmesh/sandbox@sha256:" + ("d" * 64),
        sandbox_image_digest="sha256:" + ("d" * 64),
        chart_version="2.1.0",
        chart_digest="sha256:" + ("e" * 64),
        configuration_digest="sha256:" + ("f" * 64),
        migration_head="0020_merge_mcp_task_results",
        environment=AcceptedSkillQualificationEnvironmentV2(
            kubernetes_server_version="v1.33.1",
            cluster_context="qualification-context",
            cluster_driver="kind",
            namespace="hartmesh-qualification-a1b2c3",
            schedulable_nodes=("worker-a", "worker-b"),
            gateway_node="worker-a",
            sandbox_node="worker-b",
            rwx_storage_class="rwx-storage",
            rwx_volume_uid="pvc-uid",
            token_review_authenticated=True,
            gateway_service_account="hartmesh-gateway",
            lease_uid="lease-uid",
            lease_renewals=2,
        ),
        material=AcceptedSkillMaterialEvidenceV2(
            skill_name="qualification-skill",
            snapshot_digest="sha256:" + ("1" * 64),
            skill_tree_digest="sha256:" + ("2" * 64),
            allowed_tool_policy_digest="sha256:" + ("3" * 64),
            file_count=2,
            total_bytes=192,
            materialization_digest="sha256:" + ("4" * 64),
            receipt_digest="sha256:" + ("5" * 64),
        ),
        scenarios=tuple(
            AcceptedSkillScenarioEvidenceV2(
                name=name,
                run_id=f"skill-run-{index}",
                result_digest="sha256:" + (f"{index + 6:x}" * 64),
                replacement_observed=name
                in {
                    "gateway_replacement_cleanup",
                    "process_loss_cleanup",
                },
                cleanup_outcome="deleted",
            )
            for index, name in enumerate(
                ACCEPTED_SKILL_QUALIFICATION_SCENARIOS_V2,
            )
        ),
        completed_at=datetime(2026, 8, 10, 12, 0, tzinfo=UTC),
    )


def _accepted_skill_expectation_v2() -> AcceptedSkillQualificationExpectationV2:
    evidence = _accepted_skill_evidence_v2()
    return AcceptedSkillQualificationExpectationV2(
        qualification_id=evidence.qualification_id,
        gateway_image_digest=evidence.gateway_image_digest,
        provisioner_image_digest=evidence.provisioner_image_digest,
        verifier_image_digest=evidence.verifier_image_digest,
        sandbox_image_digest=evidence.sandbox_image_digest,
        chart_version=evidence.chart_version,
        chart_digest=evidence.chart_digest,
        configuration_digest=evidence.configuration_digest,
        migration_head=evidence.migration_head,
        scope=evidence.SCOPE,
        namespace=evidence.environment.namespace,
        required_scenarios=evidence.REQUIRED_SCENARIOS,
    )


def test_v2_nonempty_cross_node_skill_evidence_verifies_exact_subjects() -> None:
    evidence = _accepted_skill_evidence_v2()
    artifact = evidence.canonical_bytes()

    parsed = KubernetesAcceptedSkillQualificationEvidenceV2.from_bytes(artifact)
    result = verify_qualification_evidence(
        artifact,
        declared_digest=qualification_evidence_digest(artifact),
        expected=_accepted_skill_expectation_v2(),
    )

    assert parsed == evidence
    assert parsed.environment.gateway_node != parsed.environment.sandbox_node
    assert len(parsed.environment.schedulable_nodes) >= 2
    assert parsed.material.file_count > 0
    assert result.scope == "durable_one_replica_rwx_verified_copy_v2_nonempty_skill"


def test_offline_command_verifies_v2_nonempty_skill_exact_artifacts(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    evidence = _accepted_skill_evidence_v2()
    artifact = evidence.canonical_bytes()
    artifact_path = tmp_path / "accepted-skill-v2.json"
    artifact_path.write_bytes(artifact)
    arguments = [
        str(artifact_path),
        "--declared-digest",
        qualification_evidence_digest(artifact),
        "--qualification-id",
        evidence.qualification_id,
        "--image-digest",
        evidence.gateway_image_digest,
        "--provisioner-image-digest",
        evidence.provisioner_image_digest,
        "--verifier-image-digest",
        evidence.verifier_image_digest,
        "--sandbox-image-digest",
        evidence.sandbox_image_digest,
        "--chart-version",
        evidence.chart_version,
        "--chart-digest",
        evidence.chart_digest,
        "--configuration-digest",
        evidence.configuration_digest,
        "--migration-head",
        evidence.migration_head,
        "--scope",
        evidence.SCOPE,
        "--namespace",
        evidence.environment.namespace,
    ]
    for scenario in evidence.REQUIRED_SCENARIOS:
        arguments.extend(("--required-scenario", scenario))

    assert verify_command(arguments) == 0
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "api_version": "deerflow.qualification-verification/v1",
        "artifact_digest": qualification_evidence_digest(artifact),
        "kind": "qualification.verification",
        "qualification_id": evidence.qualification_id,
        "scope": evidence.SCOPE,
        "status": "verified",
        "trust": "external_evidence_verified",
    }


@pytest.mark.parametrize(
    ("field_name", "different_value"),
    [
        ("gateway_image_digest", "sha256:" + ("6" * 64)),
        ("sandbox_image_digest", "sha256:" + ("9" * 64)),
        ("chart_digest", "sha256:" + ("a" * 64)),
        ("configuration_digest", "sha256:" + ("b" * 64)),
        ("migration_head", "0020_other"),
        ("namespace", "hartmesh-qualification-other"),
    ],
)
def test_v2_skill_evidence_rejects_subject_mutation(
    field_name: str,
    different_value: str,
) -> None:
    artifact = _accepted_skill_evidence_v2().canonical_bytes()
    with pytest.raises(QualificationVerificationError) as captured:
        verify_qualification_evidence(
            artifact,
            declared_digest=qualification_evidence_digest(artifact),
            expected=replace(
                _accepted_skill_expectation_v2(),
                **{field_name: different_value},
            ),
        )
    assert captured.value.code == "subject_mismatch"


def test_v2_skill_expectation_rejects_another_scope() -> None:
    with pytest.raises(ValueError, match="scope is invalid"):
        replace(
            _accepted_skill_expectation_v2(),
            scope="another_scope",
        )


@pytest.mark.parametrize("mutation", ["reference", "digest"])
def test_v2_skill_evidence_rejects_a_different_verifier_artifact(
    mutation: str,
) -> None:
    evidence = _accepted_skill_evidence_v2()
    reference = evidence.verifier_image_reference
    digest = evidence.verifier_image_digest
    if mutation == "reference":
        reference = "registry.example/hartmesh/verifier@" + digest
    else:
        digest = "sha256:" + ("c" * 64)
        reference = "registry.example/hartmesh/provisioner@" + digest

    with pytest.raises(ValueError, match="verifier image must exactly match"):
        replace(
            evidence,
            verifier_image_reference=reference,
            verifier_image_digest=digest,
        )


def test_v2_skill_evidence_parser_rejects_a_different_verifier_artifact() -> None:
    value = _accepted_skill_evidence_v2().to_dict()
    artifacts = value["artifacts"]
    assert isinstance(artifacts, dict)
    artifacts["verifier_image_reference"] = "registry.example/hartmesh/verifier@sha256:" + ("c" * 64)
    artifacts["verifier_image_digest"] = "sha256:" + ("c" * 64)
    artifact = _canonical(value)

    with pytest.raises(QualificationVerificationError) as captured:
        verify_qualification_evidence(
            artifact,
            declared_digest=qualification_evidence_digest(artifact),
            expected=_accepted_skill_expectation_v2(),
        )

    assert captured.value.code == "artifact_invalid"


def test_v2_skill_expectation_rejects_a_different_verifier_digest() -> None:
    with pytest.raises(ValueError, match="verifier image must exactly match"):
        replace(
            _accepted_skill_expectation_v2(),
            verifier_image_digest="sha256:" + ("c" * 64),
        )


def test_v2_skill_verifier_rejects_a_jointly_different_runtime_artifact() -> None:
    artifact = _accepted_skill_evidence_v2().canonical_bytes()
    different = "sha256:" + ("c" * 64)

    with pytest.raises(QualificationVerificationError) as captured:
        verify_qualification_evidence(
            artifact,
            declared_digest=qualification_evidence_digest(artifact),
            expected=replace(
                _accepted_skill_expectation_v2(),
                provisioner_image_digest=different,
                verifier_image_digest=different,
            ),
        )

    assert captured.value.code == "subject_mismatch"


@pytest.mark.parametrize(
    "mutation",
    [
        "same_node",
        "one_node",
        "token_review_missing",
        "lease_missing",
        "empty_skill",
        "scenario_missing",
        "unknown_field",
    ],
)
def test_v2_skill_evidence_rejects_incomplete_security_proof(mutation: str) -> None:
    value = _accepted_skill_evidence_v2().to_dict()
    environment = value["environment"]
    material = value["material"]
    scenarios = value["scenarios"]
    assert isinstance(environment, dict)
    assert isinstance(material, dict)
    assert isinstance(scenarios, list)
    if mutation == "same_node":
        environment["sandbox_node"] = environment["gateway_node"]
    elif mutation == "one_node":
        environment["schedulable_nodes"] = [environment["gateway_node"]]
    elif mutation == "token_review_missing":
        environment["token_review_authenticated"] = False
    elif mutation == "lease_missing":
        environment["lease_renewals"] = 0
    elif mutation == "empty_skill":
        material["file_count"] = 0
    elif mutation == "scenario_missing":
        scenarios.pop()
    else:
        value["raw_pod_logs"] = "not allowed"

    artifact = _canonical(value)
    with pytest.raises(QualificationVerificationError) as captured:
        verify_qualification_evidence(
            artifact,
            declared_digest=qualification_evidence_digest(artifact),
            expected=_accepted_skill_expectation_v2(),
        )
    assert captured.value.code == "artifact_invalid"


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        + b"\n"
    )


def _assert_failure(
    artifact: bytes,
    *,
    expected: QualificationEvidenceExpectation | None = None,
    code: str,
) -> None:
    with pytest.raises(QualificationVerificationError) as captured:
        verify_qualification_evidence(
            artifact,
            declared_digest=qualification_evidence_digest(artifact),
            expected=expected or _expectation(),
        )
    assert captured.value.code == code


def test_exact_external_evidence_verifies_against_declared_reference_and_subjects() -> None:
    artifact = _passing_evidence().canonical_bytes()

    result = verify_qualification_evidence(
        artifact,
        declared_digest=qualification_evidence_digest(artifact),
        expected=_expectation(),
    )

    assert result.to_dict() == {
        "api_version": "deerflow.qualification-verification/v1",
        "kind": "qualification.verification",
        "status": "verified",
        "trust": "external_evidence_verified",
        "qualification_id": "pod-recovery-20260808",
        "scope": "durable_one_replica_pod_recovery",
        "artifact_digest": qualification_evidence_digest(artifact),
    }


@pytest.mark.parametrize(
    ("field_name", "different_value"),
    [
        ("qualification_id", "another-run"),
        ("image_digest", "sha256:" + ("1" * 64)),
        ("chart_version", "2.1.1"),
        ("chart_digest", "sha256:" + ("2" * 64)),
        ("configuration_digest", "sha256:" + ("3" * 64)),
        ("migration_head", "0014_previous_head"),
        ("scope", "another_qualification_scope"),
        ("namespace", "hartmesh-qualification-different"),
    ],
)
def test_exact_subject_mismatch_fails_closed(
    field_name: str,
    different_value: str,
) -> None:
    artifact = _passing_evidence().canonical_bytes()

    _assert_failure(
        artifact,
        expected=replace(_expectation(), **{field_name: different_value}),
        code="subject_mismatch",
    )


def test_recomputed_digest_does_not_make_another_artifact_subject_valid() -> None:
    changed = replace(_passing_evidence(), chart_version="9.9.9").canonical_bytes()

    _assert_failure(changed, code="subject_mismatch")


def test_digest_mismatch_fails_before_malformed_inner_claims() -> None:
    with pytest.raises(QualificationVerificationError) as captured:
        verify_qualification_evidence(
            b"not-json\n",
            declared_digest="sha256:" + ("0" * 64),
            expected=_expectation(),
        )

    assert captured.value.code == "artifact_digest_mismatch"


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "failed", "skipped", "unknown"])
def test_incomplete_or_ambiguous_scenario_evidence_fails_closed(
    mutation: str,
) -> None:
    value = _passing_evidence().to_dict()
    scenarios = value["scenarios"]
    assert isinstance(scenarios, list)
    if mutation == "missing":
        scenarios.pop()
    elif mutation == "duplicate":
        scenarios[-1] = dict(scenarios[0])
    elif mutation in {"failed", "skipped"}:
        scenarios[0]["status"] = mutation
    else:
        scenarios[-1]["name"] = "unknown_scenario"

    _assert_failure(_canonical(value), code="artifact_invalid")


def test_expected_unknown_or_missing_scenario_fails_closed() -> None:
    artifact = _passing_evidence().canonical_bytes()

    _assert_failure(
        artifact,
        expected=replace(
            _expectation(),
            required_scenarios=(*_expectation().required_scenarios, "future_scenario"),
        ),
        code="scenario_mismatch",
    )


@pytest.mark.parametrize(
    "mutation",
    ["unknown_version", "unknown_field", "control", "duplicate_field"],
)
def test_strict_artifact_schema_rejects_unsafe_or_ambiguous_input(
    mutation: str,
) -> None:
    value = _passing_evidence().to_dict()
    if mutation == "unknown_version":
        value["api_version"] = "deerflow.kubernetes-qualification/v99"
        artifact = _canonical(value)
    elif mutation == "unknown_field":
        value["claims"] = {"trusted": True}
        artifact = _canonical(value)
    elif mutation == "control":
        value["qualification_id"] = "run\nother"
        artifact = _canonical(value)
    else:
        artifact = b'{"api_version":"duplicate",' + _passing_evidence().canonical_bytes()[1:]

    _assert_failure(artifact, code="artifact_invalid")


def test_oversized_artifact_is_bounded_before_parsing() -> None:
    artifact = b"{" + (b" " * MAX_QUALIFICATION_EVIDENCE_BYTES) + b"}"

    with pytest.raises(QualificationVerificationError) as captured:
        verify_qualification_evidence(
            artifact,
            declared_digest="sha256:" + hashlib.sha256(artifact).hexdigest(),
            expected=_expectation(),
        )
    assert captured.value.code == "artifact_unreadable"


def _command_arguments(path: Path, digest: str) -> list[str]:
    arguments = [
        str(path),
        "--declared-digest",
        digest,
        "--qualification-id",
        "pod-recovery-20260808",
        "--image-digest",
        "sha256:" + ("a" * 64),
        "--chart-version",
        "2.1.0",
        "--chart-digest",
        "sha256:" + ("b" * 64),
        "--configuration-digest",
        "sha256:" + ("c" * 64),
        "--migration-head",
        "0020_merge_mcp_task_results",
        "--scope",
        "durable_one_replica_pod_recovery",
        "--namespace",
        "hartmesh-qualification-a1b2c3",
    ]
    for scenario in KubernetesQualificationEvidence.REQUIRED_SCENARIOS:
        arguments.extend(("--required-scenario", scenario))
    return arguments


def test_offline_command_emits_safe_machine_readable_result(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    artifact = _passing_evidence().canonical_bytes()
    artifact_path = tmp_path / "qualification.json"
    artifact_path.write_bytes(artifact)

    assert verify_command(_command_arguments(artifact_path, qualification_evidence_digest(artifact))) == 0
    success = json.loads(capsys.readouterr().out)
    assert success["status"] == "verified"
    assert success["trust"] == "external_evidence_verified"

    assert verify_command(_command_arguments(artifact_path, "sha256:" + ("0" * 64))) == 1
    failure = json.loads(capsys.readouterr().out)
    assert failure == {
        "api_version": "deerflow.qualification-verification/v1",
        "kind": "qualification.verification",
        "status": "failed",
        "code": "artifact_digest_mismatch",
    }
