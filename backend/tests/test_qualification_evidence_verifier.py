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
    MAX_QUALIFICATION_EVIDENCE_BYTES,
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
        migration_head="0019_inbound_event_identity",
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
        migration_head="0019_inbound_event_identity",
        scope="durable_one_replica_pod_recovery",
        namespace="hartmesh-qualification-a1b2c3",
        required_scenarios=KubernetesQualificationEvidence.REQUIRED_SCENARIOS,
    )


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
        "0019_inbound_event_identity",
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
