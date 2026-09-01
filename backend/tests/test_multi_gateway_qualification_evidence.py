from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from deerflow.deployment.topology import (
    MULTI_GATEWAY_PROFILE,
    MULTI_GATEWAY_QUALIFICATION_SCOPE,
    ReplicaRegistrationV1,
    TopologyFingerprintV1,
)
from deerflow.multi_gateway_qualification import (
    MULTI_GATEWAY_QUALIFICATION_SCENARIOS,
    MULTI_GATEWAY_SCENARIO_RESULTS,
    KubernetesMultiGatewayQualificationEvidenceV1,
    MultiGatewayQualificationExpectationV1,
    MultiGatewayScenarioEvidenceV1,
    verify_multi_gateway_qualification_evidence,
)
from deerflow.qualification_evidence import (
    QualificationVerificationError,
    qualification_evidence_digest,
)
from scripts.verify_qualification_evidence import main as verify_command


def _fingerprint() -> TopologyFingerprintV1:
    return TopologyFingerprintV1.create(
        profile=MULTI_GATEWAY_PROFILE,
        tenant_digest="1" * 64,
        image_digests={
            "gateway": f"sha256:{'2' * 64}",
            "frontend": f"sha256:{'b' * 64}",
            "nginx": f"sha256:{'c' * 64}",
            "provisioner": f"sha256:{'3' * 64}",
            "sandbox": f"sha256:{'4' * 64}",
            "postgres": f"sha256:{'d' * 64}",
            "redis": f"sha256:{'e' * 64}",
        },
        config_digest=f"sha256:{'5' * 64}",
        database_schema_ref=f"schema:sha256:{'6' * 64}",
        redis_namespace_digest=f"sha256:{'7' * 64}",
        extension_artifact_digest=f"sha256:{'8' * 64}",
        extension_configuration_digest=f"sha256:{'9' * 64}",
        capability_manifest_digest="a" * 64,
        migration_head="0027_multi_gateway_topology",
        accepted_materialization_profile="rwx_verified_copy_v2",
    )


def _artifact(*, completed_at: datetime | None = None):
    completed = completed_at or datetime(2026, 9, 1, 12, 20, tzinfo=UTC)
    started = completed - timedelta(minutes=20)
    fingerprint = _fingerprint()
    registrations = tuple(
        ReplicaRegistrationV1(
            replica_id=f"gateway-{index}",
            topology_fingerprint=fingerprint,
            started_at=started,
            heartbeat_at=completed,
        )
        for index in range(2)
    )
    scenarios = tuple(
        MultiGatewayScenarioEvidenceV1(
            scenario_id=scenario_id,
            result_code=MULTI_GATEWAY_SCENARIO_RESULTS[scenario_id],
            input_digest=f"sha256:{index + 11:064x}",
            evidence_digest=f"sha256:{index + 101:064x}",
            authoritative_count=(2 if scenario_id in {"topology_identity", "scheduler_owner_loss"} else 6 if scenario_id == "owner_sigkill" else 3 if scenario_id in {"cancellation_finalization", "postgresql_interruption"} else 1),
            duplicate_count=0,
            stale_write_rejections=(6 if scenario_id == "owner_sigkill" else 7 if scenario_id == "postgresql_interruption" else 0),
            takeover_count=(
                6 if scenario_id == "owner_sigkill" else 2 if scenario_id == "scheduler_owner_loss" else 1 if scenario_id in {"sandbox_recovery", "mcp_task_notification"} else 3 if scenario_id == "postgresql_interruption" else 0
            ),
            pod_deletion_count=(6 if scenario_id == "owner_sigkill" else 2 if scenario_id in {"scheduler_owner_loss", "upgrade_truthfulness"} else 1 if scenario_id in {"sandbox_recovery", "mcp_task_notification"} else 0),
            pod_restart_count=(6 if scenario_id == "owner_sigkill" else 2 if scenario_id in {"scheduler_owner_loss", "upgrade_truthfulness"} else 1 if scenario_id in {"sandbox_recovery", "mcp_task_notification"} else 0),
            lease_epoch_before=1,
            lease_epoch_after=(
                2
                if scenario_id
                in {
                    "owner_sigkill",
                    "scheduler_owner_loss",
                    "sandbox_recovery",
                    "mcp_task_notification",
                    "postgresql_interruption",
                }
                else 1
            ),
            dependency_interruption_count=(1 if scenario_id in {"redis_outage_recovery", "postgresql_interruption"} else 0),
            duration_millis=1_000 + index,
            verified_case_count=(
                6
                if scenario_id == "owner_sigkill"
                else 3
                if scenario_id in {"cancellation_finalization", "postgresql_interruption"}
                else 2
                if scenario_id
                in {
                    "topology_identity",
                    "scheduler_owner_loss",
                    "upgrade_truthfulness",
                }
                else 8
                if scenario_id == "unsupported_surfaces"
                else 1
            ),
            cleanup_count=(3 if scenario_id == "cancellation_finalization" else 0),
            retryable_failure_count=(1 if scenario_id == "redis_outage_recovery" else 0),
        )
        for index, scenario_id in enumerate(MULTI_GATEWAY_QUALIFICATION_SCENARIOS)
    )
    return KubernetesMultiGatewayQualificationEvidenceV1(
        qualification_id="qualification-09",
        git_revision="b" * 40,
        chart_version="0.3.0",
        chart_digest=f"sha256:{'c' * 64}",
        image_digests=fingerprint.image_digests,
        configuration_digest=fingerprint.config_digest,
        migration_head=fingerprint.migration_head,
        tenant_public_ref="tenant-1111111111111111",
        tenant_digest=fingerprint.tenant_digest,
        namespace="hartmesh-qualification-two-gateway",
        kubernetes_refs={
            "gateway_service_uid": "service-uid",
            "gateway_pod_0_uid": "pod-0-uid",
            "gateway_pod_1_uid": "pod-1-uid",
            "provisioner_pod_uid": "provisioner-uid",
            "sandbox_pvc_uid": "pvc-uid",
        },
        database_schema_ref=fingerprint.database_schema_ref,
        redis_namespace_digest=fingerprint.redis_namespace_digest,
        redis_acl_proof_digest=f"sha256:{'d' * 64}",
        extension_artifact_digest=fingerprint.extension_artifact_digest,
        extension_configuration_digest=(fingerprint.extension_configuration_digest),
        capability_manifest_digest=f"sha256:{fingerprint.capability_manifest_digest}",
        topology_registrations=registrations,
        scenarios=scenarios,
        started_at=started,
        completed_at=completed,
    )


def _expectation(artifact):
    return MultiGatewayQualificationExpectationV1(
        qualification_id=artifact.qualification_id,
        git_revision=artifact.git_revision,
        chart_version=artifact.chart_version,
        chart_digest=artifact.chart_digest,
        image_digests=artifact.image_digests,
        configuration_digest=artifact.configuration_digest,
        migration_head=artifact.migration_head,
        tenant_public_ref=artifact.tenant_public_ref,
        tenant_digest=artifact.tenant_digest,
        namespace=artifact.namespace,
        kubernetes_refs=artifact.kubernetes_refs,
        database_schema_ref=artifact.database_schema_ref,
        redis_namespace_digest=artifact.redis_namespace_digest,
        redis_acl_proof_digest=artifact.redis_acl_proof_digest,
        extension_artifact_digest=artifact.extension_artifact_digest,
        extension_configuration_digest=artifact.extension_configuration_digest,
        capability_manifest_digest=artifact.capability_manifest_digest,
        topology_digest=artifact.topology_registrations[0].topology_fingerprint.digest,
        scope=MULTI_GATEWAY_QUALIFICATION_SCOPE,
        required_scenarios=MULTI_GATEWAY_QUALIFICATION_SCENARIOS,
        max_age_seconds=86_400,
    )


def test_exact_artifact_round_trips_and_verifies_freshness() -> None:
    artifact = _artifact()
    payload = artifact.canonical_bytes()
    parsed = KubernetesMultiGatewayQualificationEvidenceV1.from_bytes(payload)
    assert parsed.artifact_digest == artifact.artifact_digest
    assert len(parsed.topology_registrations) == 2

    result = verify_multi_gateway_qualification_evidence(
        payload,
        declared_digest=qualification_evidence_digest(payload),
        expected=_expectation(artifact),
        now=artifact.completed_at + timedelta(hours=1),
    )
    assert result.scope == MULTI_GATEWAY_QUALIFICATION_SCOPE


@pytest.mark.parametrize("subject", ["kubernetes_refs", "redis_acl_proof_digest"])
def test_verifier_binds_live_kubernetes_and_redis_acl_subjects(subject: str) -> None:
    artifact = _artifact()
    if subject == "kubernetes_refs":
        refs = dict(artifact.kubernetes_refs)
        refs["gateway_service_uid"] = "different-service-uid"
        mutated = replace(artifact, kubernetes_refs=refs, artifact_digest="")
    else:
        mutated = replace(
            artifact,
            redis_acl_proof_digest=f"sha256:{'e' * 64}",
            artifact_digest="",
        )
    payload = mutated.canonical_bytes()

    with pytest.raises(QualificationVerificationError) as exc_info:
        verify_multi_gateway_qualification_evidence(
            payload,
            declared_digest=qualification_evidence_digest(payload),
            expected=_expectation(artifact),
            now=artifact.completed_at + timedelta(hours=1),
        )

    assert exc_info.value.code == "subject_mismatch"


@pytest.mark.parametrize(
    ("scenario_id", "changes"),
    [
        ("owner_sigkill", {"takeover_count": 1}),
        ("scheduler_owner_loss", {"pod_deletion_count": 1}),
        ("cancellation_finalization", {"cleanup_count": 1}),
        ("unsupported_surfaces", {"verified_case_count": 1}),
        ("redis_outage_recovery", {"retryable_failure_count": 0}),
    ],
)
def test_scenario_evidence_rejects_weakened_compound_proofs(
    scenario_id: str,
    changes: dict[str, int],
) -> None:
    scenario = next(item for item in _artifact().scenarios if item.scenario_id == scenario_id)

    with pytest.raises(ValueError, match="scenario invariant counters"):
        replace(scenario, **changes)


@pytest.mark.parametrize("mutation", ["digest", "field", "scenario"])
def test_verifier_rejects_tamper_missing_and_scenario_mismatch(mutation: str) -> None:
    artifact = _artifact()
    payload = artifact.canonical_bytes()
    expected = _expectation(artifact)
    if mutation == "digest":
        payload = payload.replace(b'"chart_version":"0.3.0"', b'"chart_version":"0.3.1"')
    elif mutation == "field":
        value = json.loads(payload)
        del value["environment"]["database_schema_ref"]
        payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    else:
        value = json.loads(payload)
        value["scenarios"] = list(reversed(value["scenarios"]))
        payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    with pytest.raises(QualificationVerificationError):
        verify_multi_gateway_qualification_evidence(
            payload,
            declared_digest=qualification_evidence_digest(payload),
            expected=expected,
            now=artifact.completed_at + timedelta(hours=1),
        )


def test_verifier_rejects_stale_or_future_artifact() -> None:
    artifact = _artifact()
    payload = artifact.canonical_bytes()
    expected = _expectation(artifact)
    for now in (
        artifact.completed_at + timedelta(days=2),
        artifact.completed_at - timedelta(minutes=6),
    ):
        with pytest.raises(QualificationVerificationError) as exc_info:
            verify_multi_gateway_qualification_evidence(
                payload,
                declared_digest=qualification_evidence_digest(payload),
                expected=expected,
                now=now,
            )
        assert exc_info.value.code == "artifact_stale"


def test_offline_command_verifies_every_multi_gateway_subject(
    tmp_path,
    capsys,
) -> None:
    artifact = _artifact(completed_at=datetime.now(UTC) - timedelta(minutes=1))
    payload = artifact.canonical_bytes()
    path = tmp_path / "multi-gateway.json"
    path.write_bytes(payload)
    arguments = [
        str(path),
        "--declared-digest",
        qualification_evidence_digest(payload),
        "--qualification-id",
        artifact.qualification_id,
        "--image-digest",
        artifact.image_digests["gateway"],
        "--frontend-image-digest",
        artifact.image_digests["frontend"],
        "--nginx-image-digest",
        artifact.image_digests["nginx"],
        "--provisioner-image-digest",
        artifact.image_digests["provisioner"],
        "--sandbox-image-digest",
        artifact.image_digests["sandbox"],
        "--postgres-image-digest",
        artifact.image_digests["postgres"],
        "--redis-image-digest",
        artifact.image_digests["redis"],
        "--git-revision",
        artifact.git_revision,
        "--chart-version",
        artifact.chart_version,
        "--chart-digest",
        artifact.chart_digest,
        "--configuration-digest",
        artifact.configuration_digest,
        "--migration-head",
        artifact.migration_head,
        "--scope",
        artifact.SCOPE,
        "--namespace",
        artifact.namespace,
        "--gateway-service-uid",
        artifact.kubernetes_refs["gateway_service_uid"],
        "--gateway-pod-0-uid",
        artifact.kubernetes_refs["gateway_pod_0_uid"],
        "--gateway-pod-1-uid",
        artifact.kubernetes_refs["gateway_pod_1_uid"],
        "--provisioner-pod-uid",
        artifact.kubernetes_refs["provisioner_pod_uid"],
        "--sandbox-pvc-uid",
        artifact.kubernetes_refs["sandbox_pvc_uid"],
        "--tenant-public-ref",
        artifact.tenant_public_ref,
        "--tenant-digest",
        artifact.tenant_digest,
        "--database-schema-ref",
        artifact.database_schema_ref,
        "--redis-namespace-digest",
        artifact.redis_namespace_digest,
        "--redis-acl-proof-digest",
        artifact.redis_acl_proof_digest,
        "--extension-artifact-digest",
        artifact.extension_artifact_digest,
        "--extension-configuration-digest",
        artifact.extension_configuration_digest,
        "--capability-manifest-digest",
        artifact.capability_manifest_digest,
        "--topology-digest",
        artifact.topology_registrations[0].topology_fingerprint.digest,
    ]
    for scenario in artifact.REQUIRED_SCENARIOS:
        arguments.extend(("--required-scenario", scenario))

    assert verify_command(arguments) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["scope"] == artifact.SCOPE
    assert result["status"] == "verified"
