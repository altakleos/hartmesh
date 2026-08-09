"""Offline contract tests for the opt-in Kubernetes qualification harness."""

from __future__ import annotations

import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from support.kubernetes_qualification import (
    KUBERNETES_OPT_IN_MESSAGE,
    KubernetesQualificationConfig,
    KubernetesQualificationEvidence,
    KubernetesQualificationFailureEvidence,
    KubernetesQualificationRunner,
    QualificationCommandError,
    QualificationPrerequisiteError,
    QualificationTimeout,
    ScenarioEvidence,
    StoreContinuityEvidence,
    evidence_sha256,
    kubernetes_qualification_enabled,
    optional_cluster_driver,
    run_bounded,
    validate_kubernetes_prerequisites,
    wait_until,
)

from deerflow.config.app_config import AppConfig
from deerflow.qualification_evidence import (
    QualificationEvidenceExpectation,
    verify_qualification_evidence,
)


def test_kubernetes_commands_pin_kubeconfig_context_and_namespace(tmp_path: Path) -> None:
    config = KubernetesQualificationConfig(
        kubeconfig=tmp_path / "kubeconfig",
        context="qualification-context",
        namespace="hartmesh-qualification-a1b2c3",
        image_repository="registry.example/hartmesh/gateway",
        image_digest="sha256:" + ("a" * 64),
        evidence_path=tmp_path / "evidence.json",
    )

    command = config.kubectl("get", "pods", "-o", "json")

    assert command == (
        "kubectl",
        "--kubeconfig",
        str(tmp_path / "kubeconfig"),
        "--context",
        "qualification-context",
        "--namespace",
        "hartmesh-qualification-a1b2c3",
        "get",
        "pods",
        "-o",
        "json",
    )


def test_kubernetes_command_builder_rejects_namespace_escape(tmp_path: Path) -> None:
    config = KubernetesQualificationConfig(
        kubeconfig=(tmp_path / "kubeconfig").resolve(),
        context="qualification-context",
        namespace="hartmesh-qualification-a1b2c3",
        image_repository="registry.example/hartmesh/gateway",
        image_digest="sha256:" + ("a" * 64),
        evidence_path=(tmp_path / "evidence.json").resolve(),
    )

    with pytest.raises(ValueError, match="namespace override"):
        config.kubectl("get", "pods", "--namespace", "production")
    with pytest.raises(ValueError, match="cluster-scoped mutation"):
        config.kubectl("delete", "namespace", "production", namespaced=False)


def test_kubernetes_qualification_is_opt_in_and_enabled_mode_fails_missing_prerequisites() -> None:
    assert not kubernetes_qualification_enabled({})
    assert "DEERFLOW_TEST_KUBERNETES=1" in KUBERNETES_OPT_IN_MESSAGE

    with pytest.raises(QualificationPrerequisiteError) as raised:
        validate_kubernetes_prerequisites(
            {"DEERFLOW_TEST_KUBERNETES": "1"},
            executable_lookup=lambda _name: None,
        )

    message = str(raised.value)
    assert "KUBECONFIG" in message
    assert "DEERFLOW_TEST_KUBERNETES_CONTEXT" in message
    assert "kubectl" in message
    assert "helm" in message
    assert "DEERFLOW_TEST_GATEWAY_IMAGE_REPOSITORY" in message
    assert "DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST" in message
    assert "DEERFLOW_TEST_KUBERNETES_EVIDENCE" in message


def test_qualification_evidence_is_strict_complete_and_digestible(tmp_path: Path) -> None:
    scenarios = tuple(
        ScenarioEvidence(
            name=name,
            run_id=f"run-{index}",
            worker_attachments=0 if name == "accepted_before_worker_start" else 1,
            graph_starts=1 if "accepted_before" not in name else 0,
            model_starts=1 if "accepted_before" not in name else 0,
            terminal_status="error" if name != "graceful_rollout_termination" else "interrupted",
            termination_mode=("graceful" if name == "graceful_rollout_termination" else "forced_deadline" if name == "forced_kill_after_graceful_deadline" else "abrupt"),
            old_pod_termination_millis=500,
        )
        for index, name in enumerate(KubernetesQualificationEvidence.REQUIRED_SCENARIOS)
    )
    evidence = KubernetesQualificationEvidence(
        qualification_id="pod-recovery-20260808",
        image_reference="registry.example/hartmesh/gateway@sha256:" + ("a" * 64),
        image_digest="sha256:" + ("a" * 64),
        chart_version="2.1.0",
        chart_digest="sha256:" + ("b" * 64),
        configuration_digest="sha256:" + ("c" * 64),
        migration_head="0015_inbound_receipts",
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

    wire = evidence.to_dict()
    parsed = KubernetesQualificationEvidence.from_dict(wire)
    evidence_path = tmp_path / "evidence.json"
    parsed.write(evidence_path)

    assert parsed == evidence
    assert wire["status"] == "passed"
    assert wire["scope"] == "durable_one_replica_pod_recovery"
    assert evidence_sha256(evidence_path) == "sha256:" + __import__("hashlib").sha256(evidence_path.read_bytes()).hexdigest()
    verified = verify_qualification_evidence(
        evidence_path.read_bytes(),
        declared_digest=evidence_sha256(evidence_path),
        expected=QualificationEvidenceExpectation(
            qualification_id=evidence.qualification_id,
            image_digest=evidence.image_digest,
            chart_version=evidence.chart_version,
            chart_digest=evidence.chart_digest,
            configuration_digest=evidence.configuration_digest,
            migration_head=evidence.migration_head,
            scope=evidence.SCOPE,
            namespace=evidence.namespace,
            required_scenarios=evidence.REQUIRED_SCENARIOS,
        ),
    )
    assert verified.qualification_id == evidence.qualification_id
    with pytest.raises(ValueError, match="scenario coverage"):
        KubernetesQualificationEvidence.from_dict({**wire, "scenarios": wire["scenarios"][:-1]})
    with pytest.raises(ValueError, match="unknown evidence fields"):
        KubernetesQualificationEvidence.from_dict({**wire, "raw_logs": "secret"})
    mismatched_artifact = __import__("copy").deepcopy(wire)
    mismatched_artifact["artifact"]["image_digest"] = "sha256:" + ("f" * 64)
    with pytest.raises(ValueError, match="evidence values are invalid"):
        KubernetesQualificationEvidence.from_dict(mismatched_artifact)
    impossible_scenario = __import__("copy").deepcopy(wire)
    impossible_scenario["scenarios"][0]["worker_attachments"] = 2
    with pytest.raises(ValueError, match="evidence values are invalid"):
        KubernetesQualificationEvidence.from_dict(impossible_scenario)


def test_subprocess_and_wait_timeouts_are_bounded_and_actionable() -> None:
    captured: dict[str, object] = {}

    def timed_out(command, **kwargs):
        captured.update(command=command, **kwargs)
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    with pytest.raises(QualificationCommandError, match="timed out after 7.0s"):
        run_bounded(("kubectl", "get", "pods"), timeout_seconds=7, runner=timed_out)
    assert captured["timeout"] == 7

    def secret_failure(command, **kwargs):
        del kwargs
        return subprocess.CompletedProcess(
            command,
            1,
            stdout="",
            stderr="rejected secret super-secret-value",
        )

    with pytest.raises(QualificationCommandError) as redacted:
        run_bounded(
            ("kubectl", "apply", "-f", "-"),
            timeout_seconds=7,
            input_text='{"password":"super-secret-value"}',
            redact_diagnostics=True,
            runner=secret_failure,
        )
    assert "super-secret-value" not in str(redacted.value)
    assert "diagnostic redacted" in str(redacted.value)

    ticks = iter((0.0, 0.0, 0.3, 0.6))
    with pytest.raises(QualificationTimeout, match="barrier active_execution"):
        wait_until(
            lambda: False,
            description="barrier active_execution",
            timeout_seconds=0.5,
            interval_seconds=0,
            monotonic=lambda: next(ticks),
            sleeper=lambda _seconds: None,
        )


def test_context_confirmation_is_required_before_cluster_mutation(tmp_path: Path) -> None:
    environment = {
        "DEERFLOW_TEST_KUBERNETES": "1",
        "KUBECONFIG": str(tmp_path / "kubeconfig"),
        "DEERFLOW_TEST_KUBERNETES_CONTEXT": "qualification-context",
        "DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT": "different-context",
        "DEERFLOW_TEST_KUBERNETES_NAMESPACE": "hartmesh-qualification-a1b2c3",
        "DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID": "pod-recovery-20260808",
        "DEERFLOW_TEST_GATEWAY_IMAGE_REPOSITORY": "registry.example/hartmesh/gateway",
        "DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST": "sha256:" + ("a" * 64),
        "DEERFLOW_TEST_KUBERNETES_EVIDENCE": str(tmp_path / "evidence.json"),
    }

    with pytest.raises(QualificationPrerequisiteError, match="must exactly match"):
        KubernetesQualificationConfig.from_environment(environment)


def test_live_values_pin_artifact_shared_stores_and_test_only_hooks(tmp_path: Path) -> None:
    config = KubernetesQualificationConfig(
        kubeconfig=(tmp_path / "kubeconfig").resolve(),
        context="qualification-context",
        namespace="hartmesh-qualification-a1b2c3",
        image_repository="registry.example/hartmesh/gateway",
        image_digest="sha256:" + ("a" * 64),
        evidence_path=(tmp_path / "evidence.json").resolve(),
        qualification_id="pod-recovery-20260808",
    )

    values = KubernetesQualificationRunner(config).values()
    wire = __import__("json").dumps(values, sort_keys=True)

    assert values["deployment"] == {
        "mode": "durable_one_replica",
        "persistenceTier": "shared_durable",
    }
    assert values["gateway"]["image"] == {
        "repository": "registry.example/hartmesh/gateway",
        "digest": "sha256:" + ("a" * 64),
    }
    assert values["postgresql"]["existingSecret"] == "hartmesh-qualification-stores"
    assert values["redis"]["existingSecret"] == "hartmesh-qualification-stores"
    assert values["gateway"]["extraEnvFrom"] == [{"configMapRef": {"name": "hartmesh-qualification-runtime"}}]
    assert "KubernetesQualificationChatModel" in values["config"]
    assert "durable_production" in values["config"]
    assert "qualification-password" not in wire
    assert "postgresql://" not in wire
    assert "redis://" not in wire
    parsed = AppConfig.model_validate(yaml.safe_load(values["config"]))
    assert parsed.models[0].use.endswith(":KubernetesQualificationChatModel")
    assert parsed.run_events.backend == "db"
    assert KubernetesQualificationRunner._GRACEFUL_DEADLINE_SECONDS == 10


def test_live_runner_discovers_the_repository_chart_from_test_support(tmp_path: Path) -> None:
    config = KubernetesQualificationConfig(
        kubeconfig=(tmp_path / "kubeconfig").resolve(),
        context="qualification-context",
        namespace="hartmesh-qualification-a1b2c3",
        image_repository="registry.example/hartmesh/gateway",
        image_digest="sha256:" + ("a" * 64),
        evidence_path=(tmp_path / "evidence.json").resolve(),
    )

    runner = KubernetesQualificationRunner(config)

    assert runner.repository_root == Path(__file__).resolve().parents[2]
    assert (runner.chart_path / "Chart.yaml").is_file()


def test_manual_workflow_is_opt_in_and_runs_only_the_marked_contract() -> None:
    workflow = (Path(__file__).resolve().parents[2] / ".github/workflows/kubernetes-qualification.yml").read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert 'DEERFLOW_TEST_KUBERNETES: "1"' in workflow
    assert "QUALIFICATION_KUBECONFIG_B64" in workflow
    assert "pytest -m kubernetes_contract" in workflow
    assert "kind create cluster" not in workflow
    assert "k3d cluster create" not in workflow


def test_empty_optional_cluster_driver_is_recorded_as_unknown() -> None:
    assert optional_cluster_driver({}) is None
    assert optional_cluster_driver({"DEERFLOW_TEST_KUBERNETES_DRIVER": ""}) is None
    assert optional_cluster_driver({"DEERFLOW_TEST_KUBERNETES_DRIVER": "kind"}) == "kind"


def test_failure_evidence_is_machine_readable_but_cannot_claim_pass(
    tmp_path: Path,
) -> None:
    evidence = KubernetesQualificationFailureEvidence(
        qualification_id="pod-recovery-20260808",
        image_digest="sha256:" + ("a" * 64),
        chart_version="2.1.0",
        chart_digest="sha256:" + ("b" * 64),
        configuration_digest="sha256:" + ("c" * 64),
        cluster_context="qualification-context",
        namespace="hartmesh-qualification-a1b2c3",
        completed_at=datetime(2026, 8, 8, 12, 0, tzinfo=UTC),
        completed_scenarios=("accepted_before_client_response",),
        failure_code="QualificationTimeout",
    )
    path = tmp_path / "failed.json"
    evidence.write(path)
    wire = __import__("json").loads(path.read_text(encoding="utf-8"))

    assert wire["status"] == "failed"
    assert wire["completed_scenarios"] == ["accepted_before_client_response"]
    assert "provider exploded with credential" not in str(wire)
    with pytest.raises(ValueError):
        KubernetesQualificationEvidence.from_dict(wire)
