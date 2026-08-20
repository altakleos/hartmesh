"""Offline contract tests for the opt-in Kubernetes qualification harness."""

from __future__ import annotations

import base64
import json
import subprocess
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from support.kubernetes_qualification import (
    KUBERNETES_OPT_IN_MESSAGE,
    KubernetesAcceptedSkillQualificationConfigV2,
    KubernetesAcceptedSkillQualificationRunnerV2,
    KubernetesQualificationConfig,
    KubernetesQualificationEvidence,
    KubernetesQualificationFailureEvidence,
    KubernetesQualificationRunner,
    QualificationCommandError,
    QualificationPrerequisiteError,
    QualificationTimeout,
    ScenarioEvidence,
    StoreContinuityEvidence,
    _AcceptedSkillAttemptObservation,
    evidence_sha256,
    kubernetes_qualification_enabled,
    optional_cluster_driver,
    run_bounded,
    validate_kubernetes_prerequisites,
    wait_until,
)

from deerflow.config.app_config import AppConfig
from deerflow.qualification_evidence import (
    ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2,
    AcceptedSkillMaterialEvidenceV2,
    QualificationEvidenceExpectation,
    verify_qualification_evidence,
)


def _accepted_skill_environment(tmp_path: Path) -> dict[str, str]:
    return {
        "DEERFLOW_TEST_KUBERNETES": "1",
        "DEERFLOW_TEST_KUBERNETES_SCOPE": ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2,
        "KUBECONFIG": str((tmp_path / "kubeconfig").resolve()),
        "DEERFLOW_TEST_KUBERNETES_CONTEXT": "qualification-context",
        "DEERFLOW_TEST_KUBERNETES_CONFIRM_CONTEXT": "qualification-context",
        "DEERFLOW_TEST_KUBERNETES_NAMESPACE": "hartmesh-qualification-a1b2c3",
        "DEERFLOW_TEST_KUBERNETES_QUALIFICATION_ID": "skill-projection-20260810",
        "DEERFLOW_TEST_GATEWAY_IMAGE_REPOSITORY": "registry.example/hartmesh/gateway",
        "DEERFLOW_TEST_GATEWAY_IMAGE_DIGEST": "sha256:" + ("a" * 64),
        "DEERFLOW_TEST_PROVISIONER_IMAGE_REPOSITORY": "registry.example/hartmesh/provisioner",
        "DEERFLOW_TEST_PROVISIONER_IMAGE_DIGEST": "sha256:" + ("b" * 64),
        "DEERFLOW_TEST_VERIFIER_IMAGE_REPOSITORY": "registry.example/hartmesh/provisioner",
        "DEERFLOW_TEST_VERIFIER_IMAGE_DIGEST": "sha256:" + ("b" * 64),
        "DEERFLOW_TEST_SANDBOX_IMAGE_REPOSITORY": "registry.example/hartmesh/sandbox",
        "DEERFLOW_TEST_SANDBOX_IMAGE_DIGEST": "sha256:" + ("c" * 64),
        "DEERFLOW_TEST_KUBERNETES_RWX_STORAGE_CLASS": "rwx-storage",
        "DEERFLOW_TEST_KUBERNETES_EVIDENCE": str(
            (tmp_path / "evidence.json").resolve(),
        ),
    }


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


def test_enabled_accepted_skill_qualification_requires_every_exact_artifact(
    tmp_path: Path,
) -> None:
    environment = _accepted_skill_environment(tmp_path)
    for name in (
        "DEERFLOW_TEST_PROVISIONER_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_PROVISIONER_IMAGE_DIGEST",
        "DEERFLOW_TEST_VERIFIER_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_VERIFIER_IMAGE_DIGEST",
        "DEERFLOW_TEST_SANDBOX_IMAGE_REPOSITORY",
        "DEERFLOW_TEST_SANDBOX_IMAGE_DIGEST",
        "DEERFLOW_TEST_KUBERNETES_RWX_STORAGE_CLASS",
    ):
        environment.pop(name)

    with pytest.raises(QualificationPrerequisiteError) as captured:
        validate_kubernetes_prerequisites(
            environment,
            executable_lookup=lambda name: f"/usr/bin/{name}",
        )

    diagnostic = str(captured.value)
    assert "DEERFLOW_TEST_PROVISIONER_IMAGE_REPOSITORY" in diagnostic
    assert "DEERFLOW_TEST_PROVISIONER_IMAGE_DIGEST" in diagnostic
    assert "DEERFLOW_TEST_VERIFIER_IMAGE_REPOSITORY" in diagnostic
    assert "DEERFLOW_TEST_VERIFIER_IMAGE_DIGEST" in diagnostic
    assert "DEERFLOW_TEST_SANDBOX_IMAGE_REPOSITORY" in diagnostic
    assert "DEERFLOW_TEST_SANDBOX_IMAGE_DIGEST" in diagnostic
    assert "DEERFLOW_TEST_KUBERNETES_RWX_STORAGE_CLASS" in diagnostic


def test_accepted_skill_qualification_config_requires_verifier_identity_to_match_deployed_image(
    tmp_path: Path,
) -> None:
    environment = _accepted_skill_environment(tmp_path)
    environment["DEERFLOW_TEST_VERIFIER_IMAGE_DIGEST"] = "sha256:" + ("d" * 64)

    with pytest.raises(
        QualificationPrerequisiteError,
        match="verifier image must exactly match",
    ):
        KubernetesAcceptedSkillQualificationConfigV2.from_environment(
            environment,
        )


def test_accepted_skill_live_values_select_pinned_cross_node_profile(
    tmp_path: Path,
) -> None:
    config = KubernetesAcceptedSkillQualificationConfigV2.from_environment(
        _accepted_skill_environment(tmp_path),
    )

    values = KubernetesAcceptedSkillQualificationRunnerV2(config).values()
    sandbox = yaml.safe_load(values["config"])["sandbox"]

    assert values["deployment"] == {
        "mode": "durable_one_replica",
        "persistenceTier": "shared_durable",
    }
    assert values["gateway"]["image"] == {
        "repository": "registry.example/hartmesh/gateway",
        "digest": "sha256:" + ("a" * 64),
    }
    assert values["provisioner"]["enabled"] is True
    assert values["provisioner"]["image"] == {
        "repository": "registry.example/hartmesh/provisioner",
        "digest": "sha256:" + ("b" * 64),
    }
    assert values["provisioner"]["sandboxImage"] == ("registry.example/hartmesh/sandbox@sha256:" + ("c" * 64))
    assert values["provisioner"]["acceptedSkillProjectionProfile"] == ("rwx_verified_copy_v2")
    assert values["provisioner"]["acceptedAttempt"] == {
        "leaseSeconds": 120,
        "reconcileIntervalSeconds": 30,
        "reconcileLimit": 100,
    }
    assert values["persistence"]["home"] == {
        "enabled": True,
        "storageClass": "rwx-storage",
        "accessMode": "ReadWriteMany",
        "size": "2Gi",
    }
    assert values["skills"] == {
        "enabled": True,
        "existingClaim": "hartmesh-qualification-skill-source",
        "configMap": "",
    }
    assert sandbox["use"] == ("deerflow.community.aio_sandbox:AioSandboxProvider")
    assert sandbox["provisioner_url"] == ("http://hartmesh-qualification-deer-flow-provisioner:8002")
    assert sandbox["accepted_skill_projection_profile"] == ("rwx_verified_copy_v2")


def test_accepted_skill_attempt_binds_live_receipt_images_ledger_and_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = KubernetesAcceptedSkillQualificationRunnerV2(
        KubernetesAcceptedSkillQualificationConfigV2.from_environment(
            _accepted_skill_environment(tmp_path),
        )
    )
    snapshot = "1" * 64
    materialization = "2" * 64
    verifier_receipt = "3" * 64
    receipt = {
        "version": 2,
        "profile": "rwx_verified_copy_v2",
        "snapshot_id": snapshot,
        "content_digest": snapshot,
        "file_count": 2,
        "total_bytes": 8,
    }
    pod = {
        "metadata": {
            "name": "sandbox-attempt",
            "uid": "pod-uid",
            "labels": {
                "app.kubernetes.io/component": "sandbox",
                "sandbox-id": "attempt",
                "hartmesh.io/accepted-skill-profile": "rwx_verified_copy_v2",
            },
            "annotations": {"hartmesh.io/accepted-skill-run": "run-1"},
        },
        "spec": {"nodeName": "worker-b"},
        "status": {
            "containerStatuses": [
                {
                    "name": "sandbox",
                    "imageID": "containerd://sha256:" + ("c" * 64),
                },
                {
                    "name": "accepted-skill-gate",
                    "imageID": "containerd://sha256:" + ("b" * 64),
                },
            ],
            "initContainerStatuses": [
                {
                    "name": "accepted-skill-verifier",
                    "imageID": "containerd://sha256:" + ("b" * 64),
                }
            ],
        },
    }
    lease = {
        "metadata": {
            "name": "attempt-lease",
            "uid": "lease-uid",
            "annotations": {
                "hartmesh.io/accepted-skill-run": "run-1",
                "hartmesh.io/accepted-attempt-state": "materialized",
                "hartmesh.io/accepted-materialization-digest": materialization,
                "hartmesh.io/accepted-verifier-receipt-digest": verifier_receipt,
            },
        }
    }

    def kubectl(*arguments: str, **_kwargs: object) -> str:
        if arguments[:2] == ("get", "pods"):
            return json.dumps({"items": [pod]})
        if arguments[:2] == ("get", "leases"):
            return json.dumps({"items": [lease]})
        if arguments[:4] == (
            "exec",
            "sandbox-attempt",
            "-c",
            "accepted-skill-gate",
        ):
            return json.dumps(receipt)
        if arguments[:4] == (
            "exec",
            "sandbox-attempt",
            "-c",
            "sandbox",
        ):
            script = arguments[-1]
            payload = (
                b"---\nname: qualification-skill\ndescription: Deterministic accepted-skill qualification fixture.\nallowed-tools:\n  - read_file\n---\nRead resources/proof.txt only from the accepted immutable snapshot.\n"
                if "SKILL.md" in script
                else b"hartmesh accepted skill qualification v2\n"
            )
            return base64.b64encode(payload).decode("ascii")
        raise AssertionError(arguments)

    monkeypatch.setattr(runner, "_kubectl", kubectl)
    monkeypatch.setattr(
        runner,
        "_run_execution_evidence",
        lambda _run_id: {
            "version": 2,
            "snapshot_id": snapshot,
            "pod_uid": "pod-uid",
            "lease_uid": "lease-uid",
            "materialization_evidence_digest": materialization,
            "verifier_receipt_digest": verifier_receipt,
        },
    )

    attempt = runner._accepted_attempt(
        "active_execution",
        "run-1",
        gateway_node="worker-a",
    )

    assert attempt.gateway_node == "worker-a"
    assert attempt.pod_node == "worker-b"
    assert attempt.token_review_authenticated is False
    assert attempt.lease_renewals == 0


def test_accepted_skill_lease_renewal_and_gateway_replacement_result_are_observed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = KubernetesAcceptedSkillQualificationRunnerV2(
        KubernetesAcceptedSkillQualificationConfigV2.from_environment(
            _accepted_skill_environment(tmp_path),
        )
    )
    attempt = _AcceptedSkillAttemptObservation(
        scenario="active_execution",
        run_id="run-1",
        sandbox_id="sandbox-1",
        pod_name="pod-1",
        pod_uid="pod-uid-1",
        gateway_node="worker-a",
        pod_node="worker-b",
        lease_name="lease-1",
        lease_uid="lease-uid-1",
        receipt={"version": 2},
        materialization_digest="1" * 64,
        verifier_receipt_digest="2" * 64,
        token_review_authenticated=True,
        lease_renewals=0,
    )
    attempt_identity = "1" * 64
    leases = iter(
        (
            {
                "metadata": {
                    "uid": attempt.lease_uid,
                    "resourceVersion": "10",
                    "annotations": {
                        "hartmesh.io/accepted-attempt-identity": attempt_identity,
                    },
                },
                "spec": {
                    "holderIdentity": f"accepted:{attempt_identity}",
                    "leaseDurationSeconds": 120,
                    "renewTime": "2026-08-10T12:00:00Z",
                },
            },
            {
                "metadata": {
                    "uid": attempt.lease_uid,
                    "resourceVersion": "11",
                    "annotations": {
                        "hartmesh.io/accepted-attempt-identity": attempt_identity,
                    },
                },
                "spec": {
                    "holderIdentity": f"accepted:{attempt_identity}",
                    "leaseDurationSeconds": 120,
                    "renewTime": "2026-08-10T12:00:01.125Z",
                },
            },
        )
    )
    monkeypatch.setattr(
        runner,
        "_kubectl",
        lambda *_args, **_kwargs: json.dumps(next(leases)),
    )

    renewed = runner._wait_for_lease_renewal(attempt)
    assert renewed.lease_renewals == 1
    digest = renewed.result_digest(
        evidence_scenario="gateway_replacement_cleanup",
        cleanup_outcome="deleted",
        gateway_replacement_uid="replacement-gateway-uid",
    )
    assert digest.startswith("sha256:")
    assert digest != renewed.result_digest(
        evidence_scenario="gateway_replacement_cleanup",
        cleanup_outcome="deleted",
    )


def test_accepted_skill_lease_resource_version_change_is_not_renewal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = KubernetesAcceptedSkillQualificationRunnerV2(
        KubernetesAcceptedSkillQualificationConfigV2.from_environment(
            _accepted_skill_environment(tmp_path),
        )
    )
    attempt = _AcceptedSkillAttemptObservation(
        scenario="active_execution",
        run_id="run-1",
        sandbox_id="sandbox-1",
        pod_name="pod-1",
        pod_uid="pod-uid-1",
        gateway_node="worker-a",
        pod_node="worker-b",
        lease_name="lease-1",
        lease_uid="lease-uid-1",
        receipt={"version": 2},
        materialization_digest="1" * 64,
        verifier_receipt_digest="2" * 64,
        token_review_authenticated=True,
        lease_renewals=0,
    )
    attempt_identity = "1" * 64

    def lease(resource_version: str) -> str:
        return json.dumps(
            {
                "metadata": {
                    "uid": attempt.lease_uid,
                    "resourceVersion": resource_version,
                    "annotations": {
                        "hartmesh.io/accepted-attempt-identity": attempt_identity,
                    },
                },
                "spec": {
                    "holderIdentity": f"accepted:{attempt_identity}",
                    "leaseDurationSeconds": 120,
                    "renewTime": "2026-08-10T12:00:00Z",
                },
            }
        )

    leases = iter((lease("10"), lease("11")))
    monkeypatch.setattr(runner, "_kubectl", lambda *_args, **_kwargs: next(leases))

    def poll_once(predicate, **_kwargs) -> None:
        if not predicate():
            raise QualificationTimeout("renewTime did not advance")

    monkeypatch.setattr(
        "support.kubernetes_qualification.wait_until",
        poll_once,
    )

    with pytest.raises(QualificationTimeout, match="renewTime did not advance"):
        runner._wait_for_lease_renewal(attempt)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("uid", "replaced"),
        ("holder", "holder identity changed"),
        ("duration", "duration changed"),
        ("timestamp", "renewTime is invalid"),
        ("initial_holder", "holder identity changed"),
        ("initial_duration", "duration changed"),
        ("initial_timestamp", "renewTime is invalid"),
    ],
)
def test_accepted_skill_lease_renewal_rejects_identity_or_time_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
    message: str,
) -> None:
    runner = KubernetesAcceptedSkillQualificationRunnerV2(
        KubernetesAcceptedSkillQualificationConfigV2.from_environment(
            _accepted_skill_environment(tmp_path),
        )
    )
    attempt = _AcceptedSkillAttemptObservation(
        scenario="active_execution",
        run_id="run-1",
        sandbox_id="sandbox-1",
        pod_name="pod-1",
        pod_uid="pod-uid-1",
        gateway_node="worker-a",
        pod_node="worker-b",
        lease_name="lease-1",
        lease_uid="lease-uid-1",
        receipt={"version": 2},
        materialization_digest="1" * 64,
        verifier_receipt_digest="2" * 64,
        token_review_authenticated=True,
        lease_renewals=0,
    )
    attempt_identity = "1" * 64

    def lease(*, renewed: bool) -> dict[str, object]:
        value: dict[str, object] = {
            "metadata": {
                "uid": attempt.lease_uid,
                "resourceVersion": "11" if renewed else "10",
                "annotations": {
                    "hartmesh.io/accepted-attempt-identity": attempt_identity,
                },
            },
            "spec": {
                "holderIdentity": f"accepted:{attempt_identity}",
                "leaseDurationSeconds": 120,
                "renewTime": ("2026-08-10T12:00:01Z" if renewed else "2026-08-10T12:00:00Z"),
            },
        }
        if not renewed and mutation == "initial_holder":
            value["spec"]["holderIdentity"] = "accepted:" + ("2" * 64)
        elif not renewed and mutation == "initial_duration":
            value["spec"]["leaseDurationSeconds"] = 121
        elif not renewed and mutation == "initial_timestamp":
            value["spec"]["renewTime"] = "x" * 65
        elif renewed and mutation == "uid":
            value["metadata"]["uid"] = "replacement-uid"
        elif renewed and mutation == "holder":
            value["spec"]["holderIdentity"] = "accepted:" + ("2" * 64)
        elif renewed and mutation == "duration":
            value["spec"]["leaseDurationSeconds"] = 121
        elif renewed and mutation == "timestamp":
            value["spec"]["renewTime"] = "not-a-time"
        return value

    leases = iter((lease(renewed=False), lease(renewed=True)))
    monkeypatch.setattr(
        runner,
        "_kubectl",
        lambda *_args, **_kwargs: json.dumps(next(leases)),
    )

    with pytest.raises(QualificationCommandError, match=message):
        runner._wait_for_lease_renewal(attempt)


def test_accepted_skill_token_review_uses_confirmed_context_and_exact_service_account(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubernetesAcceptedSkillQualificationConfigV2.from_environment(
        _accepted_skill_environment(tmp_path),
    )
    runner = KubernetesAcceptedSkillQualificationRunnerV2(config)
    attempt = _AcceptedSkillAttemptObservation(
        scenario="active_execution",
        run_id="run-1",
        sandbox_id="sandbox-1",
        pod_name="pod-1",
        pod_uid="pod-uid-1",
        gateway_node="worker-a",
        pod_node="worker-b",
        lease_name="lease-1",
        lease_uid="lease-uid-1",
        receipt={"version": 2},
        materialization_digest="1" * 64,
        verifier_receipt_digest="2" * 64,
        token_review_authenticated=False,
        lease_renewals=0,
    )
    monkeypatch.setattr(runner, "_component_pod_name", lambda _name: "gateway-pod")
    monkeypatch.setattr(
        runner,
        "_kubectl",
        lambda *args, **kwargs: "projected-bearer" if args[:2] == ("exec", "gateway-pod") and kwargs.get("redact_diagnostics") is True else "",
    )
    calls: list[tuple[tuple[str, ...], str]] = []

    def run(
        command: tuple[str, ...],
        *,
        input_text: str,
        **kwargs: object,
    ) -> str:
        assert kwargs["redact_diagnostics"] is True
        calls.append((command, input_text))
        return json.dumps(
            {
                "status": {
                    "authenticated": True,
                    "audiences": ["hartmesh-provisioner"],
                    "user": {"username": ("system:serviceaccount:hartmesh-qualification-a1b2c3:hartmesh-qualification-deer-flow-gateway")},
                }
            }
        )

    monkeypatch.setattr(runner, "_run", run)

    verified = runner._verify_gateway_token_review(attempt)

    assert verified.token_review_authenticated is True
    command, request = calls[0]
    assert command == config.token_review()
    assert "projected-bearer" in request
    assert config.namespace not in request


def _wire_accepted_skill_qualify_fake(
    runner: KubernetesAcceptedSkillQualificationRunnerV2,
    monkeypatch: pytest.MonkeyPatch,
    events: list[str],
    *,
    fail_cleanup: bool = False,
) -> None:
    class PortForward:
        port = 18001

        def __enter__(self):
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    monkeypatch.setattr(
        "support.kubernetes_qualification.validate_kubernetes_prerequisites",
        lambda _environment: events.append("prerequisites"),
    )
    monkeypatch.setattr(
        "support.kubernetes_qualification._PortForward",
        lambda *_args, **_kwargs: PortForward(),
    )
    monkeypatch.setattr(runner, "_confirm_context", lambda: events.append("context"))
    monkeypatch.setattr(
        runner,
        "values",
        lambda: {"deployment": {}, "qualification": "accepted-skill-v2"},
    )
    monkeypatch.setattr(
        runner,
        "_create_namespace_and_configuration",
        lambda: events.append("namespace-created"),
    )
    monkeypatch.setattr(
        runner,
        "_install",
        lambda _values: events.append("installed"),
    )
    monkeypatch.setattr(
        runner,
        "_ready_gateway",
        lambda **_kwargs: ("gateway-0", "gateway-uid-0"),
    )
    monkeypatch.setattr(
        runner,
        "_assert_provisioner_image",
        lambda: events.append("provisioner-image"),
    )
    stores = (
        StoreContinuityEvidence(
            component="postgres",
            pod_uid="postgres-pod",
            volume_uid="postgres-volume",
            image_id="postgres@sha256:" + ("1" * 64),
            version="17.5",
        ),
        StoreContinuityEvidence(
            component="redis",
            pod_uid="redis-pod",
            volume_uid="redis-volume",
            image_id="redis@sha256:" + ("2" * 64),
            version="7.4.3",
        ),
    )
    monkeypatch.setattr(runner, "_shared_store_evidence", lambda: stores)
    monkeypatch.setattr(
        runner,
        "_schedulable_nodes",
        lambda: ("worker-a", "worker-b"),
    )
    monkeypatch.setattr(runner, "_rwx_volume_identity", lambda: "rwx-pvc-uid")
    monkeypatch.setattr(
        runner,
        "_initialize_admin",
        lambda _client: events.append("admin-initialized"),
    )
    monkeypatch.setattr(runner, "_login_admin", lambda _client: None)
    monkeypatch.setattr(runner, "_register_nonowner", lambda _client: None)
    monkeypatch.setattr(runner, "_gateway_node", lambda: "worker-a")

    def accepted_attempt(
        scenario: str,
        run_id: str,
        *,
        gateway_node: str,
    ) -> _AcceptedSkillAttemptObservation:
        events.append(f"attempt:{scenario}")
        return _AcceptedSkillAttemptObservation(
            scenario=scenario,
            run_id=run_id,
            sandbox_id=f"sandbox-{scenario}",
            pod_name=f"pod-{scenario}",
            pod_uid=f"pod-uid-{scenario}",
            gateway_node=gateway_node,
            pod_node="worker-b",
            lease_name=f"lease-{scenario}",
            lease_uid=f"lease-uid-{scenario}",
            receipt={
                "version": 2,
                "profile": "rwx_verified_copy_v2",
                "snapshot_id": "3" * 64,
                "content_digest": "3" * 64,
                "file_count": 2,
                "total_bytes": 192,
            },
            materialization_digest="4" * 64,
            verifier_receipt_digest="5" * 64,
            token_review_authenticated=False,
            lease_renewals=0,
        )

    monkeypatch.setattr(runner, "_accepted_attempt", accepted_attempt)
    monkeypatch.setattr(
        runner,
        "_verify_gateway_token_review",
        lambda attempt: events.append("token-review") or replace(attempt, token_review_authenticated=True),
    )
    monkeypatch.setattr(
        runner,
        "_wait_for_lease_renewal",
        lambda attempt: events.append("lease-renewed") or replace(attempt, lease_renewals=1),
    )
    monkeypatch.setattr(
        runner,
        "_delete_attempt_lease",
        lambda attempt: events.append(f"lease-deleted:{attempt.scenario}"),
    )

    def cleanup(attempt: _AcceptedSkillAttemptObservation) -> None:
        events.append(f"cleanup:{attempt.scenario}")
        if fail_cleanup and attempt.scenario == "active_execution":
            raise QualificationCommandError("cleanup did not converge")

    monkeypatch.setattr(runner, "_wait_for_attempt_cleanup", cleanup)

    def run_scenario(
        scenario,
        _client,
        _owner,
        _nonowner,
        gateway_pod,
        _forwarded,
        barrier_probe=None,
    ):
        events.append(f"scenario:{scenario}")
        run_id = f"run-{scenario}"
        if barrier_probe is not None:
            barrier_probe(scenario, run_id)
        replacement = gateway_pod
        if scenario == "graceful_rollout_termination":
            replacement = ("gateway-1", "gateway-uid-1")
        elif scenario == "forced_kill_after_graceful_deadline":
            replacement = ("gateway-2", "gateway-uid-2")
        starts = 0 if scenario == "accepted_before_worker_start" else 1
        termination = "graceful" if scenario == "graceful_rollout_termination" else ("forced_deadline" if scenario == "forced_kill_after_graceful_deadline" else "abrupt")
        return (
            ScenarioEvidence(
                name=scenario,
                run_id=run_id,
                worker_attachments=starts,
                graph_starts=starts,
                model_starts=starts,
                terminal_status="error",
                termination_mode=termination,
                old_pod_termination_millis=100,
            ),
            replacement,
        )

    monkeypatch.setattr(runner, "_run_scenario", run_scenario)
    monkeypatch.setattr(
        runner,
        "_environment_facts",
        lambda _gateway: {
            "migration_head": "0020_merge_mcp_task_results",
            "kubernetes_server_version": "v1.33.1",
        },
    )
    monkeypatch.setattr(
        runner,
        "_fixture_material",
        lambda _attempt: AcceptedSkillMaterialEvidenceV2(
            skill_name="qualification-skill",
            snapshot_digest="sha256:" + ("3" * 64),
            skill_tree_digest="sha256:" + ("4" * 64),
            allowed_tool_policy_digest="sha256:" + ("5" * 64),
            file_count=2,
            total_bytes=192,
            materialization_digest="sha256:" + ("6" * 64),
            receipt_digest="sha256:" + ("7" * 64),
        ),
    )
    monkeypatch.setattr(runner, "_chart_version", lambda: "2.1.0")
    monkeypatch.setattr(runner, "_chart_digest", lambda: "sha256:" + ("8" * 64))

    def publish(_values, evidence, _client) -> Path:
        assert all(
            f"cleanup:{scenario}" in events
            for scenario in (
                "active_execution",
                "terminal_before_lifecycle_commit",
                "graceful_rollout_termination",
                "forced_kill_after_graceful_deadline",
            )
        )
        events.append("published")
        path = runner.config.evidence_path.with_suffix(
            runner.config.evidence_path.suffix + ".passing",
        )
        evidence.write(path)
        return path

    monkeypatch.setattr(runner, "_publish_accepted_skill_qualification", publish)

    def kubectl(*arguments, **_kwargs) -> str:
        if arguments[:2] == ("delete", "namespace"):
            events.append("namespace-deleted")
            return ""
        raise AssertionError(f"unexpected kubectl call: {arguments}")

    monkeypatch.setattr(runner, "_kubectl", kubectl)
    monkeypatch.setattr(
        runner,
        "_collect_failure_artifacts",
        lambda: events.append("failure-artifacts"),
    )


def test_v2_qualify_maps_faults_cleans_attempts_and_publishes_atomically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = KubernetesAcceptedSkillQualificationRunnerV2(
        KubernetesAcceptedSkillQualificationConfigV2.from_environment(
            _accepted_skill_environment(tmp_path),
        )
    )
    events: list[str] = []
    _wire_accepted_skill_qualify_fake(runner, monkeypatch, events)

    evidence = runner.qualify()

    assert tuple(item.name for item in evidence.scenarios) == (
        "nonempty_material_execution",
        "token_review_and_lease_renewal",
        "gateway_replacement_cleanup",
        "sandbox_owner_loss_cleanup",
        "process_loss_cleanup",
    )
    assert evidence.environment.token_review_authenticated is True
    assert evidence.environment.lease_renewals == 1
    assert events.count("token-review") == 1
    assert events.count("lease-renewed") == 1
    assert events.count("lease-deleted:terminal_before_lifecycle_commit") == 1
    assert events.index("published") < events.index("namespace-deleted")
    assert runner.config.evidence_path.read_bytes() == evidence.canonical_bytes()
    assert not runner.config.evidence_path.with_suffix(
        runner.config.evidence_path.suffix + ".passing",
    ).exists()


def test_v2_qualify_never_publishes_when_cleanup_is_incomplete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner = KubernetesAcceptedSkillQualificationRunnerV2(
        KubernetesAcceptedSkillQualificationConfigV2.from_environment(
            _accepted_skill_environment(tmp_path),
        )
    )
    events: list[str] = []
    _wire_accepted_skill_qualify_fake(
        runner,
        monkeypatch,
        events,
        fail_cleanup=True,
    )

    with pytest.raises(QualificationCommandError, match="cleanup did not converge"):
        runner.qualify()

    failure = json.loads(runner.config.evidence_path.read_bytes())
    assert failure["status"] == "failed"
    assert failure["scope"] == ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2
    assert "published" not in events
    assert "namespace-deleted" not in events
    assert events.count("failure-artifacts") == 1


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


def test_graceful_rollout_uses_the_gateway_deployment_not_manual_pod_deletion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubernetesQualificationConfig(
        kubeconfig=(tmp_path / "kubeconfig").resolve(),
        context="qualification-context",
        namespace="hartmesh-qualification-a1b2c3",
        image_repository="registry.example/hartmesh/gateway",
        image_digest="sha256:" + ("a" * 64),
        evidence_path=(tmp_path / "evidence.json").resolve(),
    )
    runner = KubernetesQualificationRunner(config)
    commands: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        runner,
        "_kubectl",
        lambda *arguments, **_kwargs: commands.append(arguments) or "",
    )

    runner._restart_gateway_deployment()

    assert commands == [
        (
            "rollout",
            "restart",
            f"deployment/{runner.fullname}-gateway",
        )
    ]


def test_recreate_handoff_rejects_overlapping_gateway_pods(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = KubernetesQualificationConfig(
        kubeconfig=(tmp_path / "kubeconfig").resolve(),
        context="qualification-context",
        namespace="hartmesh-qualification-a1b2c3",
        image_repository="registry.example/hartmesh/gateway",
        image_digest="sha256:" + ("a" * 64),
        evidence_path=(tmp_path / "evidence.json").resolve(),
    )
    runner = KubernetesQualificationRunner(config)

    monkeypatch.setattr(
        runner,
        "_pod_json",
        lambda _component: {
            "items": [
                {"metadata": {"uid": "old-gateway-uid"}},
                {"metadata": {"uid": "replacement-gateway-uid"}},
            ]
        },
    )
    monkeypatch.setattr(
        "support.kubernetes_qualification.wait_until",
        lambda predicate, **_kwargs: predicate(),
    )

    with pytest.raises(QualificationCommandError, match="overlapped"):
        runner._wait_for_recreate_handoff("old-gateway-uid", started=0.0)


def test_manual_workflow_is_opt_in_and_runs_only_the_marked_contract() -> None:
    workflow = (Path(__file__).resolve().parents[2] / ".github/workflows/kubernetes-qualification.yml").read_text(encoding="utf-8")

    assert "workflow_dispatch:" in workflow
    assert 'DEERFLOW_TEST_KUBERNETES: "1"' in workflow
    assert "QUALIFICATION_KUBECONFIG_B64" in workflow
    assert "pytest -m kubernetes_contract" in workflow
    assert "KUBECONFIG: ${{ runner.temp }}" not in workflow
    exported_kubeconfig = 'printf \'KUBECONFIG=%s\\n\' "$RUNNER_TEMP/qualification-kubeconfig" >> "$GITHUB_ENV"'
    assert exported_kubeconfig in workflow
    assert workflow.index(exported_kubeconfig) < workflow.index("Materialize the explicitly supplied disposable-cluster config")
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
