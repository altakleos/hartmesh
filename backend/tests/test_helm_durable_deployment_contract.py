"""Semantic render contract for the durable one-replica Helm profile."""

from __future__ import annotations

import copy
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml
from support.kubernetes_qualification import (
    KubernetesQualificationConfig,
    KubernetesQualificationRunner,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CHART = _REPO_ROOT / "deploy" / "helm" / "deer-flow"
_VALUES = yaml.safe_load((_CHART / "values.yaml").read_text(encoding="utf-8"))
_HELM = shutil.which("helm")

pytestmark = pytest.mark.skipif(
    _HELM is None,
    reason="Helm semantic rendering requires the optional helm executable",
)


def _write_values(tmp_path: Path, values: dict[str, object]) -> Path:
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(values, sort_keys=False), encoding="utf-8")
    return path


def _render(
    tmp_path: Path,
    values: dict[str, object] | None = None,
    *,
    expect_success: bool = True,
) -> subprocess.CompletedProcess[str]:
    command = [
        str(_HELM),
        "template",
        "deer-flow",
        str(_CHART),
        "--namespace",
        "deer-flow",
    ]
    if values is not None:
        command.extend(["--values", str(_write_values(tmp_path, values))])
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    if expect_success:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0, result.stdout
    return result


def _documents(rendered: str) -> list[dict[str, object]]:
    return [document for document in yaml.safe_load_all(rendered) if isinstance(document, dict)]


def _workload(
    rendered: str,
    *,
    kind: str,
    component: str,
) -> dict[str, object]:
    return next(document for document in _documents(rendered) if document.get("kind") == kind and document.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == component)


def _production_values() -> dict[str, object]:
    values = copy.deepcopy(_VALUES)
    values["deployment"]["mode"] = "durable_one_replica"
    values["deployment"]["persistenceTier"] = "shared_durable"
    config = yaml.safe_load(values["config"])
    config.setdefault("deployment", {})["profile"] = "durable_production"
    config.setdefault("database", {})["backend"] = "postgres"
    values["config"] = yaml.safe_dump(config, sort_keys=False)
    values["gateway"]["image"]["digest"] = "sha256:" + ("a" * 64)
    values["provisioner"]["image"]["digest"] = "sha256:" + ("b" * 64)
    values["postgresql"]["existingSecret"] = "production-postgres"
    values["redis"]["existingSecret"] = "production-redis"
    return values


def _set_config_value(
    values: dict[str, object],
    path: tuple[str, ...],
    value: object,
) -> None:
    config = yaml.safe_load(values["config"])
    target = config
    for key in path[:-1]:
        target = target.setdefault(key, {})
    target[path[-1]] = value
    values["config"] = yaml.safe_dump(config, sort_keys=False)


def test_gateway_rollouts_preserve_single_process_ownership(tmp_path: Path) -> None:
    rendered = _render(tmp_path, _production_values()).stdout
    gateway = _workload(rendered, kind="Deployment", component="gateway")

    assert gateway["spec"]["replicas"] == 1
    assert gateway["spec"]["strategy"] == {
        "type": "Recreate",
        "rollingUpdate": None,
    }


def test_runtime_images_render_tags_or_digests_without_combining_them(
    tmp_path: Path,
) -> None:
    tagged = copy.deepcopy(_VALUES)
    tagged["gateway"]["image"] = {
        "repository": "registry.example/hartmesh-gateway",
        "tag": "v1.2.3",
        "digest": "",
    }
    tagged["frontend"]["image"] = {
        "repository": "registry.example/hartmesh-frontend",
        "tag": "v1.2.3",
        "digest": "",
    }
    tagged["provisioner"]["image"] = {
        "repository": "registry.example/hartmesh-provisioner",
        "tag": "v1.2.3",
        "digest": "",
    }
    tagged["nginx"]["image"] = {
        "repository": "registry.example/nginx",
        "tag": "v1.2.3",
        "digest": "",
    }
    tagged["postgresql"]["image"] = {
        "repository": "registry.example/postgres",
        "tag": "v1.2.3",
        "digest": "",
    }
    tagged["redis"]["image"] = {
        "repository": "registry.example/redis",
        "tag": "v1.2.3",
        "digest": "",
    }
    tagged_result = _render(tmp_path, tagged)
    tagged_gateway = _workload(
        tagged_result.stdout,
        kind="Deployment",
        component="gateway",
    )
    tagged_image = tagged_gateway["spec"]["template"]["spec"]["containers"][0]["image"]
    assert tagged_image == "registry.example/hartmesh-gateway:v1.2.3"
    tagged_frontend = _workload(
        tagged_result.stdout,
        kind="Deployment",
        component="frontend",
    )
    tagged_provisioner = _workload(
        tagged_result.stdout,
        kind="Deployment",
        component="provisioner",
    )
    assert tagged_frontend["spec"]["template"]["spec"]["containers"][0]["image"] == "registry.example/hartmesh-frontend:v1.2.3"
    assert tagged_provisioner["spec"]["template"]["spec"]["containers"][0]["image"] == "registry.example/hartmesh-provisioner:v1.2.3"
    tagged_nginx = _workload(
        tagged_result.stdout,
        kind="Deployment",
        component="nginx",
    )
    tagged_postgres = _workload(
        tagged_result.stdout,
        kind="StatefulSet",
        component="postgres",
    )
    tagged_redis = _workload(
        tagged_result.stdout,
        kind="StatefulSet",
        component="redis",
    )
    assert tagged_nginx["spec"]["template"]["spec"]["containers"][0]["image"] == "registry.example/nginx:v1.2.3"
    assert tagged_postgres["spec"]["template"]["spec"]["containers"][0]["image"] == "registry.example/postgres:v1.2.3"
    assert tagged_redis["spec"]["template"]["spec"]["containers"][0]["image"] == "registry.example/redis:v1.2.3"

    pinned = copy.deepcopy(tagged)
    pinned["gateway"]["image"]["digest"] = "sha256:" + ("c" * 64)
    pinned["frontend"]["image"]["digest"] = "sha256:" + ("d" * 64)
    pinned["provisioner"]["image"]["digest"] = "sha256:" + ("e" * 64)
    pinned["nginx"]["image"]["digest"] = "sha256:" + ("1" * 64)
    pinned["postgresql"]["image"]["digest"] = "sha256:" + ("2" * 64)
    pinned["redis"]["image"]["digest"] = "sha256:" + ("3" * 64)
    pinned_result = _render(tmp_path, pinned)
    pinned_gateway = _workload(
        pinned_result.stdout,
        kind="Deployment",
        component="gateway",
    )
    pinned_image = pinned_gateway["spec"]["template"]["spec"]["containers"][0]["image"]
    assert pinned_image == "registry.example/hartmesh-gateway@sha256:" + ("c" * 64)
    assert ":v1.2.3@" not in pinned_image
    pinned_frontend = _workload(
        pinned_result.stdout,
        kind="Deployment",
        component="frontend",
    )
    pinned_provisioner = _workload(
        pinned_result.stdout,
        kind="Deployment",
        component="provisioner",
    )
    assert pinned_frontend["spec"]["template"]["spec"]["containers"][0]["image"] == "registry.example/hartmesh-frontend@sha256:" + ("d" * 64)
    assert pinned_provisioner["spec"]["template"]["spec"]["containers"][0]["image"] == "registry.example/hartmesh-provisioner@sha256:" + ("e" * 64)
    pinned_nginx = _workload(
        pinned_result.stdout,
        kind="Deployment",
        component="nginx",
    )
    pinned_postgres = _workload(
        pinned_result.stdout,
        kind="StatefulSet",
        component="postgres",
    )
    pinned_redis = _workload(
        pinned_result.stdout,
        kind="StatefulSet",
        component="redis",
    )
    assert pinned_nginx["spec"]["template"]["spec"]["containers"][0]["image"] == "registry.example/nginx@sha256:" + ("1" * 64)
    assert pinned_postgres["spec"]["template"]["spec"]["containers"][0]["image"] == "registry.example/postgres@sha256:" + ("2" * 64)
    assert pinned_redis["spec"]["template"]["spec"]["containers"][0]["image"] == "registry.example/redis@sha256:" + ("3" * 64)


def test_existing_service_account_metadata_and_referenced_mounts_render_safely(
    tmp_path: Path,
) -> None:
    values = copy.deepcopy(_VALUES)
    values["serviceAccount"] = {
        "create": False,
        "name": "existing-runtime",
        "annotations": {},
        "automountServiceAccountToken": False,
    }
    values["provisioner"]["serviceAccount"] = {
        "create": False,
        "name": "existing-provisioner",
    }
    values["gateway"]["podLabels"] = {"operations.example/tier": "durable"}
    values["gateway"]["podAnnotations"] = {"operations.example/deployment-id": "deployment-42"}
    values["gateway"]["extraEnvFrom"] = [
        {"secretRef": {"name": "runtime-env"}},
        {"configMapRef": {"name": "runtime-settings"}},
    ]
    values["gateway"]["extraVolumes"] = [
        {"name": "credential-files", "secret": {"secretName": "runtime-files"}},
        {"name": "config-files", "configMap": {"name": "runtime-config"}},
    ]
    values["gateway"]["extraVolumeMounts"] = [
        {
            "name": "credential-files",
            "mountPath": "/var/run/hartmesh-secrets",
            "readOnly": True,
        },
        {
            "name": "config-files",
            "mountPath": "/etc/hartmesh-runtime",
            "readOnly": True,
        },
    ]

    result = _render(tmp_path, values)
    gateway = _workload(result.stdout, kind="Deployment", component="gateway")
    pod = gateway["spec"]["template"]
    spec = pod["spec"]
    container = spec["containers"][0]

    assert spec["serviceAccountName"] == "existing-runtime"
    assert spec["automountServiceAccountToken"] is False
    assert pod["metadata"]["labels"]["operations.example/tier"] == "durable"
    assert pod["metadata"]["annotations"]["operations.example/deployment-id"] == "deployment-42"
    assert container["envFrom"] == [
        {"secretRef": {"name": "runtime-env"}},
        {"configMapRef": {"name": "runtime-settings"}},
    ]
    assert {item["name"] for item in spec["volumes"]} >= {
        "credential-files",
        "config-files",
    }
    assert {item["name"] for item in container["volumeMounts"]} >= {
        "credential-files",
        "config-files",
    }
    assert "super-secret-value" not in result.stdout
    provisioner = _workload(
        result.stdout,
        kind="Deployment",
        component="provisioner",
    )
    assert provisioner["spec"]["template"]["spec"]["serviceAccountName"] == "existing-provisioner"
    service_accounts = [document for document in _documents(result.stdout) if document.get("kind") == "ServiceAccount"]
    assert all(document["metadata"]["name"] not in {"deer-flow-deer-flow-gateway", "deer-flow-deer-flow-provisioner"} for document in service_accounts)


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda values: values["gateway"]["image"].update({"digest": "sha256:not-a-digest"}),
            "gateway image digest",
        ),
        (
            lambda values: values["gateway"].update({"terminationGracePeriodSeconds": 10}),
            "terminationGracePeriodSeconds",
        ),
        (
            lambda values: values["gateway"].update({"terminationGracePeriodSeconds": 0}),
            "terminationGracePeriodSeconds",
        ),
        (
            lambda values: values["gateway"]["readinessProbe"].update({"timeoutSeconds": 5}),
            "readiness probe timeout",
        ),
        (
            lambda values: values["gateway"]["livenessProbe"].update({"timeoutSeconds": 0}),
            "liveness probe timeoutSeconds must be positive",
        ),
        (
            lambda values: values["gateway"]["livenessProbe"].update({"timeoutSeconds": 21}),
            "liveness probe periodSeconds must be at least timeoutSeconds",
        ),
        (
            lambda values: _set_config_value(
                values,
                ("deployment", "readiness", "overall_timeout_seconds"),
                0,
            ),
            "overall timeout must be greater than 0",
        ),
        (
            lambda values: _set_config_value(
                values,
                (
                    "deployment",
                    "readiness",
                    "capability_probe_timeout_seconds",
                ),
                31,
            ),
            "capability probe timeout must be greater than 0 and at most 30",
        ),
        (
            lambda values: _set_config_value(
                values,
                ("deployment", "shutdown", "run_seconds"),
                0,
            ),
            "shutdown budget run_seconds must be greater than 0",
        ),
        (
            lambda values: _set_config_value(
                values,
                ("deployment", "shutdown", "run_seconds"),
                121,
            ),
            "shutdown budget run_seconds must be greater than 0 and at most 120",
        ),
        (
            lambda values: _set_config_value(
                values,
                ("memory", "shutdown_flush_timeout_seconds"),
                301,
            ),
            "memory shutdown flush budget must be at least 1 and at most 300",
        ),
        (
            lambda values: values["gateway"]["image"].update(
                {
                    "repository": "registry.example:5000/hartmesh-gateway:v1",
                    "digest": "sha256:" + ("f" * 64),
                }
            ),
            "image repository must not contain a scheme, tag",
        ),
        (
            lambda values: values["gateway"].update({"podAnnotations": {f"example.com/key-{index}": "v" for index in range(33)}}),
            "pod annotations",
        ),
        (
            lambda values: values["gateway"].update({"extraVolumes": [{"name": "host-files", "hostPath": {"path": "/etc"}}]}),
            "exactly one secret or configMap source",
        ),
        (
            lambda values: values["provisioner"].update(
                {"gatewayTokenAudience": ""},
            ),
            "gatewayTokenAudience",
        ),
        (
            lambda values: values["gateway"].update({"extraVolumes": [{"name": "config", "configMap": {"name": "other"}}]}),
            "reserved or duplicated",
        ),
        (
            lambda values: values["gateway"].update(
                {
                    "extraVolumes": [{"name": "settings", "configMap": {"name": "settings"}}],
                    "extraVolumeMounts": [
                        {
                            "name": "settings",
                            "mountPath": "/app/backend/config.yaml",
                            "readOnly": True,
                        }
                    ],
                }
            ),
            "mountPath /app/backend/config.yaml is reserved",
        ),
        (
            lambda values: values["deployment"].update({"persistenceTier": "process_local"}),
            "persistenceTier must match",
        ),
        (
            lambda values: values["deployment"].update(
                {
                    "qualificationEvidence": [
                        {
                            "qualificationId": "durable-contract",
                            "artifactDigest": "sha256:" + ("a" * 64),
                            "completedAt": "2026-08-08T12:00:00." + ("1" * 128) + "Z",
                        }
                    ]
                }
            ),
            "completedAt must be an RFC3339 timestamp",
        ),
    ],
)
def test_invalid_digest_timing_and_metadata_are_rejected(
    tmp_path: Path,
    mutator,
    message: str,
) -> None:
    values = copy.deepcopy(_VALUES)
    mutator(values)
    result = _render(tmp_path, values, expect_success=False)
    assert message in result.stderr


def test_production_mode_requires_pinned_runtime_images_and_one_replica(
    tmp_path: Path,
) -> None:
    missing_gateway = _production_values()
    missing_gateway["gateway"]["image"]["digest"] = ""
    result = _render(tmp_path, missing_gateway, expect_success=False)
    assert "production validation requires a gateway image digest" in result.stderr

    missing_provisioner = _production_values()
    missing_provisioner["provisioner"]["image"]["digest"] = ""
    result = _render(tmp_path, missing_provisioner, expect_success=False)
    assert "production validation requires a provisioner image digest" in result.stderr

    missing_postgres_secret = _production_values()
    missing_postgres_secret["postgresql"]["existingSecret"] = ""
    result = _render(tmp_path, missing_postgres_secret, expect_success=False)
    assert "PostgreSQL credentials through an existingSecret" in result.stderr

    missing_redis_secret = _production_values()
    missing_redis_secret["redis"]["existingSecret"] = ""
    result = _render(tmp_path, missing_redis_secret, expect_success=False)
    assert "Redis connection credentials through an existingSecret" in result.stderr

    replicas = _production_values()
    replicas["gateway"]["replicas"] = 2
    result = _render(tmp_path, replicas, expect_success=False)
    assert "supported chart topology requires gateway.replicas=1" in result.stderr

    local_replicas = copy.deepcopy(_VALUES)
    local_replicas["gateway"]["replicas"] = 2
    result = _render(tmp_path, local_replicas, expect_success=False)
    assert "supported chart topology requires gateway.replicas=1" in result.stderr


def test_production_mode_rejects_process_local_storage(
    tmp_path: Path,
) -> None:
    values = _production_values()
    values["deployment"]["persistenceTier"] = "process_local"
    config = yaml.safe_load(values["config"])
    config["database"]["backend"] = "memory"
    values["config"] = yaml.safe_dump(config, sort_keys=False)

    result = _render(tmp_path, values, expect_success=False)
    assert "durable_one_replica requires shared_durable persistence" in result.stderr


def test_production_mode_rejects_process_local_inbound_receipts(
    tmp_path: Path,
) -> None:
    values = _production_values()
    _set_config_value(values, ("dedupe_storage", "backend"), "memory")

    result = _render(tmp_path, values, expect_success=False)

    assert "requires PostgreSQL inbound receipt storage" in result.stderr


@pytest.mark.parametrize(
    ("command_timeout", "expected_error"),
    [
        (None, "requires a finite database.command_timeout"),
        (float("inf"), None),
    ],
)
def test_production_mode_rejects_unbounded_postgres_commands(
    tmp_path: Path,
    command_timeout: float | None,
    expected_error: str | None,
) -> None:
    values = _production_values()
    _set_config_value(values, ("database", "command_timeout"), command_timeout)

    result = _render(tmp_path, values, expect_success=False)

    if expected_error is not None:
        assert expected_error in result.stderr


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda values: values.update({"secrets": {"OPENAI_API_KEY": "super-secret-value"}}),
            "forbids inline provider secrets",
        ),
        (
            lambda values: values["postgresql"]["auth"].update({"password": "super-secret-value"}),
            "forbids inline PostgreSQL credentials",
        ),
        (
            lambda values: values["postgresql"]["external"].update({"databaseUrl": "postgresql://user:super-secret-value@db/runtime"}),
            "forbids inline PostgreSQL credentials",
        ),
        (
            lambda values: values["redis"]["auth"].update({"password": "super-secret-value"}),
            "forbids inline Redis credentials",
        ),
        (
            lambda values: values["redis"]["external"].update({"redisUrl": "redis://:super-secret-value@cache:6379/0"}),
            "forbids inline Redis credentials",
        ),
    ],
)
def test_production_mode_rejects_inline_secret_material(
    tmp_path: Path,
    mutator,
    message: str,
) -> None:
    values = _production_values()
    mutator(values)

    result = _render(tmp_path, values, expect_success=False)

    assert message in result.stderr
    assert "super-secret-value" not in result.stdout
    assert "super-secret-value" not in result.stderr


def test_production_provenance_and_qualification_reach_trusted_environment(
    tmp_path: Path,
) -> None:
    values = _production_values()
    values["deployment"]["provenance"] = {
        "sourceRevision": "d" * 40,
    }
    values["deployment"]["qualificationEvidence"] = [
        {
            "qualificationId": "durable-contract-2026-08",
            "artifactDigest": "sha256:" + ("e" * 64),
            "completedAt": "2026-08-08T12:00:00Z",
        }
    ]

    result = _render(tmp_path, values)
    gateway = _workload(result.stdout, kind="Deployment", component="gateway")
    env = {item["name"]: item.get("value") for item in gateway["spec"]["template"]["spec"]["containers"][0]["env"] if "value" in item}

    assert env["DEER_FLOW_IMAGE_DIGEST"] == "sha256:" + ("a" * 64)
    assert env["DEER_FLOW_SOURCE_REVISION"] == "d" * 40
    assert "durable-contract-2026-08" in env["DEER_FLOW_QUALIFICATION_EVIDENCE"]
    assert "sha256:" + ("e" * 64) in env["DEER_FLOW_QUALIFICATION_EVIDENCE"]


def test_kubernetes_qualification_scope_and_status_are_strict(
    tmp_path: Path,
) -> None:
    values = _production_values()
    values["deployment"]["qualificationEvidence"] = [
        {
            "qualificationId": "pod-recovery-2026-08",
            "artifactDigest": "sha256:" + ("e" * 64),
            "completedAt": "2026-08-08T12:00:00Z",
            "scope": "durable_one_replica_pod_recovery",
            "status": "passed",
        }
    ]

    rendered = _render(tmp_path, values)
    assert "durable_one_replica_pod_recovery" in rendered.stdout

    values["deployment"]["qualificationEvidence"][0]["status"] = "collected"
    result = _render(tmp_path, values, expect_success=False)
    assert "qualification status must be passed" in result.stderr

    del values["deployment"]["qualificationEvidence"][0]["status"]
    result = _render(tmp_path, values, expect_success=False)
    assert "scope and status must be supplied together" in result.stderr


def test_default_profile_is_explicitly_local_and_unqualified(tmp_path: Path) -> None:
    result = _render(tmp_path)
    config_map = next(document for document in _documents(result.stdout) if document.get("kind") == "ConfigMap" and document.get("metadata", {}).get("name") == "deer-flow-deer-flow-config")
    config = yaml.safe_load(config_map["data"]["config.yaml"])

    assert _VALUES["deployment"]["mode"] == "local_evaluation"
    assert config["deployment"]["profile"] == "local_development"
    assert _VALUES["deployment"]["qualificationEvidence"] == []


def test_live_qualification_values_render_the_exact_one_replica_profile(
    tmp_path: Path,
) -> None:
    config = KubernetesQualificationConfig(
        kubeconfig=(tmp_path / "kubeconfig").resolve(),
        context="qualification-context",
        namespace="hartmesh-qualification-a1b2c3",
        image_repository="registry.example/hartmesh/gateway",
        image_digest="sha256:" + ("a" * 64),
        evidence_path=(tmp_path / "evidence.json").resolve(),
        qualification_id="pod-recovery-20260808",
    )

    result = _render(tmp_path, KubernetesQualificationRunner(config).values())
    gateway = _workload(result.stdout, kind="Deployment", component="gateway")

    assert gateway["spec"]["replicas"] == 1
    assert gateway["spec"]["template"]["spec"]["containers"][0]["image"] == ("registry.example/hartmesh/gateway@sha256:" + ("a" * 64))
    assert gateway["spec"]["template"]["spec"]["terminationGracePeriodSeconds"] == 12
    assert "qualification-password" not in result.stdout
    assert "postgresql+asyncpg://" not in result.stdout


def test_verified_accepted_skill_projection_rejects_same_node_rwo_fallback(
    tmp_path: Path,
) -> None:
    values = _production_values()
    values["provisioner"]["acceptedSkillProjectionProfile"] = "rwx_verified_copy_v2"
    values["provisioner"]["sandboxImage"] = "registry.example/aio@sha256:" + ("c" * 64)
    values["persistence"]["home"]["accessMode"] = "ReadWriteOnce"

    result = _render(tmp_path, values, expect_success=False)

    assert "requires persistence.home.accessMode=ReadWriteMany" in result.stderr
    assert "same-node RWO fallback is unsupported" in result.stderr


def test_verified_accepted_skill_projection_renders_pinned_cross_node_profile(
    tmp_path: Path,
) -> None:
    values = _production_values()
    values["provisioner"]["acceptedSkillProjectionProfile"] = "rwx_verified_copy_v2"
    values["provisioner"]["sandboxImage"] = "registry.example/aio@sha256:" + ("c" * 64)
    values["persistence"]["home"]["accessMode"] = "ReadWriteMany"

    rendered = _render(tmp_path, values).stdout
    provisioner = _workload(
        rendered,
        kind="Deployment",
        component="provisioner",
    )
    assert provisioner["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["httpGet"]["path"] == "/ready"
    environment = {item["name"]: item.get("value") for item in provisioner["spec"]["template"]["spec"]["containers"][0]["env"]}

    assert environment["ACCEPTED_SKILL_PROJECTION_PROFILE"] == ("rwx_verified_copy_v2")
    assert environment["ACCEPTED_SKILL_RUNTIME_IMAGE"] == ("deer-flow-provisioner@sha256:" + ("b" * 64))
    assert environment["USERDATA_PVC_NAME"] == "deer-flow-deer-flow-home"
    assert environment["ACCEPTED_ATTEMPT_LEASE_SECONDS"] == "120"
    assert environment["ACCEPTED_ATTEMPT_RECONCILE_INTERVAL_SECONDS"] == "30"
    assert environment["ACCEPTED_ATTEMPT_RECONCILE_LIMIT"] == "100"
    assert environment["PROVISIONER_AUTH_AUDIENCE"] == "hartmesh-provisioner"
    assert environment["PROVISIONER_GATEWAY_NAMESPACE"] == "deer-flow"
    assert environment["PROVISIONER_GATEWAY_SERVICE_ACCOUNT"] == ("deer-flow-deer-flow-gateway")
    documents = [document for document in yaml.safe_load_all(rendered) if isinstance(document, dict)]
    gateway = _workload(rendered, kind="Deployment", component="gateway")
    gateway_spec = gateway["spec"]["template"]["spec"]
    identity_volume = next(volume for volume in gateway_spec["volumes"] if volume["name"] == "provisioner-identity")
    token_projection = identity_volume["projected"]["sources"][0]["serviceAccountToken"]
    assert token_projection == {
        "audience": "hartmesh-provisioner",
        "expirationSeconds": 600,
        "path": "token",
    }
    gateway_mounts = gateway_spec["containers"][0]["volumeMounts"]
    assert {
        "name": "provisioner-identity",
        "mountPath": "/var/run/secrets/hartmesh-provisioner",
        "readOnly": True,
    } in gateway_mounts
    config_map = next(document for document in documents if document.get("kind") == "ConfigMap" and document.get("metadata", {}).get("name") == "deer-flow-deer-flow-config")
    rendered_config = yaml.safe_load(config_map["data"]["config.yaml"])
    assert rendered_config["sandbox"]["accepted_skill_projection_profile"] == ("rwx_verified_copy_v2")
    assert rendered_config["sandbox"]["provisioner_service_account_token_file"] == "/var/run/secrets/hartmesh-provisioner/token"
    assert rendered_config["run_ownership"]["heartbeat_enabled"] is True
    provisioner_role = next(document for document in documents if document.get("kind") == "Role" and document.get("metadata", {}).get("name") == "deer-flow-deer-flow-provisioner")
    pvc_rule = next(rule for rule in provisioner_role["rules"] if "persistentvolumeclaims" in rule["resources"])
    assert pvc_rule["verbs"] == ["get"]
    lease_rule = next(rule for rule in provisioner_role["rules"] if "leases" in rule["resources"])
    assert lease_rule["apiGroups"] == ["coordination.k8s.io"]
    assert lease_rule["verbs"] == [
        "get",
        "list",
        "create",
        "delete",
        "update",
    ]
    cluster_role = next(document for document in documents if document.get("kind") == "ClusterRole" and document.get("metadata", {}).get("name") == "deer-flow-deer-flow-provisioner-ns")
    token_review_rule = next(rule for rule in cluster_role["rules"] if "tokenreviews" in rule["resources"])
    assert token_review_rule == {
        "apiGroups": ["authentication.k8s.io"],
        "resources": ["tokenreviews"],
        "verbs": ["create"],
    }


def test_remote_provisioner_management_auth_is_wired_when_projection_is_disabled(
    tmp_path: Path,
) -> None:
    """Legacy remote AIO must remain authenticated outside the RWX profile."""

    values = copy.deepcopy(_VALUES)
    values["provisioner"]["acceptedSkillProjectionProfile"] = "disabled"

    result = _render(tmp_path, values)
    gateway = _workload(result.stdout, kind="Deployment", component="gateway")
    gateway_spec = gateway["spec"]["template"]["spec"]
    gateway_container = gateway_spec["containers"][0]
    identity_volume = next(volume for volume in gateway_spec["volumes"] if volume["name"] == "provisioner-identity")

    assert identity_volume["projected"]["sources"] == [
        {
            "serviceAccountToken": {
                "audience": "hartmesh-provisioner",
                "expirationSeconds": 600,
                "path": "token",
            }
        }
    ]
    assert {mount["name"]: mount for mount in gateway_container["volumeMounts"]}["provisioner-identity"]["readOnly"] is True

    config_map = next(document for document in _documents(result.stdout) if document.get("kind") == "ConfigMap" and document.get("metadata", {}).get("name") == "deer-flow-deer-flow-config")
    rendered_config = yaml.safe_load(config_map["data"]["config.yaml"])
    assert rendered_config["sandbox"]["provisioner_service_account_token_file"] == ("/var/run/secrets/hartmesh-provisioner/token")


def test_verified_accepted_skill_projection_rejects_unsafe_lease_timing(
    tmp_path: Path,
) -> None:
    values = _production_values()
    values["provisioner"]["acceptedSkillProjectionProfile"] = "rwx_verified_copy_v2"
    values["provisioner"]["sandboxImage"] = "registry.example/aio@sha256:" + ("c" * 64)
    values["provisioner"]["acceptedAttempt"] = {
        "leaseSeconds": 59,
        "reconcileIntervalSeconds": 30,
        "reconcileLimit": 100,
    }
    values["persistence"]["home"]["accessMode"] = "ReadWriteMany"

    result = _render(tmp_path, values, expect_success=False)

    assert "leaseSeconds must be at least twice reconcileIntervalSeconds" in (result.stderr)

    values["provisioner"]["acceptedAttempt"]["leaseSeconds"] = 120
    _set_config_value(values, ("run_ownership", "lease_seconds"), 61)
    result = _render(tmp_path, values, expect_success=False)
    assert "leaseSeconds must be at least twice config run_ownership.lease_seconds" in result.stderr
