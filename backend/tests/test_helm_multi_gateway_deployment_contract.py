"""Render contract for the exact evidence-gated two-Gateway profile."""

from __future__ import annotations

import copy
import json
import subprocess
from pathlib import Path

import pytest
import yaml
from support.helm import helm_executable

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CHART = _REPO_ROOT / "deploy" / "helm" / "deer-flow"
_VALUES = yaml.safe_load((_CHART / "values.yaml").read_text(encoding="utf-8"))
_VALUES["sandbox"]["volumeMode"] = "pvc"
_VALUES["skills"]["existingClaim"] = "deer-flow-test-skills"
_SHA_A = "sha256:" + ("a" * 64)
_SHA_B = "sha256:" + ("b" * 64)
_SHA_C = "sha256:" + ("c" * 64)
_SHA_D = "sha256:" + ("d" * 64)
_SHA_E = "sha256:" + ("e" * 64)
_SHA_F = "sha256:" + ("f" * 64)


def _write_values(tmp_path: Path, values: dict[str, object]) -> Path:
    path = tmp_path / "values.yaml"
    path.write_text(yaml.safe_dump(values, sort_keys=False), encoding="utf-8")
    return path


def _render(
    tmp_path: Path,
    values: dict[str, object],
    *,
    expect_success: bool = True,
) -> subprocess.CompletedProcess[str]:
    helm = helm_executable()
    assert helm is not None
    result = subprocess.run(
        [
            helm,
            "template",
            "deer-flow",
            str(_CHART),
            "--namespace",
            "deer-flow",
            "--values",
            str(_write_values(tmp_path, values)),
        ],
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


def _component(
    rendered: str,
    *,
    kind: str,
    component: str,
) -> dict[str, object]:
    return next(document for document in _documents(rendered) if document.get("kind") == kind and document.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == component)


def _set_config(
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


def _qualified_values() -> dict[str, object]:
    values = copy.deepcopy(_VALUES)
    values["tenant"]["id"] = "acme"
    values["deployment"]["mode"] = "durable_two_gateway_v1"
    values["deployment"]["persistenceTier"] = "shared_durable"
    values["deployment"]["provenance"]["sourceRevision"] = "9" * 40
    values["deployment"]["topology"]["databaseSchemaRef"] = "schema:sha256:" + ("1" * 64)
    values["deployment"]["qualificationEvidence"] = [
        {
            "qualificationId": "two-gateway-20260901",
            "artifactDigest": _SHA_F,
            "completedAt": "2026-09-01T12:00:00Z",
            "scope": "durable_two_gateway_v1_postgres_redis_aio_rwx",
            "status": "passed",
        }
    ]
    values["gateway"]["replicas"] = 2
    values["gateway"]["image"]["digest"] = _SHA_A
    values["frontend"]["image"]["digest"] = _SHA_B
    values["nginx"]["image"]["digest"] = _SHA_C
    values["provisioner"]["image"]["digest"] = _SHA_D
    values["provisioner"]["sandboxImage"] = "registry.example/hartmesh/sandbox@" + _SHA_E
    values["postgresql"]["image"]["digest"] = _SHA_B
    values["redis"]["image"]["digest"] = _SHA_C
    values["provisioner"]["acceptedSkillProjectionProfile"] = "rwx_verified_copy_v2"
    values["postgresql"]["enabled"] = False
    values["postgresql"]["external"]["existingSecret"] = "shared-postgres"
    values["redis"]["enabled"] = False
    values["redis"]["external"]["existingSecret"] = "shared-redis"
    values["persistence"]["home"].update(
        {
            "enabled": True,
            "existingClaim": "shared-home",
            "accessMode": "ReadWriteMany",
        }
    )
    values["skills"]["existingClaim"] = "shared-skills"
    values["skills"]["accessMode"] = "ReadWriteMany"
    values["sandbox"]["volumeMode"] = "pvc"
    values["extensions"] = {
        "artifactManifestDigest": _SHA_D,
        "configurationDigest": _SHA_E,
    }
    values["extensionsConfig"] = json.dumps(
        {
            "mcpServers": {
                "qualification-tasks": {
                    "enabled": True,
                    "type": "http",
                    "url": "http://multi-gateway-mcp:8090/mcp",
                    "credential_binding_id": "qualification-v1",
                    "task_toolsets": [
                        {
                            "name": "qualification",
                            "submit_tool": "submit_task",
                            "status_tool": "task_status",
                            "cancel_tool": "cancel_task",
                        }
                    ],
                }
            },
            "skills": {},
        }
    )

    _set_config(values, ("deployment", "profile"), "durable_two_gateway_v1")
    _set_config(values, ("database", "checkpoint_cache", "type"), "redis")
    _set_config(values, ("run_events", "backend"), "db")
    _set_config(values, ("agent_storage", "backend"), "db")
    _set_config(values, ("scheduler", "enabled"), True)
    _set_config(values, ("scheduler", "multi_instance"), True)
    _set_config(values, ("mcp_tasks", "enabled"), True)
    _set_config(values, ("run_ownership", "heartbeat_enabled"), True)
    _set_config(values, ("sandbox", "image"), values["provisioner"]["sandboxImage"])
    _set_config(values, ("sandbox", "ownership", "type"), "redis")
    _set_config(values, ("channel_connections", "enabled"), False)
    return values


def test_default_and_one_replica_modes_remain_one_gateway(tmp_path: Path) -> None:
    for values in (copy.deepcopy(_VALUES),):
        rendered = _render(tmp_path, values).stdout
        gateway = _component(rendered, kind="Deployment", component="gateway")
        assert gateway["spec"]["replicas"] == 1
        assert not any(document.get("kind") in {"PodDisruptionBudget", "Job"} for document in _documents(rendered))


def test_exact_two_gateway_candidate_renders_topology_and_migration_authority(
    tmp_path: Path,
) -> None:
    values = _qualified_values()
    values["namespace"] = "hartmesh-qualification-two-gateway"
    values["deployment"]["qualificationEvidence"] = []
    values["deployment"]["qualificationCandidate"] = {
        "enabled": True,
        "id": "qualification-09",
    }
    rendered = _render(tmp_path, values).stdout
    gateway = _component(rendered, kind="Deployment", component="gateway")
    pod_spec = gateway["spec"]["template"]["spec"]
    environment = {item["name"]: item for item in pod_spec["containers"][0]["env"]}

    assert gateway["spec"]["replicas"] == 2
    assert gateway["spec"]["strategy"] == {
        "type": "Recreate",
        "rollingUpdate": None,
    }
    assert pod_spec["affinity"]["podAntiAffinity"]["preferredDuringSchedulingIgnoredDuringExecution"]
    assert pod_spec["topologySpreadConstraints"][0]["maxSkew"] == 1
    assert environment["DEER_FLOW_REPLICA_ID"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.name"
    assert environment["DEER_FLOW_TOPOLOGY_DATABASE_SCHEMA_REF"]["value"] == ("schema:sha256:" + ("1" * 64))
    topology_images = json.loads(environment["DEER_FLOW_TOPOLOGY_IMAGE_DIGESTS"]["value"])
    assert set(topology_images) == {
        "gateway",
        "frontend",
        "nginx",
        "postgres",
        "provisioner",
        "redis",
        "sandbox",
    }
    assert environment["DEER_FLOW_TOPOLOGY_CONFIG_DIGEST"]["value"].startswith("sha256:")
    assert environment["DEER_FLOW_EXTENSIONS_CONFIG_PATH"]["value"] == ("/app/backend/extensions_config.json")

    pdb = _component(rendered, kind="PodDisruptionBudget", component="gateway")
    assert pdb["spec"]["minAvailable"] == 1

    migration = _component(rendered, kind="Job", component="gateway-migration")
    annotations = migration["metadata"]["annotations"]
    assert annotations["helm.sh/hook"] == "pre-install,pre-upgrade"
    container = migration["spec"]["template"]["spec"]["containers"][0]
    assert container["image"].endswith("@" + _SHA_A)
    assert container["command"] == ["sh", "-c"]
    assert "uv run --no-sync python -m deerflow.persistence.migration_job" in (container["args"][0])


def test_qualification_candidate_is_isolated_unqualified_and_explicit(
    tmp_path: Path,
) -> None:
    values = _qualified_values()
    values["namespace"] = "hartmesh-qualification-two-gateway"
    values["deployment"]["qualificationEvidence"] = []
    values["deployment"]["qualificationCandidate"] = {
        "enabled": True,
        "id": "qualification-09",
    }

    rendered = _render(tmp_path, values).stdout
    gateway = _component(rendered, kind="Deployment", component="gateway")
    environment = {item["name"]: item.get("value") for item in gateway["spec"]["template"]["spec"]["containers"][0]["env"]}

    assert environment["DEER_FLOW_QUALIFICATION_CANDIDATE"] == "1"
    assert environment["DEER_FLOW_QUALIFICATION_CANDIDATE_ID"] == ("qualification-09")
    assert environment["DEER_FLOW_QUALIFICATION_NAMESPACE"] == ("hartmesh-qualification-two-gateway")
    assert "DEER_FLOW_QUALIFICATION_EVIDENCE" not in environment


def test_qualification_candidate_cannot_escape_disposable_namespace(
    tmp_path: Path,
) -> None:
    values = _qualified_values()
    values["deployment"]["qualificationEvidence"] = []
    values["deployment"]["qualificationCandidate"] = {
        "enabled": True,
        "id": "qualification-09",
    }

    result = _render(tmp_path, values, expect_success=False)

    assert "qualification candidate requires a disposable namespace" in (result.stderr)


def test_qualification_candidate_cannot_publish_a_passing_artifact(
    tmp_path: Path,
) -> None:
    values = _qualified_values()
    values["namespace"] = "hartmesh-qualification-two-gateway"
    values["deployment"]["qualificationCandidate"] = {
        "enabled": True,
        "id": "qualification-09",
    }

    result = _render(tmp_path, values, expect_success=False)

    assert "qualification candidate cannot declare passing evidence" in (result.stderr)


def test_operator_asserted_evidence_cannot_unlock_profile_without_bundled_artifact(
    tmp_path: Path,
) -> None:
    result = _render(tmp_path, _qualified_values(), expect_success=False)

    assert "topology_qualification_missing" in result.stderr


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (
            lambda values: values["gateway"].update({"replicas": 3}),
            "requires gateway.replicas=2",
        ),
        (
            lambda values: values["gateway"]["autoscaling"].update({"enabled": True}),
            "forbids Gateway autoscaling",
        ),
        (
            lambda values: values["deployment"].update({"qualificationEvidence": []}),
            "requires passed qualification evidence",
        ),
        (
            lambda values: values["persistence"]["home"].update({"accessMode": "ReadWriteOnce"}),
            "ReadWriteMany",
        ),
        (
            lambda values: _set_config(
                values,
                ("channel_connections", "enabled"),
                True,
            ),
            "forbids IM/channel connectors",
        ),
        (
            lambda values: _set_config(
                values,
                ("channels", "github", "enabled"),
                True,
            ),
            "forbids IM/channel connectors",
        ),
        (
            lambda values: _set_config(
                values,
                ("plugins",),
                [
                    {
                        "name": "unsafe",
                        "package": "unsafe-extension",
                        "use": "unsafe:install",
                    }
                ],
            ),
            "allows only the qualified governance extension",
        ),
        (
            lambda values: _set_config(
                values,
                ("scheduler", "multi_instance"),
                False,
            ),
            "requires scheduler.enabled and scheduler.multi_instance",
        ),
        (
            lambda values: _set_config(
                values,
                ("subagent_batches", "enabled"),
                True,
            ),
            "subagent_batches_exact_two_unqualified",
        ),
    ],
)
def test_two_gateway_profile_rejects_unsupported_surfaces(
    tmp_path: Path,
    mutator,
    message: str,
) -> None:
    values = _qualified_values()
    mutator(values)

    result = _render(tmp_path, values, expect_success=False)

    assert message in result.stderr


def test_two_gateway_mode_cannot_be_selected_by_config_similarity(
    tmp_path: Path,
) -> None:
    values = _qualified_values()
    values["deployment"]["mode"] = "durable_one_replica"

    result = _render(tmp_path, values, expect_success=False)

    assert "supported chart topology requires gateway.replicas=1" in result.stderr
