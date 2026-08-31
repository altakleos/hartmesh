"""Helm contracts for tenant-scoped Redis key prefixes."""

from __future__ import annotations

import subprocess
from pathlib import Path

from support.helm import deployment_env, helm_executable, render_pvc_chart

_PREFIX_ENVS = {
    "DEER_FLOW_STREAM_BRIDGE_KEY_PREFIX": "hm:v1:tenant-e88e13d0bac8805e:redis",
    "DEER_FLOW_CHECKPOINT_CACHE_KEY_PREFIX": "hm:v1:tenant-e88e13d0bac8805e:redis:ckpt-hist:v1",
    "DEER_FLOW_SANDBOX_OWNERSHIP_KEY_PREFIX": "hm:v1:tenant-e88e13d0bac8805e:redis:deerflow:sandbox:owner",
}

_CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "deer-flow"


def test_local_default_omits_identity_override_but_namespaces_redis() -> None:
    environment = deployment_env("gateway")

    assert "DEER_FLOW_TENANT_ID" not in environment
    assert environment["DEER_FLOW_STREAM_BRIDGE_KEY_PREFIX"] == ("hm:v1:tenant-fd1e0d1ead4a5e20:redis")


def test_tenant_id_derives_identity_and_subsystem_namespaces() -> None:
    environment = deployment_env(
        "gateway",
        "--set-string",
        "tenant.id=acme",
    )

    assert environment["DEER_FLOW_TENANT_ID"] == "acme"
    assert {name: environment[name] for name in _PREFIX_ENVS} == _PREFIX_ENVS


def test_two_tenant_releases_render_disjoint_identity_and_prefixes() -> None:
    tenant_a = deployment_env(
        "gateway",
        "--set-string",
        "tenant.id=tenant-a",
    )
    tenant_b = deployment_env(
        "gateway",
        "--set-string",
        "tenant.id=tenant-b",
    )

    assert tenant_a["DEER_FLOW_TENANT_ID"] == "tenant-a"
    assert tenant_b["DEER_FLOW_TENANT_ID"] == "tenant-b"
    assert {tenant_a[name] for name in _PREFIX_ENVS}.isdisjoint(
        {tenant_b[name] for name in _PREFIX_ENVS},
    )


def test_tenant_environment_is_exposed_only_to_the_gateway_workload() -> None:
    documents = render_pvc_chart(
        "--set-string",
        "tenant.id=acme",
    )
    exposed_components: set[str] = set()
    tenant_environment_names = {"DEER_FLOW_TENANT_ID", *_PREFIX_ENVS}

    for document in documents:
        if document.get("kind") not in {"Deployment", "StatefulSet", "DaemonSet"}:
            continue
        component = document["metadata"]["labels"]["app.kubernetes.io/component"]
        pod_spec = document["spec"]["template"]["spec"]
        containers = [
            *pod_spec.get("initContainers", []),
            *pod_spec.get("containers", []),
        ]
        if any(item.get("name") in tenant_environment_names for container in containers for item in container.get("env", [])):
            exposed_components.add(component)

    assert exposed_components == {"gateway"}


def test_matching_legacy_prefixes_are_accepted_during_compatibility_window() -> None:
    environment = deployment_env(
        "gateway",
        "--set-string",
        "tenant.id=acme",
        "--set-string",
        "redis.tenantPrefix=hm:v1:tenant-e88e13d0bac8805e:redis",
        "--set-string",
        "redis.keyPrefixes.checkpointCache=hm:v1:tenant-e88e13d0bac8805e:redis:ckpt-hist:v1",
    )

    assert {name: environment[name] for name in _PREFIX_ENVS} == _PREFIX_ENVS


def test_exact_operator_recorded_legacy_prefixes_are_rendered() -> None:
    environment = deployment_env(
        "gateway",
        "--set-string",
        "tenant.id=acme",
        "--set-string",
        "tenant.legacyRedisPrefixes.streamBridge=legacy:stream",
        "--set-string",
        "tenant.legacyRedisPrefixes.checkpointCache=legacy:checkpoint",
        "--set-string",
        "tenant.legacyRedisPrefixes.sandboxOwnership=legacy:ownership",
        "--set-string",
        "redis.keyPrefixes.streamBridge=legacy:stream",
        "--set-string",
        "redis.keyPrefixes.checkpointCache=legacy:checkpoint",
        "--set-string",
        "redis.keyPrefixes.sandboxOwnership=legacy:ownership",
    )

    assert environment["DEER_FLOW_STREAM_BRIDGE_KEY_PREFIX"] == "legacy:stream"
    assert environment["DEER_FLOW_CHECKPOINT_CACHE_KEY_PREFIX"] == ("legacy:checkpoint")
    assert environment["DEER_FLOW_SANDBOX_OWNERSHIP_KEY_PREFIX"] == ("legacy:ownership")


def test_legacy_prefix_must_match_operator_recorded_projection() -> None:
    helm = helm_executable()
    assert helm is not None
    result = subprocess.run(
        [
            helm,
            "template",
            "deer-flow",
            str(_CHART),
            "--set-string",
            "sandbox.volumeMode=pvc",
            "--set-string",
            "skills.existingClaim=deer-flow-test-skills",
            "--set-string",
            "tenant.id=acme",
            "--set-string",
            "tenant.legacyRedisPrefixes.streamBridge=recorded:stream",
            "--set-string",
            "redis.keyPrefixes.streamBridge=different:stream",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "redis.keyPrefixes.streamBridge conflicts" in result.stderr


def test_conflicting_legacy_prefix_names_the_field() -> None:
    helm = helm_executable()
    assert helm is not None
    result = subprocess.run(
        [
            helm,
            "template",
            "deer-flow",
            str(_CHART),
            "--set-string",
            "sandbox.volumeMode=pvc",
            "--set-string",
            "skills.existingClaim=deer-flow-test-skills",
            "--set-string",
            "tenant.id=acme",
            "--set-string",
            "redis.keyPrefixes.streamBridge=another-release",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert "redis.keyPrefixes.streamBridge conflicts" in result.stderr
