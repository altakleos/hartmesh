"""Provisioner contracts for split release and sandbox namespaces."""

from __future__ import annotations

from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
import yaml
from kubernetes.client.rest import ApiException

_REPO_ROOT = Path(__file__).resolve().parents[2]


class _NamespaceApi:
    def __init__(self, *, exists: bool) -> None:
        self.exists = exists
        self.created: list[object] = []

    def read_namespace(self, _name: str) -> object:
        if not self.exists:
            raise ApiException(status=404, reason="Not Found")
        return SimpleNamespace()

    def create_namespace(self, namespace: object) -> None:
        self.created.append(namespace)


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (None, False),
        ("false", False),
        ("TRUE", True),
    ],
)
def test_create_namespace_mode_is_resolved_from_environment(
    provisioner_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    raw_value: str | None,
    expected: bool,
) -> None:
    if raw_value is None:
        monkeypatch.delenv("PROVISIONER_CREATE_NAMESPACE", raising=False)
    else:
        monkeypatch.setenv("PROVISIONER_CREATE_NAMESPACE", raw_value)

    assert provisioner_module._provisioner_create_namespace_from_env() is expected


def test_missing_sandbox_namespace_fails_closed_by_default(
    provisioner_module: ModuleType,
) -> None:
    api = _NamespaceApi(exists=False)
    provisioner_module.K8S_NAMESPACE = "acme-sbx"
    provisioner_module.core_v1 = api

    assert provisioner_module.PROVISIONER_CREATE_NAMESPACE is False
    with pytest.raises(
        RuntimeError,
        match=r"sandbox namespace 'acme-sbx' does not exist; it must be pre-created .*K8S_NAMESPACE",
    ):
        provisioner_module._ensure_namespace()

    assert api.created == []


def test_namespace_creation_requires_explicit_opt_in(
    provisioner_module: ModuleType,
) -> None:
    api = _NamespaceApi(exists=False)
    provisioner_module.K8S_NAMESPACE = "deer-flow"
    provisioner_module.PROVISIONER_CREATE_NAMESPACE = True
    provisioner_module.core_v1 = api

    provisioner_module._ensure_namespace()

    assert len(api.created) == 1
    assert api.created[0].metadata.name == "deer-flow"


@pytest.mark.parametrize(
    "compose_path",
    [
        "docker/docker-compose-dev.yaml",
        "docker/docker-compose.yaml",
    ],
)
def test_compose_explicitly_keeps_single_namespace_creation(
    compose_path: str,
) -> None:
    compose = yaml.safe_load((_REPO_ROOT / compose_path).read_text())
    environment = compose["services"]["provisioner"]["environment"]

    assert "PROVISIONER_CREATE_NAMESPACE=true" in environment


@pytest.mark.parametrize(
    ("sandbox_namespace", "gateway_namespace"),
    [
        ("tenant", "tenant"),
        ("tenant-sbx", "tenant"),
    ],
    ids=["same-namespace", "split-namespace"],
)
def test_accepted_gate_peers_select_the_gateway_namespace(
    provisioner_module: ModuleType,
    sandbox_namespace: str,
    gateway_namespace: str,
) -> None:
    provisioner_module.K8S_NAMESPACE = sandbox_namespace
    provisioner_module.PROVISIONER_GATEWAY_NAMESPACE = gateway_namespace

    policy = provisioner_module._build_accepted_network_policy("attempt-1")
    peers = policy.spec.ingress[0]._from

    assert len(peers) == 2
    assert all(peer.namespace_selector.match_labels == {"kubernetes.io/metadata.name": gateway_namespace} for peer in peers)


def test_sandbox_skills_pvc_is_read_only_at_claim_and_mount_levels(
    provisioner_module: ModuleType,
) -> None:
    provisioner_module.SKILLS_PVC_NAME = "tenant-skills"
    provisioner_module.USERDATA_PVC_NAME = "tenant-home"
    provisioner_module.SANDBOX_VOLUME_CONFIG = provisioner_module.resolve_sandbox_volume_mode(
        "pvc",
        userdata_pvc_name="tenant-home",
        skills_pvc_name="tenant-skills",
    )

    pod = provisioner_module._build_pod("sandbox-1", "thread-1")
    skills_volume = next(volume for volume in pod.spec.volumes if volume.name == "skills")
    skills_mount = next(mount for mount in pod.spec.containers[0].volume_mounts if mount.name == "skills")

    assert skills_volume.persistent_volume_claim.claim_name == "tenant-skills"
    assert skills_volume.persistent_volume_claim.read_only is True
    assert skills_mount.read_only is True
