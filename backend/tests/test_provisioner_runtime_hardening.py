"""Pod runtime and restricted-profile contracts for the K8s provisioner."""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from types import ModuleType
from typing import Any

import pytest
import yaml


def _accepted_projection(provisioner_module: ModuleType) -> Any:
    return provisioner_module.AcceptedSkillProjectionV2(
        profile="rwx_verified_copy_v2",
        snapshot_id="a" * 64,
        content_digest="a" * 64,
        run_id="run-1",
        generation=1,
        projections=[
            {
                "name": "example",
                "category": "public",
                "relative_path": "example",
                "manifest_digest": "b" * 64,
                "content_digest": "c" * 64,
                "file_count": 1,
                "total_bytes": 1,
            }
        ],
        file_count=1,
        total_bytes=1,
    )


def _assert_sandbox_mounts_avoid_entrypoint_chown_paths(pod: Any) -> None:
    forbidden_roots = {
        PurePosixPath("/home/gem"),
        PurePosixPath("/home/gem/Downloads"),
        PurePosixPath("/var/log/gem"),
        PurePosixPath("/var/lib/aio-sandbox"),
        PurePosixPath("/opt/gem"),
        PurePosixPath("/opt/jupyter"),
    }
    sandbox = next(container for container in pod.spec.containers if container.name == "sandbox")

    for mount in sandbox.volume_mounts:
        mount_path = PurePosixPath(mount.mount_path)
        for forbidden_root in forbidden_roots:
            assert mount_path != forbidden_root, mount.mount_path
            assert forbidden_root not in mount_path.parents, mount.mount_path


@pytest.mark.parametrize(
    ("runtime_class_env", "expected"),
    [(None, None), ("", None), ("gvisor", "gvisor")],
    ids=["env-unset", "empty", "configured-runtime-class"],
)
def test_sandbox_pod_runtime_class_follows_configuration(
    provisioner_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    runtime_class_env: str | None,
    expected: str | None,
) -> None:
    if runtime_class_env is None:
        monkeypatch.delenv("SANDBOX_RUNTIME_CLASS", raising=False)
    else:
        monkeypatch.setenv("SANDBOX_RUNTIME_CLASS", runtime_class_env)
    monkeypatch.setattr(
        provisioner_module,
        "SANDBOX_RUNTIME_CLASS",
        provisioner_module._sandbox_runtime_class_from_env(),
    )

    pod = provisioner_module._build_pod("runtime-class", "thread-1")

    assert pod.spec.runtime_class_name == expected
    manifest = provisioner_module.k8s_client.ApiClient().sanitize_for_serialization(
        pod,
    )
    if expected is None:
        assert "runtimeClassName" not in manifest["spec"]
    else:
        assert manifest["spec"]["runtimeClassName"] == expected


@pytest.mark.parametrize(
    ("runtime_class", "expected_label"),
    [("", "default runtime"), ("gvisor", "gvisor")],
    ids=["cluster-default", "gvisor"],
)
def test_sandbox_create_log_records_effective_runtime_class(
    provisioner_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    runtime_class: str,
    expected_label: str,
) -> None:
    monkeypatch.setattr(provisioner_module, "SANDBOX_RUNTIME_CLASS", runtime_class)
    monkeypatch.setattr(
        provisioner_module,
        "_sandbox_access_url",
        lambda *_args, **_kwargs: "http://sandbox.example",
    )
    monkeypatch.setattr(provisioner_module, "_get_pod_phase", lambda _sandbox_id: "Running")
    caplog.set_level(logging.INFO, logger=provisioner_module.__name__)

    provisioner_module.create_sandbox(
        provisioner_module.CreateSandboxRequest(
            sandbox_id="audit-log",
            thread_id="thread-1",
        )
    )

    assert f"runtime_class={expected_label}" in caplog.text


def test_default_sandbox_container_uses_restricted_security_context(
    provisioner_module: ModuleType,
) -> None:
    pod = provisioner_module._build_pod("restricted", "thread-1")

    pod_security = pod.spec.security_context
    assert pod_security.run_as_non_root is True
    assert pod_security.run_as_user == 1000
    assert pod_security.run_as_group == 1000
    assert pod_security.fs_group == 1000

    security = pod.spec.containers[0].security_context
    assert security.allow_privilege_escalation is False
    assert security.capabilities.drop == ["ALL"]
    assert security.capabilities.add is None
    assert security.seccomp_profile.type == "RuntimeDefault"
    assert security.run_as_non_root is None
    assert security.run_as_user is None
    _assert_sandbox_mounts_avoid_entrypoint_chown_paths(pod)


def test_every_container_in_an_initialized_sandbox_is_hardened(
    provisioner_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provisioner_module, "USERDATA_PVC_NAME", "home-rwx")
    monkeypatch.setattr(
        provisioner_module,
        "ACCEPTED_SKILL_PROJECTION_PROFILE",
        "rwx_verified_copy_v2",
    )
    monkeypatch.setattr(
        provisioner_module,
        "ACCEPTED_SKILL_RUNTIME_IMAGE",
        "registry.example/provisioner@sha256:" + ("d" * 64),
    )
    monkeypatch.setattr(
        provisioner_module,
        "SANDBOX_IMAGE",
        "registry.example/sandbox@sha256:" + ("e" * 64),
    )
    monkeypatch.setattr(
        provisioner_module,
        "LARK_CLI_BROKER_IMAGE",
        "registry.example/lark-broker:v1",
    )

    pod = provisioner_module._build_pod(
        "initialized",
        "thread-1",
        accepted_skill_projection=_accepted_projection(provisioner_module),
        attempt_capability="A" * 43,
        provision_lark_cli_broker=True,
    )

    containers = [*pod.spec.containers, *pod.spec.init_containers]
    assert {container.name for container in containers} == {
        "sandbox",
        "accepted-skill-gate",
        "lark-cli-broker",
        "accepted-skill-verifier",
        "lark-cli-shim-init",
    }
    for container in containers:
        security = container.security_context
        assert security.allow_privilege_escalation is False, container.name
        assert security.capabilities.drop == ["ALL"], container.name
        assert security.capabilities.add is None, container.name
        assert security.seccomp_profile.type == "RuntimeDefault", container.name
        assert security.run_as_non_root is None, container.name
        assert security.run_as_user is None, container.name
    _assert_sandbox_mounts_avoid_entrypoint_chown_paths(pod)


def test_rendered_pvc_backed_sandbox_satisfies_restricted(
    provisioner_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provisioner_module, "SKILLS_PVC_NAME", "skills-rwx")
    monkeypatch.setattr(provisioner_module, "USERDATA_PVC_NAME", "home-rwx")
    monkeypatch.setattr(
        provisioner_module,
        "SANDBOX_VOLUME_CONFIG",
        provisioner_module.resolve_sandbox_volume_mode(
            "pvc",
            userdata_pvc_name="home-rwx",
            skills_pvc_name="skills-rwx",
        ),
    )

    pod = provisioner_module._build_pod(
        "restricted-sample",
        "thread-1",
    )
    manifest = provisioner_module.k8s_client.ApiClient().sanitize_for_serialization(
        pod,
    )
    rendered_yaml = yaml.safe_dump(manifest, sort_keys=False)
    rendered = yaml.safe_load(rendered_yaml)
    spec = rendered["spec"]

    assert spec["hostNetwork"] is False
    assert spec["hostPID"] is False
    assert spec["hostIPC"] is False
    assert spec["shareProcessNamespace"] is False
    assert spec["automountServiceAccountToken"] is False
    assert spec["securityContext"] == {
        "fsGroup": 1000,
        "runAsGroup": 1000,
        "runAsNonRoot": True,
        "runAsUser": 1000,
    }
    assert "sysctls" not in spec

    allowed_volume_types = {
        "configMap",
        "csi",
        "downwardAPI",
        "emptyDir",
        "ephemeral",
        "persistentVolumeClaim",
        "projected",
        "secret",
    }
    for volume in spec["volumes"]:
        volume_types = set(volume) - {"name"}
        assert len(volume_types) == 1, volume["name"]
        assert volume_types <= allowed_volume_types, volume["name"]
        assert "hostPath" not in volume

    for container in [*spec["containers"], *spec.get("initContainers", [])]:
        security = container["securityContext"]
        assert security["privileged"] is False, container["name"]
        assert security["allowPrivilegeEscalation"] is False, container["name"]
        assert security["capabilities"] == {"drop": ["ALL"]}, container["name"]
        assert security["seccompProfile"] == {"type": "RuntimeDefault"}, container["name"]
        assert "runAsNonRoot" not in security, container["name"]
        assert "runAsUser" not in security, container["name"]
        assert "procMount" not in security, container["name"]
        assert "seLinuxOptions" not in security, container["name"]
        assert "appArmorProfile" not in security, container["name"]
        assert "windowsOptions" not in security, container["name"]
        for port in container.get("ports", []):
            assert "hostPort" not in port, container["name"]
