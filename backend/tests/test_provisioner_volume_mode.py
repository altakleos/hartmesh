"""Fail-closed volume-mode contracts for the Kubernetes provisioner."""

from __future__ import annotations

from types import ModuleType

import pytest


@pytest.mark.parametrize(
    (
        "userdata_pvc_name",
        "skills_pvc_name",
        "explicit_mode",
        "expected_mode",
        "expected_reason",
        "missing_names",
    ),
    [
        ("", "", None, "hostpath", "inferred", ()),
        ("home", "skills", None, "pvc", "inferred", ()),
        ("home", "", None, None, None, ("SKILLS_PVC_NAME",)),
        ("", "skills", None, None, None, ("USERDATA_PVC_NAME",)),
        ("", "", "pvc", None, None, ("USERDATA_PVC_NAME", "SKILLS_PVC_NAME")),
        ("home", "skills", "pvc", "pvc", "explicit", ()),
        ("home", "", "pvc", None, None, ("SKILLS_PVC_NAME",)),
        ("", "skills", "pvc", None, None, ("USERDATA_PVC_NAME",)),
        ("", "", "hostpath", "hostpath", "explicit", ()),
        ("home", "skills", "hostpath", "hostpath", "explicit", ()),
        ("home", "", "hostpath", "hostpath", "explicit", ()),
        ("", "skills", "hostpath", "hostpath", "explicit", ()),
    ],
    ids=[
        "infer-hostpath-neither-claim",
        "infer-pvc-both-claims",
        "infer-reject-missing-skills",
        "infer-reject-missing-userdata",
        "explicit-pvc-reject-both-missing",
        "explicit-pvc-both-claims",
        "explicit-pvc-reject-missing-skills",
        "explicit-pvc-reject-missing-userdata",
        "explicit-hostpath-neither-claim",
        "explicit-hostpath-both-claims",
        "explicit-hostpath-userdata-only",
        "explicit-hostpath-skills-only",
    ],
)
def test_volume_mode_resolution_matrix(
    provisioner_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    userdata_pvc_name: str,
    skills_pvc_name: str,
    explicit_mode: str | None,
    expected_mode: str | None,
    expected_reason: str | None,
    missing_names: tuple[str, ...],
) -> None:
    for name, value in (
        ("USERDATA_PVC_NAME", userdata_pvc_name),
        ("SKILLS_PVC_NAME", skills_pvc_name),
        ("SANDBOX_VOLUME_MODE", explicit_mode),
    ):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)

    if missing_names:
        with pytest.raises(RuntimeError) as exc_info:
            provisioner_module._sandbox_volume_config_from_env()

        message = str(exc_info.value)
        assert "SANDBOX_VOLUME_MODE=pvc" in message
        assert all(name in message for name in missing_names)
        return

    resolved = provisioner_module._sandbox_volume_config_from_env()

    assert resolved.mode == expected_mode
    assert resolved.reason == expected_reason


def test_pvc_mode_never_constructs_a_hostpath_volume(
    provisioner_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provisioner_module, "USERDATA_PVC_NAME", "home")
    monkeypatch.setattr(provisioner_module, "SKILLS_PVC_NAME", "skills")
    monkeypatch.setattr(
        provisioner_module,
        "SANDBOX_VOLUME_CONFIG",
        provisioner_module.resolve_sandbox_volume_mode(
            "pvc",
            userdata_pvc_name="home",
            skills_pvc_name="skills",
        ),
    )

    def reject_hostpath(*_args: object, **_kwargs: object) -> None:
        pytest.fail("pvc mode invoked the hostPath volume builder")

    monkeypatch.setattr(
        provisioner_module.k8s_client,
        "V1HostPathVolumeSource",
        reject_hostpath,
    )

    volumes = provisioner_module._build_volumes("thread-1")

    assert all(volume.host_path is None for volume in volumes)


def test_explicit_hostpath_mode_ignores_configured_claim_names(
    provisioner_module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(provisioner_module, "USERDATA_PVC_NAME", "home")
    monkeypatch.setattr(provisioner_module, "SKILLS_PVC_NAME", "skills")
    monkeypatch.setattr(
        provisioner_module,
        "SANDBOX_VOLUME_CONFIG",
        provisioner_module.resolve_sandbox_volume_mode(
            "hostpath",
            userdata_pvc_name="home",
            skills_pvc_name="skills",
        ),
    )

    volumes = provisioner_module._build_volumes("thread-1")
    mounts = provisioner_module._build_volume_mounts("thread-1")

    assert all(volume.persistent_volume_claim is None for volume in volumes)
    assert all(mount.sub_path is None for mount in mounts)
