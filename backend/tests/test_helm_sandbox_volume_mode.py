"""Helm plumbing contract for the provisioner sandbox volume mode."""

from __future__ import annotations

from support.helm import deployment_env


def test_default_volume_mode_is_omitted_for_provisioner_inference() -> None:
    env = deployment_env("provisioner")

    assert "SANDBOX_VOLUME_MODE" not in env


def test_explicit_pvc_volume_mode_renders_to_provisioner_env() -> None:
    env = deployment_env(
        "provisioner",
        "--set",
        "sandbox.volumeMode=pvc",
    )

    assert env["SANDBOX_VOLUME_MODE"] == "pvc"
