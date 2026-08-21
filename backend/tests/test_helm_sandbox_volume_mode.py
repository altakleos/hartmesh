"""Helm plumbing contract for the provisioner sandbox volume mode."""

from __future__ import annotations

import pytest
from support.helm import deployment_env, render_chart


def test_bare_defaults_refuse_half_configured_sandbox_claims() -> None:
    with pytest.raises(AssertionError) as exc_info:
        render_chart()

    message = str(exc_info.value)
    assert "persistence.home.enabled" in message
    assert "skills.existingClaim" in message


def test_default_claim_shape_renders_with_skills_claim() -> None:
    documents = render_chart("--set-string", "skills.existingClaim=deer-flow-skills")

    assert documents


def test_explicit_hostpath_renders_with_default_claim_shape() -> None:
    documents = render_chart("--set-string", "sandbox.volumeMode=hostpath")

    assert documents


def test_no_claims_render_for_inferred_hostpath_mode() -> None:
    env = deployment_env("provisioner", "--set", "persistence.home.enabled=false")

    assert "SANDBOX_VOLUME_MODE" not in env


def test_provisioner_disabled_renders_with_default_claim_shape() -> None:
    documents = render_chart("--set", "provisioner.enabled=false")

    assert documents


def test_explicit_pvc_volume_mode_renders_to_provisioner_env() -> None:
    env = deployment_env(
        "provisioner",
        "--set",
        "sandbox.volumeMode=pvc",
        "--set-string",
        "skills.existingClaim=deer-flow-skills",
    )

    assert env["SANDBOX_VOLUME_MODE"] == "pvc"
