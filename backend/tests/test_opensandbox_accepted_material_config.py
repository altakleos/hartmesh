from __future__ import annotations

import pytest
from pydantic import ValidationError

from deerflow.config.sandbox_config import SandboxConfig

_PROVIDER = "deerflow.community.opensandbox:OpenSandboxProvider"


def test_ordinary_opensandbox_keeps_materialization_disabled_by_default() -> None:
    config = SandboxConfig(use=_PROVIDER, image="python:3.11")

    assert config.accepted_materialization_profile == "disabled"


def test_opensandbox_profile_rejects_mutable_image_before_remote_work() -> None:
    with pytest.raises(ValidationError, match="opensandbox_image_unpinned"):
        SandboxConfig(
            use=_PROVIDER,
            image="python:3.11",
            accepted_materialization_profile=("durable_one_replica_opensandbox_immutable_skills_v1"),
        )


def test_digest_pinned_opensandbox_profile_remains_unavailable() -> None:
    with pytest.raises(
        ValidationError,
        match="opensandbox_qualification_unavailable",
    ):
        SandboxConfig(
            use=_PROVIDER,
            image="registry.example/hartmesh-sandbox@sha256:" + ("a" * 64),
            accepted_materialization_profile=("durable_one_replica_opensandbox_immutable_skills_v1"),
            opensandbox_control_plane_contract_version="0.1.15",
            accepted_material_verifier_digest="b" * 64,
            accepted_material_qualification_evidence="evidence.json",
        )


def test_reserved_opensandbox_profile_cannot_be_selected_for_another_provider() -> None:
    with pytest.raises(
        ValidationError,
        match="opensandbox_qualification_unavailable",
    ):
        SandboxConfig(
            use="deerflow.community.aio_sandbox:AioSandboxProvider",
            accepted_materialization_profile=("durable_one_replica_opensandbox_immutable_skills_v1"),
        )
