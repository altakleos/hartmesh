"""Governed tool-plane policy configuration contracts."""

import pytest
from pydantic import ValidationError

from deerflow.config.tool_plane_config import ToolPlaneConfig


def test_policy_digest_binds_provider_and_capability_controls() -> None:
    baseline = ToolPlaneConfig()
    provider_restricted = baseline.model_copy(update={"allowed_managed_integration_providers": ("lark-cli",)})
    capability_restricted = baseline.model_copy(update={"forbidden_skill_capabilities": ("tool:bash",)})

    assert provider_restricted.policy_digest != baseline.policy_digest
    assert capability_restricted.policy_digest != baseline.policy_digest
    assert capability_restricted.policy_digest != provider_restricted.policy_digest


@pytest.mark.parametrize(
    "capability",
    ["", "unrestricted_tool", "tool:", "tool:bad name", "shell:bash"],
)
def test_forbidden_skill_capability_rejects_unknown_or_malformed_values(
    capability: str,
) -> None:
    with pytest.raises(ValidationError):
        ToolPlaneConfig(forbidden_skill_capabilities=(capability,))


@pytest.mark.parametrize(
    "capability",
    [
        "unrestricted-tools",
        "autonomous-secrets",
        "declared-secrets",
        "tool:web_search",
    ],
)
def test_forbidden_skill_capability_accepts_supported_values(
    capability: str,
) -> None:
    config = ToolPlaneConfig(forbidden_skill_capabilities=(capability,))

    assert config.forbidden_skill_capabilities == (capability,)
