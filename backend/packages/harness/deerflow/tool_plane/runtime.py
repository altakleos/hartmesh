"""Runtime adapter for one accepted secret-safe tool-plane composition.

The governance contracts retain selector identities only.  This adapter is the
single boundary that turns those selectors into a process-local
``ExtensionsConfig`` immediately before accepted agent material is captured.
Resolved values are never returned in evidence or written back to projection
files.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from deerflow.config.app_config import AppConfig
from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.tool_plane.contracts import (
    EffectiveToolPlaneRevisionV1,
    runtime_mcp_servers_from_canonical,
)


@dataclass(frozen=True, slots=True)
class ResolvedToolPlaneRuntimeV1:
    """Process-local config plus the accepted per-server tool ceiling."""

    app_config: AppConfig
    allowed_mcp_tools_by_server: Mapping[str, frozenset[str] | None]


def _effective_from_mapping(
    value: Mapping[str, Any],
) -> EffectiveToolPlaneRevisionV1:
    effective = EffectiveToolPlaneRevisionV1(
        base_revision_digest=value["base_revision_digest"],
        user_overlay_digest=value["user_overlay_digest"],
        base_generation=value["base_generation"],
        overlay_generation=value["overlay_generation"],
        projection_digest=value["projection_digest"],
        effective_mcp_server_ids=tuple(value["effective_mcp_server_ids"]),
        effective_mcp_servers=tuple(value["effective_mcp_servers"]),
        effective_global_skill_states=tuple(value["effective_global_skill_states"]),
        effective_managed_integration_ids=tuple(value["effective_managed_integration_ids"]),
        governance_state=value["governance_state"],
    )
    if value.get("version") != 1 or value.get("effective_digest") != effective.effective_digest:
        raise ValueError("accepted tool-plane revision is malformed")
    return effective


def resolve_tool_plane_runtime(
    app_config: AppConfig,
    effective: EffectiveToolPlaneRevisionV1 | Mapping[str, Any],
) -> ResolvedToolPlaneRuntimeV1:
    """Resolve selectors only into a private config pinned to ``effective``."""

    if not isinstance(app_config, AppConfig):
        raise TypeError("app_config must be AppConfig")
    if isinstance(effective, Mapping):
        effective = _effective_from_mapping(effective)
    if not isinstance(effective, EffectiveToolPlaneRevisionV1):
        raise TypeError("effective must be EffectiveToolPlaneRevisionV1")

    unresolved = {
        "mcpServers": runtime_mcp_servers_from_canonical(effective.effective_mcp_servers),
        "skills": {str(state["name"]): {"enabled": state["enabled"]} for state in effective.effective_global_skill_states},
    }
    # Environment and context-secret resolution remains owned by the existing
    # ExtensionsConfig credential boundary. Only this process-local model sees
    # environment values; the effective contract retains selector names.
    resolved_extensions = ExtensionsConfig.model_validate(ExtensionsConfig.resolve_env_variables(unresolved))
    pinned_app_config = app_config.model_copy(
        update={"extensions": resolved_extensions},
        deep=True,
    )
    allowlists: dict[str, frozenset[str] | None] = {}
    for server in effective.effective_mcp_servers:
        names = server.get("tool_allowlist")
        if not isinstance(names, (list, tuple)):
            raise ValueError("accepted MCP tool allowlist is malformed")
        # An empty override map has historically meant "all advertised
        # tools". A nonempty map is the governed explicit ceiling.
        allowlists[str(server["server_id"])] = None if not names else frozenset(str(name) for name in names)
    return ResolvedToolPlaneRuntimeV1(
        app_config=pinned_app_config,
        allowed_mcp_tools_by_server=MappingProxyType(allowlists),
    )


__all__ = ["ResolvedToolPlaneRuntimeV1", "resolve_tool_plane_runtime"]
