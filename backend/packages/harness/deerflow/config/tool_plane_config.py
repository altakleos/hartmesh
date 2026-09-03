"""Governed skill and MCP revision policy configuration."""

from __future__ import annotations

import hashlib
import json
import re

from pydantic import BaseModel, ConfigDict, Field, field_validator

_FIXED_SKILL_CAPABILITIES = frozenset({"unrestricted-tools", "autonomous-secrets", "declared-secrets"})
_TOOL_CAPABILITY = re.compile(r"tool:[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")


class ToolPlaneConfig(BaseModel):
    """Startup-frozen validation and revision workflow policy."""

    enabled: bool = Field(
        default=True,
        description="Enable stage, validate, promote, rollback, and admission binding for skill/MCP material.",
    )
    policy_version: str = Field(default="deerflow-default-v1", min_length=1, max_length=128)
    allowed_mcp_transports: tuple[str, ...] = Field(
        default=("stdio", "sse", "http", "streamable_http"),
        max_length=16,
    )
    allowed_mcp_stdio_commands: tuple[str, ...] = Field(
        default=("npx", "uvx"),
        max_length=128,
        description="Executable basenames permitted for governed stdio MCP servers.",
    )
    allowed_mcp_endpoint_hosts: tuple[str, ...] = Field(
        default=(),
        max_length=1024,
        description="Optional exact hostname allowlist for governed HTTP/SSE MCP endpoints.",
    )
    allow_private_mcp_endpoints: bool = Field(
        default=False,
        description="Permit private, loopback, or metadata MCP endpoint addresses after resolution.",
    )
    allowed_managed_integration_providers: tuple[str, ...] = Field(
        default=(),
        max_length=256,
        description="Optional exact provider allowlist for governed managed-integration packages.",
    )
    forbidden_skill_capabilities: tuple[str, ...] = Field(
        default=(),
        max_length=1024,
        description=("Exact derived skill capabilities rejected by policy: unrestricted-tools, autonomous-secrets, declared-secrets, or tool:<name>."),
    )
    maximum_mcp_servers: int = Field(default=128, ge=1, le=1024)
    maximum_skills: int = Field(default=512, ge=1, le=4096)
    validation_requires_skill_review: bool = True
    model_config = ConfigDict(extra="forbid")

    @field_validator("forbidden_skill_capabilities")
    @classmethod
    def _validate_forbidden_skill_capabilities(
        cls,
        values: tuple[str, ...],
    ) -> tuple[str, ...]:
        for capability in values:
            if capability not in _FIXED_SKILL_CAPABILITIES and _TOOL_CAPABILITY.fullmatch(capability) is None:
                raise ValueError("forbidden skill capabilities must be unrestricted-tools, autonomous-secrets, declared-secrets, or tool:<name>")
        return values

    @property
    def policy_digest(self) -> str:
        """Return the canonical SHA-256 identity of this complete policy."""

        encoded = json.dumps(
            {"version": 1, **self.model_dump(mode="json")},
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def governed_tool_plane_enabled(config: object) -> bool:
    """Return true only for the concrete startup-frozen governance config."""

    candidate = getattr(config, "tool_plane", None)
    return isinstance(candidate, ToolPlaneConfig) and candidate.enabled is True


__all__ = ["ToolPlaneConfig", "governed_tool_plane_enabled"]
