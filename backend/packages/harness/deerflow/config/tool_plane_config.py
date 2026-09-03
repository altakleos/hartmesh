"""Governed skill and MCP revision policy configuration."""

from __future__ import annotations

import hashlib
import json

from pydantic import BaseModel, ConfigDict, Field


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
        description="Permit literal private, loopback, and metadata MCP endpoint hosts.",
    )
    maximum_mcp_servers: int = Field(default=128, ge=1, le=1024)
    maximum_skills: int = Field(default=512, ge=1, le=4096)
    validation_requires_skill_review: bool = True
    model_config = ConfigDict(extra="forbid")

    @property
    def policy_digest(self) -> str:
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
