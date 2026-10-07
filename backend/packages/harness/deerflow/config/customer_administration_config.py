"""Deployment-owned customer management permissions and local launch approval."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class CustomerAdministrationConfig(BaseModel):
    """Permission is an independent floor; role checks cannot replace it."""

    model_config = ConfigDict(strict=True, extra="forbid")

    plugin_management: bool = False
    local_skill_management: bool = False
    local_mcp_management: bool = False


class ApprovedLocalMcpDefinition(BaseModel):
    """One complete operator-approved launch, never a caller approval marker."""

    model_config = ConfigDict(strict=True, extra="forbid")

    type: Literal["stdio"] = "stdio"
    command: str = Field(min_length=1)
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    cwd: str | None = None
