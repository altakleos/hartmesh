"""Startup-only declaration of the Gateway deployment durability promise."""

from typing import Literal

from pydantic import BaseModel, Field


class DeploymentConfig(BaseModel):
    """Select local convenience or a fail-closed durable production profile."""

    profile: Literal["local_development", "durable_production"] = Field(
        default="local_development",
        description=("Deployment promise. local_development permits process-local state; durable_production requires restart-durable authoritative invocation storage."),
    )


__all__ = ["DeploymentConfig"]
