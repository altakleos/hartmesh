"""Startup-only declaration of the Gateway deployment durability promise."""

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class ReadinessConfig(BaseModel):
    """Startup-only bounded timing contract for readiness and admission."""

    capability_cache_seconds: float = Field(default=10.0, gt=0, le=300)
    admission_health_max_age_seconds: float = Field(default=10.0, gt=0, le=300)
    required_health_stale_seconds: float = Field(default=30.0, gt=0, le=900)
    capability_probe_timeout_seconds: float = Field(default=2.0, gt=0, le=30)
    overall_timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    # Required authority is deliberately fail-closed on its first failed
    # observation. Keep this explicit beside Kubernetes' independent probe
    # failure threshold without allowing policy weakening in v1.
    required_failure_threshold: Literal[1] = 1

    @model_validator(mode="after")
    def validate_windows(self) -> "ReadinessConfig":
        if self.capability_cache_seconds > self.admission_health_max_age_seconds:
            raise ValueError("capability_cache_seconds must not exceed admission_health_max_age_seconds")
        if self.admission_health_max_age_seconds > self.required_health_stale_seconds:
            raise ValueError("admission_health_max_age_seconds must not exceed required_health_stale_seconds")
        if self.overall_timeout_seconds <= self.capability_probe_timeout_seconds:
            raise ValueError("overall_timeout_seconds must exceed capability_probe_timeout_seconds")
        return self


class GracefulShutdownConfig(BaseModel):
    """Startup-only phase budgets for the single Gateway shutdown sequence."""

    admission_seconds: float = Field(default=2.0, gt=0, le=60)
    channel_seconds: float = Field(default=5.0, gt=0, le=60)
    scheduler_seconds: float = Field(default=3.0, gt=0, le=60)
    run_seconds: float = Field(default=8.0, gt=0, le=120)
    dependencies_seconds: float = Field(default=5.0, gt=0, le=60)


class DeploymentConfig(BaseModel):
    """Select local convenience or a fail-closed durable production profile."""

    profile: Literal["local_development", "durable_production"] = Field(
        default="local_development",
        description=("Deployment promise. local_development permits process-local state; durable_production requires restart-durable authoritative invocation storage."),
    )
    tenant_id: str | None = Field(
        default=None,
        description=("Startup-only operator-selected tenant identity. DEER_FLOW_TENANT_ID takes precedence; local development defaults to 'local' only when both sources are absent."),
    )
    readiness: ReadinessConfig = Field(
        default_factory=ReadinessConfig,
        description=("Startup-only health cache, admission freshness, and bounded probe timing settings."),
    )
    shutdown: GracefulShutdownConfig = Field(
        default_factory=GracefulShutdownConfig,
        description=("Startup-only bounded phase budgets for graceful Gateway shutdown."),
    )


__all__ = ["DeploymentConfig", "GracefulShutdownConfig", "ReadinessConfig"]
