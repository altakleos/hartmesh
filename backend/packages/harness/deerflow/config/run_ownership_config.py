"""Run ownership configuration for multi-worker deployments."""

from __future__ import annotations

from pydantic import BaseModel, Field


class RunOwnershipConfig(BaseModel):
    """Per-run ownership and lease configuration.

    When ``heartbeat_enabled`` is True, each worker periodically renews
    the lease on its active runs. This is required for multi-worker
    deployments to detect orphaned runs from crashed workers.

    Clock authority
    ---------------
    Qualified multi-Gateway stores advertise ``database_v1``. Callers pass a
    duration and PostgreSQL mints, renews, and compares the absolute deadline
    from one post-lock database-time sample. Pod UTC clocks are evidence-only;
    each owner uses a conservative monotonic watchdog for local fail-stop.

    Compatibility stores may retain ``process_v1`` and absolute process-clock
    deadlines outside the qualified profile. ``grace_seconds`` is an explicit
    recovery delay after authoritative expiry, never permission for an owner
    to execute longer and never a substitute for shared clock authority.
    """

    lease_seconds: int = Field(
        default=30,
        ge=5,
        description="Seconds before a run lease expires if not renewed. Heartbeat renews every lease_seconds / 3.",
    )
    grace_seconds: int = Field(
        default=10,
        ge=0,
        description=("Recovery delay after authoritative lease expiry before an orphaned run is reclaimed. It is not extra owner execution time; larger values trade slower dead-worker recovery for a wider operational observation window."),
    )
    heartbeat_enabled: bool = Field(
        default=False,
        description="When True, the worker periodically renews leases on its active runs. Enable for multi-worker deployments (GATEWAY_WORKERS > 1).",
    )
