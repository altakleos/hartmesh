from pydantic import BaseModel, Field, ValidationInfo, field_validator

from deerflow.config._boolean_guards import reject_boolean


class SchedulerConfig(BaseModel):
    enabled: bool = Field(default=False)
    tool_enabled: bool = Field(default=False, description="Offer conversation schedule tools only with a host-bound scheduler capability and scheduler.enabled=true.")
    multi_instance: bool = Field(default=False)
    poll_interval_seconds: int = Field(default=5, ge=1, le=300)
    lease_seconds: int = Field(default=120, ge=5, le=3600)
    max_concurrent_runs: int = Field(default=3, ge=1, le=32)
    max_concurrent_runs_per_user: int = Field(
        default=2,
        ge=0,
        le=32,
        description="Cap on launching/running scheduled runs per task owner; 0 = no per-owner cap. The effective cap is min(this, max_concurrent_runs). Queue selection is fair to owners either way.",
    )
    queue_timeout_seconds: int = Field(default=3600, ge=60, le=604800)
    max_run_seconds: int | None = Field(
        default=None,
        ge=300,
        le=86400,
        description=(
            "Wall-clock bound on one scheduled occurrence, in seconds (five minutes to a day; the clock includes a cold "
            "sandbox start). An occurrence still running this long after it started ends `failed` with an error that "
            "says how long it had, and its run is asked to stop (asked once: the occurrence is ended whether or not "
            "the run stops). Unset, nothing bounds an unattended run's wall time: recursion_limit bounds its model "
            "calls, not its clock. A run holds one of max_concurrent_runs slots until it ends. Captured at startup."
        ),
    )
    min_once_delay_seconds: int = Field(default=60, ge=1, le=86400)
    recursion_limit: int = Field(
        default=1000,
        ge=1,
        description=(
            "LangGraph recursion_limit for scheduler-launched runs. Read at dispatch "
            "time (not captured into ScheduledTaskService). The default matches the "
            "web UI's interactive budget (1000) so scheduled and interactive runs "
            "behave identically out of the box. Values above "
            "AppConfig.max_recursion_limit are clamped."
        ),
    )

    @field_validator(
        "poll_interval_seconds",
        "lease_seconds",
        "max_concurrent_runs",
        "max_concurrent_runs_per_user",
        "queue_timeout_seconds",
        "min_once_delay_seconds",
        "recursion_limit",
        mode="before",
    )
    @classmethod
    def _reject_boolean_scheduler_integers(cls, value: object, info: ValidationInfo) -> object:
        return reject_boolean(value, info, kind="an integer")
