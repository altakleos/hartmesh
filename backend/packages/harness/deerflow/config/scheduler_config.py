from pydantic import BaseModel, Field


class SchedulerConfig(BaseModel):
    enabled: bool = Field(default=False)
    multi_instance: bool = Field(default=False)
    poll_interval_seconds: int = Field(default=5, ge=1, le=300)
    lease_seconds: int = Field(default=120, ge=5, le=3600)
    max_concurrent_runs: int = Field(default=3, ge=1, le=32)
    queue_timeout_seconds: int = Field(default=3600, ge=60, le=604800)
    min_once_delay_seconds: int = Field(default=60, ge=1, le=86400)
    max_run_seconds: int | None = Field(
        default=None,
        ge=300,
        le=86400,
        description=(
            "Wall-clock bound on one scheduled occurrence, in seconds (five minutes to a day: a cold sandbox alone "
            "takes 80 to 91 s, and the clock includes it). An occurrence still running this long after it started "
            "ends `failed` with an error that says how long it had, and its run is asked to stop (asked once: the occurrence is ended whether or not the run stops). Unset, "
            "nothing bounds an unattended run's wall time: recursion_limit and the execution policy's "
            "scheduler_max_agent_turns bound its model calls, not its clock. A run holds one of "
            "max_concurrent_runs slots until it ends, so an unattended run that never finishes holds it "
            "until the Gateway restarts. Captured at startup."
        ),
    )
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
