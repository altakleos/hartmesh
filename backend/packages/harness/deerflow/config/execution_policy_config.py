"""Configuration for accepted execution-policy circuit breakers."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator


class ExecutionPolicyConfig(BaseModel):
    """Server ceilings resolved and pinned independently for every new run."""

    profile: str = Field(default="interactive-v1", min_length=1, max_length=64)
    scheduler_profile: str = Field(default="scheduled-v1", min_length=1, max_length=64)
    max_agent_turns: int = Field(default=1000, ge=1, le=1_000_000)
    max_total_tool_attempts: int = Field(default=1000, ge=1, le=1_000_000)
    per_tool_category_attempts: dict[str, int] = Field(default_factory=dict)
    repeated_tool_warn: int = Field(default=3, ge=1, le=256)
    repeated_tool_stop: int = Field(default=5, ge=1, le=256)
    repeated_tool_window: int = Field(default=20, ge=1, le=256)
    max_no_progress_observations: int = Field(default=3, ge=1, le=10_000)
    max_batches: int = Field(default=16, ge=1, le=100_000)
    max_batch_items: int = Field(default=1000, ge=1, le=1_000_000)
    max_batch_concurrency: int = Field(default=32, ge=1, le=100_000)
    max_batch_attempts: int = Field(default=3000, ge=1, le=3_000_000)
    max_batch_runtime_seconds: int = Field(default=86_400, ge=1, le=31_536_000)
    max_delegation_depth: int = Field(default=4, ge=1, le=1000)
    max_retrieval_calls: int = Field(default=100, ge=1, le=256)
    max_retrieval_results: int = Field(default=1000, ge=1, le=10_000_000)
    max_retrieval_sources: int = Field(default=1000, ge=1, le=10_000_000)
    max_retrieval_bytes: int = Field(default=10 * 1024 * 1024, ge=1, le=10 * 1024 * 1024 * 1024)
    max_sandbox_operations: int = Field(default=1000, ge=1, le=1_000_000)
    max_sandbox_runtime_seconds: int = Field(default=86_400, ge=1, le=31_536_000)
    terminal_grace_seconds: int = Field(default=30, ge=1, le=3600)
    scheduler_max_agent_turns: int = Field(default=500, ge=1, le=1_000_000)
    scheduler_max_total_tool_attempts: int = Field(default=500, ge=1, le=1_000_000)

    @model_validator(mode="after")
    def validate_thresholds(self) -> ExecutionPolicyConfig:
        if not self.repeated_tool_warn <= self.repeated_tool_stop <= self.repeated_tool_window:
            raise ValueError("repeated tool thresholds must satisfy warn <= stop <= window")
        if self.scheduler_max_agent_turns > self.max_agent_turns:
            raise ValueError("scheduler_max_agent_turns cannot broaden max_agent_turns")
        if self.scheduler_max_total_tool_attempts > self.max_total_tool_attempts:
            raise ValueError("scheduler_max_total_tool_attempts cannot broaden max_total_tool_attempts")
        if any(not key or len(key) > 128 or value < 1 or value > self.max_total_tool_attempts for key, value in self.per_tool_category_attempts.items()):
            raise ValueError("per-tool category limits must be bounded by max_total_tool_attempts")
        return self


__all__ = ["ExecutionPolicyConfig"]
