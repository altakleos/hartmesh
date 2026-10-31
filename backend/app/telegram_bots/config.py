"""Validate the optional managed Telegram bot section without runtime effects."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, PositiveInt

if TYPE_CHECKING:
    from deerflow.config.app_config import AppConfig


_ClockTime = Annotated[str, Field(min_length=5, max_length=5, pattern=r"^(?:[01][0-9]|2[0-3]):[0-5][0-9]$")]


class _StrictConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class _QueueConfig(_StrictConfig):
    depth: PositiveInt = 10
    max_wait_seconds: PositiveInt = 120
    retry_seconds: PositiveInt = 10


class _FilesConfig(_StrictConfig):
    max_inbound_bytes: PositiveInt = 20_000_000
    max_outbound_bytes: int = Field(default=50_000_000, ge=1, le=50_000_000)
    in_memory_max_bytes: int = Field(default=200_000_000, ge=50_000_000)
    retry_seconds: PositiveInt = 3
    retry_max_seconds: PositiveInt = 120
    pending_ttl_hours: PositiveInt = 24


class _CallbacksConfig(_StrictConfig):
    draft_ttl_hours: PositiveInt = 24
    sweep_after_days: PositiveInt = 7
    max_age_days: PositiveInt = 180


class _PromptsConfig(_StrictConfig):
    ttl_seconds: PositiveInt = 600


class _IntentsConfig(_StrictConfig):
    ttl_seconds: PositiveInt = 600


class _ProducerConfig(_StrictConfig):
    poll_seconds: PositiveInt = 15
    batch: PositiveInt = 200


class _OutboxConfig(_StrictConfig):
    per_link_per_minute: PositiveInt = 1
    per_link_per_hour: PositiveInt = 10
    per_person_per_day: PositiveInt = 60
    digest_threshold: PositiveInt = 3
    progress_edit_seconds: PositiveInt = 30
    max_attempts: PositiveInt = 5
    recent_act_seconds: PositiveInt = 1800
    decision_hold_seconds: PositiveInt = 120
    place_free_hold_seconds: PositiveInt = 600


class _QuietConfig(_StrictConfig):
    default_enabled: bool = True
    default_start: _ClockTime = "21:00"
    default_end: _ClockTime = "07:00"


class _SummaryConfig(_StrictConfig):
    default_enabled: bool = True
    default_time: _ClockTime = "08:00"


class _AssignConfig(_StrictConfig):
    model_name: str | None = None
    timeout_seconds: PositiveInt = 30
    max_chars: PositiveInt = 4000


class _VoiceConfig(_StrictConfig):
    provider: Literal["auto", "openai", "gemini", "off"] = "auto"
    openai_model: str = "gpt-4o-mini-transcribe"
    gemini_model: str | None = None
    max_seconds: PositiveInt = 300
    max_seconds_per_person_per_day: PositiveInt = 1800
    timeout_seconds: PositiveInt = 60


class _ActsConfig(_StrictConfig):
    retry_seconds: PositiveInt = 5
    max_attempts: PositiveInt = 3


class _ProfileConfig(_StrictConfig):
    retry_hours: PositiveInt = 24


class TelegramBotsConfig(_StrictConfig):
    """Operator-owned configuration; absence is handled by :func:`load_config`."""

    enabled: bool = True
    public_base_url: str | None = None
    max_bots: int = Field(default=100, ge=1, le=500)
    max_bots_per_person: int = Field(default=10, ge=1, le=40)
    reconcile_interval_seconds: PositiveInt = 30
    conflict_probe_seconds: NonNegativeInt = 30
    streaming: Literal["draft", "edit"] = "draft"
    queue: _QueueConfig = Field(default_factory=_QueueConfig)
    files: _FilesConfig = Field(default_factory=_FilesConfig)
    callbacks: _CallbacksConfig = Field(default_factory=_CallbacksConfig)
    prompts: _PromptsConfig = Field(default_factory=_PromptsConfig)
    intents: _IntentsConfig = Field(default_factory=_IntentsConfig)
    producer: _ProducerConfig = Field(default_factory=_ProducerConfig)
    outbox: _OutboxConfig = Field(default_factory=_OutboxConfig)
    quiet: _QuietConfig = Field(default_factory=_QuietConfig)
    summary: _SummaryConfig = Field(default_factory=_SummaryConfig)
    assign: _AssignConfig = Field(default_factory=_AssignConfig)
    voice: _VoiceConfig = Field(default_factory=_VoiceConfig)
    acts: _ActsConfig = Field(default_factory=_ActsConfig)
    profile: _ProfileConfig = Field(default_factory=_ProfileConfig)


def section_present(app_config: AppConfig) -> bool:
    """Distinguish a missing section from an explicitly disabled or invalid one."""
    return "telegram_bots" in (getattr(app_config, "model_extra", None) or {})


def load_config(app_config: AppConfig) -> TelegramBotsConfig:
    """Validate a present section, or return disabled defaults when it is absent."""
    extra = app_config.model_extra or {}
    if "telegram_bots" not in extra:
        return TelegramBotsConfig(enabled=False)
    return TelegramBotsConfig.model_validate(extra["telegram_bots"])
