"""Strict, side-effect-free configuration for managed Telegram bots."""

from __future__ import annotations

from copy import deepcopy

import pytest
from pydantic import ValidationError

from app.telegram_bots.config import TelegramBotsConfig, load_config, section_present
from deerflow.config.app_config import AppConfig

DEFAULTS = {
    "enabled": True,
    "public_base_url": None,
    "max_bots": 100,
    "max_bots_per_person": 10,
    "reconcile_interval_seconds": 30,
    "conflict_probe_seconds": 30,
    "streaming": "draft",
    "queue": {"depth": 10, "max_wait_seconds": 120, "retry_seconds": 10},
    "files": {
        "max_inbound_bytes": 20_000_000,
        "max_outbound_bytes": 50_000_000,
        "in_memory_max_bytes": 200_000_000,
        "retry_seconds": 3,
        "retry_max_seconds": 120,
        "pending_ttl_hours": 24,
    },
    "callbacks": {"draft_ttl_hours": 24, "sweep_after_days": 7, "max_age_days": 180},
    "prompts": {"ttl_seconds": 600},
    "intents": {"ttl_seconds": 600},
    "producer": {"poll_seconds": 15, "batch": 200},
    "outbox": {
        "per_link_per_minute": 1,
        "per_link_per_hour": 10,
        "per_person_per_day": 60,
        "digest_threshold": 3,
        "progress_edit_seconds": 30,
        "max_attempts": 5,
        "recent_act_seconds": 1800,
        "decision_hold_seconds": 120,
        "place_free_hold_seconds": 600,
    },
    "quiet": {"default_enabled": True, "default_start": "21:00", "default_end": "07:00"},
    "summary": {"default_enabled": True, "default_time": "08:00"},
    "assign": {"model_name": None, "timeout_seconds": 30, "max_chars": 4000},
    "voice": {
        "provider": "auto",
        "openai_model": "gpt-4o-mini-transcribe",
        "gemini_model": None,
        "max_seconds": 300,
        "max_seconds_per_person_per_day": 1800,
        "timeout_seconds": 60,
    },
    "acts": {"retry_seconds": 5, "max_attempts": 3},
    "profile": {"retry_hours": 24},
}


def app_config(**sections) -> AppConfig:
    # This loader reads only model_extra; avoid AppConfig validators' unrelated
    # process-global stream configuration while retaining the real extra storage.
    return AppConfig.model_construct(**sections)


def test_absent_section_gives_every_default_with_enabled_false():
    config = app_config()

    assert section_present(config) is False
    assert load_config(config).model_dump() == {**DEFAULTS, "enabled": False}


def test_present_empty_section_is_distinct_from_absent():
    config = app_config(telegram_bots={})

    assert section_present(config) is True
    assert load_config(config).model_dump() == DEFAULTS
    assert TelegramBotsConfig().model_dump() == DEFAULTS


def test_explicit_disabled_section_keeps_its_defaults():
    config = app_config(telegram_bots={"enabled": False})

    assert section_present(config) is True
    assert load_config(config).model_dump() == {**DEFAULTS, "enabled": False}


@pytest.mark.parametrize("section", [None, False, [], "on", 1])
def test_present_malformed_section_is_refused(section):
    config = app_config(telegram_bots=section)

    assert section_present(config) is True
    with pytest.raises(ValidationError):
        load_config(config)


@pytest.mark.parametrize("prefix", [None, *[key for key, value in DEFAULTS.items() if isinstance(value, dict)]])
def test_unknown_key_is_refused_with_its_name(prefix):
    body = {"unrecognized": 1} if prefix is None else {prefix: {"unrecognized": 1}}

    with pytest.raises(ValidationError) as caught:
        load_config(app_config(telegram_bots=body))

    expected_path = "unrecognized" if prefix is None else f"{prefix}.unrecognized"
    assert expected_path in str(caught.value)
    assert caught.value.errors()[0]["type"] == "extra_forbidden"


def test_default_timezone_is_not_a_configuration_key():
    with pytest.raises(ValidationError, match="default_timezone"):
        load_config(app_config(telegram_bots={"default_timezone": "UTC"}))


def test_partial_section_preserves_overrides_and_other_defaults():
    body = {
        "public_base_url": "https://tenant.example",
        "streaming": "edit",
        "max_bots": 50,
        "max_bots_per_person": 2,
        "conflict_probe_seconds": 0,
        "files": {"max_outbound_bytes": 1, "in_memory_max_bytes": 50_000_000},
        "voice": {"provider": "gemini", "gemini_model": "tenant-transcriber"},
        "quiet": {"default_start": "00:00", "default_end": "23:59"},
        "summary": {"default_time": "12:30"},
    }
    expected = deepcopy(DEFAULTS)
    for key, value in body.items():
        if isinstance(value, dict):
            expected[key].update(value)
        else:
            expected[key] = value

    assert load_config(app_config(telegram_bots=body)).model_dump() == expected


@pytest.mark.parametrize(
    "body",
    [
        {"enabled": "false"},
        {"max_bots": True},
        {"max_bots": "10"},
        {"max_bots": 10.5},
        {"max_bots": 0},
        {"max_bots": 501},
        {"max_bots_per_person": 0},
        {"max_bots_per_person": 41},
        {"streaming": "unknown"},
        {"public_base_url": 123},
        {"reconcile_interval_seconds": 0},
        {"conflict_probe_seconds": -1},
        {"queue": {"retry_seconds": 0}},
        {"files": {"max_outbound_bytes": 0}},
        {"files": {"max_outbound_bytes": 50_000_001}},
        {"files": {"in_memory_max_bytes": 49_999_999}},
        {"files": {"max_inbound_bytes": 0}},
        {"callbacks": {"max_age_days": 0}},
        {"voice": {"provider": "unknown"}},
        {"voice": {"timeout_seconds": -1}},
        {"producer": {"batch": 0}},
        {"acts": {"max_attempts": 0}},
        {"quiet": {"default_enabled": "yes"}},
        {"quiet": {"default_start": "24:00"}},
        {"quiet": {"default_end": "7:00"}},
        {"summary": {"default_time": "08:60"}},
        {"summary": {"default_time": "12:30\n"}},
    ],
)
def test_invalid_configuration_is_refused(body):
    with pytest.raises(ValidationError):
        load_config(app_config(telegram_bots=body))


@pytest.mark.parametrize(
    "body",
    [
        {"max_bots": 1, "max_bots_per_person": 1},
        {"max_bots": 500, "max_bots_per_person": 40},
        {"files": {"max_outbound_bytes": 1, "in_memory_max_bytes": 50_000_000}},
        {"files": {"max_outbound_bytes": 50_000_000}},
        {"conflict_probe_seconds": 0},
        *[{"voice": {"provider": value}} for value in ("auto", "openai", "gemini", "off")],
    ],
)
def test_supported_bounds_and_modes_are_admitted(body):
    config = load_config(app_config(telegram_bots=body))
    assert config.enabled is True


def test_loads_and_nested_defaults_are_independent():
    source = {"files": {"max_outbound_bytes": 1234}, "voice": {"provider": "off"}}
    config = app_config(telegram_bots=source)
    first = load_config(config)
    second = load_config(config)
    first.files.max_outbound_bytes = 4567
    first.voice.provider = "auto"

    assert second.files.max_outbound_bytes == 1234
    assert second.voice.provider == "off"
    assert source == {"files": {"max_outbound_bytes": 1234}, "voice": {"provider": "off"}}
    absent_first = load_config(app_config())
    absent_first.quiet.default_enabled = False
    assert load_config(app_config()).quiet.default_enabled is True
