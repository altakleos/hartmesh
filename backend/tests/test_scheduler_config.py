"""Tests for SchedulerConfig schema."""

import pytest
from pydantic import ValidationError

from deerflow.config.scheduler_config import SchedulerConfig


def test_scheduler_config_defaults():
    config = SchedulerConfig()

    assert config.enabled is False
    assert config.recursion_limit == 1000


def test_scheduler_config_accepts_positive_recursion_limit():
    assert SchedulerConfig(recursion_limit=1).recursion_limit == 1
    assert SchedulerConfig(recursion_limit=1000).recursion_limit == 1000


def test_scheduler_config_rejects_non_positive_recursion_limit():
    with pytest.raises(ValidationError):
        SchedulerConfig(recursion_limit=0)

    with pytest.raises(ValidationError):
        SchedulerConfig(recursion_limit=-5)


def test_scheduler_config_puts_no_wall_time_bound_on_a_run_unless_asked():
    assert SchedulerConfig().max_run_seconds is None


def test_scheduler_config_accepts_a_wall_time_bound_from_five_minutes_to_a_day():
    assert SchedulerConfig(max_run_seconds=300).max_run_seconds == 300
    assert SchedulerConfig(max_run_seconds=900).max_run_seconds == 900
    assert SchedulerConfig(max_run_seconds=86400).max_run_seconds == 86400


@pytest.mark.parametrize("seconds", [0, 60, 299, -1, 86401])
def test_scheduler_config_rejects_a_wall_time_bound_outside_five_minutes_to_a_day(seconds):
    """Below five minutes a bound would stop nearly every run: a cold sandbox alone takes 80 to 91 s."""
    with pytest.raises(ValidationError):
        SchedulerConfig(max_run_seconds=seconds)
