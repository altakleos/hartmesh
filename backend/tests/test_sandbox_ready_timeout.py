"""The sandbox readiness budget: one validated setting, one effective value.

The local Docker backend waits for a new sandbox's ``/v1/sandbox`` before it
hands the sandbox out, and destroys it when the wait runs out. That budget used
to be a fixed 60 seconds; the released Compose profile's one-CPU gVisor
sandboxes were measured at 80 to 91 seconds, so every cold start there was
destroyed at 60. ``sandbox.ready_timeout`` sizes the budget per deployment.
"""

from __future__ import annotations

import importlib
import math

import pytest
from pydantic import ValidationError

from deerflow.community.aio_sandbox.backend import SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT, SANDBOX_READY_TIMEOUT_MAX
from deerflow.config.sandbox_config import SandboxConfig


@pytest.mark.parametrize(("value", "expected"), [(120, 120.0), (90.5, 90.5), ("120", 120.0), (SANDBOX_READY_TIMEOUT_MAX, float(SANDBOX_READY_TIMEOUT_MAX)), (0.25, 0.25)])
def test_ready_timeout_accepts_finite_positive_seconds(value: object, expected: float) -> None:
    assert SandboxConfig(use="test", ready_timeout=value).ready_timeout == expected


def test_ready_timeout_is_optional_and_unset_means_the_provider_default() -> None:
    assert SandboxConfig(use="test").ready_timeout is None


@pytest.mark.parametrize(
    ("value", "error_type"),
    [
        (0, "greater_than"),
        (-1, "greater_than"),
        (-0.5, "greater_than"),
        (math.inf, "finite_number"),
        (-math.inf, "finite_number"),
        (math.nan, "finite_number"),
        (".inf", "float_parsing"),
        ("abc", "float_parsing"),
        ("", "float_parsing"),
        (SANDBOX_READY_TIMEOUT_MAX + 1, "less_than_equal"),
        (True, "value_error"),
        (False, "value_error"),
        ([60], "float_type"),
    ],
)
def test_ready_timeout_refuses_values_that_would_disable_or_malform_the_deadline(value: object, error_type: str) -> None:
    with pytest.raises(ValidationError) as excinfo:
        SandboxConfig(use="test", ready_timeout=value)
    [error] = excinfo.value.errors()
    assert error["loc"] == ("ready_timeout",)
    assert error["type"] == error_type


def test_provider_resolves_the_default_when_the_setting_is_absent() -> None:
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    assert aio_mod.resolve_ready_timeout(None) == float(SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT)
    assert aio_mod.resolve_ready_timeout(120) == 120.0
    assert aio_mod.resolve_ready_timeout(90.5) == 90.5


@pytest.mark.parametrize("bad", [0, -1, math.inf, math.nan, True, "120", SANDBOX_READY_TIMEOUT_MAX + 1])
def test_provider_refuses_a_budget_the_schema_would_have_refused(bad: object) -> None:
    """Belt and braces: the provider re-validates what it was handed, so a
    configuration object built without the schema cannot disable the deadline."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    with pytest.raises(ValueError, match="readiness budget"):
        aio_mod.resolve_ready_timeout(bad)


def test_load_config_carries_the_configured_budget_to_the_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    sandbox = SandboxConfig(use="deerflow.community.aio_sandbox:AioSandboxProvider", ready_timeout=120)
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: SimpleNamespace(sandbox=sandbox, skills=None, stream_bridge=None))
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)

    config = provider._load_config()

    assert config["ready_timeout"] == 120.0
    provider._config = config
    assert provider.sandbox_ready_timeout() == 120.0


def test_load_config_defaults_the_budget_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    sandbox = SandboxConfig(use="deerflow.community.aio_sandbox:AioSandboxProvider")
    monkeypatch.setattr(aio_mod, "get_app_config", lambda: SimpleNamespace(sandbox=sandbox, skills=None, stream_bridge=None))
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)

    config = provider._load_config()

    assert config["ready_timeout"] == float(SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT)


def test_a_provider_built_without_the_key_still_has_a_finite_budget() -> None:
    """Test fixtures build providers by hand; a missing key is the default, never no deadline."""
    aio_mod = importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")
    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)
    provider._config = {"replicas": 3}
    assert provider.sandbox_ready_timeout() == float(SANDBOX_LOCAL_PROVIDER_READY_TIMEOUT)
    provider._config = {"replicas": 3, "ready_timeout": 0}
    with pytest.raises(ValueError, match="readiness budget"):
        provider.sandbox_ready_timeout()
