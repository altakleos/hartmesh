"""Helm contracts for the provisioner-created sandbox startup budget."""

from __future__ import annotations

from support.helm import deployment_env

_STARTUP_ENV = {
    "SANDBOX_STARTUP_PROBE_INITIAL_DELAY_SECONDS": "0",
    "SANDBOX_STARTUP_PROBE_PERIOD_SECONDS": "10",
    "SANDBOX_STARTUP_PROBE_TIMEOUT_SECONDS": "3",
    "SANDBOX_STARTUP_PROBE_FAILURE_THRESHOLD": "15",
}


def test_sandbox_startup_probe_defaults_are_forwarded_to_provisioner() -> None:
    environment = deployment_env("provisioner")

    assert {name: environment[name] for name in _STARTUP_ENV} == _STARTUP_ENV


def test_sandbox_startup_probe_budget_is_values_driven() -> None:
    environment = deployment_env(
        "provisioner",
        "--set",
        "sandbox.startupProbe.initialDelaySeconds=5",
        "--set",
        "sandbox.startupProbe.periodSeconds=12",
        "--set",
        "sandbox.startupProbe.timeoutSeconds=4",
        "--set",
        "sandbox.startupProbe.failureThreshold=10",
    )

    assert {name: environment[name] for name in _STARTUP_ENV} == {
        "SANDBOX_STARTUP_PROBE_INITIAL_DELAY_SECONDS": "5",
        "SANDBOX_STARTUP_PROBE_PERIOD_SECONDS": "12",
        "SANDBOX_STARTUP_PROBE_TIMEOUT_SECONDS": "4",
        "SANDBOX_STARTUP_PROBE_FAILURE_THRESHOLD": "10",
    }
