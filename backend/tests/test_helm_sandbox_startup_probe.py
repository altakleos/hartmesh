"""Helm contracts for provisioner-created sandbox probe budgets."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from support.helm import deployment_env

_STARTUP_ENV = {
    "SANDBOX_STARTUP_PROBE_INITIAL_DELAY_SECONDS": "0",
    "SANDBOX_STARTUP_PROBE_PERIOD_SECONDS": "10",
    "SANDBOX_STARTUP_PROBE_TIMEOUT_SECONDS": "3",
    "SANDBOX_STARTUP_PROBE_FAILURE_THRESHOLD": "20",
}
_LIVENESS_ENV = {
    "SANDBOX_LIVENESS_PROBE_INITIAL_DELAY_SECONDS": "10",
    "SANDBOX_LIVENESS_PROBE_PERIOD_SECONDS": "10",
    "SANDBOX_LIVENESS_PROBE_TIMEOUT_SECONDS": "10",
    "SANDBOX_LIVENESS_PROBE_FAILURE_THRESHOLD": "3",
}
_REPO_ROOT = Path(__file__).resolve().parents[2]
_PROVISIONER_PATH = _REPO_ROOT / "docker" / "provisioner" / "app.py"
_PROBE_ENV_NAMES = (*_STARTUP_ENV, *_LIVENESS_ENV)


def _run_provisioner_import(
    environment: dict[str, str],
    *,
    print_liveness_probe: bool = False,
) -> subprocess.CompletedProcess[str]:
    script = f"""
import importlib.util
import json
import sys

spec = importlib.util.spec_from_file_location("provisioner_probe_contract", {_PROVISIONER_PATH.as_posix()!r})
assert spec is not None
assert spec.loader is not None
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
if {print_liveness_probe!r}:
    probe = module._build_pod("probe-contract", "thread-1").spec.containers[0].liveness_probe
    print(json.dumps({{
        "initialDelaySeconds": probe.initial_delay_seconds,
        "periodSeconds": probe.period_seconds,
        "timeoutSeconds": probe.timeout_seconds,
        "failureThreshold": probe.failure_threshold,
    }}))
"""
    process_environment = os.environ.copy()
    for name in _PROBE_ENV_NAMES:
        process_environment.pop(name, None)
    process_environment.update(environment)
    process_environment["SANDBOX_VOLUME_MODE"] = "hostpath"
    return subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        env=process_environment,
        cwd=_REPO_ROOT,
    )


def test_sandbox_startup_probe_defaults_are_forwarded_to_provisioner() -> None:
    environment = deployment_env("provisioner")

    assert {name: environment[name] for name in _STARTUP_ENV} == _STARTUP_ENV


def test_sandbox_liveness_probe_defaults_are_forwarded_to_provisioner() -> None:
    environment = deployment_env("provisioner")

    assert {name: environment[name] for name in _LIVENESS_ENV} == _LIVENESS_ENV


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


def test_sandbox_liveness_probe_budget_reaches_the_built_pod() -> None:
    environment = deployment_env(
        "provisioner",
        "--set",
        "sandbox.livenessProbe.initialDelaySeconds=7",
        "--set",
        "sandbox.livenessProbe.periodSeconds=13",
        "--set",
        "sandbox.livenessProbe.timeoutSeconds=6",
        "--set",
        "sandbox.livenessProbe.failureThreshold=4",
    )
    rendered_environment = {name: environment[name] for name in _LIVENESS_ENV if environment[name] is not None}

    result = _run_provisioner_import(
        rendered_environment,
        print_liveness_probe=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "initialDelaySeconds": 7,
        "periodSeconds": 13,
        "timeoutSeconds": 6,
        "failureThreshold": 4,
    }


@pytest.mark.parametrize(
    ("environment", "expected_message"),
    [
        (
            {"SANDBOX_LIVENESS_PROBE_INITIAL_DELAY_SECONDS": "-1"},
            "SANDBOX_LIVENESS_PROBE_INITIAL_DELAY_SECONDS; expected a value in [0, 300]",
        ),
        (
            {"SANDBOX_LIVENESS_PROBE_PERIOD_SECONDS": "0"},
            "SANDBOX_LIVENESS_PROBE_PERIOD_SECONDS; expected a value in [1, 300]",
        ),
        (
            {"SANDBOX_LIVENESS_PROBE_TIMEOUT_SECONDS": "301"},
            "SANDBOX_LIVENESS_PROBE_TIMEOUT_SECONDS; expected a value in [1, 300]",
        ),
        (
            {"SANDBOX_LIVENESS_PROBE_FAILURE_THRESHOLD": "61"},
            "SANDBOX_LIVENESS_PROBE_FAILURE_THRESHOLD; expected a value in [1, 60]",
        ),
        (
            {
                "SANDBOX_LIVENESS_PROBE_PERIOD_SECONDS": "5",
                "SANDBOX_LIVENESS_PROBE_TIMEOUT_SECONDS": "6",
            },
            "SANDBOX_LIVENESS_PROBE_TIMEOUT_SECONDS must not exceed SANDBOX_LIVENESS_PROBE_PERIOD_SECONDS",
        ),
    ],
)
def test_invalid_sandbox_liveness_probe_environment_stops_provisioner_startup(
    environment: dict[str, str],
    expected_message: str,
) -> None:
    result = _run_provisioner_import(environment)

    assert result.returncode != 0
    assert expected_message in result.stderr
