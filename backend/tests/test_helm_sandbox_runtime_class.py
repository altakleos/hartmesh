"""Helm plumbing contract for the sandbox RuntimeClass selection."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CHART = _REPO_ROOT / "deploy" / "helm" / "deer-flow"
_HELM = shutil.which("helm")


def _provisioner_env(*extra_args: str) -> dict[str, str]:
    if _HELM is None:
        pytest.skip("helm is required to verify rendered chart values")
    result = subprocess.run(
        [
            _HELM,
            "template",
            "deer-flow",
            str(_CHART),
            "--namespace",
            "deer-flow",
            *extra_args,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    deployment = next(
        document for document in yaml.safe_load_all(result.stdout) if isinstance(document, dict) and document.get("kind") == "Deployment" and document.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == "provisioner"
    )
    env = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    return {item["name"]: item.get("value") for item in env}


@pytest.mark.parametrize(
    ("extra_args", "expected"),
    [
        ((), ""),
        (("--set", "sandbox.runtimeClassName=gvisor"), "gvisor"),
    ],
    ids=["cluster-default", "gvisor"],
)
def test_sandbox_runtime_class_value_renders_to_provisioner_env(
    extra_args: tuple[str, ...],
    expected: str,
) -> None:
    env = _provisioner_env(*extra_args)

    assert env["SANDBOX_RUNTIME_CLASS"] == expected
