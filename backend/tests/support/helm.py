"""Shared helpers for Helm render contract tests."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CHART = _REPO_ROOT / "deploy" / "helm" / "deer-flow"
_HELM = shutil.which("helm")


def render_chart(
    *extra_args: str,
    namespace: str = "deer-flow",
) -> list[dict[str, object]]:
    """Render the chart into parsed Kubernetes objects."""
    if _HELM is None:
        pytest.skip("helm is required to verify rendered chart values")
    result = subprocess.run(
        [
            _HELM,
            "template",
            "deer-flow",
            str(_CHART),
            "--namespace",
            namespace,
            *extra_args,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return [document for document in yaml.safe_load_all(result.stdout) if isinstance(document, dict)]


def deployment_env(
    component: str,
    *extra_args: str,
) -> dict[str, str | None]:
    """Render the chart and return one workload's container environment."""
    deployment = next(document for document in render_chart(*extra_args) if document.get("kind") == "Deployment" and document.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == component)
    env = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    return {item["name"]: item.get("value") for item in env}
