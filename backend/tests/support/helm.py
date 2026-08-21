"""Shared helpers for Helm render contract tests."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

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
        if os.environ.get("CI"):
            pytest.fail("helm is required in CI to verify rendered chart values")
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


def find_rendered_object(
    documents: list[dict[str, object]],
    kind: str,
    *,
    component: str | None = None,
) -> dict[str, Any]:
    """Return one rendered object by kind and optional component label."""
    return next(document for document in documents if document.get("kind") == kind and (component is None or document.get("metadata", {}).get("labels", {}).get("app.kubernetes.io/component") == component))


def container_env(deployment: dict[str, Any]) -> dict[str, str | None]:
    """Return the first container's environment as a name/value mapping."""
    env = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    return {item["name"]: item.get("value") for item in env}


def deployment_env(
    component: str,
    *extra_args: str,
) -> dict[str, str | None]:
    """Render the chart and return one workload's container environment."""
    deployment = find_rendered_object(
        render_chart(*extra_args),
        "Deployment",
        component=component,
    )
    return container_env(deployment)
