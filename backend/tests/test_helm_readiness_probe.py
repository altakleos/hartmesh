"""Deployment probe contract for the Gateway Helm workload."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_GATEWAY_DEPLOYMENT = _REPO_ROOT / "deploy" / "helm" / "deer-flow" / "templates" / "gateway-deployment.yaml"


def _probe_path(template: str, probe_name: str) -> str:
    match = re.search(
        rf"(?ms)^\s+{probe_name}:\s*$.*?^\s+path:\s+(\S+)\s*$",
        template,
    )
    assert match is not None, f"{probe_name} is missing from the rendered workload template"
    return match.group(1)


def test_gateway_template_renders_distinct_readiness_and_liveness_paths() -> None:
    helm = shutil.which("helm")
    if helm is None:
        rendered = _GATEWAY_DEPLOYMENT.read_text(encoding="utf-8")
    else:
        result = subprocess.run(
            [
                helm,
                "template",
                "deer-flow",
                str(_GATEWAY_DEPLOYMENT.parents[1]),
                "--namespace",
                "deer-flow",
                "--set",
                "image.registry=example.invalid/deer-flow",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, result.stderr
        rendered = next(document for document in result.stdout.split("---") if "kind: Deployment" in document and "app.kubernetes.io/component: gateway" in document)

    assert _probe_path(rendered, "readinessProbe") == "/ready"
    assert _probe_path(rendered, "livenessProbe") == "/health"
