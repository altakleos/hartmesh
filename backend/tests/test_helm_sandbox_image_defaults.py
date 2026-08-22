"""Contracts for the chart's repository-built sandbox image defaults."""

from __future__ import annotations

from pathlib import Path

import yaml

VALUES_FILE = Path(__file__).resolve().parents[2] / "deploy/helm/deer-flow/values.yaml"


def test_chart_defaults_use_one_repository_built_sandbox_reference() -> None:
    values = yaml.safe_load(VALUES_FILE.read_text(encoding="utf-8"))
    embedded_config = yaml.safe_load(values["config"])

    provisioner_image = values["provisioner"]["sandboxImage"]
    provider_image = embedded_config["sandbox"]["image"]
    assert provisioner_image == provider_image
    assert provisioner_image == "ghcr.io/bytedance/deer-flow-sandbox:latest"
    assert "enterprise-public-cn-beijing.cr.volces.com" not in provisioner_image
