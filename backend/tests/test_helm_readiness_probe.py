"""Deployment probe contract for the Gateway Helm workload."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_GATEWAY_DEPLOYMENT = _REPO_ROOT / "deploy" / "helm" / "deer-flow" / "templates" / "gateway-deployment.yaml"
_HELM_VALUES = _REPO_ROOT / "deploy" / "helm" / "deer-flow" / "values.yaml"


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


def test_gateway_chart_declares_local_default_and_validated_durable_profile() -> None:
    values = _HELM_VALUES.read_text(encoding="utf-8")
    template = _GATEWAY_DEPLOYMENT.read_text(encoding="utf-8")
    helpers = (_GATEWAY_DEPLOYMENT.parent / "_helpers.tpl").read_text(encoding="utf-8")

    assert "mode: local_evaluation" in values
    assert "profile: local_development" in values
    assert 'eq $mode "durable_one_replica"' in helpers
    assert "config deployment.profile=durable_production" in helpers
    assert "name: DEER_FLOW_IMAGE_REFERENCE" in template
    assert 'include "deer-flow.gatewayImage"' in template


def test_gateway_probe_timeouts_bound_internal_readiness_work() -> None:
    values = yaml.safe_load(_HELM_VALUES.read_text(encoding="utf-8"))
    template = _GATEWAY_DEPLOYMENT.read_text(encoding="utf-8")

    rendered_config = yaml.safe_load(values["config"])
    internal = rendered_config["deployment"]["readiness"]
    readiness_probe = values["gateway"]["readinessProbe"]
    liveness_probe = values["gateway"]["livenessProbe"]

    assert readiness_probe["timeoutSeconds"] > internal["overall_timeout_seconds"]
    assert internal["overall_timeout_seconds"] > internal["capability_probe_timeout_seconds"]
    assert readiness_probe["failureThreshold"] == 1
    assert liveness_probe["failureThreshold"] >= 1
    assert ".Values.gateway.readinessProbe.timeoutSeconds" in template
    assert ".Values.gateway.readinessProbe.failureThreshold" in template
    assert ".Values.gateway.livenessProbe.failureThreshold" in template


def test_gateway_termination_budget_is_derived_from_all_shutdown_phases() -> None:
    helm = shutil.which("helm")
    values = yaml.safe_load(_HELM_VALUES.read_text(encoding="utf-8"))
    rendered_config = yaml.safe_load(values["config"])
    shutdown = rendered_config["deployment"]["shutdown"]
    application_budget = sum(shutdown.values()) + rendered_config["memory"].get("shutdown_flush_timeout_seconds", 30.0)
    pre_stop = values["gateway"]["preStopSleepSeconds"]
    headroom = values["gateway"]["shutdownSchedulingHeadroomSeconds"]

    if helm is None:
        template = _GATEWAY_DEPLOYMENT.read_text(encoding="utf-8")
        assert "$shutdownTotal := addf" in template
        assert "$derivedTermination := int" in template
        assert ".Values.gateway.preStopSleepSeconds" in template
        assert ".Values.gateway.shutdownSchedulingHeadroomSeconds" in template
        assert "default $derivedTermination" in template
        return

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
    gateway = next(document for document in result.stdout.split("---") if "kind: Deployment" in document and "app.kubernetes.io/component: gateway" in document)
    rendered = yaml.safe_load(gateway)
    termination = rendered["spec"]["template"]["spec"]["terminationGracePeriodSeconds"]

    assert termination >= application_budget + pre_stop + headroom
    assert termination > application_budget + pre_stop
