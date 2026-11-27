"""Retired settings fail before the Gateway starts and never echo input values."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from deerflow.config.app_config import AppConfig

BACKEND = Path(__file__).resolve().parents[1]
PROFILE = BACKEND.parent / "deploy" / "compose"
RETIRED_MODE = "strict-v1"
PRIVATE_DIGEST = "sensitive-input-for-test"


@pytest.mark.parametrize("selection", [{}, {"presentation": "legacy"}])
def test_tenant_bundle_loads_with_implicit_or_explicit_legacy(tmp_path, selection):
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump(_configuration(tmp_path, selection)), encoding="utf-8")
    assert AppConfig.from_file(str(config)).tenant_bundle.path == str(tmp_path)


@pytest.mark.parametrize(
    ("selection", "field"),
    [
        ({"presentation": RETIRED_MODE}, "tenant_bundle.presentation"),
        ({"presentation": "sensitive-mode-for-test"}, "tenant_bundle.presentation"),
        ({"presentation_sha256": PRIVATE_DIGEST}, "tenant_bundle.presentation_sha256"),
        ({"presentation_sha256": None}, "tenant_bundle.presentation_sha256"),
        ({"presentation_sha256": f"${PRIVATE_DIGEST}"}, "tenant_bundle.presentation_sha256"),
    ],
)
def test_removed_tenant_presentation_fields_are_named_without_values(tmp_path, selection, field):
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump(_configuration(tmp_path, selection)), encoding="utf-8")
    with pytest.raises((RuntimeError, ValueError)) as refused:
        AppConfig.from_file(str(config))
    _assert_migration(str(refused.value), field)


@pytest.mark.parametrize(
    ("selection", "field"),
    [({"presentation": RETIRED_MODE}, "tenant_bundle.presentation"), ({"presentation_sha256": PRIVATE_DIGEST}, "tenant_bundle.presentation_sha256")],
)
def test_removed_tenant_presentation_prevents_gateway_startup(tmp_path, selection, field):
    config = tmp_path / "config.yaml"
    config.write_text(yaml.safe_dump(_configuration(tmp_path, selection)), encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-m", "uvicorn", "app.gateway.app:app", "--host", "127.0.0.1", "--port", "0"],
        cwd=BACKEND,
        env={**os.environ, "DEER_FLOW_CONFIG_PATH": str(config)},
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert result.returncode != 0
    _assert_migration(result.stderr, field)
    assert "Uvicorn running on" not in result.stderr
    assert "Application startup complete" not in result.stderr


@pytest.mark.parametrize(
    ("selection", "field"),
    [
        ({}, None),
        ({"presentation": "legacy"}, None),
        ({"presentation": RETIRED_MODE}, "tenant_bundle.presentation"),
        ({"presentation": "sensitive-mode-for-test"}, "tenant_bundle.presentation"),
        ({"presentation_sha256": PRIVATE_DIGEST}, "tenant_bundle.presentation_sha256"),
        ({"presentation_sha256": None}, "tenant_bundle.presentation_sha256"),
    ],
)
def test_render_check_agrees_with_gateway(tmp_path, selection, field):
    data = yaml.safe_load((PROFILE / "config.yaml").read_text(encoding="utf-8"))
    data["tenant_bundle"] = {"path": str(tmp_path), **selection}
    template = tmp_path / "config.yaml"
    template.write_text(yaml.safe_dump(data), encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            str(PROFILE / "gateway/render_config.py"),
            "--template",
            str(template),
            "--catalog",
            str(PROFILE / "providers"),
            "--check",
        ],
        env={
            **os.environ,
            "HARTMESH_SYSTEM_MODELS": "",
            "HARTMESH_LOCAL_PASSWORDS": "allowed",
            "HARTMESH_LOCAL_REGISTRATION": "closed",
            "DATABASE_URL": "postgresql://test:x@postgres/test",
            "DEER_FLOW_STREAM_BRIDGE_REDIS_URL": "redis://:x@redis/0",
        },
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    if field is None:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode == 1
        _assert_migration(result.stderr, field)
        assert "Traceback" not in result.stderr


def _configuration(tmp_path, selection):
    return {
        "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"},
        "database": {"backend": "sqlite", "sqlite_dir": str(tmp_path / "data")},
        "tenant_bundle": {"path": str(tmp_path), **selection},
    }


def _assert_migration(message, field):
    assert field in message
    assert "remove" in message.lower()
    assert "brand.json" in message
    assert "HARTMESH_REPORT_TENANT_PRESENTATION" in message
    assert "HARTMESH_REPORT_TENANT_SHA256" in message
    assert "restart every Gateway worker" in message
    assert RETIRED_MODE not in message
    assert PRIVATE_DIGEST not in message
    assert "sensitive-mode-for-test" not in message
