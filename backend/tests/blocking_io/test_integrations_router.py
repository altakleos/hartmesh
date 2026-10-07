"""Regression anchors: integrations router must not block the event loop.

The Lark integration handlers are async FastAPI route handlers, but the work
they dispatch includes zip reads, filesystem staging, manifest writes, and
``lark-cli`` subprocess calls. Those phases must stay behind
``asyncio.to_thread``; if a future refactor runs them inline, the strict
Blockbuster gate raises ``BlockingError`` and these anchors fail.
"""

from __future__ import annotations

import asyncio
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.gateway.routers import integrations
from deerflow.config import paths as paths_module
from deerflow.integrations import lark_cli

pytestmark = pytest.mark.asyncio


def _skill_content(name: str) -> str:
    return f"---\nname: {name}\ndescription: {name} integration skill\n---\n\n# {name}\n"


def _build_lark_archive(archive: Path) -> None:
    archive.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, "w") as zf:
        for skill_name in lark_cli.LARK_SKILL_NAMES:
            zf.writestr(f"cli-1.0.65/skills/{skill_name}/SKILL.md", _skill_content(skill_name))
            zf.writestr(f"cli-1.0.65/skills/{skill_name}/references/readme.md", f"# {skill_name}\n")


def _write_stub_lark_cli(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name == "nt":
        path.write_text(
            """@echo off
if "%~1" == "--version" (
  echo v1.0.65
  exit /b 0
)

if "%~1" == "auth" if "%~2" == "status" (
  echo {"identities":{"user":{"userName":"Alice"}}}
  exit /b 0
)

echo {}
exit /b 0
""",
            encoding="utf-8",
        )
        return

    path.write_text(
        """#!/bin/sh
if [ "$1" = "--version" ]; then
  echo "v1.0.65"
  exit 0
fi

if [ "$1" = "auth" ] && [ "$2" = "login" ]; then
  echo "{}"
  exit 0
fi

if [ "$1" = "auth" ] && [ "$2" = "status" ]; then
  echo '{"identities":{"user":{"userName":"Alice"}}}'
  exit 0
fi

echo "{}"
exit 0
""",
        encoding="utf-8",
    )
    path.chmod(0o755)


def _reset_paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DEER_FLOW_HOME", str(tmp_path / "home"))
    monkeypatch.setattr(paths_module, "_paths", None)


def _advance_lark_flow(user_id: str) -> str:
    with lark_cli._lark_credential_lock(user_id):
        return lark_cli._advance_lark_flow_generation_locked(user_id)


async def _config(tmp_path: Path) -> SimpleNamespace:
    skills_root = tmp_path / "skills"
    await asyncio.to_thread((skills_root / "public").mkdir, parents=True, exist_ok=True)
    await asyncio.to_thread((skills_root / "custom").mkdir, parents=True, exist_ok=True)
    return SimpleNamespace(
        skills=SimpleNamespace(
            get_skills_path=lambda: skills_root,
            container_path="/mnt/skills",
            use="deerflow.skills.storage.local_skill_storage:LocalSkillStorage",
        )
    )


async def test_lark_install_route_denies_before_blocking_work(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from unittest.mock import Mock

    from fastapi import HTTPException

    install = Mock(side_effect=AssertionError("Provider-only route reached install"))
    monkeypatch.setattr(integrations, "install_lark_integration", install)
    with pytest.raises(HTTPException) as denied:
        await integrations.install_lark(request=None, config=SimpleNamespace())
    assert denied.value.status_code == 403
    install.assert_not_called()


async def test_lark_auth_complete_route_does_not_block_event_loop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _reset_paths(tmp_path, monkeypatch)
    config = await _config(tmp_path)
    cli_name = "lark-cli.cmd" if os.name == "nt" else "lark-cli"
    cli_path = tmp_path / "cli bin" / cli_name
    await asyncio.to_thread(_write_stub_lark_cli, cli_path)

    monkeypatch.setenv("PATH", f"{cli_path.parent}{os.pathsep}{os.environ.get('PATH', '')}")
    monkeypatch.setattr(integrations, "get_effective_user_id", lambda: "loop-user")
    resolved_cli_path = await asyncio.to_thread(lark_cli._resolve_lark_cli_path)
    assert resolved_cli_path is not None
    assert Path(resolved_cli_path) == cli_path
    await asyncio.to_thread(lark_cli.ensure_lark_cli_credential_tree, "loop-user")
    config_dir = await asyncio.to_thread(lark_cli.lark_cli_config_dir, "loop-user")
    await asyncio.to_thread(
        (config_dir / "config.json").write_text,
        '{"apps":[{"appId":"test-app","appSecret":"test-secret"}]}',
        encoding="utf-8",
    )
    generation = await asyncio.to_thread(_advance_lark_flow, "loop-user")

    response = await integrations.complete_lark_browser_auth(
        request=None,
        body=integrations.LarkAuthCompleteRequest(device_code="device-code", generation=generation),
        config=config,
    )

    assert response.status.cli.available is True
    assert response.status.cli.version == "v1.0.65"
    assert response.success is True
    assert response.status.auth.status == "authenticated"
    assert response.status.auth.user == "Alice"
