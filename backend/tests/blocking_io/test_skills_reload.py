"""Operator cache refresh offloads scans; customer reload is provider-only."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.gateway.routers import skills as skills_router
from deerflow.agents.lead_agent import prompt as prompt_module
from deerflow.skills.storage.local_skill_storage import LocalSkillStorage

pytestmark = pytest.mark.asyncio


def _seed_skill(skills_root: Path) -> None:
    skill_dir = skills_root / "public" / "reload-anchor"
    skill_dir.mkdir(parents=True, exist_ok=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: reload-anchor\ndescription: blocking IO regression anchor\n---\n# Reload anchor\n",
        encoding="utf-8",
    )


async def test_operator_refresh_offloads_directory_scan(tmp_path: Path, monkeypatch) -> None:
    await asyncio.to_thread(_seed_skill, tmp_path)
    storage = await asyncio.to_thread(LocalSkillStorage, host_path=str(tmp_path))

    monkeypatch.setattr(prompt_module, "get_or_new_skill_storage", lambda **_kwargs: storage)
    await prompt_module.refresh_skills_system_prompt_cache_async()


async def test_customer_reload_denies_before_scanning(monkeypatch) -> None:
    from unittest.mock import AsyncMock

    from fastapi import HTTPException

    refresh = AsyncMock()
    monkeypatch.setattr(skills_router, "refresh_skills_system_prompt_cache_async", refresh)
    with pytest.raises(HTTPException) as denied:
        await skills_router.reload_skills(request=None)
    assert denied.value.status_code == 403
    refresh.assert_not_awaited()
