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


async def test_shared_marker_publication_runs_off_the_loop(tmp_path: Path, monkeypatch) -> None:
    """Resolving the shared config path and writing the reset marker are filesystem work.

    The customer reload route is provider-only, but every skill mutation tail
    still publishes the shared marker through ``_publish_skills_cache_reset``.
    """
    config_path = tmp_path / "extensions_config.json"
    await asyncio.to_thread(config_path.write_text, '{"mcpServers": {}, "skills": {}}', encoding="utf-8")
    monkeypatch.setenv("DEER_FLOW_EXTENSIONS_CONFIG_PATH", str(config_path))

    try:
        generation = await skills_router._publish_skills_cache_reset(None)
    finally:
        prompt_module._skills_cache_reset_tracker.reset()

    assert generation is not None
    assert await asyncio.to_thread(lambda: prompt_module.SKILLS_CACHE_RESET_MARKER.path_for(config_path).exists())
