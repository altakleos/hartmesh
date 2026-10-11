"""Staged native file integrations refuse instead of touching legacy directories."""

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException


@pytest.mark.asyncio
@pytest.mark.parametrize("qualified", [False, True])
@pytest.mark.parametrize("door", ["browser", "archive", "skill"])
async def test_pending_native_file_doors_refuse_before_any_legacy_file_effect(monkeypatch, door, qualified):
    from app.gateway import chat_storage
    from app.gateway.routers import browser, skills, thread_runs
    from deerflow.config.paths import Paths
    from deerflow.spaces.chat_paths import ChatStoragePaths

    paths = ChatStoragePaths(Paths("/tmp/unused-native-chat-test"), user_id="alice", user=object() if qualified else None)
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("chat-a", user_id="alice"), reason="space-not-assigned")
    admission = AsyncMock(return_value=prepared)
    monkeypatch.setattr(chat_storage, "prepare_chat_request", admission)
    monkeypatch.setattr(skills, "require_customer_management_for_request", lambda *_: None)
    monkeypatch.setattr(skills, "require_private_skill_owner", AsyncMock())
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(storage_spaces_enabled=True)))

    def legacy(*args, **kwargs):
        pytest.fail("A staged native file route used a legacy filesystem path")

    for module in (browser, skills, thread_runs):
        if hasattr(module, "get_paths"):
            monkeypatch.setattr(module, "get_paths", legacy)
    if door == "browser":
        call = inspect.unwrap(browser.navigate_browser)("chat-a", browser.BrowserNavigateRequest(url="https://example.com"), request)
    elif door == "archive":
        call = inspect.unwrap(thread_runs.create_run_artifact_archive)("chat-a", "run-id", request)
    else:
        call = inspect.unwrap(skills.install_skill)(request, skills.SkillInstallRequest(thread_id="chat-a", path="/mnt/user-data/outputs/file.skill"), SimpleNamespace())
    with pytest.raises(HTTPException) as caught:
        await call
    if qualified:
        assert caught.value.status_code == 501 and "managed conversations" in caught.value.detail
    else:
        assert caught.value.status_code == 503 and caught.value.detail["reason"] == "space-not-assigned"
        admission.assert_awaited_once_with(request, "chat-a")
