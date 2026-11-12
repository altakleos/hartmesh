"""File warnings navigate to the outcome's attempt, never a later attempt."""

from dataclasses import replace
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_bot_adapter import message
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_result_delivery import prepare_delivery
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.contract import Permission


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review"], indirect=True)
@pytest.mark.parametrize("mode", ["normal", "url_fallback", "no_access", "attempt_changed", "refresh_after_warning", "foreign_origin"])
async def test_warning_url_uses_exact_outcome_attempt_and_durable_plain_fallback(work_exports, monkeypatch, mode):
    e = work_exports
    e.plans.outbox.config = e.plans.outbox.config.model_copy(update={"public_base_url": "https://example.test"})
    e.live.config.model_extra["telegram_bots"] = {**(e.live.config.model_extra.get("telegram_bots") or {}), "public_base_url": "https://example.test"}
    original = e.reader.read_request

    async def later_probe(*args, **kwargs):
        source = await original(*args, **kwargs)
        return replace(source, work={**source.work, "attempt": {"id": "9" * 32, "thread_id": "later-conversation"}})

    monkeypatch.setattr(e.reader, "read_request", later_probe)
    identity = await prepare_delivery(e)
    if mode == "refresh_after_warning":
        from app.telegram_bots.work_result_recovery import WorkResultRecovery

        e.channel._services.work_result_sources.recovery = WorkResultRecovery(e.plans)
    async with e.db.sessions() as reader:
        attempt = await reader.get(AgentWorkAttemptRow, e.result["outcome"]["attempt_id"])
        expected = "https://example.test/workspace/chats/" + attempt.thread_id
    if mode == "no_access":
        async with e.db.sessions.begin() as writer:
            (await writer.get(SpaceGrantRow, (e.home.id, "human", e.actor.subject_id))).permissions = int(Permission.READ)
    else:
        await e.files.remove(actor=e.instance.principal, space_id=e.home.id, expected_generation=e.home.generation, operation_id=uuid4().hex, path="outputs/result.txt")
    sdk = e.channel.bot.request
    assert await e.sender.tick() == 1
    await e.sender.drain()
    if mode == "foreign_origin":
        from copy import deepcopy

        from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramOutboxRow, identity)
            payload, parts = deepcopy(row.payload), deepcopy(row.parts)
            payload["work_result_navigation"]["public_base_url"] = "https://foreign.example.test"
            parts[1]["url_button"]["url"] = expected.replace("https://example.test", "https://foreign.example.test")
            row.payload, row.parts = payload, parts
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not [call for call in sdk.calls if call.endpoint == "sendMessage"]
        return
    if mode == "attempt_changed":
        async with e.db.sessions.begin() as writer:
            (await writer.get(AgentWorkAttemptRow, e.result["outcome"]["attempt_id"])).thread_id = "changed-after-planning"
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        assert not [call for call in sdk.calls if call.endpoint == "sendMessage"]
        return
    if mode == "url_fallback":
        sdk.enqueue("sendMessage", status=400, body={"ok": False, "error_code": 400, "description": "Bad Request: inline keyboard button URL is invalid"})
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
    message(sdk, identity=1601)
    message(sdk, identity=1602)
    for index in range(2):
        e.delivery_clock.advance(2)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        if index == 0 and mode == "refresh_after_warning":
            from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow

            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
            e.delivery_clock.advance(2)
            assert await e.sender.tick() == 1
            await e.sender.drain()
    calls = [call for call in sdk.calls if call.endpoint == "sendMessage"]
    if mode == "no_access":
        assert "reply_markup" not in calls[0].parameters and expected not in calls[0].parameters["text"]
    else:
        assert calls[0].parameters["reply_markup"] == {"inline_keyboard": [[{"text": "Open conversation", "url": expected}]]}
        if mode == "url_fallback":
            assert "reply_markup" not in calls[1].parameters and calls[1].parameters["text"].endswith(expected)
    assert all("later-conversation" not in str(call.parameters) for call in calls)
    assert len(calls) == (3 if mode == "url_fallback" else 2)
