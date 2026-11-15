"""A replacement callback owner reconciles one unknown extraction without retry."""

import asyncio
import json

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_result_suggestion_drafts import ready_suggestion
from test_telegram_result_suggestion_fences import damage_source
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assignment_requests import AssignmentDraftRequests
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["none", "marker"])
async def test_replacement_suggestion_owner_offers_manual_fallback_without_repeating_unknown_extraction(work_exports, monkeypatch, damage):
    e, calls = work_exports, []
    owner = await runtime(e)
    original = owner.chat.assignment_inputs.requests
    entered, release = asyncio.Event(), asyncio.Event()

    async def model(**kwargs):
        calls.append(kwargs)
        entered.set()
        await release.wait()
        return json.dumps({"objective": "Late draft", "success_criteria": "Must not replace recovery"})

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    try:
        _plans, identity, selected = await ready_suggestion(e)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 960, selected.id, message_id=911))
        await asyncio.wait_for(entered.wait(), 5)
        if damage != "none":
            await damage_source(e, identity, selected, damage)
        replacement = AssignmentDraftRequests(owner.host.acts)
        owner.chat.assignment_inputs.requests = replacement
        before = set(e.channel._ingress_tasks)
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 961, selected.id, message_id=911))
        second = e.channel._ingress_tasks - before
        assert len(second) == 1
        await asyncio.wait_for(asyncio.shield(next(iter(second))), 10)
        async with e.db.sessions() as reader:
            root = await reader.scalar(select(TelegramActRow))
            assert root.body["version"] == 2
            assert root.body["request"]["phase"] == ("awaiting_done" if damage == "none" else "extracting")
            prompts = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_prompt_intent"]["draft_id"].as_string() == root.id)))
            assert len(prompts) == int(damage == "none")
        release.set()
        await e.channel._drain_ingress()
        assert len(calls) == 1
        async with e.db.sessions() as reader:
            root = await reader.get(TelegramActRow, root.id)
            assert root.body["version"] == 2 and not root.operation_ids
            assert root.body["request"]["phase"] == ("awaiting_done" if damage == "none" else "extracting")
    finally:
        release.set()
        await e.channel._drain_ingress()
        await original.stop()
        await owner.stop()
