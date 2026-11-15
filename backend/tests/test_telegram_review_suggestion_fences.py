"""Current review and original suggestion share the final borrowed-writer read."""

import json

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_review_event_delivery import queued
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["resolved", "rerouted", "revision"])
async def test_late_review_change_cannot_commit_a_suggestion_draft_after_its_fresh_check(work_exports, monkeypatch, damage):
    from app.telegram_bots import result_suggestion_drafts as module

    e, calls, changed = work_exports, [], []
    owner = await runtime(e)
    armed = [False]

    async def model(**kwargs):
        calls.append(kwargs)
        return json.dumps({"objective": "Review the older evidence next.", "success_criteria": "List verified references"})

    original_projection = module.fresh_result_projection

    def projection(*args, **kwargs):
        found = original_projection(*args, **kwargs)
        original = found.check

        def checked(rows):
            original(rows)
            armed[0] = True

        found.check = checked
        return found

    original_lock = owner.host.callbacks.lock_current

    async def lock(*args, **kwargs):
        armed[0] = False
        return await original_lock(*args, **kwargs)

    original_guard = owner.chat.guard

    def guard(channel, link):
        found = original_guard(channel, link)
        original = found.check

        async def checked(writer):
            result = await original(writer)
            if armed[0] and not changed:
                root = await writer.scalar(select(TelegramActRow).where(TelegramActRow.body["version"].as_integer() == 1))
                if root is not None:
                    request = await writer.get(HumanInputRequestRow, e.result["human_input_request_id"])
                    if damage == "resolved":
                        request.state, request.closed_reason = "closed", "withdrawn"
                    elif damage == "rerouted":
                        request.recipient_id = None
                    else:
                        request.revision += 1
                        (await writer.get(AgentWorkRow, e.result["id"])).revision += 1
                    await writer.flush()
                    changed.append(True)
            return result

        found.check = checked
        return found

    monkeypatch.setattr("app.telegram_bots.assign.run_oneshot_llm", model)
    monkeypatch.setattr("app.telegram_bots.assign.get_app_config", lambda: e.live.config)
    monkeypatch.setattr(module, "fresh_result_projection", projection)
    monkeypatch.setattr(owner.host.callbacks, "lock_current", lock)
    monkeypatch.setattr(owner.chat, "guard", guard)
    try:
        plans, identity = await queued(e)
        assert await WorkReviewEventDelivery(plans).materialize(e.link, identity)
        for endpoint, message_id in (("sendDocument", 1001), ("sendMessage", 1002)):
            message(e.channel.bot.request, endpoint, identity=message_id)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            e.delivery_clock.advance(2)
        async with e.db.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, identity)
            selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"], TelegramCallbackRow.kind == "assign_from_suggestion"))
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 1003, selected.id, message_id=1002))
        await e.channel._drain_ingress()
        assert changed and len(calls) == 1
        async with e.db.sessions() as reader:
            root = await reader.scalar(select(TelegramActRow))
            assert root.body["version"] == 2 and root.body["request"]["phase"] == "extracting"
            assert not list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string().is_not(None))))
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_suggestion_event"], indirect=True)
@pytest.mark.parametrize("damage", ["actor", "attempt"])
async def test_review_suggestion_final_sdk_qualifies_its_canonical_report_after_file_ack(work_exports, monkeypatch, damage):
    e = work_exports
    owner = await runtime(e)
    try:
        plans, identity = await queued(e)
        assert await WorkReviewEventDelivery(plans).materialize(e.link, identity)
        message(e.channel.bot.request, "sendDocument", identity=1011)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
        sources = owner.host.work_result_sources
        original, changed = sources.settings.get, []

        async def settings(*args, **kwargs):
            result = await original(*args, **kwargs)
            if not changed:
                async with e.db.sessions.begin() as writer:
                    event = await writer.get(AgentWorkEventRow, e.result["suggestion"]["id"])
                    if damage == "actor":
                        event.actor_id = "another-principal"
                    else:
                        event.request = {**event.request, "attempt_id": "f" * 32}
                changed.append(True)
            return result

        monkeypatch.setattr(sources.settings, "get", settings)
        before = len(e.channel.bot.request.calls)
        await e.sender.tick()
        await e.sender.drain()
        assert changed
        assert not [call for call in e.channel.bot.request.calls[before:] if call.endpoint == "sendMessage"]
        async with e.db.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, identity)
            assert parent.parts[0]["message_ids"] == [1011]
    finally:
        await owner.stop()
