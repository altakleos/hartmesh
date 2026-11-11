"""Routing acknowledgements retain the exact completed command and its anchor."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_outbox_workers import Clock
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_routing_admission import setup, stage
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_completed_route_edits_original_selection_and_records_ack(work_delivery, monkeypatch):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    saved = await stage(e, controller, control)
    done = await controller.acts.check(saved.id)
    assert done.state == "done"
    e.clock = Clock()
    e.clock.now = datetime.now(UTC)
    e.sender.outbox.clock = e.clock
    plans = WorkOutcomePlans(e.plans, controller.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, saved.id, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.parts[0]["kind"] == "edit"
        assert row.parts[0]["target_message_id"] == 951
    message(e.channel.bot.request, endpoint="editMessageText", identity=951)
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        act = await reader.get(TelegramActRow, done.id)
        assert row.state == "sent", (row.failed_reason, e.dispatch_errors)
        assert act.continuation["work_outcome"]["outbox_id"] == identity
        assert act.body == dict(done.body) and act.attempts == done.attempts
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "editMessageText"][-1]
    assert call.parameters["text"] == "Passed on to another@example.com."
    assert call.parameters["reply_markup"] == {"inline_keyboard": []}


@pytest.mark.asyncio
async def test_missing_routing_result_anchor_replans_receipt_without_another_command(work_delivery, monkeypatch):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    saved = await stage(e, controller, control)
    done = await controller.acts.check(saved.id)
    e.clock = Clock()
    e.clock.now = datetime.now(UTC)
    e.sender.outbox.clock = e.clock
    plans = WorkOutcomePlans(e.plans, controller.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, saved.id, guard=await guard_for(e))
    e.channel.bot.request.enqueue("editMessageText", body={"ok": False, "error_code": 400, "description": "Bad Request: message to edit not found"}, status=400)
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        old = await reader.get(TelegramOutboxRow, identity)
        assert old.state == "failed" and old.failed_reason == "work-outcome-refresh-required"
        history = old.parts
    replacement = await plans.plan(e.link, saved.id, guard=await guard_for(e))
    assert replacement != identity
    assert await plans.plan(e.link, saved.id, guard=await guard_for(e)) == replacement
    async with e.db.sessions() as reader:
        new = await reader.get(TelegramOutboxRow, replacement)
        assert new.parts[0]["kind"] == "text"
        old = await reader.get(TelegramOutboxRow, identity)
        assert [part for part in old.parts if part["state"] == "sent"] == [part for part in history if part["state"] == "sent"]
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup

    e.outcome_identity = replacement
    await clear_outcome_cleanup(e)
    message(e.channel.bot.request, identity=952)
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, replacement)).state == "sent", e.dispatch_errors
        current = await reader.get(TelegramActRow, saved.id)
        assert current.attempts == done.attempts and current.body == dict(done.body)


@pytest.mark.asyncio
@pytest.mark.parametrize("queued", [False, True])
async def test_routing_result_protection_upgrade_clears_old_keyboard_before_new_reply(work_delivery, monkeypatch, queued):
    from test_telegram_work_outcome_delivery import clear_outcome_cleanup
    from test_telegram_work_preview_recovery import dispatch

    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
    from deerflow.persistence.telegram_bots.model import TelegramInstancePolicyRow

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    saved = await stage(e, controller, control)
    await controller.acts.check(saved.id)
    e.clock = Clock()
    e.clock.now = datetime.now(UTC)
    e.sender.outbox.clock = e.clock
    plans = WorkOutcomePlans(e.plans, controller.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    if queued:
        await plans.plan(e.link, saved.id, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        writer.add(TelegramInstancePolicyRow(instance_id=e.link.agent_ref, policy="protected", updated_by=e.link.owner_user_id, updated_at=e.clock()))
    identity = e.outcome_identity = await plans.plan(e.link, saved.id, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.parts[0]["kind"] == "text" and row.parts[0]["protect_content"]
        cleanup = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["kind"].as_string() == "keyboard_cleanup", TelegramOutboxRow.payload["target_message_id"].as_integer() == 951))
        assert cleanup is not None
    await clear_outcome_cleanup(e)
    await dispatch(e, 952)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state == "sent", e.dispatch_errors
    call = [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1]
    assert call.parameters["protect_content"]


@pytest.mark.asyncio
async def test_routing_result_rejects_ack_for_another_message(work_delivery, monkeypatch):
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    saved = await stage(e, controller, control)
    await controller.acts.check(saved.id)
    e.clock = Clock()
    e.clock.now = datetime.now(UTC)
    e.sender.outbox.clock = e.clock
    plans = WorkOutcomePlans(e.plans, controller.acts)
    e.channel._services.work_outcome_sources = WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, saved.id, guard=await guard_for(e))
    message(e.channel.bot.request, endpoint="editMessageText", identity=999)
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        assert row.state == "failed" and row.failed_reason == "invalid-work-outcome-receipt"
        assert (await reader.get(TelegramActRow, saved.id)).continuation is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["parent_ack", "callback_anchor", "protection"])
async def test_final_routing_result_edit_requires_original_ack(work_delivery, monkeypatch, damage):
    from copy import deepcopy

    import app.telegram_bots.work_outcome_delivery as module
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    controller, control = await setup(e, monkeypatch)
    saved = await stage(e, controller, control)
    await controller.acts.check(saved.id)
    e.clock = Clock()
    e.clock.now = datetime.now(UTC)
    e.sender.outbox.clock = e.clock
    plans = module.WorkOutcomePlans(e.plans, controller.acts)
    e.channel._services.work_outcome_sources = module.WorkOutcomeSources(plans)
    identity = await plans.plan(e.link, saved.id, guard=await guard_for(e))
    original = module.current_attention

    async def changed(*args, **kwargs):
        async with e.db.sessions.begin() as writer:
            callback = await writer.get(TelegramCallbackRow, control.id)
            parent = await writer.get(TelegramOutboxRow, callback.payload["anchor_outbox_id"])
            if damage == "callback_anchor":
                callback.message_id = 998
            else:
                parts = deepcopy(parent.parts)
                if damage == "parent_ack":
                    parts[-1]["message_ids"] = [998]
                else:
                    parts[-1]["protect_content"] = True
                parent.parts = parts
        return await original(*args, **kwargs)

    monkeypatch.setattr(module, "current_attention", changed)
    before = len(e.channel.bot.request.calls)
    message(e.channel.bot.request, endpoint="editMessageText", identity=951)
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert len(e.channel.bot.request.calls) == before
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramOutboxRow, identity)).state != "sent"
        assert (await reader.get(TelegramActRow, saved.id)).continuation is None


@pytest.mark.asyncio
async def test_routing_result_cannot_retire_another_presentations_controls(work_delivery, monkeypatch):
    from copy import deepcopy

    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans
    from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

    e = work_delivery
    unrelated_id = e.identity
    controller, control = await setup(e, monkeypatch)
    saved = await stage(e, controller, control)
    await controller.acts.check(saved.id)
    async with e.db.sessions.begin() as writer:
        unrelated = await writer.get(TelegramOutboxRow, unrelated_id)
        foreign_part = deepcopy(unrelated.parts[-1])
        parent = await writer.get(TelegramOutboxRow, control.payload["anchor_outbox_id"])
        parent.parts = [*parent.parts, foreign_part]
    with pytest.raises(ProductSourceRetired):
        await WorkOutcomePlans(e.plans, controller.acts).plan(e.link, saved.id, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == foreign_part["callback_group_id"])))
        assert controls and all(item.consumed_at is None for item in controls)
