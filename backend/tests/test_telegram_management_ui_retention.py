"""Unresolved management UI custody survives sweeping as one family."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import delete
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_management_navigation import publish_edit
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.retention import sweep_rows
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_pending_or_missing_native_keeps_actual_full_ack_and_opening_custody(work_delivery, missing):
    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    confirm = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, confirm.id, chat_id=e.link.chat_id, message_id=confirm.message_id, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        origin = parent.payload["work_management_intent"]["origin"]
        if missing:
            await writer.delete(await writer.get(TelegramActRow, act.id))
    later = e.clock.now() + timedelta(days=401)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        assert await reader.get(TelegramOutboxRow, origin["anchor_outbox_id"]) is not None
        assert await reader.get(TelegramCallbackRow, origin["gesture_id"]) is not None
        assert await reader.get(TelegramCallbackRow, confirm.id) is not None
        assert await reader.get(TelegramCallbackRow, button.id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["work_withdraw", "work_change"])
async def test_closed_callback_family_expires_complete_history_normally(work_delivery, kind):
    e = work_delivery
    plans, button = await prepare_management(e, kind)
    await open_card(e, plans, button)
    confirm = await selected(e, "work_withdraw_confirm" if kind == "work_withdraw" else "work_change_save")
    act = await plans.confirm(e.link, confirm.id, chat_id=e.link.chat_id, message_id=confirm.message_id, message_thread_id=None, guard=await guard_for(e))
    if kind == "work_withdraw":
        assert (await e.acts.check(act.id)).state == "done"
    else:
        assert act.state == "refused" and act.reason == "unchanged"
    await plans.observe(e.link, e.identity)
    await publish_edit(e)
    later = e.clock.now() + timedelta(days=401)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, act.id) is None
        if kind == "work_change":
            assert await reader.get(TelegramActRow, confirm.target_id) is None
        assert await reader.get(TelegramOutboxRow, e.identity) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["receipt", "reservation", "root"])
async def test_incomplete_closed_family_retains_callback_evidence(work_delivery, damage):
    e = work_delivery
    plans, button = await prepare_management(e, "work_change" if damage == "root" else "work_withdraw")
    await open_card(e, plans, button)
    confirm = await selected(e, "work_change_save" if damage == "root" else "work_withdraw_confirm")
    act = await plans.confirm(e.link, confirm.id, chat_id=e.link.chat_id, message_id=confirm.message_id, message_thread_id=None, guard=await guard_for(e))
    if damage != "root":
        assert (await e.acts.check(act.id)).state == "done"
    await plans.observe(e.link, e.identity)
    await publish_edit(e)
    async with e.db.sessions.begin() as writer:
        if damage == "receipt":
            await writer.execute(delete(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == act.id))
        elif damage == "root":
            await writer.delete(await writer.get(TelegramActRow, confirm.target_id))
        else:
            parent = await writer.get(TelegramOutboxRow, e.identity)
            parent.payload = {**parent.payload, "work_management_intent": {**parent.payload["work_management_intent"], "act_id": None}}
    later = e.clock.now() + timedelta(days=401)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, act.id) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        assert await reader.get(TelegramCallbackRow, confirm.id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("unchanged", [False, True])
async def test_discarded_edit_copy_releases_complete_history(work_delivery, unchanged):
    e = work_delivery
    plans, button = await prepare_management(e, "work_change")
    await open_card(e, plans, button)
    act = None
    if unchanged:
        confirm = await selected(e, "work_change_save")
        act = await plans.confirm(e.link, confirm.id, chat_id=e.link.chat_id, message_id=confirm.message_id, message_thread_id=None, guard=await guard_for(e))
        assert act.reason == "unchanged"
        await plans.observe(e.link, e.identity)
        await publish_edit(e)
    discard = await selected(e, "assign_discard")
    await plans.discard(e.link, discard.id, chat_id=e.link.chat_id, message_id=discard.message_id, message_thread_id=None, guard=await guard_for(e))
    await publish_edit(e)
    later = e.clock.now() + timedelta(days=401)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, discard.target_id) is None
        if act is not None:
            assert await reader.get(TelegramActRow, act.id) is None
        assert await reader.get(TelegramOutboxRow, e.identity) is None
