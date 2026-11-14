"""A completed preview keyboard does not erase its admitted exact command."""

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_management_navigation import publish_edit
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import ActsWorker
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


@pytest.mark.asyncio
async def test_automatic_exact_retry_survives_its_own_saving_presentation(work_delivery, monkeypatch):
    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    confirm = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, confirm.id, chat_id=e.link.chat_id, message_id=confirm.message_id, message_thread_id=None, guard=await guard_for(e))
    command, calls = e.human.work.command, []

    async def fail_once(**kwargs):
        calls.append(kwargs["request"].model_dump(mode="json", exclude_unset=True))
        if len(calls) == 1:
            raise OSError("Synthetic pre-command connection loss")
        return await command(**kwargs)

    monkeypatch.setattr(e.human.work, "command", fail_once)
    assert (await e.acts.check(act.id)).state == "pending"
    await plans.observe(e.link, e.identity)
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, confirm.id)).consumed_at is not None
    e.clock.advance(e.acts.outbox.config.acts.retry_seconds + 1)
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    result = await e.acts._work_record(act.id)
    assert result.state == "done" and len(calls) == 2 and calls[0] == calls[1]


@pytest.mark.asyncio
async def test_cancel_control_fences_an_unattempted_command_without_work_effect(work_delivery):
    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    confirm = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, confirm.id, chat_id=e.link.chat_id, message_id=confirm.message_id, message_thread_id=None, guard=await guard_for(e))
    await plans.observe(e.link, e.identity)
    await publish_edit(e)
    cancel = await selected(e, "cancel_wait")
    assert cancel is not None
    args = dict(chat_id=e.link.chat_id, message_id=cancel.message_id, message_thread_id=None)
    result = await plans.cancel(e.link, cancel.id, **args, guard=await guard_for(e))
    again = await plans.cancel(e.link, cancel.id, **args, guard=await guard_for(e))
    assert result.id == again.id == act.id and result.cancel_requested_at is not None and result.state == "refused"
    current = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"])
    assert current["status"] != "cancelled"


@pytest.mark.asyncio
async def test_one_retry_control_attempts_only_its_exact_original_operation_once(work_delivery, monkeypatch):
    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    confirm = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, confirm.id, chat_id=e.link.chat_id, message_id=confirm.message_id, message_thread_id=None, guard=await guard_for(e))
    calls = []

    async def unknown(**kwargs):
        calls.append(kwargs["request"].model_dump(mode="json", exclude_unset=True))
        raise OSError("Synthetic unknown operation response")

    monkeypatch.setattr(e.human.work, "command", unknown)
    assert (await e.acts.check(act.id)).state == "pending"
    e.clock.advance(3601)
    worker = ActsWorker(e.acts)
    try:
        await worker.tick()
    finally:
        await worker.stop()
    assert (await e.acts._work_record(act.id)).state == "uncertain"
    await plans.observe(e.link, e.identity)
    await publish_edit(e)
    retry = await selected(e, "act_check")
    assert retry is not None
    args = dict(chat_id=e.link.chat_id, message_id=retry.message_id, message_thread_id=None)
    result = await plans.retry(e.link, retry.id, **args, guard=await guard_for(e))
    again = await plans.retry(e.link, retry.id, **args, guard=await guard_for(e))
    assert result.id == again.id == act.id and len(calls) == 2 and calls[0] == calls[1]
