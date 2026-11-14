import pytest
from _telegram_repository_test_support import db as db
from test_telegram_activation import seeded_attempt
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_management_navigation import publish_edit
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import ActsWorker
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


async def logical_retry(e, monkeypatch):
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    control = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
    command = e.human.work.command
    calls = []

    async def unknown(**kwargs):
        calls.append(kwargs["request"].model_dump(mode="json", exclude_unset=True))
        raise OSError("Synthetic unknown command response")

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
    return plans, retry, act, command, calls


@pytest.mark.asyncio
@pytest.mark.parametrize("delay", [0, 3601])
async def test_one_consumed_retry_resumes_only_its_original_unattempted_check(work_delivery, monkeypatch, delay):
    e = work_delivery
    plans, retry, act, command, calls = await logical_retry(e, monkeypatch)
    args = dict(chat_id=e.link.chat_id, message_id=retry.message_id, message_thread_id=None)
    check = e.acts.check

    async def crash(*args, **kwargs):
        raise RuntimeError("Synthetic process failure before exact Act check")

    monkeypatch.setattr(e.acts, "check", crash)
    with pytest.raises(RuntimeError):
        await plans.retry(e.link, retry.id, **args, guard=await guard_for(e))
    async with e.db.sessions() as reader:
        consumed = (await reader.get(TelegramCallbackRow, retry.id)).consumed_at
    assert consumed is not None

    async def resumed(**kwargs):
        calls.append(kwargs["request"].model_dump(mode="json", exclude_unset=True))
        return await command(**kwargs)

    monkeypatch.setattr(e.human.work, "command", resumed)
    monkeypatch.setattr(e.acts, "check", check)
    e.clock.advance(delay)
    result = await plans.retry(e.link, retry.id, **args, guard=await guard_for(e))
    if delay:
        assert result.state != "done" and len(calls) == 1
    else:
        assert result.state == "done" and len(calls) == 2 and calls[0] == calls[1]
        again = await plans.retry(e.link, retry.id, **args, guard=await guard_for(e))
        assert again.state == "done" and len(calls) == 2
    async with e.db.sessions() as reader:
        assert (await reader.get(TelegramCallbackRow, retry.id)).consumed_at == consumed


@pytest.mark.asyncio
async def test_physical_retry_is_not_reissued_from_a_static_logical_counter(work_delivery):
    e = work_delivery
    attempt_id, record, manager = await seeded_attempt(e)
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    control = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
    manager.cancel.side_effect = OSError("Synthetic unknown physical cancellation")
    act = await e.acts.check(act.id)
    act = await e.acts.resume_stop_dispatch(act)
    act = await e.acts.resume_stop_dispatch(act)
    assert act.state == "done" and act.stop_dispatch["state"] == "uncertain" and manager.cancel.await_count == 3
    await plans.observe(e.link, e.identity)
    await publish_edit(e)
    retry = await selected(e, "act_check")
    args = dict(chat_id=e.link.chat_id, message_id=retry.message_id, message_thread_id=None)
    first = await plans.retry(e.link, retry.id, **args, guard=await guard_for(e))
    again = await plans.retry(e.link, retry.id, **args, guard=await guard_for(e))
    assert first.attempts == again.attempts == act.attempts
    assert manager.cancel.await_count == 4
