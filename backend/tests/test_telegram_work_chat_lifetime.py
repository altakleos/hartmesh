"""Offline final lifetime fences preserve committed native ownership.

Recorded SDK and native-journal fixtures qualify metadata admission only;
no actual worker or Compose acceptance is claimed.
"""

from copy import deepcopy

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from test_telegram_assignment_entrypoints import installed_prompt
from test_telegram_callback_commands import press
from test_telegram_notification_runtime import runtime
from test_telegram_work_chat_management_runtime import open_detail
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_execution_outcomes import stop_notice
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


async def presentation(e, identity):
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramOutboxRow, identity)
        controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == identity).order_by(TelegramCallbackRow.id)))
        return deepcopy((row.payload, row.parts, row.state, [(r.id, r.payload, r.consumed_at, r.closed_reason, r.message_id) for r in controls]))


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["assignment", "management"])
async def test_retired_navigation_does_not_write_a_late_presentation(work_delivery, monkeypatch, family):
    e = work_delivery
    if family == "assignment":
        owner, calls = await installed_prompt(e, monkeypatch)
        assert await e.channel.handle_update(make_update(6701, account=e.link.chat_id, text="Review the handbook"))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6701, e.link.chat_id))
            root_id = mark.route_target_id
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["assignment_intent"]["draft_id"].as_string() == root_id))
            e.identity = card.id
        await dispatch(e, 6702)
        button = await selected(e, "assign_discard")
        plans = owner.host.work_chat.assignments
    else:
        owner = await installed_work_runtime(e)
        await open_detail(e)
        button = await selected(e, "work_withdraw")
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6701, button.id, message_id=6174))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            card = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_management_intent"]["kind"].as_string() == "cancel"))
            e.identity = card.id
        await dispatch(e, 6702)
        button = await selected(e, "back")
        plans = owner.host.work_chat.management
    assert button is not None and not e.dispatch_errors, e.dispatch_errors
    original, held = plans.observe, []

    async def retire(*args, **kwargs):
        assert not held
        held.append(await presentation(e, e.identity))
        owner._stopping = True
        return await original(*args, **kwargs)

    monkeypatch.setattr(plans, "observe", retire)
    try:
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        try:
            await e.channel.handle_update(press(e, 6703, button.id, message_id=6702))
        except ProductSourceRetired:
            pass
        except ValueError as error:
            assert str(error) == "publication-guard-unavailable"
        await e.channel._drain_ingress()
        assert held, "The actual postcommit observer boundary was not reached"
        async with e.db.sessions() as reader:
            if family == "assignment":
                root = await reader.get(TelegramActRow, root_id)
                assert root.state == "refused" and root.reason == "discarded" and not root.operation_ids
                assert calls == ["Review the handbook"]
            else:
                row = await reader.get(TelegramOutboxRow, e.identity)
                assert row.payload["work_management_navigation"]["kind"] == "back"
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
        assert await presentation(e, e.identity) == held[0], "Retired navigation wrote a new presentation/control group after its original guarded commit"
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_retired_execution_creation_rolls_back_notice_without_losing_stop_receipt(work_delivery, monkeypatch):
    e = work_delivery
    _plans, original_act, _attempt_id, manager = await stop_notice(e)
    assert original_act.state == "done" and original_act.stop_dispatch["state"] == "requested"
    manager.cancel.assert_awaited_once()
    owner = await runtime(e, preserve=True)
    plans = owner.host.work_chat.executions
    guard = owner.host.work_chat.guard(e.channel, e.link)
    original_flush, entered = AsyncSession.flush, []
    key = "notice:work-execution:" + original_act.id

    async def retire(session, *args, **kwargs):
        result = await original_flush(session, *args, **kwargs)
        fresh = [row for row in session.identity_map.values() if isinstance(row, TelegramOutboxRow) and row.dedupe_key == key]
        if fresh and not entered:
            assert session.in_transaction() and fresh[0].parts == []
            entered.append(fresh[0].id)
            owner._stopping = True
        return result

    monkeypatch.setattr(AsyncSession, "flush", retire)
    try:
        try:
            await plans.observe(e.link, original_act.id, guard=guard)
        except ProductSourceRetired:
            pass
        except ValueError as error:
            assert str(error) == "publication-guard-unavailable"
        assert entered, "The real new execution notice was not flushed under the active writer"
        async with e.db.sessions() as reader:
            act = await reader.get(TelegramActRow, original_act.id)
            assert act.state == "done" and same_body(act.body, original_act.body) and same_body(act.result, original_act.result)
            assert same_body(act.stop_dispatch, original_act.stop_dispatch)
            assert await reader.get(TelegramOutboxRow, entered[0]) is None, "A retired execution owner committed a new unmaterialized notice"
        manager.cancel.assert_awaited_once()
    finally:
        await owner.stop()
