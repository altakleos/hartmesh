"""Installed Stop wiring uses an actual acknowledged view and native command.

The original attempt/run are journal fixtures; native worker qualification is CI.
"""

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_activation import seeded_attempt
from test_telegram_callback_commands import press
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_installed_detail_stop_keeps_its_original_attempt_and_dispatch(work_delivery):
    e = work_delivery
    owner = await installed_work_runtime(e)
    e.acts = owner.host.acts
    attempt_id, run, manager = await seeded_attempt(e)
    e.clock.advance(1)
    try:
        assert await e.channel.handle_update(make_update(6161, account=e.link.chat_id, text="/work"))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            listing = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["key"].as_string() == "view.list"))
            e.identity = listing.id
        await dispatch(e, 6162)
        async with e.db.sessions() as reader:
            detail = await reader.scalar(
                select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity, TelegramCallbackRow.kind == "work_details", TelegramCallbackRow.target_id == e.work["id"])
            )
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6163, detail.id, message_id=6162))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_view_intent"]["key"].as_string() == "view.detail", TelegramOutboxRow.work_id == e.work["id"]))
            e.identity = row.id
        await dispatch(e, 6164)
        stop = await selected(e, "stop_attempt")
        manager.cancel.assert_not_awaited()
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        update = press(e, 6165, stop.id, message_id=6164)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            act = await reader.get(TelegramActRow, stop.id)
            assert act is not None and act.kind == "stop" and act.state == "done"
            assert act.body["attempt_id"] == attempt_id and act.stop_dispatch["run_id"] == run.run_id
            assert act.stop_dispatch["state"] == "requested"
            assert (await reader.get(AgentWorkAttemptRow, attempt_id)).status == "stopping"
            assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.action == "stop", AgentWorkEventRow.work_id == e.work["id"])) == 1
            assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
        manager.cancel.assert_awaited_once_with(run.run_id)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        manager.cancel.assert_awaited_once_with(run.run_id)
    finally:
        await owner.stop()
