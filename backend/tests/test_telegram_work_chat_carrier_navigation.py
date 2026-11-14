"""Actual recorded SDK controls from Back retain full request navigation.

Review outcomes are canonical native-journal fixtures, not worker provenance.
"""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_callback_commands import press
from test_telegram_work_actions import management_request
from test_telegram_work_chat_management_runtime import open_detail
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_execution_outcomes import dispatch_edit
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


async def back_to_detail(e):
    await open_detail(e)
    withdraw = await selected(e, "work_withdraw")
    assert withdraw is not None
    e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
    assert await e.channel.handle_update(press(e, 6601, withdraw.id, message_id=6174))
    await e.channel._drain_ingress()
    async with e.db.sessions() as reader:
        parent = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["work_management_intent"]["kind"].as_string() == "cancel"))
        assert parent is not None and parent.parts
        e.identity = parent.id
    await dispatch(e, 6602)
    assert not e.dispatch_errors, e.dispatch_errors
    back = await selected(e, "back")
    assert back is not None
    e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
    assert await e.channel.handle_update(press(e, 6603, back.id, message_id=6602))
    await e.channel._drain_ingress()
    await dispatch_edit(e, 6602)
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        parent = await reader.get(TelegramOutboxRow, e.identity)
        assert parent.state == "sent" and parent.message_id == 6602
        assert parent.payload["work_news_basis"]["source"]["work"]["phase"] == "back"


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["information", "review"])
async def test_back_full_management_card_opens_current_request(work_delivery, purpose):
    e = work_delivery
    owner = await installed_work_runtime(e)
    try:
        if purpose == "review":
            e.work, e.request, _seen = await management_request(e, "review")
            e.clock.advance(1)
        await back_to_detail(e)
        kind = "accept_open" if purpose == "review" else "digest_item"
        button = await selected(e, kind)
        assert button is not None and button.target_id == e.request["id"]
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6604, button.id, message_id=6602))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            previews = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == e.request["id"], TelegramOutboxRow.class_ == "reply")))
            assert len(previews) == 1 and previews[0].parts, "Actual Back card request control produced no full current preview"
            assert previews[0].payload["work_request_basis"]["source"]["request"]["id"] == e.request["id"]
            if purpose == "review":
                assert previews[0].payload["work_request_basis"]["interaction"] == {"view": "accept"}
            e.identity = previews[0].id
        if purpose == "review":
            e.channel.bot.request.enqueue("editMessageReplyMarkup", result=True)
            e.clock.advance(10)
            assert await e.sender.tick() == 1
            await e.sender.drain()
        await dispatch(e, 6605)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, e.identity)).state == "sent"
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_back_full_management_card_stop_uses_its_original_attempt(work_delivery):
    from test_telegram_activation import seeded_attempt

    e = work_delivery
    owner = await installed_work_runtime(e)
    e.acts = owner.host.acts
    attempt_id, run, manager = await seeded_attempt(e)
    e.clock.advance(1)
    try:
        await back_to_detail(e)
        button = await selected(e, "stop_attempt")
        assert button is not None and button.target_id == attempt_id
        manager.cancel.assert_not_awaited()
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6606, button.id, message_id=6602))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            act = await reader.get(TelegramActRow, button.id)
            assert act is not None and act.state == "done" and act.body["attempt_id"] == attempt_id
            assert act.stop_dispatch["run_id"] == run.run_id and act.stop_dispatch["state"] == "requested"
        manager.cancel.assert_awaited_once_with(run.run_id)
    finally:
        await owner.stop()
