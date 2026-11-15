"""Unflagged review cards preserve their exact old rendering through recovery."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_adapter import message
from test_telegram_callback_commands import press
from test_telegram_digest_item_families import protect
from test_telegram_notification_runtime import runtime
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_review_event_delivery import queued
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import DeliveryClaimRetired
from app.telegram_bots.work_result_delivery import WorkReviewPlans
from app.telegram_bots.work_result_files import present_review_files, result_options
from app.telegram_bots.work_result_recovery import WorkResultRecovery
from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_suggestion_event"], indirect=True)
async def test_legacy_review_show_and_locale_refresh_keep_the_unflagged_keyboard(work_exports, monkeypatch):
    e = work_exports
    owner = await runtime(e)
    original = WorkReviewPlans._extras

    def extras(self, source):
        return {key: value for key, value in original(self, source).items() if key != "suggestion_controls"}

    def presentation(self, source, settings, identity, *, concealed=None):
        return present_review_files(source, locale=settings.locale, concealed=source.protected if concealed is None else concealed, outbox_id=identity, **result_options(self._extras(source)["result_files"]))

    monkeypatch.setattr(WorkReviewPlans, "_extras", extras)
    monkeypatch.setattr(WorkReviewPlans, "_presentation", presentation)
    try:
        await protect(e)
        plans, identity = await queued(e)
        owner.host.work_result_sources.recovery = WorkResultRecovery(plans)
        assert await WorkReviewEventDelivery(plans).materialize(e.link, identity)
        message(e.channel.bot.request, identity=1111)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            show = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"]))
            assert "suggestion_controls" not in row.payload["work_request_basis"]
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 1110, show.id, message_id=1111))
        await e.channel._drain_ingress()
        for endpoint, message_id in (("editMessageReplyMarkup", 1111), ("sendDocument", 1112)):
            message(e.channel.bot.request, endpoint, identity=message_id)
            assert await e.sender.tick() == 1
            await e.sender.drain()
            e.delivery_clock.advance(2)
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramPersonSettingsRow, e.link.owner_user_id)).locale = "zh-CN"
        # The real source owner refreshes the pending text after the file ACK.
        assert await e.sender.tick() == 1
        await e.sender.drain()
        e.delivery_clock.advance(2)
        message(e.channel.bot.request, identity=1113)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            assert row.state == "sent", e.dispatch_errors
            assert "suggestion_controls" not in row.payload["work_request_basis"]
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
            assert {control.kind for control in controls} == {"accept_open", "changes_open"}
        calls = [call for call in e.channel.bot.request.calls if call.endpoint == "sendDocument"]
        assert len(calls) == 1
        assert "Review the older evidence next." not in [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"][-1].parameters["text"]
        assert [type(error) for error in e.dispatch_errors] == [DeliveryClaimRetired]
    finally:
        await owner.stop()
