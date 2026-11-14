"""Work details open the existing full, source-owned request preview."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Update
from test_telegram_callback_commands import press
from test_telegram_work_chat_management_runtime import open_detail
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


def press_in_topic(e, update_id, button, topic):
    update = press(e, update_id, button.id, message_id=6174)
    data = update.to_dict()
    if topic is not None:
        data["callback_query"]["message"]["message_thread_id"] = topic
    return Update.de_json(data, e.channel.bot)


@pytest.mark.asyncio
@pytest.mark.parametrize("topic", [None, 93])
async def test_installed_work_detail_opens_a_full_current_question_as_explicit_reply(work_delivery, topic):
    e = work_delivery
    owner = await installed_work_runtime(e)
    try:
        await open_detail(e, topic=topic)
        button = await selected(e, "digest_item")
        assert button is not None and button.target_id == e.request["id"]
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        update = press_in_topic(e, 6251, button, topic)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            rows = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == e.request["id"], TelegramOutboxRow.class_ == "reply")))
            assert len(rows) == 1 and rows[0].parts
            question = rows[0]
            e.identity = question.id
            assert question.payload["work_request_basis"]["source"]["request"]["id"] == e.request["id"]
            assert question.message_thread_id == topic
            assert question.payload["work_request_basis"]["navigation_origin"]["kind"] == "view"
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        await dispatch(e, 6252)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            assert (await reader.get(TelegramOutboxRow, e.identity)).state == "sent"
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == e.identity)))
            assert controls and all(control.message_thread_id == topic for control in controls)
        sent = [call.parameters for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]
        assert sent[-1].get("message_thread_id") == topic
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press_in_topic(e, 6253, button, topic))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            previews = list(await reader.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == e.request["id"], TelegramOutboxRow.class_ == "reply")))
            assert len(previews) == 2
            assert (await reader.get(TelegramCallbackRow, button.id)).consumed_at is None
            assert not list(await reader.scalars(select(TelegramActRow)))
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["deleted_control", "changed_target", "inspect"])
async def test_question_preview_rechecks_original_view_and_access_after_provider(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots import work_request_source as module
    from app.telegram_bots.work_digest_projection import DigestProjection
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    owner = await installed_work_runtime(e)
    try:
        await open_detail(e)
        button = await selected(e, "digest_item")
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press_in_topic(e, 6251, button, None))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            e.identity = await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.request_id == e.request["id"], TelegramOutboxRow.class_ == "reply"))
        original_read, original_provider = module.current_attention, authz.resolve_route_permissions
        active, changed = [], []

        async def read(*args, **kwargs):
            if isinstance(kwargs.get("projection"), DigestProjection):
                active.append(True)
            try:
                return await original_read(*args, **kwargs)
            finally:
                active.clear()

        async def provider(*args, **kwargs):
            held = await original_provider(*args, **kwargs)
            if active and not changed:
                async with e.db.sessions.begin() as writer:
                    if change == "deleted_control":
                        await writer.delete(await writer.get(TelegramCallbackRow, button.id))
                    elif change == "changed_target":
                        (await writer.get(TelegramCallbackRow, button.id)).target_id = "f" * 32
                    else:
                        (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 1
                changed.append(True)
            return held

        monkeypatch.setattr(module, "current_attention", read)
        monkeypatch.setattr(authz, "resolve_route_permissions", provider)
        before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
        await dispatch(e, 6252)
        assert changed
        assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before
    finally:
        await owner.stop()
