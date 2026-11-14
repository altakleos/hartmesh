"""Work-detail review gestures open full current previews before any action.

The submitted outcome is a native journal fixture, not worker qualification.
"""

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Update
from test_telegram_callback_commands import press
from test_telegram_work_actions import management_request
from test_telegram_work_chat_management_runtime import open_detail
from test_telegram_work_chat_request_navigation import press_in_topic
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["accept_open", "changes_open"])
@pytest.mark.parametrize("topic", [None, 94])
async def test_installed_work_detail_review_requires_a_fresh_complete_preview(work_delivery, kind, topic):
    e = work_delivery
    owner = await installed_work_runtime(e)
    e.work, request, _seen = await management_request(e, "review")
    e.clock.advance(1)
    try:
        await open_detail(e, topic=topic)
        button = await selected(e, kind)
        assert button is not None
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        update = press_in_topic(e, 6271, button, topic)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:work-interaction:" + button.id))
            assert row is not None and row.parts
            e.identity = row.id
            assert row.payload["work_request_basis"]["interaction"] == ({"view": "accept"} if kind == "accept_open" else {"view": "prompt", "purpose": "changes"})
            assert not list(await reader.scalars(select(TelegramActRow)))
            assert (await reader.get(AgentWorkRow, e.work["id"])).status == "submitted"
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        e.channel.bot.request.enqueue("editMessageReplyMarkup", result=True)
        e.clock.advance(10)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        await dispatch(e, 6272)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, e.identity)
            assert row.state == "sent" and row.request_id == request["id"] and row.message_thread_id == topic
        if kind == "accept_open":
            confirm = await selected(e, "accept_confirm")
            assert confirm is not None and confirm.id != button.id
        else:
            from test_telegram_work_refusal_recovery import comment

            # A colleague's new comment changes the shown basis while writing.
            # Only this stale-note path requires another complete confirmation.
            await comment(e, request)
            assert await e.channel.handle_update(make_update(6273, account=e.link.chat_id, text="Add verified references to the guide", topic=topic))
            await e.channel._drain_ingress()
            async with e.db.sessions() as reader:
                row = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.request_id == request["id"], TelegramOutboxRow.payload["work_request_basis"]["version"].as_integer() == 4))
                assert row is not None and row.payload["work_request_basis"]["interaction"]["note"] == "Add verified references to the guide"
                assert not list(await reader.scalars(select(TelegramActRow)))
                e.identity = row.id
            await dispatch(e, 6274)
            assert not e.dispatch_errors, e.dispatch_errors
            confirm = await selected(e, "changes_confirm")
            assert confirm is not None
        if confirm is not None:
            e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
            update = press(e, 6275, confirm.id, message_id=6272 if kind == "accept_open" else 6274)
            data = update.to_dict()
            if topic is not None:
                data["callback_query"]["message"]["message_thread_id"] = topic
            assert await e.channel.handle_update(Update.de_json(data, e.channel.bot))
            await e.channel._drain_ingress()
            async with e.db.sessions() as reader:
                act = await reader.get(TelegramActRow, confirm.id)
                assert act is not None and act.kind == ("accept" if kind == "accept_open" else "changes_requested") and act.state == "done"
                assert (await reader.get(AgentWorkRow, e.work["id"])).status == ("completed" if kind == "accept_open" else "open")
                assert not list(await reader.scalars(select(TelegramActRow).where(TelegramActRow.kind == "activate")))
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["origin", "target", "inspect"])
async def test_review_preview_refuses_late_view_or_access_changes(work_delivery, monkeypatch, change):
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    e = work_delivery
    owner = await installed_work_runtime(e)
    e.work, _request, _seen = await management_request(e, "review")
    e.clock.advance(1)
    try:
        await open_detail(e)
        button = await selected(e, "accept_open")
        e.channel.bot.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 6281, button.id, message_id=6174))
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            e.identity = await reader.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.dedupe_key == "notice:work-interaction:" + button.id))
        e.channel.bot.request.enqueue("editMessageReplyMarkup", result=True)
        e.clock.advance(10)
        assert await e.sender.tick() == 1
        await e.sender.drain()
        sources = owner.host.work_request_sources
        original, changed = sources.settings.get, []

        async def retire(*args, **kwargs):
            settings = await original(*args, **kwargs)
            if not changed:
                async with e.db.sessions.begin() as writer:
                    if change == "inspect":
                        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
                    else:
                        control = await writer.get(TelegramCallbackRow, button.id)
                        if change == "origin":
                            control.closed_reason = "superseded"
                        else:
                            control.target_id = "f" * 32
                changed.append(True)
            return settings

        monkeypatch.setattr(sources.settings, "get", retire)
        before = len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"])
        await dispatch(e, 6282)
        assert changed
        assert len([call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"]) == before
        async with e.db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramActRow)))
    finally:
        await owner.stop()
