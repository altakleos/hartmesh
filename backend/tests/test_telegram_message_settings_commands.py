"""Actual chat preferences share settings, receipts and acknowledged edits."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_handlers import message, sent
from test_telegram_callback_commands import drain, press, setup
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback", [False, True])
@pytest.mark.parametrize(
    "field,value,expected",
    [
        ("notify", "none", {"notify": "none"}),
        ("quiet", "22:00-08:00", {"quiet_enabled": True, "quiet_start_minute": 1320, "quiet_end_minute": 480}),
        ("summary", "07:00", {"summary_enabled": True, "summary_minute": 420}),
        ("locale", "zh-CN", {"locale": "zh-CN"}),
    ],
)
async def test_message_settings_navigation_and_choice_use_one_atomic_receipt(db, monkeypatch, field, value, expected, rollback, *, off_loop=False):
    from app.telegram_bots.message_settings_commands import MessageSettings

    e = await setup(db, monkeypatch, off_loop=off_loop)
    e.services.message_settings = MessageSettings(e.services)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 1500, content="/settings"))
        await drain(e)
        async with db.sessions() as reader:
            callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_open", TelegramCallbackRow.payload["field"].as_string() == field))
            assert callback is not None
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 1501, callback.id))
        await drain(e)
        async with db.sessions() as reader:
            choice = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_set", TelegramCallbackRow.payload["field"].as_string() == field, TelegramCallbackRow.payload["value"].as_string() == value))
            assert choice is not None
        if rollback:
            previous = await db.settings.get(e.live.id)
            original = e.services.outbox.plan_product_keyboard

            async def refuse(*args, **kwargs):
                raise RuntimeError("required preference edit failed")

            monkeypatch.setattr(e.services.outbox, "plan_product_keyboard", refuse)
            with pytest.raises(RuntimeError, match="required preference edit"):
                await e.services.acts.apply_settings(
                    e.link, choice.id, chat_id=e.link.chat_id, message_id=501, message_thread_id=None, guard=e.services.commands._guard(e.channel, e.link), presentation=await e.services.message_settings.context(e.link)
                )
            assert await db.settings.get(e.live.id) == previous
            async with db.sessions() as reader:
                assert await reader.get(TelegramActRow, choice.id) is None
                assert (await reader.get(TelegramCallbackRow, choice.id)).consumed_at is None
            monkeypatch.setattr(e.services.outbox, "plan_product_keyboard", original)
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 1502, choice.id))
        await drain(e)
        settings = await db.settings.get(e.live.id)
        assert all(getattr(settings, key) == result for key, result in expected.items())
        async with db.sessions() as reader:
            acts = list(await reader.scalars(select(TelegramActRow)))
            assert len(acts) == 1 and acts[0].id == choice.id and acts[0].state == "done" and acts[0].body["field"] == field
        edits = [call for call in e.request.calls if call.endpoint == "editMessageText"]
        assert len(edits) == 2 and "/work" not in edits[-1].parameters["text"]
        if field == "notify":
            assert "Questions wait in" in edits[-1].parameters["text"]
        assert len(edits[-1].parameters["reply_markup"]["inline_keyboard"]) >= 2
        e.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 1503, choice.id))
        await drain(e)
        assert len([call for call in e.request.calls if call.endpoint == "editMessageText"]) == 2
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_settings_choice_requires_its_actual_acked_group(db, monkeypatch):
    from copy import deepcopy
    from uuid import uuid4

    from app.telegram_bots.message_settings_commands import MessageSettings

    e = await setup(db, monkeypatch)
    e.services.message_settings = MessageSettings(e.services)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 1500, content="/settings"))
        await drain(e)
        async with db.sessions() as reader:
            opening = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_open", TelegramCallbackRow.payload["field"].as_string() == "notify"))
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 1501, opening.id))
        await drain(e)
        async with db.sessions.begin() as writer:
            choice = await writer.scalar(
                select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_set", TelegramCallbackRow.payload["field"].as_string() == "notify", TelegramCallbackRow.payload["value"].as_string() == "none")
            )
            rows = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == choice.group_id)))
            group = uuid4().hex
            selected = None
            for row in rows:
                fields = {column.name: deepcopy(getattr(row, column.name)) for column in TelegramCallbackRow.__table__.columns}
                fields["id"], fields["group_id"] = uuid4().hex, group
                if row.id == choice.id:
                    selected = fields["id"]
                writer.add(TelegramCallbackRow(**fields))
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 1502, selected))
        await drain(e)
        settings = await db.settings.get(e.live.id)
        assert settings.notify == "all", "Never-ACKed parallel group changed preferences"
        async with db.sessions() as reader:
            assert await reader.get(TelegramActRow, selected) is None
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_completed_settings_receipt_requires_typed_value(db, monkeypatch):
    from copy import deepcopy

    from sqlalchemy.orm.attributes import flag_modified

    from app.telegram_bots.message_settings_commands import MessageSettings

    e = await setup(db, monkeypatch)
    e.services.message_settings = MessageSettings(e.services)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 1500, content="/settings"))
        await drain(e)
        async with db.sessions() as reader:
            opening = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_open", TelegramCallbackRow.payload["field"].as_string() == "summary"))
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 1501, opening.id))
        await drain(e)
        async with db.sessions.begin() as writer:
            choice = await writer.scalar(
                select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_set", TelegramCallbackRow.payload["field"].as_string() == "summary", TelegramCallbackRow.payload["value"].as_string() == "07:00")
            )
            selected = choice.id
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 1502, selected))
        await drain(e)
        async with db.sessions.begin() as writer:
            act = await writer.get(TelegramActRow, selected)
            result = deepcopy(act.result)
            result["value"]["enabled"] = 1
            act.result = result
            flag_modified(act, "result")
        from app.telegram_bots.callbacks import CallbackRefused

        with pytest.raises(CallbackRefused):
            await e.services.acts.apply_settings(
                e.link, selected, chat_id=e.link.chat_id, message_id=501, message_thread_id=None, guard=e.services.commands._guard(e.channel, e.link), presentation=await e.services.message_settings.context(e.link)
            )
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_final_custody_read_cannot_reopen_disabled_account(db, monkeypatch):
    from app.telegram_bots.message_settings_commands import MessageSettings

    e = await setup(db, monkeypatch)
    e.services.message_settings = MessageSettings(e.services)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=501))
        assert await e.channel.handle_update(message(e, 1500, content="/settings"))
        await drain(e)
        async with db.sessions() as reader:
            opening = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_open", TelegramCallbackRow.payload["field"].as_string() == "summary"))
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 1501, opening.id))
        await drain(e)
        async with db.sessions.begin() as writer:
            choice = await writer.scalar(
                select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_set", TelegramCallbackRow.payload["field"].as_string() == "summary", TelegramCallbackRow.payload["value"].as_string() == "07:00")
            )
            selected = choice.id
        from datetime import UTC, datetime

        from app.telegram_bots import message_settings_custody as module
        from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
        from deerflow.persistence.user.access import DisabledIdentityRow

        original = module.require_settings_group
        changed = []

        async def after_owner_guard(writer, group, **kwargs):
            if group.selected.kind == "settings_set" and not changed:
                changed.append(True)
                writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.live.id, disabled_at=datetime.now(UTC)))
                await writer.flush()
            return await original(writer, group, **kwargs)

        monkeypatch.setattr(module, "require_settings_group", after_owner_guard)
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=501))
        assert await e.channel.handle_update(press(e, 1502, selected))
        await drain(e)
        assert changed
        settings = await db.settings.get(e.live.id)
        assert settings.summary_minute == 480, "Preference committed after final account check was invalidated"
        async with db.sessions() as reader:
            assert await reader.get(TelegramActRow, selected) is None
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_settings_ack_boolean_is_not_message_one(db, monkeypatch):
    from copy import deepcopy

    from sqlalchemy.orm.attributes import flag_modified

    from app.telegram_bots.message_settings_commands import MessageSettings
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = await setup(db, monkeypatch)
    e.services.message_settings = MessageSettings(e.services)
    try:
        e.request.enqueue("sendMessage", result=sent(e, identity=1))
        assert await e.channel.handle_update(message(e, 1500, content="/settings"))
        await drain(e)
        async with db.sessions() as reader:
            opening = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_open", TelegramCallbackRow.payload["field"].as_string() == "notify"))
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=1))
        assert await e.channel.handle_update(press(e, 1501, opening.id, message_id=1))
        await drain(e)
        async with db.sessions.begin() as writer:
            choice = await writer.scalar(
                select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_set", TelegramCallbackRow.payload["field"].as_string() == "notify", TelegramCallbackRow.payload["value"].as_string() == "none")
            )
            selected = choice.id
            parent = await writer.get(TelegramOutboxRow, choice.payload["anchor_outbox_id"])
            parts = deepcopy(parent.parts)
            parts[-1]["message_ids"] = [True]
            parent.parts = parts
            flag_modified(parent, "parts")
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=1))
        assert await e.channel.handle_update(press(e, 1502, selected, message_id=1))
        await drain(e)
        settings = await db.settings.get(e.live.id)
        assert settings.notify == "all", "Boolean ACK accepted as actual message one"
        async with db.sessions() as reader:
            assert await reader.get(TelegramActRow, selected) is None
    finally:
        await e.sender.stop()
        await e.channel.stop()
