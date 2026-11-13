"""Custom quiet hours require an acknowledged prompt and durable original input."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_bot_handlers import message, sent
from test_telegram_callback_commands import drain, press, setup
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow


async def hours(e, db):
    from app.telegram_bots.message_settings_commands import MessageSettings
    from app.telegram_bots.settings_prompts import SettingsPrompts

    e.services.message_settings = MessageSettings(e.services)
    owner = SettingsPrompts(e.services)
    e.services.settings_prompts = owner
    e.request.enqueue("sendMessage", result=sent(e, identity=501))
    assert await e.channel.handle_update(message(e, 1600, content="/settings"))
    await drain(e)
    async with db.sessions() as reader:
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_open", TelegramCallbackRow.payload["field"].as_string() == "quiet"))
    e.request.enqueue("answerCallbackQuery", result=True)
    e.request.enqueue("editMessageText", result=sent(e, identity=501))
    assert await e.channel.handle_update(press(e, 1601, callback.id))
    await drain(e)
    async with db.sessions() as reader:
        callback = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_prompt"))
    e.request.enqueue("answerCallbackQuery", result=True)
    assert await e.channel.handle_update(press(e, 1602, callback.id))
    import asyncio

    await asyncio.gather(*tuple(e.channel._ingress_tasks))
    return owner


@pytest.mark.asyncio
async def test_custom_quiet_hours_apply_once_after_prompt_ack(db, monkeypatch, *, off_loop=False):
    e = await setup(db, monkeypatch, off_loop=off_loop)
    try:
        owner = await hours(e, db)
        assert await e.services.prompts.current(e.link, e.link.chat_id, None) is None
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        await drain(e)
        prompt = await e.services.prompts.current(e.link, e.link.chat_id, None)
        assert prompt and prompt.message_id == 502 and e.services.prompts.is_visible(prompt)
        update = message(e, 1602, content="22:30-06:30")
        assert await owner.handle_text(e.channel, update, captured_link=e.link)
        settings = await db.settings.get(e.live.id)
        assert settings.quiet_enabled and (settings.quiet_start_minute, settings.quiet_end_minute) == (1350, 390)
        assert await owner.handle_text(e.channel, update, captured_link=e.link)
        async with db.sessions() as reader:
            acts = list(await reader.scalars(select(TelegramActRow)))
            assert len(acts) == 1 and acts[0].id == prompt.id and acts[0].kind == "settings" and acts[0].state == "done"
        e.request.enqueue("sendMessage", result=sent(e, identity=503))
        await drain(e)
        assert "22:30" in [call for call in e.request.calls if call.endpoint == "sendMessage"][-1].parameters["text"]
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["23:00-23:00", "off"])
async def test_invalid_hours_keep_prompt_open_and_never_change_preferences(db, monkeypatch, invalid):
    e = await setup(db, monkeypatch)
    try:
        owner = await hours(e, db)
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        await drain(e)
        previous = await db.settings.get(e.live.id)
        assert await owner.handle_text(e.channel, message(e, 1602, content=invalid), captured_link=e.link)
        assert await db.settings.get(e.live.id) == previous
        e.request.enqueue("sendMessage", result=sent(e, identity=503))
        await drain(e)
        assert "two different times" in [call for call in e.request.calls if call.endpoint == "sendMessage"][-1].parameters["text"]
        async with db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramActRow)))
        assert await owner.handle_text(e.channel, message(e, 1603, content="21:30-07:30"), captured_link=e.link)
        assert (await db.settings.get(e.live.id)).quiet_start_minute == 1290
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_closed_quiet_prompt_holds_text_and_reopens_without_reusing_it(db, monkeypatch):
    from datetime import timedelta

    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = await setup(db, monkeypatch)
    try:
        owner = await hours(e, db)
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        await drain(e)
        prompt = await e.services.prompts.current(e.link, e.link.chat_id, None)
        async with db.sessions.begin() as writer:
            row = await writer.get(TelegramCallbackRow, prompt.id)
            row.expires_at = e.services.callbacks.clock() - timedelta(seconds=1)
        previous = await db.settings.get(e.live.id)
        assert await e.channel.handle_update(message(e, 1610, content="21:30-06:30"))
        e.request.enqueue("sendMessage", result=sent(e, identity=503))
        await drain(e)
        assert await db.settings.get(e.live.id) == previous
        async with db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1610, e.link.chat_id))
            assert mark.route_basis["human_input_ui"]["held_prompt_id"] == prompt.id
            assert not mark.route_complete
            reopen = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt_reopen"))
        e.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 1611, reopen.id, message_id=503))
        e.request.enqueue("editMessageReplyMarkup", result=True)
        e.request.enqueue("sendMessage", result=sent(e, identity=504))
        await drain(e)
        await drain(e)
        current = await e.services.prompts.current(e.link, e.link.chat_id, None)
        assert current.id != prompt.id and current.message_id == 504
        assert await db.settings.get(e.live.id) == previous
        assert await owner.handle_text(e.channel, message(e, 1612, content="20:00-07:00"), captured_link=e.link)
        assert (await db.settings.get(e.live.id)).quiet_start_minute == 1200
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("choice", ["prompt_send_new", "prompt_discard"])
async def test_closed_quiet_input_requires_explicit_new_or_discard(db, monkeypatch, choice):
    from types import SimpleNamespace

    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = await setup(db, monkeypatch)
    staged = []

    async def accept_staged(channel, stage):
        staged.append(stage)

    try:
        owner = await hours(e, db)
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        await drain(e)
        await e.services.prompts.close(e.link, e.link.chat_id, None, reason="cancelled")
        assert await owner.handle_text(e.channel, message(e, 1620, content="21:00-06:00"), captured_link=e.link)
        e.request.enqueue("sendMessage", result=sent(e, identity=503))
        await drain(e)
        e.services.turns = SimpleNamespace(accept_staged=accept_staged, message_sent=lambda *args, **kwargs: None)
        async with db.sessions() as reader:
            row = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == choice))
        e.request.enqueue("answerCallbackQuery", result=True)
        if choice == "prompt_discard":
            e.request.enqueue("editMessageText", result=sent(e, identity=503))
        assert await e.channel.handle_update(press(e, 1621, row.id, message_id=503))
        await drain(e)
        assert bool(staged) == (choice == "prompt_send_new")
        if staged:
            assert staged[0].text == "21:00-06:00"
        async with db.sessions() as reader:
            assert not list(await reader.scalars(select(TelegramActRow)))
            assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 1620, e.link.chat_id))).route_complete
    finally:
        e.services.turns = None
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_interrupted_quiet_answer_recovers_to_choice_not_preference(db, monkeypatch):
    from types import SimpleNamespace

    from app.telegram_bots.inbox import InboxRow
    from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

    e = await setup(db, monkeypatch)
    try:
        owner = await hours(e, db)
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        await drain(e)

        async def interrupted(*args):
            raise RuntimeError("interrupted before settings writer")

        monkeypatch.setattr(owner, "_apply", interrupted)
        with pytest.raises(RuntimeError, match="interrupted"):
            await owner.handle_text(e.channel, message(e, 1630, content="21:00-06:00"), captured_link=e.link)
        previous = await db.settings.get(e.live.id)
        async with db.sessions() as reader:
            origin = InboxRow.from_row(await reader.get(TelegramInboundMarkRow, (e.link.id, 1630, e.link.chat_id)))
        assert origin.route_kind is None
        assert await owner.recover(e.channel, e.link, SimpleNamespace(origins=[origin]), guard=e.services.commands._guard(e.channel, e.link))
        assert await db.settings.get(e.live.id) == previous
        async with db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 1630, e.link.chat_id))
            assert mark.route_basis["human_input_ui"]["held_prompt_id"] == origin.route_basis["settings_prompt_id"]
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["prompt", "account", "claim"])
async def test_quiet_prompt_final_provider_fence(db, monkeypatch, change):
    from datetime import UTC, datetime

    from app.gateway import authz
    from app.telegram_bots import settings_prompts as module
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = await setup(db, monkeypatch)
    try:
        await hours(e, db)
        original, resolve = module.authorize_prompt, authz.resolve_route_permissions
        armed, changed = [], []

        async def authorize(*args, **kwargs):
            armed.append(True)
            return await original(*args, **kwargs)

        async def provider(*args, **kwargs):
            permissions = await resolve(*args, **kwargs)
            if armed and not changed:
                async with db.sessions.begin() as writer:
                    parent = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["settings_prompt_basis"]["version"].as_integer() == 1))
                    prompt = await writer.get(TelegramCallbackRow, parent.payload["settings_prompt_basis"]["prompt_id"])
                    if change == "prompt":
                        prompt.target_id = "other"
                    elif change == "claim":
                        parent.claim_id = "foreign"
                    else:
                        writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=e.live.id, disabled_at=datetime.now(UTC)))
                changed.append(True)
            return permissions

        monkeypatch.setattr(module, "authorize_prompt", authorize)
        monkeypatch.setattr(authz, "resolve_route_permissions", provider)
        before = len(e.request.calls)
        await drain(e)
        assert changed and len(e.request.calls) == before
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_quiet_prompt_sdk_ack_never_arms_mutated_source(db, monkeypatch):
    import asyncio

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = await setup(db, monkeypatch)
    release = asyncio.Event()
    try:
        await hours(e, db)
        call = e.request.enqueue("sendMessage", release=release, result=sent(e, identity=502))
        e.clock.advance(2)
        assert await e.sender.tick() == 1
        await asyncio.wait_for(call.started.wait(), 5)
        async with db.sessions.begin() as writer:
            parent = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.payload["settings_prompt_basis"]["version"].as_integer() == 1))
            identity = parent.id
            prompt = await writer.get(TelegramCallbackRow, parent.payload["settings_prompt_basis"]["prompt_id"])
            prompt.target_id = "foreign"
        release.set()
        await e.sender.drain()
        async with db.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, identity)
            prompt = await reader.get(TelegramCallbackRow, parent.payload["settings_prompt_basis"]["prompt_id"])
            assert parent.state == "failed" and parent.failed_reason == "invalid-settings-receipt"
            assert prompt.message_id is None and prompt.payload["prompt_visibility"]["state"] == "pending"
    finally:
        release.set()
        await e.sender.stop()
        await e.channel.stop()


async def ready(e, db):
    owner = await hours(e, db)
    e.request.enqueue("sendMessage", result=sent(e, identity=502))
    await drain(e)
    return owner, await e.services.prompts.current(e.link, e.link.chat_id, None)


@pytest.mark.asyncio
async def test_no_postplan_input_recapture(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        owner, prompt = await ready(e, db)
        original = e.services.outbox.plan_product_keyboard
        changed = []

        async def plan(*args, **kwargs):
            result = await original(*args, **kwargs)
            if kwargs.get("source_id") == "settings:" + prompt.id:
                row = await kwargs["session"].get(TelegramInboundMarkRow, (e.link.id, 1680, e.link.chat_id))
                row.sanitized_input = {**row.sanitized_input, "text": "20:00-05:00"}
                await kwargs["session"].flush()
                changed.append(True)
            return result

        monkeypatch.setattr(e.services.outbox, "plan_product_keyboard", plan)
        from app.telegram_bots.callbacks import CallbackRefused

        with pytest.raises(CallbackRefused):
            await owner.handle_text(e.channel, message(e, 1680, content="22:30-06:30"), captured_link=e.link)
        assert changed
        assert (await db.settings.get(e.live.id)).quiet_start_minute == 1260
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_prompt_requires_actual_acked_message(db, monkeypatch):
    from telegram import Update

    e = await setup(db, monkeypatch)
    try:
        owner, prompt = await ready(e, db)
        async with db.sessions.begin() as writer:
            (await writer.get(TelegramCallbackRow, prompt.id)).message_id = 9999
        data = message(e, 1681, content="22:30-06:30").to_dict()
        data["message"]["reply_to_message"] = sent(e, identity=9999)
        update = Update.de_json(data, e.channel.bot)
        from app.telegram_bots.producer import ProductSourceRetired

        with pytest.raises(ProductSourceRetired):
            await owner.handle_text(e.channel, update, captured_link=e.link)
        assert (await db.settings.get(e.live.id)).quiet_start_minute == 1260
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_live_closed_choice_retains_original_settings_prompt(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        e.services.callbacks.clock = e.clock.now
        owner = await hours(e, db)
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        await drain(e)
        original = await e.services.prompts.current(e.link, e.link.chat_id, None)
        days = e.services.callbacks.config.callbacks.max_age_days
        e.clock.advance((days - 1) * 86400)
        assert await owner.handle_text(e.channel, message(e, 1682, content="21:00-06:00"), captured_link=e.link)
        e.request.enqueue("sendMessage", result=sent(e, identity=503))
        await drain(e)
        async with db.sessions() as reader:
            choice = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt_discard"))
            assert choice is not None
        e.clock.advance(2 * 86400)
        await e.services.callbacks.sweep(e.clock.now())
        from app.telegram_bots.callbacks import CallbackRefused
        from app.telegram_bots.retention import sweep_rows
        from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

        await sweep_rows(db.sessions, e.clock.now(), callback_max_age_days=days)
        with pytest.raises(CallbackRefused):
            e.services.callbacks._unexpired(original)
        async with db.sessions() as reader:
            assert await reader.get(TelegramOutboxRow, original.payload["anchor_outbox_id"]) is not None
            assert await reader.get(TelegramCallbackRow, original.id) is not None, "Live closed settings choice lost original prompt proof"
            assert await reader.get(TelegramCallbackRow, choice.id) is not None
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_discard_old_settings_input_preserves_new_current_prompt(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        owner = await hours(e, db)
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        await drain(e)
        await e.services.prompts.close(e.link, e.link.chat_id, None, reason="cancelled")
        assert await owner.handle_text(e.channel, message(e, 1683, content="21:00-06:00"), captured_link=e.link)
        e.request.enqueue("sendMessage", result=sent(e, identity=503))
        await drain(e)
        async with db.sessions() as reader:
            old_choice = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt_discard"))
            opening = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "settings_prompt"))
        await owner.open(e.channel, opening, query_id="fresh-quiet-prompt")
        e.request.enqueue("sendMessage", result=sent(e, identity=504))
        await drain(e)
        current = await e.services.prompts.current(e.link, e.link.chat_id, None)
        assert current is not None and current.message_id == 504
        e.request.enqueue("answerCallbackQuery", result=True)
        e.request.enqueue("editMessageText", result=sent(e, identity=503))
        assert await e.channel.handle_update(press(e, 1684, old_choice.id, message_id=503))
        await drain(e)
        async with db.sessions() as reader:
            actual = await reader.get(TelegramCallbackRow, current.id)
            assert actual.consumed_at is None, "Discard on old input cancelled newer current quiet prompt"
            assert (await reader.get(TelegramInboundMarkRow, (e.link.id, 1683, e.link.chat_id))).route_complete
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_closed_settings_sendnew_requires_actual_acked_group(db, monkeypatch):
    from copy import deepcopy
    from types import SimpleNamespace
    from uuid import uuid4

    e = await setup(db, monkeypatch)
    try:
        owner = await hours(e, db)
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        await drain(e)
        await e.services.prompts.close(e.link, e.link.chat_id, None, reason="cancelled")
        assert await owner.handle_text(e.channel, message(e, 1683, content="21:00-06:00"), captured_link=e.link)
        e.request.enqueue("sendMessage", result=sent(e, identity=503))
        await drain(e)
        async with db.sessions.begin() as writer:
            choice = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt_send_new"))
            group_rows = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == choice.group_id)))
            group = uuid4().hex
            selected = None
            for row in group_rows:
                fields = {column.name: deepcopy(getattr(row, column.name)) for column in TelegramCallbackRow.__table__.columns}
                fields["id"], fields["group_id"] = uuid4().hex, group
                if row.id == choice.id:
                    selected = fields["id"]
                writer.add(TelegramCallbackRow(**fields))
        staged = []

        async def accept_staged(channel, stage):
            staged.append(stage)

        e.services.turns = SimpleNamespace(accept_staged=accept_staged, message_sent=lambda *args, **kwargs: None)
        e.request.enqueue("answerCallbackQuery", result=True)
        assert await e.channel.handle_update(press(e, 1684, selected, message_id=503))
        await drain(e)
        assert not staged, "Never-ACKed group transferred settings text to ordinary execution"
    finally:
        e.services.turns = None
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_no_pre_writer_input_recapture(db, monkeypatch):
    e = await setup(db, monkeypatch)
    try:
        owner, prompt = await ready(e, db)
        original = e.services.message_settings.context
        changed = []

        async def context(*args, **kwargs):
            result = await original(*args, **kwargs)
            async with db.sessions.begin() as writer:
                row = await writer.get(TelegramInboundMarkRow, (e.link.id, 1680, e.link.chat_id))
                if row is not None and not changed:
                    row.sanitized_input = {**row.sanitized_input, "text": "20:00-05:00"}
                    changed.append(True)
            return result

        monkeypatch.setattr(e.services.message_settings, "context", context)
        from app.telegram_bots.callbacks import CallbackRefused

        with pytest.raises(CallbackRefused):
            await owner.handle_text(e.channel, message(e, 1680, content="22:30-06:30"), captured_link=e.link)
        assert changed
        assert (await db.settings.get(e.live.id)).quiet_start_minute == 1260
    finally:
        await e.sender.stop()
        await e.channel.stop()


@pytest.mark.asyncio
async def test_same_sendnew_can_resume_committed_handoff(db, monkeypatch):
    from types import SimpleNamespace

    e = await setup(db, monkeypatch)
    try:
        owner = await hours(e, db)
        e.request.enqueue("sendMessage", result=sent(e, identity=502))
        await drain(e)
        await e.services.prompts.close(e.link, e.link.chat_id, None, reason="cancelled")
        assert await owner.handle_text(e.channel, message(e, 1685, content="21:00-06:00"), captured_link=e.link)
        e.request.enqueue("sendMessage", result=sent(e, identity=503))
        await drain(e)
        async with db.sessions() as reader:
            row = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt_send_new"))
        calls = []

        async def interrupted(channel, stage):
            calls.append(stage)
            if len(calls) == 1:
                raise RuntimeError("interrupted before ordinary admission")

        e.services.turns = SimpleNamespace(accept_staged=interrupted, message_sent=lambda *args, **kwargs: None)
        with pytest.raises(RuntimeError, match="before ordinary"):
            await owner.execute_callback(e.channel, row, query_id="first-sendnew", update_id=1686)
        assert await owner.execute_callback(e.channel, row, query_id="same-sendnew-again", update_id=1687)
        assert len(calls) == 2 and calls[0].callback_id == calls[1].callback_id and calls[0].text == calls[1].text
    finally:
        e.services.turns = None
        await e.sender.stop()
        await e.channel.stop()
