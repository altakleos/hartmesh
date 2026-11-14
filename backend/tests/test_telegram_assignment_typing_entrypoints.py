"""Installed typing never loses ownership of a retired assignment prompt."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _telegram_bot_test_support import update_payload
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Update
from test_telegram_assignment_entrypoints import installed_prompt
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.callbacks import CallbackRefused
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def reply(e, *, quoted=True):
    raw = update_payload(6151, account=e.link.chat_id, text="Review the handbook")
    if quoted:
        raw["message"]["reply_to_message"] = {"message_id": 6132, "date": 1_893_456_000, "chat": raw["message"]["chat"], "from": {"id": e.link.bot_id, "is_bot": True, "first_name": "Fixture bot"}, "text": "Assignment prompt"}
    return Update.de_json(raw, e.channel.bot)


async def closed(e):
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
        assert mark is not None and mark.route_kind == "assignment_notice" and mark.route_complete
        rows = list(await reader.scalars(select(TelegramOutboxRow)))
        assert len([row for row in rows if any(part.get("source_ref", {}).get("key") == "prompt.closed" for part in row.parts)]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["expired", "missing", "ack"])
async def test_installed_retired_prompt_typing_is_closed_without_ordinary_chat(work_delivery, monkeypatch, damage):
    e = work_delivery
    owner, calls = await installed_prompt(e, monkeypatch)
    owner.host.turns = SimpleNamespace(accept=AsyncMock(), retire=AsyncMock(), typing_allowed=lambda *args, **kwargs: False)
    try:
        if damage == "expired":
            e.clock.advance(601)
        else:
            async with e.db.sessions.begin() as writer:
                prompt = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.message_id == 6132))
                if damage == "missing":
                    await writer.delete(prompt)
                else:
                    parent = await writer.get(TelegramOutboxRow, prompt.payload["anchor_outbox_id"])
                    assert parent.message_id == 6132
                    parent.parts = [{**part, "message_ids": []} for part in parent.parts]
        update = reply(e, quoted=damage != "expired")
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert calls == [] and owner.host.turns.accept.await_count == 0
        await closed(e)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        await closed(e)
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_transient_assignment_prompt_selection_does_not_claim_it_closed(work_delivery, monkeypatch):
    e = work_delivery
    owner, calls = await installed_prompt(e, monkeypatch)
    owner.host.turns = SimpleNamespace(accept=AsyncMock(), retire=AsyncMock(), typing_allowed=lambda *args, **kwargs: False)
    update = reply(e, quoted=False)
    try:
        with monkeypatch.context() as fault:
            fault.setattr(owner.host.work_chat.assignment_prompts.prompts, "current", AsyncMock(side_effect=CallbackRefused))
            with pytest.raises(CallbackRefused):
                await e.channel.handle_update(update)
        assert calls == [] and owner.host.turns.accept.await_count == 0
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6151, e.link.chat_id))
            assert mark.route_kind is None and not mark.route_complete
            assert not any(part.get("source_ref", {}).get("key") == "prompt.closed" for row in await reader.scalars(select(TelegramOutboxRow)) for part in row.parts)
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert calls == ["Review the handbook"] and owner.host.turns.accept.await_count == 0
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_prompt_deleted_between_typed_selection_and_admission_gets_one_closed_notice(work_delivery, monkeypatch):
    e = work_delivery
    owner, calls = await installed_prompt(e, monkeypatch)
    owner.host.turns = SimpleNamespace(accept=AsyncMock(), retire=AsyncMock(), typing_allowed=lambda *args, **kwargs: False)
    inputs = owner.host.work_chat.assignment_inputs
    answer = inputs.answer

    async def retire(link, origin, **kwargs):
        async with e.db.sessions.begin() as writer:
            prompt = await writer.get(TelegramCallbackRow, kwargs["prompt_id"])
            await writer.delete(prompt)
        return await answer(link, origin, **kwargs)

    monkeypatch.setattr(inputs, "answer", retire)
    try:
        assert await e.channel.handle_update(reply(e))
        await e.channel._drain_ingress()
        assert calls == [] and owner.host.turns.accept.await_count == 0
        await closed(e)
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_work_command_closes_only_the_current_shown_assignment_prompt(work_delivery, monkeypatch):
    from unittest.mock import Mock

    from _telegram_bot_test_support import make_update
    from test_telegram_work_preview_recovery import dispatch

    e = work_delivery
    owner, calls = await installed_prompt(e, monkeypatch)
    ordinary = []

    async def accept(channel, update, **kwargs):
        ordinary.append(update.message.text)
        channel._classified(update, remember=True)

    owner.host.turns = SimpleNamespace(accept=accept, retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    try:
        command = make_update(6201, account=e.link.chat_id, text="/work")
        assert await e.channel.handle_update(command)
        await e.channel._drain_ingress()
        assert await e.channel.handle_update(make_update(6202, account=e.link.chat_id, text="A general question"))
        await e.channel._drain_ingress()
        assert calls == [] and ordinary == ["A general question"]
        async with e.db.sessions() as reader:
            prompt = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.message_id == 6132))
            assert prompt.consumed_at is not None and prompt.closed_reason == "command"
        await dispatch(e, 6203)
        assert await e.channel.handle_update(make_update(6204, account=e.link.chat_id, text="/assign"))
        await e.channel._drain_ingress()
        await dispatch(e, 6205)
        assert await e.channel.handle_update(command)
        await e.channel._drain_ingress()
        assert calls == [] and ordinary == ["A general question"]
        async with e.db.sessions() as reader:
            fresh = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.message_id == 6205))
            assert fresh is not None and fresh.consumed_at is None
    finally:
        await owner.stop()
