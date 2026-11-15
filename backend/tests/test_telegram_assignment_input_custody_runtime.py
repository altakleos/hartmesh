"""A selected typed assignment keeps its original owner across failed handoff."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_assignment_entrypoints import installed_prompt
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
async def test_selected_prompt_keeps_original_custody_when_answer_fails_before_claim(work_delivery, monkeypatch, legacy):
    e = work_delivery
    owner, calls = await installed_prompt(e, monkeypatch)
    ordinary = []

    async def accept(channel, update, **kwargs):
        ordinary.append(update.message.text)
        channel._classified(update, remember=True)

    owner.host.turns = SimpleNamespace(accept=accept, retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    update = make_update(6231, account=e.link.chat_id, text="Review the handbook")
    if legacy:
        await owner.host.outbox.inbox.accept(e.link, update, sanitized_input={"text": update.message.text})

    async def unavailable(link, origin, **kwargs):
        assert kwargs.get("prompt_id") is not None
        raise RuntimeError("selected-answer-unavailable")

    try:
        with monkeypatch.context() as fault:
            fault.setattr(owner.host.work_chat.assignment_inputs, "answer", unavailable)
            with pytest.raises(RuntimeError, match="selected-answer-unavailable"):
                await e.channel.handle_update(update)
        async with e.db.sessions.begin() as writer:
            prompt = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "prompt", TelegramCallbackRow.message_id == 6132))
            prompt.consumed_at, prompt.closed_reason = e.clock.now(), "command"
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        assert ordinary == [] and calls == []
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6231, e.link.chat_id))
            assert mark.route_kind == "work_card_hold" and not mark.route_complete
            assert mark.sanitized_input["text"] == "Review the handbook"
            assert mark.thread_id is None and mark.turn_id is None and not mark.operation_ids
            assert mark.route_basis["closed_typing"]["prompt_id"] == prompt.id
            notices = [row for row in await reader.scalars(select(TelegramOutboxRow)) if any(part.get("source_ref", {}).get("key") == "prompt.closed" for part in row.parts)]
            assert len(notices) == 1
            notice = notices[0]
            assert mark.route_basis["work_closed"]["notice_id"] == notice.id
            controls = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == notice.parts[-1]["callback_group_id"])))
            assert {control.kind for control in controls} == {"prompt_reopen", "prompt_send_new", "prompt_discard"}
            assert all(control.message_id is None for control in controls)
        for index in range(len(notice.parts)):
            await dispatch(e, 6232 + index)
        assert not e.dispatch_errors, e.dispatch_errors
        assert await e.channel.handle_update(update)
        await e.channel._drain_ingress()
        async with e.db.sessions() as reader:
            mark = await reader.get(TelegramInboundMarkRow, (e.link.id, 6231, e.link.chat_id))
            assert mark.route_basis["work_closed"]["notice_id"] == notice.id and not mark.route_complete
            for control in controls:
                assert (await reader.get(TelegramCallbackRow, control.id)).message_id == 6231 + len(notice.parts)
        assert ordinary == [] and calls == []
    finally:
        await owner.stop()
