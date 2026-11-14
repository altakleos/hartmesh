"""A selected typed assignment keeps its original owner across failed handoff."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_assignment_entrypoints import installed_prompt
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_selected_prompt_keeps_original_custody_when_answer_fails_before_claim(work_delivery, monkeypatch):
    e = work_delivery
    owner, calls = await installed_prompt(e, monkeypatch)
    ordinary = []

    async def accept(channel, update, **kwargs):
        ordinary.append(update.message.text)
        channel._classified(update, remember=True)

    owner.host.turns = SimpleNamespace(accept=accept, retire=AsyncMock(), message_sent=Mock(), typing_allowed=lambda *args, **kwargs: False)
    update = make_update(6231, account=e.link.chat_id, text="Review the handbook")

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
            assert mark.route_kind == "assignment_notice" and mark.route_complete
            notices = [row for row in await reader.scalars(select(TelegramOutboxRow)) if any(part.get("source_ref", {}).get("key") == "prompt.closed" for part in row.parts)]
            assert len(notices) == 1
    finally:
        await owner.stop()
