"""Final native boundaries retain command ownership and its original proof."""

from unittest.mock import AsyncMock

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_activation import seeded_attempt
from test_telegram_work_chat_runtime import installed_work_runtime
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.turns import ChatTurns
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["command", "physical"])
@pytest.mark.parametrize("damage", ["input", "marker"])
async def test_command_stop_rechecks_custody_at_native_boundaries(work_delivery, monkeypatch, phase, damage):
    e = work_delivery
    owner = await installed_work_runtime(e)
    owner.host.turns = ChatTurns(owner.host)
    e.acts = owner.host.acts
    _attempt, _run, manager = await seeded_attempt(e)
    e.clock.advance(1)
    changed = []

    async def drift(identity=None):
        async with e.db.sessions.begin() as writer:
            mark = await writer.get(TelegramInboundMarkRow, (e.link.id, 6331, e.link.chat_id))
            assert mark is not None and mark.route_kind == "command_stop"
            identity = identity or mark.operation_ids[0]
            act = await writer.get(TelegramActRow, identity)
            assert act is not None
            if damage == "input":
                mark.sanitized_input = {"command": "help"}
            else:
                act.continuation = None
            changed.append(identity)

    if phase == "command":
        original = owner.host.acts.check

        async def checking(identity, **kwargs):
            await drift(identity)
            return await original(identity, **kwargs)

        monkeypatch.setattr(owner.host.acts, "check", checking)
    else:
        original = manager.get

        async def reading(*args, **kwargs):
            value = await original(*args, **kwargs)
            await drift()
            return value

        manager.get = AsyncMock(side_effect=reading)
    try:
        try:
            await e.channel.handle_update(make_update(6331, account=e.link.chat_id, text="/stop"))
            await e.channel._drain_ingress()
        except ProductSourceRetired:
            pass  # Damaged custody holds the original delivery too.
        assert changed
        manager.cancel.assert_not_awaited()
        async with e.db.sessions() as reader:
            count = await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == changed[0], AgentWorkEventRow.action == "stop"))
            assert count == (0 if phase == "command" else 1)
            act = await reader.get(TelegramActRow, changed[0])
            if phase == "physical":
                assert act.state == "done" and act.stop_dispatch["state"] in {"ready", "uncertain"}
    finally:
        await owner.host.turns.stop()
        await owner.stop()
