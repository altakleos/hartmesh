"""Private management delivery rechecks authority and reservations after awaits."""

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_interaction_delivery import guard_for
from test_telegram_work_management_delivery import prepare_management
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["grant", "generation", "marker"])
async def test_management_private_sdk_send_rechecks_final_authority_and_reservation(work_delivery, monkeypatch, drift):
    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    identity = await plans.open(e.link, button.id, query_id="late-sdk", chat_id=e.link.chat_id, message_id=button.message_id, message_thread_id=None, guard=await guard_for(e))
    sources = e.channel._services.work_management_sources
    get = sources.settings.get
    changed = []

    async def move(*args, **kwargs):
        settings = await get(*args, **kwargs)
        if not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                if drift == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.link.owner_user_id))).permissions = 3
                elif drift == "generation":
                    (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation += 1
                else:
                    parent = await writer.get(TelegramOutboxRow, identity)
                    group = parent.parts[-1]["callback_group_id"]
                    rows = await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group))
                    for row in rows:
                        if row.kind == "work_withdraw_confirm":
                            row.payload = {**row.payload, "work_action_id": operation()}
        return settings

    monkeypatch.setattr(sources.settings, "get", move)
    before = len([call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}])
    e.clock.advance(10)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    after = len([call for call in e.channel.bot.request.calls if call.endpoint in {"sendMessage", "editMessageText"}])
    assert changed and after == before
