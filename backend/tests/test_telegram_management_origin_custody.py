import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_management_actions import management_signature
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def pending_withdraw(e):
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    control = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
    assert act.state == "pending" and act.attempts == 0
    return plans, control, act


@pytest.mark.asyncio
async def test_missing_outer_reservation_cannot_reopen_pending_withdraw(work_delivery):
    e = work_delivery
    plans, control, act = await pending_withdraw(e)
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        parent.payload = {**parent.payload, "work_management_intent": {**parent.payload["work_management_intent"], "act_id": None}}
        await writer.flush()
        item = _item(parent)
    with pytest.raises(ProductSourceRetired):
        await plans.reader.read(e.link, item)


@pytest.mark.asyncio
async def test_callback_command_cannot_be_downgraded_to_internal_after_control_loss(work_delivery):
    e = work_delivery
    plans, control, act = await pending_withdraw(e)
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, act.id)
        source = {**row.continuation["management_source"], "origin_kind": "internal"}
        source["signature"] = management_signature(_snapshot(row), source["scope"], "internal", source["draft_ref"])
        row.continuation = {"management_source": source}
        await writer.delete(await writer.get(TelegramCallbackRow, act.id))
    result = await e.acts.check(act.id)
    assert result.state != "done"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == act.id)) == 0
