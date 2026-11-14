import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import ProductSourceRetired, _item
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_root_admission_witness_prevents_fresh_save_after_journal_and_marker_loss(work_delivery):
    e = work_delivery
    plans, button = await prepare_management(e, "work_change")
    await open_card(e, plans, button)
    control = await selected(e, "work_change_save")
    act = await plans.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        root = await writer.get(TelegramActRow, control.target_id)
        assert root.continuation["edit_admission"]["act_id"] == act.id
        root.operation_ids = []
        await writer.delete(await writer.get(TelegramActRow, act.id))
        for member in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == control.group_id)):
            member.payload = {key: value for key, value in member.payload.items() if key != "work_action_id"}
        parent = await writer.get(TelegramOutboxRow, e.identity)
        parent.payload = {**parent.payload, "work_management_intent": {**parent.payload["work_management_intent"], "act_id": None}}
        await writer.flush()
        item = _item(parent)
    with pytest.raises(ProductSourceRetired):
        await plans.reader.read(e.link, item)
