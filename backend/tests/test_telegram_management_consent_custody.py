"""Damaged command metadata never changes the body a preview authorized."""

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_management_actions import management_signature
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_even_a_resealed_command_must_match_the_original_full_edit_preview(work_delivery):
    e = work_delivery
    plans, button = await prepare_management(e, "work_change")
    await open_card(e, plans, button)
    save = await selected(e, "work_change_save")
    act = await plans.confirm(e.link, save.id, chat_id=e.link.chat_id, message_id=save.message_id, message_thread_id=None, guard=await guard_for(e))
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, act.id)
        row.body = {**row.body, "objective": "A different unconfirmed objective"}
        source = row.continuation["management_source"]
        row.continuation = {**row.continuation, "management_source": {**source, "signature": management_signature(row, source["scope"], source["origin_kind"], source["draft_ref"])}}
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, e.identity))
    with pytest.raises(ProductSourceRetired):
        await plans.reader.read(e.link, item)


@pytest.mark.asyncio
async def test_old_withdraw_receipt_does_not_claim_reopened_work_is_withdrawn(work_delivery):
    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    confirm = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, confirm.id, chat_id=e.link.chat_id, message_id=confirm.message_id, message_thread_id=None, guard=await guard_for(e))
    done = await e.acts.check(act.id)
    assert done.state == "done"
    await e.human.work.command(
        actor=e.actor,
        instance_id=e.instance.id,
        work_id=e.work["id"],
        request=WorkCommand(operation_id=operation(), action="reopen", note="Continue the review", expected_revision=done.result["revision"], expected_assignment_revision=done.result["assignment_revision"]),
    )
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, e.identity))
    source = await plans.reader.read(e.link, item)
    assert source.work["phase"] == "changed" and source.work["detail"]["record"]["status"] == "open"
