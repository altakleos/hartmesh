import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import func, select
from test_telegram_activation import seeded_attempt
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.work_actions import request_for
from app.telegram_bots.work_management_actions import management_signature
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


async def erase_native_mode(e, act, *, erase_outer=False):
    async with e.db.sessions.begin() as writer:
        row = await writer.get(TelegramActRow, act.id)
        source = {**row.continuation["management_source"], "origin_kind": "internal"}
        source["signature"] = management_signature(_snapshot(row), source["scope"], "internal", source["draft_ref"])
        row.continuation = {"management_source": source}
        await writer.delete(await writer.get(TelegramCallbackRow, act.id))
        if erase_outer:
            parent = await writer.get(TelegramOutboxRow, e.identity)
            parent.payload = {**parent.payload, "work_management_intent": {**parent.payload["work_management_intent"], "act_id": None}}
    return await e.acts._work_record(act.id)


@pytest.mark.asyncio
async def test_saved_callback_mode_cannot_bypass_physical_followup_consent(work_delivery):
    e = work_delivery
    e.http_app = e.live.authority.app
    attempt_id, record, manager = await seeded_attempt(e)
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    control = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
    receipt = await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=e.work["id"], request=request_for(act.kind, act.body, identity=act.id), can_stop=True)
    act = await e.acts._management_outcome(act, state="done", result=receipt)
    assert act.stop_dispatch["state"] == "ready"
    act = await erase_native_mode(e, act)
    await e.acts.resume_stop_dispatch(act)
    manager.cancel.assert_not_awaited()


@pytest.mark.asyncio
async def test_edit_root_keeps_callback_mode_when_other_witnesses_are_lost(work_delivery, monkeypatch):
    from app.telegram_bots.work_edit_drafts import WorkEditDrafts

    e = work_delivery
    plans, button = await prepare_management(e, "work_change")
    create = WorkEditDrafts.create

    async def modified(self, link, **kwargs):
        kwargs["draft"] = DraftAssignment(objective="Review the complete manual", success_criteria="Return verified corrections")
        return await create(self, link, **kwargs)

    monkeypatch.setattr(WorkEditDrafts, "create", modified)
    await open_card(e, plans, button)
    control = await selected(e, "work_change_save")
    act = await plans.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
    assert act.state == "pending" and act.attempts == 0
    await erase_native_mode(e, act, erase_outer=True)
    result = await e.acts.check(act.id)
    assert result.state != "done"
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(AgentWorkEventRow).where(AgentWorkEventRow.operation_id == act.id)) == 0
