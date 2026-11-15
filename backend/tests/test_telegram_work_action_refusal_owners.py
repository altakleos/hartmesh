"""Damaged assignment controls cannot bypass an actual saved draft owner."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_assignment_card_delivery import prepare_card, save_card
from test_telegram_notification_runtime import runtime
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.callbacks import _row
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["selected", "all-controls", "missing-journal"])
async def test_lifecycle_does_not_bypass_assignment_family(work_delivery, monkeypatch, damage):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await dispatch(e, 6760)
    save, admitted = await save_card(e, plans)
    start = await selected(e, "assign_start")
    async with e.db.sessions.begin() as writer:
        controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == start.group_id)))
        for control in controls:
            if control.id == start.id or damage != "selected":
                control.payload = {key: value for key, value in control.payload.items() if key != "work_action_id"}
        if damage == "missing-journal":
            await writer.delete(await writer.get(TelegramActRow, admitted.id))
        (await writer.get(AgentInstanceRow, e.link.agent_ref)).status = "suspended"
    owner = await runtime(e, preserve=True)
    calls = []

    async def notice(link):
        calls.append(link.id)

    monkeypatch.setattr(owner.host, "notice_instance_unavailable", notice, raising=False)
    try:
        async with e.db.sessions() as reader:
            control = await reader.get(TelegramCallbackRow, start.id)
            root = await reader.get(TelegramActRow, draft.id)
            assert root.operation_ids == [save.id] and root.continuation["assignment_admission"]
            captured = _row(control)
        assert await owner.host.work_chat._unadmitted_lifecycle(e.link, captured) is False
        assert calls == []
    finally:
        await owner.stop()
