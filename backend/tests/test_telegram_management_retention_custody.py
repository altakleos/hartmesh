from datetime import timedelta

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_management_delivery import open_card, prepare_management
from test_telegram_work_management_navigation import publish_edit
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.retention import sweep_rows
from app.telegram_bots.work_actions import _ACT_SCOPE
from app.telegram_bots.work_stop_retries import child_identity
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_outer", "unrecognizable_parent", "foreign_child"])
async def test_unresolved_independent_witness_keeps_complete_ui_family(work_delivery, damage):
    e = work_delivery
    plans, button = await prepare_management(e, "work_withdraw")
    await open_card(e, plans, button)
    control = await selected(e, "work_withdraw_confirm")
    act = await plans.confirm(e.link, control.id, chat_id=e.link.chat_id, message_id=control.message_id, message_thread_id=None, guard=await guard_for(e))
    if damage == "foreign_child":
        act = await e.acts.check(act.id)
        assert act.state == "done"
        await plans.observe(e.link, e.identity)
        await publish_edit(e)
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(TelegramOutboxRow, e.identity)
        origin = parent.payload["work_management_intent"]["origin"]
        if damage == "foreign_child":
            child = child_identity(act.id, 2)
            writer.add(
                TelegramActRow(
                    id=child,
                    **{**{key: getattr(act, key) for key in _ACT_SCOPE}, "kind": "stop"},
                    body=WorkCommand(operation_id=child, expected_revision=1, expected_assignment_revision=1, action="stop", attempt_id=operation()).model_dump(mode="json", exclude_unset=True),
                    state="pending",
                    operation_ids=[],
                    attempts=0,
                    continuation={"version": 1, "stop_retry": {"parent_id": act.id, "ordinal": 2}},
                    created_at=e.clock.now(),
                    updated_at=e.clock.now(),
                )
            )
        else:
            parent.payload = {key: value for key, value in parent.payload.items() if key != "work_management_intent"}
            if damage == "unrecognizable_parent":
                parent.payload = {key: value for key, value in parent.payload.items() if key != "work_news_basis"}
    later = e.clock.now() + timedelta(days=401)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, act.id) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        assert await reader.get(TelegramCallbackRow, control.id) is not None
        assert await reader.get(TelegramOutboxRow, origin["anchor_outbox_id"]) is not None
        assert await reader.get(TelegramCallbackRow, origin["gesture_id"]) is not None
        assert await reader.get(TelegramCallbackRow, button.id) is not None
