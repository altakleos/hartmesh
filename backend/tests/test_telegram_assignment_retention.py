"""Draft expiry and exact recovery dependencies survive ordinary history sweeps."""

from datetime import timedelta
from uuid import uuid4

import pytest
from _telegram_bot_test_support import make_update
from _telegram_repository_test_support import db as db
from sqlalchemy import event, func, select
from test_telegram_assignment_card_delivery import prepare_card, save_card
from test_telegram_assignment_card_recovery import publish_saving
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assign import DraftAssignment
from app.telegram_bots.assignment_drafts import AssignmentDrafts
from app.telegram_bots.retention import sweep_rows
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_unadmitted_private_draft_expires_without_a_service_attempt(work_delivery):
    e = work_delivery
    _plans, draft, _inbox = await prepare_card(e)
    await sweep_rows(e.db.sessions, e.clock.now() + timedelta(hours=25), callback_max_age_days=180, draft_ttl_hours=24)
    async with e.db.sessions() as reader:
        row = await reader.get(TelegramActRow, draft.id)
        assert row.state == "refused" and row.reason == "draft-expired" and row.attempts == 0
        assert await reader.scalar(select(func.count()).select_from(AgentWorkRow)) == 1


@pytest.mark.asyncio
async def test_uncertain_save_keeps_original_input_full_ack_and_journals(work_delivery):
    e = work_delivery
    plans, draft, inbox = await prepare_card(e)
    await dispatch(e, 5001)
    button, admitted = await save_card(e, plans)
    now = e.clock.now()
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
        mark.state, mark.closed_at = "done", now
        parent = await writer.get(TelegramOutboxRow, e.identity)
        parent.closed_at = now
        journal = await writer.get(TelegramActRow, admitted.id)
        journal.state, journal.attempts = "uncertain", 1
    later = now + timedelta(days=500)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id)) is not None
        assert await reader.get(TelegramCallbackRow, button.id) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        assert await reader.get(TelegramActRow, admitted.id) is not None
        assert await reader.get(TelegramActRow, draft.id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("days", [10, 401])
async def test_saved_live_controls_keep_inputs_then_settled_history_expires(work_delivery, days):
    e = work_delivery
    plans, draft, inbox = await prepare_card(e)
    await dispatch(e, 5002)
    button, admitted = await save_card(e, plans)
    assert (await e.acts.check(admitted.id)).state == "done"
    await publish_saving(e, plans, draft, 5002)
    later = e.clock.now() + timedelta(days=days)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        mark = await reader.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
        root = await reader.get(TelegramActRow, draft.id)
        journal = await reader.get(TelegramActRow, admitted.id)
        if days == 10:
            assert mark is not None and root is not None and journal is not None
            assert await reader.get(TelegramCallbackRow, button.id) is not None
        else:
            assert mark is None and root is None and journal is None


@pytest.mark.asyncio
async def test_large_history_uses_bounded_deletes_without_losing_an_owned_input(work_delivery):
    e = work_delivery
    plans, _draft, inbox = await prepare_card(e)
    await dispatch(e, 5003)
    _button, admitted = await save_card(e, plans)
    now = e.clock.now()
    async with e.db.sessions.begin() as writer:
        mark = await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
        mark.state, mark.closed_at = "done", now
        (await writer.get(TelegramActRow, admitted.id)).state = "uncertain"
        writer.add_all(
            TelegramInboundMarkRow(
                link_id=e.link.id,
                message_id=9000 + index,
                update_id=9000 + index,
                chat_id=e.link.chat_id,
                binding_generation=e.link.binding_generation,
                accepted_at=now,
                sanitized_input={"text": "Retired input"},
                state="done",
                route_complete=True,
                closed_at=now,
            )
            for index in range(1100)
        )
    sizes = []

    def record(_connection, _cursor, statement, parameters, _context, _many):
        if statement.lstrip().upper().startswith("DELETE FROM telegram_inbound_marks".upper()):
            sizes.append(len(parameters))

    event.listen(e.db.engine.sync_engine, "before_cursor_execute", record)
    try:
        await sweep_rows(e.db.sessions, now + timedelta(days=500), callback_max_age_days=180)
    finally:
        event.remove(e.db.engine.sync_engine, "before_cursor_execute", record)
    assert len(sizes) >= 3 and max(sizes) <= 1500
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id)) is not None
        assert await reader.scalar(select(func.count()).select_from(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id >= 9000)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("transition", ["unbind", "remove"])
async def test_completed_history_expires_after_binding_retirement(work_delivery, transition):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await dispatch(e, 5010)
    _button, admitted = await save_card(e, plans)
    assert (await e.acts.check(admitted.id)).state == "done"
    await publish_saving(e, plans, draft, 5010)
    if transition == "unbind":
        await e.db.links.unbind(e.link, actor_id=e.link.owner_user_id)
    else:
        await e.db.links.remove(e.link.id, actor_id=e.link.owner_user_id)
    later = e.clock.now() + timedelta(days=401)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, draft.id) is None
        assert await reader.get(TelegramActRow, admitted.id) is None


@pytest.mark.asyncio
async def test_malformed_uncertain_root_keeps_original_evidence(work_delivery):
    e = work_delivery
    plans, draft, inbox = await prepare_card(e)
    await dispatch(e, 5011)
    button, admitted = await save_card(e, plans)
    now = e.clock.now()
    async with e.db.sessions.begin() as writer:
        root = await writer.get(TelegramActRow, draft.id)
        root.body = {**root.body, "unrecognized_field": True}
        journal = await writer.get(TelegramActRow, admitted.id)
        journal.state, journal.attempts = "uncertain", 1
        mark = await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
        mark.state, mark.closed_at = "done", now
        (await writer.get(TelegramOutboxRow, e.identity)).closed_at = now
    later = now + timedelta(days=500)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, admitted.id) is not None
        assert await reader.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id)) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        assert await reader.get(TelegramCallbackRow, button.id) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["missing_forward", "shared_child"])
async def test_damaged_child_references_keep_both_complete_families(work_delivery, damage):
    e = work_delivery
    plans, draft, _inbox = await prepare_card(e, due_at=e.sender.outbox.clock() + timedelta(days=1), due_text="the original deadline")
    await dispatch(e, 5012)
    _button, admitted = await save_card(e, plans)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 3
    saved = await e.acts.check(admitted.id)
    assert saved.state == "done" and len(saved.operation_ids) == 1
    child_id = saved.operation_ids[0]
    if damage == "shared_child":
        mark, _ = await e.sender.outbox.inbox.accept(e.link, make_update(5013, account=e.link.chat_id, text="/assign Independent work"), sanitized_input={"command": "assign", "text": "Independent work"})
        other = await AssignmentDrafts(e.acts).create(e.link, mark, draft=DraftAssignment(objective="Independent work", success_criteria="Keep its own evidence"), source="command")
        other_parent = await e.acts.stage_delegate(e.link, other.id, operation_id=uuid4().hex)
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramActRow, other_parent.id)
            row.operation_ids, row.state, row.attempts = [child_id], "uncertain", 1
    else:
        async with e.db.sessions.begin() as writer:
            row = await writer.get(TelegramActRow, admitted.id)
            row.operation_ids, row.state, row.reason, row.result = [], "refused", "manage-required", None
    later = e.clock.now() + timedelta(days=500)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        for identity in (draft.id, admitted.id, child_id):
            assert await reader.get(TelegramActRow, identity) is not None
