"""Raw same-scope delivery pins retain custody; they grant no source authority."""

from datetime import timedelta
from uuid import uuid4

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_assignment_card_delivery import prepare_card
from test_telegram_work_interaction_delivery import guard_for, selected
from test_telegram_work_preview_recovery import dispatch
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_telegram_work_start import prepare_startable
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.assignment_controls import AssignmentAdmissions
from app.telegram_bots.assignment_retention import assignment_dependencies
from app.telegram_bots.retention import sweep_rows
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
async def test_proved_preparation_refusal_can_close_old_family(work_delivery, monkeypatch):
    from test_agent_work_records import command

    e = work_delivery
    plans, draft, _inbox = await prepare_card(e)
    await prepare_startable(e, monkeypatch)
    await dispatch(e, 7849)
    button = await selected(e, "assign_start")
    parent = await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=7849, message_thread_id=None, guard=await guard_for(e))
    parent = await e.acts.check(parent.id)
    work_id = parent.result["id"]
    record = await e.human.work.get(actor=e.actor, instance_id=e.instance.id, work_id=work_id)
    await e.human.work.command(actor=e.actor, instance_id=e.instance.id, work_id=work_id, request=command(record, "edit", objective="Changed assignment"))
    ended = await e.acts.resume_continuation(parent.id)
    assert ended.continuation["activation_continuation"]["phase"] == "refused"
    async with e.db.sessions() as reader:
        dependencies = await assignment_dependencies(reader, e.clock.now() + timedelta(days=401), max_age_days=180)
    assert {draft.id, parent.id} <= dependencies.expired_history


@pytest.mark.asyncio
async def test_settled_activation_closure_accepts_its_readonly_snapshot(work_delivery, monkeypatch):
    from app.telegram_bots.acts import _snapshot
    from app.telegram_bots.work_start_retention import closed_activation

    e = work_delivery
    plans, _draft, _inbox = await prepare_card(e)
    await prepare_startable(e, monkeypatch)
    await dispatch(e, 7850)
    button = await selected(e, "assign_start")
    parent = await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=7850, message_thread_id=None, guard=await guard_for(e))
    await e.acts.check(parent.id)
    completed = await e.acts.resume_continuation(parent.id)
    child_id = completed.continuation["activation_continuation"]["child_id"]
    async with e.db.sessions.begin() as writer:
        attempt = await writer.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id))
        attempt.status, attempt.settled_at = "cancelled", e.clock.now()
    async with e.db.sessions() as reader:
        child = await reader.get(TelegramActRow, child_id)
        attempt = await reader.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id))
        assert closed_activation(child, e.link, attempt, work_id=child.work_id)
        assert closed_activation(_snapshot(child), e.link, attempt, work_id=child.work_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("same_binding", [True, False])
async def test_raw_child_delivery_pin_keeps_complete_original_assignment_family(work_delivery, monkeypatch, same_binding):

    e = work_delivery
    plans, draft, inbox = await prepare_card(e)
    await prepare_startable(e, monkeypatch)
    await dispatch(e, 7851)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_start")
    parent = await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=7851, message_thread_id=None, guard=await guard_for(e))
    parent = await e.acts.check(parent.id)
    completed = await e.acts.resume_continuation(parent.id)
    assert completed.continuation["activation_continuation"]["phase"] == "done"
    child_id = completed.continuation["activation_continuation"]["child_id"]
    child = await e.acts._work_record(child_id)
    notice = uuid4().hex
    # Controlled raw locator only: composite notification-source integration
    # is qualified separately. An unsettled, same-scope dependency cannot
    # authorize delivery, but must preserve the complete existing family.
    async with e.db.sessions.begin() as writer:
        now = e.clock.now()
        # Controlled authoritative settlement isolates delivery custody from
        # the independently retained runless admission promise in this fixture.
        attempt = await writer.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child.id))
        attempt.status, attempt.settled_at = "cancelled", now
        generation = e.link.binding_generation if same_binding else e.link.binding_generation + 1
        if not same_binding:
            (await writer.get(TelegramBotLinkRow, e.link.id)).binding_generation = generation
        writer.add(
            TelegramOutboxRow(
                id=notice,
                link_id=e.link.id,
                owner_user_id=e.link.owner_user_id,
                binding_generation=generation,
                chat_id=e.link.chat_id,
                instance_id=e.link.agent_ref,
                work_id=child.work_id,
                dedupe_key="retained-child:" + child.id,
                class_="reply",
                payload={"work_execution_intent": {"version": 1, "act_id": child.id, "origin": dict(completed.continuation["delegate_origin"])}},
                parts=[],
                state="pending",
                silent=True,
                not_before=now,
                created_at=now,
            )
        )
        mark = await writer.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id))
        mark.state, mark.closed_at = "done", e.clock.now()
        (await writer.get(TelegramOutboxRow, e.identity)).closed_at = e.clock.now()
    later = e.clock.now() + timedelta(days=401)
    await plans.callbacks.sweep(later)
    await sweep_rows(e.db.sessions, later, callback_max_age_days=180)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramOutboxRow, notice) is not None
        assert (await reader.get(TelegramActRow, child_id) is not None) == same_binding
        assert (await reader.get(TelegramActRow, parent.id) is not None) == same_binding
        assert (await reader.get(TelegramActRow, draft.id) is not None) == same_binding
        if same_binding:
            assert await reader.get(TelegramOutboxRow, e.identity) is not None
            assert await reader.get(TelegramInboundMarkRow, (inbox.link_id, inbox.message_id, inbox.chat_id)) is not None
        # A separately pending keyboard cleanup may still retain old message
        # locators after a binding change; it cannot keep this closed Act family.


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["child-marker", "parent-forward"])
async def test_malformed_child_lineage_cannot_close_original_family(work_delivery, monkeypatch, damage):
    e = work_delivery
    plans, root, _inbox = await prepare_card(e)
    await prepare_startable(e, monkeypatch)
    await dispatch(e, 7881)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_start")
    parent = await AssignmentAdmissions(plans, e.acts).stage(e.link, button.id, chat_id=e.link.chat_id, message_id=7881, message_thread_id=None, guard=await guard_for(e))
    await e.acts.check(parent.id)
    completed = await e.acts.resume_continuation(parent.id)
    child_id = completed.continuation["activation_continuation"]["child_id"]
    async with e.db.sessions.begin() as writer:
        attempt = await writer.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.activation_id == child_id))
        attempt.status, attempt.settled_at = "cancelled", e.clock.now()
    later = e.clock.now() + timedelta(days=401)
    async with e.db.sessions() as reader:
        known = await assignment_dependencies(reader, later, max_age_days=180)
        assert {root.id, parent.id, child_id} <= known.expired_history
    async with e.db.sessions.begin() as writer:
        if damage == "child-marker":
            child = await writer.get(TelegramActRow, child_id)
            child.continuation = None
        else:
            (await writer.get(TelegramActRow, parent.id)).operation_ids = []
    async with e.db.sessions() as reader:
        held = await assignment_dependencies(reader, later, max_age_days=180)
        assert {root.id, parent.id, child_id} <= held.acts, "damaged linkage is held, not closed history"
        assert not {root.id, parent.id, child_id} & held.expired_history
