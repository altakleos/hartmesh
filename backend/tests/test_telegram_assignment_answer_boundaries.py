"""Actual acknowledged choice expiry, profile and explicit Retry custody."""

import pytest
from sqlalchemy import select
from test_telegram_assignment_proposal_cards import db as db
from test_telegram_assignment_proposal_cards import proposal_cards as proposal_cards
from test_telegram_assignment_proposal_cards import work_delivery as work_delivery
from test_telegram_assignment_proposal_cards import work_pages as work_pages
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch

from app.telegram_bots import assignment_answers as module
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY
from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow


@pytest.mark.asyncio
async def test_final_clock_expiry_rolls_back_answer_admission(proposal_cards, monkeypatch):
    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7541)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_answer")
    original = module.prepare_attention
    entered = False

    async def prepare(*args, **kwargs):
        admission = await original(*args, **kwargs)

        async def held(*args, **kwargs):
            nonlocal entered
            value = await admission(*args, **kwargs)
            projection = kwargs.get("projection")
            if isinstance(projection, module._AnswerFrame) and projection.saved.state == "refused":
                entered = True
                e.clock.advance((e.sender.outbox.config.callbacks.draft_ttl_hours + 1) * 3600)
            return value

        return held

    monkeypatch.setattr(module, "prepare_attention", prepare)
    with pytest.raises((CallbackRefused, ProductSourceRetired, ValueError)):
        await module.AssignmentAnswers(e.assignments).stage(e.link, button.id, update_id=8541, chat_id=e.link.chat_id, message_id=7541, message_thread_id=None, guard=e.proposal_guard)
    assert entered
    async with e.db.sessions() as reader:
        root = await reader.get(TelegramActRow, button.target_id)
        control = await reader.get(TelegramCallbackRow, button.id)
        assert root.state == "draft" and root.result is None
        assert control.consumed_at is None and CALLBACK_PUBLICATION_KEY not in control.payload


@pytest.mark.asyncio
async def test_owned_answer_cannot_be_read_through_a_different_callback_profile(proposal_cards):
    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7542)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_answer")
    stage = await module.AssignmentAnswers(e.assignments).stage(e.link, button.id, update_id=8542, chat_id=e.link.chat_id, message_id=7542, message_thread_id=None, guard=e.proposal_guard)
    assert await e.sender.outbox.inbox.read_submission(e.link, stage, guard=e.proposal_guard) == stage
    async with e.db.sessions.begin() as writer:
        control = await writer.get(TelegramCallbackRow, button.id)
        control.kind = "clarify_option"
    with pytest.raises((ValueError, CallbackRefused, ProductSourceRetired)):
        await e.sender.outbox.inbox.read_submission(e.link, stage, guard=e.proposal_guard)


@pytest.mark.asyncio
async def test_explicit_retry_preserves_just_answer_document(proposal_cards):
    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7543)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_answer")
    stage = await module.AssignmentAnswers(e.assignments).stage(e.link, button.id, update_id=8543, chat_id=e.link.chat_id, message_id=7543, message_thread_id=None, guard=e.proposal_guard)
    box, callbacks = e.sender.outbox, e.channel._services.callbacks
    outcome = await box.plan_input_outcome(
        e.link,
        source_id="answer-queue-full:" + button.id,
        source_ref={"key": "busy.followup_full", "values": {}},
        keyboard=[[{"label": "label.try_again", "kind": "retry_message", "target_kind": "request", "target_id": button.id}]],
        callbacks=callbacks,
        guard=e.proposal_guard,
        input_owner_callback_id=button.id,
    )
    async with e.db.sessions() as reader:
        retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message", TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == outcome))
        retry_id, group_id = retry.id, retry.group_id
    assert await callbacks.adopt(e.link, group_id, 7544, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=outcome, presentation_version=0, check_current=lambda: True)
    retried = await box.inbox.stage_callback_input(e.link, callback_id=retry_id, update_id=8544, chat_id=e.link.chat_id, message_id=7544, message_thread_id=None, callbacks=callbacks, guard=e.proposal_guard)
    assert retried.text == stage.text and retried.human_message_kwargs == stage.human_message_kwargs
    assert retried.thread_id == stage.thread_id
    assert retried.callback_id == retry_id


@pytest.mark.asyncio
async def test_proposal_source_holds_a_second_parent_reference(proposal_cards):
    from copy import deepcopy
    from uuid import uuid4

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    button = await selected(e, "assign_answer")
    async with e.db.sessions.begin() as writer:
        original = await writer.get(TelegramOutboxRow, e.identity)
        values = {column.key: deepcopy(getattr(original, column.key)) for column in TelegramOutboxRow.__mapper__.column_attrs}
        values.update(id=uuid4().hex, dedupe_key="notice:assignment:" + button.target_id, state="skipped")
        writer.add(TelegramOutboxRow(**values))
    await dispatch(e, 7545)
    assert not [call for call in e.channel.bot.request.calls if call.endpoint == "sendMessage"], "ambiguous family must remain held before SDK delivery"
