"""Grouped original inputs remain pinned while a proposal family is unresolved."""

from copy import deepcopy

import pytest
from sqlalchemy import select
from test_telegram_assignment_proposal_cards import db as db
from test_telegram_assignment_proposal_cards import proposal_cards as proposal_cards
from test_telegram_assignment_proposal_cards import work_delivery as work_delivery
from test_telegram_assignment_proposal_cards import work_pages as work_pages
from test_telegram_bot_adapter import message
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch

from app.telegram_bots.assignment_answers import AssignmentAnswers
from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY
from app.telegram_bots.retention import sweep_rows
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


@pytest.mark.asyncio
@pytest.mark.parametrize("proposal_cards", [{"grouped": True}], indirect=True)
async def test_unknown_proposal_family_keeps_every_original_input(proposal_cards):
    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7601)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_save")
    async with e.db.sessions.begin() as writer:
        root = await writer.get(TelegramActRow, button.target_id)
        root.operation_ids = ["f" * 32]
    e.clock.advance(401 * 86400)
    await e.channel._services.callbacks.sweep(e.clock.now())
    await sweep_rows(e.db.sessions, e.clock.now(), callback_max_age_days=e.channel._services.callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader:
        assert await reader.get(TelegramActRow, button.target_id) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        for origin in e.proposal_inputs:
            assert await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id)) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("proposal_cards", [{"grouped": True}], indirect=True)
@pytest.mark.parametrize("damage", [None, "version", "missing"])
async def test_pending_answer_retry_keeps_original_consent_family(proposal_cards, damage):
    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7611)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_answer")
    original = await AssignmentAnswers(e.assignments).stage(e.link, button.id, update_id=8611, chat_id=e.link.chat_id, message_id=7611, message_thread_id=None, guard=e.proposal_guard)
    e.clock.advance(10)
    message(e.channel.bot.request, endpoint="editMessageText", identity=7611)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
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
    await dispatch(e, 7613)
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions() as reader:
        retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message", TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == outcome))
        retry_id = retry.id
        assert retry.message_id == 7613
    stage = await box.inbox.stage_callback_input(e.link, callback_id=retry_id, update_id=8613, chat_id=e.link.chat_id, message_id=7613, message_thread_id=None, callbacks=callbacks, guard=e.proposal_guard)
    assert stage.human_message_kwargs == original.human_message_kwargs
    if damage:
        async with e.db.sessions.begin() as writer:
            control = await writer.get(TelegramCallbackRow, retry_id)
            payload = deepcopy(control.payload)
            if damage == "version":
                payload[CALLBACK_PUBLICATION_KEY]["submission"]["version"] = 3
            else:
                payload[CALLBACK_PUBLICATION_KEY].pop("submission")
            control.payload = payload
    e.clock.advance(401 * 86400)
    await callbacks.sweep(e.clock.now())
    await sweep_rows(e.db.sessions, e.clock.now(), callback_max_age_days=callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader, reader.begin():
        if damage:
            with pytest.raises(ValueError):
                await box.inbox.lock_callback_input(e.link, retry_id, session=reader)
        else:
            assert await box.inbox.lock_callback_input(e.link, retry_id, session=reader) == stage
        assert await reader.get(TelegramActRow, button.target_id) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        assert await reader.get(TelegramCallbackRow, button.id) is not None
        for origin in e.proposal_inputs:
            assert await reader.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id)) is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["version", "missing", "profile"])
async def test_unknown_answer_document_keeps_original_consent(proposal_cards, damage):
    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7641)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_answer")
    await AssignmentAnswers(e.assignments).stage(e.link, button.id, update_id=8641, chat_id=e.link.chat_id, message_id=7641, message_thread_id=None, guard=e.proposal_guard)
    e.clock.advance(10)
    message(e.channel.bot.request, endpoint="editMessageText", identity=7641)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    async with e.db.sessions.begin() as writer:
        control = await writer.get(TelegramCallbackRow, button.id)
        payload = deepcopy(control.payload)
        if damage == "version":
            payload[CALLBACK_PUBLICATION_KEY]["submission"]["version"] = 3
        elif damage == "missing":
            payload[CALLBACK_PUBLICATION_KEY].pop("submission")
        else:
            control.kind = "clarify_option"
        control.payload = payload
    e.clock.advance(401 * 86400)
    callbacks = e.channel._services.callbacks
    await callbacks.sweep(e.clock.now())
    await sweep_rows(e.db.sessions, e.clock.now(), callback_max_age_days=callbacks.config.callbacks.max_age_days)
    async with e.db.sessions() as reader, reader.begin():
        assert await reader.get(TelegramCallbackRow, button.id) is not None
        assert await reader.get(TelegramActRow, button.target_id) is not None
        assert await reader.get(TelegramOutboxRow, e.identity) is not None
        with pytest.raises(ValueError):
            await e.sender.outbox.inbox.lock_callback_input(e.link, button.id, session=reader)


@pytest.mark.asyncio
async def test_raw_answer_reference_does_not_pin_another_binding(proposal_cards):
    from uuid import uuid4

    from sqlalchemy import select

    from app.telegram_bots.assignment_answer_retention import answer_dependencies
    from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7642)
    assert not e.dispatch_errors, e.dispatch_errors
    button = await selected(e, "assign_answer")
    await AssignmentAnswers(e.assignments).stage(e.link, button.id, update_id=8642, chat_id=e.link.chat_id, message_id=7642, message_thread_id=None, guard=e.proposal_guard)
    foreign_id = uuid4().hex
    async with e.db.sessions.begin() as writer:
        original = await writer.get(TelegramOutboxRow, e.identity)
        values = {column.key: deepcopy(getattr(original, column.key)) for column in TelegramOutboxRow.__mapper__.column_attrs}
        values.update(id=foreign_id, dedupe_key="other-binding-ack:" + foreign_id, binding_generation=e.link.binding_generation + 1, state="sent")
        writer.add(TelegramOutboxRow(**values))
        control = await writer.get(TelegramCallbackRow, button.id)
        payload = deepcopy(control.payload)
        payload[CALLBACK_PUBLICATION_KEY]["submission"]["source_outbox_id"] = foreign_id
        control.payload = payload
    async with e.db.sessions() as reader:
        controls = list(await reader.scalars(select(TelegramCallbackRow)))
        links = {row.id: row for row in await reader.scalars(select(TelegramBotLinkRow))}
        groups, anchors = await answer_dependencies(reader, controls, links, e.clock.now(), max_age_days=e.channel._services.callbacks.config.callbacks.max_age_days)
        assert foreign_id not in anchors
