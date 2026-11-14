"""Actual dispatch/notice Retry keeps later publication and original consent distinct."""

import pytest
from sqlalchemy import select
from test_telegram_assignment_proposal_cards import db as db
from test_telegram_assignment_proposal_cards import proposal_cards as proposal_cards
from test_telegram_assignment_proposal_cards import work_delivery as work_delivery
from test_telegram_assignment_proposal_cards import work_pages as work_pages
from test_telegram_bot_adapter import message
from test_telegram_work_interaction_delivery import selected
from test_telegram_work_preview_recovery import dispatch

from app.channels.message_bus import MessageBus
from app.telegram_bots.assignment_answers import AssignmentAnswers
from app.telegram_bots.publication import PublicationCoordinator
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow


@pytest.mark.asyncio
async def test_explicit_retry_after_answer_publication_keeps_original_consent(proposal_cards):
    e = proposal_cards
    e.identity = await e.proposals.plan_terminal(e.proposal_record, e.link, guard=e.proposal_guard)
    await dispatch(e, 7671)
    assert not e.dispatch_errors, e.dispatch_errors
    original = await selected(e, "assign_answer")
    stage = await AssignmentAnswers(e.assignments).stage(e.link, original.id, update_id=8671, chat_id=e.link.chat_id, message_id=7671, message_thread_id=None, guard=e.proposal_guard)
    e.clock.advance(10)
    message(e.channel.bot.request, endpoint="editMessageText", identity=7671)
    assert await e.sender.tick() == 1
    await e.sender.drain()
    assert not e.dispatch_errors, e.dispatch_errors
    box, callbacks = e.sender.outbox, e.channel._services.callbacks
    record = await box.inbox.reserve_callback_publication(e.link, original.id, thread_id=stage.thread_id, endpoint_id=e.proposal_http.endpoint_id, guard=e.proposal_guard)
    bus = MessageBus(inbound_queue_maxsize=4)
    coordinator = PublicationCoordinator(inbox=box.inbox, outbox=box, bus=bus, http_owner=e.proposal_http, capture_attempt=e.capture_proposal_attempt)
    try:
        entered = await coordinator.dispatch_record(record, e.link)
        handed_off = bus.get_inbound_nowait()
        bus.inbound_task_done()
        assert entered.dispatch_started and handed_off.text == stage.text
        # A controlled definite no-run outcome exercises the real sealed-publication path.
        outcome = await box.plan_publication_outcome(
            entered,
            no_run=True,
            source_ref={"key": "error.generic", "values": {}},
            keyboard=[[{"label": "label.try_again", "kind": "retry_message", "target_kind": "request", "target_id": original.id}]],
            callbacks=callbacks,
            guard=e.proposal_guard,
        )
        await dispatch(e, 7672)
        assert not e.dispatch_errors, e.dispatch_errors
        async with e.db.sessions() as reader:
            retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message", TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == outcome))
            retry_id = retry.id
            assert retry.message_id == 7672
        retried = await box.inbox.stage_callback_input(e.link, callback_id=retry_id, update_id=8672, chat_id=e.link.chat_id, message_id=7672, message_thread_id=None, callbacks=callbacks, guard=e.proposal_guard)
        assert retried.text == stage.text and retried.human_message_kwargs == stage.human_message_kwargs
        assert retried.thread_id == stage.thread_id and retried.callback_id == retry_id
        assert await box.inbox.read_callback_input(e.link, retry_id, guard=e.proposal_guard) == retried
    finally:
        await coordinator.stop()
        bus.discard_pending_inbound()


@pytest.mark.asyncio
async def test_retry_source_identity_must_remain_its_actual_sealed_parent(proposal_cards):
    from copy import deepcopy
    from hashlib import sha256

    from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY

    e = proposal_cards
    await test_explicit_retry_after_answer_publication_keeps_original_consent(e)
    box = e.sender.outbox
    async with e.db.sessions() as reader:
        retry = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "retry_message"))
        identity, original_payload = retry.id, deepcopy(retry.payload)
    for damage in ("original-consent-parent", "other-publication-event"):
        payload = deepcopy(original_payload)
        owner = payload[CALLBACK_PUBLICATION_KEY]
        if damage == "original-consent-parent":
            owner["source_publication"] = deepcopy(owner["submission"]["parent_publication"])
        else:
            source = owner["source_publication"]
            source["event_id"] += ":foreign"
            source["publication_id"] = sha256((source["turn_id"] + ":" + source["event_id"]).encode()).hexdigest()[:32]
        async with e.db.sessions.begin() as writer:
            (await writer.get(TelegramCallbackRow, identity)).payload = payload
        with pytest.raises(ValueError):
            await box.inbox.read_callback_input(e.link, identity, guard=e.proposal_guard)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramCallbackRow, identity)).payload = original_payload
    assert (await box.inbox.read_callback_input(e.link, identity, guard=e.proposal_guard)).text == original_payload[CALLBACK_PUBLICATION_KEY]["submission"]["text"]
