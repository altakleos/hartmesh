"""Actual SDK reply metadata is admitted without acquiring another prompt target."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from telegram import Chat, Message, Update, User
from test_telegram_human_input_plans import parts
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_submission_publication import submission_environment as submission_environment

from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


def quoted_update(e, message_id, *, quote_id=2001, text="Actual quoted answer"):
    chat = Chat(e.link.chat_id, "private")
    quoted = Message(quote_id, datetime.now(UTC), chat, from_user=User(e.link.bot_id, "Bot", True), text="Type your answer")
    return Update(message_id + 1000, message=Message(message_id, datetime.now(UTC), chat, from_user=User(e.link.telegram_user_id, "Human", False), text=text, reply_to_message=quoted))


@pytest.mark.asyncio
async def test_ordinary_quoted_text_keeps_server_captured_reply_id_and_dispatches(coordinator_environment):
    e = coordinator_environment
    incoming = quoted_update(e, 851)
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    assert row.sanitized_input["reply_to_message_id"] == incoming.message.reply_to_message.message_id
    row = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    record = await e.coordinator.dispatch(e.link, (row,), thread_id=e.thread_id)
    sent = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    assert record.origins[0].message_id == 851 and sent.text == incoming.message.text
    assert sent.metadata["human_message_kwargs"] == {"channel_turn_id": record.publication_id}


@pytest.mark.asyncio
@pytest.mark.parametrize("quote_id", [2001, 2002])
async def test_quoted_other_answer_requires_the_actual_prompt_acknowledgement(submission_environment, quote_id):
    e = submission_environment
    step = await e.outbox.plan_human_input_step(e.parent, e.qualified, step_id="quoted-other-prompt", parts=parts(e.parent, "Type your answer", force_reply=True), keyboard=[], callbacks=e.callbacks, guard=e.guard)
    async with e.db.sessions.begin() as writer:
        saved = await writer.get(TelegramOutboxRow, step)
        changed = deepcopy(saved.parts)
        # Explicit protocol ACK seed; the full SDK transport tier owns the raw send.
        changed[0].update(state="sent", message_ids=[2001])
        saved.parts = changed
    prompt = await e.prompts.open(e.link, "clarify_other", {"thread_id": e.thread_id, "request_id": e.response["request_id"]}, chat_id=e.link.chat_id, message_thread_id=None, message_id=None)
    async with e.callbacks.writer(e.link, e.guard) as writer:
        await e.prompts.attach(e.link, prompt, outbox_id=step, guard=e.guard, session=writer)
    incoming = quoted_update(e, 861, quote_id=quote_id)
    origin, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    origin = await e.inbox.route(origin, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    response = {key: value for key, value in e.response.items() if key != "option_id"}
    response.update(response_kind="text", value=incoming.message.text)

    async def stage():
        return await e.inbox.stage_submission(e.link, source_outbox_id=step, response=response, origin=origin, prompt_id=prompt, guard=e.guard)

    if quote_id != 2001:
        with pytest.raises(ValueError):
            await stage()
        return
    typed = await stage()
    assert typed.origins[0].sanitized_input["reply_to_message_id"] == 2001
    grouped = await e.inbox.group_prepublication(typed.origins, guard=e.guard)
    record = await e.inbox.reserve_publication(e.link, grouped, thread_id=typed.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard)
    await e.coordinator.dispatch_record(record, e.link)
    sent = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    assert sent.text == incoming.message.text and sent.metadata["human_message_kwargs"]["human_input_response"] == response
