"""Repeated startup guidance follows durable primary plans, never send attempts."""

from types import MappingProxyType

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, update
from test_telegram_bot_handlers import message
from test_telegram_instance_publication import publication as publication
from test_telegram_publication_observation import run_row
from test_telegram_publication_primary import _primary

from app.channels.execution_client import ValidatedRun
from app.gateway.channel_execution import receipt_from_run_kwargs
from app.telegram_bots.callbacks import CallbackRepository
from app.telegram_bots.producer import _item, validate_publication_outcome
from app.telegram_bots.render import render_plain
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

pytestmark = pytest.mark.asyncio


async def plan(e, number, *, reason="start-failed", claimed_hint=None):
    e.channel._services.outbox.clock.__self__.advance(1)
    incoming = message(e, 3000 + number, content="Synthetic startup question")
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    origin = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    record = await e.coordinator.dispatch(e.link, [origin], thread_id=e.thread_id)
    e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http_client.endpoint_id)
    record = await e.inbox.bind_publication_run(record, run_id="startup-" + str(number), guard=attempt.read_guard)
    remote = run_row(record, run_id=record.run_id)
    remote.update(status="success" if reason == "success" else "error", stop_reason=None if reason == "success" else reason, metadata={"channel_turn_id": record.publication_id})
    run = ValidatedRun(MappingProxyType(remote), receipt_from_run_kwargs(remote["kwargs"]))
    source = {"key": "start.ws_failed" if reason == "start-failed" else "answer.on_web" if reason == "success" else "error.generic", "values": {"name": "Fixture employee", "provider": "Example Provider"}}
    if claimed_hint is not None:
        source["values"]["start_failed_again"] = claimed_hint
    args = dict(run=run, source_ref=source, keyboard=[], callbacks=CallbackRepository(e.db.sessions), guard=attempt.read_guard, undeliverable=reason == "success")
    identity = await e.channel._services.outbox.plan_publication_outcome(record, **args)
    return record, args, _item(await _primary(e.db, identity))


def repeated(item):
    return validate_publication_outcome(item).source_ref["values"].get("start_failed_again", False)


@pytest.mark.parametrize("reset", ["another-error", "success"])
async def test_second_failure_guidance_is_durable_and_replay_does_not_recount(publication, reset):
    e = publication
    first, first_args, one = await plan(e, 1, claimed_hint=True)
    assert repeated(one) is False
    second, second_args, two = await plan(e, 2)
    assert repeated(two) is True
    source = validate_publication_outcome(two).source_ref
    assert render_plain(source["key"], "en", **source["values"]).endswith("If this keeps happening, ask an administrator.")
    assert render_plain(source["key"], "en", reader_is_administrator=True, **source["values"]).endswith("If this keeps happening, ask Example Provider.")
    await plan(e, 3, reason=reset)
    for record, args, item in ((first, first_args, one), (second, second_args, two)):
        replay = await e.channel._services.outbox.plan_publication_outcome(record, **args)
        assert replay == item.id and repeated(_item(await _primary(e.db, replay))) is repeated(item)
    _, _, four = await plan(e, 4)
    assert repeated(four) is False


@pytest.mark.parametrize("change", ["malformed", "generation", "retention", "timestamps"])
async def test_inconclusive_or_foreign_history_cannot_add_guidance(publication, change):
    e = publication
    _, _, first = await plan(e, 1)
    _, _, second = await plan(e, 2)
    async with e.db.sessions.begin() as writer:
        if change == "malformed":
            row = await writer.get(TelegramOutboxRow, second.id)
            row.payload = {"unexpected": True}
        elif change == "generation":
            await writer.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id.in_([first.id, second.id])).values(binding_generation=e.link.binding_generation + 1))
        elif change == "retention":
            await writer.execute(delete(TelegramOutboxRow).where(TelegramOutboxRow.id.in_([first.id, second.id])))
        else:
            stamp = (await writer.get(TelegramOutboxRow, second.id)).created_at
            await writer.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == first.id).values(created_at=stamp))
    _, _, third = await plan(e, 3)
    assert repeated(third) is False


async def test_an_auxiliary_notice_does_not_reset_start_failures(publication):
    e = publication
    _, args, _ = await plan(e, 1)
    e.channel._services.outbox.clock.__self__.advance(1)
    await e.channel._services.outbox.plan_product_keyboard(
        e.link, source_id="fixture-auxiliary", parts=[{"kind": "text", "source_ref": {"key": "answer.thinking", "values": {}}}], keyboard=[], callbacks=args["callbacks"], guard=args["guard"]
    )
    _, _, second = await plan(e, 2)
    assert repeated(second) is True
    second.payload["turn_outcome_basis"]["source_ref"]["values"]["start_failed_again"] = "true"
    with pytest.raises(ValueError):
        validate_publication_outcome(second)


async def test_prepublication_terminal_input_resets_the_sequence(publication):
    e = publication
    _, args, _ = await plan(e, 1)
    e.channel._services.outbox.clock.__self__.advance(1)
    incoming = message(e, 4000, content="Synthetic prepublication question")
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    row = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    await e.channel._services.outbox.plan_input_outcome(
        e.link, source_id=f"chat-input:{e.link.chat_id}:{e.link.binding_generation}:{row.message_id}", source_ref={"key": "error.generic", "values": {}}, keyboard=[], callbacks=args["callbacks"], guard=args["guard"], inbox_rows=(row,)
    )
    _, _, later = await plan(e, 2)
    assert repeated(later) is False


async def test_new_conversation_starts_its_own_failure_sequence(publication):
    from uuid import uuid4

    from deerflow.spaces.contract import PrincipalRef

    e = publication
    await plan(e, 1)
    fresh = await e.http_app.state.agent_conversations.create(actor=PrincipalRef("human", e.link.owner_user_id), instance_id=e.link.agent_ref, creation_id=uuid4().hex)
    e.thread_id = fresh["thread_id"]
    _, _, later = await plan(e, 2)
    assert repeated(later) is False
