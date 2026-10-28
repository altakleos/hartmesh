"""Actual file-input SQL/bus custody, auxiliary notices and callback replay."""

import asyncio
from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import delete, select
from telegram import Chat, Document, Message, Update, User
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment

from app.telegram_bots.callbacks import CallbackRepository
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow


async def source(e, *, expected_files=True, size=3):
    incoming = Update(
        2901,
        message=Message(
            901,
            datetime.now(UTC),
            Chat(e.link.chat_id, "private"),
            from_user=User(e.link.telegram_user_id, "Actual human", False),
            document=Document("actual-file", "actual-unique", file_name="input.pdf", mime_type="application/pdf", file_size=size),
            caption="Read the actual file",
        ),
    )
    file = {"type": "file", "file_id": "actual-file", "file_unique_id": "actual-unique", "filename": "input.pdf", "mime_type": "application/pdf", "size": size, "message_id": 901, "telegram_bot_id": e.link.bot_id}
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "media", "text": incoming.message.caption, "files": [file]})
    row = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 2, "kind": "chat_input", "forwarded": False, "origin": None}}, operation_ids=[])
    record = await e.coordinator.dispatch(e.link, (row,), thread_id=e.thread_id)
    sent = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    e.original_message = sent
    assert sent.files == ([file] if expected_files else []) and sent.text == "Read the actual file"
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    return record, file, attempt.create_guard


@pytest.mark.asyncio
@pytest.mark.parametrize("collect_source", [False, True])
async def test_file_failure_auxiliary_retry_retains_original_thread_group_and_new_event(coordinator_environment, collect_source):
    e = coordinator_environment
    record, file, guard = await source(e)
    callbacks = CallbackRepository(e.db.sessions)
    identity = await e.outbox.plan_inbound_file_notice(record, file, source_ref={"key": "file.not_saved", "values": {"file": file["filename"]}}, retry=True, callbacks=callbacks, guard=guard)
    item = await e.outbox.get(e.link, identity)
    assert item.payload["product_basis"]["finish_route"] is False
    async with e.db.sessions() as reader:
        original = await reader.get(TelegramInboundMarkRow, (e.link.id, 901, e.link.chat_id))
        assert not original.route_complete and original.outbox_id is None
        selected = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.kind == "file_retry"))
        callback_id, group = selected.id, selected.group_id
    assert await callbacks.adopt(e.link, group, 1901, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=identity, presentation_version=0, check_current=lambda: True)
    if collect_source:
        async with e.db.sessions.begin() as writer:
            await writer.execute(delete(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == e.link.id))
    await e.db.connections.set_thread_id(connection_id=e.link.connection_id, owner_user_id=e.link.owner_user_id, provider="telegram", external_conversation_id=str(e.link.chat_id), thread_id="later-thread")
    stage = await e.inbox.stage_callback_input(e.link, callback_id=callback_id, update_id=3901, chat_id=e.link.chat_id, message_id=1901, message_thread_id=None, callbacks=callbacks, guard=guard)
    assert stage.thread_id == e.thread_id and stage.text == "Read the actual file"
    retried = await e.inbox.reserve_callback_publication(e.link, callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=guard)
    assert retried.event_id == "c" + callback_id and retried.publication_id != record.publication_id
    assert (await callbacks.get(callback_id)).consumed_at is None
    result = await e.coordinator.dispatch_record(retried, e.link)
    sent = e.bus.get_inbound_nowait()
    e.bus.inbound_task_done()
    assert sent.text == "Read the actual file" and sent.files == [file] and sent.execution_context.thread_id == e.thread_id
    assert (await callbacks.get(callback_id)).consumed_at is not None
    await e.coordinator.dispatch_record(result, e.link)
    with pytest.raises(asyncio.QueueEmpty):
        e.bus.get_inbound_nowait()


@pytest.mark.asyncio
async def test_file_preflight_filters_only_transport_before_the_mark_and_preserves_originals(coordinator_environment):
    e = coordinator_environment
    calls = []

    async def preflight(link, record, files, *, execution_context=None):
        attempt = await e.host.capture(link, thread_id=record.scope.thread_id, endpoint_id=e.http.endpoint_id)
        fresh, origins = await e.inbox.read_publication_input(record, guard=attempt.create_guard)
        assert not fresh.dispatch_started and origins[0].sanitized_input["files"] == files
        calls.append((record.publication_id, files))
        return []

    e.coordinator.file_preflight = preflight
    record, file, guard = await source(e, expected_files=False)
    assert calls == [(record.publication_id, [file])]
    _, origins = await e.inbox.read_publication_input(record, guard=guard)
    assert origins[0].sanitized_input["files"] == [file]


@pytest.mark.asyncio
async def test_notice_and_preflight_reject_bool_size_instead_of_actual_one(coordinator_environment):
    e = coordinator_environment
    record, file, guard = await source(e, size=1)
    with pytest.raises(ValueError):
        await e.outbox.plan_inbound_file_notice(record, {**file, "size": True}, source_ref={"key": "file.not_saved", "values": {"file": file["filename"]}}, retry=True, callbacks=CallbackRepository(e.db.sessions), guard=guard)


@pytest.mark.asyncio
async def test_preflight_refuses_a_bool_descriptor_even_when_it_compares_equal(coordinator_environment):
    e = coordinator_environment

    async def preflight(link, record, files, *, execution_context=None):
        return [{**files[0], "size": True}]

    e.coordinator.file_preflight = preflight
    from app.telegram_bots.publication import PublicationRefused

    with pytest.raises(PublicationRefused):
        await source(e, size=1)


async def actual_turn_view(e, record, guard):
    from types import SimpleNamespace

    from app.telegram_bots.turns import ChatTurns

    _, origins = await e.inbox.read_publication_input(record, guard=guard)
    e.channel._stopping = False
    e.channel._same_scope = lambda left, right: left == right
    services = SimpleNamespace(config=e.db.config, outbox=e.outbox, callbacks=CallbackRepository(e.db.sessions), links=e.db.links, prepare_live_owner=e.live.authority.prepare_live_owner, channel=lambda identity: e.channel)
    turns = ChatTurns(services)
    turn = SimpleNamespace(channel=e.channel, link=e.link, record=record, origins=origins, active=True, done=asyncio.Event(), topic=None)
    turns._turns[(e.link.id, None)] = turn
    return turns


@pytest.mark.asyncio
async def test_original_caller_scope_rejects_before_input_sql_and_expiry_rolls_back_notice(coordinator_environment, monkeypatch):
    from dataclasses import replace

    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = coordinator_environment
    record, file, guard = await source(e)
    turns = await actual_turn_view(e, record, guard)
    cap = e.original_message.execution_context
    clock = [0]
    cap.admission.control.clock = lambda: clock[0]
    cap.admission.control.deadline = 1
    calls = []
    actual_read = e.inbox.read_publication_input

    async def read(*args, **kwargs):
        calls.append(True)
        return await actual_read(*args, **kwargs)

    monkeypatch.setattr(e.inbox, "read_publication_input", read)
    foreign = replace(cap, scope=replace(cap.scope, publication_id="f" * 32))
    assert await turns.file_context(e.channel, file, execution_context=foreign) is None
    assert calls == []
    context = await turns.file_context(e.channel, file, execution_context=cap)
    assert context is not None and context.current()
    actual_prepare = turns.services.prepare_live_owner

    async def expired(owner):
        result = await actual_prepare(owner)
        clock[0] = 2
        return result

    monkeypatch.setattr(turns.services, "prepare_live_owner", expired)
    with pytest.raises(ValueError):
        await turns.file_notice(e.channel, context, key="file.not_saved", values={"file": file["filename"]}, retry=True, execution_context=cap)
    async with e.db.sessions() as reader:
        assert not any(row.payload.get("inbound_file_basis") for row in await reader.scalars(select(TelegramOutboxRow)))
