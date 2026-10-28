"""Actual pending SQL rows transfer once into a real accepted text receipt."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Chat, Document, Message, Update, User
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment

from app.telegram_bots.inbox import InboxRow, _publication_input
from app.telegram_bots.links import prepare_publication_guard
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramPendingFileRow


def update(e, identity, *, topic=None, document=False):
    return Update(
        identity + 1000,
        message=Message(
            identity,
            datetime.now(UTC),
            Chat(e.link.chat_id, "private"),
            from_user=User(e.link.telegram_user_id, "Actual human", False),
            text=None if document else "Use the actual attachment",
            document=Document("file-" + str(identity), "unique-" + str(identity), file_name="input.pdf", mime_type="application/pdf") if document else None,
            message_thread_id=topic,
        ),
    )


async def pending(e, identity, *, topic=None):
    incoming = update(e, identity, topic=topic, document=True)
    descriptor = {
        "type": "file",
        "file_id": incoming.message.document.file_id,
        "file_unique_id": incoming.message.document.file_unique_id,
        "filename": "input.pdf",
        "mime_type": "application/pdf",
        "size": None,
        "message_id": identity,
        "telegram_bot_id": e.link.bot_id,
    }
    await e.inbox.accept(e.link, incoming, sanitized_input={"type": "media", "text": "", "files": [descriptor]})
    async with e.db.sessions.begin() as writer:
        writer.add(
            TelegramPendingFileRow(
                link_id=e.link.id,
                message_id=identity,
                chat_id=e.link.chat_id,
                message_thread_id=topic,
                binding_generation=e.link.binding_generation,
                file_id=descriptor["file_id"],
                file_unique_id=descriptor["file_unique_id"],
                filename=descriptor["filename"],
                mime_type=descriptor["mime_type"],
                size=-1,
                is_image=False,
                created_at=datetime.now(UTC),
            )
        )
    return descriptor


async def consumer(e):
    incoming = update(e, 801)
    row, _ = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    attempt = await e.host.capture(e.link, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id)
    await prepare_publication_guard(attempt.create_guard)
    return incoming, row, attempt.create_guard


@pytest.mark.asyncio
async def test_pending_transfer_is_atomic_and_replay_never_takes_later_files(coordinator_environment):
    e = coordinator_environment
    descriptor = await pending(e, 800)
    incoming, row, guard = await consumer(e)
    async with e.inbox._transaction() as writer:
        fresh, taken = await e.inbox.pending_file_take(e.link, row, session=writer, guard=guard)
        assert taken is None
        selected = list(await writer.scalars(select(TelegramPendingFileRow)))
        attached = await e.inbox.attach_pending_files(e.link, fresh, selected, session=writer, guard=guard)
    assert attached.sanitized_input["files"] == [descriptor]
    replay, created = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
    assert not created and replay.sanitized_input == attached.sanitized_input
    await pending(e, 802)
    async with e.inbox._transaction() as writer:
        again, taken = await e.inbox.pending_file_take(e.link, replay, session=writer, guard=guard)
        assert taken == [descriptor] and again.sanitized_input == attached.sanitized_input
        assert [p.message_id for p in await writer.scalars(select(TelegramPendingFileRow))] == [802]
    with pytest.raises(ValueError, match="inbox-identity-conflict"):
        await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": "Different claimed original"})


@pytest.mark.asyncio
async def test_pending_transfer_rollback_preserves_both_inputs_and_pending(coordinator_environment):
    e = coordinator_environment
    await pending(e, 800)
    _, row, guard = await consumer(e)
    with pytest.raises(RuntimeError, match="abort"):
        async with e.inbox._transaction() as writer:
            selected = list(await writer.scalars(select(TelegramPendingFileRow)))
            await e.inbox.attach_pending_files(e.link, row, selected, session=writer, guard=guard)
            raise RuntimeError("abort")
    async with e.db.sessions() as reader:
        actual = await reader.get(TelegramInboundMarkRow, (row.link_id, row.message_id, row.chat_id))
        assert InboxRow.from_row(actual).sanitized_input == row.sanitized_input
        assert list(await reader.scalars(select(TelegramPendingFileRow.message_id))) == [800]


@pytest.mark.asyncio
async def test_pending_foreign_topic_is_rejected_without_consumption(coordinator_environment):
    e = coordinator_environment
    await pending(e, 800, topic=7)
    _, row, guard = await consumer(e)
    async with e.inbox._transaction() as writer:
        selected = list(await writer.scalars(select(TelegramPendingFileRow)))
        with pytest.raises(ValueError, match="pending-file-conflict"):
            await e.inbox.attach_pending_files(e.link, row, selected, session=writer, guard=guard)
    async with e.db.sessions() as reader:
        assert list(await reader.scalars(select(TelegramPendingFileRow.message_id))) == [800]


@pytest.mark.asyncio
async def test_empty_pending_take_is_reserved_and_preserves_plain_hash(coordinator_environment):
    e = coordinator_environment
    _, row, guard = await consumer(e)
    async with e.inbox._transaction() as writer:
        attached = await e.inbox.attach_pending_files(e.link, row, [], session=writer, guard=guard)
    with pytest.raises(ValueError, match="invalid-inbox-route"):
        await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"pending_files": {}}, operation_ids=[])
    routed = await e.inbox.route(attached, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
    assert routed.route_basis["pending_files"] == attached.route_basis["pending_files"]
    from dataclasses import replace

    assert _publication_input([replace(routed, route_basis={k: v for k, v in routed.route_basis.items() if k != "pending_files"})]) == _publication_input([routed])
    async with e.inbox._transaction() as writer:
        _, taken = await e.inbox.pending_file_take(e.link, routed, session=writer, guard=guard)
        assert taken == []


@pytest.mark.asyncio
async def test_malformed_v1_profile_cannot_augment_or_consume_actual_pending(coordinator_environment):
    e = coordinator_environment
    await pending(e, 800)
    _incoming, first, guard = await consumer(e)
    for index, profile in enumerate(
        (
            {"version": 1, "kind": "plain_text", "forwarded": 0},
            {"version": True, "kind": "plain_text", "forwarded": False},
        )
    ):
        if index == 0:
            row = first
        else:
            incoming = update(e, 803)
            row, _created = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": incoming.message.text})
        row = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": profile}, operation_ids=[])
        async with e.inbox._transaction() as writer:
            selected = list(await writer.scalars(select(TelegramPendingFileRow)))
            with pytest.raises(ValueError, match="pending-file-conflict"):
                await e.inbox.attach_pending_files(e.link, row, selected, session=writer, guard=guard)
        async with e.db.sessions() as reader:
            actual = await reader.get(TelegramInboundMarkRow, (row.link_id, row.message_id, row.chat_id))
            assert actual.sanitized_input == row.sanitized_input
            assert "pending_files" not in actual.route_basis
            assert list(await reader.scalars(select(TelegramPendingFileRow.message_id))) == [800]
