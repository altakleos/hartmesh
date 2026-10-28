"""Pending attachment custody over actual admitted updates and both SQL engines."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from telegram import Chat, Document, Message, Update, User
from test_telegram_callbacks import callbacks_environment as callbacks_environment
from test_telegram_publication_custody import publication_environment as publication_environment

from app.telegram_bots import files as module
from app.telegram_bots import inbox as inbox_module
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramPendingFileRow

NOW = datetime(2026, 10, 25, 14, tzinfo=UTC)


@pytest_asyncio.fixture
async def pending(callbacks_environment, monkeypatch):
    e = callbacks_environment
    clock = [NOW]
    service = module.PendingFiles(e.callbacks, e.inbox, guard_factory=lambda link: e.guard_factory(captured_link=link), config=e.db.config, clock=lambda: clock[0])

    async def accepted(identity, *, media=False, topic=None, size=None, at=NOW):
        descriptor = {
            "type": "file",
            "file_id": "remote-" + str(identity),
            "file_unique_id": "unique-" + str(identity),
            "filename": "input.csv",
            "mime_type": "text/csv",
            "size": size,
            "message_id": identity,
            "telegram_bot_id": e.link.bot_id,
        }
        incoming = Update(
            identity + 10000,
            message=Message(
                identity,
                at,
                Chat(e.link.chat_id, "private"),
                from_user=User(e.link.telegram_user_id, "Actual human", False),
                text=None if media else "Use these files",
                document=Document(descriptor["file_id"], descriptor["file_unique_id"], file_name=descriptor["filename"], mime_type=descriptor["mime_type"], file_size=size) if media else None,
                message_thread_id=topic,
            ),
        )
        value = {"type": "media", "text": "", "files": [descriptor]} if media else {"type": "text", "text": "Use these files"}
        with monkeypatch.context() as timing:
            timing.setattr(inbox_module, "_now", lambda: at)
            row, _ = await e.inbox.accept(e.link, incoming, sanitized_input=value)
        profile = {"version": 2, "kind": "chat_input", "forwarded": False, "origin": None} if media else {"version": 1, "kind": "plain_text", "forwarded": False}
        row = await e.inbox.route(row, kind="turn", target_id=e.link.agent_ref, basis={"input_profile": profile}, operation_ids=[])
        return incoming, row, descriptor

    yield SimpleNamespace(**vars(e), service=service, now=clock, accepted=accepted)


@pytest.mark.asyncio
async def test_pending_take_append_and_consume_rollback_then_replay_excludes_later_files(pending):
    e = pending
    _incoming, source, descriptor = await e.accepted(801, media=True, size=None)
    await e.service.add(e.link, source, descriptor)
    incoming, target, _ = await e.accepted(802)
    with pytest.raises(RuntimeError, match="next step failed"):
        async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
            assert await e.service.take(e.link, target, guard=e.guard_factory(), session=writer) == [descriptor]
            raise RuntimeError("next step failed")
    async with e.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, source.message_id)) is not None
        actual = await reader.get(TelegramInboundMarkRow, (target.link_id, target.message_id, target.chat_id))
        assert actual.sanitized_input == {"type": "text", "text": "Use these files"}
    assert await e.service.take(e.link, target) == [descriptor]
    _update, later, later_descriptor = await e.accepted(803, media=True, size=0)
    await e.service.add(e.link, later, later_descriptor)
    replay, created = await e.inbox.accept(e.link, incoming, sanitized_input={"type": "text", "text": "Use these files"})
    assert not created and replay.sanitized_input["files"] == [descriptor]
    assert await e.service.take(e.link, replay) == [descriptor]
    async with e.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, source.message_id)) is None
        actual = await reader.get(TelegramPendingFileRow, (e.link.id, later.message_id))
        assert actual is not None and actual.size == 0


@pytest.mark.asyncio
async def test_empty_take_is_frozen_before_a_later_pending_file(pending):
    e = pending
    _update, target, _ = await e.accepted(810)
    assert await e.service.take(e.link, target) == []
    _update, source, descriptor = await e.accepted(811, media=True)
    await e.service.add(e.link, source, descriptor)
    assert await e.service.take(e.link, target) == []
    async with e.sessions() as reader:
        actual = await reader.get(TelegramPendingFileRow, (e.link.id, source.message_id))
        assert actual is not None and actual.size == -1


@pytest.mark.asyncio
async def test_topic_and_expiry_are_selected_before_atomic_take_and_sweep(pending):
    e = pending
    _update, first, one = await e.accepted(820, media=True, topic=2, size=0)
    _update, other, two = await e.accepted(821, media=True, topic=3, size=4)
    _update, expired, three = await e.accepted(822, media=True, topic=2, at=NOW - timedelta(hours=25))
    for row, file in [(first, one), (other, two), (expired, three)]:
        await e.service.add(e.link, row, file)
    _update, target, _file = await e.accepted(823, topic=2)
    assert await e.service.take(e.link, target) == [one]
    assert await e.service.sweep(NOW) == 1
    async with e.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramPendingFileRow)))
        assert [(row.message_id, row.message_thread_id) for row in rows] == [(other.message_id, 3)]


@pytest.mark.asyncio
async def test_add_requires_actual_admitted_descriptor_and_borrowed_writer_rollback(pending):
    e = pending
    _update, row, descriptor = await e.accepted(830, media=True, size=1)
    with pytest.raises(ValueError):
        await e.service.add(e.link, row, {**descriptor, "file_id": "not-actually-admitted"})
    with pytest.raises(ValueError):
        await e.service.add(e.link, row, {**descriptor, "size": True})
    with pytest.raises(RuntimeError):
        async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
            await e.service.add(e.link, row, descriptor, guard=e.guard_factory(), session=writer)
            raise RuntimeError("writer rolled back")
    async with e.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, row.message_id)) is None
    await e.service.add(e.link, row, descriptor)
    await e.service.add(e.link, row, descriptor)
    async with e.sessions() as reader:
        rows = list(await reader.scalars(select(TelegramPendingFileRow)))
        assert len(rows) == 1 and rows[0].created_at.replace(tzinfo=UTC) == row.accepted_at


@pytest.mark.asyncio
async def test_actual_binding_retirement_cannot_adopt_old_pending_input(pending):
    e = pending
    _update, row, descriptor = await e.accepted(840, media=True)
    await e.service.add(e.link, row, descriptor)
    await e.links.unbind(e.link, actor_id=e.owner_id)
    with pytest.raises(ValueError):
        await e.service.add(e.link, row, descriptor)
    async with e.sessions() as reader:
        assert list(await reader.scalars(select(TelegramPendingFileRow))) == []


@pytest.mark.asyncio
async def test_completed_source_cannot_recreate_a_taken_attachment(pending):
    e = pending
    _update, row, descriptor = await e.accepted(850, media=True)
    await e.service.add(e.link, row, descriptor)
    await e.outbox.plan_product(e.link, source_id="pending-received:850", parts=[{"kind": "text", "source_ref": {"key": "file.saved_ask_plain", "values": {"file": descriptor["filename"]}}}], inbox_rows=[row], finish_route=True)
    _update, target, _file = await e.accepted(851)
    assert await e.service.take(e.link, target) == [descriptor]
    await e.service.add(e.link, row, descriptor)
    async with e.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, row.message_id)) is None


@pytest.mark.asyncio
async def test_final_owner_guard_rolls_back_both_append_and_pending_consumption(pending):
    e = pending
    _update, source, descriptor = await e.accepted(860, media=True)
    await e.service.add(e.link, source, descriptor)
    _update, target, _file = await e.accepted(861)
    try:
        with pytest.raises(ValueError):
            async with e.callbacks.writer(e.link, e.guard_factory()) as writer:
                assert await e.service.take(e.link, target, guard=e.guard_factory(), session=writer) == [descriptor]
                e.attempt_current[0] = False
    finally:
        e.attempt_current[0] = True
    async with e.sessions() as reader:
        assert await reader.get(TelegramPendingFileRow, (e.link.id, source.message_id)) is not None
        actual = await reader.get(TelegramInboundMarkRow, (target.link_id, target.message_id, target.chat_id))
        assert actual.sanitized_input == {"type": "text", "text": "Use these files"}


@pytest.mark.asyncio
async def test_expiry_is_measured_after_the_actual_owner_preparation_wait(pending):
    e = pending
    _update, source, descriptor = await e.accepted(870, media=True)
    await e.service.add(e.link, source, descriptor)
    _update, target, _file = await e.accepted(871)
    actual = e.guard_factory()

    class DelayedGuard:
        captured_link = e.link

        async def prepare(self):
            e.now[0] += timedelta(hours=25)

        async def check(self, writer):
            return await actual.check(writer)

    assert await e.service.take(e.link, target, guard=DelayedGuard()) == []
    assert await e.service.sweep(e.now[0]) == 1
