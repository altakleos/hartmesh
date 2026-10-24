"""Reply plans are real SQL transactions and keep exact delivery identities."""

import asyncio
import hashlib
from copy import deepcopy
from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import select, text, update
from sqlalchemy.exc import DBAPIError
from telegram import Chat, Message, Update, User

from app.telegram_bots.producer import Outbox
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def text_part(key="chat.restarted", **extra):
    return {"kind": "text", "source_ref": {"key": key, "values": {}}, **extra}


async def connected(db):
    link = await bind_link(db, await add_link(db))
    async with db.sessions.begin() as session:
        welcome = (await session.scalars(select(TelegramOutboxRow))).one()
        welcome.state = "sent"
    return link


async def model_turn(db, link, *, topic=None):
    repo = Outbox(db.sessions).inbox
    marks = []
    for message_id in (102, 103):
        message = Message(message_id, datetime.now(UTC), Chat(link.chat_id, "private"), from_user=User(link.telegram_user_id, "Fixture", False), text="synthetic turn", message_thread_id=topic)
        mark, _ = await repo.accept(link, Update(message_id + 100, message=message), sanitized_input={"text": "synthetic turn"})
        marks.append(await repo.route(mark, kind="chat", target_id="thread-original", basis={"conversation_id": "thread-original"}, operation_ids=["a" * 32]))
    await repo.bind_turn(marks, turn_id="a" * 32, thread_id="thread-original")
    publication = hashlib.sha256((("a" * 32) + ":m102").encode()).hexdigest()[:32]
    await repo.bind_run(turn_id="a" * 32, publication_id=publication, run_id="run-exact")
    source = {"thread_id": "thread-original", "run_id": "run-exact", "publication_id": publication, "assistant_message_id": "assistant-exact", "message_ids": [102, 103]}
    return marks, source


@pytest.mark.asyncio
async def test_borrowed_delivery_plan_rolls_back_with_caller(db):
    link = await connected(db)
    outbox = Outbox(db.sessions)
    async with db.sessions() as session:
        async with session.begin():
            assert await outbox.add(link, dedupe_key="notice:atomic", class_="reply", payload={"parts": [text_part()]}, session=session)
            assert (await session.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == "notice:atomic"))).parts[0]["part_id"]
            await session.rollback()
    assert len(await rows(db, TelegramOutboxRow)) == 1


@pytest.mark.asyncio
async def test_reply_plan_dedupes_and_freezes_grouped_sources(db):
    link = await connected(db)
    outbox = Outbox(db.sessions)
    marks, source = await model_turn(db, link, topic=77)
    plan_id = await outbox.plan_reply(link, turn_id="a" * 32, source=source, parts=[text_part(), text_part("hello.privacy")], inbox_rows=marks)
    saved = next(row for row in await rows(db, TelegramOutboxRow) if row.id == plan_id)
    original = deepcopy(saved.parts)
    assert [part["ordinal"] for part in original] == [0, 1]
    assert len({part["part_id"] for part in original}) == 2
    assert saved.payload["source"] == source
    assert saved.message_thread_id == 77
    assert await outbox.plan_reply(link, turn_id="a" * 32, source=source, parts=[text_part()], inbox_rows=marks) == plan_id
    assert next(row for row in await rows(db, TelegramOutboxRow) if row.id == plan_id).parts == original
    for mark in await rows(db, TelegramInboundMarkRow):
        if mark.message_id != 101:
            assert mark.delivery_ids == [plan_id] and mark.outbox_id == plan_id
            assert mark.route_complete and mark.state == "reply_pending"


@pytest.mark.asyncio
async def test_old_publication_output_cannot_seal_current_turn(db):
    link = await connected(db)
    outbox = Outbox(db.sessions)
    marks, old_source = await model_turn(db, link)
    current = await outbox.inbox.publish(marks, event_id="m102:r1", expected_publication_id=old_source["publication_id"])
    await outbox.inbox.bind_run(turn_id="a" * 32, publication_id=current, run_id="new-run")
    with pytest.raises(ValueError, match="reply-source-conflict"):
        await outbox.plan_reply(link, turn_id="a" * 32, source=old_source, parts=[text_part()], inbox_rows=marks)
    assert len(await rows(db, TelegramOutboxRow)) == 1
    assert all(not mark.route_complete for mark in await rows(db, TelegramInboundMarkRow) if mark.message_id != 101)


@pytest.mark.asyncio
async def test_concurrent_final_frames_commit_one_complete_plan(db):
    link = await connected(db)
    outbox = Outbox(db.sessions)
    marks, source = await model_turn(db, link)
    identities = await asyncio.gather(*(outbox.plan_reply(link, turn_id="a" * 32, source=source, parts=[text_part()], inbox_rows=marks) for _ in range(4)))
    assert len(set(identities)) == 1 and len(await rows(db, TelegramOutboxRow)) == 2
    assert all(mark.delivery_ids == identities[:1] and mark.route_complete for mark in await rows(db, TelegramInboundMarkRow) if mark.message_id != 101)


@pytest.mark.asyncio
async def test_generic_payload_cannot_impersonate_fixed_status_notice(db):
    link = await connected(db)
    with pytest.raises(ValueError, match="status-notice"):
        await Outbox(db.sessions).add(link, dedupe_key="notice:forged", class_="reply", payload={"delivery_kind": "status_notice", "parts": [text_part()]})


@pytest.mark.asyncio
async def test_retry_preserves_frozen_parts_and_does_not_steal_live_claim(db):
    link = await connected(db)
    outbox = Outbox(db.sessions)
    await outbox.add(link, dedupe_key="notice:retry", class_="reply", payload={"parts": [text_part()]})
    row = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "notice:retry")
    async with db.sessions.begin() as session:
        current = await session.get(TelegramOutboxRow, row.id)
        current.state, current.claim_owner, current.claim_id = "sending", "owner", "claim"
    await outbox.retry(link, row.id)
    saved = next(current for current in await rows(db, TelegramOutboxRow) if current.id == row.id)
    assert (saved.state, saved.claim_owner, saved.claim_id) == ("sending", "owner", "claim")
    assert saved.parts == row.parts


@pytest.mark.asyncio
async def test_replacement_fences_version_and_retains_acknowledged_history(db):
    link = await connected(db)
    outbox = Outbox(db.sessions)
    group = "b" * 32
    await outbox.add(link, dedupe_key="notice:replace", class_="reply", payload={"source": {"assistant_message_id": "exact-original"}, "parts": [text_part(), text_part(callback_group_id=group)]})
    row = next(row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "notice:replace")
    async with db.sessions.begin() as session:
        current = await session.get(TelegramOutboxRow, row.id)
        current.state, current.claim_owner, current.claim_id = "sending", "process", "claim-current"
        current.parts = [{**current.parts[0], "state": "sent", "message_ids": [800]}, current.parts[1]]
        session.add(
            TelegramCallbackRow(
                id="c" * 32,
                group_id=group,
                link_id=link.id,
                chat_id=link.chat_id,
                binding_generation=link.binding_generation,
                message_id=800,
                kind="delivery_retry",
                target_kind="outbox",
                target_id="other",
                payload={},
                created_at=datetime.now(UTC),
            )
        )
    replacement = [text_part(), text_part(callback_group_id=group)]
    await outbox.replace_presentation(link, row.id, expected_version=0, claim_owner="process", claim_id="stale", reason="edit-anchor-missing", parts=replacement, message_thread_id=None)
    assert next(value for value in await rows(db, TelegramOutboxRow) if value.id == row.id).payload["presentation_version"] == 0
    await outbox.replace_presentation(link, row.id, expected_version=0, claim_owner="process", claim_id="claim-current", reason="edit-anchor-missing", parts=replacement, message_thread_id=None)
    saved = next(value for value in await rows(db, TelegramOutboxRow) if value.id == row.id)
    assert saved.state == "pending" and saved.claim_owner is None and saved.claim_id is None
    assert [part["state"] for part in saved.parts] == ["sent", "retired", "pending", "pending"]
    assert saved.parts[0]["message_ids"] == [800]
    assert {part["part_id"] for part in saved.parts[2:]}.isdisjoint({part["part_id"] for part in row.parts})
    assert (await rows(db, TelegramCallbackRow))[0].payload["presentation_pending"] is True
    with pytest.raises(ValueError, match="changed-presentation-basis"):
        async with db.sessions.begin() as session:
            current = await session.get(TelegramOutboxRow, row.id)
            current.state, current.claim_owner, current.claim_id = "sending", "process", "new-claim"
        await outbox.replace_presentation(link, row.id, expected_version=1, claim_owner="process", claim_id="new-claim", reason="edit-anchor-missing", parts=[text_part("different-answer")], message_thread_id=None)


@pytest.mark.asyncio
async def test_unsafe_sources_never_reach_durable_storage(db):
    link = await connected(db)
    for source in ({"download_url": "https://example.invalid/temporary"}, {"key": "chat.restarted", "values": {"raw_update": {"message": "private"}}}):
        with pytest.raises(ValueError):
            await Outbox(db.sessions).add(link, dedupe_key="notice:unsafe", class_="reply", payload={"parts": [{"kind": "text", "source_ref": source}]})
    assert len(await rows(db, TelegramOutboxRow)) == 1


@pytest.mark.asyncio
async def test_action_controls_cannot_precede_required_preview_parts(db):
    link = await connected(db)
    with pytest.raises(ValueError, match="delivery-controls"):
        await Outbox(db.sessions).add(link, dedupe_key="notice:early-controls", class_="reply", payload={"parts": [text_part(callback_group_id="b" * 32), text_part()]})
    assert len(await rows(db, TelegramOutboxRow)) == 1


@pytest.mark.asyncio
async def test_borrowed_lazy_writer_cannot_commit_from_an_overlapped_stale_binding(db, monkeypatch):
    link = await connected(db)
    outbox = Outbox(db.sessions)
    read, resume = asyncio.Event(), asyncio.Event()
    async with db.sessions() as caller:
        original = caller.scalar

        async def pause_authority(statement, *args, **kwargs):
            value = await original(statement, *args, **kwargs)
            if not read.is_set() and statement.column_descriptions[0].get("entity") is TelegramBotLinkRow:
                read.set()
                await resume.wait()
            return value

        monkeypatch.setattr(caller, "scalar", pause_authority)

        async def borrow():
            async with caller.begin():
                await outbox.add(link, dedupe_key="notice:borrowed-overlap", class_="reply", payload={"parts": [text_part()]}, session=caller)

        pending = asyncio.create_task(borrow())
        try:
            await read.wait()
            # A separate native writer probes the lock while the exact first
            # authority result is held. No sleep or synthetic lock substitute.
            try:
                async with db.sessions() as other, other.begin():
                    await other.execute(text("PRAGMA busy_timeout=0" if db.dialect == "sqlite" else "SET LOCAL lock_timeout='1ms'"))
                    await other.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(binding_generation=link.binding_generation + 1, state="held"))
            except DBAPIError as error:
                assert getattr(error.orig, "sqlite_errorcode", None) == 5 if db.dialect == "sqlite" else getattr(error.orig, "sqlstate", None) == "55P03"
        finally:
            resume.set()
            result = (await asyncio.gather(pending, return_exceptions=True))[0]
    if isinstance(result, Exception):
        assert isinstance(result, ValueError) and str(result) == "link-not-connected"
    plans = [row for row in await rows(db, TelegramOutboxRow) if row.dedupe_key == "notice:borrowed-overlap"]
    current = await db.links.get(link.id)
    assert not plans or plans[0].state == "skipped" or (plans[0].binding_generation == current.binding_generation and current.state == "connected")
