"""Durable content admission, immutable grouping and complete delivery settlement."""

import asyncio
import dataclasses
import hashlib
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select, text, update
from sqlalchemy.orm import Session
from telegram import Chat, Message, Update, User

from app.telegram_bots.inbox import InboxRepository, InboxRow
from app.telegram_bots.producer import Outbox
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkRow, TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio
ACCOUNT = 7_000_000_001
NOW = datetime(2026, 10, 24, tzinfo=UTC)


def incoming(*, message=102, update_id=202, account=ACCOUNT, topic=None, reply=None, album=None, edited=False):
    value = Message(
        message_id=message,
        date=NOW,
        chat=Chat(account, "private"),
        from_user=User(account, "Fixture", False),
        text="synthetic accepted text",
        message_thread_id=topic,
        reply_to_message=Message(message_id=reply, date=NOW, chat=Chat(account, "private")) if reply else None,
        media_group_id=album,
    )
    return Update(update_id, edited_message=value) if edited else Update(update_id, message=value)


async def setup(db):
    return InboxRepository(db.sessions), await bind_link(db, await add_link(db))


async def accepted(repo, link, *, message=102, update_id=202, topic=None, payload=None, **kwargs):
    return (await repo.accept(link, incoming(message=message, update_id=update_id, topic=topic, **kwargs), sanitized_input=payload or {"text": "synthetic accepted text"}))[0]


async def routed(repo, link, *, message=102, update_id=202, topic=None, basis=None, payload=None, **kwargs):
    mark = await accepted(repo, link, message=message, update_id=update_id, topic=topic, payload=payload, **kwargs)
    return await repo.route(mark, kind="chat", target_id="original-thread", basis=basis or {"conversation_id": "original-thread"}, operation_ids=["a" * 32])


async def plan(db, link, *, state="pending", identity=None):
    identity = identity or uuid4().hex
    async with db.sessions() as session, session.begin():
        session.add(
            TelegramOutboxRow(
                id=identity,
                link_id=link.id,
                owner_user_id=link.owner_user_id,
                binding_generation=link.binding_generation,
                dedupe_key="notice:" + identity,
                class_="reply",
                payload={},
                parts=[],
                state=state,
                chat_id=link.chat_id,
                not_before=NOW,
                created_at=NOW,
            )
        )
    return identity


async def writer(db):
    # The public borrowed helpers require the caller's existing writer scope.
    session = db.sessions()
    await session.begin()
    if db.dialect == "sqlite":
        await session.execute(text("BEGIN IMMEDIATE"))
    await session.scalar(select(TelegramBotLinkRow).with_for_update())
    return session


async def test_accept_is_atomic_replay_and_detaches_all_saved_fields(db):
    repo, link = await setup(db)
    payload = {"text": "synthetic accepted text", "files": [{"file_id": "opaque-id", "filename": "test.txt"}]}
    mark, created = await repo.accept(link, incoming(topic=1), sanitized_input=payload)
    assert created is True and isinstance(mark, InboxRow)
    assert {field.name for field in dataclasses.fields(mark)} == set(TelegramInboundMarkRow.__table__.columns.keys())
    assert (mark.link_id, mark.message_id, mark.update_id, mark.chat_id, mark.message_thread_id, mark.binding_generation) == (link.id, 102, 202, ACCOUNT, None, link.binding_generation)
    assert mark.state == "accepted" and not mark.route_complete and mark.closed_at is None
    with pytest.raises(dataclasses.FrozenInstanceError):
        mark.state = "done"
    payload["files"][0]["filename"] = "mutated.txt"
    mark.sanitized_input["files"][0]["filename"] = "also-mutated.txt"
    second, created = await InboxRepository(db.sessions).accept(link, incoming(topic=1), sanitized_input={"text": "redelivered"})
    assert created is False and second.sanitized_input["files"][0]["filename"] == "test.txt"
    assert len(await rows(db, TelegramInboundMarkRow)) == 2  # retained Start plus content


async def test_concurrent_admission_has_one_creator_and_one_immutable_receipt(db):
    repo, link = await setup(db)
    results = await asyncio.gather(*(repo.accept(link, incoming(), sanitized_input={"text": f"candidate-{n}"}) for n in range(8)))
    assert sum(created for _, created in results) == 1
    assert all(mark == results[0][0] for mark, _ in results)


async def test_admission_commit_failure_leaves_no_acknowledgeable_identity(db):
    repo, link = await setup(db)

    def reject(session):
        if session.get_bind() is db.engine.sync_engine:
            raise RuntimeError("fixture-commit-refused")

    event.listen(Session, "before_commit", reject)
    try:
        with pytest.raises(RuntimeError, match="fixture-commit-refused"):
            await accepted(repo, link)
    finally:
        event.remove(Session, "before_commit", reject)
    assert len(await rows(db, TelegramInboundMarkRow)) == 1
    assert (await repo.accept(link, incoming(), sanitized_input={"text": "retry"}))[1] is True


async def test_admission_rechecks_a_binding_changed_while_waiting_for_its_writer(db):
    repo, link = await setup(db)
    holder = await writer(db)
    attempted = asyncio.Event()

    def capture(connection, cursor, statement, parameters, context, executemany):
        if statement == "BEGIN IMMEDIATE" or statement.startswith("SELECT telegram_bot_links."):
            attempted.set()

    event.listen(db.engine.sync_engine, "before_cursor_execute", capture)
    task = asyncio.create_task(accepted(repo, link))
    try:
        await asyncio.wait_for(attempted.wait(), 5)
        await holder.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(binding_generation=link.binding_generation + 1))
        await holder.commit()
        with pytest.raises(ValueError, match="link-changed"):
            await asyncio.wait_for(task, 10)
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", capture)
        await holder.close()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    assert len(await rows(db, TelegramInboundMarkRow)) == 1


@pytest.mark.parametrize("change", ["message", "update", "chat", "topic"])
async def test_admission_rejects_identity_collisions(db, change):
    repo, link = await setup(db)
    await accepted(repo, link)
    kwargs = {"message": 103} if change == "message" else {"update_id": 203} if change == "update" else {"account": ACCOUNT + 1} if change == "chat" else {"topic": 99}
    with pytest.raises(ValueError, match="inbox-identity-conflict"):
        await accepted(repo, link, **kwargs)
    assert len(await rows(db, TelegramInboundMarkRow)) == 2


@pytest.mark.parametrize("change", ["generation", "token", "owner", "connection", "account"])
async def test_first_insert_fences_stale_binding_token_and_account_context(db, change):
    repo, link = await setup(db)
    async with db.sessions() as session, session.begin():
        if change == "account":
            await session.execute(update(TelegramAccountRow).values(owner_user_id="other-owner"))
        elif change == "connection":
            await session.execute(update(ChannelConnectionRow).values(status="revoked"))
        else:
            values = {"binding_generation": link.binding_generation + 1} if change == "generation" else {"token_revision": link.token_revision + 1} if change == "token" else {"owner_user_id": "other-owner"}
            await session.execute(update(TelegramBotLinkRow).values(**values))
    with pytest.raises(ValueError, match="link-changed"):
        await accepted(repo, link)
    assert len(await rows(db, TelegramInboundMarkRow)) == 1


async def test_old_replay_returns_original_basis_without_reviving_an_ended_generation(db):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    await db.links.unbind(link, actor_id=link.owner_user_id)
    replay, created = await repo.accept(link, incoming(), sanitized_input={"text": "new request"})
    assert replay == dataclasses.replace(mark, route_complete=True, state="done", closed_at=replay.closed_at) and created is False
    with pytest.raises(ValueError, match="link-changed"):
        await repo.bind_turn([replay], turn_id="a" * 32, thread_id="new-thread")


async def test_same_message_id_in_a_new_chat_preserves_both_tombstones(db):
    repo, link = await setup(db)
    old = await routed(repo, link)
    waiting = await db.links.unbind(link, actor_id=link.owner_user_id)
    new = await bind_link(db, waiting, account=ACCOUNT + 1, message=101, update=301)
    mark = await accepted(repo, new, account=ACCOUNT + 1, update_id=302)
    assert mark.message_id == old.message_id and mark.chat_id != old.chat_id
    replay, created = await repo.accept(new, incoming(), sanitized_input={"text": "ignored"})
    assert replay == dataclasses.replace(old, route_complete=True, state="done", closed_at=replay.closed_at) and not created
    assert len(await rows(db, TelegramInboundMarkRow)) == 4


@pytest.mark.parametrize(
    "payload",
    [
        {"raw_update": {"message": "raw"}},
        {"files": [{"download_url": "https://example.invalid/raw"}]},
        {"voice": {"audio_bytes": "encoded-data"}},
        {"text": "900000" + ":" + "x" * 35},
        {"files": [{"file_id": b"audio bytes"}]},
        {"voice": "encoded-audio-is-not-a-descriptor"},
        {"voice": {"audio_data": "encoded-audio"}},
        {"text": float("nan")},
    ],
)
async def test_sanitized_input_rejects_raw_secret_url_and_byte_payloads(db, payload):
    repo, link = await setup(db)
    with pytest.raises(ValueError, match="unsafe-sanitized-input"):
        await accepted(repo, link, payload=payload)
    assert len(await rows(db, TelegramInboundMarkRow)) == 1


async def test_edited_content_never_enters_inbox(db):
    repo, link = await setup(db)
    with pytest.raises(ValueError, match="invalid-inbox-update"):
        await accepted(repo, link, edited=True)


async def test_route_is_immutable_and_independent_of_mutable_snapshots(db):
    repo, link = await setup(db)
    mark = await accepted(repo, link)
    basis = {"prompt_id": "saved-prompt", "revision": 4, "choices": ["one"]}
    result = await repo.route(mark, kind="prompt", target_id="saved-target", basis=basis, operation_ids=["a" * 32])
    basis["choices"].append("two")
    replay = await repo.route(mark, kind="prompt", target_id="saved-target", basis={"prompt_id": "saved-prompt", "revision": 4, "choices": ["one"]}, operation_ids=["a" * 32])
    assert result == replay
    with pytest.raises(ValueError, match="inbox-route-conflict"):
        await repo.route(mark, kind="chat", target_id="later-thread", basis={}, operation_ids=["b" * 32])
    assert (await repo.accept(link, incoming(), sanitized_input={}))[0] == result


async def test_competing_routes_commit_one_choice_and_never_replace_it(db):
    repo, link = await setup(db)
    mark = await accepted(repo, link)
    outcomes = await asyncio.gather(*(repo.route(mark, kind="prompt", target_id=f"prompt-{n}", basis={"revision": n}, operation_ids=[str(n) * 32]) for n in range(2)), return_exceptions=True)
    assert sum(isinstance(outcome, InboxRow) for outcome in outcomes) == 1
    loser = next(outcome for outcome in outcomes if isinstance(outcome, ValueError))
    assert str(loser) == "inbox-route-conflict"
    assert (await repo.accept(link, incoming(), sanitized_input={}))[0] == next(outcome for outcome in outcomes if isinstance(outcome, InboxRow))


async def test_immutable_basis_compares_json_types_without_conflating_true_and_one(db):
    repo, link = await setup(db)
    mark = await accepted(repo, link)
    await repo.route(mark, kind="prompt", target_id="saved-prompt", basis={"revision": 1}, operation_ids=[])
    with pytest.raises(ValueError, match="inbox-route-conflict"):
        await repo.route(mark, kind="prompt", target_id="saved-prompt", basis={"revision": True}, operation_ids=[])


async def test_group_basis_does_not_merge_json_boolean_with_numeric_revision(db):
    repo, link = await setup(db)
    first = await routed(repo, link, basis={"revision": 1})
    second = await routed(repo, link, message=103, update_id=203, basis={"revision": True})
    with pytest.raises(ValueError, match="inbox-group-conflict"):
        await repo.bind_turn([first, second], turn_id="a" * 32, thread_id="original-thread")


async def test_bind_turn_persists_exact_group_and_initial_publication_before_dispatch(db):
    repo, link = await setup(db)
    first = await routed(repo, link)
    second = await routed(repo, link, message=103, update_id=203)
    await repo.bind_turn([first, second], turn_id="a" * 32, thread_id="original-thread")
    publication = hashlib.sha256(("a" * 32 + ":m102").encode()).hexdigest()[:32]
    saved = [(await repo.accept(link, incoming(message=m, update_id=u), sanitized_input={}))[0] for m, u in [(102, 202), (103, 203)]]
    for mark in saved:
        assert (mark.turn_id, mark.thread_id, mark.state) == ("a" * 32, "original-thread", "dispatched")
        assert mark.route_basis == {"conversation_id": "original-thread", "current_publication_id": publication, "publications": [{"publication_id": publication, "event_id": "m102", "run_id": None}]}
        assert mark.grouped_message_ids == ([103] if mark.message_id == 102 else [102])
    await repo.bind_turn([first, second], turn_id="a" * 32, thread_id="original-thread")
    with pytest.raises(ValueError, match="inbox-turn-conflict"):
        await repo.bind_turn([first], turn_id="b" * 32, thread_id="later-thread")


@pytest.mark.parametrize("difference", ["topic", "basis", "reply", "prompt", "album"])
async def test_grouping_rejects_incompatible_captured_scope(db, difference):
    repo, link = await setup(db)
    first = await routed(repo, link)
    kwargs = (
        {"topic": 99}
        if difference == "topic"
        else {"basis": {"conversation_id": "other"}}
        if difference == "basis"
        else {"reply": 51}
        if difference == "reply"
        else {"basis": {"prompt_id": "other"}}
        if difference == "prompt"
        else {"payload": {"text": "text", "media_group_id": "album-1"}, "album": "album-1"}
    )
    second = await routed(repo, link, message=103, update_id=203, **kwargs)
    with pytest.raises(ValueError, match="inbox-group-conflict"):
        await repo.bind_turn([first, second], turn_id="a" * 32, thread_id="original-thread")
    assert all(row.turn_id is None for row in await rows(db, TelegramInboundMarkRow))


async def test_publication_retry_is_explicit_idempotent_and_fences_old_run_observers(db):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    turn = "a" * 32
    await repo.bind_turn([mark], turn_id=turn, thread_id="original-thread")
    first = hashlib.sha256((turn + ":m102").encode()).hexdigest()[:32]
    await repo.bind_run(turn_id=turn, publication_id=first, run_id="run-one")
    second = await repo.publish([mark], event_id="m102:r1", expected_publication_id=first)
    assert second == hashlib.sha256((turn + ":m102:r1").encode()).hexdigest()[:32]
    assert await repo.publish([mark], event_id="m102:r1", expected_publication_id=first) == second
    assert (await repo.accept(link, incoming(), sanitized_input={}))[0].run_id is None
    with pytest.raises(ValueError, match="inbox-publication-conflict"):
        await repo.bind_run(turn_id=turn, publication_id=first, run_id="late-old-run")
    with pytest.raises(ValueError, match="inbox-publication-conflict"):
        await repo.publish([mark], event_id="m102:r2", expected_publication_id=first)
    await repo.bind_run(turn_id=turn, publication_id=second, run_id="run-two")
    saved = (await repo.accept(link, incoming(), sanitized_input={}))[0]
    assert saved.run_id == "run-two" and saved.thread_id == "original-thread"
    assert [p["run_id"] for p in saved.route_basis["publications"]] == ["run-one", "run-two"]
    with pytest.raises(ValueError, match="inbox-run-conflict"):
        await repo.bind_run(turn_id=turn, publication_id=second, run_id="unrelated-run")


async def test_retry_event_sequence_and_interrupted_turn_cannot_be_overridden(db):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    await repo.bind_turn([mark], turn_id="a" * 32, thread_id="original-thread")
    initial = hashlib.sha256(("a" * 32 + ":m102").encode()).hexdigest()[:32]
    for event_id in ("m102:r2", "m999:r1", "cother-callback"):
        with pytest.raises(ValueError, match="inbox-publication-conflict"):
            await repo.publish([mark], event_id=event_id, expected_publication_id=initial)
    async with db.sessions() as session, session.begin():
        await session.execute(update(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == 102).values(state="interrupted"))
    with pytest.raises(ValueError, match="inbox-route-complete"):
        await repo.publish([mark], event_id="m102:r1", expected_publication_id=initial)


async def test_joined_group_cannot_be_extended_or_partially_republished(db):
    repo, link = await setup(db)
    first, second, third = [await routed(repo, link, message=102 + n, update_id=202 + n) for n in range(3)]
    await repo.bind_turn([first, second], turn_id="a" * 32, thread_id="original-thread")
    with pytest.raises(ValueError, match="inbox-turn-conflict"):
        await repo.bind_turn([first, second, third], turn_id="a" * 32, thread_id="original-thread")
    initial = hashlib.sha256(("a" * 32 + ":m102").encode()).hexdigest()[:32]
    with pytest.raises(ValueError, match="inbox-group-conflict"):
        await repo.publish([first], event_id="m102:r1", expected_publication_id=initial)


async def test_finish_waits_for_every_delivery_and_failed_delivery_stays_unresolved(db):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    auxiliary, final = await plan(db, link, state="sent"), await plan(db, link, state="failed")
    session = await writer(db)
    try:
        await repo.register_delivery([mark], outbox_id=auxiliary, session=session)
        await repo.register_delivery([mark], outbox_id=final, primary=True, session=session)
        await session.commit()
    finally:
        await session.close()
    await repo.finish_route(mark)
    saved = (await repo.accept(link, incoming(), sanitized_input={}))[0]
    assert saved.route_complete and saved.state == "reply_pending" and saved.closed_at is None
    assert saved.delivery_ids == [auxiliary, final] and saved.outbox_id == final
    session = await writer(db)
    try:
        await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == final).values(state="skipped"))
        await repo.settle_deliveries(link_id=link.id, session=session)
        await session.commit()
    finally:
        await session.close()
    saved = (await repo.accept(link, incoming(), sanitized_input={}))[0]
    assert saved.state == "done" and saved.closed_at is not None


async def test_auxiliary_delivery_cannot_finish_processing_and_missing_delivery_is_unresolved(db):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    delivery = await plan(db, link, state="sent")
    session = await writer(db)
    try:
        await repo.register_delivery([mark], outbox_id=delivery, session=session)
        await repo.settle_deliveries(link_id=link.id, session=session)
        await session.commit()
    finally:
        await session.close()
    assert (await repo.accept(link, incoming(), sanitized_input={}))[0].state != "done"
    async with db.sessions() as session, session.begin():
        await session.execute(update(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == 102).values(delivery_ids=[delivery, "f" * 32]))
    await repo.finish_route(mark)
    saved = (await repo.accept(link, incoming(), sanitized_input={}))[0]
    assert saved.route_complete and saved.state != "done" and saved.closed_at is None


async def test_empty_route_finishes_but_dispatched_model_needs_its_final_plan(db):
    repo, link = await setup(db)
    finished = await routed(repo, link)
    await repo.finish_route(finished)
    assert (await repo.accept(link, incoming(), sanitized_input={}))[0].state == "done"
    pending = await routed(repo, link, message=103, update_id=203)
    await repo.bind_turn([pending], turn_id="a" * 32, thread_id="original-thread")
    with pytest.raises(ValueError, match="inbox-route-unfinished"):
        await repo.finish_route(pending)


async def test_final_registration_seals_all_joined_sources_and_prevents_republication(db):
    repo, link = await setup(db)
    marks = [await routed(repo, link, message=102 + n, update_id=202 + n) for n in range(2)]
    await repo.bind_turn(marks, turn_id="a" * 32, thread_id="original-thread")
    delivery = await plan(db, link, state="sent")
    session = await writer(db)
    try:
        with pytest.raises(ValueError, match="inbox-group-conflict"):
            await repo.register_delivery(marks[:1], outbox_id=delivery, primary=True, finish=True, session=session)
        await repo.register_delivery(marks, outbox_id=delivery, primary=True, finish=True, session=session)
        await session.commit()
    finally:
        await session.close()
    assert all(mark.state == "done" and mark.route_complete and mark.outbox_id == delivery for mark in await rows(db, TelegramInboundMarkRow) if mark.message_id in (102, 103))
    with pytest.raises(ValueError, match="inbox-route-complete"):
        await repo.bind_turn(marks, turn_id="a" * 32, thread_id="original-thread")
    with pytest.raises(ValueError, match="inbox-route-complete"):
        await repo.publish(marks, event_id="m102:r1", expected_publication_id=hashlib.sha256(("a" * 32 + ":m102").encode()).hexdigest()[:32])


async def test_borrowed_registration_and_finish_are_rollback_safe_and_never_commit(db, monkeypatch):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    delivery = await plan(db, link)
    session = await writer(db)
    monkeypatch.setattr(session, "commit", AsyncMock(side_effect=AssertionError("helper committed")))
    try:
        await repo.register_delivery([mark], outbox_id=delivery, primary=True, session=session)
        await repo.finish_route(mark, session=session)
        await session.flush()
        await session.rollback()
    finally:
        await session.close()
    saved = (await repo.accept(link, incoming(), sanitized_input={}))[0]
    assert saved.delivery_ids == [] and not saved.route_complete and saved.outbox_id is None


async def test_borrowed_delivery_helpers_use_only_the_existing_checkout(db):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    delivery = await plan(db, link)
    session = await writer(db)

    def reject_checkout(connection, record, proxy):
        raise AssertionError("borrowed helper opened a database checkout")

    event.listen(db.engine.sync_engine, "checkout", reject_checkout)
    try:
        await repo.register_delivery([mark], outbox_id=delivery, primary=True, session=session)
        await repo.finish_route(mark, session=session)
        await repo.settle_deliveries(link_id=link.id, session=session)
    finally:
        event.remove(db.engine.sync_engine, "checkout", reject_checkout)
        await session.rollback()
        await session.close()


async def test_borrowed_settlement_does_not_depend_on_caller_autoflush(db):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    delivery = await plan(db, link, state="sent")
    session = await writer(db)
    session.autoflush = False
    try:
        await repo.register_delivery([mark], outbox_id=delivery, primary=True, finish=True, session=session)
        await session.commit()
    finally:
        await session.close()
    assert (await repo.accept(link, incoming(), sanitized_input={}))[0].state == "done"


async def test_borrowed_helpers_reject_a_session_without_a_writer_transaction(db):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    async with db.sessions() as session:
        for call in (repo.register_delivery([mark], outbox_id="a" * 32, session=session), repo.finish_route(mark, session=session), repo.settle_deliveries(link_id=link.id, session=session)):
            with pytest.raises(ValueError, match="existing writer transaction"):
                await call


async def test_registration_replay_dedupes_and_refuses_foreign_generation_and_completed_plan_mutation(db):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    delivery, other = await plan(db, link), await plan(db, link)
    session = await writer(db)
    try:
        await repo.register_delivery([mark], outbox_id=delivery, primary=True, session=session)
        await repo.register_delivery([mark], outbox_id=delivery, primary=True, session=session)
        with pytest.raises(ValueError, match="inbox-delivery-conflict"):
            await repo.register_delivery([mark], outbox_id=other, primary=True, session=session)
        await session.commit()
    finally:
        await session.close()
    assert (await repo.accept(link, incoming(), sanitized_input={}))[0].delivery_ids == [delivery]
    await repo.finish_route(mark)
    session = await writer(db)
    try:
        with pytest.raises(ValueError, match="inbox-route-complete"):
            await repo.register_delivery([mark], outbox_id=other, session=session)
    finally:
        await session.rollback()
        await session.close()


@pytest.mark.parametrize("difference", ["generation", "chat", "topic", "owner"])
async def test_delivery_registration_requires_its_saved_destination_and_binding(db, difference):
    repo, link = await setup(db)
    mark = await routed(repo, link)
    delivery = await plan(db, link)
    values = (
        {"binding_generation": link.binding_generation + 1} if difference == "generation" else {"chat_id": ACCOUNT + 1} if difference == "chat" else {"message_thread_id": 99} if difference == "topic" else {"owner_user_id": "other-owner"}
    )
    session = await writer(db)
    try:
        await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == delivery).values(**values))
        with pytest.raises(ValueError, match="inbox-delivery-conflict"):
            await repo.register_delivery([mark], outbox_id=delivery, session=session)
    finally:
        await session.rollback()
        await session.close()


async def test_settlement_accepts_existing_start_welcome_marks(db):
    repo, link = await setup(db)
    session = await writer(db)
    try:
        await session.execute(update(TelegramOutboxRow).values(state="sent"))
        await repo.settle_deliveries(link_id=link.id, session=session)
        await session.commit()
    finally:
        await session.close()
    mark = (await rows(db, TelegramInboundMarkRow))[0]
    assert mark.state == "done" and mark.route_complete and mark.closed_at is not None


async def test_delivery_topic_fallback_settles_without_rewriting_captured_source_topic(db):
    repo, link = await setup(db)
    mark = await routed(repo, link, topic=99)
    delivery = await plan(db, link)
    session = await writer(db)
    try:
        await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == delivery).values(message_thread_id=99))
        await repo.register_delivery([mark], outbox_id=delivery, primary=True, finish=True, session=session)
        # A vanished topic relocates the saved presentation to General. Its
        # acknowledged delivery still satisfies the original inbox dependency.
        await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == delivery).values(message_thread_id=None, state="sent"))
        await repo.settle_deliveries(link_id=link.id, session=session)
        await session.commit()
    finally:
        await session.close()
    saved = (await repo.accept(link, incoming(topic=99), sanitized_input={}))[0]
    assert saved.state == "done" and saved.message_thread_id == 99


@pytest.mark.parametrize("settled", [False, True])
async def test_plan_reply_replay_reuses_attached_delivery_after_authorized_topic_relocation(db, settled):
    repo, link = await setup(db)
    mark = await routed(repo, link, topic=99)
    turn_id = "a" * 32
    await repo.bind_turn([mark], turn_id=turn_id, thread_id="original-thread")
    current = (await repo.accept(link, incoming(topic=99), sanitized_input={}))[0]
    publication_id = current.route_basis["current_publication_id"]
    await repo.bind_run(turn_id=turn_id, publication_id=publication_id, run_id="original-run")
    source = {"thread_id": "original-thread", "run_id": "original-run", "publication_id": publication_id, "assistant_message_id": "exact-assistant"}
    parts = [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {"product": "HartMesh"}}}]
    outbox = Outbox(db.sessions, inbox=repo)
    delivery = await outbox.plan_reply(link, turn_id=turn_id, source=source, parts=parts, inbox_rows=[mark])
    claim_owner, claim_id = uuid4().hex, uuid4().hex
    session = await writer(db)
    try:
        await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == delivery).values(state="sending", claim_owner=claim_owner, claim_id=claim_id, claimed_at=NOW))
        await session.commit()
    finally:
        await session.close()
    await outbox.replace_presentation(link, delivery, expected_version=0, claim_owner=claim_owner, claim_id=claim_id, reason="topic-missing", parts=parts, message_thread_id=None)
    if settled:
        session = await writer(db)
        try:
            await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == delivery).values(state="sent"))
            await repo.settle_deliveries(link_id=link.id, session=session)
            await session.commit()
        finally:
            await session.close()
    before = (await repo.accept(link, incoming(topic=99), sanitized_input={}))[0]
    saved = next(row for row in await rows(db, TelegramOutboxRow) if row.id == delivery)
    assert saved.message_thread_id is None and saved.payload["replacement_reason"] == "topic-missing" and saved.payload["presentation_version"] == 1
    original_part_ids = [part["part_id"] for part in saved.parts]
    assert await outbox.plan_reply(link, turn_id=turn_id, source=source, parts=parts, inbox_rows=[mark]) == delivery
    after = (await repo.accept(link, incoming(topic=99), sanitized_input={}))[0]
    assert after == before
    assert after.message_thread_id == 99 and after.delivery_ids == [delivery] and after.outbox_id == delivery
    assert after.state == ("done" if settled else "reply_pending")
    assert len(await rows(db, TelegramOutboxRow)) == 2
    saved = next(row for row in await rows(db, TelegramOutboxRow) if row.id == delivery)
    assert saved.message_thread_id is None and [part["part_id"] for part in saved.parts] == original_part_ids
    for foreign_scope in ({"binding_generation": link.binding_generation + 1}, {"chat_id": ACCOUNT + 1}, {"owner_user_id": "other-owner"}):
        session = await writer(db)
        try:
            await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.id == delivery).values(**foreign_scope))
            with pytest.raises(ValueError, match="inbox-delivery-conflict"):
                await repo.register_delivery([mark], outbox_id=delivery, primary=True, finish=True, session=session)
        finally:
            await session.rollback()
            await session.close()


async def test_leftovers_keep_all_unfinished_ages_and_preserve_original_thread(db):
    repo, link = await setup(db)
    old = await routed(repo, link)
    await repo.bind_turn([old], turn_id="a" * 32, thread_id="original-thread")
    async with db.sessions() as session, session.begin():
        await session.execute(update(TelegramInboundMarkRow).where(TelegramInboundMarkRow.message_id == 102).values(accepted_at=NOW - timedelta(days=20)))
    saved = await repo.leftovers(before=NOW)
    assert len(saved) == 1 and saved[0].message_id == old.message_id and saved[0].thread_id == "original-thread"
    assert await repo.leftovers(before=NOW - timedelta(days=30)) == []


async def test_writer_locks_parent_before_reading_inbox_and_sqlite_reserves_before_any_read(db):
    repo, link = await setup(db)
    statements = []

    def capture(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(db.engine.sync_engine, "before_cursor_execute", capture)
    try:
        await accepted(repo, link)
    finally:
        event.remove(db.engine.sync_engine, "before_cursor_execute", capture)
    if db.dialect == "sqlite":
        assert statements[0] == "BEGIN IMMEDIATE"
    else:
        assert statements[0].startswith("SELECT telegram_bot_links.") and statements[0].endswith("FOR UPDATE")
        assert statements[1].startswith("SELECT telegram_inbound_marks.") and statements[1].endswith("FOR UPDATE")
