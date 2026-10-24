"""Atomic bot binding, durable Start receipts, and stale-transport fences."""

import dataclasses
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from _telegram_repository_test_support import add_link, bind_link, code_of, rows, welcome
from _telegram_repository_test_support import db as db
from sqlalchemy import event, select, update
from sqlalchemy.orm import Session

from app.telegram_bots.links import BotLinkRepository
from deerflow.persistence.channel_connections.model import ChannelConnectionRow, ChannelOAuthStateRow
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkEventRow, TelegramBotLinkRow, TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio

ACCOUNT = 7_000_000_001


async def bind(db, link, code, *, account=ACCOUNT, chat=None, update_id=201, message_id=101, identity=None, parts=None):
    return await db.links.bind(
        link,
        telegram_user_id=account,
        chat_id=account if chat is None else chat,
        first_name="Fixture",
        username="fixture_person",
        code=code,
        start_identity=identity if identity is not None else {"update_id": update_id, "message_id": message_id, "chat_id": account if chat is None else chat},
        hello_parts=parts if parts is not None else welcome(),
    )


async def assert_no_binding(db):
    for cls in [TelegramAccountRow, ChannelConnectionRow, TelegramInboundMarkRow, TelegramOutboxRow]:
        assert await rows(db, cls) == []
    assert not [row for row in await rows(db, TelegramBotLinkEventRow) if row.action == "connect"]


async def test_binding_atomically_saves_private_identity_start_and_ordered_welcome(db, monkeypatch):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    separate_commit = AsyncMock(side_effect=AssertionError("binding opened an independently committing helper"))
    monkeypatch.setattr(db.connections, "upsert_connection", separate_commit)
    monkeypatch.setattr(db.connections, "consume_oauth_state", separate_commit)
    parts = welcome()
    parts[-1].pop("silent")
    bound = await bind(db, link, code, parts=parts)
    assert bound.state == "connected" and bound.binding_generation == link.binding_generation + 1
    assert (bound.telegram_user_id, bound.chat_id, bound.connected_name) == (ACCOUNT, ACCOUNT, "Fixture")
    account = (await rows(db, TelegramAccountRow))[0]
    assert (account.telegram_user_id, account.owner_user_id, account.first_name, account.username) == (ACCOUNT, link.owner_user_id, "Fixture", "fixture_person")
    connection = (await rows(db, ChannelConnectionRow))[0]
    assert (connection.id, connection.owner_user_id, connection.provider, connection.external_account_id, connection.workspace_id, connection.bot_user_id, connection.status) == (
        bound.connection_id,
        link.owner_user_id,
        "telegram",
        str(ACCOUNT),
        str(link.bot_id),
        str(link.bot_id),
        "connected",
    )
    mark = (await rows(db, TelegramInboundMarkRow))[0]
    plan = (await rows(db, TelegramOutboxRow))[0]
    assert (mark.link_id, mark.update_id, mark.message_id, mark.chat_id, mark.binding_generation) == (link.id, 201, 101, ACCOUNT, bound.binding_generation)
    assert mark.route_complete is True and mark.outbox_id == plan.id
    assert plan.class_ == "reply" and plan.state == "pending" and plan.binding_generation == bound.binding_generation
    assert [part["source_ref"] for part in plan.parts] == [part["source_ref"] for part in parts]
    assert plan.parts[-1]["silent"] is True
    assert (await rows(db, ChannelOAuthStateRow))[0].consumed_at is not None
    assert [row.action for row in await rows(db, TelegramBotLinkEventRow)].count("connect") == 1
    assert code not in json.dumps(mark.sanitized_input) + json.dumps(plan.payload) + json.dumps(plan.parts)
    assert not separate_commit.called


async def test_start_redelivery_replays_saved_binding_before_state_and_code_checks(db):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    bound = await bind(db, link, code)
    # Replay the originally captured waiting DTO and already-consumed code.
    replay = await bind(db, link, code)
    assert replay == bound
    assert len(await rows(db, TelegramAccountRow)) == len(await rows(db, ChannelConnectionRow)) == 1
    assert len(await rows(db, TelegramInboundMarkRow)) == len(await rows(db, TelegramOutboxRow)) == 1
    assert [row.action for row in await rows(db, TelegramBotLinkEventRow)].count("connect") == 1


async def test_old_start_redelivery_cannot_rebind_after_disconnect(db):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    bound = await bind(db, link, code)
    disconnected = await db.links.unbind(bound, actor_id=link.owner_user_id)
    with pytest.raises(ValueError, match="link-changed"):
        await bind(db, link, code)
    assert await db.links.get(link.id) == disconnected
    assert disconnected.state == "waiting_connect" and disconnected.connection_id is None
    assert len(await rows(db, TelegramOutboxRow)) == 1


async def test_binding_does_not_consume_a_code_without_its_atomic_effects(db):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    waiting = await db.links.get(link.id)

    def refuse_commit(session):
        if session.get_bind() is db.engine.sync_engine:
            raise RuntimeError("fixture-precommit-failure")

    event.listen(Session, "before_commit", refuse_commit)
    try:
        with pytest.raises(RuntimeError, match="fixture-precommit-failure"):
            await bind(db, link, code)
    finally:
        event.remove(Session, "before_commit", refuse_commit)
    assert await db.links.get(link.id) == waiting
    assert (await rows(db, ChannelOAuthStateRow))[0].consumed_at is None
    await assert_no_binding(db)
    assert (await bind(db, await db.links.get(link.id), code)).state == "connected"


@pytest.mark.parametrize("chat", [-100_000_001, ACCOUNT + 1])
async def test_private_chat_identity_is_required_before_code_consumption(db, chat):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    with pytest.raises(ValueError):
        await bind(db, link, code, chat=chat)
    await assert_no_binding(db)
    assert (await rows(db, ChannelOAuthStateRow))[0].consumed_at is None


@pytest.mark.parametrize(
    "identity",
    [
        {"update_id": 201, "message_id": 101, "chat_id": ACCOUNT + 1},
        {"update_id": 201, "message_id": 101, "chat_id": ACCOUNT, "raw_code": "forbidden"},
        {"update_id": 201, "message_id": 101, "chat_id": ACCOUNT, "raw_update": {"text": "forbidden"}},
    ],
)
async def test_binding_refuses_unsanitized_or_mismatched_start_identity(db, identity):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    with pytest.raises(ValueError):
        await bind(db, link, code, identity=identity)
    await assert_no_binding(db)
    assert (await rows(db, ChannelOAuthStateRow))[0].consumed_at is None


async def test_binding_refuses_a_welcome_without_the_privacy_part(db):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    with pytest.raises(ValueError):
        await bind(db, link, code, parts=welcome()[:-1])
    await assert_no_binding(db)


async def test_connect_code_is_once_only_even_when_used_on_another_bot(db):
    first = await add_link(db)
    other = await add_link(db, bot=first.bot_id + 1)
    code = code_of(await db.links.mint_code(first))
    async with db.links.capacity_writer(first.owner_user_id) as session:
        assert await db.links.consume_code(other, code, session=session) == "other-bot"
    async with db.links.capacity_writer(first.owner_user_id) as session:
        assert await db.links.consume_code(first, code, session=session) == "expired"
    await assert_no_binding(db)


async def test_consuming_a_successful_code_borrows_transaction_and_rolls_back(db):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    with pytest.raises(RuntimeError, match="rollback-consume"):
        async with db.links.capacity_writer(link.owner_user_id) as session:
            assert await db.links.consume_code(link, code, session=session) == "ok"
            raise RuntimeError("rollback-consume")
    assert (await rows(db, ChannelOAuthStateRow))[0].consumed_at is None
    assert (await bind(db, await db.links.get(link.id), code)).state == "connected"


async def test_connect_code_expires_after_its_saved_deadline(db):
    link = await add_link(db)
    before = datetime.now(UTC)
    connect = await db.links.mint_code(link)
    after = datetime.now(UTC)
    assert before + timedelta(seconds=600) <= connect.expires_at <= after + timedelta(seconds=600)
    with pytest.raises(dataclasses.FrozenInstanceError):
        connect.url = "changed"
    code = code_of(connect)
    assert len(code) == 32
    stored = (await rows(db, ChannelOAuthStateRow))[0]
    assert stored.state_hash == db.connections.hash_state(code) and code != stored.state_hash
    async with db.sessions.begin() as session:
        await session.execute(update(ChannelOAuthStateRow).values(expires_at=before - timedelta(seconds=1)))
    with pytest.raises(ValueError, match="expired"):
        await bind(db, await db.links.get(link.id), code)
    await assert_no_binding(db)


@pytest.mark.parametrize("field,value", [("connect_conflict_epoch", None), ("token_revision", None), ("connect_conflict_epoch", 99), ("token_revision", 99)])
async def test_missing_or_stale_code_metadata_cannot_write_binding_or_welcome(db, field, value):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    async with db.sessions.begin() as session:
        state = await session.scalar(select(ChannelOAuthStateRow))
        metadata = dict(state.metadata_json)
        if value is None:
            metadata.pop(field)
        else:
            metadata[field] = value
        state.metadata_json = metadata
    with pytest.raises(ValueError, match="expired"):
        await bind(db, await db.links.get(link.id), code)
    await assert_no_binding(db)


async def test_committed_conflict_epoch_survives_repository_restart_and_retires_old_code(db):
    link = await add_link(db)
    old_code = code_of(await db.links.mint_code(link))
    before = await db.links.get(link.id)
    assert await db.links.record_conflict(link.id, token_revision=link.token_revision)
    db.links = BotLinkRepository(db.sessions, db.connections, db.config)
    current = await db.links.get(link.id)
    assert current.connect_conflict_epoch == before.connect_conflict_epoch + 1
    for name in ["state", "health", "conflict_since", "acts_paused_since", "connection_id", "binding_generation", "notices"]:
        assert getattr(current, name) == getattr(before, name)
    assert current.connect_expires_at is None and await db.links.codes_waiting(link.owner_user_id) == 0
    with pytest.raises(ValueError, match="expired"):
        await bind(db, current, old_code)
    await assert_no_binding(db)
    fresh = code_of(await db.links.mint_code(current))
    assert (await bind(db, await db.links.get(link.id), fresh)).state == "connected"


async def test_failed_conflict_write_leaves_code_valid_until_the_fence_commits(db):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    before = await db.links.get(link.id)

    def refuse_commit(session):
        if session.get_bind() is db.engine.sync_engine:
            raise RuntimeError("fixture-conflict-write")

    event.listen(Session, "before_commit", refuse_commit)
    try:
        with pytest.raises(RuntimeError, match="fixture-conflict-write"):
            await db.links.record_conflict(link.id, token_revision=link.token_revision)
    finally:
        event.remove(Session, "before_commit", refuse_commit)
    assert await db.links.get(link.id) == before
    assert (await rows(db, ChannelOAuthStateRow))[0].consumed_at is None
    assert await db.links.record_conflict(link.id, token_revision=link.token_revision)
    with pytest.raises(ValueError, match="expired"):
        await bind(db, await db.links.get(link.id), code)


async def test_old_token_conflict_cannot_invalidate_new_revision_or_a_removed_link(db):
    link = await add_link(db)
    replacement = await db.links.store_token(link.id, "rewrapped-new-logical-token", actor_id=link.owner_user_id)
    code = code_of(await db.links.mint_code(replacement))
    before = await db.links.get(link.id)
    assert not await db.links.record_conflict(link.id, token_revision=link.token_revision)
    assert await db.links.get(link.id) == before
    assert (await bind(db, before, code)).state == "connected"
    await db.links.remove(link.id, actor_id=link.owner_user_id)
    assert not await db.links.record_conflict(link.id, token_revision=replacement.token_revision)


async def test_exhausted_conflict_epoch_refuses_mint_bind_and_further_conflicts(db):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    maximum = 2**63 - 1
    async with db.sessions.begin() as session:
        await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(connect_conflict_epoch=maximum))
        state = await session.scalar(select(ChannelOAuthStateRow))
        state.metadata_json = {**state.metadata_json, "connect_conflict_epoch": maximum}
    current = await db.links.get(link.id)
    for operation in [lambda: db.links.mint_code(current), lambda: bind(db, current, code), lambda: db.links.record_conflict(link.id, token_revision=current.token_revision)]:
        with pytest.raises(RuntimeError, match="conflict-epoch-exhausted"):
            await operation()
    assert (await db.links.get(link.id)).connect_conflict_epoch == maximum
    await assert_no_binding(db)


@pytest.mark.parametrize("changed", ["chat_id", "binding_generation", "token_revision"])
async def test_blocked_state_requires_exact_current_private_delivery_context(db, changed):
    bound = await bind_link(db, await add_link(db))
    context = {"link_id": bound.id, "chat_id": bound.chat_id, "binding_generation": bound.binding_generation, "token_revision": bound.token_revision}
    stale = {**context, changed: context[changed] + 1}
    assert not await db.links.mark_blocked_if_current(**stale)
    assert await db.links.get(bound.id) == bound
    assert await db.links.mark_blocked_if_current(**context)
    assert (await db.links.get(bound.id)).state == "blocked"
    assert not await db.links.mark_blocked_if_current(**context)


async def test_reconnecting_another_account_with_the_same_chat_local_message_id_retains_both_starts(db):
    original = await bind_link(db, await add_link(db), account=ACCOUNT, message=101, update=201)
    waiting = await db.links.unbind(original, actor_id=original.owner_user_id)
    current = await bind_link(db, waiting, account=ACCOUNT + 1, message=101, update=202)
    assert current.id == original.id and current.telegram_user_id == ACCOUNT + 1
    assert current.binding_generation > original.binding_generation
    marks = await rows(db, TelegramInboundMarkRow)
    assert {(row.chat_id, row.message_id, row.update_id) for row in marks} == {(ACCOUNT, 101, 201), (ACCOUNT + 1, 101, 202)}
    assert len({row.outbox_id for row in marks}) == 2
    assert len(await rows(db, TelegramOutboxRow)) == 2
    assert all(row.route_complete for row in marks)
    with pytest.raises(ValueError, match="link-changed"):
        await db.links.bind(original, telegram_user_id=ACCOUNT, chat_id=ACCOUNT, first_name="Fixture", username=None, start_identity={"update_id": 201, "message_id": 101, "chat_id": ACCOUNT}, hello_parts=welcome())
    assert await db.links.get(current.id) == current
    assert len(await rows(db, TelegramInboundMarkRow)) == 2
