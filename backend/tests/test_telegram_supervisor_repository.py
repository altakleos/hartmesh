"""Supervisor lifecycle and notice claims retain their exact SQL binding basis."""

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta, timezone

import pytest
from _telegram_repository_test_support import add_link, bind_link, rows
from _telegram_repository_test_support import db as db
from sqlalchemy import event, update
from sqlalchemy.orm import Session

from app.telegram_bots.links import BotLinkRepository
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkEventRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPendingFileRow, TelegramWorkTopicRow

pytestmark = pytest.mark.asyncio
NOW = datetime(2030, 1, 2, 12, tzinfo=UTC)


def another_repository(db):
    return BotLinkRepository(db.sessions, db.connections, db.config)


async def hold(repo, link):
    return await repo.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)


async def release(repo, link):
    return await repo.release_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)


async def notice(repo, link, key="link.conflict", **kwargs):
    return await repo.mark_notice_if_current(link.id, key, token_revision=link.token_revision, binding_generation=link.binding_generation, at=kwargs.pop("at", NOW), **kwargs)


@pytest.mark.parametrize("state,binding_status", [("connected", "connected"), ("connected", "held"), ("blocked", "held")])
async def test_hold_atomically_fences_old_work_and_preserves_bound_identity(db, state, binding_status):
    link = await bind_link(db, await add_link(db))
    other = await bind_link(db, await add_link(db, bot=link.bot_id + 1), message=102, update=202)
    async with db.sessions.begin() as session:
        await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(state=state))
        await session.execute(update(ChannelConnectionRow).where(ChannelConnectionRow.id == link.connection_id).values(status=binding_status))
        session.add(TelegramCallbackRow(id="a" * 32, group_id="b" * 32, link_id=link.id, chat_id=link.chat_id, binding_generation=link.binding_generation, kind="fixture", created_at=NOW))
        session.add(
            TelegramOutboxRow(
                id="c" * 32,
                link_id=link.id,
                owner_user_id=link.owner_user_id,
                dedupe_key="fixture",
                class_="reply",
                binding_generation=link.binding_generation,
                state="sending",
                not_before=NOW,
                claim_owner="old-worker",
                claim_id="old-admission",
                created_at=NOW,
            )
        )
        session.add(
            TelegramPendingFileRow(
                link_id=link.id, message_id=1, chat_id=link.chat_id, binding_generation=link.binding_generation, file_id="file", file_unique_id="unique", filename="f.txt", mime_type="text/plain", size=1, is_image=False, created_at=NOW
            )
        )
        session.add(TelegramWorkTopicRow(link_id=link.id, work_id="w" * 32, chat_id=link.chat_id, binding_generation=link.binding_generation, creation_state="ready", name="fixture", created_at=NOW))
    result = await hold(db.links, link)
    assert result.state == "held" and result.binding_generation == link.binding_generation + 1
    assert (result.connection_id, result.telegram_user_id, result.chat_id, result.connected_name) == (link.connection_id, link.telegram_user_id, link.chat_id, link.connected_name)
    assert result.token_revision == link.token_revision and result.health == link.health
    bindings = {row.id: row for row in await rows(db, ChannelConnectionRow)}
    assert bindings[link.connection_id].status == "held" and bindings[other.connection_id].status == "connected"
    deliveries = await rows(db, TelegramOutboxRow)
    assert all(row.state == "skipped" and row.skip_reason == "disabled" and row.claim_owner is None and row.claim_id is None for row in deliveries if row.link_id == link.id)
    assert all(row.state == "pending" for row in deliveries if row.link_id == other.id)
    assert (await rows(db, TelegramCallbackRow))[0].consumed_at is not None
    assert not await rows(db, TelegramPendingFileRow) and not await rows(db, TelegramWorkTopicRow)
    assert [(row.link_id, row.action) for row in await rows(db, TelegramBotLinkEventRow) if row.action == "hold"] == [(link.id, "hold")]
    assert await hold(db.links, link) is None
    assert await hold(db.links, result) is None


@pytest.mark.parametrize("operation", ["hold", "release", "notice"])
@pytest.mark.parametrize(
    "changes",
    [{"owner_user_id": "foreign"}, {"provider": "slack"}, {"external_account_id": "99"}, {"workspace_id": "99"}, {"status": "revoked"}, pytest.param({"bot_user_id": "99"}, id="foreign-bot-user")],
)
async def test_lifecycle_and_notice_recheck_exact_connection_authority(db, changes, operation):
    link = await bind_link(db, await add_link(db))
    if operation == "release":
        link = await hold(db.links, link)
        await db.connections.restore_held_connection(link.connection_id, owner_user_id=link.owner_user_id)
    async with db.sessions.begin() as session:
        await session.execute(update(ChannelConnectionRow).where(ChannelConnectionRow.id == link.connection_id).values(**changes))
    before = await db.links.get(link.id)
    result = await {"hold": hold, "release": release, "notice": notice}[operation](db.links, link)
    assert result is (False if operation == "notice" else None)
    assert await db.links.get(link.id) == before
    assert not [row for row in await rows(db, TelegramBotLinkEventRow) if row.action == operation]


async def test_hold_commit_failure_rolls_back_binding_generation_and_deliveries(db):
    link = await bind_link(db, await add_link(db))

    def refuse(session):
        if session.get_bind() is db.engine.sync_engine:
            raise RuntimeError("fixture-lifecycle-commit")

    event.listen(Session, "before_commit", refuse)
    try:
        with pytest.raises(RuntimeError, match="fixture-lifecycle-commit"):
            await hold(db.links, link)
    finally:
        event.remove(Session, "before_commit", refuse)
    assert await db.links.get(link.id) == link
    assert (await rows(db, ChannelConnectionRow))[0].status == "connected"
    assert all(row.state == "pending" for row in await rows(db, TelegramOutboxRow))
    assert not [row for row in await rows(db, TelegramBotLinkEventRow) if row.action == "hold"]


async def test_release_only_joins_the_exact_binding_already_restored_by_operator(db):
    original = await bind_link(db, await add_link(db))
    held = await hold(db.links, original)
    assert await release(db.links, held) is None
    assert (await rows(db, ChannelConnectionRow))[0].status == "held"
    assert await db.connections.restore_held_connection(held.connection_id, owner_user_id=held.owner_user_id) == "restored"
    current = await release(db.links, held)
    assert current.state == "connected" and current.binding_generation > original.binding_generation
    assert (current.connection_id, current.telegram_user_id, current.chat_id) == (original.connection_id, original.telegram_user_id, original.chat_id)
    assert all(row.state == "skipped" and row.skip_reason == "disabled" for row in await rows(db, TelegramOutboxRow))
    assert [row.action for row in await rows(db, TelegramBotLinkEventRow) if row.action == "release"] == ["release"]
    assert await release(db.links, held) is None


@pytest.mark.parametrize("operation", ["hold", "notice", "release"])
@pytest.mark.parametrize("change", ["token", "binding"])
async def test_delayed_lifecycle_writer_rechecks_fences_after_another_repository_changed_them(db, monkeypatch, operation, change):
    link = await bind_link(db, await add_link(db))
    if operation == "release":
        link = await hold(db.links, link)
        await db.connections.restore_held_connection(link.connection_id, owner_user_id=link.owner_user_id)
    entered, resume = asyncio.Event(), asyncio.Event()
    writer = db.links._owner_writer

    @asynccontextmanager
    async def paused(owner):
        entered.set()
        await resume.wait()
        async with writer(owner) as session:
            yield session

    monkeypatch.setattr(db.links, "_owner_writer", paused)
    call = {"hold": hold, "notice": notice, "release": release}[operation]
    task = asyncio.create_task(call(db.links, link))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        other = another_repository(db)
        if change == "token":
            current = await other.store_token(link.id, "wrapped-replacement", actor_id=link.owner_user_id)
        else:
            current = await other.unbind(link, actor_id=link.owner_user_id)
        resume.set()
        assert await asyncio.wait_for(task, 3) is (False if operation == "notice" else None)
        assert await db.links.get(link.id) == current
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize(
    "key,stored_key,value",
    [
        ("link.conflict", "conflict", None),
        ("link.conflict_over", "conflict_over", None),
        ("link.back_online", "back_online", None),
        ("access.off", "access_off", None),
        ("access.back", "access_back", None),
        ("policy.off", "policy", "off"),
        ("policy.on", "policy", "allowed"),
        ("policy.on_protected", "policy", "protected"),
        ("policy.protected", "policy", "protected"),
        ("policy.unprotected", "policy", "allowed"),
        ("link.removed", "removed", None),
        ("agent.deleted", "agent_deleted", None),
        ("link.token_rejected_other", "token_rejected_other", None),
    ],
)
async def test_notice_claims_use_closed_keys_and_documented_metadata_shape(db, key, stored_key, value):
    link = await bind_link(db, await add_link(db))
    assert await notice(db.links, link, key)
    expected = NOW.isoformat() if value is None else {"value": value, "at": NOW.isoformat()}
    assert (await db.links.get(link.id)).notices == {stored_key: expected}


async def test_notice_claim_serializes_competing_repositories_and_obeys_exact_interval(db):
    link = await bind_link(db, await add_link(db))
    outcomes = await asyncio.gather(*(notice(repo, link, minimum_interval_seconds=24 * 3600) for repo in (db.links, another_repository(db))))
    assert outcomes.count(True) == outcomes.count(False) == 1
    before = await db.links.get(link.id)
    for at in (NOW - timedelta(seconds=1), NOW, NOW + timedelta(hours=24) - timedelta(microseconds=1)):
        assert not await notice(db.links, link, at=at, minimum_interval_seconds=24 * 3600)
        assert await db.links.get(link.id) == before
    local = (NOW + timedelta(hours=24)).astimezone(timezone(timedelta(hours=3)))
    assert await notice(db.links, link, at=local, minimum_interval_seconds=24 * 3600)
    assert (await db.links.get(link.id)).notices["conflict"] == (NOW + timedelta(hours=24)).isoformat()
    assert await notice(db.links, link, "link.conflict_over", at=NOW)
    assert set((await db.links.get(link.id)).notices) == {"conflict", "conflict_over"}


async def test_notice_claim_commit_failure_does_not_consume_the_attempt(db):
    link = await bind_link(db, await add_link(db))

    def refuse(session):
        if session.get_bind() is db.engine.sync_engine:
            raise RuntimeError("fixture-notice-commit")

    event.listen(Session, "before_commit", refuse)
    try:
        with pytest.raises(RuntimeError, match="fixture-notice-commit"):
            await notice(db.links, link)
    finally:
        event.remove(Session, "before_commit", refuse)
    assert await db.links.get(link.id) == link
    assert await notice(db.links, link)


@pytest.mark.parametrize(
    "changes",
    [
        {"token_revision": True},
        {"token_revision": -1},
        {"binding_generation": "1"},
        {"binding_generation": -1},
        {"key": "arbitrary"},
        {"at": datetime(2030, 1, 2)},
        {"at": "later"},
        {"minimum_interval_seconds": True},
        {"minimum_interval_seconds": -1},
        {"minimum_interval_seconds": float("nan")},
        {"minimum_interval_seconds": float("inf")},
    ],
)
async def test_notice_rejects_ambiguous_fences_dates_and_unbounded_metadata(db, changes):
    link = await bind_link(db, await add_link(db))
    values = {"key": "link.conflict", "token_revision": link.token_revision, "binding_generation": link.binding_generation, "at": NOW, **changes}
    with pytest.raises(ValueError):
        await db.links.mark_notice_if_current(link.id, **values)
    assert await db.links.get(link.id) == link


async def test_lifecycle_and_notice_refuse_missing_or_unbound_rows_without_writes(db):
    link = await add_link(db)
    assert await hold(db.links, link) is None
    assert await release(db.links, link) is None
    assert not await notice(db.links, link)
    assert await db.links.get(link.id) == link
    await db.links.remove(link.id, actor_id=link.owner_user_id)
    assert await hold(db.links, link) is None
    assert await release(db.links, link) is None
    assert not await notice(db.links, link)


@pytest.mark.parametrize(
    "key,metadata",
    [
        ("link.conflict", {"conflict": "not-a-time"}),
        ("link.conflict", {"conflict": "2030-01-01T12:00:00"}),
        ("policy.off", {"policy": {"value": [], "at": NOW.isoformat()}}),
        ("policy.off", {"policy": {"value": "unknown", "at": NOW.isoformat()}}),
        ("policy.off", {"policy": {"value": "allowed", "at": "not-a-time"}}),
    ],
)
async def test_invalid_stored_notice_metadata_fails_closed_without_replacement(db, key, metadata):
    link = await bind_link(db, await add_link(db))
    async with db.sessions.begin() as session:
        await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(notices=metadata))
    before = await db.links.get(link.id)
    assert not await notice(db.links, link, key)
    assert await db.links.get(link.id) == before
