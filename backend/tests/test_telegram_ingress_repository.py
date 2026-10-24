"""Current owner and complete SQL binding fences govern reconnection."""

import asyncio
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta

import pytest
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import add_link, bind_link, code_of, rows, welcome
from _telegram_repository_test_support import db as db
from sqlalchemy import event, update

from app.telegram_bots.authority import AccessOff
from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
from deerflow.persistence.channel_connections.model import ChannelConnectionRow, ChannelOAuthStateRow
from deerflow.persistence.channel_connections.sql import ChannelConnectionRepository
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkEventRow, TelegramBotLinkRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.persistence.user.access import DisabledIdentityRow

pytestmark = pytest.mark.asyncio


async def blocked(db, monkeypatch):
    owner = await install_live_owner(db, monkeypatch)
    link = await bind_link(db, await add_link(db, owner=owner.id))
    await db.links.mark_blocked_if_current(link_id=link.id, chat_id=link.chat_id, token_revision=link.token_revision, binding_generation=link.binding_generation)
    return owner, await db.links.get(link.id)


async def test_unblock_preserves_scope_pause_and_existing_deadlines(db, monkeypatch):
    owner, link = await blocked(db, monkeypatch)
    pause, later = datetime.now(UTC), datetime.now(UTC) + timedelta(days=1)
    async with db.sessions.begin() as session:
        await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(acts_paused_since=pause))
        await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id).values(not_before=later))
    link = await db.links.get(link.id)
    checker = await owner.authority.prepare_live_owner(owner.id)
    result = await db.links.mark_unblocked_if_current(link, check_live_owner=checker)
    assert result is not None
    before, after = asdict(link), asdict(result)
    for key in ("state", "updated_at"):
        before.pop(key)
        after.pop(key)
    assert before == after
    assert result.state == "connected"
    assert all(row.not_before.replace(tzinfo=UTC) == later for row in await rows(db, TelegramOutboxRow))
    assert await db.links.mark_unblocked_if_current(link, check_live_owner=checker) is None
    assert len([row for row in await rows(db, TelegramBotLinkEventRow) if row.action == "unblocked"]) == 1


@pytest.mark.parametrize(
    "column,value", [("owner_user_id", "changed"), ("bot_id", 9000009999), ("connection_id", None), ("chat_id", 7000000099), ("telegram_user_id", 7000000099), ("token_revision", 99), ("binding_generation", 99), ("state", "held")]
)
async def test_unblock_refuses_stale_link_scope(db, monkeypatch, column, value):
    owner, link = await blocked(db, monkeypatch)
    checker = await owner.authority.prepare_live_owner(owner.id)
    if column == "owner_user_id":
        captured = replace(link, owner_user_id=value)
    else:
        captured = link
        async with db.sessions.begin() as session:
            await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link.id).values(**{column: value}))
    assert await db.links.mark_unblocked_if_current(captured, check_live_owner=checker) is None
    assert not [row for row in await rows(db, TelegramBotLinkEventRow) if row.action == "unblocked"]


@pytest.mark.parametrize("column,value", [("owner_user_id", "other"), ("provider", "slack"), ("external_account_id", "12345"), ("workspace_id", "12345"), ("bot_user_id", "12345"), ("status", "held"), ("status", "revoked")])
async def test_unblock_requires_exact_connected_binding(db, monkeypatch, column, value):
    owner, link = await blocked(db, monkeypatch)
    checker = await owner.authority.prepare_live_owner(owner.id)
    async with db.sessions.begin() as session:
        await session.execute(update(ChannelConnectionRow).where(ChannelConnectionRow.id == link.connection_id).values(**{column: value}))
    assert await db.links.mark_unblocked_if_current(link, check_live_owner=checker) is None
    assert (await db.links.get(link.id)).state == "blocked"


async def test_unblock_requires_current_telegram_account_owner(db, monkeypatch):
    owner, link = await blocked(db, monkeypatch)
    checker = await owner.authority.prepare_live_owner(owner.id)
    async with db.sessions.begin() as session:
        await session.execute(update(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == link.telegram_user_id).values(owner_user_id="other"))
    assert await db.links.mark_unblocked_if_current(link, check_live_owner=checker) is None


async def test_unblock_rechecks_derived_disable_after_preparation(db, monkeypatch):
    owner, link = await blocked(db, monkeypatch)
    checker = await owner.authority.prepare_live_owner(owner.id)
    async with db.sessions.begin() as session:
        session.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=owner.id, disabled_at=datetime.now(UTC)))
    with pytest.raises(AccessOff):
        await db.links.mark_unblocked_if_current(link, check_live_owner=checker)
    assert (await db.links.get(link.id)).state == "blocked"
    assert not [row for row in await rows(db, TelegramBotLinkEventRow) if row.action == "unblocked"]


async def test_unblock_event_failure_rolls_back_state(db, monkeypatch):
    owner, link = await blocked(db, monkeypatch)
    checker = await owner.authority.prepare_live_owner(owner.id)
    original = db.links._event

    async def fail(session, row, action, **kwargs):
        if action == "unblocked":
            raise RuntimeError("fixture event failure")
        await original(session, row, action, **kwargs)

    monkeypatch.setattr(db.links, "_event", fail)
    with pytest.raises(RuntimeError, match="fixture event"):
        await db.links.mark_unblocked_if_current(link, check_live_owner=checker)
    assert (await db.links.get(link.id)).state == "blocked"


async def test_cancelled_unblock_drains_borrowed_account_cursor_before_rollback(db, monkeypatch):
    owner, link = await blocked(db, monkeypatch)
    checker = await owner.authority.prepare_live_owner(owner.id)
    entered, release = asyncio.Event(), asyncio.Event()

    async def pause(driver):
        entered.set()
        await release.wait()

    def intercept(connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "FROM USERS" in statement.upper():
            connection.connection.dbapi_connection.run_async(pause)

    event.listen(db.engine.sync_engine, "before_cursor_execute", intercept)
    task = asyncio.create_task(db.links.mark_unblocked_if_current(link, check_live_owner=checker))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
        assert (await db.links.get(link.id)).state == "blocked"
        assert not [row for row in await rows(db, TelegramBotLinkEventRow) if row.action == "unblocked"]
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(db.engine.sync_engine, "before_cursor_execute", intercept)


async def start(db, link, code, *, checker=None):
    return await db.links.bind(
        link, telegram_user_id=7000000001, chat_id=7000000001, first_name="Fixture", username=None, start_identity={"update_id": 301, "message_id": 201, "chat_id": 7000000001}, hello_parts=welcome(), code=code, check_live_owner=checker
    )


async def test_bind_rechecks_live_owner_before_consuming_code(db, monkeypatch):
    owner = await install_live_owner(db, monkeypatch)
    link = await add_link(db, owner=owner.id)
    code = code_of(await db.links.mint_code(link))
    checker = await owner.authority.prepare_live_owner(owner.id)
    async with db.sessions.begin() as session:
        session.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=owner.id, disabled_at=datetime.now(UTC)))
    with pytest.raises(AccessOff):
        await start(db, link, code, checker=checker)
    states = await rows(db, ChannelOAuthStateRow)
    assert next(row for row in states if row.state_hash == ChannelConnectionRepository.hash_state(code)).consumed_at is None
    assert not await rows(db, TelegramAccountRow)
    assert not await rows(db, ChannelConnectionRow)
    assert not await rows(db, TelegramOutboxRow)
    assert not await rows(db, TelegramInboundMarkRow)


async def test_wrong_bot_bind_commits_only_deliberate_code_consumption(db):
    intended = await add_link(db)
    wrong = await add_link(db, bot=intended.bot_id + 1)
    code = code_of(await db.links.mint_code(intended))
    # Use the existing public API so this is a behavioral red on current source.
    with pytest.raises(ValueError, match="other-bot"):
        await db.links.bind(wrong, telegram_user_id=7000000001, chat_id=7000000001, first_name="Fixture", username=None, start_identity={"update_id": 301, "message_id": 201, "chat_id": 7000000001}, hello_parts=welcome(), code=code)
    assert (await rows(db, ChannelOAuthStateRow))[0].consumed_at is not None
    assert not await rows(db, TelegramAccountRow)
    assert not await rows(db, ChannelConnectionRow)
    assert not await rows(db, TelegramOutboxRow)
    assert not await rows(db, TelegramInboundMarkRow)
    assert all(link.state == "waiting_connect" for link in await db.links.list_all())
    with pytest.raises(ValueError, match="expired"):
        await start(db, intended, code)


async def test_successful_code_exception_still_rolls_back_every_binding_write(db, monkeypatch):
    link = await add_link(db)
    code = code_of(await db.links.mint_code(link))
    original = db.links._event

    async def fail(session, row, action, **kwargs):
        if action == "connect":
            raise RuntimeError("fixture binding failure")
        await original(session, row, action, **kwargs)

    monkeypatch.setattr(db.links, "_event", fail)
    with pytest.raises(RuntimeError, match="fixture binding"):
        await start(db, link, code)
    assert (await rows(db, ChannelOAuthStateRow))[0].consumed_at is None
    assert not await rows(db, TelegramAccountRow)
    assert not await rows(db, ChannelConnectionRow)


async def test_account_taken_still_rolls_back_successful_code_consumption(db):
    await bind_link(db, await add_link(db))
    link = await add_link(db, owner="owner-b", bot=9000000002)
    code = code_of(await db.links.mint_code(link))
    with pytest.raises(ValueError, match="account-taken"):
        await start(db, link, code)
    states = await rows(db, ChannelOAuthStateRow)
    assert next(row for row in states if row.state_hash == ChannelConnectionRepository.hash_state(code)).consumed_at is None
    assert (await db.links.get(link.id)).state == "waiting_connect"
    assert len(await rows(db, ChannelConnectionRow)) == 1


async def test_bind_saved_start_replay_does_not_repeat_guard_or_welcome(db, monkeypatch):
    owner = await install_live_owner(db, monkeypatch)
    link = await add_link(db, owner=owner.id)
    code = code_of(await db.links.mint_code(link))
    checker = await owner.authority.prepare_live_owner(owner.id)
    result = await start(db, link, code, checker=checker)

    async def never_new_authority(owner_id, writer):
        pytest.fail("committed receipt replay attempted a new binding")

    repeated = await start(db, link, code, checker=never_new_authority)
    assert repeated.binding_generation == result.binding_generation
    assert len(await rows(db, TelegramOutboxRow)) == 1
    assert len(await rows(db, TelegramInboundMarkRow)) == 1


async def test_access_off_claim_is_atomic_and_only_a_new_hold_clears_it(db):
    link = await bind_link(db, await add_link(db))
    at = datetime.now(UTC)
    historical = {"access_off": (at - timedelta(days=1)).isoformat(), "back_online": (at - timedelta(days=2)).isoformat()}
    await db.links.update(link.id, notices=historical)
    held = await db.links.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
    assert held.notices == {"back_online": historical["back_online"]}

    async def claim(captured, when):
        return await db.links.mark_notice_if_current(captured.id, "access.off", token_revision=captured.token_revision, binding_generation=captured.binding_generation, at=when, only_if_unsent=True)

    results = await asyncio.gather(claim(held, at), claim(held, at + timedelta(seconds=1)))
    assert sum(results) == 1
    saved = await db.links.get(link.id)
    assert set(saved.notices) == {"access_off", "back_online"}
    assert isinstance(saved.notices["access_off"], str)
    assert not await claim(saved, at + timedelta(seconds=2))
    assert await db.links.hold_if_current(held.id, token_revision=held.token_revision, binding_generation=held.binding_generation) is None
    assert not await claim(await db.links.get(link.id), at + timedelta(seconds=3))
    assert await db.connections.restore_held_connection(held.connection_id, owner_user_id=held.owner_user_id) == "restored"
    restored = await db.links.release_if_current(held.id, token_revision=held.token_revision, binding_generation=held.binding_generation)
    later = await db.links.hold_if_current(restored.id, token_revision=restored.token_revision, binding_generation=restored.binding_generation)
    assert later.binding_generation > held.binding_generation and "access_off" not in later.notices
    assert not await claim(held, at + timedelta(seconds=4))
    assert not await claim(replace(later, token_revision=later.token_revision + 1), at + timedelta(seconds=4))
    assert await claim(later, at + timedelta(seconds=4))


async def test_failed_new_hold_preserves_previous_access_off_claim(db, monkeypatch):
    link = await bind_link(db, await add_link(db))
    stamp = datetime.now(UTC).isoformat()
    link = await db.links.update(link.id, notices={"access_off": stamp})
    original = db.links._event

    async def fail(session, row, action, **kwargs):
        if action == "hold":
            raise RuntimeError("fixture hold failure")
        await original(session, row, action, **kwargs)

    monkeypatch.setattr(db.links, "_event", fail)
    with pytest.raises(RuntimeError, match="fixture hold"):
        await db.links.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
    assert await db.links.get(link.id) == link


async def test_only_if_unsent_is_closed_to_access_off(db):
    link = await bind_link(db, await add_link(db))
    with pytest.raises(ValueError, match="invalid-notice-claim"):
        await db.links.mark_notice_if_current(link.id, "access.back", token_revision=link.token_revision, binding_generation=link.binding_generation, at=datetime.now(UTC), only_if_unsent=True)
