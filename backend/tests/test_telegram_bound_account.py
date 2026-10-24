"""Telegram-account custody and owner-wide connection revocation."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from _telegram_repository_test_support import add_link, bind_link, code_of, rows, welcome
from _telegram_repository_test_support import db as db
from sqlalchemy import update

from app.telegram_bots.links import BotLink
from deerflow.persistence.channel_connections.model import ChannelConnectionRow, ChannelConversationRow, ChannelOAuthStateRow
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotIntentRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPersonSettingsRow

pytestmark = pytest.mark.asyncio

ACCOUNT = 7_000_000_001


async def test_account_of_another_person_is_refused_and_not_transferred(db, monkeypatch):
    original = await bind_link(db, await add_link(db))
    contender = await add_link(db, owner="owner-b", bot=original.bot_id + 1)
    code = code_of(await db.links.mint_code(contender))
    upsert = AsyncMock(side_effect=AssertionError("a bot link must not transfer channel ownership"))
    monkeypatch.setattr(db.connections, "upsert_connection", upsert)
    with pytest.raises(ValueError, match="account-taken"):
        await db.links.bind(contender, telegram_user_id=ACCOUNT, chat_id=ACCOUNT, first_name="Other", username="other_person", code=code, start_identity={"update_id": 202, "message_id": 102, "chat_id": ACCOUNT}, hello_parts=welcome())
    assert await db.links.get(original.id) == original
    assert (await db.links.get(contender.id)).state == "waiting_connect"
    assert not upsert.called
    accounts = await rows(db, TelegramAccountRow)
    assert len(accounts) == 1 and accounts[0].owner_user_id == original.owner_user_id
    assert len(await rows(db, ChannelConnectionRow)) == len(await rows(db, TelegramInboundMarkRow)) == len(await rows(db, TelegramOutboxRow)) == 1
    waiting = [row for row in await rows(db, ChannelOAuthStateRow) if row.owner_user_id == contender.owner_user_id]
    assert len(waiting) == 1 and waiting[0].consumed_at is None


async def test_preexisting_foreign_channel_identity_is_never_transferred_when_account_row_is_missing(db, monkeypatch):
    link = await add_link(db)
    now = datetime.now(UTC)
    async with db.sessions.begin() as session:
        session.add(
            ChannelConnectionRow(
                id="f" * 32, owner_user_id="foreign-owner", provider="telegram", external_account_id=str(ACCOUNT), workspace_id=str(link.bot_id), bot_user_id=str(link.bot_id), status="connected", created_at=now, updated_at=now
            )
        )
    code = code_of(await db.links.mint_code(link))
    upsert = AsyncMock(side_effect=AssertionError("existing identity transfer was reached"))
    monkeypatch.setattr(db.connections, "upsert_connection", upsert)
    with pytest.raises(ValueError):
        await db.links.bind(link, telegram_user_id=ACCOUNT, chat_id=ACCOUNT, first_name="Fixture", username=None, code=code, start_identity={"update_id": 201, "message_id": 101, "chat_id": ACCOUNT}, hello_parts=welcome())
    existing = (await rows(db, ChannelConnectionRow))[0]
    assert (existing.id, existing.owner_user_id, existing.status) == ("f" * 32, "foreign-owner", "connected")
    assert not upsert.called
    assert not await rows(db, TelegramAccountRow)
    assert not await rows(db, TelegramInboundMarkRow)
    assert (await rows(db, ChannelOAuthStateRow))[0].consumed_at is None


async def test_distinct_owners_cannot_win_the_same_telegram_account_concurrently(db):
    contenders = [await add_link(db, owner=f"owner-{index}", bot=9_000_000_001 + index) for index in range(2)]
    codes = [code_of(await db.links.mint_code(link)) for link in contenders]
    start = asyncio.Event()

    async def compete(index):
        await start.wait()
        return await db.links.bind(
            contenders[index],
            telegram_user_id=ACCOUNT,
            chat_id=ACCOUNT,
            first_name="Fixture",
            username=None,
            code=codes[index],
            start_identity={"update_id": 201 + index, "message_id": 101 + index, "chat_id": ACCOUNT},
            hello_parts=welcome(),
        )

    tasks = [asyncio.create_task(compete(index)) for index in range(2)]
    start.set()
    outcomes = await asyncio.gather(*tasks, return_exceptions=True)
    successful = [outcome for outcome in outcomes if isinstance(outcome, BotLink)]
    refused = [outcome for outcome in outcomes if isinstance(outcome, ValueError)]
    assert len(successful) == len(refused) == 1 and str(refused[0]) == "account-taken"
    assert (await rows(db, TelegramAccountRow))[0].owner_user_id == successful[0].owner_user_id
    for cls in [ChannelConnectionRow, TelegramInboundMarkRow, TelegramOutboxRow]:
        assert len(await rows(db, cls)) == 1
    assert sum(row.consumed_at is not None for row in await rows(db, ChannelOAuthStateRow)) == 1


async def test_one_account_two_bots_has_separate_bindings_and_conversation_mappings(db):
    first = await bind_link(db, await add_link(db))
    second = await bind_link(db, await add_link(db, bot=first.bot_id + 1), message=102, update=202)
    assert first.connection_id != second.connection_id
    for link, thread in [(first, "a" * 32), (second, "b" * 32)]:
        await db.connections.set_thread_id(connection_id=link.connection_id, owner_user_id=link.owner_user_id, provider="telegram", external_conversation_id=str(ACCOUNT), thread_id=thread)
    assert len(await rows(db, ChannelConversationRow)) == 2
    assert await db.connections.get_thread_id(first.connection_id, str(ACCOUNT)) == "a" * 32
    assert await db.connections.get_thread_id(second.connection_id, str(ACCOUNT)) == "b" * 32
    await db.links.remove(first.id, actor_id=first.owner_user_id)
    successor = await bind_link(db, await add_link(db, bot=first.bot_id), message=103, update=203)
    assert successor.connection_id == first.connection_id, "the legacy connection identity is reused, but its old thread must be gone"
    assert await db.connections.get_thread_id(successor.connection_id, str(ACCOUNT)) is None
    assert await db.connections.get_thread_id(second.connection_id, str(ACCOUNT)) == "b" * 32


async def test_managed_creator_binding_requires_the_exact_connected_manager_account(db):
    manager = await bind_link(db, await add_link(db))
    child = await add_link(db, bot=manager.bot_id + 1, origin="managed", manager_link_id=manager.id, creator_telegram_user_id=ACCOUNT, bind_mode="creator")
    for account in [ACCOUNT + 1, ACCOUNT]:
        if account == ACCOUNT:
            await db.links.unbind(manager, actor_id=manager.owner_user_id)
        with pytest.raises(ValueError):
            await db.links.bind(child, telegram_user_id=account, chat_id=account, first_name="Fixture", username=None, start_identity={"update_id": 202, "message_id": 102, "chat_id": account}, hello_parts=welcome())
    assert (await db.links.get(child.id)).state == "waiting_connect"
    assert not [row for row in await rows(db, ChannelConnectionRow) if row.workspace_id == str(child.bot_id)]


async def test_creator_binding_without_a_code_uses_its_current_manager_owner(db):
    manager = await bind_link(db, await add_link(db))
    child = await add_link(db, bot=manager.bot_id + 1, origin="managed", manager_link_id=manager.id, creator_telegram_user_id=ACCOUNT, bind_mode="creator")
    bound = await db.links.bind(child, telegram_user_id=ACCOUNT, chat_id=ACCOUNT, first_name="Fixture", username=None, start_identity={"update_id": 202, "message_id": 102, "chat_id": ACCOUNT}, hello_parts=welcome())
    assert bound.state == "connected" and bound.owner_user_id == manager.owner_user_id and bound.telegram_user_id == manager.telegram_user_id
    assert bound.connection_id != manager.connection_id


async def test_disconnect_all_fences_connected_blocked_held_waiting_and_creator_authority(db):
    manager = await bind_link(db, await add_link(db))
    blocked = await bind_link(db, await add_link(db, bot=manager.bot_id + 1), message=102, update=202)
    held = await bind_link(db, await add_link(db, bot=manager.bot_id + 2), message=103, update=203)
    await db.links.update(blocked.id, state="blocked")
    async with db.sessions.begin() as session:
        await session.execute(update(ChannelConnectionRow).where(ChannelConnectionRow.id == held.connection_id).values(status="held"))
    await db.links.update(held.id, state="held")
    waiting = await add_link(db, bot=manager.bot_id + 3)
    await db.links.mint_code(waiting)
    creator = await add_link(db, bot=manager.bot_id + 4, origin="managed", manager_link_id=manager.id, creator_telegram_user_id=ACCOUNT, bind_mode="creator")
    foreign = await add_link(db, owner="owner-b", bot=manager.bot_id + 5)
    await db.links.mint_code(foreign)
    now = datetime.now(UTC)
    intent_id = uuid4().hex
    async with db.sessions.begin() as session:
        session.add(
            TelegramBotIntentRow(
                id=intent_id,
                owner_user_id=manager.owner_user_id,
                manager_link_id=manager.id,
                manager_binding_generation=manager.binding_generation,
                agent_kind="custom_agent",
                agent_ref="future-target",
                suggested_username="future_bot",
                suggested_name="Future",
                source="web",
                expires_at=now + timedelta(minutes=10),
                created_at=now,
            )
        )
    before = {link.id: link for link in await db.links.list_for_owner(manager.owner_user_id)}
    assert await db.links.disconnect_all(manager.owner_user_id, actor_id=manager.owner_user_id) == 5
    after = await db.links.list_for_owner(manager.owner_user_id)
    for link in after:
        assert link.state == "waiting_connect" and link.bind_mode == "code"
        assert link.binding_generation > before[link.id].binding_generation
        assert link.connection_id is None and link.telegram_user_id is None and link.chat_id is None and link.connected_name is None
        assert link.connect_expires_at is None
    assert await db.links.codes_waiting(manager.owner_user_id) == 0
    assert await db.links.codes_waiting(foreign.owner_user_id) == 1
    assert not await rows(db, TelegramAccountRow)
    assert all(row.status == "revoked" for row in await rows(db, ChannelConnectionRow))
    assert (await rows(db, TelegramBotIntentRow))[0].cancelled_at is not None
    with pytest.raises(ValueError):
        await db.links.bind(creator, telegram_user_id=ACCOUNT, chat_id=ACCOUNT, first_name="Fixture", username=None, start_identity={"update_id": 204, "message_id": 104, "chat_id": ACCOUNT}, hello_parts=welcome())
    assert (await bind_link(db, await db.links.get(waiting.id), message=105, update=205)).state == "connected"


async def test_disconnect_all_empty_owner_creates_no_settings_or_account(db):
    assert await db.links.disconnect_all("missing", actor_id="missing") == 0
    assert not await rows(db, TelegramPersonSettingsRow)
    assert not await rows(db, TelegramAccountRow)


@pytest.mark.parametrize("changed_accounts", [False, True])
async def test_remove_cleans_historical_unbound_conversations_before_reusing_a_bot(db, changed_accounts):
    original = await bind_link(db, await add_link(db))
    await db.connections.set_thread_id(connection_id=original.connection_id, owner_user_id=original.owner_user_id, provider="telegram", external_conversation_id=str(ACCOUNT), thread_id="old-first-thread")
    waiting = await db.links.unbind(original, actor_id=original.owner_user_id)
    second = None
    if changed_accounts:
        second = await bind_link(db, waiting, account=ACCOUNT + 1, message=102, update=202)
        await db.connections.set_thread_id(connection_id=second.connection_id, owner_user_id=second.owner_user_id, provider="telegram", external_conversation_id=str(ACCOUNT + 1), thread_id="old-second-thread")
        waiting = await db.links.unbind(second, actor_id=second.owner_user_id)
    other = await bind_link(db, await add_link(db, owner="other", bot=original.bot_id + 1), account=ACCOUNT + 10)
    await db.connections.set_thread_id(connection_id=other.connection_id, owner_user_id=other.owner_user_id, provider="telegram", external_conversation_id=str(other.chat_id), thread_id="kept-other-thread")
    assert waiting.connection_id is None
    await db.links.remove(waiting.id, actor_id=waiting.owner_user_id)
    rebound = await bind_link(db, await add_link(db, bot=original.bot_id), account=ACCOUNT, message=103, update=203)
    assert rebound.connection_id == original.connection_id
    assert await db.connections.get_thread_id(rebound.connection_id, str(ACCOUNT)) is None
    if changed_accounts:
        new_waiting = await db.links.unbind(rebound, actor_id=rebound.owner_user_id)
        rebound_second = await bind_link(db, new_waiting, account=ACCOUNT + 1, message=104, update=204)
        assert rebound_second.connection_id == second.connection_id
        assert await db.connections.get_thread_id(rebound_second.connection_id, str(ACCOUNT + 1)) is None
    assert await db.connections.get_thread_id(other.connection_id, str(other.chat_id)) == "kept-other-thread"
