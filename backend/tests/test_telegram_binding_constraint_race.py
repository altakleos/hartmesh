"""A competing legacy channel bind must roll back the entire bot binding unit."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import add_link, code_of, rows, welcome
from _telegram_repository_test_support import db as db
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import operators

from deerflow.persistence.channel_connections.model import ChannelConnectionRow, ChannelOAuthStateRow
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkEventRow, TelegramInboundMarkRow, TelegramOutboxRow

pytestmark = pytest.mark.asyncio

ACCOUNT = 7_000_000_001
FOREIGN_OWNER = "foreign-owner"


async def _waiting_with_revoked_identity(db):
    link = await add_link(db)
    owned = await db.connections.upsert_connection(
        owner_user_id=link.owner_user_id,
        provider="telegram",
        external_account_id=str(ACCOUNT),
        workspace_id=str(link.bot_id),
        bot_user_id=str(link.bot_id),
        status="revoked",
    )
    code = code_of(await db.links.mint_code(link))
    return await db.links.get(link.id), owned["id"], code


async def _bind(db, link, code):
    return await db.links.bind(
        link,
        telegram_user_id=ACCOUNT,
        chat_id=ACCOUNT,
        first_name="Fixture",
        username="fixture_person",
        code=code,
        start_identity={"update_id": 201, "message_id": 101, "chat_id": ACCOUNT},
        hello_parts=welcome(),
    )


def _is_competing_owner_read(statement):
    if not getattr(statement, "is_select", False):
        return False
    columns = list(statement.selected_columns)
    if len(columns) != 1 or columns[0].table is not ChannelConnectionRow.__table__ or columns[0].name != "id":
        return False
    return any(getattr(criterion, "operator", None) is operators.ne and getattr(getattr(criterion, "left", None), "name", None) == "owner_user_id" for criterion in statement._where_criteria)


async def _assert_binding_unit_rolled_back(db, waiting, owned_id):
    assert await db.links.get(waiting.id) == waiting
    owned = next(row for row in await rows(db, ChannelConnectionRow) if row.id == owned_id)
    assert owned.status == "revoked" and owned.owner_user_id == waiting.owner_user_id
    for cls in (TelegramAccountRow, TelegramInboundMarkRow, TelegramOutboxRow):
        assert await rows(db, cls) == []
    assert not [row for row in await rows(db, TelegramBotLinkEventRow) if row.action == "connect"]
    codes = await rows(db, ChannelOAuthStateRow)
    assert len(codes) == 1 and codes[0].consumed_at is None


async def test_revoked_identity_reactivation_translates_only_after_complete_rollback(db, monkeypatch):
    waiting, owned_id, code = await _waiting_with_revoked_identity(db)
    foreign = None

    async def legacy_foreign_bind():
        return await db.connections.upsert_connection(
            owner_user_id=FOREIGN_OWNER,
            provider="telegram",
            external_account_id=str(ACCOUNT),
            workspace_id=str(waiting.bot_id),
            bot_user_id=str(waiting.bot_id),
        )

    if db.dialect == "sqlite":
        # BEGIN IMMEDIATE prevents a second writer during bind. Seed the
        # competitor and expose an earlier empty read to reach the real UPDATE
        # UNIQUE violation without pretending SQLite permits that interleaving.
        foreign = await legacy_foreign_bind()

    scalar = AsyncSession.scalar
    competing_reads = 0

    async def after_competing_read(session, statement, *args, **kwargs):
        nonlocal foreign, competing_reads
        result = await scalar(session, statement, *args, **kwargs)
        if session.bind is db.engine and _is_competing_owner_read(statement):
            competing_reads += 1
            assert competing_reads == 1
            if db.dialect == "postgres":
                assert result is None
                # The legacy repository uses execute rather than scalar and
                # leaves this owner's revoked row untouched. Its own transaction
                # really commits after our completed empty competing-owner read.
                foreign = await legacy_foreign_bind()
            else:
                assert result == foreign["id"]
            return None
        return result

    monkeypatch.setattr(AsyncSession, "scalar", after_competing_read)
    with pytest.raises(ValueError, match="^account-taken$"):
        await _bind(db, waiting, code)
    assert competing_reads == 1
    await _assert_binding_unit_rolled_back(db, waiting, owned_id)
    bindings = await rows(db, ChannelConnectionRow)
    assert len(bindings) == 2
    competitor = next(row for row in bindings if row.id == foreign["id"])
    assert competitor.owner_user_id == FOREIGN_OWNER and competitor.status == "connected"


async def test_unrelated_event_integrity_failure_is_not_translated_and_rolls_back(db, monkeypatch):
    waiting, owned_id, code = await _waiting_with_revoked_identity(db)
    original_events = await rows(db, TelegramBotLinkEventRow)
    assert len(original_events) == 1 and original_events[0].action == "add"

    async def duplicate_event(session, link, action, *, actor_id, **kwargs):
        assert action == "connect"
        # Exercise an actual unrelated database constraint after account and
        # connection writes, rather than raising a synthetic IntegrityError.
        await session.execute(
            insert(TelegramBotLinkEventRow).values(
                id=original_events[0].id,
                link_id=link.id,
                owner_user_id=link.owner_user_id,
                bot_id=link.bot_id,
                actor_id=actor_id,
                action=action,
                detail={},
                created_at=datetime.now(UTC),
            )
        )

    monkeypatch.setattr(db.links, "_event", duplicate_event)
    with pytest.raises(IntegrityError):
        await _bind(db, waiting, code)
    await _assert_binding_unit_rolled_back(db, waiting, owned_id)
    assert len(await rows(db, ChannelConnectionRow)) == 1
    events = await rows(db, TelegramBotLinkEventRow)
    assert len(events) == 1 and events[0].id == original_events[0].id
