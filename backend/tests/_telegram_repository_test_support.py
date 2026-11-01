"""Disposable SQL backends for Telegram repository tests; no runtime bootstrap."""

import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import event, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.links import BotLinkRepository
from app.telegram_bots.settings import SettingsRepository
from deerflow.persistence.base import Base
from deerflow.persistence.channel_connections.model import ChannelConnectionRow, ChannelConversationRow, ChannelCredentialRow, ChannelOAuthStateRow
from deerflow.persistence.channel_connections.sql import ChannelConnectionRepository
from deerflow.persistence.telegram_bots import model

TABLES = [
    ChannelConnectionRow.__table__,
    ChannelConversationRow.__table__,
    ChannelCredentialRow.__table__,
    ChannelOAuthStateRow.__table__,
    *(row.__table__ for row in vars(model).values() if isinstance(row, type) and row.__module__ == model.__name__ and hasattr(row, "__table__")),
]


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def db(request, tmp_path):
    schema = "telegram_links_" + uuid4().hex
    admin = None
    if request.param == "postgres":
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("TEST_POSTGRES_URI is not configured")
        uri = make_url(uri).set(drivername="postgresql+asyncpg")
        ssl = {"ssl": False} if uri.query.get("sslmode") == "disable" else {}
        uri = uri.difference_update_query(["sslmode"])
        admin = create_async_engine(uri, connect_args=ssl)
        async with admin.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        engine = create_async_engine(uri, connect_args={**ssl, "server_settings": {"search_path": schema}})
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'telegram.sqlite'}", connect_args={"timeout": 30})

        @event.listens_for(engine.sync_engine, "connect")
        def pragmas(connection, record):
            # Production's pragmas, so a commit costs here what it costs there
            # and a slow disk does not decide how long these tests take.
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=30000")

    try:
        async with engine.begin() as connection:
            if request.param == "sqlite":
                # sqlite3's legacy transaction mode otherwise autocommits each
                # DDL statement separately. Group fixture creation, while all
                # repository transactions retain normal disk-backed behavior.
                await connection.execute(text("BEGIN"))
            await connection.run_sync(lambda sync: Base.metadata.create_all(sync, tables=TABLES))
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        config = TelegramBotsConfig()
        connections = ChannelConnectionRepository(sessions)
        yield SimpleNamespace(
            engine=engine,
            sessions=sessions,
            connections=connections,
            config=config,
            settings=SettingsRepository(sessions, config),
            links=BotLinkRepository(sessions, connections, config),
            dialect=request.param,
        )
    finally:
        await engine.dispose()
        if admin is not None:
            async with admin.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
            await admin.dispose()


async def add_link(db, *, owner="owner-a", bot=9_000_000_001, target=None, **kwargs):
    await db.settings.ensure(owner, timezone="America/New_York", locale="en-US")
    return await db.links.add(
        owner_user_id=owner,
        agent_kind="custom_agent",
        agent_ref=target or f"target-{bot}",
        bot_id=bot,
        bot_username=f"fixture_{bot}_bot",
        encrypted_token=f"wrapped-fixture-{bot}",
        actor_id=owner,
        **kwargs,
    )


async def rows(db, cls):
    async with db.sessions() as session:
        return list((await session.execute(select(cls))).scalars())


def code_of(connect):
    from urllib.parse import parse_qs, urlsplit

    return parse_qs(urlsplit(connect.url).query)["start"][0]


def welcome(key="hello.custom_agent"):
    return [{"source_ref": {"key": key, "values": {"product": "HartMesh"}}}, {"source_ref": {"key": "hello.privacy", "values": {}}, "silent": True}]


async def bind_link(db, link, *, account=7_000_000_001, message=101, update=201):
    code = code_of(await db.links.mint_code(link))
    return await db.links.bind(
        await db.links.get(link.id),
        telegram_user_id=account,
        chat_id=account,
        first_name="Fixture",
        username="fixture_person",
        code=code,
        start_identity={"update_id": update, "message_id": message, "chat_id": account},
        hello_parts=welcome(),
    )
