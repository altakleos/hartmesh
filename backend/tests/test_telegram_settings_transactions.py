"""Actual SQL settings ownership; no callback or human authority is fabricated."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.mark.asyncio
async def test_borrowed_settings_update_obeys_caller_rollback(db):
    await db.settings.ensure("owner", timezone="UTC", locale="en-US")
    with pytest.raises(RuntimeError, match="caller rolls back"):
        async with db.sessions() as session, session.begin():
            if db.dialect == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            updated = await db.settings.update("owner", locale="zh-CN", session=session)
            assert updated.locale == "zh-CN" and session.in_transaction()
            raise RuntimeError("caller rolls back")
    assert (await db.settings.get("owner")).locale == "en-US"


@pytest.mark.asyncio
async def test_borrowed_settings_update_and_caller_commit_share_one_checkout(db):
    await db.settings.ensure("owner", timezone="UTC", locale="en-US")
    async with db.sessions() as session, session.begin():
        if db.dialect == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))
        else:
            # PostgreSQL begins lazily; observe an actual caller checkout.
            await session.execute(text("SELECT 1"))
        assert db.engine.sync_engine.pool.checkedout() == 1
        value = await db.settings.update("owner", locale="zh-CN", session=session)
        assert db.engine.sync_engine.pool.checkedout() == 1
        assert value.locale == "zh-CN" and session.in_transaction()
    assert (await db.settings.get("owner")).locale == "zh-CN"


@pytest.mark.asyncio
async def test_borrowed_settings_requires_active_same_engine_transaction(db):
    await db.settings.ensure("owner", timezone="UTC", locale="en-US")
    async with db.sessions() as session:
        with pytest.raises(ValueError, match="settings-writer-required"):
            await db.settings.update("owner", locale="zh-CN", session=session)
    assert (await db.settings.get("owner")).locale == "en-US"


@pytest.mark.asyncio
async def test_owned_settings_update_keeps_existing_validation_and_scope(db):
    await db.settings.ensure("owner", timezone="UTC", locale="en-US")
    await db.settings.ensure("other", timezone="UTC", locale="zh-CN")
    value = await db.settings.update("owner", locale="zh-CN")
    assert value.locale == "zh-CN"
    with pytest.raises(ValueError):
        await db.settings.update("owner", locale=True)
    with pytest.raises(ValueError):
        await db.settings.update("owner", unexpected=True)
    assert (await db.settings.get("other")).locale == "zh-CN"


@pytest.mark.asyncio
async def test_borrowed_settings_rejects_another_engine_before_sql(db, tmp_path):
    foreign = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'foreign-settings.sqlite'}")
    sessions = async_sessionmaker(foreign)
    try:
        async with sessions() as session, session.begin():
            with pytest.raises(ValueError, match="settings-writer-required"):
                await db.settings.update("owner", locale="zh-CN", session=session)
    finally:
        await foreign.dispose()
