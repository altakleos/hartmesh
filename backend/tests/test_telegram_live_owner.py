"""Preparation owns cold work; SQL checks reread the same live human account."""

import asyncio
import threading
from datetime import UTC, datetime

import pytest
from _telegram_ingress_test_support import install_live_owner
from _telegram_repository_test_support import db as db
from sqlalchemy import event, update

from app.telegram_bots.authority import AccessOff, AuthorityUnavailable
from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow

pytestmark = pytest.mark.asyncio


async def test_prepared_checker_exposes_only_email_and_borrows_active_writer(db, monkeypatch):
    owner = await install_live_owner(db, monkeypatch)
    checker = await owner.authority.prepare_live_owner(owner.id)
    assert checker.email == "ingress@example.com"
    assert not hasattr(checker, "password_hash")
    async with db.sessions.begin() as writer:
        await checker(owner.id, writer)
        with pytest.raises(AccessOff):
            await checker("another-owner", writer)
        assert writer.in_transaction()


@pytest.mark.parametrize("reason", ["local-disabled", "provider-disabled", "released", "inert", "missing"])
async def test_prepared_checker_rejects_fresh_account_refusal(db, monkeypatch, reason):
    owner = await install_live_owner(db, monkeypatch, provider=reason == "provider-disabled", local_enabled=reason != "inert")
    if reason == "inert":
        with pytest.raises(AccessOff):
            await owner.authority.prepare_live_owner(owner.id)
        return
    checker = await owner.authority.prepare_live_owner(owner.id)
    async with db.sessions.begin() as session:
        if reason.endswith("disabled"):
            session.add(DisabledIdentityRow(issuer="https://issuer.example" if reason == "provider-disabled" else LOCAL_ACCOUNT_ISSUER, subject=owner.id, disabled_at=datetime.now(UTC)))
        elif reason == "released":
            await session.execute(update(UserRow).where(UserRow.id == owner.id).values(released_at=datetime.now(UTC)))
        else:
            await session.delete(await session.get(UserRow, owner.id))
    async with db.sessions.begin() as writer:
        with pytest.raises(AccessOff):
            await checker(owner.id, writer)


async def test_unreadable_configuration_cannot_prepare_a_checker(db, monkeypatch):
    from deerflow.config import app_config

    owner = await install_live_owner(db, monkeypatch)

    def fail():
        raise OSError("fixture unreadable configuration")

    monkeypatch.setattr(app_config, "get_app_config", fail)
    with pytest.raises(AuthorityUnavailable):
        await owner.authority.prepare_live_owner(owner.id)


async def test_provider_failure_is_strict_and_runs_on_the_original_loop(db, monkeypatch):
    from app.gateway import authz

    owner = await install_live_owner(db, monkeypatch)
    loop = asyncio.get_running_loop()

    async def fail(user, *, is_internal, prepared_config, strict):
        assert asyncio.get_running_loop() is loop
        assert str(user.id) == owner.id and not is_internal and strict
        assert prepared_config is owner.config.authorization
        raise RuntimeError("fixture policy failure")

    monkeypatch.setattr(authz, "resolve_route_permissions", fail)
    with pytest.raises(AuthorityUnavailable):
        await owner.authority.prepare_live_owner(owner.id)


async def test_checker_requires_active_same_engine_transaction(db, monkeypatch, tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    owner = await install_live_owner(db, monkeypatch)
    checker = await owner.authority.prepare_live_owner(owner.id)
    async with db.sessions() as inactive:
        with pytest.raises(AuthorityUnavailable):
            await checker(owner.id, inactive)
    foreign = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'foreign.sqlite'}")
    try:
        async with async_sessionmaker(foreign).begin() as writer:
            with pytest.raises(AuthorityUnavailable):
                await checker(owner.id, writer)
    finally:
        await foreign.dispose()


async def test_cancelled_preparation_drains_started_configuration_worker(db, monkeypatch):
    from deerflow.config import app_config

    owner = await install_live_owner(db, monkeypatch)
    entered, release, exited = threading.Event(), threading.Event(), threading.Event()
    loop_thread = threading.get_ident()

    def prepare():
        assert threading.get_ident() != loop_thread
        entered.set()
        try:
            assert release.wait(10)
            return owner.config
        finally:
            exited.set()

    monkeypatch.setattr(app_config, "get_app_config", prepare)
    task = asyncio.create_task(owner.authority.prepare_live_owner(owner.id))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert exited.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_cancelled_preparation_drains_first_preflight_account_cursor(db, monkeypatch):
    owner = await install_live_owner(db, monkeypatch)
    entered, release, exited = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def pause(driver):
        entered.set()
        try:
            await release.wait()
        finally:
            exited.set()

    def intercept(connection, cursor, statement, parameters, context, executemany):
        if statement.lstrip().upper().startswith("SELECT") and "FROM USERS" in statement.upper():
            connection.connection.dbapi_connection.run_async(pause)

    event.listen(db.engine.sync_engine, "before_cursor_execute", intercept)
    task = asyncio.create_task(owner.authority.prepare_live_owner(owner.id))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not exited.is_set(), "preflight account cursor was cancelled instead of drained"
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 3)
        assert exited.is_set()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        event.remove(db.engine.sync_engine, "before_cursor_execute", intercept)
