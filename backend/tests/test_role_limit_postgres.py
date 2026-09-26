"""The role limit against a sign-in racing it, on live PostgreSQL (set DEERFLOW_TEST_POSTGRES_URL to run).

A PostgreSQL statement reads other tables as of its own start. A sign-in's
write that began before a limit committed -- held up on the account's row by
another writer, say -- would read no limit and store the claim's ``admin``
after the limit's own lowering had already run. The limit, its lift, a
sign-in and a first sign-in's insert therefore serialise on one advisory
lock per identity; these tests hold the other side open at the exact moment
that matters and assert the column the race used to leave at ``admin``.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from support.postgres import postgres_async_url

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-role-limit-pg-min-32-chars")

from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence.engine import close_engine, get_engine, get_session_factory, init_engine_from_config
from deerflow.persistence.user.access import identity_lock_key

POSTGRES_URL = os.getenv("DEERFLOW_TEST_POSTGRES_URL")
ISSUER = "https://login.example.com"

pytestmark = pytest.mark.skipif(not POSTGRES_URL, reason="set DEERFLOW_TEST_POSTGRES_URL to run live PostgreSQL tests")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def users() -> AsyncIterator[SQLiteUserRepository]:
    schema = f"deerflow_test_{uuid.uuid4().hex[:12]}"
    await init_engine_from_config(DatabaseConfig(backend="postgres", postgres_url=postgres_async_url(POSTGRES_URL or ""), postgres_schema=schema))
    try:
        yield SQLiteUserRepository(get_session_factory())
    finally:
        engine = get_engine()
        if engine is not None:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await close_engine()


@pytest.fixture
def subject() -> str:
    """One per test: the advisory lock is per database, not per schema, and parallel tests must not wait on each other."""
    return f"sub-{uuid.uuid4().hex[:12]}"


def _account(role: str, subject: str) -> User:
    return User(email="pat@example.com", password_hash=None, system_role=role, oauth_provider="sso", oauth_id=subject, oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC))


async def _column(user_id: str) -> str:
    async with get_engine().connect() as connection:  # type: ignore[union-attr]
        return (await connection.execute(text("SELECT system_role FROM users WHERE id = :id"), {"id": user_id})).scalar_one()


@pytest.mark.anyio
async def test_a_sign_in_held_up_on_the_row_while_the_limit_commits_stores_the_limit(users: SQLiteUserRepository, subject: str) -> None:
    """The person already reads ``user``, so the limit's own lowering touches nothing; another writer holds the row."""
    account = await users.create_user(_account("user", subject))
    engine = get_engine()
    assert engine is not None
    async with engine.connect() as other_writer:
        # An end-sessions bump, or a second callback, holding the account's row.
        await other_writer.begin()
        await other_writer.execute(text("UPDATE users SET token_version = token_version + 1 WHERE id = :id"), {"id": str(account.id)})
        sign_in = asyncio.create_task(users.record_sign_in(str(account.id), system_role="admin", oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC)))
        await asyncio.sleep(0.5)
        assert not sign_in.done(), "the sign-in waits on the row"
        limit = asyncio.create_task(users.limit_role(ISSUER, subject, "user"))
        await asyncio.sleep(0.5)
        await other_writer.commit()
    await asyncio.wait_for(asyncio.gather(sign_in, limit), timeout=30)

    assert await _column(str(account.id)) == "user", "the claim's admin never outlives the limit in the column"
    assert (await users.get_user_by_id(str(account.id))).system_role == "user"


@pytest.mark.anyio
async def test_a_first_sign_in_whose_insert_meets_a_limit_in_flight_is_created_at_the_limit(users: SQLiteUserRepository, subject: str) -> None:
    engine = get_engine()
    assert engine is not None
    async with engine.connect() as limiting:
        # The limit's transaction, as far as it has got: the identity's lock and the uncommitted row.
        await limiting.begin()
        await limiting.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": identity_lock_key(ISSUER, subject)})
        await limiting.execute(text("INSERT INTO role_limits (issuer, subject, role, limited_at) VALUES (:issuer, :subject, 'user', now())"), {"issuer": ISSUER, "subject": subject})
        create = asyncio.create_task(users.create_user(_account("admin", subject)))
        await asyncio.sleep(0.5)
        assert not create.done(), "the insert waits for the limit's transaction"
        await limiting.commit()
    created = await asyncio.wait_for(create, timeout=30)

    assert created.system_role == "user" and created.role_limit == "user"
    assert await _column(str(created.id)) == "user"


@pytest.mark.anyio
async def test_the_lift_waits_for_a_sign_in_in_flight_and_leaves_the_column_at_the_limit(users: SQLiteUserRepository, subject: str) -> None:
    account = await users.create_user(_account("admin", subject))
    await users.limit_role(ISSUER, subject, "user")
    engine = get_engine()
    assert engine is not None
    async with engine.connect() as other_writer:
        await other_writer.begin()
        await other_writer.execute(text("UPDATE users SET token_version = token_version + 1 WHERE id = :id"), {"id": str(account.id)})
        # Read before the lift, written after it: stores the limit, since the limit still held when it read.
        sign_in = asyncio.create_task(users.record_sign_in(str(account.id), system_role="admin", oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC)))
        await asyncio.sleep(0.5)
        lift = asyncio.create_task(users.lift_role_limit(ISSUER, subject))
        await asyncio.sleep(0.5)
        assert not lift.done(), "the lift waits for the sign-in that holds the identity"
        await other_writer.commit()
    await asyncio.wait_for(asyncio.gather(sign_in, lift), timeout=30)

    assert await _column(str(account.id)) == "user"
    assert await users.role_limit_for(ISSUER, subject) is None
