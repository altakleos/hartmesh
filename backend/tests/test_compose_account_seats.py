"""Seat creation is atomic across local, OIDC and initial-administrator doors."""

import asyncio
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from _storage_spaces_test_support import make_storage_fixture

from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from deerflow.persistence.user.access import DisabledIdentityRow, IdentityHoldRow, RoleLimitRow
from deerflow.persistence.user.model import UserRow


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def seated_accounts(tmp_path, request):
    async for files, sf, catalog in make_storage_fixture(tmp_path, request):
        async with sf.kw["bind"].begin() as connection:
            await connection.run_sync(UserRow.__table__.create)
            for row in (DisabledIdentityRow, IdentityHoldRow, RoleLimitRow):
                await connection.run_sync(row.__table__.create)
        yield SQLiteUserRepository(sf), sf, files, catalog


@pytest.mark.asyncio
async def test_concurrent_last_seat_is_taken_once_by_local_or_oidc(seated_accounts):
    from app.gateway.auth.repositories.sqlite import NoSeatLeft

    repo, _sf, _files, _catalog = seated_accounts
    await repo.create_first_admin(User(email="owner@example.com", system_role="admin"))

    async def limit(_session):
        return 2

    repo.seats_allowed = limit
    local = User(email="local@example.com")
    oidc = User(email="sso@example.com", oauth_provider="sso", oauth_id="new-subject", oauth_issuer="https://identity.example.com")
    outcomes = await asyncio.gather(repo.create_user(local), repo.create_user(oidc), return_exceptions=True)
    assert sum(isinstance(item, User) for item in outcomes) == 1
    refusal = next(item for item in outcomes if isinstance(item, BaseException))
    assert isinstance(refusal, NoSeatLeft) and not isinstance(refusal, ValueError)
    assert (refusal.allowed, refusal.in_use) == (2, 2)
    assert await repo.count_seated_users() == 2
    assert await repo.count_users() == 2


@pytest.mark.asyncio
async def test_first_empty_admin_bootstraps_even_when_prepared_allowance_is_zero(seated_accounts):
    from app.gateway.auth.repositories.sqlite import NoSeatLeft

    repo, _sf, _files, _catalog = seated_accounts

    async def limit(_session):
        return 0

    repo.seats_allowed = limit
    owner = await repo.create_first_admin(User(email="owner@example.com", system_role="admin"))
    assert owner is not None and await repo.count_seated_users() == 1
    with pytest.raises(NoSeatLeft):
        await repo.create_user(User(email="another@example.com"))
    assert await repo.create_first_admin(User(email="second-admin@example.com", system_role="admin")) is None


@pytest.mark.asyncio
async def test_disabled_account_keeps_seat_released_account_does_not(seated_accounts):
    from app.gateway.auth.repositories.sqlite import NoSeatLeft

    repo, sf, _files, _catalog = seated_accounts
    held = await repo.create_user(User(email="held@example.com", oauth_provider="sso", oauth_id="held", oauth_issuer="https://identity.example.com"))
    await repo.disable_identity("https://identity.example.com", "held")

    async def limit(_session):
        return 1

    repo.seats_allowed = limit
    with pytest.raises(NoSeatLeft):
        await repo.create_user(User(email="another@example.com"))
    async with sf() as session, session.begin():
        (await session.get(UserRow, str(held.id))).released_at = datetime.now(UTC)
    assert await repo.count_users() == 1 and await repo.count_seated_users() == 0
    await repo.create_user(User(email="new@example.com"))
    assert await repo.count_users() == 2 and await repo.count_seated_users() == 1


@pytest.mark.asyncio
async def test_seat_write_lock_is_reacquired_after_rollback_on_same_session(seated_accounts):
    from sqlalchemy import event

    repo, sf, _files, _catalog = seated_accounts
    engine = sf.kw["bind"]
    statements = []

    def executed(_connection, _cursor, statement, _parameters, _context, _many):
        if statement == "BEGIN IMMEDIATE" or "pg_advisory_xact_lock" in statement:
            statements.append(statement)

    event.listen(engine.sync_engine, "before_cursor_execute", executed)
    try:
        async with sf() as session:
            await repo.serialize_seats(session)
            await session.rollback()
            await repo.serialize_seats(session)
            assert len(statements) == 2
            await session.rollback()
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", executed)


@pytest.mark.asyncio
async def test_duplicate_email_keeps_its_existing_error_at_full_capacity(seated_accounts):
    repo, _sf, *_ = seated_accounts
    await repo.create_user(User(email="owner@example.com"))

    async def full(_session):
        return 1

    repo.seats_allowed = full
    with pytest.raises(ValueError, match="Email already registered"):
        await repo.create_user(User(email="OWNER@example.com"))


@pytest.mark.asyncio
async def test_borrowed_allowance_works_with_one_database_connection(seated_accounts, tmp_path):
    from types import SimpleNamespace

    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.gateway import storage_spaces as ss
    from deerflow.config.storage_spaces_config import StoragePlan, StorageSpacesConfig

    _repo, original_sf, *_ = seated_accounts
    original = original_sf.kw["bind"]
    if original.dialect.name == "postgresql":
        async with original_sf() as session:
            schema = await session.scalar(text("select current_schema()"))
        engine = create_async_engine(original.url, pool_size=1, max_overflow=0, connect_args={"server_settings": {"search_path": schema}})
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/one-connection.db", pool_size=1, max_overflow=0)
        async with engine.begin() as connection:
            await connection.run_sync(UserRow.__table__.create)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    repo = SQLiteUserRepository(sf)
    config = StorageSpacesConfig(enabled=True, inventory_path=str(tmp_path / "unused.json"), plan=StoragePlan(users=1, user_gib=1, ai_assistants_per_user=0, ai_assistant_gib=1, ai_employees=0, ai_employee_gib=1, company_gib=1))
    files = SimpleNamespace(registry=SimpleNamespace(_sf=sf), catalog=SimpleNamespace(volumes={"slot": SimpleNamespace(kind="user")}))
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, storage_spaces_config=config))

    async def allowed(session):
        return (await ss.allowance(app, "user", session=session)).allowed

    repo.seats_allowed = allowed
    try:
        owner = await asyncio.wait_for(repo.create_first_admin(User(email="owner@example.com", system_role="admin")), 5)
        assert owner is not None
        from app.gateway.auth.repositories.sqlite import NoSeatLeft

        with pytest.raises(NoSeatLeft):
            await asyncio.wait_for(repo.create_user(User(email="another@example.com")), 5)
        assert await repo.count_seated_users() == 1
    finally:
        await engine.dispose()
