"""Credential writes preserve concurrent account changes and revocations."""

import asyncio
import os
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import bcrypt
import pytest
import pytest_asyncio
from fastapi import HTTPException
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from starlette.requests import Request
from starlette.responses import Response

from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.models import User
from app.gateway.auth.password import hash_password, verify_password
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from deerflow.persistence.user.access import DisabledIdentityRow, RoleLimitRow
from deerflow.persistence.user.model import UserRow

PASSWORD = "OwnedFixturePassword!27"


@pytest_asyncio.fixture
async def repo(tmp_path):
    # A dedicated disposable PostgreSQL database can run this same contract.
    url = os.environ.get("HARTMESH_CREDENTIAL_TEST_DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path / 'users.db'}")
    engine = create_async_engine(url)
    tables = (UserRow.__table__, DisabledIdentityRow.__table__, RoleLimitRow.__table__)
    async with engine.begin() as connection:
        for table in tables:
            await connection.run_sync(table.create)
    repository = SQLiteUserRepository(async_sessionmaker(engine, expire_on_commit=False))
    try:
        yield repository
    finally:
        async with engine.begin() as connection:
            for table in reversed(tables):
                await connection.run_sync(table.drop)
        await engine.dispose()


async def legacy_user(repo):
    legacy = bcrypt.hashpw(PASSWORD.encode(), bcrypt.gensalt(rounds=4)).decode()
    return await repo.create_user(User(email="owned@example.com", password_hash=legacy))


@pytest.mark.asyncio
@pytest.mark.parametrize("revoke", [False, True])
async def test_rehash_preserves_newer_metadata_and_refuses_revoked_credentials(repo, monkeypatch, revoke):
    user = await legacy_user(repo)
    entered, release = asyncio.Event(), asyncio.Event()
    replacement = hash_password(PASSWORD)

    async def delayed_hash(_password):
        entered.set()
        await release.wait()
        return replacement

    monkeypatch.setattr("app.gateway.auth.local_provider.hash_password_async", delayed_hash)
    pending = asyncio.create_task(LocalAuthProvider(repo).authenticate({"email": user.email, "password": PASSWORD}))
    await asyncio.wait_for(entered.wait(), 5)
    current = await repo.get_user_by_id(str(user.id))
    current.system_role = "admin"
    current.needs_setup = True
    current.last_sign_in_at = datetime(2026, 10, 4, tzinfo=UTC)
    if revoke:
        current.password_hash = hash_password("ResetFixturePassword!28")
        current.token_version += 1
    await repo.update_user(current)
    release.set()
    authenticated = await pending
    persisted = await repo.get_user_by_id(str(user.id))
    assert persisted.system_role == "admin"
    assert persisted.needs_setup is True
    assert persisted.last_sign_in_at == current.last_sign_in_at
    if revoke:
        assert authenticated is None
        assert persisted.password_hash == current.password_hash
        assert persisted.token_version == 1
    else:
        assert authenticated.system_role == "admin"
        assert persisted.password_hash == replacement
        assert persisted.token_version == 0


@pytest.mark.asyncio
async def test_concurrent_password_changes_have_one_winner_and_no_cookie_for_stale_request(repo, monkeypatch):
    from app.gateway.routers import auth

    user = await repo.create_user(User(email="owned@example.com", password_hash=hash_password(PASSWORD)))
    snapshots = [user.model_copy(), user.model_copy()]
    entered, release = asyncio.Event(), asyncio.Event()
    first_password, second_password = "FirstFixturePassword!28", "SecondFixturePassword!29"
    first_hash, second_hash = hash_password(first_password), hash_password(second_password)

    async def delayed_hash(password):
        if password == second_password:
            entered.set()
            await release.wait()
            return second_hash
        return first_hash

    async def current_user(request):
        return request.state.user

    monkeypatch.setattr(auth, "sign_on_only", lambda: False)
    monkeypatch.setattr(auth, "get_current_user_from_request", current_user)
    monkeypatch.setattr(auth, "get_local_provider", lambda: LocalAuthProvider(repo))
    monkeypatch.setattr(auth, "create_access_token", lambda _id, token_version: f"version-{token_version}")
    monkeypatch.setattr(auth, "_set_csrf_cookie", lambda *_args: None)
    monkeypatch.setattr("app.gateway.auth.password.hash_password_async", delayed_hash)

    def request(snapshot):
        req = Request({"type": "http", "method": "POST", "path": "/", "headers": [], "scheme": "https", "server": ("owned.test", 443)})
        req.state.user = snapshot
        req.state.auth_source = "session"
        return req

    responses = [Response(), Response()]
    pending = asyncio.create_task(auth.change_password(request(snapshots[1]), responses[1], auth.ChangePasswordRequest(current_password=PASSWORD, new_password=second_password)))
    await asyncio.wait_for(entered.wait(), 5)
    await auth.change_password(request(snapshots[0]), responses[0], auth.ChangePasswordRequest(current_password=PASSWORD, new_password=first_password))
    release.set()
    with pytest.raises(HTTPException) as exc:
        await pending
    assert exc.value.status_code == 409
    assert "set-cookie" not in responses[1].headers
    assert "version-1" in responses[0].headers["set-cookie"]
    persisted = await repo.get_user_by_id(str(user.id))
    assert persisted.token_version == 1
    assert verify_password(first_password, persisted.password_hash)
    assert snapshots[0].token_version == snapshots[1].token_version == 0


@pytest.mark.asyncio
async def test_resets_increment_persisted_version_and_preserve_account_fields(repo):
    user = await legacy_user(repo)
    current = user.model_copy(update={"system_role": "admin", "needs_setup": True, "token_version": 7})
    await repo.update_user(current)
    results = await asyncio.gather(*(repo.replace_password(str(user.id), f"owned-hash-{i}", needs_setup=True) for i in range(3)))
    assert sorted(result.token_version for result in results) == [8, 9, 10]
    persisted = await repo.get_user_by_id(str(user.id))
    assert persisted.token_version == 10
    assert persisted.system_role == "admin"
    assert persisted.needs_setup is True


@pytest.mark.asyncio
async def test_rehash_returned_version_does_not_adopt_later_reset(repo, monkeypatch):
    user = await legacy_user(repo)
    committed, release = asyncio.Event(), asyncio.Event()
    rehash = repo.rehash_password

    async def paused_rehash(*args, **kwargs):
        result = await rehash(*args, **kwargs)
        committed.set()
        await release.wait()
        return result

    monkeypatch.setattr(repo, "rehash_password", paused_rehash)
    pending = asyncio.create_task(LocalAuthProvider(repo).authenticate({"email": user.email, "password": PASSWORD}))
    await asyncio.wait_for(committed.wait(), 5)
    await repo.replace_password(str(user.id), hash_password("ResetFixturePassword!28"), needs_setup=True)
    release.set()
    authenticated = await pending
    assert authenticated.token_version == 0
    assert (await repo.get_user_by_id(str(user.id))).token_version == 1


@pytest.mark.asyncio
async def test_password_only_write_preserves_legacy_email_and_rejects_genuine_collision(repo):
    user = await legacy_user(repo)
    await repo.create_user(User(email="second@example.com"))
    async with repo._sf() as session:
        await session.execute(update(UserRow).where(UserRow.id == str(user.id)).values(email="Owned@example.com"))
        await session.commit()
    duplicate = await repo.create_user(User(email="third@example.com"))
    async with repo._sf() as session:
        await session.execute(update(UserRow).where(UserRow.id == str(duplicate.id)).values(email="owned@example.com"))
        await session.commit()
    changed = await repo.replace_password(str(user.id), "owned-new-hash", new_email="OWNED@example.com", needs_setup=False)
    assert changed.email == "Owned@example.com"
    assert changed.token_version == 1
    with pytest.raises(ValueError, match="Email"):
        await repo.replace_password(str(user.id), "uncommitted-hash", new_email="SECOND@example.com")
    persisted = await repo.get_user_by_id(str(user.id))
    assert persisted.token_version == 1
    assert persisted.password_hash == "owned-new-hash"
    assert await repo.replace_password(str(user.id), "stale-hash", expected_password_hash=user.password_hash, expected_token_version=0) is None


@pytest.mark.asyncio
async def test_failed_opportunistic_rehash_leaves_verified_snapshot_unchanged(repo, monkeypatch):
    user = await legacy_user(repo)

    async def unavailable(*_args, **_kwargs):
        raise RuntimeError("owned fixture database temporarily unavailable")

    monkeypatch.setattr(repo, "rehash_password", unavailable)
    authenticated = await LocalAuthProvider(repo).authenticate({"email": user.email, "password": PASSWORD})
    assert authenticated.password_hash == user.password_hash
    assert authenticated.token_version == 0
    assert (await repo.get_user_by_id(str(user.id))).password_hash == user.password_hash


@pytest.mark.asyncio
async def test_targeted_writes_preserve_populated_identity_and_account_metadata(repo):
    user = await repo.create_user(
        User(
            email="owned@example.com",
            password_hash="owned-old-hash",
            token_version=5,
            system_role="admin",
            needs_setup=True,
            oauth_provider="owned-provider",
            oauth_id="owned-subject",
            oauth_issuer="https://owned.example.com",
            last_sign_in_at=datetime(2026, 10, 4, tzinfo=UTC),
            email_released_from="previous@example.com",
        )
    )
    upgraded = await repo.rehash_password(str(user.id), expected_password_hash=user.password_hash, expected_token_version=5, password_hash="owned-rehashed")
    reset = await repo.replace_password(str(user.id), "owned-reset", needs_setup=True)
    for result in (upgraded, reset):
        assert result.model_dump(exclude={"password_hash", "token_version"}) == user.model_dump(exclude={"password_hash", "token_version"})
    assert upgraded.token_version == 5
    assert reset.token_version == 6


@pytest.mark.asyncio
async def test_stale_setup_change_leaves_email_and_setup_untouched(repo):
    user = await repo.create_user(User(email="owned@example.com", password_hash="owned-old", needs_setup=True))
    await repo.replace_password(str(user.id), "owned-reset")
    assert await repo.replace_password(str(user.id), "owned-stale", expected_password_hash=user.password_hash, expected_token_version=0, new_email="new@example.com", needs_setup=False) is None
    persisted = await repo.get_user_by_id(str(user.id))
    assert persisted.email == user.email
    assert persisted.needs_setup is True
    assert persisted.token_version == 1


@pytest.mark.asyncio
async def test_admin_reset_concurrent_delete_emits_no_credential_file(monkeypatch):
    from app.gateway.auth import reset_admin
    from app.gateway.auth.repositories.base import UserNotFoundError

    user = User(email="owned@example.com", system_role="admin")
    repository = SimpleNamespace(get_user_by_email=AsyncMock(return_value=user), replace_password=AsyncMock(return_value=None))
    config = SimpleNamespace(auth=SimpleNamespace(local=SimpleNamespace(enabled=True)), database=None)
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: config)
    monkeypatch.setattr("deerflow.persistence.engine.init_engine_from_config", AsyncMock())
    monkeypatch.setattr("deerflow.persistence.engine.get_session_factory", lambda: object())
    close = AsyncMock()
    monkeypatch.setattr("deerflow.persistence.engine.close_engine", close)
    monkeypatch.setattr(reset_admin, "SQLiteUserRepository", lambda _sf: repository)
    monkeypatch.setattr(reset_admin, "hash_password", lambda _password: "owned-fixture-hash")

    def must_not_write(*_args, **_kwargs):
        pytest.fail("a deleted account must not emit credentials")

    monkeypatch.setattr(reset_admin, "write_initial_credentials", must_not_write)
    with pytest.raises(UserNotFoundError):
        await reset_admin._run(user.email)
    close.assert_awaited_once()
