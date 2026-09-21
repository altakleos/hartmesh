"""The deployer's command over a real users table: disable, enable, end sessions, list.

Every form is idempotent and answers with one document. ``disable`` records
the refusal by issuer and subject (before or after an account exists), ends
the sessions and revokes the tokens; ``enable`` withdraws the refusal and
revives nothing; ``end-sessions`` touches sessions only; ``list`` shows
every account and every identity turned off without one. The refusal every
credential path derives from the row is proved on the served Gateway in
``test_membership_e2e.py``; the command line itself (JSON on stdout, exit
status) is exercised there as a subprocess, the way the deployer runs it.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-accounts-command-min-32")

from app.gateway.auth.accounts import AccountsCommand, CommandError
from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository

ISSUER = "https://login.example.com/realms/tenant"


@pytest.fixture
def stores(tmp_path) -> Iterator[tuple[SQLiteUserRepository, object, object]]:
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/accounts.db", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    tenant = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
    try:
        yield SQLiteUserRepository(session_factory), PersonalAccessTokenRepository(session_factory, tenant=tenant), ScheduledTaskRepository(session_factory)
    finally:
        asyncio.run(close_engine())


def _provider_account(email: str = "pat@example.com", subject: str = "sub-pat", *, issuer: str = ISSUER, role: str = "user") -> User:
    return User(email=email, password_hash=None, system_role=role, oauth_provider="sso", oauth_id=subject, oauth_issuer=issuer, last_sign_in_at=datetime.now(UTC))


async def _seed(users: SQLiteUserRepository, tokens, schedules, account: User, *, pats: int = 2, tasks: int = 1) -> User:
    created = await users.create_user(account)
    for index in range(pats):
        await tokens.create(user_id=str(created.id), name=f"token-{index}", scopes=["threads:read"], token_digest=f"digest-{created.id}-{index}")
    for index in range(tasks):
        await schedules.create(
            task_id=str(uuid4()),
            user_id=str(created.id),
            thread_id=None,
            context_mode="fresh",
            assistant_id=None,
            title=f"task-{index}",
            prompt="hello",
            schedule_type="once",
            schedule_spec={"run_at": "2030-01-01T00:00:00+00:00"},
            timezone="UTC",
            next_run_at=datetime.now(UTC) + timedelta(days=365),
        )
    return created


@pytest.mark.anyio
async def test_disable_records_the_refusal_ends_sessions_revokes_tokens_and_is_idempotent(stores) -> None:
    users, tokens, schedules = stores
    account = await _seed(users, tokens, schedules, _provider_account())
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)

    first = await command.run("disable", issuer=ISSUER + "/", subject="sub-pat")
    assert first["verdict"] == "disabled" and first["sessions_ended"] is True and first["tokens_revoked"] == 2 and first["schedules_held"] == 1
    assert first["identity"] == {"issuer": ISSUER, "subject": "sub-pat"}
    assert first["account"]["disabled"] is True and first["account"]["email"] == "pat@example.com" and first["account"]["last_sign_in_at"]

    # Derived at every read; sessions minted before carry the old token_version.
    refreshed = await users.get_user_by_id(str(account.id))
    assert refreshed is not None and refreshed.disabled_at is not None and refreshed.token_version == account.token_version + 1
    assert all(record["revoked_at"] is not None for record in await tokens.list_for_user(str(account.id)))
    assert await users.is_identity_disabled(ISSUER, "sub-pat") is True

    again = await command.run("disable", issuer=ISSUER, subject="sub-pat")
    assert again["verdict"] == "already_disabled" and again["tokens_revoked"] == 0
    assert (await users.get_user_by_id(str(account.id))).token_version == account.token_version + 2, "the sessions are ended again, harmlessly"


@pytest.mark.anyio
async def test_disable_before_the_first_sign_in_records_the_refusal_and_says_no_account_existed(stores) -> None:
    users, tokens, schedules = stores
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    document = await command.run("disable", issuer=ISSUER, subject="sub-early")
    assert document["verdict"] == "disabled" and document["account"] is None and "no account existed" in document["note"]
    assert document["sessions_ended"] is False and document["tokens_revoked"] == 0
    assert await users.is_identity_disabled(ISSUER, "sub-early") is True
    listed = await command.run("list")
    assert listed["disabled_without_account"][0]["issuer"] == ISSUER and listed["disabled_without_account"][0]["subject"] == "sub-early"


@pytest.mark.anyio
async def test_enable_withdraws_the_refusal_and_revives_nothing(stores) -> None:
    users, tokens, schedules = stores
    account = await _seed(users, tokens, schedules, _provider_account())
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    await command.run("disable", issuer=ISSUER, subject="sub-pat")
    version_after_disable = (await users.get_user_by_id(str(account.id))).token_version

    enabled = await command.run("enable", email="pat@example.com")
    assert enabled["verdict"] == "enabled" and enabled["account"]["disabled"] is False
    refreshed = await users.get_user_by_id(str(account.id))
    assert refreshed.disabled_at is None
    assert refreshed.token_version == version_after_disable, "old sessions stay dead"
    assert all(record["revoked_at"] is not None for record in await tokens.list_for_user(str(account.id))), "revoked tokens stay revoked"
    assert (await command.run("enable", issuer=ISSUER, subject="sub-pat"))["verdict"] == "already_enabled"
    assert (await command.run("enable", issuer=ISSUER, subject="sub-nobody"))["verdict"] == "already_enabled"


@pytest.mark.anyio
async def test_end_sessions_touches_sessions_only(stores) -> None:
    users, tokens, schedules = stores
    account = await _seed(users, tokens, schedules, _provider_account(role="admin"))
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    document = await command.run("end-sessions", issuer=ISSUER, subject="sub-pat")
    assert document["verdict"] == "sessions_ended" and document["tokens_revoked"] == 0 and document["account"]["role"] == "admin"
    refreshed = await users.get_user_by_id(str(account.id))
    assert refreshed.token_version == account.token_version + 1 and refreshed.disabled_at is None
    assert all(record["revoked_at"] is None for record in await tokens.list_for_user(str(account.id)))
    with pytest.raises(CommandError, match="no account exists"):
        await command.run("end-sessions", issuer=ISSUER, subject="sub-nobody")


@pytest.mark.anyio
async def test_list_shows_every_account_with_the_disabled_one_marked(stores) -> None:
    users, tokens, schedules = stores
    await _seed(users, tokens, schedules, _provider_account("owner@example.com", "sub-owner", role="admin"), pats=0, tasks=0)
    await _seed(users, tokens, schedules, _provider_account(), pats=0, tasks=0)
    await users.create_user(User(email="local@example.com", password_hash="x", system_role="user"))
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    await command.run("disable", issuer=ISSUER, subject="sub-pat")
    listed = await command.run("list")
    by_email = {entry["email"]: entry for entry in listed["accounts"]}
    assert set(by_email) == {"owner@example.com", "pat@example.com", "local@example.com"}
    assert by_email["owner@example.com"] == {**by_email["owner@example.com"], "issuer": ISSUER, "subject": "sub-owner", "role": "admin", "disabled": False, "disabled_at": None, "provider": "sso"}
    assert by_email["pat@example.com"]["disabled"] is True and by_email["pat@example.com"]["disabled_at"] and by_email["pat@example.com"]["last_sign_in_at"]
    assert by_email["local@example.com"]["issuer"] is None and by_email["local@example.com"]["subject"] is None and by_email["local@example.com"]["last_sign_in_at"] is None
    assert listed["disabled_without_account"] == [], "an identity with an account is listed once, on the account"


@pytest.mark.anyio
async def test_the_email_convenience_resolves_exactly_one_provider_account(stores) -> None:
    users, tokens, schedules = stores
    await users.create_user(User(email="local@example.com", password_hash="x", system_role="user"))
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    with pytest.raises(CommandError, match="no account has the email"):
        await command.run("disable", email="nobody@example.com")
    with pytest.raises(CommandError, match="no identity-provider identity"):
        await command.run("disable", email="local@example.com")
    with pytest.raises(CommandError, match="not both"):
        await command.run("disable", email="local@example.com", issuer=ISSUER, subject="x")
    with pytest.raises(CommandError, match="go together"):
        await command.run("disable", issuer=ISSUER)
    with pytest.raises(CommandError, match="address the account"):
        await command.run("disable")
    assert await users.list_disabled_identities() == [], "a refused selector records nothing"
