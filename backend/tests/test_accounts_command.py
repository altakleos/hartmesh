"""The deployer's command over a real users table: disable, enable, end sessions, release an address, list.

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
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-accounts-command-min-32")

from app.gateway.auth.accounts import AccountsCommand, CommandError, released_email_for
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


def _provider_account(email: str = "pat@example.com", subject: str = "sub-pat", *, issuer: str | None = ISSUER, role: str = "user") -> User:
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
async def test_an_account_linked_before_its_issuer_was_recorded_is_turned_off_by_subject(stores) -> None:
    """A NULL ``oauth_issuer`` (0039) must not leave the account impossible to turn off: fail closed on the subject."""
    users, tokens, schedules = stores
    legacy = await _seed(users, tokens, schedules, _provider_account("legacy@example.com", "sub-legacy", issuer=None), pats=1, tasks=0)
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    with pytest.raises(CommandError, match="before its issuer was recorded"):
        await command.run("disable", email="legacy@example.com")
    document = await command.run("disable", issuer=ISSUER, subject="sub-legacy")
    assert document["account"]["email"] == "legacy@example.com" and document["sessions_ended"] is True and document["tokens_revoked"] == 1, document
    refreshed = await users.get_user_by_id(str(legacy.id))
    assert refreshed.disabled_at is not None and refreshed.token_version == legacy.token_version + 1
    assert (await users.get_user_by_email("legacy@example.com")).disabled_at is not None
    listed = await command.run("list")
    assert next(entry for entry in listed["accounts"] if entry["email"] == "legacy@example.com")["disabled"] is True


@pytest.mark.anyio
async def test_a_sign_in_racing_end_sessions_cannot_carry_a_stale_version_back(stores) -> None:
    users, tokens, schedules = stores
    account = await _seed(users, tokens, schedules, _provider_account(), pats=0, tasks=0)
    stale = await users.get_user_by_id(str(account.id))
    assert await users.end_sessions(str(account.id)) is True
    # What a sign-in writes on an existing account, with the object it read before the bump.
    stale.system_role = "admin"
    stale.last_sign_in_at = datetime.now(UTC)
    await users.record_sign_in(str(stale.id), system_role=stale.system_role, oauth_issuer=stale.oauth_issuer, last_sign_in_at=stale.last_sign_in_at)
    refreshed = await users.get_user_by_id(str(account.id))
    assert refreshed.token_version == account.token_version + 1 and refreshed.system_role == "admin"


def test_main_answers_with_one_document_and_a_non_zero_exit_whatever_failed(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from app.gateway.auth import accounts

    async def _refuse(command: str, **_: object) -> dict:
        raise CommandError("no account exists for this subject")

    async def _break(command: str, **_: object) -> dict:
        raise RuntimeError("database unreachable")

    monkeypatch.setattr(accounts, "_run", _refuse)
    assert accounts.main(["end-sessions", "--issuer", ISSUER, "--subject", "sub-x"]) == 1
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1 and json.loads(out[0]) == {"command": "end-sessions", "error": "no account exists for this subject"}
    monkeypatch.setattr(accounts, "_run", _break)
    assert accounts.main(["list"]) == 1
    out = capsys.readouterr().out.strip().splitlines()
    assert len(out) == 1 and json.loads(out[0]) == {"command": "list", "error": "RuntimeError: database unreachable"}


def test_a_disabled_account_is_refused_by_the_browser_websocket_and_the_langgraph_hook(stores, monkeypatch: pytest.MonkeyPatch) -> None:
    """The two paths that resolve a cookie outside the middleware, with a cookie carrying the *current* version."""
    from types import SimpleNamespace

    from app.gateway import deps
    from app.gateway.auth import create_access_token
    from app.gateway.auth.config import AuthConfig, set_auth_config
    from app.gateway.langgraph_auth import authenticate
    from app.gateway.routers.browser import _authenticate_ws
    from deerflow.config.app_config import AppConfig
    from deerflow.config.sandbox_config import SandboxConfig

    users, tokens, schedules = stores
    set_auth_config(AuthConfig(jwt_secret=os.environ["AUTH_JWT_SECRET"]))
    monkeypatch.setattr(deps, "_cached_local_provider", None)
    monkeypatch.setattr(deps, "_cached_repo", None)
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: AppConfig(sandbox=SandboxConfig(use="deerflow.sandbox.local:LocalSandboxProvider")))
    account = asyncio.run(_seed(users, tokens, schedules, _provider_account(), pats=0, tasks=0))
    live_cookie = create_access_token(str(account.id), token_version=account.token_version)
    assert asyncio.run(_authenticate_ws(SimpleNamespace(cookies={"access_token": live_cookie}))).email == "pat@example.com"
    asyncio.run(users.disable_identity(ISSUER, "sub-pat"))
    current = asyncio.run(users.get_user_by_id(str(account.id)))
    cookie = create_access_token(str(account.id), token_version=current.token_version)
    assert asyncio.run(_authenticate_ws(SimpleNamespace(cookies={"access_token": cookie}))) is None
    with pytest.raises(Exception, match="turned off"):
        asyncio.run(authenticate(SimpleNamespace(cookies={"access_token": cookie}, headers={}, method="GET", url=SimpleNamespace(path="/api/threads"))))


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


# ── Releasing an address ────────────────────────────────────────────────


@pytest.mark.anyio
async def test_release_email_frees_the_address_and_keeps_everything_else(stores) -> None:
    """The remedy for an address held by an account nobody can use any more."""
    users, tokens, schedules = stores
    account = await _seed(users, tokens, schedules, _provider_account(role="admin"))
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    await command.run("disable", issuer=ISSUER, subject="sub-pat")

    document = await command.run("release-email", issuer=ISSUER, subject="sub-pat")

    assert document["verdict"] == "released"
    assert document["released"] == "pat@example.com"
    assert document["identity"] == {"issuer": ISSUER, "subject": "sub-pat"}
    # Everything the account is, it still is.
    assert document["account"]["role"] == "admin"
    assert document["account"]["subject"] == "sub-pat" and document["account"]["issuer"] == ISSUER
    assert document["account"]["disabled"] is True
    assert document["account"]["released"] is True and document["account"]["released_from"] == "pat@example.com"
    assert document["account"]["email"] == released_email_for(str(account.id))

    # The address is free: nothing answers to it any more.
    assert await users.get_user_by_email("pat@example.com") is None
    # And the account is still there, addressable by the identity that owns it.
    kept = await users.get_user_by_identity(ISSUER, "sub-pat")
    assert kept is not None and str(kept.id) == str(account.id) and kept.system_role == "admin"


@pytest.mark.anyio
async def test_releasing_twice_is_a_no_op_that_says_so(stores) -> None:
    users, tokens, schedules = stores
    account = await _seed(users, tokens, schedules, _provider_account())
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    await command.run("disable", issuer=ISSUER, subject="sub-pat")
    first = await command.run("release-email", issuer=ISSUER, subject="sub-pat")

    again = await command.run("release-email", issuer=ISSUER, subject="sub-pat")

    assert again["verdict"] == "already_released"
    assert again["released"] == "pat@example.com", "it still says which address was given up"
    assert again["account"]["email"] == first["account"]["email"], "the address it holds is unchanged"
    assert (await users.get_user_by_id(str(account.id))).email == released_email_for(str(account.id))


@pytest.mark.anyio
async def test_release_email_refuses_an_account_the_deployer_has_not_turned_off(stores) -> None:
    """While a person can still sign in, their address is theirs."""
    users, tokens, schedules = stores
    account = await _seed(users, tokens, schedules, _provider_account())
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)

    with pytest.raises(CommandError, match="not turned off"):
        await command.run("release-email", issuer=ISSUER, subject="sub-pat")

    assert (await users.get_user_by_id(str(account.id))).email == "pat@example.com"
    assert (await users.get_user_by_id(str(account.id))).email_released_from is None


@pytest.mark.anyio
async def test_release_email_refuses_a_subject_with_no_account(stores) -> None:
    users, tokens, schedules = stores
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    with pytest.raises(CommandError, match="no account exists"):
        await command.run("release-email", issuer=ISSUER, subject="sub-nobody")


@pytest.mark.anyio
async def test_a_released_address_can_never_be_a_persons_and_the_record_accepts_it(stores) -> None:
    """The representation, pinned: reserved by RFC 2606, and one the model will hold."""
    users, tokens, schedules = stores
    account = await _seed(users, tokens, schedules, _provider_account())
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    await command.run("disable", issuer=ISSUER, subject="sub-pat")
    await command.run("release-email", issuer=ISSUER, subject="sub-pat")

    released = released_email_for(str(account.id))
    assert released.endswith("@released.example"), "a reserved TLD: never registrable, never deliverable"
    assert str(account.id) in released, "derived from the account, so two released accounts never collide"
    # The account record must accept it, or every later read of this row would raise.
    assert User(email=released).email == released
    reread = await users.get_user_by_id(str(account.id))
    assert reread is not None and reread.email == released


@pytest.mark.anyio
async def test_list_shows_a_released_account_as_released_and_which_address_it_held(stores) -> None:
    users, tokens, schedules = stores
    await _seed(users, tokens, schedules, _provider_account())
    await _seed(users, tokens, schedules, _provider_account(email="sam@example.com", subject="sub-sam"), pats=0, tasks=0)
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    await command.run("disable", issuer=ISSUER, subject="sub-pat")
    await command.run("release-email", issuer=ISSUER, subject="sub-pat")

    listed = {entry["subject"]: entry for entry in (await command.run("list"))["accounts"]}

    assert listed["sub-pat"]["released"] is True and listed["sub-pat"]["released_from"] == "pat@example.com"
    assert listed["sub-sam"]["released"] is False and listed["sub-sam"]["released_from"] is None


@pytest.mark.anyio
async def test_a_released_address_stays_released_on_a_second_process_and_a_copied_database(stores, tmp_path) -> None:
    """Durability: the fact is a column, so it survives a restart and a copy."""
    import shutil
    import sqlite3

    import sqlalchemy as sa

    from deerflow.persistence.engine import get_session_factory

    users, tokens, schedules = stores
    account = await _seed(users, tokens, schedules, _provider_account())
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)
    await command.run("disable", issuer=ISSUER, subject="sub-pat")
    await command.run("release-email", issuer=ISSUER, subject="sub-pat")

    # SQLite is in WAL mode here, so a commit lives in the -wal file until it
    # is checkpointed; copying the database alone would copy an empty one.
    # Checkpoint first, which is what makes the copy a copy.
    async with get_session_factory()() as session:
        await session.execute(sa.text("PRAGMA wal_checkpoint(TRUNCATE)"))
        await session.commit()

    # Read the file the way a restarted process would, and again from a copy.
    original = tmp_path / "accounts.db"
    copied = tmp_path / "copy.db"
    shutil.copy(original, copied)
    for database in (original, copied):
        with sqlite3.connect(database) as connection:
            email, released_from = connection.execute("SELECT email, email_released_from FROM users WHERE id = ?", (str(account.id),)).fetchone()
        assert email == released_email_for(str(account.id))
        assert released_from == "pat@example.com"


@pytest.mark.anyio
async def test_one_subject_with_an_account_under_each_of_two_providers_is_refused_rather_than_guessed(stores) -> None:
    """Uniqueness is (provider, subject); two providers may point at one issuer.

    Acting on whichever row came back first would be a wrong-target write --
    ``release-email`` would give up an address that is still someone's. The
    command says which accounts it found and how to name one.
    """
    users, tokens, schedules = stores
    under_sso = await _seed(users, tokens, schedules, _provider_account(email="pat@example.com", subject="sub-pat"), pats=0, tasks=0)
    other = User(email="pat.chen@example.com", password_hash=None, system_role="user", oauth_provider="sso-basic", oauth_id="sub-pat", oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC))
    await users.create_user(other)
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)

    for form in ("disable", "enable", "end-sessions", "release-email"):
        with pytest.raises(CommandError) as refused:
            await command.run(form, issuer=ISSUER, subject="sub-pat")
        message = str(refused.value)
        assert "2 accounts" in message and "sso (pat@example.com)" in message and "sso-basic (pat.chen@example.com)" in message, (form, message)
        assert "--email" in message, form

    # Addressed by its email, each is the account the deployer named -- both
    # ways round, because whichever comes back first would pass one of them.
    for addressed, untouched, provider in ((under_sso, other, "sso"), (other, under_sso, "sso-basic")):
        before = {account.id: (await users.get_user_by_id(str(account.id))).token_version for account in (addressed, untouched)}
        document = await command.run("disable", email=addressed.email)
        assert document["account"]["provider"] == provider and document["account"]["email"] == addressed.email, provider
        assert (await users.get_user_by_id(str(addressed.id))).token_version == before[addressed.id] + 1, "the addressed account is signed out"
        assert (await users.get_user_by_id(str(untouched.id))).token_version == before[untouched.id], "the other account's sessions are not"
        # The refusal itself is keyed by (issuer, subject) -- it is the person
        # at the provider who is turned off -- so it covers both accounts, and
        # the document says which others it reached.
        assert (await users.get_user_by_id(str(untouched.id))).disabled_at is not None
        assert [entry["email"] for entry in document["identity_also_covers"]] == [untouched.email], provider
        await command.run("enable", email=addressed.email)


@pytest.mark.anyio
async def test_one_account_for_the_subject_is_still_addressed_by_issuer_and_subject(stores) -> None:
    """The refusal above must not cost the ordinary form anything."""
    users, tokens, schedules = stores
    await _seed(users, tokens, schedules, _provider_account(), pats=0, tasks=0)
    command = AccountsCommand(users, tokens=tokens, schedules=schedules)

    document = await command.run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["verdict"] == "disabled" and document["account"]["email"] == "pat@example.com"
