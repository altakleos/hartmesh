"""A durable role limit: a demoted administrator holds no administrator capability, whatever the next sign-in's claim says.

The identity provider can be restored from a backup taken before the
demotion; its claim then says ``admin`` again. A limit the next sign-in
overrides would hand the role back between the deployer's passes. So the
limit is one row keyed by the provider's ``(issuer, subject)`` -- valid before
an account exists, covering every account the identity holds here -- and every
read of an account derives its role from it, as the turned-off state is
derived. On top of that the stored column never reads above the limit:
setting it lowers the column in the same transaction, a sign-in stores the
lower of its claim and the limit in the same statement that stores the role,
and lifting it leaves the column where the limit held it, so lifting changes
nothing until the next sign-in reads the claim.

These are the bare pieces on a real users table; the served Gateway, the
command as a subprocess, and the run a token starts are in
``test_membership_e2e.py``.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-role-limit-min-32-chars")

from app.gateway.auth.accounts import EXIT_UNCONFIRMED_RUNS, AccountsCommand, CommandError
from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository

ISSUER = "https://login.example.com/realms/tenant"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def stores(tmp_path) -> Iterator[SimpleNamespace]:
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/role-limit.db", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    tenant = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
    try:
        yield SimpleNamespace(
            users=SQLiteUserRepository(session_factory),
            tokens=PersonalAccessTokenRepository(session_factory, tenant=tenant),
            schedules=ScheduledTaskRepository(session_factory),
            runs=RunRepository(session_factory, tenant=tenant),
            path=tmp_path / "role-limit.db",
        )
    finally:
        asyncio.run(close_engine())


def _account(email: str = "pat@example.com", subject: str = "sub-pat", *, provider: str = "sso", role: str = "admin", issuer: str | None = ISSUER) -> User:
    return User(email=email, password_hash=None, system_role=role, oauth_provider=provider, oauth_id=subject, oauth_issuer=issuer, last_sign_in_at=datetime.now(UTC))


def _command(stores: SimpleNamespace, **kwargs) -> AccountsCommand:
    return AccountsCommand(stores.users, tokens=stores.tokens, schedules=stores.schedules, runs=stores.runs, **kwargs)


def _column(stores: SimpleNamespace, user_id: str) -> str:
    """The stored role, read past the repository: the column itself."""
    with sqlite3.connect(stores.path) as connection:
        return connection.execute("SELECT system_role FROM users WHERE id = ?", (user_id,)).fetchone()[0]


def _write_column(stores: SimpleNamespace, user_id: str, role: str) -> None:
    with sqlite3.connect(stores.path) as connection:
        connection.execute("UPDATE users SET system_role = ? WHERE id = ?", (role, user_id))


async def _sign_in(stores: SimpleNamespace, user_id: str, *, claim_role: str) -> None:
    """What a provider sign-in writes on an existing account whose claim reads ``claim_role``."""
    await stores.users.record_sign_in(user_id, system_role=claim_role, oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC))


async def _seed_run(stores: SimpleNamespace, user_id: str, *, role: str | None = None) -> str:
    """A running run; ``role`` is the role sealed at its admission, unrecorded when ``None``."""
    run_id = str(uuid4())
    sealed = None if role is None else {"user_id": user_id, "role": role}
    await stores.runs.put(run_id, thread_id=str(uuid4()), user_id=user_id, status="running", created_at=datetime.now(UTC).isoformat(), principal_projection_json=sealed)
    return run_id


# ── The limit on the stored role ────────────────────────────────────────


@pytest.mark.anyio
async def test_the_limit_lowers_the_role_at_once_on_every_account_the_identity_covers(stores) -> None:
    """Two configured providers at one issuer give one person two accounts; the limit is the person's, so it reaches both."""
    under_sso = await stores.users.create_user(_account())
    under_basic = await stores.users.create_user(_account("pat.chen@example.com", provider="sso-basic"))
    bystander = await stores.users.create_user(_account("owner@example.com", "sub-owner"))

    document = await _command(stores).run("limit-role", issuer=ISSUER + "/", subject="sub-pat", role="user")

    assert document["verdict"] == "limited" and document["role"] == "user" and document["identity"] == {"issuer": ISSUER, "subject": "sub-pat"}
    assert sorted(entry["email"] for entry in document["accounts"]) == ["pat.chen@example.com", "pat@example.com"]
    assert all(entry["role"] == "user" and entry["role_limit"] == "user" for entry in document["accounts"])
    for account in (under_sso, under_basic):
        assert (await stores.users.get_user_by_id(str(account.id))).system_role == "user", "every read derives it"
        assert _column(stores, str(account.id)) == "user", "and the column itself reads the limit, not only the reads"
    assert (await stores.users.get_user_by_id(str(bystander.id))).system_role == "admin", "another person is untouched"
    assert _column(stores, str(bystander.id)) == "admin"
    assert [user.system_role for user in await stores.users.list_users_by_identity(ISSUER, "sub-pat")] == ["user", "user"], "the identity's own listing derives it too"


@pytest.mark.anyio
async def test_a_limit_at_one_issuer_leaves_the_same_subject_at_another_issuer_alone(stores) -> None:
    """Subjects are only unique within an issuer."""
    here = await stores.users.create_user(_account())
    elsewhere = await stores.users.create_user(_account("pat@other.example.com", provider="other-sso", issuer="https://login.other.example.com"))

    await _command(stores).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")

    assert (await stores.users.get_user_by_id(str(here.id))).system_role == "user"
    held = await stores.users.get_user_by_id(str(elsewhere.id))
    assert held.system_role == "admin" and held.role_limit is None and _column(stores, str(elsewhere.id)) == "admin"


@pytest.mark.anyio
async def test_a_legacy_account_with_no_recorded_issuer_is_held_by_a_limit_on_its_subject(stores) -> None:
    """Linked before its issuer was recorded: it will adopt the configured issuer, so it fails closed on its subject."""
    legacy = await stores.users.create_user(_account("legacy@example.com", "sub-legacy", issuer=None))

    document = await _command(stores).run("limit-role", issuer=ISSUER, subject="sub-legacy", role="user")

    assert [entry["email"] for entry in document["accounts"]] == ["legacy@example.com"]
    assert (await stores.users.get_user_by_id(str(legacy.id))).system_role == "user" and _column(stores, str(legacy.id)) == "user"
    assert await stores.users.count_admin_users() == 0


@pytest.mark.anyio
async def test_a_limit_recorded_before_the_first_sign_in_holds_when_the_account_is_created(stores) -> None:
    document = await _command(stores).run("limit-role", issuer=ISSUER, subject="sub-early", role="user")
    assert document["verdict"] == "limited" and document["accounts"] == [] and "no account" in document["note"]

    created = await stores.users.create_user(_account("early@example.com", "sub-early", role="admin"))

    assert created.system_role == "user" and created.role_limit == "user", "the account the sign-in gets back"
    assert _column(stores, str(created.id)) == "user"
    assert (await stores.users.get_user_by_id(str(created.id))).system_role == "user"


@pytest.mark.anyio
async def test_a_sign_in_whose_claim_says_admin_stores_the_limit(stores) -> None:
    account = await stores.users.create_user(_account())
    await _command(stores).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")

    await _sign_in(stores, str(account.id), claim_role="admin")

    assert _column(stores, str(account.id)) == "user", "the lower of the claim and the limit, in the write that stores the role"
    assert (await stores.users.get_user_by_id(str(account.id))).system_role == "user"


@pytest.mark.anyio
async def test_a_role_written_past_the_limit_is_still_read_as_the_limit_and_a_lift_does_not_hand_it_back(stores) -> None:
    """A sign-in that read ``admin`` before the limit committed, and wrote after it.

    On SQLite the statement cannot interleave; on PostgreSQL a sign-in whose
    snapshot predates the limit can still land ``admin`` in the column. The
    derived read is what makes that harmless while the limit holds, and the
    lift is what keeps it from surfacing afterwards.
    """
    account = await stores.users.create_user(_account())
    command = _command(stores)
    await command.run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    _write_column(stores, str(account.id), "admin")

    read = await stores.users.get_user_by_id(str(account.id))
    assert read.system_role == "user" and read.role_limit == "user"
    assert (await stores.users.get_user_by_oauth("sso", "sub-pat")).system_role == "user"
    assert (await stores.users.get_user_by_email("pat@example.com")).system_role == "user"
    assert [user.system_role for user in await stores.users.list_users()] == ["user"]
    assert [user.system_role for user in await stores.users.list_users_by_identity(ISSUER, "sub-pat")] == ["user"]
    assert await stores.users.count_admin_users() == 0, "the count of administrators derives it too"

    lifted = await command.run("lift-role-limit", issuer=ISSUER, subject="sub-pat")
    assert lifted["verdict"] == "lifted"
    assert _column(stores, str(account.id)) == "user", "the lift leaves the column where the limit held it"
    assert (await stores.users.get_user_by_id(str(account.id))).system_role == "user"


@pytest.mark.anyio
async def test_a_limit_row_holding_a_role_it_does_not_know_holds_the_lowest_role(stores) -> None:
    """The command cannot write one; a hand-edited row must neither break every read of the account nor let it through."""
    account = await stores.users.create_user(_account())
    with sqlite3.connect(stores.path) as connection:
        connection.execute("INSERT INTO role_limits (issuer, subject, role, limited_at) VALUES (?, ?, 'viewer', ?)", (ISSUER, "sub-pat", datetime.now(UTC).isoformat()))

    assert (await stores.users.get_user_by_id(str(account.id))).system_role == "user"
    await _sign_in(stores, str(account.id), claim_role="admin")
    assert _column(stores, str(account.id)) == "user", "a sign-in stores the lowest role too"


@pytest.mark.anyio
async def test_a_re_run_lowers_a_role_written_past_the_limit_and_says_so(stores) -> None:
    account = await stores.users.create_user(_account())
    command = _command(stores)
    await command.run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    _write_column(stores, str(account.id), "admin")
    before = (await stores.users.get_user_by_id(str(account.id))).token_version

    again = await command.run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")

    assert again["verdict"] == "already_limited" and again["surfaces"]["stored_role"] == {**again["surfaces"]["stored_role"], "action": "lowered", "count": 1}
    assert _column(stores, str(account.id)) == "user"
    assert (await stores.users.get_user_by_id(str(account.id))).token_version == before + 1, "a stored administrator's sessions are ended"


@pytest.mark.anyio
async def test_lifting_changes_nothing_until_the_next_sign_in_reads_the_claim(stores) -> None:
    account = await stores.users.create_user(_account())
    command = _command(stores)
    await command.run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")

    lifted = await command.run("lift-role-limit", issuer=ISSUER, subject="sub-pat")
    assert lifted["verdict"] == "lifted" and [entry["role"] for entry in lifted["accounts"]] == ["user"] and lifted["accounts"][0]["role_limit"] is None
    assert "next sign-in" in lifted["note"] and "administrators' list" in lifted["note"], "the note says when the role is read again, both ways"
    assert lifted["changed"] is True and lifted["returncode"] == 0 and lifted["surfaces_unconfirmed"] == []
    assert lifted["surfaces"]["role_limit"]["action"] == "lifted" and 0 <= lifted["surfaces"]["role_limit"]["stopped_after_ms"] <= lifted["elapsed_ms"]
    assert (await stores.users.get_user_by_id(str(account.id))).system_role == "user"

    await _sign_in(stores, str(account.id), claim_role="admin")
    assert (await stores.users.get_user_by_id(str(account.id))).system_role == "admin", "the claim is read again after the lift"

    nothing = await command.run("lift-role-limit", issuer=ISSUER, subject="sub-pat")
    assert nothing["verdict"] == "no_limit" and nothing["changed"] is False and nothing["surfaces"] == {} and nothing["returncode"] == 0
    assert (await command.run("lift-role-limit", issuer=ISSUER, subject="sub-nobody"))["verdict"] == "no_limit"


# ── Sessions, tokens, running work ──────────────────────────────────────


@pytest.mark.anyio
async def test_the_limit_ends_every_covered_account_s_sessions_and_leaves_their_tokens(stores) -> None:
    under_sso = await stores.users.create_user(_account())
    under_basic = await stores.users.create_user(_account("pat.chen@example.com", provider="sso-basic"))
    for account in (under_sso, under_basic):
        await stores.tokens.create(user_id=str(account.id), name="automation", scopes=["threads:read"], token_digest=f"digest-{account.id}")
    # A token already revoked is not one the limit reaches.
    dead = await stores.tokens.create(user_id=str(under_sso.id), name="old", scopes=["threads:read"], token_digest=f"digest-old-{under_sso.id}")
    await stores.tokens.revoke(str(dead["id"]), str(under_sso.id))

    document = await _command(stores).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")

    for account in (under_sso, under_basic):
        assert (await stores.users.get_user_by_id(str(account.id))).token_version == account.token_version + 1
        assert all(record["revoked_at"] is None for record in await stores.tokens.list_for_user(str(account.id)) if record["name"] == "automation"), "a token keeps working, at the limited role"
    assert document["surfaces"]["sessions"]["count"] == 2 and document["surfaces"]["personal_access_tokens"] == {**document["surfaces"]["personal_access_tokens"], "action": "limited_at_next_use", "count": 2}


@pytest.mark.anyio
async def test_a_re_run_with_nothing_to_lower_does_not_sign_the_person_out_again(stores) -> None:
    """The deployer re-applies its record of a demotion on every pass; the person must not be signed out every ten minutes."""
    account = await stores.users.create_user(_account())
    command = _command(stores)
    first = await command.run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    version = (await stores.users.get_user_by_id(str(account.id))).token_version

    again = await command.run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")

    assert first["verdict"] == "limited" and again["verdict"] == "already_limited"
    assert first["changed"] is True and again["changed"] is False and "no session was ended" in again["note"]
    assert (await stores.users.get_user_by_id(str(account.id))).token_version == version
    assert again["surfaces"]["sessions"]["action"] == "already_limited" and again["surfaces"]["stored_role"]["action"] == "already_limited"
    assert again["returncode"] == 0 and again["surfaces_unconfirmed"] == []


@pytest.mark.anyio
async def test_the_limit_ends_the_sessions_in_the_transaction_that_records_it(stores) -> None:
    """A command killed right after the limit commits must not leave the sessions it would have ended: every later pass reads no change."""
    account = await stores.users.create_user(_account())
    before = account.token_version

    # The repository's write alone, as if the command died right after it.
    await stores.users.limit_role(ISSUER, "sub-pat", "user")
    assert (await stores.users.get_user_by_id(str(account.id))).token_version == before + 1

    again = await _command(stores).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    assert again["changed"] is False and (await stores.users.get_user_by_id(str(account.id))).token_version == before + 1


@pytest.mark.anyio
async def test_running_work_is_left_alone_unless_asked_and_the_document_says_what_reads_its_role(stores) -> None:
    account = await stores.users.create_user(_account())
    run_id = await _seed_run(stores, str(account.id))

    left = await _command(stores, wait_seconds=0).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")

    running = left["surfaces"]["running_work"]
    assert running["action"] == "left_alone" and running["count"] == 1 and running["stopped_after_ms"] is None
    assert running["role_read_by"] == [], "nothing in this deployment reads a run's role"
    assert (await stores.runs.get(run_id, user_id=None)).get("cancel_action") is None
    assert left["runs_found"] == 0 and left["returncode"] == 0 and left["changed"] is True


@pytest.mark.anyio
async def test_the_document_names_what_would_read_a_run_s_role(stores) -> None:
    await stores.users.create_user(_account())
    document = await _command(stores, role_readers=("authorization", "guardrails")).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    assert document["surfaces"]["running_work"]["role_read_by"] == ["authorization", "guardrails"]
    assert "--end-running-work" in document["note"]


def test_what_reads_a_run_s_role_is_read_from_the_configuration() -> None:
    from app.gateway.auth.accounts import run_role_readers
    from deerflow.config.app_config import AppConfig
    from deerflow.config.authorization_config import AuthorizationConfig
    from deerflow.config.guardrails_config import GuardrailProviderConfig, GuardrailsConfig
    from deerflow.config.sandbox_config import SandboxConfig

    sandbox = SandboxConfig(use="deerflow.sandbox.local:LocalSandboxProvider")
    assert run_role_readers(AppConfig(sandbox=sandbox)) == ()
    assert run_role_readers(AppConfig(sandbox=sandbox, authorization=AuthorizationConfig(enabled=True))) == ("authorization",)
    guarded = GuardrailsConfig(enabled=True, provider=GuardrailProviderConfig(use="deerflow.guardrails.builtin:AllowlistProvider"))
    assert run_role_readers(AppConfig(sandbox=sandbox, guardrails=guarded)) == ("guardrails",)
    assert run_role_readers(AppConfig(sandbox=sandbox, guardrails=GuardrailsConfig(enabled=True))) == (), "enabled with no provider reads nothing"


@pytest.mark.anyio
async def test_asked_to_it_cancels_every_covered_account_s_running_work_and_reports_unconfirmed_runs(stores) -> None:
    under_sso = await stores.users.create_user(_account())
    under_basic = await stores.users.create_user(_account("pat.chen@example.com", provider="sso-basic"))
    runs = [await _seed_run(stores, str(account.id)) for account in (under_sso, under_basic)]

    document = await _command(stores, wait_seconds=0.2).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user", end_running_work=True)

    assert document["runs_found"] == 2 and document["runs_unconfirmed"] == sorted(runs), "no worker applied them here"
    assert [(await stores.runs.get(run_id, user_id=None))["cancel_action"] for run_id in runs] == ["interrupt", "interrupt"]
    assert document["surfaces_unconfirmed"] == ["running_work"] and document["returncode"] == EXIT_UNCONFIRMED_RUNS
    assert document["surfaces"]["running_work"] == {**document["surfaces"]["running_work"], "action": "ended", "count": 2, "stopped_after_ms": None, "stopped_at": None, "role_read_by": []}


@pytest.mark.anyio
async def test_re_applied_with_the_flag_it_ends_only_work_still_carrying_the_role_it_took_away(stores) -> None:
    """The deployer re-applies every pass; what the person starts as a user afterwards is theirs to finish."""
    account = await stores.users.create_user(_account())
    as_admin = await _seed_run(stores, str(account.id), role="admin")
    as_user = await _seed_run(stores, str(account.id), role="user")
    unrecorded = await _seed_run(stores, str(account.id))

    document = await _command(stores, wait_seconds=0).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user", end_running_work=True)

    assert document["runs_found"] == 2 and document["surfaces"]["running_work"]["count"] == 2
    assert (await stores.runs.get(as_admin, user_id=None))["cancel_action"] == "interrupt"
    assert (await stores.runs.get(unrecorded, user_id=None))["cancel_action"] == "interrupt", "a run whose role is not recorded is not assumed harmless"
    assert (await stores.runs.get(as_user, user_id=None)).get("cancel_action") is None
    left = await _command(stores, wait_seconds=0).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    assert left["surfaces"]["running_work"]["count"] == 2, "left alone, it counts the same runs"


@pytest.mark.anyio
async def test_asked_to_it_also_ends_a_run_admitted_above_the_limit_after_it_listed_the_runs(stores) -> None:
    """A request that authenticated just before the limit committed can insert its run after the command looked.

    Past the commit nothing new can be admitted above the limit, so looking
    once more after the wait finds every such run.
    """
    account = await stores.users.create_user(_account())
    early = await _seed_run(stores, str(account.id), role="admin")
    late: list[str] = []
    request_cancel = stores.runs.request_cancel_compat

    async def _admitted_meanwhile(run_id: str, **kwargs):
        if not late:
            late.append(await _seed_run(stores, str(account.id), role="admin"))
        return await request_cancel(run_id, **kwargs)

    stores.runs.request_cancel_compat = _admitted_meanwhile
    document = await _command(stores, wait_seconds=0).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user", end_running_work=True)

    assert late and (await stores.runs.get(late[0], user_id=None))["cancel_action"] == "interrupt"
    assert document["runs_found"] == 2 and document["runs_unconfirmed"] == sorted([early, *late])
    assert document["surfaces"]["running_work"]["count"] == 2


@pytest.mark.anyio
async def test_asked_to_with_the_runs_stopped_the_running_work_entry_carries_its_time(stores) -> None:
    account = await stores.users.create_user(_account())
    run_id = await _seed_run(stores, str(account.id))

    async def _stop_it() -> None:
        # The owning worker's part: apply the cancellation once it is asked for, a little later.
        while (await stores.runs.get(run_id, user_id=None)).get("cancel_action") is None:
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.3)
        await stores.runs.update_status(run_id, "interrupted")

    stopper = asyncio.create_task(_stop_it())
    document = await _command(stores, wait_seconds=10).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user", end_running_work=True)
    await stopper

    running = document["surfaces"]["running_work"]
    assert document["runs_cancelled"] == 1 and document["surfaces_unconfirmed"] == [] and document["returncode"] == 0
    assert running["action"] == "ended" and 300 <= running["stopped_after_ms"] <= document["elapsed_ms"]
    assert running["confirmed_by"] == "run_status", "what the stop time was confirmed from"


# ── The document ────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_the_document_times_every_surface_from_the_command_s_start(stores) -> None:
    await stores.users.create_user(_account())
    before = datetime.now(UTC)
    document = await _command(stores).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    after = datetime.now(UTC)

    started = datetime.fromisoformat(document["started_at"])
    assert before - timedelta(seconds=1) <= started <= after and started.tzinfo is not None
    assert set(document["surfaces"]) == {"stored_role", "sign_in", "sessions", "personal_access_tokens", "internal_launches", "running_work"}
    for name, entry in document["surfaces"].items():
        assert {"action", "count", "stopped_after_ms", "stopped_at"} <= set(entry), name
        if name == "running_work":
            continue
        assert 0 <= entry["stopped_after_ms"] <= document["elapsed_ms"], name
        # One anchor: the wall time is the start plus the monotonic offset, so the two never disagree.
        assert datetime.fromisoformat(entry["stopped_at"]) == started + timedelta(milliseconds=entry["stopped_after_ms"]), name
    # A surface refused (here: limited) at its next use reports the limit's commit.
    committed = document["surfaces"]["stored_role"]["stopped_after_ms"]
    # Sessions end in the limit's own transaction, so they report its commit too.
    assert all(document["surfaces"][name]["stopped_after_ms"] == committed for name in ("sign_in", "personal_access_tokens", "internal_launches", "sessions"))
    assert document["surfaces_unconfirmed"] == [] and document["returncode"] == 0
    json.dumps(document)


@pytest.mark.anyio
async def test_list_shows_each_account_s_limit_and_the_limits_held_without_an_account(stores) -> None:
    await stores.users.create_user(_account())
    await stores.users.create_user(_account("owner@example.com", "sub-owner"))
    command = _command(stores)
    await command.run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    await command.run("limit-role", issuer=ISSUER, subject="sub-early", role="user")

    listed = await command.run("list")

    by_email = {entry["email"]: entry for entry in listed["accounts"]}
    assert by_email["pat@example.com"]["role_limit"] == "user" and by_email["pat@example.com"]["role"] == "user"
    assert by_email["owner@example.com"]["role_limit"] is None and by_email["owner@example.com"]["role"] == "admin"
    assert [(entry["issuer"], entry["subject"], entry["role"]) for entry in listed["role_limits_without_account"]] == [(ISSUER, "sub-early", "user")]
    assert listed["role_limits_without_account"][0]["limited_at"]


@pytest.mark.anyio
async def test_a_limit_that_reaches_an_account_with_no_recorded_issuer_is_listed_on_that_account(stores) -> None:
    """A row linked before its issuer was recorded matches on its subject; the limit applies to it, so it is not "without an account"."""
    await stores.users.create_user(_account("legacy@example.com", "sub-legacy", issuer=None))
    command = _command(stores)
    await command.run("limit-role", issuer=ISSUER, subject="sub-legacy", role="user")
    await command.run("disable", issuer=ISSUER, subject="sub-legacy")

    listed = await command.run("list")

    legacy = next(entry for entry in listed["accounts"] if entry["email"] == "legacy@example.com")
    assert legacy["role"] == "user" and legacy["role_limit"] == "user" and legacy["disabled"] is True
    assert listed["role_limits_without_account"] == [] and listed["disabled_without_account"] == []


@pytest.mark.anyio
async def test_a_limit_committed_between_a_first_sign_in_s_read_and_its_insert_still_holds(stores, monkeypatch: pytest.MonkeyPatch) -> None:
    """The creating transaction applies the limit again after its insert, so the read before it is not the last word."""
    command = _command(stores)
    original = SQLiteUserRepository._role_limit
    fired: list[dict] = []

    async def _then_the_limit_lands(self, session, row):
        seen = await original(self, session, row)
        if not fired and row.id is None:  # the creating read, before the row exists
            fired.append(await command.run("limit-role", issuer=ISSUER, subject="sub-new", role="user"))
        return seen

    monkeypatch.setattr(SQLiteUserRepository, "_role_limit", _then_the_limit_lands)
    created = await stores.users.create_user(_account("new@example.com", "sub-new"))
    monkeypatch.setattr(SQLiteUserRepository, "_role_limit", original)

    assert fired and fired[0]["accounts"] == [], "the limit committed before the account existed"
    assert _column(stores, str(created.id)) == "user"
    assert created.system_role == "user" and created.role_limit == "user", "and the account handed back says so"


@pytest.mark.anyio
async def test_an_address_names_its_identity_and_a_local_account_has_none_to_limit(stores) -> None:
    await stores.users.create_user(_account())
    await stores.users.create_user(User(email="local@example.com", password_hash="x", system_role="admin"))
    command = _command(stores)
    assert (await command.run("limit-role", email="pat@example.com", role="user"))["identity"] == {"issuer": ISSUER, "subject": "sub-pat"}
    with pytest.raises(CommandError, match="local-password account"):
        await command.run("limit-role", email="local@example.com", role="user")


@pytest.mark.anyio
async def test_only_a_role_below_administrator_is_a_limit(stores) -> None:
    with pytest.raises(CommandError, match="user"):
        await _command(stores).run("limit-role", issuer=ISSUER, subject="sub-pat", role="admin")


@pytest.mark.anyio
async def test_in_local_mode_the_last_administrator_is_not_limited(stores) -> None:
    """With local passwords on, no administrator reopens first-boot setup to whoever reaches it first."""
    only = await stores.users.create_user(_account())
    local = _command(stores, setup_opens_without_admin=True)
    with pytest.raises(CommandError, match="first-boot setup"):
        await local.run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    assert (await stores.users.get_user_by_id(str(only.id))).system_role == "admin" and await stores.users.role_limit_for(ISSUER, "sub-pat") is None, "refused before anything changed"

    await stores.users.create_user(_account("owner@example.com", "sub-owner"))
    assert (await local.run("limit-role", issuer=ISSUER, subject="sub-pat", role="user"))["verdict"] == "limited", "another administrator remains"
    # Sign-on only has no first-boot setup to reopen; nothing to refuse there.
    assert (await _command(stores).run("limit-role", issuer=ISSUER, subject="sub-owner", role="user"))["verdict"] == "limited"


@pytest.mark.anyio
async def test_in_local_mode_a_person_who_is_every_administrator_through_two_accounts_is_not_limited(stores) -> None:
    """The count is of administrator accounts, not people: two accounts of one person can be all there are."""
    await stores.users.create_user(_account())
    await stores.users.create_user(_account("pat.chen@example.com", provider="sso-basic"))
    with pytest.raises(CommandError, match="first-boot setup"):
        await _command(stores, setup_opens_without_admin=True).run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    assert await stores.users.count_admin_users() == 2


def test_the_command_reads_local_mode_and_what_reads_a_run_s_role_from_the_deployment(monkeypatch: pytest.MonkeyPatch) -> None:
    """What ``_run`` builds the command with; the unit tests above pass these by hand."""
    from app.gateway.auth import accounts, mode
    from deerflow.config.app_config import AppConfig
    from deerflow.config.authorization_config import AuthorizationConfig
    from deerflow.config.sandbox_config import SandboxConfig

    config = AppConfig(sandbox=SandboxConfig(use="deerflow.sandbox.local:LocalSandboxProvider"), authorization=AuthorizationConfig(enabled=True))
    monkeypatch.setattr(mode, "sign_on_only", lambda: False)
    assert accounts.deployment_options(config) == {"role_readers": ("authorization",), "setup_opens_without_admin": True}
    monkeypatch.setattr(mode, "sign_on_only", lambda: True)
    assert accounts.deployment_options(config)["setup_opens_without_admin"] is False


# ── Sign-in, with the claim on the other side of the limit ──────────────


@pytest.mark.anyio
async def test_a_sign_in_with_an_admin_claim_yields_the_limit_and_after_the_lift_yields_admin(stores, caplog: pytest.LogCaptureFixture) -> None:
    from app.gateway.auth.local_provider import LocalAuthProvider
    from app.gateway.auth.oidc import OIDCIdentity
    from app.gateway.auth.user_provisioning import get_or_provision_oidc_user
    from deerflow.config.auth_config import OIDCProviderConfig

    claim = "urn:zitadel:iam:org:project:roles"
    provider = OIDCProviderConfig(
        display_name="Company", issuer=ISSUER, client_id="hartmesh", client_secret="FAKE-CREDENTIAL-SENTINEL-NOT-A-KEY", access_claim=claim, access_values=["admin", "member"], access_roles={"admin": "admin", "member": "user"}
    )
    token = {"sub": "sub-pat", "email": "pat@example.com", "email_verified": True, claim: ["admin"]}
    identity = OIDCIdentity(provider="sso", subject="sub-pat", email="pat@example.com", email_verified=True, name="Pat", claims=token, id_token_claims=token, userinfo_claims={"sub": "sub-pat"})
    local = LocalAuthProvider(stores.users)
    command = _command(stores)

    # Limited before the first sign-in: the account is created at the limit.
    await command.run("limit-role", issuer=ISSUER, subject="sub-pat", role="user")
    created = await get_or_provision_oidc_user("sso", provider, identity, local)
    assert created["created"] is True and created["user"].system_role == "user"
    # And every later sign-in, although the claim still says admin.
    caplog.set_level("INFO", logger="app.gateway.auth.user_provisioning")
    again = await get_or_provision_oidc_user("sso", provider, identity, local)
    assert again["user"].system_role == "user" and _column(stores, str(again["user"].id)) == "user"
    assert not any("is now admin" in record.getMessage() for record in caplog.records), "the log does not claim a role the limit withholds"
    assert any("role limit" in record.getMessage() for record in caplog.records)

    await command.run("lift-role-limit", issuer=ISSUER, subject="sub-pat")
    lifted = await get_or_provision_oidc_user("sso", provider, identity, local)
    assert lifted["user"].system_role == "admin" and _column(stores, str(lifted["user"].id)) == "admin"


# ── The administrator routes ────────────────────────────────────────────


@pytest.mark.anyio
async def test_require_admin_user_refuses_a_personal_access_token_itself() -> None:
    """The token route allowlist was the only barrier; the dependency is one too now."""
    from fastapi import HTTPException

    from app.gateway.auth_disabled import AUTH_SOURCE_PAT, AUTH_SOURCE_SESSION
    from app.gateway.deps import require_admin_user

    administrator = SimpleNamespace(id=uuid4(), system_role="admin")
    by_token = SimpleNamespace(state=SimpleNamespace(user=administrator, auth_source=AUTH_SOURCE_PAT))
    with pytest.raises(HTTPException) as refused:
        await require_admin_user(by_token, detail="Administrators only")
    assert refused.value.status_code == 403 and refused.value.detail == "Administrators only"

    by_session = SimpleNamespace(state=SimpleNamespace(user=administrator, auth_source=AUTH_SOURCE_SESSION))
    assert await require_admin_user(by_session, detail="Administrators only") is administrator


# ── The command line ────────────────────────────────────────────────────


def test_main_takes_the_role_and_the_running_work_flag_for_the_limit_only(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from app.gateway.auth import accounts

    seen: dict = {}

    async def _record(command: str, **kwargs: object) -> dict:
        seen.update(kwargs, command=command)
        return {"command": command, "returncode": 0}

    monkeypatch.setattr(accounts, "_run", _record)
    assert accounts.main(["limit-role", "--issuer", ISSUER, "--subject", "sub-x", "--end-running-work"]) == 0
    assert seen["command"] == "limit-role" and seen["role"] == "user" and seen["end_running_work"] is True
    capsys.readouterr()
    assert accounts.main(["lift-role-limit", "--issuer", ISSUER, "--subject", "sub-x", "--end-running-work"]) == 1
    assert "--end-running-work" in json.loads(capsys.readouterr().out)["error"]
    assert accounts.main(["disable", "--issuer", ISSUER, "--subject", "sub-x", "--role", "user"]) == 1
    assert "--role" in json.loads(capsys.readouterr().out)["error"]


@pytest.mark.parametrize(
    ("argv", "command", "names"),
    [
        (["limit-role", "--issuer", ISSUER, "--subject", "sub-x", "--force"], "limit-role", "--force"),
        (["limit-roles", "--issuer", ISSUER], None, "limit-roles"),
        (["limit-role", "--wait-seconds", "soon"], "limit-role", "soon"),
        (["--subject", "list", "limit-role", "--force"], "limit-role", "--force"),
    ],
    ids=["unknown-flag", "unknown-command", "bad-value", "flag-value-spelled-like-a-command"],
)
def test_a_malformed_command_line_is_one_document_and_exit_1_never_the_2_of_an_unconfirmed_run(argv: list[str], command: str | None, names: str, capsys: pytest.CaptureFixture[str]) -> None:
    from app.gateway.auth import accounts

    assert accounts.main(argv) == 1
    captured = capsys.readouterr()
    lines = [line for line in captured.out.splitlines() if line.strip()]
    assert len(lines) == 1, captured
    document = json.loads(lines[0])
    assert document["command"] == command and names in document["error"]


def test_a_role_no_limit_can_hold_is_refused_as_a_document(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """``--role admin`` reaches the command's own refusal rather than argparse's usage text."""
    from app.gateway.auth import accounts

    async def _refuse_through_the_command(command: str, **kwargs: object) -> dict:
        return await accounts.AccountsCommand(None, tokens=None, schedules=None).limit_role(("https://x", "sub-x"), role=str(kwargs["role"]))  # type: ignore[arg-type]

    monkeypatch.setattr(accounts, "_run", _refuse_through_the_command)
    assert accounts.main(["limit-role", "--issuer", ISSUER, "--subject", "sub-x", "--role", "admin"]) == 1
    assert "below administrator" in json.loads(capsys.readouterr().out)["error"]


# ── The refused accounts, in one read ───────────────────────────────────


@pytest.mark.anyio
async def test_the_refused_accounts_are_every_account_a_refusal_covers_derived_as_each_read_derives_it(stores) -> None:
    """What a Gateway's refusal watch reads in one query: the same match the per-account read makes."""
    under_sso = await stores.users.create_user(_account(role="user"))
    under_basic = await stores.users.create_user(_account("pat.chen@example.com", provider="sso-basic", role="user"))
    legacy = await stores.users.create_user(_account("legacy@example.com", "sub-legacy", role="user", issuer=None))
    elsewhere = await stores.users.create_user(_account("pat@other.example.com", provider="other-sso", issuer="https://login.other.example.com", role="user"))
    await stores.users.create_user(_account("sam@example.com", "sub-sam", role="user"))
    assert await stores.users.list_refused_user_ids() == set()

    await stores.users.disable_identity(ISSUER + "/", "sub-pat")
    await stores.users.disable_identity(ISSUER, "sub-legacy")

    refused = await stores.users.list_refused_user_ids()
    assert refused == {str(under_sso.id), str(under_basic.id), str(legacy.id)}
    assert str(elsewhere.id) not in refused, "the same subject at another issuer is another person"
    for user_id in refused:
        assert (await stores.users.get_user_by_id(user_id)).disabled_at is not None
