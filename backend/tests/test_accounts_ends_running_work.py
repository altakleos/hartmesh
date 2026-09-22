"""Turning an account off ends the work it has executing, and says whether it did.

The refusal ``disable`` records stops the next request and the next launch. A
run already executing was the one path left: on a deployment a person's bash
call (``sleep 541`` in the gVisor sandbox) ran to its natural end 537 s after
their role was removed and ``accounts disable`` ran, with the stream open the
whole time, and finished ``success``. Stopping the output would not have been
enough -- the work itself has to stop, and the operator has to be able to tell
that it did.

The command holds the database and nothing else. So it cancels the way a
person's own cancel does, durably, and the worker that owns the run applies it;
then it waits, bounded, and reports each run it could not confirm stopped as a
failure rather than as a silent success.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-accounts-runs-min-32-chars")

from app.gateway.auth.accounts import EXIT_UNCONFIRMED_RUNS, AccountsCommand
from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository

ISSUER = "https://login.example.com/realms/tenant"
posix_only = pytest.mark.skipif(os.name == "nt", reason="POSIX process-group semantics")


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def stores(tmp_path) -> Iterator[tuple[SQLiteUserRepository, object]]:
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.run import RunRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/accounts-runs.db", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    tenant = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
    try:
        yield SQLiteUserRepository(session_factory), RunRepository(session_factory, tenant=tenant)
    finally:
        asyncio.run(close_engine())


def _account(email: str = "pat@example.com", subject: str = "sub-pat", *, role: str = "user") -> User:
    return User(email=email, password_hash=None, system_role=role, oauth_provider="sso", oauth_id=subject, oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC))


async def _seed_run(runs, user_id: str, *, status: str = "running", thread_id: str | None = None) -> str:
    run_id = str(uuid4())
    await runs.put(
        run_id,
        thread_id=thread_id or str(uuid4()),
        user_id=user_id,
        status=status,
        created_at=datetime.now(UTC).isoformat(),
    )
    return run_id


async def _row(runs, run_id: str):
    """Read a row without the ambient user scope the repository applies by default."""
    return await runs.get(run_id, user_id=None)


def _command(users, runs, *, wait_seconds: float = 2.0) -> AccountsCommand:
    return AccountsCommand(users, tokens=None, schedules=None, runs=runs, wait_seconds=wait_seconds)


# ── What the command finds, asks for, and reports ───────────────────────


@pytest.mark.anyio
async def test_disable_cancels_the_account_s_running_runs(stores) -> None:
    users, runs = stores
    account = await users.create_user(_account())
    run_id = await _seed_run(runs, str(account.id))

    document = await _command(users, runs, wait_seconds=0).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["runs_found"] == 1
    # Same durable request a person's own cancel makes; the owner applies it.
    assert (await _row(runs, run_id))["cancel_action"] == "interrupt"


@pytest.mark.anyio
async def test_disable_reports_a_run_it_could_not_confirm_stopped_as_a_failure(stores) -> None:
    """No worker ever applied the cancellation: that is a failure, not a success."""
    users, runs = stores
    account = await users.create_user(_account())
    run_id = await _seed_run(runs, str(account.id))

    document = await _command(users, runs, wait_seconds=0.2).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["runs_found"] == 1
    assert document["runs_cancelled"] == 0
    assert document["runs_unconfirmed"] == [run_id]
    # Its own status, distinct from the 1 a refusal exits with: the refusal
    # here *was* recorded, and an offboarding script must not read this as
    # "the command did nothing".
    assert document["returncode"] == EXIT_UNCONFIRMED_RUNS == 2
    assert "runs_unconfirmed" in document["note"]
    assert "Gateway" not in document["note"], "the command cannot tell a slow unwind from a dead Gateway"


@pytest.mark.anyio
async def test_disable_confirms_a_run_the_worker_stopped(stores) -> None:
    users, runs = stores
    account = await users.create_user(_account())
    run_id = await _seed_run(runs, str(account.id))
    original_get = runs.get
    applied = {"done": False}

    async def worker_applies_the_cancellation(run_id_arg, *args, **kwargs):
        row = await original_get(run_id_arg, *args, **kwargs)
        if row is not None and row.get("cancel_action") and not applied["done"]:
            applied["done"] = True
            await runs.update_status(run_id_arg, "interrupted")
            return await original_get(run_id_arg, *args, **kwargs)
        return row

    runs.get = worker_applies_the_cancellation
    document = await _command(users, runs, wait_seconds=5).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["runs_found"] == 1
    assert document["runs_cancelled"] == 1
    assert document["runs_unconfirmed"] == []
    assert document["returncode"] == 0
    assert run_id not in str(document), "a run that stopped is counted, not named"


@pytest.mark.anyio
async def test_disable_leaves_another_account_s_run_alone(stores) -> None:
    users, runs = stores
    account = await users.create_user(_account())
    colleague = await users.create_user(_account(email="other@example.com", subject="sub-other"))
    theirs = await _seed_run(runs, str(account.id))
    not_theirs = await _seed_run(runs, str(colleague.id))

    document = await _command(users, runs, wait_seconds=0).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["runs_found"] == 1
    assert (await _row(runs, theirs))["cancel_action"] == "interrupt"
    assert (await _row(runs, not_theirs))["cancel_action"] is None


@pytest.mark.anyio
async def test_disable_ignores_runs_that_already_finished(stores) -> None:
    users, runs = stores
    account = await users.create_user(_account())
    finished = await _seed_run(runs, str(account.id), status="success")

    document = await _command(users, runs, wait_seconds=0).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["runs_found"] == 0
    assert (await _row(runs, finished))["cancel_action"] is None


@pytest.mark.anyio
async def test_disable_with_nothing_running_reports_zero_and_keeps_every_other_field(stores) -> None:
    """The rest of the document is exactly what it was before runs entered it."""
    users, runs = stores
    await users.create_user(_account())

    document = await _command(users, runs).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["runs_found"] == 0 and document["runs_cancelled"] == 0
    assert document["runs_unconfirmed"] == [] and document["returncode"] == 0
    assert document["verdict"] == "disabled"
    assert document["sessions_ended"] is True
    assert document["tokens_revoked"] == 0 and document["schedules_held"] == 0
    assert document["account"]["disabled"] is True
    assert document["identity"] == {"issuer": ISSUER, "subject": "sub-pat"}


@pytest.mark.anyio
async def test_disable_before_the_first_sign_in_still_reports_no_runs(stores) -> None:
    users, runs = stores
    document = await _command(users, runs).run("disable", issuer=ISSUER, subject="sub-never-seen")

    assert document["account"] is None
    assert document["runs_found"] == 0 and document["returncode"] == 0


@pytest.mark.anyio
async def test_one_run_that_refuses_the_request_does_not_hide_the_others(stores) -> None:
    users, runs = stores
    account = await users.create_user(_account())
    first = await _seed_run(runs, str(account.id))
    second = await _seed_run(runs, str(account.id))
    original = runs.request_cancel_compat

    async def refuse_the_first(run_id, **kwargs):
        if run_id == first:
            raise RuntimeError("store unavailable")
        return await original(run_id, **kwargs)

    runs.request_cancel_compat = refuse_the_first
    document = await _command(users, runs, wait_seconds=0.2).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["runs_found"] == 2
    assert sorted(document["runs_unconfirmed"]) == sorted([first, second])
    assert (await _row(runs, second))["cancel_action"] == "interrupt"


# ── end-sessions: only when asked ───────────────────────────────────────


@pytest.mark.anyio
async def test_end_sessions_leaves_a_demoted_administrator_s_run_running(stores) -> None:
    """Demoting someone is not removing them; their work is still theirs."""
    users, runs = stores
    account = await users.create_user(_account(role="admin"))
    run_id = await _seed_run(runs, str(account.id))

    document = await _command(users, runs).run("end-sessions", issuer=ISSUER, subject="sub-pat")

    assert document["sessions_ended"] is True
    # The keys are present either way, zeroed, so one script can read this
    # document without knowing which form produced it.
    assert document["runs_found"] == 0 and document["runs_cancelled"] == 0
    assert document["runs_unconfirmed"] == [] and document["returncode"] == 0
    assert (await _row(runs, run_id))["cancel_action"] is None
    assert "--end-running-work" in document["note"]


@pytest.mark.anyio
async def test_end_sessions_ends_the_run_when_the_deployer_asks(stores) -> None:
    users, runs = stores
    account = await users.create_user(_account(role="admin"))
    run_id = await _seed_run(runs, str(account.id))

    document = await _command(users, runs, wait_seconds=0).run("end-sessions", issuer=ISSUER, subject="sub-pat", end_running_work=True)

    assert document["runs_found"] == 1
    assert (await _row(runs, run_id))["cancel_action"] == "interrupt"


# ── The command line ────────────────────────────────────────────────────


def test_the_flag_belongs_to_end_sessions_only(capsys) -> None:
    import json

    from app.gateway.auth.accounts import main

    assert main(["disable", "--issuer", ISSUER, "--subject", "s", "--end-running-work"]) == 1
    assert "--end-running-work belongs to end-sessions" in json.loads(capsys.readouterr().out)["error"]


def test_a_negative_wait_is_refused(capsys) -> None:
    import json

    from app.gateway.auth.accounts import main

    assert main(["disable", "--issuer", ISSUER, "--subject", "s", "--wait-seconds", "-1"]) == 1
    assert "cannot be negative" in json.loads(capsys.readouterr().out)["error"]


@pytest.mark.anyio
async def test_a_run_that_finished_on_its_own_is_named_rather_than_counted_as_cancelled(stores) -> None:
    """It stopped -- but it also delivered its result into a thread after the removal."""
    users, runs = stores
    account = await users.create_user(_account())
    run_id = await _seed_run(runs, str(account.id))
    original_get = runs.get
    finished = {"done": False}

    async def it_completes_first(run_id_arg, *args, **kwargs):
        if not finished["done"]:
            finished["done"] = True
            await runs.update_status(run_id_arg, "success")
        return await original_get(run_id_arg, *args, **kwargs)

    runs.get = it_completes_first
    document = await _command(users, runs, wait_seconds=5).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["runs_found"] == 1
    assert document["runs_finished_first"] == [run_id]
    assert document["runs_cancelled"] == 0
    assert document["runs_unconfirmed"] == [] and document["returncode"] == 0
    assert "delivered" in document["note"]


@pytest.mark.anyio
async def test_the_run_of_a_sibling_account_the_refusal_covers_is_ended_too(stores) -> None:
    """One identity can hold an account under each configured provider.

    `disable` records the refusal against the identity, so both accounts are
    refused; leaving one of them writing files would be the same defect one
    account over.
    """
    users, runs = stores
    named = await users.create_user(_account(email="pat@example.com", subject="sub-pat"))
    sibling = await users.create_user(User(email="pat.other@example.com", password_hash=None, system_role="user", oauth_provider="entra", oauth_id="sub-pat", oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC)))
    named_run = await _seed_run(runs, str(named.id))
    sibling_run = await _seed_run(runs, str(sibling.id))

    document = await _command(users, runs, wait_seconds=0).run("disable", email="pat@example.com")

    assert document["runs_found"] == 2
    assert [entry["email"] for entry in document["identity_also_covers"]] == ["pat.other@example.com"]
    assert (await _row(runs, named_run))["cancel_action"] == "interrupt"
    assert (await _row(runs, sibling_run))["cancel_action"] == "interrupt"


def test_the_exit_status_carries_an_unconfirmed_run(stores, monkeypatch, capsys) -> None:
    """The headline of the feature, through the command line the deployer runs.

    Synchronous on purpose: ``main`` owns its own ``asyncio.run``, which is
    what the deployer invokes, and asserting the document alone would leave
    the exit status -- the part a runbook branches on -- unproven.
    """
    import json as json_module

    from app.gateway.auth import accounts as accounts_module

    users, runs = stores

    async def seed() -> str:
        account = await users.create_user(_account())
        return await _seed_run(runs, str(account.id))

    run_id = asyncio.run(seed())

    async def fake_run(command, **kwargs):
        return await _command(users, runs, wait_seconds=0).run(command, issuer=kwargs.get("issuer"), subject=kwargs.get("subject"), email=kwargs.get("email"))

    monkeypatch.setattr(accounts_module, "_run", fake_run)
    status = accounts_module.main(["disable", "--issuer", ISSUER, "--subject", "sub-pat"])

    document = json_module.loads(capsys.readouterr().out)
    assert document["runs_unconfirmed"] == [run_id], document
    assert status == EXIT_UNCONFIRMED_RUNS
