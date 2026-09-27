"""Many identities at one issuer in one call: every run cancelled first, one wait, one entry each.

A company that removes twenty people at once used to cost twenty calls, each
with its own wait for that person's runs to stop, so one removal spanned
several of the deployer's passes. ``--subjects`` names them all: each
identity is refused and its runs are asked to stop before any wait begins,
and then the command waits once for all of them. The document carries one
entry per identity -- the same document the single-subject form prints --
with totals across them, and the exit status is the worst: 1 if some
identity was refused, else 2 if any was unconfirmed, else 0. One identity's
refusal does not stop the others.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-accounts-batch-min-32-chars")

from app.gateway.auth.accounts import EXIT_UNCONFIRMED_RUNS, MAX_BATCH_SUBJECTS, AccountsCommand, CommandError
from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.gateway.refusal_watch import RefusalWatch
from deerflow.runtime.owner_holdings import OwnerHoldings

ISSUER = "https://login.example.com/realms/tenant"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def stores(tmp_path) -> Iterator[SimpleNamespace]:
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.persistence.refusal_sweeps import RefusalSweepRepository
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/batch.db", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    tenant = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
    try:
        yield SimpleNamespace(
            users=SQLiteUserRepository(session_factory),
            tokens=PersonalAccessTokenRepository(session_factory, tenant=tenant),
            schedules=ScheduledTaskRepository(session_factory),
            runs=RunRepository(session_factory, tenant=tenant),
            sweeps=RefusalSweepRepository(session_factory),
        )
    finally:
        asyncio.run(close_engine())


def _account(subject: str, *, provider: str = "sso", role: str = "user", email: str | None = None) -> User:
    return User(
        email=email or f"{subject}@example.com",
        password_hash=None,
        system_role=role,
        oauth_provider=provider,
        oauth_id=subject,
        oauth_issuer=ISSUER,
        last_sign_in_at=datetime.now(UTC),
    )


def _command(stores: SimpleNamespace, **kwargs) -> AccountsCommand:
    kwargs.setdefault("wait_seconds", 5)
    return AccountsCommand(stores.users, tokens=stores.tokens, schedules=stores.schedules, runs=stores.runs, **kwargs)


async def _seed_run(stores: SimpleNamespace, user_id: str, *, role: str | None = None) -> str:
    """A run no worker will ever stop: its cancellation stays unconfirmed until the wait runs out."""
    run_id = str(uuid4())
    sealed = None if role is None else {"user_id": user_id, "role": role}
    await stores.runs.put(run_id, thread_id=str(uuid4()), user_id=user_id, status="running", created_at=datetime.now(UTC).isoformat(), principal_projection_json=sealed)
    return run_id


async def _schedule(stores: SimpleNamespace, user_id: str) -> str:
    task_id = str(uuid4())
    await stores.schedules.create(
        task_id=task_id,
        user_id=user_id,
        thread_id=f"thread-{task_id}",
        context_mode="reuse_thread",
        assistant_id="lead_agent",
        title="Weekly numbers",
        prompt="Send me the weekly numbers",
        schedule_type="cron",
        schedule_spec={"cron": "0 9 * * 1"},
        timezone="UTC",
        next_run_at=datetime.now(UTC) + timedelta(days=1),
    )
    return task_id


def _entry(document: dict, subject: str) -> dict:
    [entry] = [entry for entry in document["identities"] if entry["identity"]["subject"] == subject]
    return entry


async def _worker(stores: SimpleNamespace, run_id: str, status: str, stop: asyncio.Event) -> None:
    """The run's worker: it applies the cancellation as soon as it is asked, ending the run with ``status``."""
    while not stop.is_set():
        row = await stores.runs.get(run_id, user_id=None)
        if row and row.get("cancel_action") == "interrupt":
            await stores.runs.update_status(run_id, status)
            return
        await asyncio.sleep(0.02)


# ── disable ─────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_ten_identities_take_one_wait_one_entry_each_and_the_worst_exit_status(stores) -> None:
    """Three with a running tool call, one with no account, one refused: every run is asked to stop before the one wait."""
    subjects = [f"sub-{index}" for index in range(10)]
    accounts = {}
    for subject in subjects[:8]:
        accounts[subject] = await stores.users.create_user(_account(subject))
    # Two configured providers at one issuer: this subject names two accounts,
    # which the command will not guess between.
    await stores.users.create_user(_account("sub-8"))
    await stores.users.create_user(_account("sub-8", provider="sso-basic", email="sub-8.basic@example.com"))
    # sub-9 has no account yet.
    running = {subject: await _seed_run(stores, str(accounts[subject].id)) for subject in subjects[:3]}
    wait = 1.0

    started = time.monotonic()
    document = await _command(stores, wait_seconds=wait).run("disable", issuer=ISSUER, subjects=subjects)
    took = time.monotonic() - started

    # Bounded by the one wait; that every run is asked to stop before it is
    # the next test's, where a run stops only when asked.
    assert took < wait + 3.0, took
    assert document["command"] == "disable" and document["issuer"] == ISSUER
    assert [entry["identity"]["subject"] for entry in document["identities"]] == subjects
    for subject, run_id in running.items():
        entry = _entry(document, subject)
        assert entry["verdict"] == "disabled" and entry["runs_unconfirmed"] == [run_id] and entry["returncode"] == EXIT_UNCONFIRMED_RUNS
        assert (await stores.runs.get(run_id, user_id=None))["cancel_action"] == "interrupt"
    for subject in subjects[3:8]:
        assert _entry(document, subject)["verdict"] == "disabled" and _entry(document, subject)["returncode"] == 0, subject
        # With no runs, its running work stopped at once: not when the others' wait ended.
        assert _entry(document, subject)["surfaces"]["running_work"]["stopped_after_ms"] < wait * 1000 / 2, subject
    no_account = _entry(document, "sub-9")
    assert no_account["verdict"] == "disabled" and no_account["account"] is None
    refused = _entry(document, "sub-8")
    assert refused["returncode"] == 1 and "2 accounts have subject 'sub-8'" in refused["error"] and refused["verdict"] == "refused"
    # The refusal of one did not stop the others, and was not recorded for it.
    refused_ids = await stores.users.list_refused_user_ids()
    assert all(str(accounts[subject].id) in refused_ids for subject in subjects[:8])
    assert {issuer_subject[1] for issuer_subject in await stores.users.list_disabled_identities()} == set(subjects) - {"sub-8"}
    totals = document["totals"]
    assert totals["identities"] == 10
    assert totals["verdicts"] == {"disabled": 9, "refused": 1}
    assert totals["subjects_refused"] == ["sub-8"] and totals["subjects_failed"] == []
    assert totals["subjects_unconfirmed"] == sorted(running)
    assert totals["runs_found"] == 3 and totals["runs_cancelled"] == 0
    assert totals["runs_unconfirmed"] == sorted(running.values())
    assert "running_work" in totals["surfaces_unconfirmed"]
    # The worst: some identity was refused.
    assert document["returncode"] == 1
    assert datetime.fromisoformat(document["started_at"]) <= datetime.now(UTC) and document["elapsed_ms"] >= int(wait * 1000)
    # Every entry is timed from the command's start, not from when its identity came up.
    timed = [entry for entry in document["identities"] if "started_at" in entry]
    assert {entry["started_at"] for entry in timed} == {document["started_at"]}
    assert all(entry["elapsed_ms"] >= int(wait * 1000) for entry in timed)


@pytest.mark.anyio
async def test_with_none_refused_the_exit_status_is_two_when_any_is_unconfirmed_else_zero(stores) -> None:
    first = await stores.users.create_user(_account("sub-a"))
    await stores.users.create_user(_account("sub-b"))
    await _seed_run(stores, str(first.id))

    unconfirmed = await _command(stores, wait_seconds=0.2).run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b"])
    assert unconfirmed["returncode"] == EXIT_UNCONFIRMED_RUNS and unconfirmed["totals"]["subjects_unconfirmed"] == ["sub-a"]

    # The run finished meanwhile; a re-run re-checks and confirms everyone.
    [row] = await stores.runs.list_active_by_user(str(first.id))
    await stores.runs.update_status(row["run_id"], "interrupted")
    again = await _command(stores, wait_seconds=0.2).run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b"])
    assert again["returncode"] == 0 and again["totals"]["verdicts"] == {"already_disabled": 2}


@pytest.mark.anyio
async def test_each_entry_is_the_document_the_single_subject_form_prints(stores) -> None:
    await stores.users.create_user(_account("sub-a"))
    await stores.users.create_user(_account("sub-b"))

    single = await _command(stores).run("disable", issuer=ISSUER, subject="sub-a")
    batch = await _command(stores).run("disable", issuer=ISSUER, subjects=["sub-b"])

    [entry] = batch["identities"]
    assert set(entry) == set(single)
    assert set(entry["surfaces"]) == set(single["surfaces"])
    # The single-subject document carries no batch keys.
    assert not {"identities", "totals"} & set(single)


@pytest.mark.anyio
async def test_the_gateway_processes_are_asked_to_look_twice_for_the_whole_batch_not_twice_per_person(stores) -> None:
    for subject in ("sub-a", "sub-b", "sub-c"):
        await stores.users.create_user(_account(subject))
    asked: list[int] = []
    request_check = stores.sweeps.request_check

    async def _counted() -> int:
        asked.append(await request_check())
        return asked[-1]

    stores.sweeps.request_check = _counted

    document = await _command(stores, sweeps=stores.sweeps).run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b", "sub-c"])

    # Once the refusals have committed, and once more after the runs are over.
    assert len(asked) == 2
    assert all(set(entry["surfaces"]) >= {"sse_streams", "sandboxes"} for entry in document["identities"])


@pytest.mark.anyio
async def test_a_run_admitted_during_the_wait_is_found_and_counted_for_its_own_identity(stores) -> None:
    """A request that authenticated before the refusal committed can insert a run after the first look."""
    first = await stores.users.create_user(_account("sub-a"))
    second = await stores.users.create_user(_account("sub-b"))
    await _seed_run(stores, str(first.id))
    await _seed_run(stores, str(second.id))
    command = _command(stores, wait_seconds=0.3)
    cancel_and_wait = command._cancel_and_wait
    late: list[str] = []

    async def _meanwhile(owners: dict[str, str], **kwargs) -> dict[str, str]:
        stopped = await cancel_and_wait(owners, **kwargs)
        if not late:
            late.append(await _seed_run(stores, str(second.id)))
        return stopped

    command._cancel_and_wait = _meanwhile
    document = await command.run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b"])

    assert _entry(document, "sub-b")["runs_found"] == 2 and late[0] in _entry(document, "sub-b")["runs_unconfirmed"]
    assert _entry(document, "sub-a")["runs_found"] == 1
    assert (await stores.runs.get(late[0], user_id=None))["cancel_action"] == "interrupt"


@pytest.mark.anyio
async def test_every_run_is_asked_to_stop_before_the_one_wait_and_each_stop_is_its_own_identity_s(stores) -> None:
    """A run that stops when asked is confirmed even beside one that never stops: it was not left for after that one's wait."""
    stuck = await stores.users.create_user(_account("sub-a"))
    stoppable = await stores.users.create_user(_account("sub-b"))
    finishing = await stores.users.create_user(_account("sub-c"))
    stuck_run = await _seed_run(stores, str(stuck.id))
    runs = {"sub-b": await _seed_run(stores, str(stoppable.id)), "sub-c": await _seed_run(stores, str(finishing.id))}
    stop = asyncio.Event()
    workers = [asyncio.create_task(_worker(stores, runs["sub-b"], "interrupted", stop)), asyncio.create_task(_worker(stores, runs["sub-c"], "success", stop))]
    try:
        document = await _command(stores, wait_seconds=1.0).run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b", "sub-c"])
    finally:
        stop.set()
        await asyncio.gather(*workers)

    a, b, c = (_entry(document, subject) for subject in ("sub-a", "sub-b", "sub-c"))
    assert (a["runs_found"], a["runs_cancelled"], a["runs_finished_first"], a["runs_unconfirmed"], a["returncode"]) == (1, 0, [], [stuck_run], EXIT_UNCONFIRMED_RUNS)
    assert (b["runs_found"], b["runs_cancelled"], b["runs_finished_first"], b["runs_unconfirmed"], b["returncode"]) == (1, 1, [], [], 0)
    assert (c["runs_found"], c["runs_cancelled"], c["runs_finished_first"], c["runs_unconfirmed"], c["returncode"]) == (1, 0, [runs["sub-c"]], [], 0)
    # Each stopped when its own run did, well before the stuck one's wait ran out.
    assert b["surfaces"]["running_work"]["stopped_after_ms"] < 1000 and c["surfaces"]["running_work"]["stopped_after_ms"] < 1000
    totals = document["totals"]
    assert (totals["runs_found"], totals["runs_cancelled"], totals["runs_finished_first"], totals["runs_unconfirmed"]) == (3, 1, [runs["sub-c"]], [stuck_run])
    assert totals["subjects_unconfirmed"] == ["sub-a"]


@pytest.mark.anyio
async def test_a_run_admitted_during_the_wait_is_found_for_an_identity_that_had_none_before(stores) -> None:
    """The others' wait is this identity's too: a request that authenticated before its refusal can insert a run meanwhile."""
    first = await stores.users.create_user(_account("sub-a"))
    second = await stores.users.create_user(_account("sub-b"))
    await _seed_run(stores, str(first.id))
    command = _command(stores, wait_seconds=0.3)
    cancel_and_wait = command._cancel_and_wait
    late: list[str] = []

    async def _meanwhile(owners: dict[str, str], **kwargs) -> dict[str, str]:
        stopped = await cancel_and_wait(owners, **kwargs)
        if not late:
            late.append(await _seed_run(stores, str(second.id)))
        return stopped

    command._cancel_and_wait = _meanwhile
    document = await command.run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b"])

    assert _entry(document, "sub-b")["runs_found"] == 1 and _entry(document, "sub-b")["runs_unconfirmed"] == late
    assert (await stores.runs.get(late[0], user_id=None))["cancel_action"] == "interrupt"


@pytest.mark.anyio
async def test_one_person_s_run_that_outlasts_the_wait_leaves_everyone_else_confirmed(stores) -> None:
    """The Gateway processes' second look is left time inside the one wait, so a slow run is its own identity's only."""
    stuck = await stores.users.create_user(_account("sub-a"))
    for subject in ("sub-b", "sub-c"):
        await stores.users.create_user(_account(subject))
    await _seed_run(stores, str(stuck.id))
    watch = RefusalWatch(stores.sweeps, OwnerHoldings(), refused_owners=stores.users.list_refused_user_ids, process_id="gw-1", interval_seconds=0.05)
    wait = 2.0
    await watch.start()
    try:
        started = time.monotonic()
        document = await _command(stores, sweeps=stores.sweeps, wait_seconds=wait).run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b", "sub-c"])
        took = time.monotonic() - started
    finally:
        await watch.stop()

    assert took < wait + 1.0, took
    for subject in ("sub-b", "sub-c"):
        entry = _entry(document, subject)
        assert entry["surfaces_unconfirmed"] == [] and entry["returncode"] == 0, (subject, entry["surfaces_unconfirmed"])
    assert _entry(document, "sub-a")["surfaces_unconfirmed"] == ["running_work"]
    assert document["totals"]["subjects_unconfirmed"] == ["sub-a"] and document["returncode"] == EXIT_UNCONFIRMED_RUNS


@pytest.mark.anyio
async def test_a_fault_listing_one_identity_s_runs_is_that_identity_s_alone(stores, monkeypatch: pytest.MonkeyPatch) -> None:
    broken = await stores.users.create_user(_account("sub-a"))
    fine = await stores.users.create_user(_account("sub-b"))
    run_id = await _seed_run(stores, str(fine.id))
    list_active_by_user = stores.runs.list_active_by_user

    async def _list(user_id: str, *args, **kwargs):
        if user_id == str(broken.id):
            raise RuntimeError("run store unreachable")
        return await list_active_by_user(user_id, *args, **kwargs)

    monkeypatch.setattr(stores.runs, "list_active_by_user", _list)

    document = await _command(stores, wait_seconds=0.2).run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b"])

    assert _entry(document, "sub-a")["verdict"] == "failed" and "run store unreachable" in _entry(document, "sub-a")["error"]
    assert _entry(document, "sub-b")["verdict"] == "disabled" and _entry(document, "sub-b")["runs_found"] == 1
    assert (await stores.runs.get(run_id, user_id=None))["cancel_action"] == "interrupt"
    assert document["totals"]["subjects_failed"] == ["sub-a"] and document["returncode"] == 1


@pytest.mark.anyio
async def test_a_fault_in_a_shared_wait_fails_every_identity_it_left_unfinished_and_still_answers_for_each(stores, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every refusal has committed by then, so the document must still say who was reached; the one-subject form raises as before."""
    for subject in ("sub-a", "sub-b"):
        await stores.users.create_user(_account(subject))
    await stores.users.create_user(_account("sub-c"))
    await stores.users.create_user(_account("sub-c", provider="sso-basic", email="sub-c.basic@example.com"))

    async def _unreadable(**kwargs):
        raise RuntimeError("process record unreadable")

    monkeypatch.setattr(stores.sweeps, "live_processes", _unreadable)

    document = await _command(stores, sweeps=stores.sweeps).run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b", "sub-c"])

    for subject in ("sub-a", "sub-b"):
        entry = _entry(document, subject)
        assert entry["verdict"] == "failed" and "process record unreadable" in entry["error"], entry
    assert _entry(document, "sub-c")["verdict"] == "refused"
    assert document["totals"]["subjects_failed"] == ["sub-a", "sub-b"] and document["totals"]["subjects_refused"] == ["sub-c"]
    # The refusals committed before the fault; a re-run finishes what it left.
    assert {row[1] for row in await stores.users.list_disabled_identities()} == {"sub-a", "sub-b"}
    with pytest.raises(RuntimeError, match="process record unreadable"):
        await _command(stores, sweeps=stores.sweeps).run("disable", issuer=ISSUER, subject="sub-a")


@pytest.mark.anyio
async def test_when_every_identity_failed_the_gateway_processes_are_not_asked_to_look(stores, monkeypatch: pytest.MonkeyPatch) -> None:
    for subject in ("sub-a", "sub-b"):
        await stores.users.create_user(_account(subject))
    asked: list[int] = []
    request_check = stores.sweeps.request_check

    async def _counted() -> int:
        asked.append(await request_check())
        return asked[-1]

    async def _refusal_fails(*args):
        raise RuntimeError("database unreachable")

    stores.sweeps.request_check = _counted
    monkeypatch.setattr(stores.users, "disable_identity", _refusal_fails)

    document = await _command(stores, sweeps=stores.sweeps).run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b"])

    assert document["totals"]["verdicts"] == {"failed": 2} and asked == []


@pytest.mark.anyio
async def test_a_subject_named_twice_is_one_identity(stores) -> None:
    await stores.users.create_user(_account("sub-a"))

    document = await _command(stores).run("disable", issuer=ISSUER, subjects=["sub-a", " sub-a ", "sub-a"])

    assert [entry["identity"]["subject"] for entry in document["identities"]] == ["sub-a"]
    assert document["totals"]["identities"] == 1


@pytest.mark.anyio
async def test_an_empty_subject_is_refused_on_its_own(stores) -> None:
    await stores.users.create_user(_account("sub-a"))

    document = await _command(stores).run("disable", issuer=ISSUER, subjects=["sub-a", "  "])

    assert _entry(document, "sub-a")["verdict"] == "disabled"
    assert _entry(document, "")["returncode"] == 1 and document["returncode"] == 1


@pytest.mark.anyio
async def test_a_failure_while_disabling_one_identity_does_not_stop_the_others(stores, monkeypatch: pytest.MonkeyPatch) -> None:
    broken = await stores.users.create_user(_account("sub-a"))
    fine = await stores.users.create_user(_account("sub-b"))
    end_sessions = stores.users.end_sessions

    async def _end_sessions(user_id: str) -> bool:
        if user_id == str(broken.id):
            raise RuntimeError("database unreachable")
        return await end_sessions(user_id)

    monkeypatch.setattr(stores.users, "end_sessions", _end_sessions)

    document = await _command(stores).run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b"])

    failed = _entry(document, "sub-a")
    assert failed["verdict"] == "failed" and failed["error"] == "RuntimeError: database unreachable" and failed["returncode"] == 1
    assert _entry(document, "sub-b")["verdict"] == "disabled"
    assert (await stores.users.get_user_by_id(str(fine.id))).token_version == fine.token_version + 1
    # Failed is not refused: a refusal comes back the same every time, a failure may be part-done and is re-run.
    assert document["returncode"] == 1 and document["totals"]["subjects_failed"] == ["sub-a"] and document["totals"]["subjects_refused"] == []


@pytest.mark.anyio
async def test_too_many_subjects_are_refused_before_anything_changes(stores) -> None:
    await stores.users.create_user(_account("sub-0"))

    with pytest.raises(CommandError, match=str(MAX_BATCH_SUBJECTS)):
        await _command(stores).run("disable", issuer=ISSUER, subjects=[f"sub-{index}" for index in range(MAX_BATCH_SUBJECTS + 1)])

    assert await stores.users.list_disabled_identities() == []


@pytest.mark.anyio
async def test_exactly_the_cap_is_taken(stores) -> None:
    document = await _command(stores, wait_seconds=0).run("lift-role-limit", issuer=ISSUER, subjects=[f"sub-{index}" for index in range(MAX_BATCH_SUBJECTS)])

    assert document["totals"]["identities"] == MAX_BATCH_SUBJECTS


@pytest.mark.anyio
async def test_the_command_itself_refuses_subjects_for_another_verb_or_without_an_issuer(stores) -> None:
    """Not only its command line: a caller that builds the call itself gets the same refusals, before anything changes."""
    await stores.users.create_user(_account("sub-a"))

    with pytest.raises(CommandError, match="--subjects belongs to"):
        await _command(stores).run("end-sessions", issuer=ISSUER, subjects=["sub-a"])
    with pytest.raises(CommandError, match="one --issuer"):
        await _command(stores).run("disable", issuer=None, subjects=["sub-a"])

    assert await stores.users.list_disabled_identities() == []


# ── enable ──────────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_enable_takes_many_identities_and_restores_what_each_disable_held(stores) -> None:
    first = await stores.users.create_user(_account("sub-a"))
    second = await stores.users.create_user(_account("sub-b"))
    schedules = {"sub-a": await _schedule(stores, str(first.id)), "sub-b": await _schedule(stores, str(second.id))}
    command = _command(stores)
    await command.run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b"])

    document = await command.run("enable", issuer=ISSUER, subjects=["sub-a", "sub-b", "sub-none"], restore_held=True)

    for subject, task_id in schedules.items():
        entry = _entry(document, subject)
        assert entry["verdict"] == "enabled" and entry["restored"]["schedules"] == [task_id]
        assert (await stores.schedules.get_internal(task_id))["status"] == "enabled"
    assert _entry(document, "sub-none")["verdict"] == "already_enabled"
    assert document["totals"]["verdicts"] == {"enabled": 2, "already_enabled": 1}
    assert document["returncode"] == 0
    assert await stores.users.list_refused_user_ids() == set()


@pytest.mark.anyio
async def test_enable_refuses_a_subject_that_names_two_accounts_and_isolates_a_fault(stores, monkeypatch: pytest.MonkeyPatch) -> None:
    """As its one-subject form: it will not guess between two accounts, and one identity's fault is its own."""
    for subject in ("sub-a", "sub-b", "sub-c"):
        await stores.users.create_user(_account(subject))
    await stores.users.create_user(_account("sub-c", provider="sso-basic", email="sub-c.basic@example.com"))
    await _command(stores).run("disable", issuer=ISSUER, subjects=["sub-a", "sub-b"])
    enable_identity = stores.users.enable_identity

    async def _enable_identity(issuer: str, subject: str, *args, **kwargs):
        if subject == "sub-a":
            raise RuntimeError("database unreachable")
        return await enable_identity(issuer, subject, *args, **kwargs)

    monkeypatch.setattr(stores.users, "enable_identity", _enable_identity)

    document = await _command(stores).run("enable", issuer=ISSUER, subjects=["sub-a", "sub-b", "sub-c"])

    assert _entry(document, "sub-a")["verdict"] == "failed" and _entry(document, "sub-b")["verdict"] == "enabled" and _entry(document, "sub-c")["verdict"] == "refused"
    assert (document["totals"]["subjects_failed"], document["totals"]["subjects_refused"], document["returncode"]) == (["sub-a"], ["sub-c"], 1)


# ── the role limit ──────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_limit_role_takes_many_identities_and_waits_once_for_the_runs_above_it(stores) -> None:
    first = await stores.users.create_user(_account("sub-a", role="admin"))
    second = await stores.users.create_user(_account("sub-b", role="admin"))
    runs = [await _seed_run(stores, str(account.id), role="admin") for account in (first, second)]
    wait = 1.0

    started = time.monotonic()
    document = await _command(stores, wait_seconds=wait).run("limit-role", issuer=ISSUER, subjects=["sub-a", "sub-b", "sub-new"], end_running_work=True)
    took = time.monotonic() - started

    assert took < wait + 3.0, took
    for account in (first, second):
        assert (await stores.users.get_user_by_id(str(account.id))).system_role == "user"
    assert _entry(document, "sub-new")["accounts"] == [] and _entry(document, "sub-new")["verdict"] == "limited"
    assert document["totals"]["runs_unconfirmed"] == sorted(runs)
    assert document["returncode"] == EXIT_UNCONFIRMED_RUNS
    lifted = await _command(stores).run("lift-role-limit", issuer=ISSUER, subjects=["sub-a", "sub-b", "sub-new", "sub-never"])
    assert lifted["totals"]["verdicts"] == {"lifted": 3, "no_limit": 1} and lifted["returncode"] == 0


@pytest.mark.anyio
async def test_each_limit_role_entry_carries_its_own_runs_and_exit_status(stores) -> None:
    first = await stores.users.create_user(_account("sub-a", role="admin"))
    await stores.users.create_user(_account("sub-b", role="admin"))
    run_id = await _seed_run(stores, str(first.id), role="admin")

    document = await _command(stores, wait_seconds=0.2).run("limit-role", issuer=ISSUER, subjects=["sub-a", "sub-b"], end_running_work=True)

    a, b = _entry(document, "sub-a"), _entry(document, "sub-b")
    assert (a["runs_unconfirmed"], a["surfaces_unconfirmed"], a["returncode"]) == ([run_id], ["running_work"], EXIT_UNCONFIRMED_RUNS)
    assert (b["runs_unconfirmed"], b["surfaces_unconfirmed"], b["returncode"]) == ([], [], 0)
    assert a["surfaces"]["sessions"]["count"] == 1 and b["surfaces"]["sessions"]["count"] == 1


@pytest.mark.anyio
async def test_a_failure_lifting_one_limit_does_not_stop_the_others(stores, monkeypatch: pytest.MonkeyPatch) -> None:
    await _command(stores).run("limit-role", issuer=ISSUER, subjects=["sub-a", "sub-b"])
    lift_role_limit = stores.users.lift_role_limit

    async def _lift(issuer: str, subject: str, *args, **kwargs):
        if subject == "sub-a":
            raise RuntimeError("database unreachable")
        return await lift_role_limit(issuer, subject, *args, **kwargs)

    monkeypatch.setattr(stores.users, "lift_role_limit", _lift)

    document = await _command(stores).run("lift-role-limit", issuer=ISSUER, subjects=["sub-a", "sub-b"])

    assert _entry(document, "sub-a")["verdict"] == "failed" and _entry(document, "sub-b")["verdict"] == "lifted"
    assert document["totals"]["subjects_failed"] == ["sub-a"] and document["returncode"] == 1


@pytest.mark.anyio
async def test_a_fault_listing_one_identity_s_runs_above_the_limit_is_that_identity_s_alone(stores, monkeypatch: pytest.MonkeyPatch) -> None:
    broken = await stores.users.create_user(_account("sub-a", role="admin"))
    fine = await stores.users.create_user(_account("sub-b", role="admin"))
    run_id = await _seed_run(stores, str(fine.id), role="admin")
    list_active_by_user = stores.runs.list_active_by_user

    async def _list(user_id: str, *args, **kwargs):
        if user_id == str(broken.id):
            raise RuntimeError("run store unreachable")
        return await list_active_by_user(user_id, *args, **kwargs)

    monkeypatch.setattr(stores.runs, "list_active_by_user", _list)

    document = await _command(stores, wait_seconds=0.2).run("limit-role", issuer=ISSUER, subjects=["sub-a", "sub-b"], end_running_work=True)

    assert _entry(document, "sub-a")["verdict"] == "failed"
    assert _entry(document, "sub-b")["verdict"] == "limited" and _entry(document, "sub-b")["runs_unconfirmed"] == [run_id]
    assert document["totals"]["subjects_failed"] == ["sub-a"] and document["returncode"] == 1


@pytest.mark.anyio
async def test_the_last_administrator_is_refused_and_the_others_are_still_limited(stores) -> None:
    """With local passwords on, no limit may leave the deployment without an administrator."""
    first = await stores.users.create_user(_account("sub-a", role="admin"))
    second = await stores.users.create_user(_account("sub-b", role="admin"))

    document = await _command(stores, setup_opens_without_admin=True).run("limit-role", issuer=ISSUER, subjects=["sub-a", "sub-b"])

    assert _entry(document, "sub-a")["verdict"] == "limited"
    assert "no administrator" in _entry(document, "sub-b")["error"] and _entry(document, "sub-b")["verdict"] == "refused" and _entry(document, "sub-b")["returncode"] == 1
    assert (await stores.users.get_user_by_id(str(first.id))).system_role == "user"
    assert (await stores.users.get_user_by_id(str(second.id))).system_role == "admin"
    assert document["returncode"] == 1


# ── The command line ────────────────────────────────────────────────────


def _recording(monkeypatch: pytest.MonkeyPatch) -> dict:
    from app.gateway.auth import accounts

    seen: dict = {}

    async def _record(command: str, **kwargs: object) -> dict:
        seen.update(kwargs, command=command)
        return {"command": command, "returncode": 0}

    monkeypatch.setattr(accounts, "_run", _record)
    return seen


def test_main_takes_subjects_for_the_four_verbs_one_word_each(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from app.gateway.auth import accounts

    seen = _recording(monkeypatch)
    for command in ("disable", "enable", "limit-role", "lift-role-limit"):
        assert accounts.main([command, "--issuer", ISSUER, "--subjects", "sub-a", "sub-b", "--subjects=-starts-with-a-dash"]) == 0
        assert seen["command"] == command and seen["subjects"] == ["sub-a", "sub-b", "-starts-with-a-dash"] and seen["subject"] is None
    capsys.readouterr()
    assert accounts.main(["disable", "--issuer", ISSUER, "--subject", "sub-a"]) == 0
    assert seen["subjects"] is None


def test_the_command_line_reaches_the_deployment_s_database_with_every_subject(tmp_path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    """End to end through ``main``, nothing replaced but the configuration: the subjects arrive, one entry each."""
    from app.gateway.auth import accounts
    from deerflow.config.app_config import AppConfig
    from deerflow.config.database_config import DatabaseConfig
    from deerflow.config.sandbox_config import SandboxConfig

    config = AppConfig(sandbox=SandboxConfig(use="deerflow.sandbox.local:LocalSandboxProvider"), database=DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path)))
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: config)
    monkeypatch.setattr("deerflow.config.app_config.get_app_config", lambda: config)

    assert accounts.main(["limit-role", "--issuer", ISSUER, "--subjects", "sub-a", "sub-b", "--wait-seconds", "0"]) == 0
    document = json.loads(capsys.readouterr().out)

    assert [entry["identity"]["subject"] for entry in document["identities"]] == ["sub-a", "sub-b"]
    assert document["totals"]["verdicts"] == {"limited": 2}


def test_a_refused_command_line_names_its_command_never_a_subject(capsys: pytest.CaptureFixture[str]) -> None:
    from app.gateway.auth import accounts

    assert accounts.main(["--subjects", "disable", "list", "--issuer", ISSUER, "enable", "--force"]) == 1
    assert json.loads(capsys.readouterr().out)["command"] == "enable"


@pytest.mark.parametrize(
    ("argv", "names"),
    [
        (["end-sessions", "--issuer", ISSUER, "--subjects", "sub-a"], "--subjects belongs to disable, enable, limit-role and lift-role-limit"),
        (["release-email", "--issuer", ISSUER, "--subjects", "sub-a"], "--subjects belongs to"),
        (["disable", "--issuer", ISSUER, "--subject", "sub-a", "--subjects", "sub-b"], "--subjects or --subject, not both"),
        (["disable", "--email", "who@example.com", "--subjects", "sub-b"], "--subjects addresses identities at one --issuer"),
        (["disable", "--subjects", "sub-b"], "--subjects addresses identities at one --issuer"),
        (["disable", "--issuer", ISSUER, "--email", "who@example.com", "--subjects", "sub-b"], "and not --email"),
    ],
)
def test_main_refuses_subjects_where_they_do_not_belong(argv: list[str], names: str, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    from app.gateway.auth import accounts

    seen = _recording(monkeypatch)
    assert accounts.main(argv) == 1
    document = json.loads(capsys.readouterr().out)
    assert names in document["error"] and document["command"] == argv[0]
    assert seen == {}, "refused before anything ran"
