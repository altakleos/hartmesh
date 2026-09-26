"""`disable` reaches every surface, for every account the identity covers, and says when each stopped.

A surface refused at its next use reports the refusal's commit; the
connections a Gateway process holds -- WebSockets, SSE streams, streaming
downloads -- report when every live process confirmed it had looked, with
what it ended. A process that does not confirm within ``--wait-seconds``
leaves those surfaces unconfirmed, and the exit status is 2; a process that
stopped beating holds nothing and is not waited on. What a process keeps for
the person between requests -- sandboxes, pooled MCP sessions, browsers,
queued memory updates -- is confirmed by a second check once the runs are
over, since a run that is ending leaves its sandbox parked; a run reports
when its sandboxes were confirmed stopped.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-disable-surfaces-min-32-chars")

from app.gateway.auth.accounts import EXIT_UNCONFIRMED_RUNS, AccountsCommand
from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.gateway.refusal_watch import RefusalWatch
from deerflow.runtime.owner_holdings import Ended, OwnerHoldings

ISSUER = "https://login.example.com/realms/tenant"
CONNECTIONS = ("websockets", "sse_streams", "downloads")
RETAINED = ("sandboxes", "mcp_sessions", "browser_sessions", "memory_updates")
PROCESS_SURFACES = (*CONNECTIONS, *RETAINED)
DURABLE = ("mcp_tasks", "subagent_batches", "mcp_task_notifications", "channel_ingress")


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

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/disable.db", sqlite_dir=str(tmp_path)))
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


def _account(email: str = "pat@example.com", subject: str = "sub-pat", *, provider: str = "sso") -> User:
    return User(email=email, password_hash=None, system_role="user", oauth_provider=provider, oauth_id=subject, oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC))


def _command(stores: SimpleNamespace, **kwargs) -> AccountsCommand:
    kwargs.setdefault("wait_seconds", 5)
    return AccountsCommand(stores.users, tokens=stores.tokens, schedules=stores.schedules, runs=stores.runs, sweeps=stores.sweeps, **kwargs)


async def _seed_run(stores: SimpleNamespace, user_id: str) -> str:
    run_id = str(uuid4())
    await stores.runs.put(run_id, thread_id=str(uuid4()), user_id=user_id, status="running", created_at=datetime.now(UTC).isoformat())
    return run_id


def _watch(stores: SimpleNamespace, holdings: OwnerHoldings, process_id: str = "gw-1") -> RefusalWatch:
    return RefusalWatch(stores.sweeps, holdings, refused_owners=stores.users.list_refused_user_ids, process_id=process_id, interval_seconds=0.05)


@pytest.mark.anyio
async def test_the_document_names_every_surface_with_a_time_and_the_refusal_s_commit_for_those_refused_at_next_use(stores) -> None:
    account = await stores.users.create_user(_account())
    await stores.tokens.create(user_id=str(account.id), name="automation", scopes=["threads:read"], token_digest="digest-1")
    before = datetime.now(UTC)

    document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")

    started = datetime.fromisoformat(document["started_at"])
    assert before - timedelta(seconds=1) <= started <= datetime.now(UTC)
    assert set(document["surfaces"]) == {"sign_in", "sessions", "personal_access_tokens", "internal_launches", "running_work", *PROCESS_SURFACES, *DURABLE}
    committed = document["surfaces"]["sign_in"]["stopped_after_ms"]
    for name in ("sign_in", "sessions", "personal_access_tokens", "internal_launches"):
        entry = document["surfaces"][name]
        assert entry["stopped_after_ms"] == committed, name
        assert datetime.fromisoformat(entry["stopped_at"]) == started + timedelta(milliseconds=committed), name
    assert document["surfaces"]["sign_in"]["action"] == "refused_at_next_use"
    assert document["surfaces"]["personal_access_tokens"] == {**document["surfaces"]["personal_access_tokens"], "action": "revoked", "count": 1}
    assert document["surfaces"]["internal_launches"]["action"] == "refused_at_next_use"
    # No Gateway process is running: nothing holds a connection or keeps a
    # session for them, and that is confirmed, not assumed. A sandbox's
    # container outlives its Gateway, so nothing is there to vouch for it.
    for name in PROCESS_SURFACES:
        entry = document["surfaces"][name]
        assert entry["count"] == 0 and entry["processes"] == 0 and entry["confirmed_by"] == "gateway_record", name
        assert (entry["stopped_after_ms"] is None) == (name == "sandboxes"), name
    assert document["surfaces_unconfirmed"] == ["sandboxes"] and document["returncode"] == EXIT_UNCONFIRMED_RUNS
    assert document["surfaces_not_reached"] == [] and "no Gateway process is running" in document["note"]
    assert document["tokens_revoked"] == 1 and document["sessions_ended"] is True, "the existing keys keep their meanings"


@pytest.mark.anyio
async def test_sessions_and_tokens_end_on_every_account_the_identity_covers(stores) -> None:
    named = await stores.users.create_user(_account())
    sibling = await stores.users.create_user(_account("pat.chen@example.com", provider="sso-basic"))
    for account in (named, sibling):
        await stores.tokens.create(user_id=str(account.id), name="automation", scopes=["threads:read"], token_digest=f"digest-{account.id}")

    document = await _command(stores).run("disable", email="pat@example.com")

    for account in (named, sibling):
        assert (await stores.users.get_user_by_id(str(account.id))).token_version == account.token_version + 1
        assert all(record["revoked_at"] is not None for record in await stores.tokens.list_for_user(str(account.id)))
    assert document["tokens_revoked"] == 2 and document["surfaces"]["sessions"]["count"] == 2
    assert [entry["email"] for entry in document["identity_also_covers"]] == ["pat.chen@example.com"]


@pytest.mark.anyio
async def test_every_live_process_confirms_what_it_ended_and_the_document_times_it(stores) -> None:
    account = await stores.users.create_user(_account())
    sibling = await stores.users.create_user(_account("pat.chen@example.com", provider="sso-basic"))
    first, second = OwnerHoldings(), OwnerHoldings()
    first.hold(str(account.id), "sse_streams", lambda: None)
    first.hold(str(account.id), "websockets", lambda: None)
    second.hold(str(sibling.id), "downloads", lambda: None)
    second.hold("someone-else", "sse_streams", lambda: None)
    watches = [_watch(stores, first, "gw-1"), _watch(stores, second, "gw-2")]
    for watch in watches:
        await watch.start()
    try:
        document = await _command(stores).run("disable", email="pat@example.com")
    finally:
        for watch in watches:
            await watch.stop()

    surfaces = document["surfaces"]
    assert (surfaces["sse_streams"]["count"], surfaces["websockets"]["count"], surfaces["downloads"]["count"]) == (1, 1, 1)
    for name in CONNECTIONS:
        entry = surfaces[name]
        assert entry["action"] == "ended" and entry["processes"] == 2 and entry["processes_unconfirmed"] == [] and entry["confirmed_by"] == "gateway_record", name
        assert surfaces["sign_in"]["stopped_after_ms"] <= entry["stopped_after_ms"] <= document["elapsed_ms"], name
    assert second.owners() == {"someone-else"}
    assert document["surfaces_unconfirmed"] == [] and document["returncode"] == 0


@pytest.mark.anyio
async def test_a_live_process_that_does_not_confirm_leaves_the_connections_unconfirmed_and_exits_2(stores) -> None:
    await stores.users.create_user(_account())
    await stores.sweeps.register("gw-stalled")  # beats, but never acts on a check

    document = await _command(stores, wait_seconds=0.3).run("disable", issuer=ISSUER, subject="sub-pat")

    # The batch items such a process may be executing are not confirmed stopped either.
    assert document["surfaces_unconfirmed"] == sorted([*PROCESS_SURFACES, "subagent_batches"])
    for name in PROCESS_SURFACES:
        entry = document["surfaces"][name]
        assert entry["stopped_after_ms"] is None and entry["processes_unconfirmed"] == ["gw-stalled"] and entry["processes"] == 0, name
    assert document["returncode"] == EXIT_UNCONFIRMED_RUNS
    assert document["runs_unconfirmed"] == [], "runs_unconfirmed keeps listing only runs"
    assert "surfaces_unconfirmed" in document["note"]


@pytest.mark.anyio
async def test_a_process_that_stopped_beating_holds_nothing_and_is_not_waited_on(stores) -> None:
    await stores.users.create_user(_account())
    await stores.sweeps.register("gw-gone")
    await asyncio.sleep(1.2)

    document = await _command(stores, live_window_seconds=1).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["surfaces"]["sse_streams"]["processes"] == 0 and document["surfaces"]["sse_streams"]["stopped_after_ms"] is not None
    assert document["surfaces_unconfirmed"] == ["sandboxes"], "its connections went with it; a container it started may not have"


@pytest.mark.anyio
async def test_a_run_admitted_after_the_command_listed_the_runs_is_cancelled_too(stores) -> None:
    """A request that authenticated just before the refusal committed can insert its run after the command looked."""
    account = await stores.users.create_user(_account())
    early = await _seed_run(stores, str(account.id))
    late: list[str] = []
    request_cancel = stores.runs.request_cancel_compat

    async def _admitted_meanwhile(run_id: str, **kwargs):
        if not late:
            late.append(await _seed_run(stores, str(account.id)))
        return await request_cancel(run_id, **kwargs)

    stores.runs.request_cancel_compat = _admitted_meanwhile
    document = await _command(stores, wait_seconds=0).run("disable", issuer=ISSUER, subject="sub-pat")

    assert late and (await stores.runs.get(late[0], user_id=None))["cancel_action"] == "interrupt"
    assert document["runs_found"] == 2 and document["runs_unconfirmed"] == sorted([early, *late])
    assert document["surfaces"]["running_work"]["count"] == 2


@pytest.mark.anyio
async def test_a_re_run_re_checks_and_re_reports(stores) -> None:
    account = await stores.users.create_user(_account())
    holdings = OwnerHoldings()
    watch = _watch(stores, holdings)
    await watch.start()
    holdings.hold(str(account.id), "sse_streams", lambda: None)
    try:
        first = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")
        # A stream that got in before the refusal was read everywhere, found by the re-run's check.
        holdings.set_refused(set())
        holdings.hold(str(account.id), "sse_streams", lambda: None)
        again = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")
    finally:
        await watch.stop()

    assert first["verdict"] == "disabled" and again["verdict"] == "already_disabled"
    assert first["surfaces"]["sse_streams"]["count"] == 1
    assert again["surfaces"]["sse_streams"]["count"] == 1 and again["returncode"] == 0, "what this check closed, not the first one's too"


@pytest.mark.anyio
async def test_one_wait_bounds_the_whole_command_and_the_connections_are_confirmed_alongside_the_runs(stores) -> None:
    """A run that never stops and a process that never confirms cost one --wait-seconds, not one each."""
    account = await stores.users.create_user(_account())
    await _seed_run(stores, str(account.id))
    await stores.sweeps.register("gw-stalled")

    started = asyncio.get_running_loop().time()
    document = await _command(stores, wait_seconds=1.5).run("disable", issuer=ISSUER, subject="sub-pat")
    took = asyncio.get_running_loop().time() - started

    assert took < 1.5 + 1.0, f"took {took:.1f}s for a 1.5s wait"
    assert sorted(document["surfaces_unconfirmed"]) == sorted(["running_work", *PROCESS_SURFACES, "subagent_batches"]) and document["returncode"] == EXIT_UNCONFIRMED_RUNS


@pytest.mark.anyio
async def test_enable_asks_every_process_to_look_again_so_the_person_is_not_cut_for_the_periodic_look(stores) -> None:
    account = await stores.users.create_user(_account())
    holdings = OwnerHoldings()
    watch = _watch(stores, holdings)
    watch._full_look_every = 3600
    await watch.start()
    try:
        await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")
        ended: list[str] = []
        holdings.hold(str(account.id), "sse_streams", lambda: ended.append("cut"))
        assert ended == ["cut"], "refused: a stream it opens is cut"

        before = await stores.sweeps.latest_check()
        await _command(stores).run("enable", issuer=ISSUER, subject="sub-pat")
        assert await stores.sweeps.latest_check() > before
        deadline = asyncio.get_running_loop().time() + 5
        while holdings.owners() == set() and asyncio.get_running_loop().time() < deadline:
            holdings.hold(str(account.id), "sse_streams", lambda: None)
            await asyncio.sleep(0.1)
        assert holdings.owners() == {str(account.id)}, "enabled again: what the person opens is held, not cut"
    finally:
        await watch.stop()


@pytest.mark.anyio
async def test_an_identity_with_no_account_names_every_surface_all_the_same(stores) -> None:
    document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-nobody")

    assert document["account"] is None
    assert set(document["surfaces"]) == {"sign_in", "sessions", "personal_access_tokens", "internal_launches", "running_work", *PROCESS_SURFACES, *DURABLE}
    assert all(entry["count"] == 0 for entry in document["surfaces"].values())
    assert document["surfaces_unconfirmed"] == [] and document["returncode"] == 0


@pytest.mark.anyio
async def test_the_check_is_asked_for_only_after_the_refusal_has_committed(stores) -> None:
    """A process that acts on the check must be able to read the refusal, or it would confirm closing nothing."""
    await stores.users.create_user(_account())
    order: list[str] = []
    disable_identity, request_check = stores.users.disable_identity, stores.sweeps.request_check

    async def _disable(*args, **kwargs):
        result = await disable_identity(*args, **kwargs)
        order.append("refusal committed")
        return result

    async def _check(*args, **kwargs):
        order.append("check requested")
        return await request_check(*args, **kwargs)

    stores.users.disable_identity = _disable
    stores.sweeps.request_check = _check
    await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")
    assert order[:2] == ["refusal committed", "check requested"]


@pytest.mark.anyio
async def test_what_a_run_leaves_behind_is_confirmed_once_the_run_is_over_and_the_run_reports_when_its_sandbox_stopped(stores) -> None:
    """A cancelled run parks its sandbox as it ends -- after the first look -- with whatever it left running inside."""
    account = await stores.users.create_user(_account())
    run_id = await _seed_run(stores, str(account.id))
    parked: list[str] = []
    request_cancel = stores.runs.request_cancel_compat

    async def _run_ends_and_parks_its_sandbox(run_id: str, **kwargs):
        result = await request_cancel(run_id, **kwargs)

        async def _later() -> None:
            await asyncio.sleep(0.4)
            parked.append(str(account.id))
            await stores.runs.update_status(run_id, "interrupted")

        asyncio.get_running_loop().create_task(_later())
        return result

    stores.runs.request_cancel_compat = _run_ends_and_parks_its_sandbox

    def _stop_parked(owners: frozenset[str]) -> dict[str, Ended]:
        stopped = [owner for owner in parked if owner in owners]
        for owner in stopped:
            parked.remove(owner)
        return {owner: Ended(1) for owner in stopped}

    holdings = OwnerHoldings()
    holdings.add_source("sandboxes", _stop_parked)
    watch = _watch(stores, holdings)
    await watch.start()
    try:
        document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")
    finally:
        await watch.stop()

    surfaces = document["surfaces"]
    assert document["runs_cancelled"] == 1 and document["runs_unconfirmed"] == [] and run_id
    assert surfaces["sandboxes"]["count"] == 1 and surfaces["sandboxes"]["action"] == "ended" and surfaces["sandboxes"]["not_ended"] == 0
    assert surfaces["running_work"]["confirmed_by"] == "sandbox_gone"
    assert surfaces["running_work"]["stopped_after_ms"] == surfaces["sandboxes"]["stopped_after_ms"]
    assert document["surfaces_unconfirmed"] == [] and document["returncode"] == 0


@pytest.mark.anyio
async def test_a_sandbox_that_would_not_stop_is_unconfirmed_and_the_run_reports_only_its_row(stores) -> None:
    account = await stores.users.create_user(_account())
    holdings = OwnerHoldings()
    holdings.add_source("sandboxes", lambda owners: {str(account.id): Ended(0, failed=1)} if str(account.id) in owners else {})
    watch = _watch(stores, holdings)
    await watch.start()
    try:
        document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")
    finally:
        await watch.stop()

    entry = document["surfaces"]["sandboxes"]
    assert entry["stopped_after_ms"] is None and entry["not_ended"] == 1 and entry["processes_unconfirmed"] == []
    assert document["surfaces_unconfirmed"] == ["sandboxes"] and document["returncode"] == EXIT_UNCONFIRMED_RUNS
    assert document["surfaces"]["running_work"]["confirmed_by"] == "run_status"
    assert "sandboxes" in document["note"]


@pytest.mark.anyio
async def test_a_process_whose_sandboxes_cannot_be_ended_makes_them_not_reached_never_none(stores) -> None:
    """A sandbox provider without the capability says so when it registers; its silence is not read as nothing held."""
    await stores.users.create_user(_account())
    watch = RefusalWatch(stores.sweeps, OwnerHoldings(), refused_owners=stores.users.list_refused_user_ids, process_id="gw-local", interval_seconds=0.05, unreached=("sandboxes",))
    await watch.start()
    try:
        document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")
    finally:
        await watch.stop()

    entry = document["surfaces"]["sandboxes"]
    assert entry["action"] == "not_reached" and entry["processes_unreached"] == ["gw-local"] and entry["stopped_after_ms"] is None
    assert document["surfaces_unconfirmed"] == ["sandboxes"] and document["returncode"] == EXIT_UNCONFIRMED_RUNS
    assert document["surfaces_not_reached"] == ["sandboxes"], "a retry loop can tell this 2 apart: re-running will not change it"
    assert document["surfaces"]["mcp_sessions"]["stopped_after_ms"] is not None, "the other surfaces are reached as ever"
    assert "not_reached" in document["note"]


@pytest.mark.anyio
async def test_an_identity_with_no_account_is_not_reported_unreached_by_a_process_that_cannot_end_sandboxes(stores) -> None:
    """Nothing can be kept for an account that does not exist, reachable or not."""
    watch = RefusalWatch(stores.sweeps, OwnerHoldings(), refused_owners=stores.users.list_refused_user_ids, process_id="gw-local", interval_seconds=0.05, unreached=("sandboxes",))
    await watch.start()
    try:
        document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-nobody")
    finally:
        await watch.stop()

    assert document["surfaces"]["sandboxes"]["action"] == "ended" and document["surfaces"]["sandboxes"]["processes_unreached"] == []
    assert document["surfaces_unconfirmed"] == [] and document["surfaces_not_reached"] == [] and document["returncode"] == 0


@pytest.mark.anyio
async def test_a_sandbox_a_process_cannot_attribute_is_not_confirmed_ended_for_the_person(stores) -> None:
    """Taken over after a restart without learning whose: it may be theirs."""
    await stores.users.create_user(_account())
    holdings = OwnerHoldings()
    holdings.add_source("sandboxes", lambda owners: {"*": Ended(0, failed=1)} if owners else {})
    watch = _watch(stores, holdings)
    await watch.start()
    try:
        document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")
    finally:
        await watch.stop()

    assert document["surfaces"]["sandboxes"]["not_ended"] == 1 and document["surfaces_unconfirmed"] == ["sandboxes"]
    assert "owner it does not know" in document["note"]
