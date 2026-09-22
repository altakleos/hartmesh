"""End to end: turning an account off ends the run, the command, and the stream.

These are the acceptance cases for the whole path, wired together over one
real database:

    accounts disable  ->  durable cancellation on the row
                      ->  the owning worker's cancellation watch
                      ->  the run task is cancelled
                      ->  the sandbox command is killed
                      ->  the run reaches a terminal status
                      ->  the command confirms it and reports one found, one cancelled

The run task here stands in for the Gateway's worker: it runs one real sandbox
command and, on cancellation, persists the terminal status the worker persists.
That last step is the worker's own contract and has its own tests; what these
pin is everything between the deployer's write and the dead process, which is
what no test covered while a released build let a ``sleep 541`` run on for
537 s after the account was turned off.

A run that is terminal is a run whose stream has ended: the SSE bridge closes
when the run does, so the terminal row is the assertion that reaches it here.
"""

from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-disable-e2e-min-32-chars")

from app.gateway.auth.accounts import AccountsCommand
from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.runtime.runs.manager import RunManager, RunStatus
from deerflow.sandbox.lease import run_sync_sandbox_command
from deerflow.sandbox.local.local_sandbox import LocalSandbox

ISSUER = "https://login.example.com/realms/tenant"
posix_only = pytest.mark.skipif(os.name == "nt", reason="POSIX process-group semantics")

#: What the acceptance calls "within a bounded time". The watch ticks every
#: 0.05 s in these tests; live it is every five seconds.
BOUND_SECONDS = 20.0


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def acting_as():
    """Run the worker's writes as the account, the way a served request does.

    Repository methods resolve ``user_id`` from a contextvar; the Gateway sets
    it from the authenticated principal, and the suite's autouse default is a
    different user, so a manager writing a run for this account could not read
    its own row back.
    """
    from deerflow.runtime.user_context import set_current_user

    def _act(account) -> None:
        # Not reset here: the token is minted inside the test's own async
        # context, which ends with the test, and a reset from the fixture's
        # context is refused. The autouse default is restored the same way.
        set_current_user(SimpleNamespace(id=str(account.id), email=account.email))

    return _act


@pytest.fixture
def deployment(tmp_path) -> Iterator[tuple[SQLiteUserRepository, object]]:
    """One database, as the Gateway and the deployer's command both see it."""
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.run import RunRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/deployment.db", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    assert session_factory is not None
    tenant = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
    try:
        yield SQLiteUserRepository(session_factory), lambda: RunRepository(session_factory, tenant=tenant)
    finally:
        asyncio.run(close_engine())


def _worker(runs, *, heartbeat: bool = False) -> RunManager:
    manager = RunManager(store=runs, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=heartbeat))
    manager.out_of_band_cancellation_poll_seconds = 0.05
    return manager


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


async def _await_file(path: Path, *, timeout: float = 15.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            text = path.read_text().strip()
            if text:
                return text
        await asyncio.sleep(0.05)
    raise AssertionError(f"{path} never appeared; the sandbox command never started")


async def _await(predicate, *, timeout: float = BOUND_SECONDS) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.05)
    return False


async def _start_run_with_a_long_command(manager: RunManager, sandbox: LocalSandbox, user_id: str, pid_file: Path, *, thread_id: str = "thread-1"):
    """One run whose tool call blocks on a command that would run for five minutes."""
    record = await manager.create(thread_id, user_id=user_id)
    await manager.set_status(record.run_id, RunStatus.running)

    async def worker_body() -> None:
        try:
            await run_sync_sandbox_command(
                sandbox,
                sandbox.execute_command,
                f"echo $$ > {pid_file}; sleep 300",
                None,
                300.0,
            )
        except asyncio.CancelledError:
            # What the Gateway's worker does on abort: persist the terminal
            # status, which is what ends the run's stream.
            await manager.set_status(record.run_id, RunStatus.interrupted)
            raise

    record.task = asyncio.create_task(worker_body())
    return record


# ── The acceptance case ─────────────────────────────────────────────────


@posix_only
@pytest.mark.anyio
async def test_disabling_the_owner_ends_the_run_the_command_and_the_stream(deployment, acting_as, tmp_path) -> None:
    users, run_store = deployment
    runs = run_store()
    account = await users.create_user(User(email="pat@example.com", password_hash=None, system_role="user", oauth_provider="sso", oauth_id="sub-pat", oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC)))
    acting_as(account)
    sandbox = LocalSandbox("acceptance")
    pid_file = tmp_path / "command.pid"

    manager = _worker(runs)
    await manager.start_cancellation_watch()
    try:
        record = await _start_run_with_a_long_command(manager, sandbox, str(account.id), pid_file)
        command_pid = int(await _await_file(pid_file))
        assert _alive(command_pid), "the command under test is not running"

        started = time.monotonic()
        document = await AccountsCommand(users, tokens=None, schedules=None, runs=run_store(), wait_seconds=BOUND_SECONDS).run(
            "disable",
            issuer=ISSUER,
            subject="sub-pat",
        )
        elapsed = time.monotonic() - started

        # The JSON is the answer the deployer reads.
        assert document["runs_found"] == 1
        assert document["runs_cancelled"] == 1
        assert document["runs_unconfirmed"] == []
        assert document["returncode"] == 0
        assert elapsed < BOUND_SECONDS

        # The run is terminal, which is what closes its stream.
        assert (await runs.get(record.run_id, user_id=None))["status"] == "interrupted"
        # The work itself stopped: the sandbox process is gone.
        assert await _await(lambda: not _alive(command_pid)), "the sandbox command outlived the refusal"
        assert await _await(lambda: record.task.done())
    finally:
        await manager.stop_cancellation_watch()
        sandbox.abort_running_commands()


@posix_only
@pytest.mark.anyio
async def test_the_run_of_another_worker_is_ended_by_the_worker_that_owns_it(deployment, acting_as, tmp_path) -> None:
    """Two workers over one database; only the owner acts, and it does act."""
    users, run_store = deployment
    account = await users.create_user(User(email="pat@example.com", password_hash=None, system_role="user", oauth_provider="sso", oauth_id="sub-pat", oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC)))
    acting_as(account)
    sandbox = LocalSandbox("acceptance-two-workers")
    pid_file = tmp_path / "command.pid"

    bystander = _worker(run_store())
    owner = _worker(run_store())
    await bystander.start_cancellation_watch()
    await owner.start_cancellation_watch()
    try:
        # The bystander has its own unrelated run, so its watch is awake and
        # querying: it sees the cancelled row and must still leave it alone.
        bystander_record = await bystander.create("thread-bystander", user_id=str(account.id))
        await bystander.set_status(bystander_record.run_id, RunStatus.running)
        bystander_record.task = asyncio.create_task(asyncio.Event().wait())

        record = await _start_run_with_a_long_command(owner, sandbox, str(account.id), pid_file, thread_id="thread-owner")
        command_pid = int(await _await_file(pid_file))

        document = await AccountsCommand(users, tokens=None, schedules=None, runs=run_store(), wait_seconds=BOUND_SECONDS).run(
            "disable",
            issuer=ISSUER,
            subject="sub-pat",
        )

        # Both runs belonged to the account; the bystander's has no worker that
        # terminalizes it here, so it is named rather than rounded down.
        assert document["runs_found"] == 2
        assert document["runs_cancelled"] == 1
        assert document["runs_unconfirmed"] == [bystander_record.run_id]
        assert document["returncode"] == 1

        assert record.run_id not in bystander._runs, "the run belongs to the other worker"
        assert await _await(lambda: not _alive(command_pid)), "the owner never reached its command"
        assert await _await(lambda: record.task.done())
    finally:
        bystander_record.task.cancel()
        await bystander.stop_cancellation_watch()
        await owner.stop_cancellation_watch()
        sandbox.abort_running_commands()


@posix_only
@pytest.mark.anyio
async def test_the_lease_heartbeat_carries_the_cancellation_where_it_is_the_observer(deployment, acting_as, tmp_path) -> None:
    """The topology a deployment with more than one worker actually runs.

    With ``run_ownership.heartbeat_enabled`` on, the renewal is what reads
    ``cancel_action``, and the cancellation watch deliberately stays off so one
    row never has two observers. The command's reach has to be the same either
    way, so it is asserted against the same real sandbox command.
    """
    users, run_store = deployment
    runs = run_store()
    account = await users.create_user(User(email="pat@example.com", password_hash=None, system_role="user", oauth_provider="sso", oauth_id="sub-pat", oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC)))
    acting_as(account)
    sandbox = LocalSandbox("acceptance-heartbeat")
    pid_file = tmp_path / "command.pid"

    manager = _worker(runs, heartbeat=True)
    await manager.start_cancellation_watch()
    assert manager._cancellation_watch_task is None, "the heartbeat already observes cancellations here"
    record = await _start_run_with_a_long_command(manager, sandbox, str(account.id), pid_file)
    command_pid = int(await _await_file(pid_file))
    try:
        await run_store().request_cancel_compat(record.run_id, action="interrupt", user_id=str(account.id))
        await manager._renew_leases()

        assert record.abort_event.is_set(), "the renewal did not carry the cancellation"
        assert await _await(lambda: not _alive(command_pid)), "the sandbox command outlived the cancellation"
        assert await _await(lambda: record.task.done())
        assert (await runs.get(record.run_id, user_id=None))["status"] == "interrupted"
    finally:
        sandbox.abort_running_commands()


@posix_only
@pytest.mark.anyio
async def test_a_demoted_administrator_keeps_their_run_unless_the_deployer_says_otherwise(deployment, acting_as, tmp_path) -> None:
    users, run_store = deployment
    runs = run_store()
    account = await users.create_user(User(email="boss@example.com", password_hash=None, system_role="admin", oauth_provider="sso", oauth_id="sub-boss", oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC)))
    acting_as(account)
    sandbox = LocalSandbox("acceptance-demotion")
    pid_file = tmp_path / "command.pid"

    manager = _worker(runs)
    await manager.start_cancellation_watch()
    try:
        record = await _start_run_with_a_long_command(manager, sandbox, str(account.id), pid_file)
        command_pid = int(await _await_file(pid_file))

        plain = await AccountsCommand(users, tokens=None, schedules=None, runs=run_store(), wait_seconds=BOUND_SECONDS).run(
            "end-sessions",
            issuer=ISSUER,
            subject="sub-boss",
        )
        await asyncio.sleep(0.4)

        assert "runs_found" not in plain
        assert _alive(command_pid), "demotion is not removal; the run must keep going"
        assert not record.task.done()

        asked = await AccountsCommand(users, tokens=None, schedules=None, runs=run_store(), wait_seconds=BOUND_SECONDS).run(
            "end-sessions",
            issuer=ISSUER,
            subject="sub-boss",
            end_running_work=True,
        )

        assert asked["runs_found"] == 1 and asked["runs_cancelled"] == 1
        assert asked["runs_unconfirmed"] == [] and asked["returncode"] == 0
        assert await _await(lambda: not _alive(command_pid))
    finally:
        await manager.stop_cancellation_watch()
        sandbox.abort_running_commands()
