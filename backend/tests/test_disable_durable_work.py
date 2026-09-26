"""`disable` ends the durable work a person left running outside a run, and names what is refused at its next use.

Two kinds of work outlive the run that started them and are kept in the
database, not in a Gateway's memory: durable MCP tasks (a remote job the
deployment polls) and durable subagent batches. `disable` asks each to stop
through the same request a person's own cancel makes -- an MCP task's
cancellation is carried out remotely by the Gateway's task loop, a batch's
is applied at once -- and waits within the one `--wait-seconds`. It looks
again once the runs are over, for work a dying run started. A task
notification and a channel message are refused at their next use; the
document counts what is waiting.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-disable-durable-work-min-32-chars")

from app.gateway.auth.accounts import EXIT_UNCONFIRMED_RUNS, AccountsCommand
from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository

ISSUER = "https://login.example.com/realms/tenant"
DURABLE = ("mcp_tasks", "subagent_batches", "mcp_task_notifications", "channel_ingress")


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _Tasks:
    """The part of ``McpTaskRepository`` the command uses; a task becomes terminal when the Gateway's loop cancels it remotely."""

    def __init__(self) -> None:
        self.tenant = SimpleNamespace(digest="d" * 64)
        self.rows: dict[str, dict] = {}
        self.cancel_requests: list[dict] = []
        self.pending_notifications: dict[str, int] = {}
        self.remote_cancels = True

    def add(self, task_id: str, user_id: str, status: str = "working") -> None:
        self.rows[task_id] = {"id": task_id, "user_id": user_id, "thread_id": f"thread-{task_id}", "status": status}

    async def list_active_by_user(self, user_id: str, *, tenant_digest: str) -> list[dict]:
        assert tenant_digest == self.tenant.digest
        return [dict(row) for row in self.rows.values() if row["user_id"] == user_id and row["status"] not in ("completed", "failed", "cancelled")]

    async def request_cancel(self, task_id: str, **kwargs) -> dict:
        self.cancel_requests.append({"task_id": task_id, **kwargs})

        async def _gateway_loop_cancels() -> None:
            await asyncio.sleep(0.2)
            if self.remote_cancels:
                self.rows[task_id]["status"] = "cancelled"

        asyncio.get_running_loop().create_task(_gateway_loop_cancels())
        return dict(self.rows[task_id])

    async def statuses(self, task_ids: list[str], *, tenant_digest: str) -> dict[str, str]:
        return {task_id: self.rows[task_id]["status"] for task_id in task_ids if task_id in self.rows}

    async def count_pending_notifications(self, user_ids: list[str], *, tenant_digest: str) -> int:
        return sum(self.pending_notifications.get(user_id, 0) for user_id in user_ids)


class _Batches:
    """The part of ``SubagentBatchRepository`` the command uses; a cancel is applied at once."""

    def __init__(self) -> None:
        self.rows: dict[str, dict] = {}
        self.cancelled: list[tuple[str, str, str]] = []

    def add(self, batch_id: str, user_id: str, status: str = "running") -> None:
        self.rows[batch_id] = {"id": batch_id, "user_id": user_id, "status": status}

    async def list_active_by_user(self, user_id: str) -> list[dict]:
        return [dict(row) for row in self.rows.values() if row["user_id"] == user_id and row["status"] not in ("completed", "failed", "cancelled")]

    async def cancel_batch(self, batch_id: str, *, user_id: str, reason: str) -> dict:
        self.cancelled.append((batch_id, user_id, reason))
        self.rows[batch_id]["status"] = "cancelled"
        return dict(self.rows[batch_id])


class _Connections:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    async def list_connections(self, owner_user_id: str) -> list[dict]:
        return [row for row in self.rows if row["owner_user_id"] == owner_user_id]


@pytest.fixture
def stores(tmp_path) -> Iterator[SimpleNamespace]:
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.persistence.refusal_sweeps import RefusalSweepRepository
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/durable.db", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    tenant = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
    try:
        yield SimpleNamespace(
            users=SQLiteUserRepository(session_factory),
            tokens=PersonalAccessTokenRepository(session_factory, tenant=tenant),
            schedules=ScheduledTaskRepository(session_factory),
            runs=RunRepository(session_factory, tenant=tenant),
            tasks=_Tasks(),
            batches=_Batches(),
            connections=_Connections(),
            sweeps=RefusalSweepRepository(session_factory),
        )
    finally:
        asyncio.run(close_engine())


def _account(email: str = "pat@example.com", subject: str = "sub-pat", *, provider: str = "sso") -> User:
    return User(email=email, password_hash=None, system_role="user", oauth_provider=provider, oauth_id=subject, oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC))


def _command(stores: SimpleNamespace, *, with_processes: bool = False, **kwargs) -> AccountsCommand:
    """``with_processes``: the command confirms with the Gateway processes, as it does against a real database."""
    kwargs.setdefault("wait_seconds", 5)
    if with_processes:
        kwargs["sweeps"] = stores.sweeps
    return AccountsCommand(
        stores.users,
        tokens=stores.tokens,
        schedules=stores.schedules,
        runs=stores.runs,
        mcp_tasks=stores.tasks,
        batches=stores.batches,
        channel_connections=stores.connections,
        **kwargs,
    )


@pytest.mark.anyio
async def test_the_persons_mcp_tasks_are_cancelled_as_the_deployer_and_confirmed_by_their_status(stores) -> None:
    account = await stores.users.create_user(_account())
    sibling = await stores.users.create_user(_account("pat.chen@example.com", provider="sso-basic"))
    stores.tasks.add("task-1", str(account.id))
    stores.tasks.add("task-2", str(sibling.id))
    stores.tasks.add("task-done", str(account.id), status="completed")
    stores.tasks.add("task-sam", "someone-else")

    document = await _command(stores).run("disable", email="pat@example.com")

    from app.mcp_tasks.service import _cancel_actor_ref, deployer_cancel_actor_ref

    assert sorted(request["task_id"] for request in stores.tasks.cancel_requests) == ["task-1", "task-2"]
    digest = stores.tasks.tenant.digest
    for request in stores.tasks.cancel_requests:
        assert request["reason_code"] == "account_disabled" and request["retry_now"] is True
        assert request["actor_ref"] == deployer_cancel_actor_ref(tenant_digest=digest), "attributed to the deployer"
        assert request["actor_ref"] != _cancel_actor_ref(tenant_digest=digest, user_id=request["user_id"]), "not to the person"
    entry = document["surfaces"]["mcp_tasks"]
    assert entry["action"] == "ended" and entry["count"] == 2 and entry["not_ended"] == 0 and entry["confirmed_by"] == "task_status"
    assert entry["stopped_after_ms"] is not None and stores.tasks.rows["task-sam"]["status"] == "working"


@pytest.mark.anyio
async def test_a_task_the_remote_does_not_let_go_of_is_unconfirmed(stores) -> None:
    account = await stores.users.create_user(_account())
    stores.tasks.add("task-1", str(account.id))
    stores.tasks.remote_cancels = False

    document = await _command(stores, wait_seconds=0.5).run("disable", issuer=ISSUER, subject="sub-pat")

    entry = document["surfaces"]["mcp_tasks"]
    assert entry["stopped_after_ms"] is None and entry["not_ended"] == 1
    assert "mcp_tasks" in document["surfaces_unconfirmed"] and document["returncode"] == EXIT_UNCONFIRMED_RUNS


@pytest.mark.anyio
async def test_the_persons_subagent_batches_are_cancelled_at_once_with_a_reason_that_says_why(stores) -> None:
    account = await stores.users.create_user(_account())
    stores.batches.add("batch-1", str(account.id))
    stores.batches.add("batch-done", str(account.id), status="completed")
    stores.batches.add("batch-sam", "someone-else")

    document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")

    assert [(batch_id, user_id) for batch_id, user_id, _ in stores.batches.cancelled] == [("batch-1", str(account.id))]
    assert "turned off" in stores.batches.cancelled[0][2]
    entry = document["surfaces"]["subagent_batches"]
    assert entry["action"] == "ended" and entry["count"] == 1 and entry["confirmed_by"] == "batch_status" and entry["stopped_after_ms"] is not None
    assert stores.batches.rows["batch-sam"]["status"] == "running"


@pytest.mark.anyio
async def test_work_a_dying_run_started_after_the_first_look_is_found_by_the_second(stores) -> None:
    """A run being cancelled can still accept a batch or submit a task before it stops."""
    account = await stores.users.create_user(_account())
    list_batches = stores.batches.list_active_by_user
    looks: list[int] = []

    async def _run_starts_one_meanwhile(user_id: str) -> list[dict]:
        looks.append(1)
        if len(looks) == 2:
            stores.batches.add("batch-late", user_id)
            stores.tasks.add("task-late", user_id)
        return await list_batches(user_id)

    stores.batches.list_active_by_user = _run_starts_one_meanwhile
    document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")

    assert [batch_id for batch_id, _, _ in stores.batches.cancelled] == ["batch-late"]
    assert [request["task_id"] for request in stores.tasks.cancel_requests] == ["task-late"]
    assert document["surfaces"]["subagent_batches"]["count"] == 1 and document["surfaces"]["mcp_tasks"]["count"] == 1
    assert str(account.id)


@pytest.mark.anyio
async def test_notifications_and_channel_messages_are_refused_at_their_next_use_and_counted(stores) -> None:
    account = await stores.users.create_user(_account())
    stores.tasks.pending_notifications[str(account.id)] = 2
    stores.connections.rows = [
        {"id": "c1", "owner_user_id": str(account.id), "status": "connected"},
        {"id": "c2", "owner_user_id": str(account.id), "status": "revoked"},
        {"id": "c3", "owner_user_id": "someone-else", "status": "connected"},
    ]

    document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")

    committed = document["surfaces"]["sign_in"]["stopped_after_ms"]
    notifications, channels = document["surfaces"]["mcp_task_notifications"], document["surfaces"]["channel_ingress"]
    assert notifications["action"] == "refused_at_next_use" and notifications["count"] == 2 and notifications["stopped_after_ms"] == committed
    assert channels["action"] == "refused_at_next_use" and channels["count"] == 1 and channels["stopped_after_ms"] == committed


@pytest.mark.anyio
async def test_an_identity_with_no_account_names_the_durable_surfaces_too(stores) -> None:
    document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-nobody")

    for name in DURABLE:
        assert document["surfaces"][name]["count"] == 0 and document["surfaces"][name]["stopped_after_ms"] is not None, name


@pytest.mark.anyio
async def test_a_batch_whose_cancel_is_refused_is_not_ended(stores) -> None:
    account = await stores.users.create_user(_account())
    stores.batches.add("batch-1", str(account.id))

    async def _refused(batch_id: str, *, user_id: str, reason: str) -> None:
        return None

    stores.batches.cancel_batch = _refused
    document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")

    entry = document["surfaces"]["subagent_batches"]
    assert entry["stopped_after_ms"] is None and entry["not_ended"] == 1
    assert "subagent_batches" in document["surfaces_unconfirmed"] and document["returncode"] == EXIT_UNCONFIRMED_RUNS


@pytest.mark.anyio
async def test_a_batch_that_finished_before_its_cancel_counts_as_ended(stores) -> None:
    account = await stores.users.create_user(_account())
    stores.batches.add("batch-1", str(account.id))

    async def _finished_first(batch_id: str, *, user_id: str, reason: str) -> dict:
        stores.batches.rows[batch_id]["status"] = "completed"
        return dict(stores.batches.rows[batch_id])

    stores.batches.cancel_batch = _finished_first
    document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["surfaces"]["subagent_batches"]["not_ended"] == 0 and "subagent_batches" not in document["surfaces_unconfirmed"]


@pytest.mark.anyio
async def test_a_task_whose_row_went_away_is_not_ended(stores) -> None:
    """A row that went away says nothing about the remote job."""
    account = await stores.users.create_user(_account())
    stores.tasks.add("task-1", str(account.id))
    stores.tasks.remote_cancels = False
    request_cancel = stores.tasks.request_cancel

    async def _then_gone(task_id: str, **kwargs) -> dict:
        row = await request_cancel(task_id, **kwargs)
        del stores.tasks.rows[task_id]
        return row

    stores.tasks.request_cancel = _then_gone
    document = await _command(stores, wait_seconds=0.5).run("disable", issuer=ISSUER, subject="sub-pat")

    assert document["surfaces"]["mcp_tasks"]["not_ended"] == 1 and "mcp_tasks" in document["surfaces_unconfirmed"]


@pytest.mark.anyio
async def test_a_cancel_that_fails_on_the_first_look_is_asked_again_on_the_second(stores) -> None:
    account = await stores.users.create_user(_account())
    stores.batches.add("batch-1", str(account.id))
    stores.tasks.add("task-1", str(account.id))
    cancel_batch, request_cancel = stores.batches.cancel_batch, stores.tasks.request_cancel
    attempts = {"batch": 0, "task": 0}

    async def _batch_fails_once(batch_id: str, **kwargs) -> dict:
        attempts["batch"] += 1
        if attempts["batch"] == 1:
            raise ConnectionError("the database blinked")
        return await cancel_batch(batch_id, **kwargs)

    async def _task_fails_once(task_id: str, **kwargs) -> dict:
        attempts["task"] += 1
        if attempts["task"] == 1:
            raise ConnectionError("the database blinked")
        return await request_cancel(task_id, **kwargs)

    stores.batches.cancel_batch, stores.tasks.request_cancel = _batch_fails_once, _task_fails_once
    document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-pat")

    assert attempts == {"batch": 2, "task": 2}
    assert document["surfaces"]["subagent_batches"]["not_ended"] == 0 and document["surfaces"]["mcp_tasks"]["not_ended"] == 0
    assert document["surfaces"]["subagent_batches"]["count"] == 1 and document["surfaces"]["mcp_tasks"]["count"] == 1


async def _with_process(stores: SimpleNamespace, holdings, run, *, unreached: tuple[str, ...] = ()):
    from app.gateway.refusal_watch import RefusalWatch

    watch = RefusalWatch(stores.sweeps, holdings, refused_owners=stores.users.list_refused_user_ids, process_id="gw-1", interval_seconds=0.05, unreached=unreached)
    await watch.start()
    try:
        return await run()
    finally:
        await watch.stop()


@pytest.mark.anyio
async def test_the_batch_items_a_process_executes_are_confirmed_stopped_by_that_process(stores) -> None:
    """The row's cancel reaches an executing item only at its next lease renewal; the process stops it at its look."""
    from deerflow.runtime.owner_holdings import Ended, OwnerHoldings

    account = await stores.users.create_user(_account())
    stores.batches.add("batch-1", str(account.id))
    holdings = OwnerHoldings()
    holdings.add_source("subagent_batches", lambda owners: {str(account.id): Ended(1)} if str(account.id) in owners else {})

    document = await _with_process(stores, holdings, lambda: _command(stores, with_processes=True).run("disable", issuer=ISSUER, subject="sub-pat"))

    entry = document["surfaces"]["subagent_batches"]
    assert entry["count"] == 1 and entry["items_stopped"] >= 1 and entry["processes"] == 1 and entry["not_ended"] == 0
    assert entry["confirmed_by"] == "batch_status" and entry["stopped_after_ms"] is not None
    assert "subagent_batches" not in document["surfaces_unconfirmed"]


@pytest.mark.anyio
async def test_a_batch_item_that_would_not_stop_keeps_the_batches_unconfirmed(stores) -> None:
    from deerflow.runtime.owner_holdings import Ended, OwnerHoldings

    account = await stores.users.create_user(_account())
    stores.batches.add("batch-1", str(account.id))
    holdings = OwnerHoldings()
    holdings.add_source("subagent_batches", lambda owners: {str(account.id): Ended(0, failed=1)} if str(account.id) in owners else {})

    document = await _with_process(stores, holdings, lambda: _command(stores, with_processes=True).run("disable", issuer=ISSUER, subject="sub-pat"))

    entry = document["surfaces"]["subagent_batches"]
    assert entry["stopped_after_ms"] is None and entry["not_ended"] == 1 and entry["count"] == 1
    assert "subagent_batches" in document["surfaces_unconfirmed"] and document["returncode"] == EXIT_UNCONFIRMED_RUNS
    assert "batch item" in document["note"]


@pytest.mark.anyio
async def test_tasks_no_live_process_can_cancel_are_not_reached(stores) -> None:
    """A deployment whose Gateways run no task loop will never carry the cancellation out; re-running will not change that."""
    from deerflow.runtime.owner_holdings import OwnerHoldings

    account = await stores.users.create_user(_account())
    stores.tasks.add("task-1", str(account.id))
    stores.tasks.remote_cancels = False

    document = await _with_process(stores, OwnerHoldings(), lambda: _command(stores, with_processes=True, wait_seconds=0.5).run("disable", issuer=ISSUER, subject="sub-pat"), unreached=("mcp_tasks",))

    entry = document["surfaces"]["mcp_tasks"]
    assert entry["action"] == "not_reached" and entry["processes_unreached"] == ["gw-1"] and entry["not_ended"] == 1
    assert "mcp_tasks" in document["surfaces_not_reached"] and document["returncode"] == EXIT_UNCONFIRMED_RUNS
    assert "task loop" in document["note"]


@pytest.mark.anyio
async def test_a_task_a_task_loop_has_not_cancelled_yet_is_unconfirmed_not_unreached(stores) -> None:
    from deerflow.runtime.owner_holdings import OwnerHoldings

    account = await stores.users.create_user(_account())
    stores.tasks.add("task-1", str(account.id))
    stores.tasks.remote_cancels = False

    document = await _with_process(stores, OwnerHoldings(), lambda: _command(stores, with_processes=True, wait_seconds=0.5).run("disable", issuer=ISSUER, subject="sub-pat"))

    entry = document["surfaces"]["mcp_tasks"]
    assert entry["action"] == "ended" and entry["processes_unreached"] == [] and entry["not_ended"] == 1
    assert "mcp_tasks" in document["surfaces_unconfirmed"] and document["surfaces_not_reached"] == []
    assert "remote server" in document["note"] and "owner it does not know" not in document["note"]
