"""Rejoining revives nothing: what ``disable`` turned off stays off after ``enable``, unless the deployer restores exactly that.

``disable`` holds every covered account's active schedules and connected
channel bindings, records what it held for the identity, and ends the work
waiting to run for them: queued scheduled occurrences (manual triggers
included), task notifications, and channel messages still waiting, dead
letters included. After ``enable`` the held schedules and bindings stay off
until their owner turns them on, and none of that work runs.
``enable --restore-held`` turns back on exactly what the matching disable
held -- nothing the owner had paused themselves -- with each schedule at its
next future occurrence; every ``enable`` discards the record.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

os.environ.setdefault("AUTH_JWT_SECRET", "test-secret-key-disable-hold-restore-min-32-chars")

from support.scheduled_task_runtime import CallbackInvocationRuntime, NeverLaunchInvocationRuntime

from app.gateway.auth.accounts import EXIT_UNCONFIRMED_RUNS, AccountsCommand, main
from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.runtime.invocation import OwnerRefusedLaunchError

ISSUER = "https://login.example.com/realms/tenant"
HOLD_SURFACES = ("schedules", "channel_bindings", "scheduled_occurrences", "mcp_task_notifications", "channel_receipts")


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _Tasks:
    """The part of ``McpTaskRepository`` the command uses; nothing is active, and notifications wait per owner."""

    def __init__(self) -> None:
        self.tenant = SimpleNamespace(digest="d" * 64)
        self.waiting: dict[str, int] = {}
        self.in_a_loop: dict[str, int] = {}
        self.ended: list[tuple[tuple[str, ...], str]] = []

    async def list_active_by_user(self, user_id: str, *, tenant_digest: str) -> list[dict]:
        return []

    async def end_waiting_notifications(self, user_ids: list[str], *, error: str, tenant_digest: str) -> int:
        assert tenant_digest == self.tenant.digest
        self.ended.append((tuple(user_ids), error))
        return sum(self.waiting.pop(user_id, 0) for user_id in user_ids)

    async def count_waiting_notifications(self, user_ids: list[str], *, tenant_digest: str) -> int:
        """What a task loop holds: not ended by the command, whose own launch reads the refusal."""
        return sum(self.in_a_loop.get(user_id, 0) for user_id in user_ids)


@pytest.fixture
def stores(tmp_path) -> Iterator[SimpleNamespace]:
    from app.channels.inbound_receipts import SqlInboundReceiptStore
    from deerflow.persistence.channel_connections import ChannelConnectionRepository, ChannelCredentialCipher
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.inbound_receipt.model import InboundReceiptRow  # noqa: F401 - registers the table
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.scheduled_task_runs import ScheduledTaskRunRepository
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    asyncio.run(init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path}/holds.db", sqlite_dir=str(tmp_path)))
    session_factory = get_session_factory()
    tenant = TenantIdentityV1.from_canonical_id("local").to_persisted_reference()
    try:
        yield SimpleNamespace(
            session_factory=session_factory,
            users=SQLiteUserRepository(session_factory),
            tokens=PersonalAccessTokenRepository(session_factory, tenant=tenant),
            schedules=ScheduledTaskRepository(session_factory),
            occurrences=ScheduledTaskRunRepository(session_factory),
            runs=RunRepository(session_factory, tenant=tenant),
            connections=ChannelConnectionRepository(session_factory, cipher=ChannelCredentialCipher.from_key("test-encryption-key")),
            receipts=SqlInboundReceiptStore(session_factory),
            tasks=_Tasks(),
        )
    finally:
        asyncio.run(close_engine())


def _account(email: str = "pat@example.com", *, provider: str = "sso") -> User:
    return User(email=email, password_hash=None, system_role="user", oauth_provider=provider, oauth_id="sub-pat", oauth_issuer=ISSUER, last_sign_in_at=datetime.now(UTC))


def _command(stores: SimpleNamespace, **kwargs) -> AccountsCommand:
    kwargs.setdefault("wait_seconds", 5)
    return AccountsCommand(
        stores.users,
        tokens=stores.tokens,
        schedules=stores.schedules,
        runs=stores.runs,
        mcp_tasks=stores.tasks,
        channel_connections=stores.connections,
        receipts=stores.receipts,
        **kwargs,
    )


async def _schedule(stores: SimpleNamespace, user_id: str, *, once_at: datetime | None = None, due: datetime | None = None, paused: bool = False) -> str:
    task_id = str(uuid4())
    await stores.schedules.create(
        task_id=task_id,
        user_id=user_id,
        thread_id=f"thread-{task_id}",
        context_mode="reuse_thread",
        assistant_id="lead_agent",
        title="Weekly numbers",
        prompt="Send me the weekly numbers",
        schedule_type="once" if once_at else "cron",
        schedule_spec={"run_at": once_at.isoformat()} if once_at else {"cron": "0 9 * * 1"},
        timezone="UTC",
        next_run_at=once_at or due or datetime.now(UTC) + timedelta(days=1),
    )
    if paused:
        await stores.schedules.update(task_id, user_id=user_id, updates={"status": "paused"})
    return task_id


async def _bind(stores: SimpleNamespace, user_id: str, account: str = "U-pat", *, revoked: bool = False) -> str:
    binding = await stores.connections.upsert_connection(owner_user_id=user_id, provider="slack", external_account_id=account, workspace_id="T1")
    if revoked:
        await stores.connections.disconnect_connection(connection_id=binding["id"], owner_user_id=user_id)
    return binding["id"]


async def _status(stores: SimpleNamespace, task_id: str) -> str:
    return (await stores.schedules.get_internal(task_id))["status"]


@pytest.mark.anyio
async def test_disable_holds_every_covered_accounts_active_schedules_and_bindings_and_says_what_it_held(stores) -> None:
    account = await stores.users.create_user(_account())
    sibling = await stores.users.create_user(_account("pat.chen@example.com", provider="sso-basic"))
    weekly = await _schedule(stores, str(account.id))
    siblings = await _schedule(stores, str(sibling.id))
    self_paused = await _schedule(stores, str(account.id), paused=True)
    theirs = await _schedule(stores, "someone-else")
    binding = await _bind(stores, str(account.id))
    await _bind(stores, str(account.id), "U-old", revoked=True)
    their_binding = await _bind(stores, "someone-else", "U-sam")

    await stores.connections.create_oauth_state(owner_user_id=str(account.id), provider="slack", state="code-before-the-refusal", expires_at=datetime.now(UTC) + timedelta(minutes=10))

    document = await _command(stores).run("disable", email="pat@example.com")

    assert document["held"] == {"schedules": sorted([weekly, siblings]), "channel_bindings": [binding]}
    # A connect code minted before the refusal cannot turn the held binding back on.
    assert await stores.connections.consume_oauth_state(provider="slack", state="code-before-the-refusal") is None
    assert document["surfaces"]["channel_bindings"]["connect_codes_ended"] == 1
    # Unchanged meaning: the addressed account's active schedules when the command ran.
    assert document["schedules_held"] == 1
    for task_id in (weekly, siblings, self_paused):
        assert await _status(stores, task_id) == "paused"
    assert await _status(stores, theirs) == "enabled"
    assert await stores.connections.find_connection_by_external_identity(provider="slack", external_account_id="U-pat", workspace_id="T1") is None
    assert (await stores.connections.find_connection_by_external_identity(provider="slack", external_account_id="U-sam", workspace_id="T1"))["id"] == their_binding
    committed = document["surfaces"]["sign_in"]["stopped_after_ms"]
    for name, count in (("schedules", 2), ("channel_bindings", 1)):
        entry = document["surfaces"][name]
        assert entry["action"] == "held" and entry["count"] == count, name
        assert entry["stopped_after_ms"] >= committed and entry["not_ended"] == 0, name
    assert document["returncode"] == 0


@pytest.mark.anyio
async def test_after_disable_and_enable_a_held_schedule_does_not_fire_at_its_next_due_time(stores) -> None:
    from app.scheduler.service import ScheduledTaskService

    account = await stores.users.create_user(_account())
    weekly = await _schedule(stores, str(account.id), due=datetime.now(UTC) + timedelta(hours=1))
    binding = await _bind(stores, str(account.id))
    command = _command(stores)
    await command.run("disable", email="pat@example.com")

    enabled = await command.run("enable", email="pat@example.com")

    assert enabled["held"] == {"schedules": [weekly], "channel_bindings": [binding]}
    assert enabled["restored"] == {"schedules": [], "channel_bindings": []}
    assert "stay off until" in enabled["note"]
    assert await stores.users.list_holds(ISSUER, "sub-pat") == [], "a plain enable discards the record"
    scheduler = ScheduledTaskService(
        task_repo=stores.schedules,
        task_run_repo=stores.occurrences,
        invocation_runtime=NeverLaunchInvocationRuntime(),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )
    await scheduler.run_once(now=datetime.now(UTC) + timedelta(days=8))
    assert await _status(stores, weekly) == "paused"
    assert await stores.occurrences.list_by_task(weekly) == [], "no occurrence was even queued"
    assert await stores.connections.find_connection_by_external_identity(provider="slack", external_account_id="U-pat", workspace_id="T1") is None


@pytest.mark.anyio
async def test_restore_held_turns_back_on_exactly_what_the_disable_held_at_its_next_future_occurrence(stores) -> None:
    account = await stores.users.create_user(_account())
    user_id = str(account.id)
    now = datetime.now(UTC)
    overdue = await _schedule(stores, user_id, due=now - timedelta(days=3))
    self_paused = await _schedule(stores, user_id, paused=True)
    once_ahead = await _schedule(stores, user_id, once_at=now + timedelta(days=2))
    once_passed = await _schedule(stores, user_id, once_at=now + timedelta(seconds=1))
    binding = await _bind(stores, user_id)
    command = _command(stores)
    await command.run("disable", email="pat@example.com")
    await asyncio.sleep(1.2)

    restored = await command.run("enable", email="pat@example.com", restore_held=True)

    assert restored["verdict"] == "enabled" and restored["restore_held"] is True
    assert restored["restored"] == {"schedules": sorted([overdue, once_ahead]), "channel_bindings": [binding]}
    assert restored["stayed_off"] == [{"surface": "schedules", "id": once_passed, "reason": "time_passed"}]
    assert restored["surfaces_unconfirmed"] == [] and restored["returncode"] == 0
    assert await _status(stores, overdue) == "enabled"
    # Not the occurrence it missed three days ago: the next one from now.
    assert datetime.fromisoformat((await stores.schedules.get_internal(overdue))["next_run_at"]) > datetime.now(UTC)
    assert await _status(stores, once_ahead) == "enabled"
    assert await _status(stores, once_passed) == "paused"
    assert await _status(stores, self_paused) == "paused", "the owner's own pause is theirs"
    assert (await stores.connections.find_connection_by_external_identity(provider="slack", external_account_id="U-pat", workspace_id="T1"))["id"] == binding
    assert await stores.users.list_holds(ISSUER, "sub-pat") == []


@pytest.mark.anyio
async def test_a_plain_enable_discards_the_hold_so_a_later_restore_revives_nothing_from_it(stores) -> None:
    account = await stores.users.create_user(_account())
    first = await _schedule(stores, str(account.id))
    command = _command(stores)
    await command.run("disable", email="pat@example.com")
    await command.run("enable", email="pat@example.com")
    second = await _schedule(stores, str(account.id))

    held = await command.run("disable", email="pat@example.com")
    restored = await command.run("enable", email="pat@example.com", restore_held=True)

    assert held["held"]["schedules"] == [second]
    assert restored["restored"]["schedules"] == [second]
    assert await _status(stores, first) == "paused", "the first hold went with the plain enable"


@pytest.mark.anyio
async def test_list_shows_what_each_identity_has_held_so_a_deployer_can_check_before_a_restore(stores) -> None:
    account = await stores.users.create_user(_account())
    weekly = await _schedule(stores, str(account.id))
    binding = await _bind(stores, str(account.id))
    command = _command(stores)
    await command.run("disable", email="pat@example.com")

    listed = await command.run("list")

    assert listed["holds"] == [{"issuer": ISSUER, "subject": "sub-pat", "schedules": [weekly], "channel_bindings": [binding]}]
    await command.run("enable", email="pat@example.com")
    assert (await command.run("list"))["holds"] == []


@pytest.mark.anyio
async def test_a_restore_that_fails_is_unconfirmed_kept_in_the_record_and_finished_by_a_rerun(stores, monkeypatch) -> None:
    account = await stores.users.create_user(_account())
    weekly = await _schedule(stores, str(account.id))
    binding = await _bind(stores, str(account.id))
    command = _command(stores)
    await command.run("disable", email="pat@example.com")
    restore_held = stores.schedules.restore_held

    async def _unavailable(*args, **kwargs):
        raise ConnectionError("the database went away")

    monkeypatch.setattr(stores.schedules, "restore_held", _unavailable)
    failed = await command.run("enable", email="pat@example.com", restore_held=True)

    assert failed["returncode"] == EXIT_UNCONFIRMED_RUNS and failed["surfaces_unconfirmed"] == ["schedules"]
    assert failed["stayed_off"] == [{"surface": "schedules", "id": weekly, "reason": "restore_failed"}]
    assert failed["restored"] == {"schedules": [], "channel_bindings": [binding]}
    assert await stores.users.list_holds(ISSUER, "sub-pat") == [("schedule", weekly, str(account.id))], "only what was settled leaves the record"

    monkeypatch.setattr(stores.schedules, "restore_held", restore_held)
    again = await command.run("enable", email="pat@example.com", restore_held=True)

    assert again["verdict"] == "already_enabled" and again["restored"] == {"schedules": [weekly], "channel_bindings": []}
    assert again["returncode"] == 0 and await _status(stores, weekly) == "enabled"


@pytest.mark.anyio
async def test_a_restore_with_no_record_says_so(stores) -> None:
    await stores.users.create_user(_account())
    command = _command(stores)
    await command.run("disable", email="pat@example.com")

    restored = await command.run("enable", email="pat@example.com", restore_held=True)

    assert restored["held"] == {"schedules": [], "channel_bindings": []} and restored["returncode"] == 0
    assert "no hold was recorded for this identity" in restored["note"]


@pytest.mark.anyio
async def test_disable_ends_the_work_waiting_to_run_for_them(stores) -> None:
    from sqlalchemy import select

    from deerflow.persistence.inbound_receipt.model import InboundReceiptRow
    from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow

    account = await stores.users.create_user(_account())
    user_id = str(account.id)
    self_paused = await _schedule(stores, user_id, paused=True)
    # A manual trigger runs even on a paused task.
    await stores.occurrences.create(run_record_id="manual-occurrence", task_id=self_paused, thread_id="thread-manual", scheduled_for=datetime.now(UTC), trigger="manual", status="queued")
    stores.tasks.waiting[user_id] = 2
    async with stores.session_factory() as session:
        session.add(
            InboundReceiptRow(
                receipt_id="00000000-0000-0000-0000-000000000001",
                provider="slack",
                binding_kind="connection",
                binding_reference="binding-1",
                provider_delivery_id="delivery-1",
                thread_id="thread-receipt",
                payload_json={"version": 1, "owner_user_id": user_id},
                payload_digest="a" * 64,
                provider_event_digest="b" * 64,
                state="dead_letter",
                fencing_token=1,
                next_attempt_at=datetime.now(UTC),
                received_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
            )
        )
        await session.commit()

    document = await _command(stores).run("disable", email="pat@example.com")

    surfaces = document["surfaces"]
    assert surfaces["scheduled_occurrences"]["action"] == "ended" and surfaces["scheduled_occurrences"]["count"] == 1
    assert surfaces["mcp_task_notifications"]["action"] == "ended" and surfaces["mcp_task_notifications"]["count"] == 2
    assert surfaces["channel_receipts"]["action"] == "ended" and surfaces["channel_receipts"]["count"] == 1
    assert stores.tasks.ended[0] == ((user_id,), "mcp_task_notification_owner_refused")
    async with stores.session_factory() as session:
        occurrence = await session.get(ScheduledTaskRunRow, "manual-occurrence")
        receipt = (await session.execute(select(InboundReceiptRow))).scalar_one()
    assert occurrence.status == "interrupted"
    assert receipt.state == "completed" and receipt.outcome_code == "owner_refused"


@pytest.mark.anyio
async def test_what_appears_while_the_runs_unwind_is_held_and_ended_by_the_second_look(stores) -> None:
    """A request that authenticated before the refusal can still make a schedule, and a task that went terminal has a notification waiting."""
    account = await stores.users.create_user(_account())
    user_id = str(account.id)
    command = _command(stores)
    late: list[str] = []
    end_running_work = command._end_running_work

    async def _meanwhile(*args, **kwargs):
        result = await end_running_work(*args, **kwargs)
        late.append(await _schedule(stores, user_id))
        stores.tasks.waiting[user_id] = 1
        return result

    command._end_running_work = _meanwhile
    document = await command.run("disable", email="pat@example.com")

    assert document["held"]["schedules"] == late
    assert await _status(stores, late[0]) == "paused"
    assert document["surfaces"]["mcp_task_notifications"]["count"] == 1
    assert len(stores.tasks.ended) == 2, "both looks"


@pytest.mark.anyio
async def test_work_a_gateway_still_has_in_hand_is_not_reported_ended(stores) -> None:
    """A notification a task loop is launching, or an occurrence a scheduler is: its own launch reads the refusal, and until it has, a re-run is needed."""
    account = await stores.users.create_user(_account())
    user_id = str(account.id)
    weekly = await _schedule(stores, user_id)
    await stores.occurrences.create(run_record_id="launching-occurrence", task_id=weekly, thread_id="thread-launching", scheduled_for=datetime.now(UTC), trigger="scheduled", status="queued")
    await stores.occurrences.claim_queued_run("launching-occurrence", lease_owner="scheduler-a", now=datetime.now(UTC), lease_seconds=60, global_max_concurrent_runs=10)
    stores.tasks.in_a_loop[user_id] = 1

    document = await _command(stores).run("disable", email="pat@example.com")

    for name in ("scheduled_occurrences", "mcp_task_notifications"):
        entry = document["surfaces"][name]
        assert entry["not_ended"] == 1 and entry["stopped_after_ms"] is None, name
        assert name in document["surfaces_unconfirmed"], name
    assert document["surfaces"]["channel_receipts"]["not_ended"] == 0
    assert document["returncode"] == EXIT_UNCONFIRMED_RUNS


@pytest.mark.anyio
async def test_a_schedule_its_owner_paused_while_the_command_looked_is_not_recorded_as_held(stores, monkeypatch) -> None:
    """Active when listed, paused by its owner before the hold reached it: a restore must not resume it."""
    account = await stores.users.create_user(_account())
    user_id = str(account.id)
    weekly = await _schedule(stores, user_id)
    hold = stores.schedules.hold

    async def _owner_paused_first(task_id: str, **kwargs):
        await stores.schedules.update(task_id, user_id=user_id, updates={"status": "paused"})
        return await hold(task_id, **kwargs)

    monkeypatch.setattr(stores.schedules, "hold", _owner_paused_first)
    document = await _command(stores).run("disable", email="pat@example.com")

    assert document["held"]["schedules"] == []
    restored = await _command(stores).run("enable", email="pat@example.com", restore_held=True)
    assert restored["restored"]["schedules"] == [] and await _status(stores, weekly) == "paused"


@pytest.mark.anyio
async def test_a_look_that_cannot_record_what_to_hold_still_cancels_the_runs_and_is_unconfirmed(stores, monkeypatch) -> None:
    account = await stores.users.create_user(_account())
    run_id = str(uuid4())
    await stores.runs.put(run_id, thread_id="thread-running", user_id=str(account.id), status="running")

    async def _locked(*args, **kwargs):
        raise ConnectionError("database is locked")

    monkeypatch.setattr(stores.users, "record_holds", _locked)
    document = await _command(stores, wait_seconds=1).run("disable", email="pat@example.com")

    assert document["runs_found"] == 1, "the command went on to the runs"
    assert {"schedules", "channel_bindings"} <= set(document["surfaces_unconfirmed"])
    assert document["returncode"] == EXIT_UNCONFIRMED_RUNS


@pytest.mark.anyio
async def test_a_queue_that_cannot_be_ended_is_unconfirmed_and_does_not_stop_the_others(stores, monkeypatch) -> None:
    account = await stores.users.create_user(_account())
    stores.tasks.waiting[str(account.id)] = 1

    async def _unavailable(*args, **kwargs):
        raise ConnectionError("the database went away")

    monkeypatch.setattr(stores.receipts, "end_for_owners", _unavailable)
    document = await _command(stores).run("disable", email="pat@example.com")

    assert "channel_receipts" in document["surfaces_unconfirmed"] and document["returncode"] == EXIT_UNCONFIRMED_RUNS
    assert document["surfaces"]["channel_receipts"]["stopped_after_ms"] is None
    assert document["surfaces"]["mcp_task_notifications"]["count"] == 1 and "mcp_task_notifications" not in document["surfaces_unconfirmed"]


@pytest.mark.anyio
async def test_a_queue_the_first_look_could_not_end_and_the_second_did_is_confirmed(stores, monkeypatch) -> None:
    await stores.users.create_user(_account())
    end_for_owners = stores.receipts.end_for_owners
    calls: list[int] = []

    async def _fails_once(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("database is locked")
        return await end_for_owners(*args, **kwargs)

    monkeypatch.setattr(stores.receipts, "end_for_owners", _fails_once)
    document = await _command(stores).run("disable", email="pat@example.com")

    assert len(calls) == 2
    assert "channel_receipts" not in document["surfaces_unconfirmed"] and document["returncode"] == 0


@pytest.mark.anyio
async def test_what_is_held_is_recorded_before_it_is_paused_and_held_before_the_runs_unwind(stores, monkeypatch) -> None:
    account = await stores.users.create_user(_account())
    weekly = await _schedule(stores, str(account.id))
    command = _command(stores)
    hold = stores.schedules.hold
    recorded_when_paused: list[bool] = []

    async def _check_the_record(task_id: str, **kwargs):
        recorded_when_paused.append(any(target == task_id for _, target, _ in await stores.users.list_holds(ISSUER, "sub-pat")))
        return await hold(task_id, **kwargs)

    monkeypatch.setattr(stores.schedules, "hold", _check_the_record)
    paused_while_the_runs_unwind: list[str] = []
    end_running_work = command._end_running_work

    async def _meanwhile(*args, **kwargs):
        paused_while_the_runs_unwind.append(await _status(stores, weekly))
        return await end_running_work(*args, **kwargs)

    command._end_running_work = _meanwhile
    await command.run("disable", email="pat@example.com")

    assert recorded_when_paused and all(recorded_when_paused)
    assert paused_while_the_runs_unwind == ["paused"]


@pytest.mark.anyio
async def test_a_plain_enable_discards_the_record_before_it_lets_the_person_back(stores, monkeypatch) -> None:
    """Stopped in between, it must not leave a record a later restore would revive."""
    account = await stores.users.create_user(_account())
    await _schedule(stores, str(account.id))
    command = _command(stores)
    await command.run("disable", email="pat@example.com")

    async def _stopped(*args, **kwargs):
        raise ConnectionError("the command was stopped")

    monkeypatch.setattr(stores.users, "enable_identity", _stopped)
    with pytest.raises(ConnectionError):
        await command.run("enable", email="pat@example.com")

    assert await stores.users.list_holds(ISSUER, "sub-pat") == []


@pytest.mark.anyio
async def test_a_rerun_reports_the_whole_hold_and_holds_nothing_twice(stores) -> None:
    account = await stores.users.create_user(_account())
    weekly = await _schedule(stores, str(account.id))
    command = _command(stores)
    await command.run("disable", email="pat@example.com")

    again = await command.run("disable", email="pat@example.com")

    assert again["held"] == {"schedules": [weekly], "channel_bindings": []}
    assert again["surfaces"]["schedules"]["count"] == 1
    assert again["schedules_held"] == 0, "nothing is active any more"
    assert (await stores.schedules.get_internal(weekly))["schedule_version"] == 2, "held once"


@pytest.mark.anyio
async def test_a_hold_that_fails_is_unconfirmed_and_a_rerun_holds_it(stores, monkeypatch) -> None:
    account = await stores.users.create_user(_account())
    weekly = await _schedule(stores, str(account.id))
    command = _command(stores)
    hold = stores.schedules.hold

    async def _unavailable(*args, **kwargs):
        raise ConnectionError("the database went away")

    monkeypatch.setattr(stores.schedules, "hold", _unavailable)
    failed = await command.run("disable", email="pat@example.com")

    assert "schedules" in failed["surfaces_unconfirmed"] and failed["returncode"] == EXIT_UNCONFIRMED_RUNS
    assert failed["surfaces"]["schedules"]["not_ended"] == 1 and failed["surfaces"]["schedules"]["stopped_after_ms"] is None
    # Recorded before it was acted on, so a re-run can finish it and a restore still knows it.
    assert failed["held"]["schedules"] == [weekly]
    assert await _status(stores, weekly) == "enabled"

    monkeypatch.setattr(stores.schedules, "hold", hold)
    again = await command.run("disable", email="pat@example.com")

    assert again["returncode"] == 0 and await _status(stores, weekly) == "paused"


@pytest.mark.anyio
@pytest.mark.parametrize("launch_beat_the_refusal", [False, True])
async def test_the_hold_stands_through_a_launch_in_flight_during_the_disable(stores, launch_beat_the_refusal) -> None:
    from app.scheduler.service import ScheduledTaskService

    account = await stores.users.create_user(_account())
    weekly = await _schedule(stores, str(account.id), due=datetime.now(UTC) - timedelta(minutes=1))
    command = _command(stores, wait_seconds=1)

    async def _launch_while_the_owner_is_turned_off(**kwargs):
        await command.run("disable", email="pat@example.com")
        if not launch_beat_the_refusal:
            raise OwnerRefusedLaunchError("trusted internal launch owner's account is disabled")
        return {"run_id": "run-before-the-refusal", "thread_id": kwargs["thread_id"]}

    scheduler = ScheduledTaskService(
        task_repo=stores.schedules,
        task_run_repo=stores.occurrences,
        invocation_runtime=CallbackInvocationRuntime(_launch_while_the_owner_is_turned_off),
        poll_interval_seconds=5,
        lease_seconds=120,
        max_concurrent_runs=3,
    )
    await scheduler.run_once(now=datetime.now(UTC))

    assert await _status(stores, weekly) == "paused"
    await command.run("enable", email="pat@example.com")
    await scheduler.run_once(now=datetime.now(UTC) + timedelta(days=8))
    assert await _status(stores, weekly) == "paused"


@pytest.mark.anyio
async def test_an_identity_with_no_account_holds_nothing_and_names_every_surface(stores) -> None:
    document = await _command(stores).run("disable", issuer=ISSUER, subject="sub-nobody")

    assert document["held"] == {"schedules": [], "channel_bindings": []}
    for name in HOLD_SURFACES:
        assert document["surfaces"][name]["count"] == 0 and document["surfaces"][name]["stopped_after_ms"] is not None, name


def test_restore_held_belongs_to_enable(capsys) -> None:
    assert main(["disable", "--issuer", ISSUER, "--subject", "sub-pat", "--restore-held"]) == 1
    assert json.loads(capsys.readouterr().out)["error"] == "--restore-held belongs to enable"
