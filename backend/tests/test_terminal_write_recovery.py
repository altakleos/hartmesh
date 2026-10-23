"""P-ba: real manager/worker/SQL transitions, with offline persistence faults."""

import asyncio
import os
import sqlite3
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest
from sqlalchemy import update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.persistence.run.model import RunChangeClockRow, RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.runs.manager import ConflictError, RunManager
from deerflow.runtime.runs.schemas import RunStatus
from deerflow.runtime.runs.worker import RunContext, run_agent
from deerflow.runtime.stream_bridge.memory import MemoryStreamBridge

SECRET = "private-test-canary-provider-key-and-sql-parameters"


@pytest.fixture(params=["sqlite", "postgres"])
async def repository(tmp_path, request):
    admin = None
    schema = None
    if request.param == "postgres":
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("TEST_POSTGRES_URI is required for PostgreSQL coverage")
        url = make_url(uri).set(drivername="postgresql+asyncpg")
        options = {"ssl": False} if url.query.get("sslmode") == "disable" else {}
        url = url.difference_update_query(["sslmode"])
        schema = "terminal_" + uuid4().hex
        admin = create_async_engine(url, connect_args=options)
        async with admin.begin() as connection:
            await connection.execute(CreateSchema(schema))
        engine = create_async_engine(url, connect_args={**options, "server_settings": {"search_path": schema}})
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/runs.db")
    async with engine.begin() as connection:
        await connection.run_sync(lambda conn: RunRow.metadata.create_all(conn, tables=[RunRow.__table__, RunChangeClockRow.__table__]))
    try:
        yield RunRepository(async_sessionmaker(engine, expire_on_commit=False))
    finally:
        checked_out = engine.sync_engine.pool.checkedout()
        await engine.dispose()
        if admin is not None:
            async with admin.begin() as connection:
                await connection.execute(DropSchema(schema, cascade=True))
            await admin.dispose()
        assert checked_out == 0, "a terminal transaction retained a checked-out connection"


class PersistenceFault:
    """Fail only actual terminal transactions; all reads use the real database."""

    def __init__(self, repository, *, after_commit=False, persistent=False, metadata=False):
        self.factory = repository._sf
        self.after_commit = after_commit
        self.persistent = persistent
        self.metadata = metadata
        self.attempts = 0
        self.failed = False
        self.failure_seen = asyncio.Event()
        repository._sf = self.session

    @asynccontextmanager
    async def session(self):
        async with self.factory() as session:
            execute, commit = session.execute, session.commit
            terminal = False

            async def fault_execute(statement, *args, **kwargs):
                nonlocal terminal
                values = getattr(statement, "_values", None) or {}
                terminal = terminal or (
                    getattr(statement, "is_update", False)
                    and statement.table.name == "runs"
                    and any(getattr(key, "name", key) == "status" and getattr(value, "value", None) in {"success", "error", "timeout", "interrupted"} for key, value in values.items())
                )
                if self.metadata:
                    terminal = terminal or (getattr(statement, "is_update", False) and statement.table.name == "runs" and any(getattr(key, "name", key) == "first_human_message" for key in values))
                if terminal and not self.after_commit and (self.persistent or not self.failed):
                    self.attempts += 1
                    self.failed = True
                    self.failure_seen.set()
                    raise OperationalError("UPDATE private_sql", {"provider_key": SECRET}, sqlite3.OperationalError(SECRET))
                return await execute(statement, *args, **kwargs)

            async def fault_commit():
                if terminal:
                    self.attempts += 1
                await commit()
                if terminal and self.after_commit and not self.failed:
                    self.failed = True
                    raise ConnectionError(SECRET)

            session.execute, session.commit = fault_execute, fault_commit
            yield session


async def run_turn(manager, record, *, agent=None, event_store=None):
    class Agent:
        async def astream(self, *args, **kwargs):
            yield {"messages": []}

    bridge = MemoryStreamBridge()
    await run_agent(bridge, manager, record, ctx=RunContext(checkpointer=None, event_store=event_store), agent_factory=lambda **kwargs: agent or Agent(), graph_input={}, config={})
    frames = []
    async with asyncio.timeout(1):
        async for frame in bridge.subscribe(record.run_id):
            frames.append(("end" if frame.event == "__end__" else frame.event, frame.data))
    return frames


@pytest.mark.anyio
@pytest.mark.parametrize("heartbeat", [False, True])
async def test_rolled_back_terminal_write_recovers_and_does_not_disclose_payload(repository, heartbeat, caplog):
    manager = RunManager(repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=heartbeat))
    record = await manager.create_or_reject("rollback")
    fault = PersistenceFault(repository)

    frames = await run_turn(manager, record)

    row = await repository.get(record.run_id)
    assert row["status"] == "success"
    assert row["stop_reason"] is None
    assert record.status == RunStatus.success
    assert frames[-1] == ("end", None)
    assert fault.attempts == 2
    assert "outcome=rolled_back" in caplog.text
    assert SECRET not in caplog.text and "private_sql" not in caplog.text
    assert not any(item.exc_info for item in caplog.records if "terminal" in item.message.lower())
    assert (await manager.create_or_reject(record.thread_id)).run_id != record.run_id


@pytest.mark.anyio
@pytest.mark.parametrize("heartbeat", [False, True])
async def test_connection_lost_after_commit_is_read_back_without_replay(repository, heartbeat, caplog):
    manager = RunManager(repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=heartbeat))
    record = await manager.create_or_reject("ambiguous")
    fault = PersistenceFault(repository, after_commit=True)

    frames = await run_turn(manager, record)

    assert (await repository.get(record.run_id))["status"] == "success"
    assert record.status == RunStatus.success
    assert fault.attempts == 1, "a committed transition must never be replayed"
    assert "outcome=unknown" in caplog.text
    assert SECRET not in caplog.text
    assert frames[-1] == ("end", None)
    assert (await manager.create_or_reject(record.thread_id)).run_id != record.run_id


@pytest.mark.anyio
@pytest.mark.parametrize("heartbeat", [False, True])
async def test_validation_refusal_before_sql_has_a_sanitized_recovery(repository, heartbeat, monkeypatch, caplog):
    manager = RunManager(repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=heartbeat))
    record = await manager.create_or_reject("validation")
    original = repository._terminal_values
    calls = 0

    def validate(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError(SECRET)
        return original(*args)

    monkeypatch.setattr(repository, "_terminal_values", validate)
    frames = await run_turn(manager, record)
    row = await repository.get(record.run_id)
    assert row["status"] == "success" and row["stop_reason"] is None
    assert "outcome=validation_refused" in caplog.text and "exception_class=validation_error" in caplog.text
    assert SECRET not in caplog.text
    assert frames[-1] == ("end", None)
    assert (await manager.create_or_reject(record.thread_id)).run_id != record.run_id


@pytest.mark.anyio
async def test_outage_ends_stream_unconfirmed_and_restart_recovers_without_heartbeat(repository, caplog):
    manager = RunManager(repository)
    record = await manager.create_or_reject("outage")
    fault = PersistenceFault(repository, persistent=True)
    frames = await asyncio.wait_for(run_turn(manager, record), timeout=10)
    assert (await repository.get(record.run_id))["status"] == "running"
    assert record.status == RunStatus.error and record.ownership_lost
    assert not record.terminal_confirmed
    assert fault.attempts == 3
    assert any(event == "error" and data.get("reason") == "run_state_unconfirmed" for event, data in frames)
    assert record.terminal_write_unconfirmed
    assert frames[-1] == ("end", None)
    assert SECRET not in caplog.text
    with pytest.raises(ConflictError):
        await manager.create_or_reject(record.thread_id)

    repository._sf = fault.factory
    replacement = RunManager(repository)
    recovered = await replacement.reconcile_orphaned_inflight_runs(error="Gateway restarted before confirmation", stop_reason="orphan_recovered")
    assert [row.run_id for row in recovered] == [record.run_id]
    assert (await repository.get(record.run_id))["status"] == "error"
    assert (await replacement.create_or_reject(record.thread_id)).run_id != record.run_id


@pytest.mark.anyio
async def test_persistence_restored_inside_recovery_window_settles_without_restart(repository):
    manager = RunManager(repository)
    record = await manager.create_or_reject("restored")
    fault = PersistenceFault(repository, persistent=True)
    turn = asyncio.create_task(run_turn(manager, record))
    try:
        await asyncio.wait_for(fault.failure_seen.wait(), timeout=5)
        fault.persistent = False
        frames = await asyncio.wait_for(turn, timeout=5)
    finally:
        if not turn.done():
            turn.cancel()
        await asyncio.gather(turn, return_exceptions=True)
    assert (await repository.get(record.run_id))["status"] == "success"
    assert record.terminal_confirmed and not record.ownership_lost
    assert frames[-1] == ("end", None)
    await manager.create_or_reject(record.thread_id)


@pytest.mark.anyio
async def test_final_conditional_rejection_cannot_narrate_onto_new_owners_stream(repository, monkeypatch):
    from deerflow.runtime.runs.terminal import TerminalRecoveryPolicy

    manager = RunManager(repository, terminal_recovery_policy=TerminalRecoveryPolicy(max_attempts=1))
    record = await manager.create_or_reject("final-cas-rejection")
    original = repository.finalize_owned

    async def takeover_then_finalize(run_id, **kwargs):
        async with repository._sf() as session:
            await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(owner_worker_id="new-owner", change_seq=RunRow.change_seq + 1))
            await session.commit()
        return await original(run_id, **kwargs)

    monkeypatch.setattr(repository, "finalize_owned", takeover_then_finalize)
    frames = await run_turn(manager, record)
    row = await repository.get(record.run_id)
    assert row["owner_worker_id"] == "new-owner" and row["status"] == "running"
    assert record.ownership_lost and not record.terminal_write_unconfirmed
    assert not any(event == "error" for event, data in frames)
    assert frames[-1] == ("end", None)


@pytest.mark.anyio
async def test_expired_lease_during_final_cleanup_suppresses_unconfirmed_narrative(repository, monkeypatch):
    from datetime import UTC, datetime, timedelta

    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    manager = RunManager(repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True))
    record = await manager.create_or_reject("cleanup-lease-expiry")
    fault = PersistenceFault(repository, persistent=True)
    original = manager.set_finalizing
    expired = False

    async def expire_during_cleanup(run_id, finalizing):
        nonlocal expired
        await original(run_id, finalizing)
        if not finalizing and record.terminal_write_unconfirmed:
            # Recovery was allowed to narrate when it fenced local execution,
            # but the finalizing barrier drains before stream publication.
            assert manager._parse_lease_deadline(record.lease_expires_at) > datetime.now(UTC)
            record.lease_expires_at = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
            expired = True

    monkeypatch.setattr(manager, "set_finalizing", expire_during_cleanup)
    frames = await run_turn(manager, record, event_store=MemoryRunEventStore())
    assert expired and fault.attempts == 3
    assert record.ownership_lost and not record.terminal_write_unconfirmed
    assert not any(event == "error" for event, data in frames)
    assert frames[-1] == ("end", None)
    assert (await repository.get(record.run_id))["status"] == "running"


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["owner", "version", "lease"])
async def test_fenced_worker_cannot_change_a_new_owner_or_version(repository, change, monkeypatch, caplog):
    heartbeat = change == "lease"
    manager = RunManager(repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=heartbeat))
    record = await manager.create_or_reject("fenced")
    original = repository.finalize_owned
    entered, release = asyncio.Event(), asyncio.Event()

    async def pause(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(repository, "finalize_owned", pause)
    turn = asyncio.create_task(run_turn(manager, record))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        async with repository._sf() as session:
            changes = {"change_seq": RunRow.change_seq + 1} if change == "version" else {}
            if change == "owner":
                changes["owner_worker_id"] = "new-owner"
            if change == "lease":
                from datetime import UTC, datetime, timedelta

                changes["lease_expires_at"] = datetime.now(UTC) - timedelta(seconds=1)
            await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(**changes))
            await session.commit()
        expected = await repository.get(record.run_id)
        release.set()
        frames = await asyncio.wait_for(turn, timeout=5)
    finally:
        release.set()
        if not turn.done():
            turn.cancel()
        await asyncio.gather(turn, return_exceptions=True)
    assert await repository.get(record.run_id) == expected
    assert record.ownership_lost and not record.terminal_confirmed
    assert f"outcome=stale_{change}" in caplog.text if change != "lease" else "outcome=lease_expired" in caplog.text
    assert not any(event == "error" and data.get("reason") == "run_state_unconfirmed" for event, data in frames)
    assert not record.terminal_write_unconfirmed
    assert frames[-1] == ("end", None)
    with pytest.raises(ConflictError):
        await manager.create_or_reject(record.thread_id)


@pytest.mark.anyio
@pytest.mark.parametrize("heartbeat", [False, True])
@pytest.mark.parametrize("action,expected", [("interrupt", "interrupted"), ("rollback", "error")])
async def test_durable_cancellation_wins_during_terminal_failure_recovery(repository, heartbeat, action, expected):
    manager = RunManager(repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=heartbeat))
    record = await manager.create_or_reject("cancel-race")
    fault = PersistenceFault(repository)
    turn = asyncio.create_task(run_turn(manager, record))
    try:
        await asyncio.wait_for(fault.failure_seen.wait(), timeout=5)
        await repository.request_cancel(record.run_id, action=action)
        frames = await asyncio.wait_for(turn, timeout=5)
    finally:
        if not turn.done():
            turn.cancel()
        await asyncio.gather(turn, return_exceptions=True)
    row = await repository.get(record.run_id)
    assert row["status"] == expected
    assert row["cancel_action"] == action
    assert record.abort_action == action and record.abort_event.is_set()
    assert record.terminal_confirmed and not record.ownership_lost
    assert frames[-1] == ("end", None)
    await manager.create_or_reject(record.thread_id)


@pytest.mark.anyio
async def test_capacity_stop_reason_remains_accepted(repository):
    manager = RunManager(repository)
    record = await manager.create_or_reject("capacity")
    await manager.try_start(record.run_id)
    await manager.set_status_if_not_cancelled(record.run_id, RunStatus.success, stop_reason="sandbox_capacity_exceeded")
    row = await repository.get(record.run_id)
    assert row["status"] == "success" and row["stop_reason"] == "sandbox_capacity_exceeded"
    await manager.create_or_reject(record.thread_id)


@pytest.mark.anyio
async def test_late_completion_metadata_cannot_recreate_or_change_a_new_owners_row(repository):
    manager = RunManager(repository)
    record = await manager.create_or_reject("completion-fence")
    await manager.try_start(record.run_id)
    await manager.set_status_if_not_cancelled(record.run_id, RunStatus.success)
    async with repository._sf() as session:
        await session.execute(update(RunRow).where(RunRow.run_id == record.run_id).values(owner_worker_id="new-owner", total_tokens=42))
        await session.commit()
    expected = await repository.get(record.run_id)
    await manager.update_run_completion(record.run_id, status="success", total_tokens=999)
    assert await repository.get(record.run_id) == expected
    assert record.ownership_lost


@pytest.mark.anyio
async def test_receipt_ordering_and_terminal_recovery_preserve_the_existing_delivery(repository):
    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    events = MemoryRunEventStore()
    manager = RunManager(repository, event_store=events)
    record = await manager.create_or_reject("receipt")
    PersistenceFault(repository)
    frames = await run_turn(manager, record, event_store=events)
    row = await repository.get(record.run_id)
    assert row["status"] == "success" and record.terminal_confirmed
    deliveries = [item for item in await events.list_events(record.thread_id, record.run_id) if item["event_type"] == "run.delivery"]
    assert len(deliveries) == 1
    assert deliveries[0]["content"] == {"presented": 0, "paths": [], "by_tool": {}}
    assert not any(event == "error" for event, data in frames)
    assert frames[-1] == ("end", None)


@pytest.mark.anyio
@pytest.mark.parametrize("extra_cancels", [0, 2])
async def test_timeout_and_repeated_cancellation_drain_owned_sql_close(repository, extra_cancels):
    from deerflow.runtime.runs.terminal import TerminalRecoveryPolicy

    manager = RunManager(repository, terminal_recovery_policy=TerminalRecoveryPolicy(attempt_timeout=1, retry_delay=0))
    record = await manager.create_or_reject("drain-close")
    await manager.try_start(record.run_id)
    factory = repository._sf
    closing, release = asyncio.Event(), asyncio.Event()
    opened = 0

    @asynccontextmanager
    async def blocking_session():
        nonlocal opened
        opened += 1
        async with factory() as session:
            execute, close = session.execute, session.close
            terminal = False

            async def track(statement, *args, **kwargs):
                nonlocal terminal
                terminal = terminal or (getattr(statement, "is_update", False) and statement.table.name == "runs")
                return await execute(statement, *args, **kwargs)

            async def blocked_close():
                if terminal:
                    closing.set()
                    await release.wait()
                await close()

            session.execute, session.close = track, blocked_close
            yield session

    repository._sf = blocking_session
    transition = asyncio.create_task(manager.set_status_if_not_cancelled(record.run_id, RunStatus.success))
    try:
        await asyncio.wait_for(closing.wait(), timeout=5)
        await asyncio.sleep(1.05)  # Let the close-only attempt timeout fire.
        for _ in range(extra_cancels):
            transition.cancel()
            await asyncio.sleep(0)
        assert not transition.done(), "neither the deadline nor repeated cancellation may detach close"
        assert opened == 2, "no readback/retry is authorized while the old session is closing"
        release.set()
        result = await asyncio.wait_for(asyncio.gather(transition, return_exceptions=True), timeout=5)
        assert isinstance(result[0], asyncio.CancelledError) if extra_cancels else result == [None]
        assert (await repository.get(record.run_id))["status"] == "success"
    finally:
        release.set()
        if not transition.done():
            transition.cancel()
        await asyncio.gather(transition, return_exceptions=True)
        repository._sf = factory


@pytest.mark.anyio
async def test_staged_finalizing_run_keeps_its_lease_until_terminal_confirmation(repository):
    manager = RunManager(repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True))
    record = await manager.create_or_reject("staged-lease")
    await manager.try_start(record.run_id)
    before = (await repository.get(record.run_id))["lease_expires_at"]
    await manager.set_status(record.run_id, RunStatus.success, persist=False)
    assert record.finalizing and not record.terminal_confirmed
    await manager._renew_leases()
    assert (await repository.get(record.run_id))["lease_expires_at"] > before
    assert not record.ownership_lost


@pytest.mark.anyio
async def test_lost_ownership_during_terminal_finally_still_ends_the_stream(repository, monkeypatch):
    from deerflow.runtime.events.store.memory import MemoryRunEventStore

    manager = RunManager(repository)
    record = await manager.create_or_reject("fenced-finally")
    entered = asyncio.Event()

    async def blocked(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(repository, "finalize_owned", blocked)
    bridge = MemoryStreamBridge()

    class Agent:
        async def astream(self, *args, **kwargs):
            yield {"messages": []}

    task = asyncio.create_task(run_agent(bridge, manager, record, ctx=RunContext(checkpointer=None, event_store=MemoryRunEventStore()), agent_factory=lambda **kwargs: Agent(), graph_input={}, config={}))
    record.task = task
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        await manager._mark_ownership_lost(record, reason="Test ownership rejection", require_active=False)
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), timeout=5)
        async with asyncio.timeout(1):
            frames = [frame async for frame in bridge.subscribe(record.run_id)]
        assert frames[-1].event == "__end__"
        assert not any(frame.event == "error" and frame.data.get("reason") == "run_state_unconfirmed" for frame in frames)
        assert not record.finalizing
        assert (await repository.get(record.run_id))["status"] == "running"
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_recovered_success_preserves_successful_edit_checkpoint(repository, monkeypatch):
    from langchain_core.messages import AIMessage, HumanMessage
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.graph import MessagesState, StateGraph

    from deerflow.runtime.checkpoint_state import CheckpointStateAccessor
    from deerflow.runtime.events.store.memory import MemoryRunEventStore
    from deerflow.runtime.runs import worker

    checkpointer = InMemorySaver()

    async def answer(state):
        return {"messages": [AIMessage("replacement", id="replacement")]}

    builder = StateGraph(MessagesState)
    builder.add_node("answer", answer)
    builder.set_entry_point("answer")
    builder.set_finish_point("answer")
    graph = builder.compile(checkpointer=checkpointer)
    config = {"configurable": {"thread_id": "edit-recovery"}}
    original_messages = [HumanMessage("original", id="human"), AIMessage("before", id="before")]
    accessor = CheckpointStateAccessor.bind(graph, checkpointer, mode="full")
    await accessor.aupdate(config, {"messages": original_messages}, as_node="answer")
    manager = RunManager(repository)
    record = await manager.create_or_reject("edit-recovery", metadata={"replay_kind": "edit", "regenerate_from_run_id": "source"})
    PersistenceFault(repository)
    original_rollback = worker._rollback_to_pre_run_checkpoint
    observed = []

    async def rollback(**kwargs):
        observed.append((await repository.get(record.run_id))["status"])
        with pytest.raises(ConflictError):
            await RunManager(repository).create_or_reject(record.thread_id)
        return await original_rollback(**kwargs)

    monkeypatch.setattr(worker, "_rollback_to_pre_run_checkpoint", rollback)
    bridge = MemoryStreamBridge()
    await run_agent(bridge, manager, record, ctx=RunContext(checkpointer=checkpointer, event_store=MemoryRunEventStore()), agent_factory=lambda **kwargs: graph, graph_input={"messages": [HumanMessage("edited", id="human")]}, config=config)
    state = await accessor.aget(config)
    assert [message.content for message in state.values["messages"]] == ["edited", "before", "replacement"]
    assert observed == []
    assert record.status == RunStatus.success and record.terminal_confirmed
    assert (await repository.get(record.run_id))["stop_reason"] is None
    async with asyncio.timeout(1):
        frames = [frame async for frame in bridge.subscribe(record.run_id)]
    assert frames[-1].event == "__end__"
    await manager.create_or_reject(record.thread_id)


@pytest.mark.anyio
async def test_shutdown_drains_a_staged_unconfirmed_terminal_task(repository, monkeypatch):
    manager = RunManager(repository)
    record = await manager.create_or_reject("shutdown-finalizing")
    await manager.try_start(record.run_id)
    await manager.set_status(record.run_id, RunStatus.success, persist=False)
    entered, release, stopping = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def finalize():
        entered.set()
        await release.wait()
        await manager.persist_current_status(record.run_id)

    async def stop_heartbeat(**kwargs):
        stopping.set()

    monkeypatch.setattr(manager, "stop_heartbeat", stop_heartbeat)
    record.task = asyncio.create_task(finalize())
    shutdown = None
    try:
        await entered.wait()
        shutdown = asyncio.create_task(manager.shutdown(timeout=5))
        await stopping.wait()
        await asyncio.sleep(0)
        assert not shutdown.done(), "shutdown must retain the staged worker's persistence resources"
        assert not record.abort_event.is_set() and record.task.cancelling() == 0
        release.set()
        await asyncio.wait_for(asyncio.gather(record.task, shutdown), timeout=5)
        assert record.status == RunStatus.success and record.terminal_confirmed
        assert (await repository.get(record.run_id))["status"] == "success"
    finally:
        release.set()
        await asyncio.gather(record.task, *([shutdown] if shutdown else []), return_exceptions=True)


@pytest.mark.anyio
async def test_original_commit_winning_after_recovery_read_is_adopted_without_another_write(repository, monkeypatch):
    from deerflow.runtime.runs.terminal import TerminalWriteFailure

    manager = RunManager(repository)
    record = await manager.create_or_reject("late-commit")
    original = repository.finalize_owned
    first = None
    commits = []

    async def ambiguous(run_id, **kwargs):
        nonlocal first
        if first is None:
            first = kwargs
            raise TerminalWriteFailure("unknown", "transport_error")
        # The original transaction commits after recovery read active state.
        # Recovery must lose its CAS and confirm that commit on the next read.
        commits.append(await original(run_id, **first))
        result = await original(run_id, **kwargs)
        commits.append(result)
        return result

    monkeypatch.setattr(repository, "finalize_owned", ambiguous)
    frames = await run_turn(manager, record)
    assert commits == [True, False]
    assert record.status == RunStatus.success and record.terminal_confirmed
    assert (await repository.get(record.run_id))["status"] == "success"
    assert frames[-1] == ("end", None)
    await manager.create_or_reject(record.thread_id)


@pytest.mark.anyio
@pytest.mark.parametrize("action,expected", [("interrupt", "interrupted"), ("rollback", "error")])
async def test_cancellation_arriving_after_recovery_read_defeats_the_cas(repository, monkeypatch, action, expected):
    from deerflow.runtime.runs.terminal import TerminalWriteFailure

    manager = RunManager(repository)
    record = await manager.create_or_reject("late-cancel")
    original = repository.finalize_owned
    attempts = 0

    async def cancel_before_recovery_write(run_id, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise TerminalWriteFailure("rolled_back", "database_error")
        if attempts == 2:
            await repository.request_cancel(run_id, action=action)
        return await original(run_id, **kwargs)

    monkeypatch.setattr(repository, "finalize_owned", cancel_before_recovery_write)
    frames = await run_turn(manager, record)
    assert (await repository.get(record.run_id))["status"] == expected
    assert record.terminal_confirmed and record.abort_action == action
    assert frames[-1] == ("end", None)
    await manager.create_or_reject(record.thread_id)


@pytest.mark.anyio
async def test_invalid_metadata_is_repaired_without_changing_executed_status(repository):
    manager = RunManager(repository)
    record = await manager.create_or_reject("bad-metadata")
    await manager.try_start(record.run_id)
    await manager.set_status_if_not_cancelled(record.run_id, RunStatus.error, error={"secret": SECRET}, stop_reason="x" * 51)
    row = await repository.get(record.run_id)
    assert row["status"] == "error"
    assert row["stop_reason"] == "terminal_metadata_recovered"
    assert SECRET not in row["error"]
    assert record.terminal_confirmed


@pytest.mark.anyio
async def test_ambiguous_terminal_commit_can_be_confirmed_after_its_old_lease_expires(repository, monkeypatch):
    from datetime import UTC, datetime, timedelta

    manager = RunManager(repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=True))
    record = await manager.create_or_reject("terminal-expired-lease")
    original = repository.finalize_owned
    attempts = 0

    async def late_ack(run_id, **kwargs):
        nonlocal attempts
        attempts += 1
        await original(run_id, **kwargs)
        async with repository._sf() as session:
            await session.execute(update(RunRow).where(RunRow.run_id == run_id).values(lease_expires_at=datetime.now(UTC) - timedelta(seconds=1)))
            await session.commit()
        raise ConnectionError(SECRET)

    monkeypatch.setattr(repository, "finalize_owned", late_ack)
    frames = await run_turn(manager, record)
    assert record.status == RunStatus.success and record.terminal_confirmed
    assert not record.ownership_lost and attempts == 1
    assert frames[-1] == ("end", None)


@pytest.mark.anyio
async def test_missing_terminal_row_is_fenced_without_recreation(repository):
    manager = RunManager(repository)
    record = await manager.create_or_reject("missing")
    await manager.try_start(record.run_id)
    await repository.delete(record.run_id)
    await manager.set_status_if_not_cancelled(record.run_id, RunStatus.success)
    assert await repository.get(record.run_id) is None
    assert record.ownership_lost and not record.terminal_confirmed


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["progress", "finalizing_progress", "completion"])
async def test_progress_and_completion_write_diagnostics_never_disclose_sql_or_messages(repository, phase, caplog):
    manager = RunManager(repository)
    record = await manager.create_or_reject("metadata-logging")
    await manager.try_start(record.run_id)
    if phase == "finalizing_progress":
        await manager.set_status(record.run_id, RunStatus.success, persist=False)
    elif phase == "completion":
        await manager.set_status_if_not_cancelled(record.run_id, RunStatus.success)
    fault = PersistenceFault(repository, metadata=True)
    if phase == "progress":
        await manager.update_run_progress(record.run_id, first_human_message=SECRET)
    elif phase == "finalizing_progress":
        await manager.update_finalizing_progress(record.run_id, first_human_message=SECRET)
    else:
        await manager.update_run_completion(record.run_id, status="success", first_human_message=SECRET)
    assert fault.failed
    assert f"operation={phase}" in caplog.text
    assert "exception_class=database_error" in caplog.text
    assert "reason=run_metadata_write_failed" in caplog.text
    assert SECRET not in caplog.text and "private_sql" not in caplog.text
    assert not any(item.exc_info for item in caplog.records if "persistence failed" in item.message)


@pytest.mark.anyio
@pytest.mark.parametrize("heartbeat", [False, True])
async def test_live_worker_rollback_outcome_is_not_overwritten_by_cancel_acknowledgement(repository, heartbeat, monkeypatch):
    from deerflow.runtime.events.store.memory import MemoryRunEventStore
    from deerflow.runtime.runs import worker

    manager = RunManager(repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=heartbeat))
    record = await manager.create_or_reject("local-rollback-race")
    streaming, rolling_back, finish_rollback = asyncio.Event(), asyncio.Event(), asyncio.Event()

    class Agent:
        async def astream(self, *args, **kwargs):
            streaming.set()
            await asyncio.Event().wait()
            yield {"messages": []}

    async def rollback(**kwargs):
        rolling_back.set()
        await finish_rollback.wait()
        return True

    original_persist = manager._persist_status

    async def delayed_ack(record, status, **kwargs):
        if status == RunStatus.interrupted:
            # Force the cancel caller's acknowledgement behind the worker's
            # staged rollback result, while checkpoint restoration is draining.
            await rolling_back.wait()
        return await original_persist(record, status, **kwargs)

    monkeypatch.setattr(worker, "_rollback_to_pre_run_checkpoint", rollback)
    monkeypatch.setattr(manager, "_persist_status", delayed_ack)
    turn = asyncio.create_task(run_turn(manager, record, agent=Agent(), event_store=MemoryRunEventStore()))
    record.task = turn
    try:
        await asyncio.wait_for(streaming.wait(), timeout=5)
        await asyncio.wait_for(manager.cancel(record.run_id, action="rollback"), timeout=5)
        await asyncio.wait_for(rolling_back.wait(), timeout=5)
        assert (await repository.get(record.run_id))["status"] == "running", "checkpoint cleanup must retain the active durable fence"
        finish_rollback.set()
        frames = await asyncio.wait_for(turn, timeout=5)
    finally:
        finish_rollback.set()
        if not turn.done():
            turn.cancel()
        await asyncio.gather(turn, return_exceptions=True)
    row = await repository.get(record.run_id)
    assert row["status"] == "error" and row["error"] == "Rolled back by user"
    assert row["cancel_action"] == "rollback"
    assert record.status == RunStatus.error and record.terminal_confirmed and not record.ownership_lost
    assert frames[-1] == ("end", None)
    await manager.create_or_reject(record.thread_id)
