from __future__ import annotations

import asyncio
import os
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from support.postgres import postgres_async_url

from deerflow.persistence.base import Base
from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.events.appender import (
    AdministrativeRunEventAppender,
    FencedRunEventAppender,
    RuntimeEventAuthority,
    RuntimeEventOwnershipLost,
)
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.store.base import LifecycleTransition, LifecycleType
from deerflow.runtime.runs.store.memory import MemoryRunStore

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")


class _OwnedRunStore:
    def __init__(self) -> None:
        self.row = {
            "run_id": "run-1",
            "thread_id": "thread-1",
            "operation_kind": "run",
            "status": "running",
            "owner_worker_id": "worker-1",
            "state_version": 3,
            "lease_expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
            "tenant_digest": None,
        }

    async def authoritative_get(self, run_id: str) -> dict | None:
        return dict(self.row) if run_id == self.row["run_id"] else None

    @asynccontextmanager
    async def hold_execution_fence(
        self,
        run_id: str,
        *,
        owner_worker_id: str,
        state_version: int,
        terminal_state_version: int | None = None,
        allowed_active_statuses: tuple[str, ...] = ("running",),
    ):
        del terminal_state_version
        yield bool(run_id == self.row["run_id"] and owner_worker_id == self.row["owner_worker_id"] and state_version == self.row["state_version"] and self.row["status"] in allowed_active_statuses)


def _authority() -> RuntimeEventAuthority:
    return RuntimeEventAuthority(
        tenant=None,
        thread_id="thread-1",
        run_id="run-1",
        owner_id="worker-1",
        lease_epoch=3,
    )


@pytest.mark.anyio
async def test_runtime_appender_rejects_a_stale_writer_without_appending() -> None:
    runs = _OwnedRunStore()
    store = MemoryRunEventStore(run_store=runs)
    appender = FencedRunEventAppender(store, _authority())

    await appender.put(event_type="run.start", category="trace", content={"chain": "lead"})
    runs.row["owner_worker_id"] = "worker-2"
    runs.row["state_version"] = 4

    with pytest.raises(RuntimeEventOwnershipLost, match="runtime_event_ownership_lost"):
        await appender.put_batch(
            [
                {
                    "thread_id": "thread-1",
                    "run_id": "run-1",
                    "event_type": "subagent.step",
                    "category": "subagent",
                    "content": {"task_id": "task-1"},
                }
            ]
        )

    events = await store.list_events("thread-1", "run-1")
    assert [event["event_type"] for event in events] == ["run.start"]


@pytest.mark.anyio
async def test_pending_runtime_receipt_is_epoch_fenced_across_takeover() -> None:
    """Pre-start failures cannot install a singleton after ownership moves."""

    runs = _OwnedRunStore()
    runs.row["status"] = "pending"
    store = MemoryRunEventStore(run_store=runs)
    stale = FencedRunEventAppender(store, _authority())

    runs.row.update(owner_worker_id="worker-2", state_version=4)
    with pytest.raises(RuntimeEventOwnershipLost, match="runtime_event_ownership_lost"):
        await stale.put_if_absent(
            event_type="run.delivery",
            category="outputs",
            content={"presented": 0, "paths": [], "by_tool": {}},
        )

    winner = FencedRunEventAppender(
        store,
        RuntimeEventAuthority(
            tenant=None,
            thread_id="thread-1",
            run_id="run-1",
            owner_id="worker-2",
            lease_epoch=4,
        ),
    )
    event, created = await winner.put_if_absent(
        event_type="run.delivery",
        category="outputs",
        content={
            "presented": 1,
            "paths": ["/mnt/user-data/outputs/report.md"],
            "by_tool": {"present_files": ["/mnt/user-data/outputs/report.md"]},
        },
    )

    assert created is True
    assert event["content"]["presented"] == 1


@pytest.mark.anyio
async def test_runtime_appender_binds_every_event_to_one_thread_and_run() -> None:
    runs = _OwnedRunStore()
    store = MemoryRunEventStore(run_store=runs)
    appender = FencedRunEventAppender(store, _authority())

    with pytest.raises(ValueError, match="authority"):
        await appender.put_batch(
            [
                {
                    "thread_id": "another-thread",
                    "run_id": "run-1",
                    "event_type": "run.start",
                    "category": "trace",
                }
            ]
        )
    assert await store.list_events("thread-1", "run-1") == []


@pytest.mark.anyio
async def test_process_local_runtime_authority_requires_an_explicit_validator() -> None:
    store = MemoryRunEventStore()
    current = True

    async def validate(authority: RuntimeEventAuthority) -> bool:
        assert authority == _authority()
        return current

    appender = FencedRunEventAppender(
        store,
        _authority(),
        process_local_validator=validate,
    )
    await appender.put(event_type="run.start", category="trace")
    current = False

    with pytest.raises(RuntimeEventOwnershipLost, match="runtime_event_ownership_lost"):
        await appender.put(event_type="run.end", category="outputs")

    assert [event["event_type"] for event in await store.list_events("thread-1", "run-1")] == ["run.start"]


@pytest.mark.anyio
async def test_administrative_appender_is_an_explicit_unfenced_recovery_path() -> None:
    runs = _OwnedRunStore()
    store = MemoryRunEventStore(run_store=runs)
    runs.row.update(status="error", owner_worker_id=None, lease_expires_at=None, state_version=4)

    appender = AdministrativeRunEventAppender(store)
    event, created = await appender.put_if_absent(
        thread_id="thread-1",
        run_id="run-1",
        event_type="run.delivery",
        category="outputs",
        content={"presented": 0, "paths": [], "by_tool": {}},
    )

    assert created is True
    assert event["event_type"] == "run.delivery"


@pytest.mark.anyio
async def test_database_runtime_append_checks_owner_and_epoch_in_the_write_transaction(tmp_path) -> None:
    from deerflow.runtime.events.store.db import DbRunEventStore

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'events.db'}")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        async with session_factory.begin() as session:
            session.add(
                RunRow(
                    run_id="run-1",
                    thread_id="thread-1",
                    operation_kind="run",
                    status="running",
                    owner_worker_id="worker-1",
                    state_version=3,
                    lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
                )
            )
        store = DbRunEventStore(session_factory)
        appender = FencedRunEventAppender(store, _authority())
        await appender.put(event_type="run.start", category="trace")

        async with session_factory.begin() as session:
            row = await session.get(RunRow, "run-1")
            assert row is not None
            row.owner_worker_id = "worker-2"
            row.state_version = 4

        with pytest.raises(RuntimeEventOwnershipLost, match="runtime_event_ownership_lost"):
            await appender.put(event_type="run.end", category="outputs")
        assert [event["event_type"] for event in await store.list_events("thread-1", "run-1", user_id=None)] == ["run.start"]
    finally:
        await engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("append_kind", ["batch", "singleton"])
async def test_database_runtime_append_rejects_a_database_expired_lease_when_process_clock_lags(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    append_kind: str,
) -> None:
    from deerflow.runtime.events.store import db as db_event_store

    class _LaggingProcessDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            observed = datetime(1999, 1, 1, tzinfo=UTC)
            return observed if tz is not None else observed.replace(tzinfo=None)

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'database-clock-events.db'}")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        async with session_factory.begin() as session:
            session.add(
                RunRow(
                    run_id="run-1",
                    thread_id="thread-1",
                    operation_kind="run",
                    status="running",
                    owner_worker_id="worker-1",
                    state_version=3,
                    lease_expires_at=datetime(2000, 1, 1, tzinfo=UTC),
                )
            )
        monkeypatch.setattr(db_event_store, "datetime", _LaggingProcessDateTime)
        store = db_event_store.DbRunEventStore(session_factory)
        appender = FencedRunEventAppender(store, _authority())

        with pytest.raises(RuntimeEventOwnershipLost, match="runtime_event_ownership_lost"):
            if append_kind == "batch":
                await appender.put(event_type="run.start", category="trace")
            else:
                await appender.put_if_absent(event_type="run.delivery", category="outputs")

        assert await store.list_events("thread-1", "run-1", user_id=None) == []
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_database_runtime_append_accepts_a_single_node_null_lease(tmp_path) -> None:
    from deerflow.runtime.events.store.db import DbRunEventStore

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'null-lease-events.db'}")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        async with session_factory.begin() as session:
            session.add(
                RunRow(
                    run_id="run-1",
                    thread_id="thread-1",
                    operation_kind="run",
                    status="running",
                    owner_worker_id="worker-1",
                    state_version=3,
                    lease_expires_at=None,
                )
            )
        store = DbRunEventStore(session_factory)
        event = await FencedRunEventAppender(store, _authority()).put(
            event_type="run.start",
            category="trace",
        )

        assert event["event_type"] == "run.start"
        assert [record["event_type"] for record in await store.list_events("thread-1", "run-1", user_id=None)] == ["run.start"]
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_jsonl_runtime_append_rejects_stale_authority(tmp_path) -> None:
    from deerflow.runtime.events.store.jsonl import JsonlRunEventStore

    runs = _OwnedRunStore()
    store = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    appender = FencedRunEventAppender(store, _authority())
    await appender.put(event_type="run.start", category="trace")

    runs.row["state_version"] = 4
    with pytest.raises(RuntimeEventOwnershipLost, match="runtime_event_ownership_lost"):
        await appender.put(event_type="run.end", category="outputs")

    assert [event["event_type"] for event in await store.list_events("thread-1", "run-1")] == ["run.start"]


@pytest.mark.anyio
async def test_jsonl_runtime_append_with_real_memory_store_serializes_ownership(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deerflow.runtime.events.store.jsonl import JsonlRunEventStore

    runs = MemoryRunStore()
    admitted, _ = await runs.create_thread_operation_atomic(
        "run-1",
        thread_id="thread-1",
        owner_worker_id="worker-1",
        lease_expires_at=None,
    )
    started = await runs.transition_owned_run_atomic(
        "run-1",
        expected_state_version=admitted["state_version"],
        expected_statuses=("pending",),
        transition=LifecycleTransition(
            lifecycle_type=LifecycleType.started,
            status="running",
        ),
        expected_owner_worker_id="worker-1",
        require_unexpired_lease=False,
    )
    assert started.applied
    assert started.row is not None
    authority = RuntimeEventAuthority(
        tenant=None,
        thread_id="thread-1",
        run_id="run-1",
        owner_id="worker-1",
        lease_epoch=started.row["state_version"],
    )
    store = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    appender = FencedRunEventAppender(store, authority)
    original_commit = store._commit_prepared_record
    commit_started = threading.Event()
    allow_commit = threading.Event()

    def blocked_commit(target, temp_path) -> None:
        commit_started.set()
        assert allow_commit.wait(timeout=5)
        original_commit(target, temp_path)

    monkeypatch.setattr(store, "_commit_prepared_record", blocked_commit)
    append_task = asyncio.create_task(
        appender.put(event_type="run.start", category="trace"),
    )
    assert await asyncio.to_thread(commit_started.wait, 2)
    cancellation_task = asyncio.create_task(
        runs.request_cancel_fenced(
            "run-1",
            action="interrupt",
            expected_state_version=authority.lease_epoch,
        )
    )
    await asyncio.sleep(0.05)
    assert cancellation_task.done() is False

    allow_commit.set()
    await append_task
    cancellation = await cancellation_task
    assert cancellation.row is not None
    assert cancellation.row["state_version"] == authority.lease_epoch + 1
    with pytest.raises(RuntimeEventOwnershipLost, match="runtime_event_ownership_lost"):
        await appender.put(event_type="run.end", category="outputs")
    assert [event["event_type"] for event in await store.list_events("thread-1", "run-1")] == ["run.start"]


@pytest.mark.anyio
async def test_jsonl_runtime_append_with_sql_store_accepts_single_node_null_lease(
    tmp_path,
) -> None:
    from deerflow.runtime.events.store.jsonl import JsonlRunEventStore

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'runs.db'}")
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    try:
        runs = RunRepository(session_factory)
        await runs.put(
            "run-1",
            thread_id="thread-1",
            owner_worker_id="worker-1",
            lease_expires_at=None,
            status="running",
        )
        row = await runs.get("run-1")
        assert row is not None
        authority = RuntimeEventAuthority(
            tenant=None,
            thread_id="thread-1",
            run_id="run-1",
            owner_id="worker-1",
            lease_epoch=row["state_version"],
        )
        store = JsonlRunEventStore(
            base_dir=tmp_path / "events",
            run_store=runs,
        )
        appender = FencedRunEventAppender(store, authority)

        event = await appender.put(event_type="run.start", category="trace")

        assert event["seq"] == 1
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_jsonl_cancelled_preparation_does_not_publish_or_leak_temp_file(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deerflow.runtime.events.store import jsonl as jsonl_store

    prepared = threading.Event()
    release_prepare = threading.Event()
    prepare_finished = threading.Event()
    original_fsync = jsonl_store.os.fsync

    def blocking_fsync(descriptor: int) -> None:
        original_fsync(descriptor)
        prepared.set()
        try:
            release_prepare.wait(timeout=5)
        finally:
            prepare_finished.set()

    monkeypatch.setattr(jsonl_store.os, "fsync", blocking_fsync)
    runs = _OwnedRunStore()
    base_dir = tmp_path / "events"
    store = jsonl_store.JsonlRunEventStore(
        base_dir=base_dir,
        run_store=runs,
    )
    appender = FencedRunEventAppender(store, _authority())
    append_task = asyncio.create_task(appender.put(event_type="run.start", category="trace"))
    try:
        assert await asyncio.to_thread(prepared.wait, 2)
        append_task.cancel()
        release_prepare.set()
        with pytest.raises(asyncio.CancelledError):
            await append_task
        assert await asyncio.to_thread(prepare_finished.wait, 2)
    finally:
        release_prepare.set()
        if not append_task.done():
            append_task.cancel()
            await asyncio.gather(append_task, return_exceptions=True)

    assert await store.list_events("thread-1", "run-1") == []
    assert list(base_dir.rglob("*.tmp")) == []


@pytest.mark.anyio
async def test_jsonl_cancelled_committed_runtime_append_advances_sequence(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deerflow.runtime.events.store.jsonl import JsonlRunEventStore

    runs = _OwnedRunStore()
    store = JsonlRunEventStore(base_dir=tmp_path / "events", run_store=runs)
    appender = FencedRunEventAppender(store, _authority())
    original_commit = store._commit_prepared_record
    commit_started = threading.Event()
    allow_commit = threading.Event()

    def blocked_commit(target, temp_path) -> None:
        commit_started.set()
        assert allow_commit.wait(timeout=5)
        original_commit(target, temp_path)

    monkeypatch.setattr(store, "_commit_prepared_record", blocked_commit)
    append_task = asyncio.create_task(
        appender.put(event_type="run.start", category="trace"),
    )
    assert await asyncio.to_thread(commit_started.wait, 2)
    append_task.cancel()
    allow_commit.set()
    with pytest.raises(asyncio.CancelledError):
        await append_task

    second = await appender.put(event_type="run.end", category="outputs")
    events = await store.list_events("thread-1", "run-1")
    assert second["seq"] == 2
    assert [event["seq"] for event in events] == [1, 2]


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for the row-lock race gate",
)
async def test_postgres_runtime_append_holds_owner_row_through_event_insert() -> None:
    from deerflow.runtime.events.store.db import DbRunEventStore

    assert _POSTGRES_URL is not None
    engine = create_async_engine(postgres_async_url(_POSTGRES_URL))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    unique = uuid.uuid4().hex[:12]
    run_id = f"event-run-{unique}"
    thread_id = f"event-thread-{unique}"

    class _PausingStore(DbRunEventStore):
        def __init__(self) -> None:
            super().__init__(session_factory)
            self.row_locked = asyncio.Event()
            self.allow_insert = asyncio.Event()

        async def _max_seq_for_thread(self, session, selected_thread_id, *, tenant=None):
            self.row_locked.set()
            await self.allow_insert.wait()
            return await super()._max_seq_for_thread(
                session,
                selected_thread_id,
                tenant=tenant,
            )

    try:
        async with session_factory.begin() as session:
            session.add(
                RunRow(
                    run_id=run_id,
                    thread_id=thread_id,
                    operation_kind="run",
                    status="running",
                    owner_worker_id="worker-1",
                    state_version=3,
                    lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
                )
            )
        store = _PausingStore()
        appender = FencedRunEventAppender(
            store,
            RuntimeEventAuthority(
                tenant=None,
                thread_id=thread_id,
                run_id=run_id,
                owner_id="worker-1",
                lease_epoch=3,
            ),
        )
        append_task = asyncio.create_task(appender.put(event_type="run.start", category="trace"))
        await asyncio.wait_for(store.row_locked.wait(), timeout=2)

        async def transfer_owner() -> None:
            async with session_factory.begin() as session:
                row = await session.scalar(select(RunRow).where(RunRow.run_id == run_id).with_for_update())
                assert row is not None
                row.owner_worker_id = "worker-2"
                row.state_version = 4

        transfer_task = asyncio.create_task(transfer_owner())
        await asyncio.sleep(0.05)
        assert transfer_task.done() is False

        store.allow_insert.set()
        await append_task
        await transfer_task

        events = await store.list_events(thread_id, run_id, user_id=None)
        assert [event["event_type"] for event in events] == ["run.start"]
        async with session_factory() as session:
            row = await session.get(RunRow, run_id)
            assert row is not None
            assert (row.owner_worker_id, row.state_version) == ("worker-2", 4)
    finally:
        async with session_factory.begin() as session:
            await session.execute(delete(RunEventRow).where(RunEventRow.run_id == run_id))
            await session.execute(delete(RunRow).where(RunRow.run_id == run_id))
        await engine.dispose()
