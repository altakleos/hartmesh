from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.base import Base
from deerflow.persistence.models.run_event import RunEventRow
from deerflow.persistence.run import RunRepository
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.scheduled_task_runs import ScheduledTaskRunRepository
from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
from deerflow.runtime.events.store.db import DbRunEventStore
from deerflow.runtime.events.store.jsonl import JsonlRunEventStore
from deerflow.runtime.events.store.memory import MemoryRunEventStore
from deerflow.runtime.runs.store.memory import MemoryRunStore
from deerflow.runtime.tenant_identity import TenantIdentityV1

_TENANT_A = TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference()
_TENANT_B = TenantIdentityV1.from_canonical_id("tenant-b").to_persisted_reference()


@pytest.mark.asyncio
async def test_memory_store_filters_every_basic_read_and_mutation_by_tenant() -> None:
    seed = MemoryRunStore()
    await seed.put(
        "run-a",
        thread_id="shared-thread",
        tenant=_TENANT_A,
    )
    await seed.put(
        "run-b",
        thread_id="shared-thread",
        tenant=_TENANT_B,
    )
    store = MemoryRunStore(tenant=_TENANT_A)
    store._runs = seed._runs
    store._runs_by_thread = seed._runs_by_thread
    store._runs_by_external_identity = seed._runs_by_external_identity
    store._lifecycle_events = seed._lifecycle_events
    store._lifecycle_cursor = seed._lifecycle_cursor

    assert await store.get("run-b", user_id=None) is None
    assert await store.authoritative_get("run-b") is None
    assert [row["run_id"] for row in await store.list_by_thread("shared-thread", user_id=None)] == ["run-a"]
    assert [row["run_id"] for row in await store.list_inflight()] == ["run-a"]
    assert await store.update_status("run-b", "error") is False
    assert seed._runs["run-b"]["status"] == "pending"
    assert {event["run_id"] for event in await store.list_lifecycle_events()} == {"run-a"}


@pytest.mark.asyncio
async def test_sql_store_filters_every_basic_read_and_mutation_by_tenant(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tenant-scope.db'}")
    import deerflow.persistence.models  # noqa: F401

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = RunRepository(sessions, tenant=_TENANT_A)
    try:
        await store.initialize_lifecycle()
        await store.put("run-a", thread_id="shared-thread", status="success")
        async with sessions() as session:
            session.add(
                RunRow(
                    run_id="run-b",
                    thread_id="shared-thread",
                    status="pending",
                    metadata_json={},
                    kwargs_json={},
                    tenant_ref=_TENANT_B.public_ref,
                    tenant_digest=_TENANT_B.digest,
                )
            )
            await session.commit()

        assert await store.get("run-b", user_id=None) is None
        assert await store.authoritative_get("run-b") is None
        assert [row["run_id"] for row in await store.list_by_thread("shared-thread", user_id=None)] == ["run-a"]
        assert await store.list_inflight() == []
        assert await store.update_status("run-b", "error") is False
        async with sessions() as session:
            foreign = await session.get(RunRow, "run-b")
            assert foreign is not None
            assert foreign.status == "pending"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_scheduler_recovery_does_not_associate_a_foreign_tenant_run(
    tmp_path,
) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tenant-scheduler.db'}")
    import deerflow.persistence.models  # noqa: F401

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    run_repository = RunRepository(sessions, tenant=_TENANT_A)
    task_repository = ScheduledTaskRepository(
        sessions,
        run_repository=run_repository,
    )
    task_run_repository = ScheduledTaskRunRepository(
        sessions,
        run_repository=run_repository,
    )
    try:
        async with sessions() as session:
            session.add(
                RunRow(
                    run_id="foreign-run",
                    thread_id="shared-thread",
                    status="pending",
                    metadata_json={},
                    kwargs_json={},
                    tenant_ref=_TENANT_B.public_ref,
                    tenant_digest=_TENANT_B.digest,
                )
            )
            await session.commit()

        async with sessions() as session:
            task = SimpleNamespace(id="task-a", last_run_id="foreign-run")
            task_run = SimpleNamespace(id="task-run-a", run_id="foreign-run")
            assert (
                await task_repository._find_underlying_run(
                    session,
                    None,
                    task,
                )
                is None
            )
            assert (
                await task_run_repository._find_underlying_run(
                    session,
                    task_run,
                    task,
                )
                is None
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_memory_event_store_filters_and_deletes_only_its_tenant() -> None:
    foreign = MemoryRunEventStore(tenant=_TENANT_B)
    await foreign.put(
        thread_id="shared-thread",
        run_id="run-b",
        event_type="human_message",
        category="message",
    )
    store = MemoryRunEventStore(tenant=_TENANT_A)
    store._events = foreign._events
    store._messages = foreign._messages
    store._events_by_run = foreign._events_by_run
    store._messages_by_run = foreign._messages_by_run
    store._seq_counters = foreign._seq_counters
    await store.put(
        thread_id="shared-thread",
        run_id="run-a",
        event_type="human_message",
        category="message",
    )

    assert [event["run_id"] for event in await store.list_messages("shared-thread")] == ["run-a"]
    assert await store.count_messages("shared-thread") == 1
    assert await store.delete_by_thread("shared-thread") == 1
    assert [event["run_id"] for event in foreign._events["shared-thread"]] == ["run-b"]


@pytest.mark.asyncio
async def test_db_event_store_filters_and_deletes_only_its_tenant(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'tenant-events.db'}")
    import deerflow.persistence.models  # noqa: F401

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    store = DbRunEventStore(sessions, tenant=_TENANT_A)
    try:
        await store.put(
            thread_id="shared-thread",
            run_id="run-a",
            event_type="human_message",
            category="message",
        )
        async with sessions() as session:
            session.add(
                RunEventRow(
                    thread_id="shared-thread",
                    run_id="run-b",
                    tenant_ref=_TENANT_B.public_ref,
                    tenant_digest=_TENANT_B.digest,
                    event_type="human_message",
                    category="message",
                    content="",
                    event_metadata={},
                    seq=2,
                )
            )
            await session.commit()

        assert [event["run_id"] for event in await store.list_messages("shared-thread", user_id=None)] == ["run-a"]
        assert await store.count_messages("shared-thread", user_id=None) == 1
        assert await store.delete_by_thread("shared-thread", user_id=None) == 1
        async with sessions() as session:
            rows = (await session.execute(select(RunEventRow))).scalars().all()
            assert [row.run_id for row in rows] == ["run-b"]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_jsonl_event_store_filters_and_deletes_only_its_tenant(tmp_path) -> None:
    foreign = JsonlRunEventStore(tmp_path, tenant=_TENANT_B)
    await foreign.put(
        thread_id="shared-thread",
        run_id="run-b",
        event_type="human_message",
        category="message",
    )
    store = JsonlRunEventStore(tmp_path, tenant=_TENANT_A)
    await store.put(
        thread_id="shared-thread",
        run_id="run-a",
        event_type="human_message",
        category="message",
    )

    assert [event["run_id"] for event in await store.list_messages("shared-thread")] == ["run-a"]
    assert await store.count_messages("shared-thread") == 1
    assert await store.delete_by_thread("shared-thread") == 1
    assert [event["run_id"] for event in await foreign.list_messages("shared-thread")] == ["run-b"]
