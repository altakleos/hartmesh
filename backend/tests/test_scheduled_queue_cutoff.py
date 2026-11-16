"""Fair queue selection and atomic claiming use the same queue-age boundary."""

import os
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from support.postgres import asyncpg_test_url

from deerflow.persistence.postgres_schema import build_asyncpg_connect_args
from deerflow.persistence.scheduled_task_runs.model import ScheduledTaskRunRow
from deerflow.persistence.scheduled_task_runs.retirement import EXECUTION_RETIREMENT_PENDING
from deerflow.persistence.scheduled_task_runs.sql import ScheduledTaskRunRepository
from deerflow.persistence.scheduled_tasks.model import ScheduledTaskRow


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("blocker", ["queued", "launching", "running", "retiring"])
async def test_age_cutoff_admits_only_expired_idle_blockers(tmp_path, backend, blocker):
    now = datetime.now(UTC)
    schema = None
    if backend == "sqlite":
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'queue.db'}")
    else:
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("requires TEST_POSTGRES_URI")
        schema = "queue_cutoff_" + uuid4().hex
        engine = create_async_engine(asyncpg_test_url(uri), connect_args=build_asyncpg_connect_args(schema))
    try:
        async with engine.begin() as connection:
            if schema:
                await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.run_sync(lambda sync: ScheduledTaskRow.metadata.create_all(sync, tables=[ScheduledTaskRow.__table__, ScheduledTaskRunRow.__table__]))
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            for name in ("old", "fresh"):
                session.add(ScheduledTaskRow(id=name, user_id="owner", title=name, prompt="Check", schedule_type="interval", schedule_spec={"every_seconds": 3600}, timezone="UTC"))
            # The expired queued row deliberately remains after a failed
            # expiry sweep. It owns no execution; active/retiring rows do.
            session.add(
                ScheduledTaskRunRow(
                    id="old",
                    task_id="old",
                    thread_id="same",
                    scheduled_for=now,
                    trigger="scheduled",
                    status="failed" if blocker == "retiring" else blocker,
                    lease_owner=EXECUTION_RETIREMENT_PENDING if blocker == "retiring" else None,
                    created_at=now - timedelta(hours=2),
                )
            )
            session.add(ScheduledTaskRunRow(id="fresh", task_id="fresh", thread_id="same", scheduled_for=now, trigger="scheduled", status="queued", created_at=now - timedelta(minutes=1)))
            await session.commit()
        repo = ScheduledTaskRunRepository(factory)
        cutoff = now - timedelta(hours=1)
        selected = await repo.list_queued_runs(limit=10, created_after=cutoff)
        claimed = await repo.claim_queued_run("fresh", now=now, lease_owner="worker", lease_seconds=120, global_max_concurrent_runs=3, created_after=cutoff)
        if blocker == "queued":
            assert [row["id"] for row in selected] == ["fresh"]
            assert claimed is not None and claimed["status"] == "launching"
        else:
            assert selected == []
            assert claimed is None
        async with factory() as session:
            old = await session.get(ScheduledTaskRunRow, "old")
            assert old.attempt_count == 0
            assert old.status == ("failed" if blocker == "retiring" else blocker)
    finally:
        if schema:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("backend", ["sqlite", "postgres"])
@pytest.mark.parametrize("newer_status", ["running", "queued"])
async def test_late_launch_keeps_its_budget_until_exact_worker_retirement(backend, newer_status):
    from deerflow.persistence.run.model import RunRow

    now = datetime.now(UTC)
    schema = None
    if backend == "sqlite":
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    else:
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("requires TEST_POSTGRES_URI")
        schema = "late_launch_" + uuid4().hex
        engine = create_async_engine(asyncpg_test_url(uri), connect_args=build_asyncpg_connect_args(schema))
    try:
        async with engine.begin() as connection:
            if schema:
                await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            await connection.run_sync(lambda sync: ScheduledTaskRow.metadata.create_all(sync, tables=[ScheduledTaskRow.__table__, ScheduledTaskRunRow.__table__, RunRow.__table__]))
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            for name in ("task", "independent"):
                session.add(ScheduledTaskRow(id=name, user_id="owner", title=name, prompt="Check", schedule_type="interval", schedule_spec={"every_seconds": 3600}, timezone="UTC"))
            session.add(ScheduledTaskRunRow(id="old", task_id="task", thread_id="old-home", scheduled_for=now, trigger="scheduled", status="failed", error="queue timeout", finished_at=now))
            session.add(ScheduledTaskRunRow(id="new", task_id="task", thread_id="new-home", scheduled_for=now, trigger="manual", status=newer_status))
            session.add(ScheduledTaskRunRow(id="independent", task_id="independent", thread_id="other", scheduled_for=now, trigger="manual", status="queued"))
            session.add(RunRow(run_id="late", thread_id="old-home", user_id="owner", status="running", metadata_json={"scheduled_task_id": "task", "scheduled_task_run_id": "old"}))
            await session.commit()
        repo = ScheduledTaskRunRepository(factory)
        late_start = now + timedelta(seconds=1)
        assert await repo.reconcile_launched_run("old", task_id="task", run_id="late", started_at=late_start)
        assert await repo.count_active_runs() == (2 if newer_status == "running" else 1)
        assert [row["id"] for row in await repo.list_retirement_pending()] == ["old"]
        assert await repo.claim_queued_run("independent", now=now, lease_owner="peer", lease_seconds=120, global_max_concurrent_runs=2 if newer_status == "running" else 1) is None
        assert await repo.reconcile_launched_run("old", task_id="task", run_id="late", started_at=late_start)
        assert not await repo.confirm_execution_retirement("old", run_id="different")
        # The supervisor supplies positive worker-drain evidence through this
        # exact capability. Neither runtime terminal status nor time releases it.
        assert await repo.confirm_execution_retirement("old", run_id="late")
        assert await repo.reconcile_launched_run("old", task_id="task", run_id="late", started_at=late_start)
        assert await repo.update_status("old", status="running", run_id="late", started_at=late_start, protect_terminal=True)
        async with factory() as session:
            old = await session.get(ScheduledTaskRunRow, "old")
            assert (old.status, old.error, old.started_at) == ("failed", "queue timeout", None)
            assert old.finished_at.replace(tzinfo=UTC) == now
        assert await repo.claim_queued_run("independent", now=now, lease_owner="peer", lease_seconds=120, global_max_concurrent_runs=2 if newer_status == "running" else 1) is not None
    finally:
        if schema:
            async with engine.begin() as connection:
                await connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
