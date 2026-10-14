"""Complete recovery snapshots borrow one SQL connection without acquiring another."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.run.model import RunRow
from deerflow.persistence.run.sql import RunRepository


@pytest.mark.asyncio
async def test_recovery_snapshot_borrows_writer_and_includes_future_dated_live_leases(tmp_path):
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'runs.db'}", pool_size=1, max_overflow=0)
    try:
        async with engine.begin() as connection:
            await connection.run_sync(RunRow.__table__.create)
        sf = async_sessionmaker(engine, expire_on_commit=False)
        store = RunRepository(sf)
        async with sf() as session, session.begin():
            session.add_all(
                [
                    RunRow(
                        run_id="foreign",
                        thread_id="thread",
                        user_id="requester",
                        operation_kind="run",
                        status="running",
                        created_at=datetime.now(UTC) + timedelta(days=1),
                        metadata_json={"agent_instance_id": "instance"},
                        kwargs_json={},
                        owner_worker_id="foreign-worker",
                        lease_expires_at=datetime.now(UTC) + timedelta(days=2),
                    ),
                    RunRow(run_id="done", thread_id="thread", user_id="requester", operation_kind="run", status="success", metadata_json={}, kwargs_json={}),
                ]
            )
        async with sf() as session, session.begin():
            rows = await store._borrowed_inflight(session)
            assert [row["run_id"] for row in rows] == ["foreign"]
            assert rows[0]["metadata"] == {"agent_instance_id": "instance"}
            assert session.in_transaction() and session.is_active
    finally:
        await engine.dispose()
