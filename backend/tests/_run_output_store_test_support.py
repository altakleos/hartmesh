"""Task-isolated real SQL stores for output-evidence ownership tests."""

import os
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from deerflow.config.run_ownership_config import RunOwnershipConfig
from deerflow.persistence.run.model import RunChangeClockRow, RunRow
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.runs.manager import RunManager

RAW_OWNER = "human:output/owner"


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def output_store(tmp_path, request):
    admin = None
    schema = None
    if request.param == "postgres":
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("TEST_POSTGRES_URI is required for PostgreSQL coverage")
        url = make_url(uri).set(drivername="postgresql+asyncpg")
        options = {"ssl": False} if url.query.get("sslmode") == "disable" else {}
        url = url.difference_update_query(["sslmode"])
        schema = "output_" + uuid4().hex
        admin = create_async_engine(url, connect_args=options)
        async with admin.begin() as connection:
            await connection.execute(CreateSchema(schema))
        engine = create_async_engine(url, connect_args={**options, "server_settings": {"search_path": schema}})
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'runs.db'}", connect_args={"check_same_thread": False})
    async with engine.begin() as connection:
        if request.param == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda conn: RunRow.metadata.create_all(conn, tables=[RunRow.__table__, RunChangeClockRow.__table__]))
    sf = async_sessionmaker(engine, expire_on_commit=False)
    try:
        yield SimpleNamespace(repository=RunRepository(sf), sf=sf, engine=engine, dialect=request.param)
    finally:
        checked_out = engine.sync_engine.pool.checkedout()
        await engine.dispose()
        if admin is not None:
            try:
                async with admin.begin() as connection:
                    await connection.execute(DropSchema(schema, cascade=True))
            finally:
                await admin.dispose()
        assert checked_out == 0, "output evidence retained a checked-out SQL connection"


async def capture_run(environment, *, heartbeat=False, capture=True, owner=RAW_OWNER):
    manager = RunManager(environment.repository, run_ownership_config=RunOwnershipConfig(heartbeat_enabled=heartbeat))
    kwargs = {"input": {"messages": [{"type": "human", "id": "human-source", "content": "original"}]}, "host_value": {"nested": ["retained"]}}
    if capture:
        kwargs["output_capture"] = {"version": 1}
    record = await manager.create_or_reject(uuid4().hex, "lead_agent", user_id=owner, kwargs=kwargs)
    await manager.try_start(record.run_id)
    return manager, record
