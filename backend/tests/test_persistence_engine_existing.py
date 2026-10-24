"""Existing database operator access uses the one engine without bootstrap."""

import asyncio
import os
import sqlite3
import subprocess
import sys
import threading
from types import SimpleNamespace
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.config.database_config import DatabaseConfig
from deerflow.persistence import engine as lifecycle


@pytest_asyncio.fixture(autouse=True)
async def isolated_lifecycle():
    previous = lifecycle._engine, lifecycle._session_factory
    lifecycle._engine = lifecycle._session_factory = None
    try:
        yield
    finally:
        await lifecycle.close_engine()
        lifecycle._engine, lifecycle._session_factory = previous


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def existing(request, tmp_path):
    if request.param == "sqlite":
        directory = tmp_path / "existing space # percent %"
        config = DatabaseConfig(backend="sqlite", sqlite_dir=str(directory))

        def seed():
            directory.mkdir()
            keeper = sqlite3.connect(config.sqlite_path, check_same_thread=False)
            keeper.execute("PRAGMA journal_mode=WAL")
            keeper.execute("PRAGMA wal_autocheckpoint=0")
            keeper.executescript("CREATE TABLE parent(id INTEGER PRIMARY KEY); CREATE TABLE child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES parent(id) ON DELETE CASCADE);")
            keeper.execute("INSERT INTO parent VALUES (1)")
            keeper.execute("INSERT INTO child VALUES (2, 1)")
            keeper.commit()
            return keeper

        keeper = await asyncio.to_thread(seed)
        try:
            yield SimpleNamespace(config=config, backend="sqlite", keeper=keeper)
        finally:
            await lifecycle.close_engine()
            await asyncio.to_thread(keeper.close)
        return
    raw = os.environ.get("TEST_POSTGRES_URI")
    if not raw:
        pytest.skip("TEST_POSTGRES_URI is not configured")
    url = make_url(raw).set(drivername="postgresql+asyncpg")
    if url.query.get("sslmode") == "disable":
        url = url.difference_update_query(["sslmode"]).update_query_dict({"ssl": "disable"})
    schema = "engine_existing_" + uuid4().hex
    admin = create_async_engine(url)
    async with admin.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA "{schema}"'))
        await connection.execute(text(f'CREATE TABLE "{schema}".parent(id INTEGER PRIMARY KEY)'))
        await connection.execute(text(f'CREATE TABLE "{schema}".child(id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES "{schema}".parent(id) ON DELETE CASCADE)'))
        await connection.execute(text(f'INSERT INTO "{schema}".parent VALUES (1)'))
        await connection.execute(text(f'INSERT INTO "{schema}".child VALUES (2, 1)'))
    config = DatabaseConfig(backend="postgres", postgres_url=url.render_as_string(hide_password=False), postgres_schema=schema, pool_size=2)
    try:
        yield SimpleNamespace(config=config, backend="postgres", admin=admin)
    finally:
        await lifecycle.close_engine()
        async with admin.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest.mark.asyncio
async def test_existing_read_only_uses_current_data_without_schema_changes(existing):
    await lifecycle.init_engine_from_config(existing.config, existing_only=True, read_only=True)
    engine = lifecycle.get_engine()
    async with engine.connect() as connection:
        assert await connection.scalar(text("SELECT parent_id FROM child WHERE id=2")) == 1
        tables = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
        assert sorted(tables) == ["child", "parent"]
        if existing.backend == "sqlite":
            assert await connection.scalar(text("PRAGMA journal_mode")) == "wal"
            assert await connection.scalar(text("PRAGMA query_only")) == 1
            await connection.execute(text("PRAGMA query_only=OFF"))
        with pytest.raises(DBAPIError):
            await connection.execute(text("INSERT INTO parent VALUES (9)"))
    async with engine.connect() as first, engine.connect() as second:
        if existing.backend == "postgres":
            assert await first.scalar(text("SHOW transaction_read_only")) == "on"
            assert await second.scalar(text("SHOW transaction_read_only")) == "on"
            assert await second.scalar(text("SHOW search_path")) == existing.config.postgres_schema
        else:
            assert await second.scalar(text("PRAGMA query_only")) == 1
        assert await second.scalar(text("SELECT COUNT(*) FROM parent")) == 1


@pytest.mark.asyncio
async def test_existing_write_mode_preserves_foreign_key_cascades(existing):
    await lifecycle.init_engine_from_config(existing.config, existing_only=True)
    async with lifecycle.get_engine().begin() as connection:
        await connection.execute(text("DELETE FROM parent WHERE id=1"))
        assert await connection.scalar(text("SELECT COUNT(*) FROM child")) == 0
    async with lifecycle.get_engine().connect() as connection:
        assert sorted(await connection.run_sync(lambda sync: inspect(sync).get_table_names())) == ["child", "parent"]


@pytest.mark.asyncio
@pytest.mark.parametrize("read_only", [False, True])
@pytest.mark.parametrize("parent_exists", [False, True])
async def test_missing_sqlite_is_not_created_or_its_parent_made(tmp_path, read_only, parent_exists):
    directory = tmp_path / "missing" / "deeper"
    if parent_exists:
        directory.mkdir(parents=True)
    config = DatabaseConfig(backend="sqlite", sqlite_dir=str(directory))
    with pytest.raises(DBAPIError):
        await lifecycle.init_engine_from_config(config, existing_only=True, read_only=read_only)
    assert directory.exists() is parent_exists
    assert not (directory / "deerflow.db").exists()
    assert lifecycle.get_engine() is None
    assert lifecycle.get_session_factory() is None


@pytest.mark.asyncio
async def test_config_path_property_is_prepared_off_loop(existing):
    config = existing.config
    loop_thread = threading.get_ident()

    class PreparedConfig:
        backend = config.backend
        echo_sql, pool_size, pool_recycle, command_timeout = False, 2, 300, 30
        sqlite_dir, postgres_schema = config.sqlite_dir, config.postgres_schema

        @property
        def app_sqlalchemy_url(self):
            assert threading.get_ident() != loop_thread, "config path resolution ran on the event loop"
            return config.app_sqlalchemy_url

    await lifecycle.init_engine_from_config(PreparedConfig(), existing_only=True)
    async with lifecycle.get_engine().connect() as connection:
        assert await connection.scalar(text("SELECT COUNT(*) FROM parent")) == 1


@pytest.mark.asyncio
async def test_cancelled_config_preparation_drains_the_started_worker(tmp_path):
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    config = DatabaseConfig(backend="sqlite", sqlite_dir=str(tmp_path))

    class BlockedConfig:
        backend = "sqlite"
        echo_sql, pool_size, pool_recycle, command_timeout = False, 2, 300, 30
        sqlite_dir, postgres_schema = config.sqlite_dir, ""

        @property
        def app_sqlalchemy_url(self):
            entered.set()
            assert release.wait(timeout=10), "fixture did not release its config worker"
            try:
                return config.app_sqlalchemy_url
            finally:
                finished.set()

    task = asyncio.create_task(lifecycle.init_engine_from_config(BlockedConfig(), existing_only=True))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        for _ in range(5):
            await asyncio.sleep(0)
        assert not task.done(), "configuration filesystem worker outlived its cancelled owner"
        task.cancel()
        for _ in range(5):
            await asyncio.sleep(0)
        assert not task.done(), "repeated cancellation abandoned configuration preparation"
        assert lifecycle.get_engine() is None
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set()
        assert lifecycle.get_engine() is None
        assert not (tmp_path / "deerflow.db").exists()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(finished.wait, 3)


@pytest.mark.asyncio
async def test_missing_postgres_schema_is_not_created(existing):
    if existing.backend != "postgres":
        pytest.skip("PostgreSQL schema behavior")
    missing = "engine_missing_" + uuid4().hex
    config = existing.config.model_copy(update={"postgres_schema": missing})
    await lifecycle.init_engine_from_config(config, existing_only=True)
    async with lifecycle.get_engine().connect() as connection:
        assert await connection.scalar(text("SELECT current_schema()")) is None
    async with existing.admin.connect() as connection:
        assert await connection.scalar(text("SELECT COUNT(*) FROM pg_namespace WHERE nspname=:schema"), {"schema": missing}) == 0


@pytest.mark.asyncio
async def test_missing_postgres_database_is_not_created(existing, monkeypatch):
    if existing.backend != "postgres":
        pytest.skip("PostgreSQL database behavior")
    missing = "engine_missing_" + uuid4().hex
    url = make_url(existing.config.postgres_url).set(database=missing)
    config = existing.config.model_copy(update={"postgres_url": url.render_as_string(hide_password=False)})

    async def forbidden(*args, **kwargs):
        raise AssertionError("existing-only mode tried to create a database")

    monkeypatch.setattr(lifecycle, "_auto_create_postgres_db", forbidden)
    with pytest.raises(Exception) as failure:
        await lifecycle.init_engine_from_config(config, existing_only=True)
    assert not isinstance(failure.value, (AssertionError, TypeError))
    assert lifecycle.get_engine() is None
    async with existing.admin.connect() as connection:
        assert await connection.scalar(text("SELECT COUNT(*) FROM pg_database WHERE datname=:name"), {"name": missing}) == 0


def test_cold_read_only_engine_imports_no_bootstrap_or_migrations(tmp_path):
    path = tmp_path / "existing.sqlite"
    sqlite3.connect(path).close()
    before = path.read_bytes()
    program = """
import asyncio, importlib.abc, sys
class NoBootstrap(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in ('deerflow.persistence.bootstrap', 'deerflow.persistence.models', 'alembic') or fullname.startswith('alembic.'):
            raise AssertionError('existing database access imported ' + fullname)
sys.meta_path.insert(0, NoBootstrap())
from deerflow.persistence.engine import init_engine, get_engine, close_engine
from sqlalchemy import text
async def run():
    try:
        await init_engine('sqlite', url='sqlite+aiosqlite:///' + sys.argv[1], read_only=True)
        async with get_engine().connect() as connection:
            assert await connection.scalar(text('SELECT COUNT(*) FROM sqlite_master')) == 0
    finally:
        await close_engine()
asyncio.run(run())
"""
    completed = subprocess.run([sys.executable, "-c", program, str(path)], capture_output=True, text=True, timeout=20)
    assert completed.returncode == 0, completed.stderr
    assert path.read_bytes() == before
