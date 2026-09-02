from __future__ import annotations

from pathlib import Path

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import (
    SchemaMigrationHeadError,
    get_expected_migration_head,
    verify_schema_head,
)


@pytest.mark.asyncio
async def test_gateway_head_verifier_is_read_only_and_exact(tmp_path: Path) -> None:
    database = tmp_path / "schema.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database}")
    try:
        async with engine.begin() as connection:
            await connection.execute(sa.text("CREATE TABLE alembic_version (version_num varchar(64) NOT NULL)"))
            await connection.execute(
                sa.text("INSERT INTO alembic_version VALUES (:head)"),
                {"head": get_expected_migration_head()},
            )
        assert await verify_schema_head(engine) == "0031_merge_upstream_0017"
        async with engine.connect() as connection:
            tables = await connection.run_sync(lambda sync: set(sa.inspect(sync).get_table_names()))
        assert tables == {"alembic_version"}
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_gateway_head_verifier_rejects_missing_and_mismatched_heads(
    tmp_path: Path,
) -> None:
    for index, actual in enumerate((None, "0026_mcp_task_lineage")):
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / f'bad-{index}.db'}")
        try:
            if actual is not None:
                async with engine.begin() as connection:
                    await connection.execute(sa.text("CREATE TABLE alembic_version (version_num varchar(64) NOT NULL)"))
                    await connection.execute(
                        sa.text("INSERT INTO alembic_version VALUES (:head)"),
                        {"head": actual},
                    )
            with pytest.raises(SchemaMigrationHeadError) as exc_info:
                await verify_schema_head(engine)
            assert exc_info.value.code == "migration_head_mismatch"
            assert exc_info.value.expected == "0031_merge_upstream_0017"
            assert exc_info.value.actual == actual
        finally:
            await engine.dispose()


@pytest.mark.asyncio
async def test_migration_job_uses_the_normal_advisory_locked_bootstrap(
    monkeypatch,
) -> None:
    from deerflow.persistence import migration_job

    calls: list[object] = []
    config = type("Config", (), {"database": object()})()

    async def initialize(database, *, migration_mode):
        calls.append((database, migration_mode))

    async def verify(_engine):
        calls.append("verify")
        return "0031_merge_upstream_0017"

    async def close():
        calls.append("close")

    monkeypatch.setattr(migration_job, "get_app_config", lambda: config)
    monkeypatch.setattr(migration_job, "init_engine_from_config", initialize)
    monkeypatch.setattr(migration_job, "get_engine", lambda: object())
    monkeypatch.setattr(migration_job, "verify_schema_head", verify)
    monkeypatch.setattr(migration_job, "close_engine", close)

    assert await migration_job.run() == "0031_merge_upstream_0017"
    assert calls == [(config.database, "upgrade"), "verify", "close"]
