from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0025_tenant_identity"
_PREVIOUS_REVISION = "0024_tool_receipt_idempotency"


@pytest.mark.asyncio
async def test_sqlite_upgrade_adds_nullable_anchors_and_singleton_binding(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tenant-identity-migration.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            assert "hartmesh_deployment_identity" not in {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}

        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            table_names = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            assert "hartmesh_deployment_identity" in table_names
            binding_columns = {row[1] for row in connection.execute("PRAGMA table_info(hartmesh_deployment_identity)")}
            assert "legacy_redis_prefixes_json" in binding_columns
            for table_name in ("runs", "run_lifecycle_events", "run_events"):
                columns = {row[1]: row for row in connection.execute(f"PRAGMA table_info({table_name})")}
                assert columns["tenant_ref"][3] == 0
                assert columns["tenant_digest"][3] == 0
                indexes = {row[1] for row in connection.execute(f"PRAGMA index_list({table_name})")}
                assert any("tenant_digest" in name for name in indexes)

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            assert "hartmesh_deployment_identity" not in {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            assert "tenant_ref" not in {row[1] for row in connection.execute("PRAGMA table_info(runs)")}
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_downgrade_refuses_to_erase_an_established_tenant_binding(
    tmp_path: Path,
) -> None:
    path = tmp_path / "tenant-identity-bound-downgrade.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            connection.execute(
                """
                INSERT INTO hartmesh_deployment_identity (
                    singleton_key, identity_version, tenant_ref,
                    tenant_digest, legacy_redis_prefixes_json, bound_at
                ) VALUES (1, 1, ?, ?, NULL, ?)
                """,
                (
                    "tenant-aaaaaaaaaaaaaaaa",
                    "a" * 64,
                    "2026-09-01T00:00:00+00:00",
                ),
            )
            connection.commit()

        with pytest.raises(RuntimeError, match="tenant_identity_downgrade_blocked"):
            await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)

        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT version_num FROM alembic_version").fetchone() == (_REVISION,)
            assert connection.execute("SELECT tenant_digest FROM hartmesh_deployment_identity").fetchone() == ("a" * 64,)
    finally:
        await engine.dispose()
