from __future__ import annotations

import sqlite3
from pathlib import Path

from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config


def test_topology_registry_migration_has_bounded_exact_shape(tmp_path: Path) -> None:
    database_path = tmp_path / "migration.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{database_path}")
    try:
        config = _get_alembic_config(engine)
        command.upgrade(config, "0027_multi_gateway_topology")
    finally:
        import asyncio

        asyncio.run(engine.dispose())

    with sqlite3.connect(database_path) as connection:
        columns = {row[1]: row for row in connection.execute("PRAGMA table_info(hartmesh_topology_replicas)")}
        assert set(columns) == {
            "tenant_digest",
            "profile",
            "replica_id",
            "topology_digest",
            "fingerprint_json",
            "started_at",
            "heartbeat_at",
        }
        assert all(columns[name][3] == 1 for name in columns)
        indexes = {row[1] for row in connection.execute("PRAGMA index_list(hartmesh_topology_replicas)")}
        assert "ix_hartmesh_topology_live" in indexes
        scheduled_columns = {row[1]: row for row in connection.execute("PRAGMA table_info(scheduled_tasks)")}
        assert scheduled_columns["schedule_version"][3] == 1
        assert scheduled_columns["schedule_version"][4] == "'1'"
        revision = connection.execute("SELECT version_num FROM alembic_version").fetchone()
        assert revision == ("0027_multi_gateway_topology",)

    command.downgrade(config, "0026_mcp_task_lineage")
    with sqlite3.connect(database_path) as connection:
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        assert "hartmesh_topology_replicas" not in tables
        scheduled_columns = {row[1] for row in connection.execute("PRAGMA table_info(scheduled_tasks)")}
        assert "schedule_version" not in scheduled_columns
        revision = connection.execute("SELECT version_num FROM alembic_version").fetchone()
        assert revision == ("0026_mcp_task_lineage",)
