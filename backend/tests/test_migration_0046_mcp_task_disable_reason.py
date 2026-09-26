"""A task cancelled because its owner was turned off says so; the downgrade will not rewrite whose request it was."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0046_mcp_task_disable_reason"
_PREVIOUS = "0045_refusal_sweep_reach"


def _shape(path: Path) -> str:
    with sqlite3.connect(path) as connection:
        return connection.execute("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'mcp_tasks'").fetchone()[0]


@pytest.mark.asyncio
async def test_the_disable_reason_is_admitted_and_a_downgrade_refuses_while_a_task_carries_it(tmp_path: Path) -> None:
    path = tmp_path / "mcp-task-disable-reason.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS)
        assert "'account_disabled'" not in _shape(path)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        assert "'account_disabled'" in _shape(path) and _shape(path).count("ck_mcp_tasks_cancel_intent_shape") == 1

        with sqlite3.connect(path) as connection:
            connection.execute("UPDATE mcp_tasks SET cancel_reason_code = 'account_disabled'")  # no rows: nothing to block yet
        await asyncio.to_thread(command.downgrade, config, _PREVIOUS)
        assert "'account_disabled'" not in _shape(path)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        await asyncio.to_thread(command.upgrade, config, _REVISION)
    finally:
        await engine.dispose()
