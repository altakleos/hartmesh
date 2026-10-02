"""Migration tests for 0027_account_access.

Adds the account-access schema: three nullable columns on ``users`` and the
``disabled_identities``, ``role_limits`` and ``identity_holds`` tables. The
chain-head pin is in ``test_migration_chain_head``.
"""

from __future__ import annotations

import asyncio

import pytest
import sqlalchemy as sa
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence import bootstrap

pytestmark = pytest.mark.asyncio

REVISION = "0027_account_access"
PREVIOUS = "0026_mcp_task_lease_tokens"
USER_COLUMNS = {"oauth_issuer", "last_sign_in_at", "email_released_from"}
TABLES = {"disabled_identities", "role_limits", "identity_holds"}


async def test_0027_follows_the_last_upstream_revision():
    from alembic.script import ScriptDirectory

    assert ScriptDirectory(str(bootstrap._MIGRATIONS_DIR)).get_revision(REVISION).down_revision == PREVIOUS


def _shape(sync_conn) -> tuple[set[str], set[str]]:
    inspector = sa.inspect(sync_conn)
    return {column["name"] for column in inspector.get_columns("users")}, set(inspector.get_table_names())


async def test_0027_adds_and_removes_the_account_access_schema(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'chain.db'}"
    engine = create_async_engine(url)
    cfg = bootstrap._get_alembic_config(engine)
    try:
        await asyncio.to_thread(bootstrap._upgrade, cfg, PREVIOUS)
        async with engine.connect() as conn:
            columns, tables = await conn.run_sync(_shape)
        assert not USER_COLUMNS & columns
        assert not TABLES & tables

        await asyncio.to_thread(bootstrap._upgrade, cfg, "head")
        async with engine.connect() as conn:
            columns, tables = await conn.run_sync(_shape)
        assert USER_COLUMNS <= columns
        assert TABLES <= tables

        await asyncio.to_thread(command.downgrade, cfg, PREVIOUS)
        async with engine.connect() as conn:
            columns, tables = await conn.run_sync(_shape)
        assert not USER_COLUMNS & columns
        assert not TABLES & tables
    finally:
        await engine.dispose()
