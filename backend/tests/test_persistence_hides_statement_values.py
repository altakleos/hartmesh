"""What a failed statement was given stays out of its error, which is what a log prints."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from deerflow.persistence.engine import close_engine, get_session_factory, init_engine


@pytest.mark.asyncio
async def test_a_failed_statement_does_not_carry_its_values(tmp_path):
    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path / 'values.db'}", sqlite_dir=str(tmp_path))
    try:
        async with get_session_factory()() as session:
            with pytest.raises(DBAPIError) as failed:
                await session.execute(text("INSERT INTO no_such_table VALUES (:value)"), {"value": "7123456789"})
    finally:
        await close_engine()
    assert "7123456789" not in str(failed.value)
