"""Populate only the disposable PostgreSQL measurement database with synthetic titles."""

import asyncio
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from deerflow.persistence.postgres_url import normalize_asyncpg_url
from deerflow.persistence.thread_meta.model import ThreadMetaRow


async def main(owner: UUID, other: UUID) -> None:
    engine = create_async_engine(normalize_asyncpg_url(os.environ["ACCEPTANCE_DATABASE_URL"]))
    try:
        async with async_sessionmaker(engine).begin() as session:
            if await session.scalar(text("SELECT current_database()")) != "acceptance":
                raise RuntimeError("Title fixture requires the disposable acceptance database")
            started = datetime(2024, 1, 1, tzinfo=UTC)
            for i in range(2001):
                stamp = started + timedelta(seconds=i)
                title = "Quarterly archive" if i == 0 else f"Planning notes {i:04d}"
                if i == 2000:
                    title = "Quarterly archive private"
                session.add(ThreadMetaRow(
                    thread_id=f"sample-{i:04d}", incarnation=f"{i + 1:032x}", display_name=title,
                    user_id=str(owner if i < 2000 else other), assistant_id="lead_agent",
                    status="idle", metadata_json={}, created_at=stamp, updated_at=stamp,
                ))
            await session.flush()
            count = await session.scalar(select(func.count()).select_from(ThreadMetaRow).where(ThreadMetaRow.user_id == str(owner)))
            if count != 2000:
                raise RuntimeError(f"Unexpected size of title corpus: {count}")
            print(json.dumps({
                "owned": count, "foreign": 1,
                "synchronousCommit": await session.scalar(text("SHOW synchronous_commit")),
                "walWriterDelay": await session.scalar(text("SHOW wal_writer_delay")),
            }))
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main(UUID(sys.argv[1]), UUID(sys.argv[2])))
