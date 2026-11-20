"""Own transaction settlement and connection return during Telegram shutdown."""

import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.readers import reader_session
from deerflow.utils.file_io import await_drained


@asynccontextmanager
async def writer_session(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncSession]:
    # The shared session scope retains close. A writer also returns its
    # connection during transaction exit, before AsyncSession.close runs.
    async with reader_session(sessions) as session:
        transaction = session.begin()
        await transaction.__aenter__()
        try:
            yield session
        except BaseException:
            await await_drained(transaction.__aexit__(*sys.exc_info()))
            raise
        else:
            await await_drained(transaction.__aexit__(None, None, None))
