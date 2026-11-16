"""Cancellation-safe cleanup for repository-owned read sessions."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.utils.file_io import await_drained


@asynccontextmanager
async def reader_session(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncSession]:
    session = sessions()
    try:
        yield session
    finally:
        # AsyncSession.__aexit__ shields close in a detached task. A cancelled
        # reader must instead retain its own rollback/connection return until
        # shutdown can safely let the engine owner dispose persistence.
        await await_drained(session.close())
