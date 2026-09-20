"""SQLAlchemy-backed publication records for the Shared area.

Each method acquires its own short-lived session. Rows are appended and
amended, never deleted: a removal is a second fact on the same row.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.shared_publications.model import SharedPublicationRow
from deerflow.utils.time import coerce_iso


class SharedPublicationRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory

    @staticmethod
    def _row_to_dict(row: SharedPublicationRow) -> dict:
        d = row.to_dict()
        for key in ("published_at", "removed_at"):
            value = d.get(key)
            if isinstance(value, datetime):
                # SQLite drops tzinfo on read; ``coerce_iso`` returns the
                # value as tz-aware ISO text.
                d[key] = coerce_iso(value)
        return d

    async def record_publication(
        self,
        *,
        path: str,
        size: int,
        sha256: str,
        published_by: str,
        from_thread_id: str | None,
        from_path: str | None,
    ) -> dict:
        """Record that *published_by* put these bytes at *path*."""
        row = SharedPublicationRow(
            publication_id=str(uuid.uuid4()),
            path=path,
            size=size,
            sha256=sha256,
            published_by=published_by,
            published_at=datetime.now(UTC),
            from_thread_id=from_thread_id,
            from_path=from_path,
        )
        async with self._sf() as session:
            session.add(row)
            await session.commit()
            await session.refresh(row)
            return self._row_to_dict(row)

    async def live_publication(self, path: str) -> dict | None:
        """The record of what is at *path* now: the latest publication not yet removed."""
        async with self._sf() as session:
            stmt = select(SharedPublicationRow).where(SharedPublicationRow.path == path, SharedPublicationRow.removed_at.is_(None)).order_by(SharedPublicationRow.published_at.desc())
            row = (await session.execute(stmt)).scalars().first()
            return None if row is None else self._row_to_dict(row)

    async def live_publications(self) -> dict[str, dict]:
        """Every publication not yet removed, keyed by path."""
        async with self._sf() as session:
            stmt = select(SharedPublicationRow).where(SharedPublicationRow.removed_at.is_(None)).order_by(SharedPublicationRow.published_at.asc())
            rows = (await session.execute(stmt)).scalars().all()
        # A path published twice without a removal in between is a directory
        # the record did not see change; the latest word wins.
        return {row.path: self._row_to_dict(row) for row in rows}

    async def record_removal(self, publication_id: str, *, removed_by: str) -> dict | None:
        """Mark the publication *publication_id* removed by *removed_by*; the row stays.

        Addressed by id, not by path: a publish that claims the same name
        between the caller's read and this write would otherwise have its own
        fresh row marked removed, leaving the new file live with no live
        record and the original row live forever.
        """
        async with self._sf() as session:
            stmt = select(SharedPublicationRow).where(SharedPublicationRow.publication_id == publication_id, SharedPublicationRow.removed_at.is_(None))
            row = (await session.execute(stmt)).scalars().first()
            if row is None:
                return None
            row.removed_by = removed_by
            row.removed_at = datetime.now(UTC)
            await session.commit()
            await session.refresh(row)
            return self._row_to_dict(row)
