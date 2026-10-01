"""ORM model for the Shared area's publication records.

The Shared directory holds bytes; this table holds the story: who published
what, when, from where, and who removed it. A row is never deleted, so a
removed file still says who put it there and who took it away.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class SharedPublicationRow(Base):
    __tablename__ = "shared_publications"

    publication_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    # Where it landed in Shared, relative to the root; the same name can be
    # published again after a removal, so the path is not unique across time.
    path: Mapped[str] = mapped_column(String(1024), nullable=False, index=True)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    published_by: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    published_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    # Where the bytes came from: the conversation, when it was one, and the
    # path the person named (an output, an upload, or one of their own files).
    from_thread_id: Mapped[str | None] = mapped_column(String(64))
    from_path: Mapped[str | None] = mapped_column(String(4096))
    removed_by: Mapped[str | None] = mapped_column(String(64))
    removed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
