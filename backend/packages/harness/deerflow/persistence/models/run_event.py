"""ORM model for run events."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, CheckConstraint, DateTime, Index, String, Text, UniqueConstraint, text
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.constants import RUN_EVENT_CATEGORY_MAX_LENGTH, RUN_EVENT_TYPE_MAX_LENGTH
from deerflow.persistence.base import Base


class RunEventRow(Base):
    __tablename__ = "run_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    thread_id: Mapped[str] = mapped_column(String(64), nullable=False)
    run_id: Mapped[str] = mapped_column(String(64), nullable=False)
    # Owner of the conversation this event belongs to. Nullable for data
    # created before auth was introduced; populated by auth middleware on
    # new writes and by the boot-time orphan migration on existing rows.
    user_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    tenant_ref: Mapped[str | None] = mapped_column(String(23), nullable=True)
    tenant_digest: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    event_type: Mapped[str] = mapped_column(String(RUN_EVENT_TYPE_MAX_LENGTH), nullable=False)
    # Nullable for every legacy/non-receipt event. Receipt writers use the
    # partial unique index below as their cross-process idempotency boundary.
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    category: Mapped[str] = mapped_column(String(RUN_EVENT_CATEGORY_MAX_LENGTH), nullable=False)
    # Category values and semantics are defined by runtime/events/catalog.py
    content: Mapped[str] = mapped_column(Text, default="")
    event_metadata: Mapped[dict] = mapped_column(JSON, default=dict)
    seq: Mapped[int] = mapped_column(nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    __table_args__ = (
        CheckConstraint(
            "(tenant_ref IS NULL) = (tenant_digest IS NULL)",
            name="ck_run_events_tenant_pair",
        ),
        UniqueConstraint("thread_id", "seq", name="uq_events_thread_seq"),
        Index("ix_events_thread_cat_seq", "thread_id", "category", "seq"),
        Index("ix_events_run", "thread_id", "run_id", "seq"),
        Index(
            "uq_run_events_receipt_idempotency",
            "run_id",
            "event_type",
            "idempotency_key",
            unique=True,
            sqlite_where=text("idempotency_key IS NOT NULL"),
            postgresql_where=text("idempotency_key IS NOT NULL"),
        ),
    )
