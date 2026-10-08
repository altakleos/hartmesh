"""Durable environment containment facts, independent of conversation lifetime."""

from datetime import UTC, datetime

from sqlalchemy import BigInteger, CheckConstraint, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class SpaceAttachmentRow(Base):
    __tablename__ = "storage_space_attachments"
    __table_args__ = (
        UniqueConstraint("incarnation", name="uq_storage_space_attachments_incarnation"),
        CheckConstraint("actor_kind IN ('human', 'nonhuman')", name="ck_storage_space_attachments_actor"),
        CheckConstraint("phase IN ('pending', 'active', 'fence_pending', 'fenced')", name="ck_storage_space_attachments_phase"),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    incarnation: Mapped[str] = mapped_column(String(32))
    actor_kind: Mapped[str] = mapped_column(String(16))
    actor_id: Mapped[str] = mapped_column(String(128))
    host_id: Mapped[str] = mapped_column(String(128))
    container_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    phase: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class SpaceMountRow(Base):
    __tablename__ = "storage_space_mounts"
    __table_args__ = (
        UniqueConstraint("attachment_id", "alias", name="uq_storage_space_mounts_alias"),
        CheckConstraint("generation > 0", name="ck_storage_space_mounts_generation"),
        CheckConstraint("writable IN (0, 1)", name="ck_storage_space_mounts_writable"),
    )

    attachment_id: Mapped[str] = mapped_column(String(32), ForeignKey("storage_space_attachments.id"), primary_key=True)
    space_id: Mapped[str] = mapped_column(String(32), ForeignKey("storage_spaces.id"), primary_key=True)
    generation: Mapped[int] = mapped_column(Integer)
    alias: Mapped[str] = mapped_column(String(64))
    writable: Mapped[int] = mapped_column(Integer)


class SpaceBackupRow(Base):
    __tablename__ = "storage_space_backups"
    __table_args__ = (
        CheckConstraint("generation > 0 AND size_bytes > 0", name="ck_storage_space_backups_size"),
        CheckConstraint("actor_kind IN ('human', 'nonhuman')", name="ck_storage_space_backups_actor"),
        CheckConstraint("consistency = 'quiesced-filesystem'", name="ck_storage_space_backups_consistency"),
    )

    space_id: Mapped[str] = mapped_column(String(32), ForeignKey("storage_spaces.id"), primary_key=True)
    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    generation: Mapped[int] = mapped_column(Integer)
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    sha256: Mapped[str] = mapped_column(String(64))
    consistency: Mapped[str] = mapped_column(String(32))
    actor_kind: Mapped[str] = mapped_column(String(16))
    actor_id: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
