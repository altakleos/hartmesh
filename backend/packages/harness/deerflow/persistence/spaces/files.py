"""Durable binding and interrupted host file operations, independent of chats."""

from datetime import UTC, datetime

from sqlalchemy import JSON, BigInteger, CheckConstraint, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class SpaceBackingRow(Base):
    __tablename__ = "storage_space_backings"
    __table_args__ = (
        UniqueConstraint("space_id", name="uq_storage_space_backings_space"),
        UniqueConstraint("slot_id", name="uq_storage_space_backings_slot"),
        UniqueConstraint("filesystem_uuid", name="uq_storage_space_backings_filesystem"),
        CheckConstraint("max_bytes > 0 AND max_inodes > 0", name="ck_storage_space_backings_capacity"),
        CheckConstraint("root_inode > 0 AND control_inode > 0", name="ck_storage_space_backings_incarnation"),
    )

    backing_handle: Mapped[str] = mapped_column(String(32), primary_key=True)
    space_id: Mapped[str] = mapped_column(String(32), ForeignKey("storage_spaces.id"))
    slot_id: Mapped[str] = mapped_column(String(32))
    filesystem_uuid: Mapped[str] = mapped_column(String(36))
    max_bytes: Mapped[int] = mapped_column(BigInteger)
    max_inodes: Mapped[int] = mapped_column(BigInteger)
    root_inode: Mapped[int] = mapped_column(BigInteger)
    control_inode: Mapped[int] = mapped_column(BigInteger)


class SpaceFileOperationRow(Base):
    __tablename__ = "storage_space_file_operations"
    __table_args__ = (
        CheckConstraint("actor_kind IN ('human', 'nonhuman')", name="ck_storage_space_file_operations_actor"),
        CheckConstraint("generation > 0", name="ck_storage_space_file_operations_generation"),
        CheckConstraint("phase IN ('pending', 'complete', 'failed')", name="ck_storage_space_file_operations_phase"),
    )

    space_id: Mapped[str] = mapped_column(String(32), ForeignKey("storage_spaces.id"), primary_key=True)
    operation_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    actor_kind: Mapped[str] = mapped_column(String(16))
    actor_id: Mapped[str] = mapped_column(String(128))
    generation: Mapped[int] = mapped_column(Integer)
    phase: Mapped[str] = mapped_column(String(16))
    request: Mapped[dict] = mapped_column(JSON)
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
