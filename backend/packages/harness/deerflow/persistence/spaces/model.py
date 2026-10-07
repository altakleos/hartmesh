"""Generic space custody and explicit grants; no product types or user cascade."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, CheckConstraint, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class SpaceRow(Base):
    __tablename__ = "storage_spaces"
    __table_args__ = (
        UniqueConstraint("backing_handle", name="uq_storage_spaces_backing_handle"),
        CheckConstraint("mode IN ('native', 'mediated')", name="ck_storage_spaces_mode"),
        CheckConstraint("status IN ('active', 'archived', 'deleted')", name="ck_storage_spaces_status"),
        CheckConstraint("generation > 0", name="ck_storage_spaces_generation"),
        CheckConstraint(
            "(custody_kind = 'company' AND custodian_kind IS NULL AND custodian_id IS NULL) OR (custody_kind = 'personal' AND custodian_kind IS NOT NULL AND custodian_kind IN ('human', 'nonhuman') AND custodian_id IS NOT NULL)",
            name="ck_storage_spaces_custody",
        ),
        CheckConstraint(
            "(feature_namespace IS NULL AND feature_controller IS NULL AND feature_metadata_version IS NULL AND mode = 'native') OR "
            "(feature_namespace IS NOT NULL AND feature_controller IS NOT NULL AND feature_metadata_version IS NOT NULL AND feature_metadata_version > 0)",
            name="ck_storage_spaces_feature",
        ),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    backing_handle: Mapped[str] = mapped_column(String(32))
    name: Mapped[str] = mapped_column(String(128))
    custody_kind: Mapped[str] = mapped_column(String(16))
    custodian_kind: Mapped[str | None] = mapped_column(String(16))
    custodian_id: Mapped[str | None] = mapped_column(String(128))
    mode: Mapped[str] = mapped_column(String(16))
    status: Mapped[str] = mapped_column(String(16), default="active")
    generation: Mapped[int] = mapped_column(Integer, default=1)
    feature_namespace: Mapped[str | None] = mapped_column(String(128))
    feature_controller: Mapped[str | None] = mapped_column(String(128))
    feature_metadata_version: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class SpaceGrantRow(Base):
    __tablename__ = "storage_space_grants"
    __table_args__ = (
        CheckConstraint("principal_kind IN ('human', 'nonhuman')", name="ck_storage_space_grants_kind"),
        CheckConstraint("permissions > 0 AND permissions <= 31", name="ck_storage_space_grants_permissions"),
    )

    space_id: Mapped[str] = mapped_column(String(32), ForeignKey("storage_spaces.id"), primary_key=True)
    principal_kind: Mapped[str] = mapped_column(String(16), primary_key=True)
    subject_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    permissions: Mapped[int] = mapped_column(Integer)


class SpaceEventRow(Base):
    """One immutable authority event per generation, including nonhuman actors."""

    __tablename__ = "storage_space_events"
    __table_args__ = (
        CheckConstraint("actor_kind IN ('human', 'nonhuman')", name="ck_storage_space_events_actor"),
        CheckConstraint("generation > 0", name="ck_storage_space_events_generation"),
    )

    space_id: Mapped[str] = mapped_column(String(32), ForeignKey("storage_spaces.id"), primary_key=True)
    generation: Mapped[int] = mapped_column(Integer, primary_key=True)
    action: Mapped[str] = mapped_column(String(32))
    actor_kind: Mapped[str] = mapped_column(String(16))
    actor_id: Mapped[str] = mapped_column(String(128))
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
