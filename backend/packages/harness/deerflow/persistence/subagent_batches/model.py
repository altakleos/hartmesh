from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    false,
    text,
    true,
)
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base

SUBAGENT_BATCH_SCHEMA_WRITER_VERSION = 2


class SubagentBatchRow(Base):
    __tablename__ = "subagent_batches"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    schema_writer_version: Mapped[int] = mapped_column(
        Integer,
        default=SUBAGENT_BATCH_SCHEMA_WRITER_VERSION,
        server_default=text("1"),
    )
    tenant_ref: Mapped[str | None] = mapped_column(String(80), nullable=True)
    tenant_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(64), index=True)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tool_call_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    submission_key: Mapped[str] = mapped_column(String(256))
    title: Mapped[str] = mapped_column(String(256))
    subagent_type: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(24), index=True)
    total_items: Mapped[int] = mapped_column(Integer)
    max_live_items: Mapped[int] = mapped_column(Integer)
    max_running_items: Mapped[int] = mapped_column(Integer)
    max_attempts: Mapped[int] = mapped_column(Integer)
    execution_spec: Mapped[dict[str, Any]] = mapped_column(JSON)
    acceptance_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    acceptance_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    execution_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    execution_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    parent_invocation_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    parent_assembly_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    parent_tool_receipt_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    parent_tool_attempt: Mapped[int | None] = mapped_column(Integer, nullable=True)
    subagent_catalog_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    subagent_definition_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    item_root_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    parent_cancellable: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        server_default=false(),
    )
    cancel_epoch: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default=text("0"),
    )
    terminal_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "tenant_digest",
            "parent_tool_receipt_id",
            "submission_key",
            name="uq_subagent_batches_tenant_receipt_submission",
        ),
        CheckConstraint(
            "schema_writer_version IN (1, 2)",
            name="ck_subagent_batches_writer_version",
        ),
        CheckConstraint(
            "cancel_epoch >= 0",
            name="ck_subagent_batches_cancel_epoch",
        ),
        CheckConstraint(
            "schema_writer_version = 1 OR ("
            "tenant_ref IS NOT NULL AND tenant_digest IS NOT NULL AND "
            "acceptance_json IS NOT NULL AND acceptance_digest IS NOT NULL AND "
            "execution_json IS NOT NULL AND execution_digest IS NOT NULL AND "
            "parent_invocation_digest IS NOT NULL AND "
            "parent_assembly_fingerprint IS NOT NULL AND "
            "parent_tool_receipt_id IS NOT NULL AND parent_tool_attempt IS NOT NULL AND "
            "subagent_catalog_digest IS NOT NULL AND "
            "subagent_definition_digest IS NOT NULL AND "
            "item_root_digest IS NOT NULL AND accepted_at IS NOT NULL)",
            name="ck_subagent_batches_bound_writer",
        ),
        Index(
            "uq_subagent_batches_legacy_user_submission",
            "user_id",
            "submission_key",
            unique=True,
            sqlite_where=schema_writer_version == 1,
            postgresql_where=schema_writer_version == 1,
        ),
        Index(
            "ix_subagent_batches_tenant_thread_created",
            "tenant_digest",
            "thread_id",
            "created_at",
        ),
    )


class SubagentBatchItemRow(Base):
    __tablename__ = "subagent_batch_items"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    batch_id: Mapped[str] = mapped_column(
        ForeignKey("subagent_batches.id", ondelete="CASCADE"),
        index=True,
    )
    item_key: Mapped[str] = mapped_column(String(128))
    position: Mapped[int] = mapped_column(Integer)
    prompt: Mapped[str] = mapped_column(Text)
    request_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    lease_epoch: Mapped[int] = mapped_column(
        Integer,
        default=0,
        server_default=text("0"),
    )
    active_attempt_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    model_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    result: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_preview: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_truncated: Mapped[bool] = mapped_column(default=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    stop_reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    terminal_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    terminal_evidence_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    token_usage: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

    __table_args__ = (
        UniqueConstraint("batch_id", "item_key", name="uq_subagent_batch_items_key"),
        UniqueConstraint("batch_id", "position", name="uq_subagent_batch_items_position"),
        CheckConstraint(
            "lease_epoch >= 0",
            name="ck_subagent_batch_items_lease_epoch",
        ),
        Index("ix_subagent_batch_items_claim", "status", "lease_expires_at", "batch_id"),
    )


class SubagentBatchAttemptRow(Base):
    """Append-only evidence for one item claim/execute cycle."""

    __tablename__ = "subagent_batch_attempts"

    id: Mapped[str] = mapped_column(String(80), primary_key=True)
    batch_id: Mapped[str] = mapped_column(
        ForeignKey("subagent_batches.id", ondelete="CASCADE"),
        index=True,
    )
    item_id: Mapped[str] = mapped_column(
        ForeignKey("subagent_batch_items.id", ondelete="CASCADE"),
        index=True,
    )
    tenant_digest: Mapped[str] = mapped_column(String(64), index=True)
    attempt_number: Mapped[int] = mapped_column(Integer)
    lease_epoch: Mapped[int] = mapped_column(Integer)
    worker_ref: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), index=True)
    consumed: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        server_default=true(),
    )
    terminal_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    evidence_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    evidence_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    terminal_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "item_id",
            "lease_epoch",
            name="uq_subagent_batch_attempt_item_epoch",
        ),
        CheckConstraint(
            "attempt_number >= 1",
            name="ck_subagent_batch_attempt_number",
        ),
        CheckConstraint(
            "lease_epoch >= 1",
            name="ck_subagent_batch_attempt_epoch",
        ),
        Index(
            "ix_subagent_batch_attempts_tenant_batch_item",
            "tenant_digest",
            "batch_id",
            "item_id",
            "lease_epoch",
        ),
    )
