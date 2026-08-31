from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import JSON, Boolean, CheckConstraint, DateTime, Index, Integer, String, Text, UniqueConstraint, false
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.constants import (
    MCP_TASK_CANCEL_ACTOR_REF_LENGTH,
    MCP_TASK_CANCEL_REASON_MAX_LENGTH,
    MCP_TASK_NAME_MAX_LENGTH,
    MCP_TASK_REMOTE_ID_MAX_LENGTH,
    MCP_TASK_SERVER_NAME_MAX_LENGTH,
)
from deerflow.persistence.base import Base


class McpTaskRow(Base):
    __tablename__ = "mcp_tasks"

    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    schema_writer_version: Mapped[int] = mapped_column(
        Integer,
        default=2,
        server_default="1",
    )
    tenant_ref: Mapped[str | None] = mapped_column(String(23), nullable=True)
    tenant_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    lineage_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    lineage_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    parent_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    parent_tool_receipt_id: Mapped[str | None] = mapped_column(
        String(67),
        nullable=True,
    )
    user_id: Mapped[str] = mapped_column(String(64), index=True)
    thread_id: Mapped[str] = mapped_column(String(64), index=True)
    run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    tool_call_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    server_name: Mapped[str] = mapped_column(String(MCP_TASK_SERVER_NAME_MAX_LENGTH))
    driver_name: Mapped[str] = mapped_column(String(64))
    remote_task_id: Mapped[str] = mapped_column(String(MCP_TASK_REMOTE_ID_MAX_LENGTH))
    task_name: Mapped[str] = mapped_column(String(MCP_TASK_NAME_MAX_LENGTH))
    status: Mapped[str] = mapped_column(String(32), index=True)
    result: Mapped[Any | None] = mapped_column(JSON, nullable=True)
    result_preview: Mapped[str | None] = mapped_column(Text, nullable=True)
    result_truncated: Mapped[bool] = mapped_column(Boolean, default=False, server_default=false())
    result_artifact: Mapped[dict[str, str] | None] = mapped_column(JSON, nullable=True)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    input_required: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    driver_data: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    notification_status: Mapped[str] = mapped_column(String(16), default="none", index=True)
    event_fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    event_version: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    notified_version: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    dispatch_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    dispatch_attempt: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    dispatch_event: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    notification_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    notification_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    notification_attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    next_notification_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notification_lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    notification_lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    next_poll_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True, nullable=True)
    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_poll_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    poll_attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    consecutive_poll_error_count: Mapped[int] = mapped_column(Integer, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancel_actor_ref: Mapped[str | None] = mapped_column(
        String(MCP_TASK_CANCEL_ACTOR_REF_LENGTH),
        nullable=True,
    )
    cancel_reason_code: Mapped[str | None] = mapped_column(
        String(MCP_TASK_CANCEL_REASON_MAX_LENGTH),
        nullable=True,
    )
    cancel_attempt_count: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    next_cancel_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_cancel_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(UTC),
        onupdate=lambda: datetime.now(UTC),
    )

    __table_args__ = (
        UniqueConstraint(
            "tenant_digest",
            "user_id",
            "server_name",
            "remote_task_id",
            name="uq_mcp_tasks_tenant_user_server_remote",
        ),
        UniqueConstraint(
            "tenant_digest",
            "lineage_digest",
            name="uq_mcp_tasks_tenant_lineage",
        ),
        CheckConstraint(
            "(tenant_ref IS NULL) = (tenant_digest IS NULL)",
            name="ck_mcp_tasks_tenant_pair",
        ),
        CheckConstraint(
            "(lineage_json IS NULL) = (lineage_digest IS NULL)",
            name="ck_mcp_tasks_lineage_pair",
        ),
        CheckConstraint(
            "lineage_digest IS NOT NULL OR (parent_run_id IS NULL AND parent_tool_receipt_id IS NULL)",
            name="ck_mcp_tasks_legacy_parent_null",
        ),
        CheckConstraint(
            "schema_writer_version >= 1",
            name="ck_mcp_tasks_schema_writer_version",
        ),
        CheckConstraint(
            "schema_writer_version < 2 OR (tenant_ref IS NOT NULL AND tenant_digest IS NOT NULL AND lineage_json IS NOT NULL AND lineage_digest IS NOT NULL)",
            name="ck_mcp_tasks_writer_lineage_required",
        ),
        CheckConstraint(
            "(cancel_actor_ref IS NULL) = (cancel_reason_code IS NULL)",
            name="ck_mcp_tasks_cancel_intent_pair",
        ),
        CheckConstraint(
            "schema_writer_version < 2 OR cancel_requested_at IS NULL OR cancel_actor_ref IS NOT NULL",
            name="ck_mcp_tasks_writer_cancel_intent",
        ),
        CheckConstraint(
            "(cancel_actor_ref IS NULL OR length(cancel_actor_ref) = 64) AND (cancel_reason_code IS NULL OR cancel_reason_code IN ('user_api', 'agent_tool'))",
            name="ck_mcp_tasks_cancel_intent_shape",
        ),
        CheckConstraint(
            "(tenant_ref IS NULL OR length(tenant_ref) = 23) AND "
            "(tenant_digest IS NULL OR length(tenant_digest) = 64) AND "
            "(lineage_digest IS NULL OR length(lineage_digest) = 64) AND "
            "(parent_run_id IS NULL OR length(parent_run_id) <= 64) AND "
            "(parent_tool_receipt_id IS NULL OR length(parent_tool_receipt_id) = 67) AND "
            "(notification_run_id IS NULL OR length(notification_run_id) <= 64)",
            name="ck_mcp_tasks_lineage_lengths",
        ),
        Index(
            "ix_mcp_tasks_tenant_thread_created",
            "tenant_digest",
            "thread_id",
            "created_at",
            "id",
        ),
        Index("ix_mcp_tasks_tenant_due", "tenant_digest", "status", "next_poll_at"),
        Index(
            "ix_mcp_tasks_tenant_notification_due",
            "tenant_digest",
            "notification_status",
            "next_notification_at",
        ),
        Index(
            "ix_mcp_tasks_tenant_cancel_due",
            "tenant_digest",
            "cancel_requested_at",
            "next_cancel_at",
        ),
        Index(
            "ix_mcp_tasks_tenant_parent_run",
            "tenant_digest",
            "parent_run_id",
            "created_at",
            "id",
        ),
        Index(
            "ix_mcp_tasks_tenant_parent_receipt",
            "tenant_digest",
            "parent_tool_receipt_id",
        ),
        Index(
            "ix_mcp_tasks_tenant_notification_run",
            "tenant_digest",
            "notification_run_id",
        ),
    )
