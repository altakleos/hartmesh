"""ORM model for run metadata."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, CheckConstraint, DateTime, Index, String, Text, text
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class RunRow(Base):
    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    assistant_id: Mapped[str | None] = mapped_column(String(128))
    user_id: Mapped[str | None] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    # "pending" | "running" | "success" | "error" | "timeout" | "interrupted"
    operation_kind: Mapped[str] = mapped_column(String(32), nullable=False, default="run", server_default=text("'run'"))

    model_name: Mapped[str | None] = mapped_column(String(128))
    multitask_strategy: Mapped[str] = mapped_column(String(20), default="reject")
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict)
    kwargs_json: Mapped[dict] = mapped_column(JSON, default=dict)
    error: Mapped[str | None] = mapped_column(Text)
    stop_reason: Mapped[str | None] = mapped_column(String(50))

    # Host-sealed invocation facts. Nullable for historical rows and for
    # auxiliary checkpoint operations, which are not agent invocations.
    origin_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    principal_projection_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    principal_projection_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    base_origin_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    accepted_context_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    agent_revision_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    agent_revision_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    extension_generation: Mapped[int | None] = mapped_column(nullable=True)
    decision_evidence_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # Durable external retry identity. These columns are nullable for legacy
    # and ordinary unkeyed rows; keyed admission writes the whole set in the
    # same transaction as the pending run.
    external_scope: Mapped[str | None] = mapped_column(String(96), nullable=True)
    external_key: Mapped[str | None] = mapped_column(String(320), nullable=True)
    request_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    request_digest_version: Mapped[str | None] = mapped_column(String(40), nullable=True)

    # Convenience fields (for listing pages without querying RunEventStore)
    message_count: Mapped[int] = mapped_column(default=0)
    first_human_message: Mapped[str | None] = mapped_column(Text)
    last_ai_message: Mapped[str | None] = mapped_column(Text)

    # Token usage (accumulated in-memory by RunJournal, written on run completion)
    total_input_tokens: Mapped[int] = mapped_column(default=0)
    total_output_tokens: Mapped[int] = mapped_column(default=0)
    total_tokens: Mapped[int] = mapped_column(default=0)
    llm_call_count: Mapped[int] = mapped_column(default=0)
    lead_agent_tokens: Mapped[int] = mapped_column(default=0)
    subagent_tokens: Mapped[int] = mapped_column(default=0)
    middleware_tokens: Mapped[int] = mapped_column(default=0)
    token_usage_by_model: Mapped[dict] = mapped_column(JSON, default=dict, server_default=text("'{}'"))

    # Follow-up association
    follow_up_to_run_id: Mapped[str | None] = mapped_column(String(64))

    # Multi-worker run ownership
    owner_worker_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # A non-owning worker records cancellation here; the owner consumes it
    # while renewing its lease. The first action wins.
    cancel_action: Mapped[str | None] = mapped_column(String(20), nullable=True)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC), onupdate=lambda: datetime.now(UTC))

    __table_args__ = (
        CheckConstraint(
            "(external_scope IS NULL) = (external_key IS NULL)",
            name="ck_runs_external_key_pair",
        ),
        CheckConstraint(
            "external_scope IS NULL OR (request_digest IS NOT NULL AND request_digest_version IS NOT NULL)",
            name="ck_runs_keyed_request_digest",
        ),
        CheckConstraint(
            "operation_kind = 'run' OR (external_scope IS NULL AND external_key IS NULL AND request_digest IS NULL AND request_digest_version IS NULL)",
            name="ck_runs_external_identity_run_only",
        ),
        CheckConstraint(
            "external_scope IS NULL OR length(external_scope) <= 96",
            name="ck_runs_external_scope_length",
        ),
        CheckConstraint(
            "external_key IS NULL OR length(external_key) <= 320",
            name="ck_runs_external_key_length",
        ),
        CheckConstraint(
            "request_digest IS NULL OR (length(request_digest) = 64 AND lower(request_digest) = request_digest "
            "AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace("
            "replace(replace(replace(replace(replace(replace(request_digest, '0', ''), '1', ''), '2', ''), '3', ''), "
            "'4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), "
            "'e', ''), 'f', '')) = 0)",
            name="ck_runs_request_digest_format",
        ),
        CheckConstraint(
            "request_digest_version IS NULL OR request_digest_version = 'sha256-canonical-json-v1'",
            name="ck_runs_request_digest_version_format",
        ),
        Index("ix_runs_thread_status", "thread_id", "status"),
        Index("ix_runs_lease", "lease_expires_at"),
        # Cross-process atomicity guarantee: at most one pending/running run per
        # thread. Must live in ORM ``__table_args__`` (not just the migration)
        # because the empty-DB bootstrap path runs ``create_all`` + ``stamp head``
        # and never executes the migration that also defines this index.
        Index(
            "uq_runs_thread_active",
            "thread_id",
            unique=True,
            sqlite_where=text("status IN ('pending', 'running')"),
            postgresql_where=text("status IN ('pending', 'running')"),
        ),
        Index(
            "uq_runs_external_identity",
            "external_scope",
            "external_key",
            unique=True,
            sqlite_where=text("external_scope IS NOT NULL AND external_key IS NOT NULL"),
            postgresql_where=text("external_scope IS NOT NULL AND external_key IS NOT NULL"),
        ),
    )
