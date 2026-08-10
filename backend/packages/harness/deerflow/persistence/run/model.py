"""ORM model for run metadata."""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import JSON, BigInteger, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, event, inspect, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.schema import MetaData

from deerflow.persistence.base import Base


class RunRow(Base):
    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    thread_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    assistant_id: Mapped[str | None] = mapped_column(String(128))
    user_id: Mapped[str | None] = mapped_column(String(64), index=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    state_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default=text("0"))
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
    caller_intent_json: Mapped[dict | None] = mapped_column(JSON(none_as_null=True), nullable=True)
    caller_intent_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    caller_intent_digest_version: Mapped[str | None] = mapped_column(String(40), nullable=True)

    # Bounded proof of the exact sandbox materialization used when this row
    # atomically entered ``running``. Nullable for legacy/local executions.
    execution_evidence_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    execution_evidence_digest: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )

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
        CheckConstraint("state_version >= 0", name="ck_runs_state_version_nonnegative"),
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
            "(execution_evidence_json IS NULL) = (execution_evidence_digest IS NULL)",
            name="ck_runs_execution_evidence_pair",
        ),
        CheckConstraint(
            "execution_evidence_digest IS NULL OR (operation_kind = 'run' AND length(execution_evidence_digest) = 64)",
            name="ck_runs_execution_evidence_run_only",
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
        CheckConstraint(
            "(caller_intent_json IS NULL AND caller_intent_digest IS NULL AND caller_intent_digest_version IS NULL) OR (caller_intent_json IS NOT NULL AND caller_intent_digest IS NOT NULL AND caller_intent_digest_version IS NOT NULL)",
            name="ck_runs_caller_intent_set",
        ),
        CheckConstraint(
            "operation_kind = 'run' OR (caller_intent_json IS NULL AND caller_intent_digest IS NULL AND caller_intent_digest_version IS NULL)",
            name="ck_runs_caller_intent_run_only",
        ),
        CheckConstraint(
            "caller_intent_digest IS NULL OR (length(caller_intent_digest) = 64 AND lower(caller_intent_digest) = caller_intent_digest "
            "AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace("
            "replace(replace(replace(replace(replace(replace(caller_intent_digest, '0', ''), '1', ''), '2', ''), '3', ''), "
            "'4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), "
            "'e', ''), 'f', '')) = 0)",
            name="ck_runs_caller_intent_digest_format",
        ),
        CheckConstraint(
            "caller_intent_digest_version IS NULL OR caller_intent_digest_version = 'caller-intent-canonical-json-v1'",
            name="ck_runs_caller_intent_digest_version_format",
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


class RunLifecycleCursorStateRow(Base):
    __tablename__ = "run_lifecycle_cursor_state"

    singleton_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    last_cursor: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))
    pruned_through: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))
    retained_count: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0, server_default=text("0"))

    __table_args__ = (
        CheckConstraint("singleton_id = 1", name="ck_run_lifecycle_cursor_singleton"),
        CheckConstraint("last_cursor >= 0", name="ck_run_lifecycle_cursor_nonnegative"),
        CheckConstraint("pruned_through >= 0 AND pruned_through <= last_cursor", name="ck_run_lifecycle_pruned_range"),
        CheckConstraint(
            "retained_count >= 0",
            name="ck_run_lifecycle_retained_count_nonnegative",
        ),
    )


class RunLifecycleEventRow(Base):
    __tablename__ = "run_lifecycle_events"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    cursor: Mapped[int] = mapped_column(BigInteger, nullable=False, unique=True)
    run_id: Mapped[str] = mapped_column(String(64), ForeignKey("runs.run_id", ondelete="CASCADE"), nullable=False)
    thread_id: Mapped[str] = mapped_column(String(64), nullable=False)
    owner_scope: Mapped[str] = mapped_column(String(96), nullable=False)
    lifecycle_type: Mapped[str] = mapped_column(String(32), nullable=False)
    state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
        server_default=text("CURRENT_TIMESTAMP"),
    )
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict, server_default=text("'{}'"))

    __table_args__ = (
        CheckConstraint("cursor > 0", name="ck_run_lifecycle_event_cursor_positive"),
        CheckConstraint("state_version > 0", name="ck_run_lifecycle_event_version_positive"),
        CheckConstraint(
            "lifecycle_type IN ('accepted', 'started', 'cancellation_requested', 'cancelled', 'succeeded', 'failed', 'timed_out', 'interrupted')",
            name="ck_run_lifecycle_event_type",
        ),
        Index("ix_run_lifecycle_events_run_cursor", "run_id", "cursor"),
        Index("ix_run_lifecycle_events_thread_cursor", "thread_id", "cursor"),
        Index("ix_run_lifecycle_events_owner_cursor", "owner_scope", "cursor"),
    )


def _seed_lifecycle_cursor_after_create(
    _target: MetaData,
    connection: Connection,
    **_kwargs: object,
) -> None:
    """Seed fresh full schemas without repairing corrupt partial schemas."""

    requested_tables = _kwargs.get("tables")
    if requested_tables is not None:
        requested_names = {table.name for table in requested_tables}
        if not {"run_lifecycle_cursor_state", "run_lifecycle_events"} <= requested_names:
            # Metadata ``after_create`` also fires for partial create_all()
            # calls. Those callers do not own lifecycle DDL and must not
            # reinstall triggers on an already-versioned shared database.
            return
    tables = set(inspect(connection).get_table_names())
    if not {"run_lifecycle_cursor_state", "run_lifecycle_events"} <= tables:
        # Legacy bootstrap deliberately creates only the baseline table subset.
        return
    if connection.scalar(text("SELECT count(*) FROM run_lifecycle_events")):
        return
    connection.execute(text("INSERT INTO run_lifecycle_cursor_state (singleton_id, last_cursor, pruned_through, retained_count) SELECT 1, 0, 0, 0 WHERE NOT EXISTS (SELECT 1 FROM run_lifecycle_cursor_state WHERE singleton_id = 1)"))
    _install_lifecycle_integrity_triggers(connection)


def _install_lifecycle_integrity_triggers(connection: Connection) -> None:
    """Maintain an independent retained-row count in the ledger transaction."""

    if connection.dialect.name == "sqlite":
        connection.execute(
            text("CREATE TRIGGER IF NOT EXISTS trg_run_lifecycle_retained_insert AFTER INSERT ON run_lifecycle_events BEGIN UPDATE run_lifecycle_cursor_state SET retained_count = retained_count + 1 WHERE singleton_id = 1; END")
        )
        connection.execute(
            text("CREATE TRIGGER IF NOT EXISTS trg_run_lifecycle_retained_delete AFTER DELETE ON run_lifecycle_events BEGIN UPDATE run_lifecycle_cursor_state SET retained_count = retained_count - 1 WHERE singleton_id = 1; END")
        )
    elif connection.dialect.name == "postgresql":
        connection.execute(
            text(
                "CREATE OR REPLACE FUNCTION deerflow_update_lifecycle_retained_count() "
                "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                "IF TG_OP = 'INSERT' THEN "
                "UPDATE run_lifecycle_cursor_state SET retained_count = retained_count + 1 WHERE singleton_id = 1; "
                "RETURN NEW; "
                "ELSE "
                "UPDATE run_lifecycle_cursor_state SET retained_count = retained_count - 1 WHERE singleton_id = 1; "
                "RETURN OLD; "
                "END IF; END; $$"
            )
        )
        connection.execute(text("DROP TRIGGER IF EXISTS trg_run_lifecycle_retained_insert ON run_lifecycle_events"))
        connection.execute(text("CREATE TRIGGER trg_run_lifecycle_retained_insert AFTER INSERT ON run_lifecycle_events FOR EACH ROW EXECUTE FUNCTION deerflow_update_lifecycle_retained_count()"))
        connection.execute(text("DROP TRIGGER IF EXISTS trg_run_lifecycle_retained_delete ON run_lifecycle_events"))
        connection.execute(text("CREATE TRIGGER trg_run_lifecycle_retained_delete AFTER DELETE ON run_lifecycle_events FOR EACH ROW EXECUTE FUNCTION deerflow_update_lifecycle_retained_count()"))


# Empty-database bootstrap uses ``Base.metadata.create_all`` and stamps the
# Alembic head, so the singleton seed belongs to ORM metadata as well as the
# migration. Seed only after all full-schema tables exist, and never repair the
# ordering state when lifecycle evidence already exists.
event.listen(Base.metadata, "after_create", _seed_lifecycle_cursor_after_create)
