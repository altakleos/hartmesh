"""PostgreSQL qualification for the durable invocation migration chain.

These tests deliberately use the repository's existing PostgreSQL service,
``postgres_contract`` marker, Alembic configuration helper, and runtime
repository. SQLite migration tests remain the fast local tier; they are not
evidence for PostgreSQL DDL, locking, or transaction behavior.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from support.postgres import postgres_async_url

import deerflow.persistence.models  # noqa: F401
from app.channels.inbound_receipts import (
    InboundReceiptCandidate,
    InboundReceiptEnvelope,
    SqlInboundReceiptStore,
)
from app.channels.message_bus import InboundMessage
from app.runtime.idempotency import CanonicalCallerIntent, canonical_request_digest, normalize_external_key, scope_for_http
from app.runtime.native_binding import (
    InternalVerifiedNativeBinding,
    InternalVerifiedNativeBindingKind,
)
from deerflow.persistence.bootstrap import _get_alembic_config, _get_head_revision
from deerflow.persistence.postgres_schema import build_asyncpg_connect_args
from deerflow.persistence.run.sql import RunRepository
from deerflow.runtime.runs.lifecycle_query import LifecycleQuery
from deerflow.runtime.runs.store.base import (
    AdmissionOutcome,
    CancellationRequestOutcome,
    LifecycleTransition,
    LifecycleType,
    lifecycle_owner_scope,
)

_POSTGRES_URL = os.environ.get("DEERFLOW_TEST_POSTGRES_URL")
_PRE_FEATURE_REVISION = "0011_mcp_tasks"
_INVOCATION_REVISIONS = (
    "0011_accepted_invocation",
    "0012_invocation_idempotency",
    "0013_invocation_lifecycle",
    "0014_canonical_caller_intent",
    "0015_inbound_receipts",
    "0016_sandbox_execution_evidence",
    "0017_lifecycle_integrity",
    "0018_inbound_receipt_failures",
    "0019_inbound_event_identity",
)
_REVISION_COLUMNS = {
    "0011_accepted_invocation": {
        "origin_json",
        "principal_projection_json",
        "principal_projection_digest",
        "base_origin_digest",
        "accepted_context_digest",
        "agent_revision_json",
        "agent_revision_digest",
        "extension_generation",
        "decision_evidence_json",
    },
    "0012_invocation_idempotency": {
        "external_scope",
        "external_key",
        "request_digest",
        "request_digest_version",
    },
    "0013_invocation_lifecycle": {"state_version"},
    "0014_canonical_caller_intent": {
        "caller_intent_json",
        "caller_intent_digest",
        "caller_intent_digest_version",
    },
    "0015_inbound_receipts": set(),
    "0016_sandbox_execution_evidence": {
        "execution_evidence_json",
        "execution_evidence_digest",
    },
    "0017_lifecycle_integrity": set(),
    "0018_inbound_receipt_failures": set(),
    "0019_inbound_event_identity": set(),
}

_INBOUND_RECEIPT_COLUMNS = {
    "receipt_id": ("character varying", 36, False),
    "provider": ("character varying", 64, False),
    "binding_kind": ("character varying", 32, False),
    "binding_reference": ("character varying", 320, False),
    "provider_delivery_id": ("character varying", 320, False),
    "thread_id": ("character varying", 64, False),
    "payload_json": ("json", None, False),
    "payload_digest": ("character varying", 64, False),
    "provider_event_digest": ("character varying", 64, True),
    "state": ("character varying", 16, False),
    "lease_owner": ("character varying", 96, True),
    "lease_expires_at": ("timestamp with time zone", None, True),
    "fencing_token": ("integer", None, False),
    "attempt_count": ("integer", None, False),
    "failure_count": ("integer", None, False),
    "next_attempt_at": ("timestamp with time zone", None, False),
    "run_id": ("character varying", 64, True),
    "outcome_code": ("character varying", 64, True),
    "received_at": ("timestamp with time zone", None, False),
    "updated_at": ("timestamp with time zone", None, False),
    "completed_at": ("timestamp with time zone", None, True),
}

_ACCEPTED_COLUMNS = {
    "origin_json": ("json", None, True),
    "principal_projection_json": ("json", None, True),
    "principal_projection_digest": ("character varying", 64, True),
    "base_origin_digest": ("character varying", 64, True),
    "accepted_context_digest": ("character varying", 64, True),
    "agent_revision_json": ("json", None, True),
    "agent_revision_digest": ("character varying", 64, True),
    "extension_generation": ("integer", None, True),
    "decision_evidence_json": ("json", None, True),
    "external_scope": ("character varying", 96, True),
    "external_key": ("character varying", 320, True),
    "request_digest": ("character varying", 64, True),
    "request_digest_version": ("character varying", 40, True),
    "state_version": ("integer", None, False),
    "caller_intent_json": ("json", None, True),
    "caller_intent_digest": ("character varying", 64, True),
    "caller_intent_digest_version": ("character varying", 40, True),
    "execution_evidence_json": ("json", None, True),
    "execution_evidence_digest": ("character varying", 64, True),
}
_RUN_CHECKS = {
    "ck_runs_state_version_nonnegative",
    "ck_runs_external_key_pair",
    "ck_runs_keyed_request_digest",
    "ck_runs_execution_evidence_pair",
    "ck_runs_execution_evidence_run_only",
    "ck_runs_external_identity_run_only",
    "ck_runs_external_scope_length",
    "ck_runs_external_key_length",
    "ck_runs_request_digest_format",
    "ck_runs_request_digest_version_format",
    "ck_runs_caller_intent_set",
    "ck_runs_caller_intent_run_only",
    "ck_runs_caller_intent_digest_format",
    "ck_runs_caller_intent_digest_version_format",
}
_LIFECYCLE_INDEXES = {
    "ix_run_lifecycle_events_run_cursor": ("run_id", "cursor"),
    "ix_run_lifecycle_events_thread_cursor": ("thread_id", "cursor"),
    "ix_run_lifecycle_events_owner_cursor": ("owner_scope", "cursor"),
}
_LIFECYCLE_EVENT_COLUMNS = {
    "event_id": ("character varying", 36, False),
    "cursor": ("bigint", None, False),
    "run_id": ("character varying", 64, False),
    "thread_id": ("character varying", 64, False),
    "owner_scope": ("character varying", 96, False),
    "lifecycle_type": ("character varying", 32, False),
    "state_version": ("integer", None, False),
    "status": ("character varying", 20, False),
    "created_at": ("timestamp with time zone", None, False),
    "payload_json": ("json", None, False),
}
_LIFECYCLE_CURSOR_COLUMNS = {
    "singleton_id": ("integer", None, False),
    "last_cursor": ("bigint", None, False),
    "pruned_through": ("bigint", None, False),
    "retained_count": ("bigint", None, False),
}
_LEGACY_COLUMNS = (
    "run_id",
    "thread_id",
    "assistant_id",
    "user_id",
    "status",
    "operation_kind",
    "model_name",
    "multitask_strategy",
    "metadata_json",
    "kwargs_json",
    "error",
    "stop_reason",
    "message_count",
    "first_human_message",
    "last_ai_message",
    "total_input_tokens",
    "total_output_tokens",
    "total_tokens",
    "llm_call_count",
    "lead_agent_tokens",
    "subagent_tokens",
    "middleware_tokens",
    "token_usage_by_model",
    "follow_up_to_run_id",
    "owner_worker_id",
    "lease_expires_at",
    "cancel_action",
    "cancel_requested_at",
    "created_at",
    "updated_at",
)

_LEGACY_TOKEN_USAGE_BY_MODEL = '{"legacy-model":{"input_tokens":11,"output_tokens":13}}'


def _legacy_run_insert_statement() -> sa.TextClause:
    return sa.text(
        """
        INSERT INTO runs (
            run_id, thread_id, assistant_id, user_id, status,
            operation_kind, model_name, multitask_strategy,
            metadata_json, kwargs_json, error, stop_reason,
            message_count, total_input_tokens, total_output_tokens,
            total_tokens, llm_call_count, lead_agent_tokens,
            subagent_tokens, middleware_tokens, token_usage_by_model,
            first_human_message, last_ai_message, follow_up_to_run_id,
            owner_worker_id, lease_expires_at, cancel_action,
            cancel_requested_at,
            created_at, updated_at
        ) VALUES (
            :run_id, :thread_id, :assistant_id, 'legacy-owner', :status,
            :operation_kind, :model_name, 'reject',
            CAST(:metadata AS json), CAST(:kwargs AS json), :error, :stop_reason,
            3, 11, 13, 24, 2, 17, 7, 0, CAST(:token_usage_by_model AS json),
            :first_human_message, :last_ai_message, :follow_up_to_run_id,
            :owner_worker_id, :lease_expires_at, 'interrupt',
            :cancel_requested_at,
            :created_at, :created_at
        )
        """
    )


def test_legacy_run_fixture_binds_json_as_one_value() -> None:
    """JSON punctuation must not be parsed as SQLAlchemy bind names."""

    parameters = _legacy_run_insert_statement().compile().params
    assert "token_usage_by_model" in parameters
    assert "11" not in parameters
    assert "13" not in parameters


def _revision_at_least(revision: str, introduced_at: str) -> bool:
    return _INVOCATION_REVISIONS.index(revision) >= _INVOCATION_REVISIONS.index(introduced_at)


def _lifecycle_cursor_columns_at(revision: str) -> dict[str, tuple[str, int | None, bool]]:
    columns = dict(_LIFECYCLE_CURSOR_COLUMNS)
    if not _revision_at_least(revision, "0017_lifecycle_integrity"):
        columns.pop("retained_count")
    return columns


def _inbound_receipt_columns_at(revision: str) -> dict[str, tuple[str, int | None, bool]]:
    columns = dict(_INBOUND_RECEIPT_COLUMNS)
    if not _revision_at_least(revision, "0018_inbound_receipt_failures"):
        columns.pop("failure_count")
    if not _revision_at_least(revision, "0019_inbound_event_identity"):
        columns.pop("provider_event_digest")
    return columns


def _legacy_null_predicate(revision: str) -> str | None:
    columns = _REVISION_COLUMNS[revision]
    if not columns:
        return None
    return " OR ".join(f"{name} IS NOT NULL" for name in sorted(columns))


@asynccontextmanager
async def _isolated_postgres_schema() -> AsyncIterator[tuple[str, AsyncEngine]]:
    assert _POSTGRES_URL is not None
    database_url = postgres_async_url(_POSTGRES_URL)
    admin_engine = create_async_engine(database_url)
    schema = f"invocation_migration_{uuid.uuid4().hex}"
    async with admin_engine.begin() as connection:
        await connection.execute(sa.text(f'CREATE SCHEMA "{schema}"'))

    engine = create_async_engine(
        database_url,
        connect_args=build_asyncpg_connect_args(schema),
    )
    try:
        yield schema, engine
    finally:
        await engine.dispose()
        async with admin_engine.begin() as connection:
            await connection.execute(sa.text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await admin_engine.dispose()


async def _upgrade(engine: AsyncEngine, schema: str, revision: str) -> None:
    config = _get_alembic_config(engine, postgres_schema=schema)
    await asyncio.to_thread(command.upgrade, config, revision)


async def _downgrade(engine: AsyncEngine, schema: str, revision: str) -> None:
    config = _get_alembic_config(engine, postgres_schema=schema)
    await asyncio.to_thread(command.downgrade, config, revision)


async def _column_contract(engine: AsyncEngine, schema: str, table: str) -> dict[str, tuple[str, int | None, bool, str | None]]:
    async with engine.connect() as connection:
        rows = (
            await connection.execute(
                sa.text(
                    """
                    SELECT column_name, data_type, character_maximum_length,
                           is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = :schema AND table_name = :table
                    """
                ),
                {"schema": schema, "table": table},
            )
        ).mappings()
        return {
            row["column_name"]: (
                row["data_type"],
                row["character_maximum_length"],
                row["is_nullable"] == "YES",
                row["column_default"],
            )
            for row in rows
        }


async def _schema_names(engine: AsyncEngine, schema: str) -> tuple[dict[str, set[str]], dict[str, str]]:
    async with engine.connect() as connection:
        constraint_rows = (
            await connection.execute(
                sa.text(
                    """
                    SELECT table_name, constraint_name
                    FROM information_schema.table_constraints
                    WHERE table_schema = :schema
                    """
                ),
                {"schema": schema},
            )
        ).all()
        index_rows = (
            await connection.execute(
                sa.text(
                    """
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE schemaname = :schema
                    """
                ),
                {"schema": schema},
            )
        ).all()

    constraints: dict[str, set[str]] = {}
    for table_name, constraint_name in constraint_rows:
        constraints.setdefault(table_name, set()).add(constraint_name)
    return constraints, dict(index_rows)


async def _constraint_definitions(engine: AsyncEngine, schema: str, table: str) -> dict[str, tuple[str, str]]:
    async with engine.connect() as connection:
        rows = (
            await connection.execute(
                sa.text(
                    """
                    SELECT constraint_name, constraint_type, pg_get_constraintdef(pg_constraint.oid)
                    FROM information_schema.table_constraints
                    JOIN pg_constraint ON pg_constraint.conname = constraint_name
                    JOIN pg_class ON pg_class.oid = pg_constraint.conrelid
                    JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace
                    WHERE table_schema = :schema
                      AND table_name = :table
                      AND pg_namespace.nspname = :schema
                    """
                ),
                {"schema": schema, "table": table},
            )
        ).all()
    return {name: (constraint_type, definition) for name, constraint_type, definition in rows}


def _normalized_ddl(value: str) -> str:
    return " ".join(value.lower().replace('"', "").split())


def _assert_index_definition(
    indexes: dict[str, str],
    name: str,
    columns: tuple[str, ...],
    *,
    unique: bool = False,
    predicate_terms: tuple[str, ...] = (),
) -> None:
    definition = _normalized_ddl(indexes[name])
    assert ("create unique index" in definition) is unique
    assert f"({', '.join(columns)})" in definition
    if predicate_terms:
        assert " where " in definition
        assert all(term in definition for term in predicate_terms)


async def _legacy_snapshot(engine: AsyncEngine) -> list[dict[str, object]]:
    async with engine.connect() as connection:
        rows = (await connection.execute(sa.text(f"SELECT {', '.join(_LEGACY_COLUMNS)} FROM runs WHERE run_id LIKE 'legacy-%' ORDER BY run_id"))).mappings()
        return [dict(row) for row in rows]


async def _mcp_task_snapshot(engine: AsyncEngine) -> dict[str, object]:
    async with engine.connect() as connection:
        row = (
            (
                await connection.execute(
                    sa.text(
                        """
                    SELECT id, user_id, thread_id, run_id, tool_call_id,
                           server_name, driver_name, remote_task_id, task_name,
                           status, result, error, input_required, driver_data,
                           notification_status, next_poll_at, last_polled_at,
                           last_poll_error, poll_attempt_count,
                           consecutive_poll_error_count, lease_owner,
                           lease_expires_at, cancel_requested_at, completed_at,
                           created_at, updated_at
                    FROM mcp_tasks
                    WHERE id = 'legacy-mcp-task'
                    """
                    )
                )
            )
            .mappings()
            .one()
        )
    return dict(row)


async def _assert_revision_columns(engine: AsyncEngine, schema: str, revision: str) -> None:
    columns = await _column_contract(engine, schema, "runs")
    for name in _REVISION_COLUMNS[revision]:
        assert columns[name][:3] == _ACCEPTED_COLUMNS[name]
    async with engine.connect() as connection:
        if revision == "0013_invocation_lifecycle":
            assert columns["state_version"][3] == "0"
            assert await connection.scalar(sa.text("SELECT count(*) FROM runs WHERE run_id LIKE 'legacy-%' AND state_version <> 0")) == 0
        elif introduced_values := _legacy_null_predicate(revision):
            assert await connection.scalar(sa.text(f"SELECT count(*) FROM runs WHERE run_id LIKE 'legacy-%' AND ({introduced_values})")) == 0

    if revision in {"0013_invocation_lifecycle", "0017_lifecycle_integrity"}:
        await _assert_lifecycle_ddl(engine, schema, revision=revision)
    elif revision in {"0015_inbound_receipts", "0018_inbound_receipt_failures", "0019_inbound_event_identity"}:
        await _assert_inbound_receipt_ddl(engine, schema, revision=revision)


async def _assert_idempotency_ddl(engine: AsyncEngine, schema: str) -> None:
    constraints, indexes = await _schema_names(engine, schema)
    definitions = await _constraint_definitions(engine, schema, "runs")
    idempotency_checks = {
        "ck_runs_external_key_pair",
        "ck_runs_keyed_request_digest",
        "ck_runs_external_identity_run_only",
        "ck_runs_external_scope_length",
        "ck_runs_external_key_length",
        "ck_runs_request_digest_format",
        "ck_runs_request_digest_version_format",
    }
    assert idempotency_checks <= constraints["runs"]
    assert all(definitions[name][0] == "CHECK" for name in idempotency_checks)
    _assert_index_definition(
        indexes,
        "uq_runs_external_identity",
        ("external_scope", "external_key"),
        unique=True,
        predicate_terms=("external_scope is not null", "external_key is not null"),
    )


async def _assert_lifecycle_ddl(engine: AsyncEngine, schema: str, *, revision: str = "0019_inbound_event_identity") -> None:
    cursor_columns = await _column_contract(engine, schema, "run_lifecycle_cursor_state")
    expected_cursor_columns = _lifecycle_cursor_columns_at(revision)
    for name, expected in expected_cursor_columns.items():
        assert cursor_columns[name][:3] == expected
    assert ("retained_count" in cursor_columns) is _revision_at_least(revision, "0017_lifecycle_integrity")
    assert cursor_columns["last_cursor"][3] == "0"
    assert cursor_columns["pruned_through"][3] == "0"

    event_columns = await _column_contract(engine, schema, "run_lifecycle_events")
    for name, expected in _LIFECYCLE_EVENT_COLUMNS.items():
        assert event_columns[name][:3] == expected
    assert event_columns["created_at"][3] is not None
    assert "now()" in event_columns["created_at"][3].lower()
    assert event_columns["payload_json"][3] is not None
    assert "{}" in event_columns["payload_json"][3]

    constraints, indexes = await _schema_names(engine, schema)
    cursor_definitions = await _constraint_definitions(engine, schema, "run_lifecycle_cursor_state")
    event_definitions = await _constraint_definitions(engine, schema, "run_lifecycle_events")
    cursor_checks = {
        "ck_run_lifecycle_cursor_singleton",
        "ck_run_lifecycle_cursor_nonnegative",
        "ck_run_lifecycle_pruned_range",
    }
    if _revision_at_least(revision, "0017_lifecycle_integrity"):
        cursor_checks.add("ck_run_lifecycle_retained_count_nonnegative")
    event_checks = {
        "ck_run_lifecycle_event_cursor_positive",
        "ck_run_lifecycle_event_version_positive",
        "ck_run_lifecycle_event_type",
    }
    assert cursor_checks <= constraints["run_lifecycle_cursor_state"]
    assert event_checks <= constraints["run_lifecycle_events"]
    assert all(cursor_definitions[name][0] == "CHECK" for name in cursor_checks)
    assert all(event_definitions[name][0] == "CHECK" for name in event_checks)
    assert _normalized_ddl(event_definitions["run_lifecycle_events_cursor_key"][1]) == "unique (cursor)"
    run_fk = event_definitions["run_lifecycle_events_run_id_fkey"]
    assert run_fk[0] == "FOREIGN KEY"
    normalized_fk = _normalized_ddl(run_fk[1])
    assert normalized_fk.startswith("foreign key (run_id) references ")
    assert "runs(run_id)" in normalized_fk
    assert normalized_fk.endswith("on delete cascade")
    for name, columns in _LIFECYCLE_INDEXES.items():
        _assert_index_definition(indexes, name, columns)

    async with engine.connect() as connection:
        singleton = (await connection.execute(sa.text(f"SELECT {', '.join(expected_cursor_columns)} FROM run_lifecycle_cursor_state"))).one()
    assert tuple(singleton) == tuple(1 if name == "singleton_id" else 0 for name in expected_cursor_columns)


async def _assert_inbound_receipt_ddl(engine: AsyncEngine, schema: str, *, revision: str = "0019_inbound_event_identity") -> None:
    columns = await _column_contract(engine, schema, "inbound_receipts")
    expected_columns = _inbound_receipt_columns_at(revision)
    for name, expected in expected_columns.items():
        assert columns[name][:3] == expected
    assert ("failure_count" in columns) is _revision_at_least(revision, "0018_inbound_receipt_failures")
    assert ("provider_event_digest" in columns) is _revision_at_least(revision, "0019_inbound_event_identity")
    constraints, indexes = await _schema_names(engine, schema)
    expected_constraints = {
        "ck_inbound_receipts_state",
        "ck_inbound_receipts_counters_nonnegative",
        "ck_inbound_receipts_claim_has_lease",
        "ck_inbound_receipts_admitted_has_run",
        "ck_inbound_receipts_identity_bounds",
        "ck_inbound_receipts_digest_format",
    }
    if _revision_at_least(revision, "0019_inbound_event_identity"):
        expected_constraints.add("ck_inbound_receipts_provider_event_digest_format")
    assert expected_constraints <= constraints["inbound_receipts"]
    assert ("ck_inbound_receipts_provider_event_digest_format" in constraints["inbound_receipts"]) is _revision_at_least(revision, "0019_inbound_event_identity")
    definitions = await _constraint_definitions(engine, schema, "inbound_receipts")
    counter_definition = _normalized_ddl(definitions["ck_inbound_receipts_counters_nonnegative"][1])
    assert ("failure_count" in counter_definition) is _revision_at_least(revision, "0018_inbound_receipt_failures")
    for name, index_columns in {
        "ix_inbound_receipts_due": (
            "state",
            "next_attempt_at",
            "received_at",
            "receipt_id",
        ),
        "ix_inbound_receipts_run_id": ("run_id",),
        "ix_inbound_receipts_completed_at": ("completed_at",),
    }.items():
        _assert_index_definition(indexes, name, index_columns)


async def _assert_postgres_head_contract(engine: AsyncEngine, schema: str) -> None:
    run_columns = await _column_contract(engine, schema, "runs")
    for column, (data_type, length, nullable) in _ACCEPTED_COLUMNS.items():
        assert run_columns[column][:3] == (data_type, length, nullable)
    assert run_columns["state_version"][3] == "0"

    constraints, indexes = await _schema_names(engine, schema)
    run_check_names = {name for name in constraints["runs"] if name.startswith("ck_runs_")}
    assert run_check_names == _RUN_CHECKS
    run_definitions = await _constraint_definitions(engine, schema, "runs")
    assert all(run_definitions[name][0] == "CHECK" for name in _RUN_CHECKS)
    _assert_index_definition(
        indexes,
        "uq_runs_thread_active",
        ("thread_id",),
        unique=True,
        predicate_terms=("pending", "running"),
    )
    await _assert_idempotency_ddl(engine, schema)
    await _assert_lifecycle_ddl(engine, schema)
    await _assert_inbound_receipt_ddl(engine, schema)


async def _assert_postgres_checks_reject_invalid_rows(engine: AsyncEngine) -> None:
    required_columns = (
        "run_id, thread_id, status, state_version, operation_kind, multitask_strategy, "
        "metadata_json, kwargs_json, message_count, total_input_tokens, "
        "total_output_tokens, total_tokens, llm_call_count, lead_agent_tokens, "
        "subagent_tokens, middleware_tokens, token_usage_by_model, created_at, updated_at"
    )
    required_values = ":run_id, :thread_id, 'success', :state_version, :operation_kind, 'reject', CAST('{}' AS json), CAST('{}' AS json), 0, 0, 0, 0, 0, 0, 0, 0, CAST('{}' AS json), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP"

    async def assert_rejected(
        case: str,
        *,
        state_version: int = 0,
        operation_kind: str = "run",
        extra_columns: str = "",
        extra_values: str = "",
        **values: object,
    ) -> None:
        columns = required_columns + (f", {extra_columns}" if extra_columns else "")
        sql_values = required_values + (f", {extra_values}" if extra_values else "")
        with pytest.raises(sa.exc.DBAPIError):
            async with engine.begin() as connection:
                await connection.execute(
                    sa.text(f"INSERT INTO runs ({columns}) VALUES ({sql_values})"),
                    {
                        "run_id": f"invalid-{case}",
                        "thread_id": f"thread-invalid-{case}",
                        "state_version": state_version,
                        "operation_kind": operation_kind,
                        **values,
                    },
                )

    scope = "http:v1:sha256:" + "a" * 64
    digest = "b" * 64
    caller_intent = '{"version":"caller-intent/v1","intent":{}}'
    await assert_rejected("negative-version", state_version=-1)
    await assert_rejected("key-pair", extra_columns="external_scope", extra_values=":external_scope", external_scope=scope)
    await assert_rejected("reverse-key-pair", extra_columns="external_key", extra_values=":external_key", external_key="raw:key-only")
    await assert_rejected(
        "keyed-without-digest",
        extra_columns="external_scope, external_key",
        extra_values=":external_scope, :external_key",
        external_scope=scope,
        external_key="raw:missing-digest",
    )
    await assert_rejected(
        "keyed-without-digest-value",
        extra_columns="external_scope, external_key, request_digest_version",
        extra_values=":external_scope, :external_key, 'sha256-canonical-json-v1'",
        external_scope=scope,
        external_key="raw:missing-digest-value",
    )
    await assert_rejected(
        "keyed-without-digest-version",
        extra_columns="external_scope, external_key, request_digest",
        extra_values=":external_scope, :external_key, :request_digest",
        external_scope=scope,
        external_key="raw:missing-digest-version",
        request_digest=digest,
    )
    await assert_rejected(
        "auxiliary-external-identity",
        operation_kind="checkpoint_write",
        extra_columns="external_scope, external_key, request_digest, request_digest_version",
        extra_values=":external_scope, :external_key, :request_digest, 'sha256-canonical-json-v1'",
        external_scope=scope,
        external_key="raw:auxiliary",
        request_digest=digest,
    )
    await assert_rejected(
        "auxiliary-request-digest",
        operation_kind="checkpoint_write",
        extra_columns="request_digest",
        extra_values=":request_digest",
        request_digest=digest,
    )
    await assert_rejected(
        "auxiliary-request-digest-version",
        operation_kind="checkpoint_write",
        extra_columns="request_digest_version",
        extra_values="'sha256-canonical-json-v1'",
    )
    await assert_rejected(
        "scope-length",
        extra_columns="external_scope, external_key, request_digest, request_digest_version",
        extra_values=":external_scope, :external_key, :request_digest, 'sha256-canonical-json-v1'",
        external_scope="s" * 97,
        external_key="raw:long-scope",
        request_digest=digest,
    )
    await assert_rejected(
        "key-length",
        extra_columns="external_scope, external_key, request_digest, request_digest_version",
        extra_values=":external_scope, :external_key, :request_digest, 'sha256-canonical-json-v1'",
        external_scope=scope,
        external_key="k" * 321,
        request_digest=digest,
    )
    await assert_rejected(
        "request-digest-format",
        extra_columns="external_scope, external_key, request_digest, request_digest_version",
        extra_values=":external_scope, :external_key, :request_digest, 'sha256-canonical-json-v1'",
        external_scope=scope,
        external_key="raw:bad-digest",
        request_digest="G" * 64,
    )
    await assert_rejected(
        "request-digest-uppercase",
        extra_columns="external_scope, external_key, request_digest, request_digest_version",
        extra_values=":external_scope, :external_key, :request_digest, 'sha256-canonical-json-v1'",
        external_scope=scope,
        external_key="raw:uppercase-digest",
        request_digest="A" * 64,
    )
    await assert_rejected(
        "request-digest-length",
        extra_columns="external_scope, external_key, request_digest, request_digest_version",
        extra_values=":external_scope, :external_key, :request_digest, 'sha256-canonical-json-v1'",
        external_scope=scope,
        external_key="raw:short-digest",
        request_digest="a" * 63,
    )
    await assert_rejected(
        "request-digest-version",
        extra_columns="external_scope, external_key, request_digest, request_digest_version",
        extra_values=":external_scope, :external_key, :request_digest, 'unknown-version'",
        external_scope=scope,
        external_key="raw:bad-version",
        request_digest=digest,
    )
    await assert_rejected(
        "partial-caller-intent",
        extra_columns="caller_intent_digest",
        extra_values=":caller_intent_digest",
        caller_intent_digest=digest,
    )
    await assert_rejected(
        "caller-intent-json-only",
        extra_columns="caller_intent_json",
        extra_values="CAST(:caller_intent_json AS json)",
        caller_intent_json=caller_intent,
    )
    await assert_rejected(
        "caller-intent-version-only",
        extra_columns="caller_intent_digest_version",
        extra_values="'caller-intent-canonical-json-v1'",
    )
    await assert_rejected(
        "caller-intent-without-version",
        extra_columns="caller_intent_json, caller_intent_digest",
        extra_values="CAST(:caller_intent_json AS json), :caller_intent_digest",
        caller_intent_json=caller_intent,
        caller_intent_digest=digest,
    )
    await assert_rejected(
        "caller-intent-without-digest",
        extra_columns="caller_intent_json, caller_intent_digest_version",
        extra_values="CAST(:caller_intent_json AS json), 'caller-intent-canonical-json-v1'",
        caller_intent_json=caller_intent,
    )
    await assert_rejected(
        "caller-intent-without-json",
        extra_columns="caller_intent_digest, caller_intent_digest_version",
        extra_values=":caller_intent_digest, 'caller-intent-canonical-json-v1'",
        caller_intent_digest=digest,
    )
    await assert_rejected(
        "auxiliary-caller-intent",
        operation_kind="checkpoint_write",
        extra_columns="caller_intent_json, caller_intent_digest, caller_intent_digest_version",
        extra_values="CAST(:caller_intent_json AS json), :caller_intent_digest, 'caller-intent-canonical-json-v1'",
        caller_intent_json=caller_intent,
        caller_intent_digest=digest,
    )
    await assert_rejected(
        "caller-intent-digest-format",
        extra_columns="caller_intent_json, caller_intent_digest, caller_intent_digest_version",
        extra_values="CAST(:caller_intent_json AS json), :caller_intent_digest, 'caller-intent-canonical-json-v1'",
        caller_intent_json=caller_intent,
        caller_intent_digest="G" * 64,
    )
    await assert_rejected(
        "caller-intent-digest-uppercase",
        extra_columns="caller_intent_json, caller_intent_digest, caller_intent_digest_version",
        extra_values="CAST(:caller_intent_json AS json), :caller_intent_digest, 'caller-intent-canonical-json-v1'",
        caller_intent_json=caller_intent,
        caller_intent_digest="A" * 64,
    )
    await assert_rejected(
        "caller-intent-digest-length",
        extra_columns="caller_intent_json, caller_intent_digest, caller_intent_digest_version",
        extra_values="CAST(:caller_intent_json AS json), :caller_intent_digest, 'caller-intent-canonical-json-v1'",
        caller_intent_json=caller_intent,
        caller_intent_digest="a" * 63,
    )
    await assert_rejected(
        "caller-intent-digest-version",
        extra_columns="caller_intent_json, caller_intent_digest, caller_intent_digest_version",
        extra_values="CAST(:caller_intent_json AS json), :caller_intent_digest, 'unknown-version'",
        caller_intent_json=caller_intent,
        caller_intent_digest=digest,
    )


async def _assert_lifecycle_constraints_reject_invalid_rows(engine: AsyncEngine) -> None:
    async def execute_rejected(statement: str, **values: object) -> None:
        with pytest.raises(sa.exc.DBAPIError):
            async with engine.begin() as connection:
                await connection.execute(sa.text(statement), values)

    await execute_rejected("INSERT INTO run_lifecycle_cursor_state (singleton_id, last_cursor, pruned_through) VALUES (2, 0, 0)")
    await execute_rejected("UPDATE run_lifecycle_cursor_state SET last_cursor = -1 WHERE singleton_id = 1")
    await execute_rejected("UPDATE run_lifecycle_cursor_state SET pruned_through = 1 WHERE singleton_id = 1")

    repository = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
    await repository.put("ddl-run", thread_id="thread-ddl", user_id="ddl-owner")
    event_values = "event_id, cursor, run_id, thread_id, owner_scope, lifecycle_type, state_version, status"
    await execute_rejected(f"INSERT INTO run_lifecycle_events ({event_values}) VALUES ('bad-cursor', 0, 'ddl-run', 'thread-ddl', 'user:ddl-owner', 'accepted', 1, 'pending')")
    await execute_rejected(f"INSERT INTO run_lifecycle_events ({event_values}) VALUES ('bad-version', 2, 'ddl-run', 'thread-ddl', 'user:ddl-owner', 'accepted', 0, 'pending')")
    await execute_rejected(f"INSERT INTO run_lifecycle_events ({event_values}) VALUES ('bad-type', 2, 'ddl-run', 'thread-ddl', 'user:ddl-owner', 'invented', 1, 'pending')")
    await execute_rejected(f"INSERT INTO run_lifecycle_events ({event_values}) VALUES ('bad-fk', 2, 'missing-run', 'thread-ddl', 'user:ddl-owner', 'accepted', 1, 'pending')")

    async with engine.begin() as connection:
        await connection.execute(sa.text(f"INSERT INTO run_lifecycle_events ({event_values}) VALUES ('valid-event', 2, 'ddl-run', 'thread-ddl', 'user:ddl-owner', 'started', 2, 'running')"))
        payload, created_at = (await connection.execute(sa.text("SELECT payload_json, created_at FROM run_lifecycle_events WHERE event_id = 'valid-event'"))).one()
        assert payload == {}
        assert created_at is not None

    await execute_rejected(f"INSERT INTO run_lifecycle_events ({event_values}) VALUES ('duplicate-cursor', 2, 'ddl-run', 'thread-ddl', 'user:ddl-owner', 'started', 2, 'running')")
    async with engine.begin() as connection:
        await connection.execute(sa.text("DELETE FROM runs WHERE run_id = 'ddl-run'"))
        assert await connection.scalar(sa.text("SELECT count(*) FROM run_lifecycle_events WHERE run_id = 'ddl-run'")) == 0


def _accepted_fields() -> dict[str, object]:
    return {
        "origin_json": {
            "version": 1,
            "source_kind": "http",
            "references": {"request_id": "qualified-request"},
        },
        "principal_projection_json": {
            "version": 2,
            "identity": {
                "effective_subject": {"kind": "user", "subject_id": "qualified-owner"},
                "acting_service": None,
            },
        },
        "principal_projection_digest": "1" * 64,
        "base_origin_digest": "2" * 64,
        "accepted_context_digest": "3" * 64,
        "agent_revision_json": {"version": 1, "agent_id": "lead-agent"},
        "agent_revision_digest": "4" * 64,
        "extension_generation": 7,
        "decision_evidence_json": {
            "version": 1,
            "decisions": [{"evidence_digest": "5" * 64}],
            "constraints": {"evidence_digest": "6" * 64},
            "capability_manifest": {"version": 1, "generation": 7, "digest": "7" * 64},
        },
    }


def test_ci_runs_the_mandatory_postgres_contract_gate_and_records_identity() -> None:
    workflow = (Path(__file__).resolve().parents[2] / ".github" / "workflows" / "backend-unit-tests.yml").read_text(encoding="utf-8")

    assert "Qualify PostgreSQL invocation persistence" in workflow
    assert "uv run pytest -m postgres_contract -v -s" in workflow
    assert workflow.count("DEERFLOW_TEST_POSTGRES_URL: ${{ env.TEST_POSTGRES_URI }}") >= 2


def test_invocation_migration_tail_starts_after_mcp_tasks() -> None:
    config = AlembicConfig()
    config.set_main_option(
        "script_location",
        str(Path(__file__).resolve().parents[1] / "packages" / "harness" / "deerflow" / "persistence" / "migrations"),
    )
    script = ScriptDirectory.from_config(config)

    accepted = script.get_revision("0011_accepted_invocation")

    assert accepted is not None
    assert accepted.down_revision == "0011_mcp_tasks"
    assert _PRE_FEATURE_REVISION == accepted.down_revision
    assert _INVOCATION_REVISIONS[0] == accepted.revision
    actual_tail = tuple(revision.revision for revision in reversed(list(script.iterate_revisions("head", _PRE_FEATURE_REVISION))))
    assert actual_tail == _INVOCATION_REVISIONS


def test_intermediate_revision_contracts_exclude_future_schema() -> None:
    assert "retained_count" not in _lifecycle_cursor_columns_at("0013_invocation_lifecycle")
    assert "retained_count" in _lifecycle_cursor_columns_at("0017_lifecycle_integrity")

    receipt_v1 = _inbound_receipt_columns_at("0015_inbound_receipts")
    receipt_failures = _inbound_receipt_columns_at("0018_inbound_receipt_failures")
    receipt_identity = _inbound_receipt_columns_at("0019_inbound_event_identity")
    assert "failure_count" not in receipt_v1
    assert "provider_event_digest" not in receipt_v1
    assert "failure_count" in receipt_failures
    assert "provider_event_digest" not in receipt_failures
    assert "provider_event_digest" in receipt_identity


@pytest.mark.parametrize(
    "revision",
    (
        "0015_inbound_receipts",
        "0017_lifecycle_integrity",
        "0018_inbound_receipt_failures",
        "0019_inbound_event_identity",
    ),
)
def test_revision_without_run_columns_has_no_empty_legacy_predicate(revision: str) -> None:
    assert _legacy_null_predicate(revision) is None


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for PostgreSQL migration qualification",
)
async def test_fresh_postgres_migration_chain_reaches_exact_head_schema() -> None:
    async with _isolated_postgres_schema() as (schema, engine):
        await _upgrade(engine, schema, "head")

        async with engine.connect() as connection:
            server_version = await connection.scalar(sa.text("SHOW server_version"))
            revision = await connection.scalar(sa.text("SELECT version_num FROM alembic_version"))
        print(f"PostgreSQL qualification: server_version={server_version} migration_head={revision}")

        assert revision == _get_head_revision() == _INVOCATION_REVISIONS[-1]
        await _assert_postgres_head_contract(engine, schema)
        await _assert_postgres_checks_reject_invalid_rows(engine)
        await _assert_lifecycle_constraints_reject_invalid_rows(engine)


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for PostgreSQL receipt arbitration",
)
async def test_postgres_inbound_receipt_acquisition_and_claim_are_atomic() -> None:
    async with _isolated_postgres_schema() as (schema, engine):
        await _upgrade(engine, schema, "head")
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        store = SqlInboundReceiptStore(sessions)
        envelope = InboundReceiptEnvelope.from_message(
            InboundMessage(
                channel_name="github",
                chat_id="hartmesh/runtime",
                user_id="octocat",
                text="Review this change",
                topic_id="17:reviewer",
                owner_user_id="owner-1",
                workspace_id="hartmesh/runtime",
                verified_source_binding=InternalVerifiedNativeBinding(
                    kind=InternalVerifiedNativeBindingKind.webhook_route,
                    reference="route:v1:sha256:" + ("a" * 64),
                ),
                metadata={
                    "message_id": "delivery-postgres:owner-1:reviewer",
                    "agent_name": "reviewer",
                    "preferred_thread_id": "thread-postgres-receipt",
                    "github": {
                        "repo": "hartmesh/runtime",
                        "number": 17,
                        "event": "pull_request",
                        "delivery_id": "delivery-postgres",
                        "installation_id": 42,
                        "recursion_limit": 100,
                        "thread_id": "thread-postgres-receipt",
                    },
                },
            )
        )

        first, second = await asyncio.gather(
            store.receive_batch(
                (
                    InboundReceiptCandidate(
                        envelope=envelope,
                        provider_event_digest="a" * 64,
                    ),
                )
            ),
            store.receive_batch(
                (
                    InboundReceiptCandidate(
                        envelope=envelope,
                        provider_event_digest="a" * 64,
                    ),
                )
            ),
        )
        assert first[0].receipt_id == second[0].receipt_id

        claims = await asyncio.gather(
            store.claim(first[0].receipt_id, lease_owner="worker-a", lease_seconds=30),
            store.claim(first[0].receipt_id, lease_owner="worker-b", lease_seconds=30),
        )
        assert sum(claim is not None for claim in claims) == 1


@pytest.mark.anyio
@pytest.mark.postgres_contract
@pytest.mark.skipif(
    not _POSTGRES_URL,
    reason="requires DEERFLOW_TEST_POSTGRES_URL for PostgreSQL migration qualification",
)
async def test_pre_feature_postgres_upgrade_downgrade_reupgrade_and_runtime_io() -> None:
    async with _isolated_postgres_schema() as (schema, engine):
        await _upgrade(engine, schema, _PRE_FEATURE_REVISION)
        legacy_created_at = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        legacy_lease_expires_at = datetime(2026, 1, 2, 4, 4, 5, tzinfo=UTC)
        legacy_cancel_requested_at = datetime(2026, 1, 2, 3, 14, 5, tzinfo=UTC)
        async with engine.begin() as connection:
            for run_id, operation_kind in (
                ("legacy-normal", "run"),
                ("legacy-auxiliary", "checkpoint_write"),
            ):
                await connection.execute(
                    _legacy_run_insert_statement(),
                    {
                        "run_id": run_id,
                        "thread_id": f"thread-{run_id}",
                        "assistant_id": f"assistant-{run_id}",
                        "status": "success" if operation_kind == "run" else "error",
                        "operation_kind": operation_kind,
                        "model_name": f"model-{operation_kind}",
                        "metadata": f'{{"legacy":true,"kind":"{operation_kind}"}}',
                        "kwargs": f'{{"legacy_option":"{operation_kind}"}}',
                        "token_usage_by_model": _LEGACY_TOKEN_USAGE_BY_MODEL,
                        "error": None if operation_kind == "run" else "legacy auxiliary error",
                        "stop_reason": None if operation_kind == "run" else "legacy_auxiliary",
                        "first_human_message": f"first-{run_id}",
                        "last_ai_message": f"last-{run_id}",
                        "follow_up_to_run_id": f"prior-{run_id}",
                        "owner_worker_id": f"worker-{run_id}",
                        "lease_expires_at": legacy_lease_expires_at,
                        "cancel_requested_at": legacy_cancel_requested_at,
                        "created_at": legacy_created_at,
                    },
                )
            await connection.execute(
                sa.text(
                    """
                    INSERT INTO mcp_tasks (
                        id, user_id, thread_id, run_id, tool_call_id,
                        server_name, driver_name, remote_task_id, task_name,
                        status, result, error, input_required, driver_data,
                        notification_status, next_poll_at, last_polled_at,
                        last_poll_error, poll_attempt_count,
                        consecutive_poll_error_count, lease_owner,
                        lease_expires_at, cancel_requested_at, completed_at,
                        created_at, updated_at
                    ) VALUES (
                        'legacy-mcp-task', 'legacy-owner', 'legacy-mcp-thread',
                        NULL, 'legacy-tool-call', 'legacy-server', 'legacy-driver',
                        'legacy-remote-task', 'Legacy MCP task', 'running',
                        CAST('{"progress":25}' AS json), NULL,
                        CAST('{"question":"continue?"}' AS json),
                        CAST('{"cursor":"opaque"}' AS json), 'pending',
                        :next_poll_at, :last_polled_at, NULL, 3, 1,
                        'legacy-mcp-worker', :lease_expires_at, NULL, NULL,
                        :created_at, :created_at
                    )
                    """
                ),
                {
                    "next_poll_at": legacy_created_at + timedelta(minutes=1),
                    "last_polled_at": legacy_created_at,
                    "lease_expires_at": legacy_lease_expires_at,
                    "created_at": legacy_created_at,
                },
            )

        legacy_before = await _legacy_snapshot(engine)
        mcp_task_before = await _mcp_task_snapshot(engine)
        assert len(legacy_before) == 2
        assert legacy_before[0]["operation_kind"] == "checkpoint_write"
        assert legacy_before[0]["status"] == "error"
        assert legacy_before[1]["operation_kind"] == "run"
        assert legacy_before[1]["status"] == "success"

        expected_columns: set[str] = set()
        for revision in _INVOCATION_REVISIONS:
            await _upgrade(engine, schema, revision)
            expected_columns.update(_REVISION_COLUMNS[revision])
            revision_columns = await _column_contract(engine, schema, "runs")
            assert expected_columns <= revision_columns.keys()
            await _assert_revision_columns(engine, schema, revision)
            assert await _legacy_snapshot(engine) == legacy_before
            assert await _mcp_task_snapshot(engine) == mcp_task_before
            async with engine.connect() as connection:
                assert await connection.scalar(sa.text("SELECT version_num FROM alembic_version")) == revision
                assert await connection.scalar(sa.text("SELECT count(*) FROM runs")) == 2
            if revision == "0012_invocation_idempotency":
                await _assert_idempotency_ddl(engine, schema)

        await _assert_postgres_head_contract(engine, schema)

        async with engine.connect() as connection:
            legacy_rows = (
                await connection.execute(
                    sa.text(
                        """
                        SELECT run_id, state_version, origin_json, external_scope,
                               request_digest, caller_intent_json
                        FROM runs
                        WHERE run_id LIKE 'legacy-%'
                        ORDER BY run_id
                        """
                    )
                )
            ).all()
            assert [tuple(row) for row in legacy_rows] == [
                ("legacy-auxiliary", 0, None, None, None, None),
                ("legacy-normal", 0, None, None, None, None),
            ]
            assert await connection.scalar(sa.text("SELECT count(*) FROM run_lifecycle_events")) == 0

        repository = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
        legacy = await repository.get("legacy-normal", user_id=None)
        auxiliary = await repository.get("legacy-auxiliary", user_id=None)
        assert legacy is not None and legacy["state_version"] == 0
        assert auxiliary is not None and auxiliary["operation_kind"] == "checkpoint_write"

        caller_intent = CanonicalCallerIntent({"input": {"messages": [{"role": "user", "content": "qualified"}]}})
        effective_digest = canonical_request_digest({"accepted": "qualified"})
        admission = await repository.ensure_run_atomic(
            "qualified-run",
            thread_id="thread-qualified",
            owner_worker_id="worker-qualified",
            lease_expires_at=None,
            external_scope=scope_for_http("user", "qualified-owner"),
            external_key=normalize_external_key("qualified-key"),
            request_digest=effective_digest,
            request_digest_version="sha256-canonical-json-v1",
            caller_intent_json=caller_intent.to_persisted(),
            caller_intent_digest=caller_intent.digest,
            caller_intent_digest_version=caller_intent.digest_version,
            user_id="qualified-owner",
            **_accepted_fields(),
        )
        response_loss_replay = await repository.ensure_run_atomic(
            "ignored-response-loss-run",
            thread_id="thread-qualified",
            owner_worker_id="worker-retry",
            lease_expires_at=None,
            external_scope=scope_for_http("user", "qualified-owner"),
            external_key=normalize_external_key("qualified-key"),
            request_digest=effective_digest,
            request_digest_version="sha256-canonical-json-v1",
            caller_intent_json=caller_intent.to_persisted(),
            caller_intent_digest=caller_intent.digest,
            caller_intent_digest_version=caller_intent.digest_version,
            user_id="qualified-owner",
            **_accepted_fields(),
        )
        changed_intent = CanonicalCallerIntent({"input": {"messages": [{"role": "user", "content": "changed"}]}})
        conflict = await repository.ensure_run_atomic(
            "ignored-conflict-run",
            thread_id="thread-qualified",
            owner_worker_id="worker-conflict",
            lease_expires_at=None,
            external_scope=scope_for_http("user", "qualified-owner"),
            external_key=normalize_external_key("qualified-key"),
            request_digest=canonical_request_digest({"accepted": "changed"}),
            request_digest_version="sha256-canonical-json-v1",
            caller_intent_json=changed_intent.to_persisted(),
            caller_intent_digest=changed_intent.digest,
            caller_intent_digest_version=changed_intent.digest_version,
            user_id="qualified-owner",
            **_accepted_fields(),
        )
        assert admission.outcome is AdmissionOutcome.created
        assert response_loss_replay.outcome is AdmissionOutcome.known_same
        assert response_loss_replay.row["run_id"] == "qualified-run"
        assert conflict.outcome is AdmissionOutcome.key_conflict
        assert conflict.row["run_id"] == "qualified-run"

        assert await repository.start_run("qualified-run") is True
        cancellation = await repository.request_cancel_fenced(
            "qualified-run",
            action="interrupt",
            expected_state_version=2,
        )
        assert cancellation.outcome is CancellationRequestOutcome.requested
        cancelled = await repository.transition_run_atomic(
            "qualified-run",
            expected_state_version=3,
            expected_statuses=("running",),
            transition=LifecycleTransition(
                lifecycle_type=LifecycleType.cancelled,
                status="interrupted",
            ),
        )
        assert cancelled.applied is True

        past_lease = (datetime.now(UTC) - timedelta(minutes=5)).isoformat()
        await repository.put(
            "orphan-run",
            thread_id="thread-orphan",
            user_id="qualified-owner",
            owner_worker_id="dead-worker",
            lease_expires_at=past_lease,
        )
        assert await repository.claim_for_takeover(
            "orphan-run",
            grace_seconds=0,
            error="worker disappeared",
            stop_reason="orphan_recovered",
            expected_state_version=1,
        )

        lifecycle = await repository.list_lifecycle_events(run_id="qualified-run")
        assert [event["lifecycle_type"] for event in lifecycle] == [
            LifecycleType.accepted,
            LifecycleType.started,
            LifecycleType.cancellation_requested,
            LifecycleType.cancelled,
        ]
        qualified = await repository.get("qualified-run", user_id=None)
        assert qualified is not None
        assert (qualified["status"], qualified["state_version"]) == ("interrupted", 4)
        assert qualified["origin_json"] == _accepted_fields()["origin_json"]
        assert qualified["principal_projection_json"] == _accepted_fields()["principal_projection_json"]
        assert qualified["agent_revision_json"] == _accepted_fields()["agent_revision_json"]
        assert qualified["decision_evidence_json"] == _accepted_fields()["decision_evidence_json"]
        assert qualified["external_key"] == normalize_external_key("qualified-key")
        assert qualified["caller_intent_json"] == caller_intent.to_persisted()
        assert qualified["caller_intent_digest"] == caller_intent.digest
        assert lifecycle[-1]["status"] == qualified["status"]
        assert lifecycle[-1]["state_version"] == qualified["state_version"]
        orphan = await repository.get("orphan-run", user_id=None)
        orphan_events = await repository.list_lifecycle_events(run_id="orphan-run")
        assert orphan is not None
        assert (orphan["status"], orphan["state_version"], orphan["stop_reason"]) == (
            "error",
            2,
            "orphan_recovered",
        )
        assert orphan_events[-1]["lifecycle_type"] is LifecycleType.failed
        assert orphan_events[-1]["payload"]["reason"] == "orphan_recovered"

        page = await repository.query_lifecycle(
            LifecycleQuery(
                run_id="qualified-run",
                owner_scope=lifecycle_owner_scope("qualified-owner"),
                limit=10,
            )
        )
        assert len(page.summaries) == 1
        summary = page.summaries[0]
        assert summary["source_kind"] == "http"
        assert summary["caller_intent_digest"] == caller_intent.digest
        assert summary["accepted_context_digest"] == "3" * 64
        assert summary["extension_manifest_digest"] == "7" * 64

        # Feature-tail downgrade is structurally supported back to the real
        # pre-invocation main-line schema. Invocation evidence and lifecycle
        # history are not representable there, while unrelated MCP task state
        # must survive unchanged.
        await _downgrade(engine, schema, _PRE_FEATURE_REVISION)
        downgraded_columns = await _column_contract(engine, schema, "runs")
        assert _ACCEPTED_COLUMNS.keys().isdisjoint(downgraded_columns)
        async with engine.connect() as connection:
            table_names = set(await connection.run_sync(lambda sync: sa.inspect(sync).get_table_names()))
            assert "run_lifecycle_events" not in table_names
            assert "run_lifecycle_cursor_state" not in table_names
            assert "mcp_tasks" in table_names
            assert await connection.scalar(sa.text("SELECT count(*) FROM runs")) == 4
        assert await _legacy_snapshot(engine) == legacy_before
        assert await _mcp_task_snapshot(engine) == mcp_task_before

        await _upgrade(engine, schema, "head")
        await _assert_postgres_head_contract(engine, schema)
        assert await _legacy_snapshot(engine) == legacy_before
        assert await _mcp_task_snapshot(engine) == mcp_task_before
        reupgraded = RunRepository(async_sessionmaker(engine, expire_on_commit=False))
        retained = await reupgraded.get("qualified-run", user_id=None)
        assert retained is not None
        assert retained["state_version"] == 0
        assert retained["origin_json"] is None
        assert retained["external_scope"] is None
        assert retained["caller_intent_json"] is None

        post_round_trip_intent = CanonicalCallerIntent({"input": {"messages": []}})
        post_round_trip = await reupgraded.ensure_run_atomic(
            "post-round-trip",
            thread_id="thread-post-round-trip",
            owner_worker_id="worker-post-round-trip",
            lease_expires_at=None,
            external_scope=scope_for_http("user", "qualified-owner"),
            external_key=normalize_external_key("post-round-trip-key"),
            request_digest=canonical_request_digest({"accepted": "post-round-trip"}),
            request_digest_version="sha256-canonical-json-v1",
            caller_intent_json=post_round_trip_intent.to_persisted(),
            caller_intent_digest=post_round_trip_intent.digest,
            caller_intent_digest_version=post_round_trip_intent.digest_version,
            user_id="qualified-owner",
            **_accepted_fields(),
        )
        assert post_round_trip.outcome is AdmissionOutcome.created
        assert post_round_trip.row["state_version"] == 1
        post_events = await reupgraded.list_lifecycle_events(run_id="post-round-trip")
        assert [(event["lifecycle_type"], event["state_version"]) for event in post_events] == [(LifecycleType.accepted, 1)]
