"""Migration coverage for atomic idempotent ``runs`` admission."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
import sqlalchemy as sa
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

import deerflow.persistence.models  # noqa: F401
from deerflow.persistence.bootstrap import _get_alembic_config, bootstrap_schema
from deerflow.persistence.run.model import RunRow

_COLUMNS = {
    "external_scope",
    "external_key",
    "request_digest",
    "request_digest_version",
}
_CHECKS = {
    "ck_runs_external_key_pair",
    "ck_runs_keyed_request_digest",
    "ck_runs_external_identity_run_only",
    "ck_runs_external_scope_length",
    "ck_runs_external_key_length",
    "ck_runs_request_digest_format",
    "ck_runs_request_digest_version_format",
}
_INDEX = "uq_runs_external_identity"


def _columns(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[1] for row in connection.execute("PRAGMA table_info(runs)")}


def _indexes(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[1] for row in connection.execute("PRAGMA index_list(runs)")}


def _schema_sql(path: Path) -> str:
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='runs'").fetchone()
    assert row is not None
    return row[0]


def test_orm_metadata_declares_idempotency_checks_and_partial_unique_index() -> None:
    table = RunRow.__table__
    assert _COLUMNS <= set(table.columns.keys())
    assert _CHECKS <= {constraint.name for constraint in table.constraints}
    index = next(index for index in table.indexes if index.name == _INDEX)
    assert index.unique is True
    assert index.dialect_options["sqlite"]["where"] is not None
    assert index.dialect_options["postgresql"]["where"] is not None


@pytest.mark.asyncio
async def test_fresh_schema_has_idempotency_invariants(tmp_path: Path) -> None:
    path = tmp_path / "fresh.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    try:
        await bootstrap_schema(engine, backend="sqlite")
    finally:
        await engine.dispose()

    assert _COLUMNS <= _columns(path)
    assert _INDEX in _indexes(path)
    schema = _schema_sql(path)
    assert _CHECKS <= {name for name in _CHECKS if name in schema}

    with sqlite3.connect(path) as connection:
        base_values = "'r', 't', 'pending', 'run', 'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP"
        columns = (
            "run_id, thread_id, status, operation_kind, multitask_strategy, metadata_json, kwargs_json, "
            "message_count, total_input_tokens, total_output_tokens, total_tokens, llm_call_count, lead_agent_tokens, "
            "subagent_tokens, middleware_tokens, token_usage_by_model, created_at, updated_at"
        )
        connection.execute(f"INSERT INTO runs ({columns}) VALUES ({base_values})")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                f"INSERT INTO runs ({columns}, external_scope) VALUES ('bad-pair', 't2', 'success', 'run', 'reject', '{{}}', '{{}}', 0, 0, 0, 0, 0, 0, 0, 0, '{{}}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'http:v1:sha256:" + "a" * 64 + "')"
            )
        keyed_suffix = ", external_scope, external_key, request_digest, request_digest_version) VALUES (?, ?, 'success', 'run', 'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?)"
        keyed_insert = f"INSERT INTO runs ({columns}{keyed_suffix}"
        valid = (
            "keyed",
            "t3",
            "http:v1:sha256:" + "b" * 64,
            "raw:key",
            "c" * 64,
            "sha256-canonical-json-v1",
        )
        connection.execute(keyed_insert, valid)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                keyed_insert,
                (
                    "duplicate",
                    "t4",
                    *valid[2:],
                ),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                keyed_insert,
                (
                    "bad-digest",
                    "t5",
                    "http:v1:sha256:" + "d" * 64,
                    "raw:other",
                    "NOT-HEX",
                    "sha256-canonical-json-v1",
                ),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                f"INSERT INTO runs ({columns}, external_scope, external_key, request_digest, request_digest_version) "
                "VALUES ('aux-keyed', 't6', 'success', 'checkpoint_write', 'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', "
                "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?)",
                (
                    "http:v1:sha256:" + "e" * 64,
                    "raw:aux",
                    "f" * 64,
                    "sha256-canonical-json-v1",
                ),
            )


@pytest.mark.asyncio
async def test_upgrade_downgrade_and_reupgrade_preserve_legacy_rows(tmp_path: Path) -> None:
    path = tmp_path / "upgrade.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "0011_accepted_invocation")
    finally:
        await engine.dispose()

    sync = sa.create_engine(f"sqlite:///{path}")
    with sync.begin() as connection:
        connection.execute(
            sa.text(
                "INSERT INTO runs (run_id, thread_id, status, operation_kind, multitask_strategy, metadata_json, kwargs_json, "
                "message_count, total_input_tokens, total_output_tokens, total_tokens, llm_call_count, lead_agent_tokens, "
                "subagent_tokens, middleware_tokens, token_usage_by_model, created_at, updated_at) "
                "VALUES ('legacy', 'thread-1', 'success', 'run', 'reject', '{}', '{}', 0, 0, 0, 0, 0, 0, 0, 0, '{}', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
            )
        )
    sync.dispose()

    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "head")
        assert _COLUMNS <= _columns(path)
        assert _INDEX in _indexes(path)
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT external_scope, external_key, request_digest, request_digest_version FROM runs WHERE run_id='legacy'").fetchone() == (None, None, None, None)

        await asyncio.to_thread(command.downgrade, config, "0011_accepted_invocation")
        assert not (_COLUMNS & _columns(path))
        await asyncio.to_thread(command.upgrade, config, "head")
        assert _COLUMNS <= _columns(path)
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT COUNT(*) FROM runs WHERE run_id='legacy'").fetchone()[0] == 1
    finally:
        await engine.dispose()
