"""SQLite migration coverage for durable agent assembly evidence."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config
from deerflow.persistence.run.model import RunRow

_REVISION = "0023_agent_assembly_evidence"
_PREVIOUS_REVISION = "0022_merge_scheduled_enqueue"
_COLUMNS = {"assembly_evidence_json", "assembly_evidence_digest"}
_CHECKS = {
    "ck_runs_assembly_evidence_pair",
    "ck_runs_assembly_evidence_run_only",
    "ck_runs_assembly_evidence_digest_format",
}


def _columns(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[1] for row in connection.execute("PRAGMA table_info(runs)")}


def _schema_sql(path: Path) -> str:
    with sqlite3.connect(path) as connection:
        row = connection.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='runs'").fetchone()
    assert row is not None
    return row[0]


def _insert_sql(*, run_id: str, operation_kind: str = "run", evidence: bool = False) -> tuple[str, tuple[object, ...]]:
    evidence_columns = ", assembly_evidence_json, assembly_evidence_digest" if evidence else ""
    evidence_values = ", ?, ?" if evidence else ""
    parameters: tuple[object, ...] = ()
    if evidence:
        parameters = (json.dumps({"version": 1}), "a" * 64)
    return (
        "INSERT INTO runs (run_id, thread_id, status, state_version, operation_kind, multitask_strategy, "
        "metadata_json, kwargs_json, message_count, total_input_tokens, total_output_tokens, total_tokens, "
        "llm_call_count, lead_agent_tokens, subagent_tokens, middleware_tokens, token_usage_by_model, "
        f"created_at, updated_at{evidence_columns}) VALUES ('{run_id}', 'thread-{run_id}', 'success', 1, "
        f"'{operation_kind}', 'reject', '{{}}', '{{}}', 0, 0, 0, 0, 0, 0, 0, 0, '{{}}', "
        f"CURRENT_TIMESTAMP, CURRENT_TIMESTAMP{evidence_values})",
        parameters,
    )


def test_orm_metadata_declares_assembly_evidence_columns_and_checks() -> None:
    table = RunRow.__table__
    assert _COLUMNS <= set(table.columns.keys())
    assert _CHECKS <= {constraint.name for constraint in table.constraints}


@pytest.mark.asyncio
async def test_sqlite_upgrade_preserves_legacy_rows_enforces_shape_and_downgrades(tmp_path: Path) -> None:
    path = tmp_path / "assembly-migration.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            statement, parameters = _insert_sql(run_id="legacy")
            connection.execute(statement, parameters)
            connection.commit()

        await asyncio.to_thread(command.upgrade, config, "head")
        assert _COLUMNS <= _columns(path)
        assert all(name in _schema_sql(path) for name in _CHECKS)
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT assembly_evidence_json, assembly_evidence_digest FROM runs WHERE run_id='legacy'").fetchone() == (None, None)

            valid_statement, valid_parameters = _insert_sql(run_id="valid", evidence=True)
            connection.execute(valid_statement, valid_parameters)
            with pytest.raises(sqlite3.IntegrityError):
                partial_statement, _ = _insert_sql(run_id="partial")
                connection.execute(
                    partial_statement.replace(
                        ") VALUES",
                        ", assembly_evidence_digest) VALUES",
                    ).replace(
                        ", CURRENT_TIMESTAMP)",
                        f", CURRENT_TIMESTAMP, '{'b' * 64}')",
                    )
                )
            with pytest.raises(sqlite3.IntegrityError):
                auxiliary_statement, auxiliary_parameters = _insert_sql(
                    run_id="auxiliary",
                    operation_kind="checkpoint_write",
                    evidence=True,
                )
                connection.execute(auxiliary_statement, auxiliary_parameters)
            with pytest.raises(sqlite3.IntegrityError):
                uppercase_statement, uppercase_parameters = _insert_sql(run_id="uppercase", evidence=True)
                connection.execute(
                    uppercase_statement,
                    (uppercase_parameters[0], "A" * 64),
                )
            connection.commit()

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
        assert not (_COLUMNS & _columns(path))
        await asyncio.to_thread(command.upgrade, config, _REVISION)
        assert _COLUMNS <= _columns(path)
    finally:
        await engine.dispose()
