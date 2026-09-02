from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
from alembic import command
from sqlalchemy.ext.asyncio import create_async_engine

from deerflow.persistence.bootstrap import _get_alembic_config

_REVISION = "0030_run_delivery_owner_backfill"
_PREVIOUS_REVISION = "0029_run_recovery_policy"


def _insert_run(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    thread_id: str,
    user_id: str | None,
    tenant_digest: str | None,
) -> None:
    tenant_ref = None if tenant_digest is None else f"tenant-{tenant_digest[:16]}"
    connection.execute(
        """
        INSERT INTO runs (
            run_id, thread_id, user_id, status, state_version,
            operation_kind, multitask_strategy, metadata_json, kwargs_json,
            message_count, total_input_tokens, total_output_tokens,
            total_tokens, llm_call_count, lead_agent_tokens,
            subagent_tokens, middleware_tokens, token_usage_by_model,
            tenant_ref, tenant_digest, created_at, updated_at
        ) VALUES (
            ?, ?, ?, 'success', 1, 'run', 'reject', '{}', '{}',
            0, 0, 0, 0, 0, 0, 0, 0, '{}', ?, ?,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        )
        """,
        (run_id, thread_id, user_id, tenant_ref, tenant_digest),
    )


def _insert_event(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    thread_id: str,
    user_id: str | None,
    tenant_digest: str | None,
    event_type: str = "run.delivery",
    seq: int,
) -> None:
    tenant_ref = None if tenant_digest is None else f"tenant-{tenant_digest[:16]}"
    connection.execute(
        """
        INSERT INTO run_events (
            thread_id, run_id, user_id, tenant_ref, tenant_digest,
            event_type, idempotency_key, category, content,
            event_metadata, seq, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, 'system', '', '{}', ?, CURRENT_TIMESTAMP)
        """,
        (
            thread_id,
            run_id,
            user_id,
            tenant_ref,
            tenant_digest,
            event_type,
            f"receipt-{thread_id}-{seq}",
            seq,
        ),
    )


@pytest.mark.asyncio
async def test_upgrade_backfills_only_authoritatively_matched_delivery_owners(
    tmp_path: Path,
) -> None:
    path = tmp_path / "run-delivery-owner-backfill.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    tenant_a = "a" * 64
    tenant_b = "b" * 64
    try:
        await asyncio.to_thread(command.upgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            _insert_run(
                connection,
                run_id="matched-tenant",
                thread_id="thread-matched-tenant",
                user_id="owner-a",
                tenant_digest=tenant_a,
            )
            _insert_run(
                connection,
                run_id="matched-null-tenant",
                thread_id="thread-matched-null-tenant",
                user_id="owner-null-tenant",
                tenant_digest=None,
            )
            _insert_run(
                connection,
                run_id="existing-owner",
                thread_id="thread-existing-owner",
                user_id="authoritative-owner",
                tenant_digest=tenant_a,
            )
            _insert_run(
                connection,
                run_id="non-delivery",
                thread_id="thread-non-delivery",
                user_id="owner-non-delivery",
                tenant_digest=tenant_a,
            )
            _insert_run(
                connection,
                run_id="wrong-thread",
                thread_id="authoritative-thread",
                user_id="owner-wrong-thread",
                tenant_digest=tenant_a,
            )
            _insert_run(
                connection,
                run_id="wrong-tenant",
                thread_id="thread-wrong-tenant",
                user_id="owner-wrong-tenant",
                tenant_digest=tenant_a,
            )
            _insert_run(
                connection,
                run_id="ownerless-run",
                thread_id="thread-ownerless-run",
                user_id=None,
                tenant_digest=tenant_a,
            )

            _insert_event(
                connection,
                run_id="matched-tenant",
                thread_id="thread-matched-tenant",
                user_id=None,
                tenant_digest=tenant_a,
                seq=1,
            )
            _insert_event(
                connection,
                run_id="matched-null-tenant",
                thread_id="thread-matched-null-tenant",
                user_id=None,
                tenant_digest=None,
                seq=1,
            )
            _insert_event(
                connection,
                run_id="existing-owner",
                thread_id="thread-existing-owner",
                user_id="preserved-owner",
                tenant_digest=tenant_a,
                seq=1,
            )
            _insert_event(
                connection,
                run_id="non-delivery",
                thread_id="thread-non-delivery",
                user_id=None,
                tenant_digest=tenant_a,
                event_type="run.error",
                seq=1,
            )
            _insert_event(
                connection,
                run_id="missing-run",
                thread_id="thread-missing-run",
                user_id=None,
                tenant_digest=tenant_a,
                seq=1,
            )
            _insert_event(
                connection,
                run_id="wrong-thread",
                thread_id="different-thread",
                user_id=None,
                tenant_digest=tenant_a,
                seq=1,
            )
            _insert_event(
                connection,
                run_id="wrong-tenant",
                thread_id="thread-wrong-tenant",
                user_id=None,
                tenant_digest=tenant_b,
                seq=1,
            )
            _insert_event(
                connection,
                run_id="ownerless-run",
                thread_id="thread-ownerless-run",
                user_id=None,
                tenant_digest=tenant_a,
                seq=1,
            )
            connection.commit()

        await asyncio.to_thread(command.upgrade, config, _REVISION)
        with sqlite3.connect(path) as connection:
            rows = dict(connection.execute("SELECT run_id, user_id FROM run_events ORDER BY id").fetchall())
            assert rows == {
                "matched-tenant": "owner-a",
                "matched-null-tenant": "owner-null-tenant",
                "existing-owner": "preserved-owner",
                "non-delivery": None,
                "missing-run": None,
                "wrong-thread": None,
                "wrong-tenant": None,
                "ownerless-run": None,
            }

        await asyncio.to_thread(command.downgrade, config, _PREVIOUS_REVISION)
        with sqlite3.connect(path) as connection:
            assert connection.execute("SELECT user_id FROM run_events WHERE run_id='matched-tenant'").fetchone() == ("owner-a",)
            assert connection.execute("SELECT version_num FROM alembic_version").fetchone() == (_PREVIOUS_REVISION,)
    finally:
        await engine.dispose()
