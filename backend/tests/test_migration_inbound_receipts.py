"""Schema qualification for leased native-ingress receipts."""

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
from deerflow.persistence.inbound_receipt.model import InboundReceiptRow

_COLUMNS = {
    "receipt_id",
    "provider",
    "binding_kind",
    "binding_reference",
    "provider_delivery_id",
    "thread_id",
    "payload_json",
    "payload_digest",
    "provider_event_digest",
    "state",
    "lease_owner",
    "lease_expires_at",
    "fencing_token",
    "attempt_count",
    "failure_count",
    "next_attempt_at",
    "run_id",
    "outcome_code",
    "received_at",
    "updated_at",
    "completed_at",
}
_CHECKS = {
    "ck_inbound_receipts_state",
    "ck_inbound_receipts_counters_nonnegative",
    "ck_inbound_receipts_claim_has_lease",
    "ck_inbound_receipts_admitted_has_run",
    "ck_inbound_receipts_identity_bounds",
    "ck_inbound_receipts_digest_format",
    "ck_inbound_receipts_provider_event_digest_format",
}
_INDEXES = {
    "ix_inbound_receipts_due",
    "ix_inbound_receipts_run_id",
    "ix_inbound_receipts_completed_at",
}


def _table_columns(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {row[1] for row in connection.execute("PRAGMA table_info(inbound_receipts)")}


@pytest.mark.asyncio
async def test_fresh_schema_matches_inbound_receipt_metadata(tmp_path: Path) -> None:
    path = tmp_path / "fresh.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    try:
        await bootstrap_schema(engine, backend="sqlite")
    finally:
        await engine.dispose()

    assert _table_columns(path) == _COLUMNS
    sync = sa.create_engine(f"sqlite:///{path}")
    inspector = sa.inspect(sync)
    assert _CHECKS <= {item["name"] for item in inspector.get_check_constraints("inbound_receipts")}
    assert _INDEXES <= {item["name"] for item in inspector.get_indexes("inbound_receipts")}
    assert {constraint.name for constraint in InboundReceiptRow.__table__.constraints} >= _CHECKS
    sync.dispose()


@pytest.mark.asyncio
async def test_upgrade_downgrade_and_reupgrade_preserve_legacy_schema(tmp_path: Path) -> None:
    path = tmp_path / "upgrade.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "0014_canonical_caller_intent")
        assert not _table_columns(path)
        await asyncio.to_thread(command.upgrade, config, "head")
        assert _table_columns(path) == _COLUMNS

        sync = sa.create_engine(f"sqlite:///{path}")
        with sync.begin() as connection:
            connection.execute(
                sa.text(
                    "INSERT INTO inbound_receipts "
                    "(receipt_id, provider, binding_kind, binding_reference, provider_delivery_id, "
                    "thread_id, payload_json, payload_digest, state, fencing_token, attempt_count, failure_count) "
                    "VALUES ('receipt-1', 'github', 'webhook_route', 'route-1', 'delivery-1', "
                    "'thread-1', '{}', :digest, 'received', 0, 0, 0)"
                ),
                {"digest": "a" * 64},
            )
        sync.dispose()

        await asyncio.to_thread(command.downgrade, config, "0014_canonical_caller_intent")
        assert not _table_columns(path)
        await asyncio.to_thread(command.upgrade, config, "head")
        assert _table_columns(path) == _COLUMNS
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_event_identity_upgrade_preserves_legacy_receipt_without_invented_evidence(
    tmp_path: Path,
) -> None:
    path = tmp_path / "event-identity.db"
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    config = _get_alembic_config(engine)
    try:
        await asyncio.to_thread(command.upgrade, config, "0018_inbound_receipt_failures")
        sync = sa.create_engine(f"sqlite:///{path}")
        with sync.begin() as connection:
            connection.execute(
                sa.text(
                    "INSERT INTO inbound_receipts "
                    "(receipt_id, provider, binding_kind, binding_reference, provider_delivery_id, "
                    "thread_id, payload_json, payload_digest, state, fencing_token, attempt_count, failure_count) "
                    "VALUES ('legacy-receipt', 'github', 'webhook_route', 'route-1', "
                    "'delivery-1', 'thread-1', '{}', :digest, 'received', 0, 0, 0)"
                ),
                {"digest": "a" * 64},
            )
        sync.dispose()

        await asyncio.to_thread(command.upgrade, config, "head")
        sync = sa.create_engine(f"sqlite:///{path}")
        with sync.connect() as connection:
            assert connection.execute(sa.text("SELECT provider_event_digest FROM inbound_receipts WHERE receipt_id = 'legacy-receipt'")).scalar_one() is None
        sync.dispose()

        await asyncio.to_thread(command.downgrade, config, "0018_inbound_receipt_failures")
        sync = sa.create_engine(f"sqlite:///{path}")
        with sync.connect() as connection:
            assert connection.execute(sa.text("SELECT payload_digest FROM inbound_receipts WHERE receipt_id = 'legacy-receipt'")).scalar_one() == "a" * 64
        sync.dispose()

        await asyncio.to_thread(command.upgrade, config, "head")
        assert _table_columns(path) == _COLUMNS
    finally:
        await engine.dispose()
