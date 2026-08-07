"""atomic idempotent run admission.

Revision ID: 0012_invocation_idempotency
Revises: 0011_accepted_invocation
Create Date: 2026-08-06
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0012_invocation_idempotency"
down_revision: str | Sequence[str] | None = "0011_accepted_invocation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = (
    sa.Column("external_scope", sa.String(length=96), nullable=True),
    sa.Column("external_key", sa.String(length=320), nullable=True),
    sa.Column("request_digest", sa.String(length=64), nullable=True),
    sa.Column("request_digest_version", sa.String(length=40), nullable=True),
)

_CHECKS = (
    ("ck_runs_external_key_pair", "(external_scope IS NULL) = (external_key IS NULL)"),
    (
        "ck_runs_keyed_request_digest",
        "external_scope IS NULL OR (request_digest IS NOT NULL AND request_digest_version IS NOT NULL)",
    ),
    (
        "ck_runs_external_identity_run_only",
        "operation_kind = 'run' OR (external_scope IS NULL AND external_key IS NULL AND request_digest IS NULL AND request_digest_version IS NULL)",
    ),
    ("ck_runs_external_scope_length", "external_scope IS NULL OR length(external_scope) <= 96"),
    ("ck_runs_external_key_length", "external_key IS NULL OR length(external_key) <= 320"),
    (
        "ck_runs_request_digest_format",
        "request_digest IS NULL OR (length(request_digest) = 64 AND lower(request_digest) = request_digest "
        "AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace("
        "replace(replace(replace(replace(replace(replace(request_digest, '0', ''), '1', ''), '2', ''), '3', ''), "
        "'4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), "
        "'e', ''), 'f', '')) = 0)",
    ),
    (
        "ck_runs_request_digest_version_format",
        "request_digest_version IS NULL OR request_digest_version = 'sha256-canonical-json-v1'",
    ),
)


def upgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    for column in _COLUMNS:
        safe_add_column("runs", column)

    bind = op.get_bind()
    existing_checks = {constraint["name"] for constraint in sa.inspect(bind).get_check_constraints("runs")}
    missing_checks = [(name, condition) for name, condition in _CHECKS if name not in existing_checks]
    if missing_checks:
        with op.batch_alter_table("runs") as batch_op:
            for name, condition in missing_checks:
                batch_op.create_check_constraint(name, condition)

    # ``create_all`` databases already contain metadata-owned indexes even when
    # an operator stamps an older revision before upgrading. Re-inspect after
    # SQLite's batch table rebuild, which preserves the existing index.
    existing_indexes = {index["name"] for index in sa.inspect(bind).get_indexes("runs")}
    if "uq_runs_external_identity" not in existing_indexes:
        op.create_index(
            "uq_runs_external_identity",
            "runs",
            ["external_scope", "external_key"],
            unique=True,
            sqlite_where=sa.text("external_scope IS NOT NULL AND external_key IS NOT NULL"),
            postgresql_where=sa.text("external_scope IS NOT NULL AND external_key IS NOT NULL"),
        )


def downgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_drop_column

    op.drop_index("uq_runs_external_identity", table_name="runs")
    with op.batch_alter_table("runs") as batch_op:
        for name, _condition in reversed(_CHECKS):
            batch_op.drop_constraint(name, type_="check")
    for column in reversed(_COLUMNS):
        safe_drop_column("runs", column.name)
