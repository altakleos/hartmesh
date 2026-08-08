"""persist canonical caller intent for idempotent replay.

Revision ID: 0014_canonical_caller_intent
Revises: 0013_invocation_lifecycle
Create Date: 2026-08-07
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0014_canonical_caller_intent"
down_revision: str | Sequence[str] | None = "0013_invocation_lifecycle"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COLUMNS = (
    sa.Column("caller_intent_json", sa.JSON(none_as_null=True), nullable=True),
    sa.Column("caller_intent_digest", sa.String(length=64), nullable=True),
    sa.Column("caller_intent_digest_version", sa.String(length=40), nullable=True),
)

_CHECKS = (
    (
        "ck_runs_caller_intent_set",
        "(caller_intent_json IS NULL AND caller_intent_digest IS NULL AND caller_intent_digest_version IS NULL) OR (caller_intent_json IS NOT NULL AND caller_intent_digest IS NOT NULL AND caller_intent_digest_version IS NOT NULL)",
    ),
    (
        "ck_runs_caller_intent_run_only",
        "operation_kind = 'run' OR (caller_intent_json IS NULL AND caller_intent_digest IS NULL AND caller_intent_digest_version IS NULL)",
    ),
    (
        "ck_runs_caller_intent_digest_format",
        "caller_intent_digest IS NULL OR (length(caller_intent_digest) = 64 AND lower(caller_intent_digest) = caller_intent_digest "
        "AND length(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace("
        "replace(replace(replace(replace(replace(replace(caller_intent_digest, '0', ''), '1', ''), '2', ''), '3', ''), "
        "'4', ''), '5', ''), '6', ''), '7', ''), '8', ''), '9', ''), 'a', ''), 'b', ''), 'c', ''), 'd', ''), "
        "'e', ''), 'f', '')) = 0)",
    ),
    (
        "ck_runs_caller_intent_digest_version_format",
        "caller_intent_digest_version IS NULL OR caller_intent_digest_version = 'caller-intent-canonical-json-v1'",
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


def downgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_drop_column

    with op.batch_alter_table("runs") as batch_op:
        for name, _condition in reversed(_CHECKS):
            batch_op.drop_constraint(name, type_="check")
    for column in reversed(_COLUMNS):
        safe_drop_column("runs", column.name)
