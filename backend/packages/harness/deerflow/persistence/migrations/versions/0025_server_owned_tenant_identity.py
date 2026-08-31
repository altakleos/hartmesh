"""Bind durable application state to one server-owned tenant identity.

Revision ID: 0025_tenant_identity
Revises: 0024_tool_receipt_idempotency
Create Date: 2026-08-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0025_tenant_identity"
down_revision: str | Sequence[str] | None = "0024_tool_receipt_idempotency"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _create_binding_table() -> None:
    bind = op.get_bind()
    if "hartmesh_deployment_identity" in sa.inspect(bind).get_table_names():
        return
    op.create_table(
        "hartmesh_deployment_identity",
        sa.Column("singleton_key", sa.Integer(), nullable=False),
        sa.Column("identity_version", sa.Integer(), nullable=False),
        sa.Column("tenant_ref", sa.String(length=23), nullable=False),
        sa.Column("tenant_digest", sa.String(length=64), nullable=False),
        sa.Column("legacy_redis_prefixes_json", sa.JSON(), nullable=True),
        sa.Column("bound_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "singleton_key = 1",
            name="ck_hartmesh_deployment_identity_singleton",
        ),
        sa.CheckConstraint(
            "identity_version = 1",
            name="ck_hartmesh_deployment_identity_version",
        ),
        sa.PrimaryKeyConstraint("singleton_key"),
        sa.UniqueConstraint("tenant_digest"),
    )


def _add_tenant_columns(table_name: str) -> None:
    from deerflow.persistence.migrations._helpers import safe_add_column

    safe_add_column(
        table_name,
        sa.Column("tenant_ref", sa.String(length=23), nullable=True),
    )
    safe_add_column(
        table_name,
        sa.Column("tenant_digest", sa.String(length=64), nullable=True),
    )


def _add_pair_constraint(table_name: str, constraint_name: str) -> None:
    bind = op.get_bind()
    if table_name not in sa.inspect(bind).get_table_names():
        return
    existing = {constraint["name"] for constraint in sa.inspect(bind).get_check_constraints(table_name)}
    if constraint_name in existing:
        return
    with op.batch_alter_table(table_name) as batch_op:
        batch_op.create_check_constraint(
            constraint_name,
            "(tenant_ref IS NULL) = (tenant_digest IS NULL)",
        )


def _add_tenant_index(table_name: str, index_name: str) -> None:
    bind = op.get_bind()
    if table_name not in sa.inspect(bind).get_table_names():
        return
    existing = {index["name"] for index in sa.inspect(bind).get_indexes(table_name)}
    if index_name not in existing:
        op.create_index(index_name, table_name, ["tenant_digest"], unique=False)


def upgrade() -> None:
    _create_binding_table()
    for table_name, constraint_name, index_name in (
        ("runs", "ck_runs_tenant_pair", "ix_runs_tenant_digest"),
        (
            "run_lifecycle_events",
            "ck_run_lifecycle_event_tenant_pair",
            "ix_run_lifecycle_events_tenant_digest",
        ),
        ("run_events", "ck_run_events_tenant_pair", "ix_run_events_tenant_digest"),
    ):
        _add_tenant_columns(table_name)
        _add_pair_constraint(table_name, constraint_name)
        _add_tenant_index(table_name, index_name)


def downgrade() -> None:
    from deerflow.persistence.migrations._helpers import safe_drop_column

    bind = op.get_bind()
    for table_name, constraint_name, index_name in (
        ("run_events", "ck_run_events_tenant_pair", "ix_run_events_tenant_digest"),
        (
            "run_lifecycle_events",
            "ck_run_lifecycle_event_tenant_pair",
            "ix_run_lifecycle_events_tenant_digest",
        ),
        ("runs", "ck_runs_tenant_pair", "ix_runs_tenant_digest"),
    ):
        if table_name not in sa.inspect(bind).get_table_names():
            continue
        indexes = {index["name"] for index in sa.inspect(bind).get_indexes(table_name)}
        if index_name in indexes:
            op.drop_index(index_name, table_name=table_name)
        constraints = {constraint["name"] for constraint in sa.inspect(bind).get_check_constraints(table_name)}
        if constraint_name in constraints:
            with op.batch_alter_table(table_name) as batch_op:
                batch_op.drop_constraint(constraint_name, type_="check")
        safe_drop_column(table_name, "tenant_digest")
        safe_drop_column(table_name, "tenant_ref")
    if "hartmesh_deployment_identity" in sa.inspect(bind).get_table_names():
        op.drop_table("hartmesh_deployment_identity")
