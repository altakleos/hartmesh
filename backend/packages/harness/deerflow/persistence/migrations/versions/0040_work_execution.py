"""Work completion candidates and explicit human/AI employee attribution."""

import sqlalchemy as sa
from alembic import op

from deerflow.persistence.migrations._helpers import _normalize_default, safe_add_column, safe_drop_column

revision = "0040_work_execution"
down_revision = "0039_human_input"
branch_labels = None
depends_on = None


def columns():
    return [
        ("agent_work_attempts", sa.Column("candidate", sa.JSON(), nullable=True)),
        ("agent_work_attempts", sa.Column("settled_at", sa.DateTime(timezone=True), nullable=True)),
        ("human_input_requests", sa.Column("creator_kind", sa.VARCHAR(16), nullable=False, server_default=sa.text("'human'"))),
        ("human_input_events", sa.Column("actor_kind", sa.VARCHAR(16), nullable=False, server_default=sa.text("'human'"))),
    ]


def upgrade():
    # Fail before partial DDL when a manually installed column has incompatible shape.
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for table, column in columns():
        actual = next((c for c in inspector.get_columns(table) if c["name"] == column.name), None)
        if actual and (
            actual["nullable"] != column.nullable or actual["type"].compile(dialect=bind.dialect) != column.type.compile(dialect=bind.dialect) or _normalize_default(actual.get("default")) != _normalize_default(column.server_default)
        ):
            raise RuntimeError(f"Incompatible Work execution column: {table}.{column.name}")
    for table, column in columns():
        safe_add_column(table, column)


def downgrade():
    bind = op.get_bind()
    if bind.execute(sa.text("SELECT 1 FROM agent_work_attempts LIMIT 1")).first():
        raise RuntimeError("Refusing to erase used Work execution facts")
    for table, field in (("human_input_requests", "creator_kind"), ("human_input_events", "actor_kind")):
        if bind.execute(sa.text(f"SELECT 1 FROM {table} WHERE {field} != 'human' LIMIT 1")).first():
            raise RuntimeError("Refusing to erase AI employee attribution")
    for table, column in reversed(columns()):
        safe_drop_column(table, column.name)
