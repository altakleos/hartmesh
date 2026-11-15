"""Index cross-target Work operation evidence without changing any records.

Revision ID: 0053_work_operation_indexes
Revises: 0052_batch_continuation
"""

import sqlalchemy as sa
from alembic import op

revision = "0053_work_operation_indexes"
down_revision = "0052_batch_continuation"
branch_labels = None
depends_on = None

INDEXES = (
    ("agent_work_attempts", "ix_agent_work_attempt_activation_id", "activation_id"),
    ("agent_work_events", "ix_agent_work_event_operation_id", "operation_id"),
)


def upgrade():
    for table, name, column in INDEXES:
        indexes = {index["name"]: index for index in sa.inspect(op.get_bind()).get_indexes(table)}
        if name in indexes:
            if indexes[name]["column_names"] != [column] or indexes[name]["unique"]:
                raise RuntimeError(f"Unexpected index definition: {name}")
        else:
            op.create_index(name, table, [column])


def downgrade():
    for table, name, _column in reversed(INDEXES):
        if name in {index["name"] for index in sa.inspect(op.get_bind()).get_indexes(table)}:
            op.drop_index(name, table_name=table)
