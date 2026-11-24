"""Record the role of the person whose Stop request won."""

import sqlalchemy as sa

from deerflow.persistence.migrations._helpers import safe_add_column, safe_drop_column

revision = "0057_run_cancel_actor_kind"
down_revision = "0056_upstream_adoption"
branch_labels = None
depends_on = None


def upgrade():
    safe_add_column("runs", sa.Column("cancel_actor_kind", sa.String(16), nullable=True))


def downgrade():
    safe_drop_column("runs", "cancel_actor_kind")
