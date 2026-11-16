"""Join HartMesh Work indexes with adopted upstream schema.

Published ancestors retain their original definitions. This merge adds no DDL.
"""

revision = "0056_upstream_adoption"
down_revision = ("0053_work_operation_indexes", "0055_thread_shares")
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass
