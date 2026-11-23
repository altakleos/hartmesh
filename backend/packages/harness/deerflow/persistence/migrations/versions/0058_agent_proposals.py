"""Retain reviewed revisions and exact lifecycle decision identities."""

from importlib import import_module

import sqlalchemy as sa
from alembic import op

revision = "0058_agent_proposals"
down_revision = "0057_run_cancel_actor_kind"
branch_labels = None
depends_on = None

_metadata = sa.MetaData()
sa.Table("agent_instances", _metadata, sa.Column("id", sa.String(32), primary_key=True))
sa.Table("agent_definition_revisions", _metadata, sa.Column("revision", sa.String(64), primary_key=True))
_table = sa.Table(
    "agent_configuration_proposals",
    _metadata,
    sa.Column("id", sa.String(32), primary_key=True, nullable=False),
    sa.Column("instance_id", sa.String(32), sa.ForeignKey("agent_instances.id"), nullable=False),
    sa.Column("author_id", sa.String(128), nullable=False),
    sa.Column("requester_id", sa.String(128), nullable=False),
    sa.Column("thread_id", sa.String(64), nullable=False),
    sa.Column("thread_incarnation", sa.String(64), nullable=False),
    sa.Column("run_id", sa.String(128), nullable=False),
    sa.Column("issuer_id", sa.String(256), nullable=False),
    sa.Column("call_id", sa.String(256), nullable=False),
    sa.Column("base_generation", sa.Integer, nullable=False),
    sa.Column("base_revision", sa.String(64), sa.ForeignKey("agent_definition_revisions.revision"), nullable=False),
    sa.Column("field", sa.String(64), nullable=False),
    sa.Column("proposed_value", sa.Boolean, nullable=False),
    sa.Column("candidate_digest", sa.String(64), nullable=False),
    sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
    sa.Column("decided_by", sa.String(128), nullable=True),
    sa.Column("operation_id", sa.String(32), nullable=True),
    sa.UniqueConstraint("instance_id", "thread_incarnation", "run_id", "issuer_id", "call_id", name="uq_agent_proposal_issuance"),
    sa.UniqueConstraint("instance_id", "operation_id", name="uq_agent_proposal_operation"),
    sa.Index("ix_agent_proposal_page", "instance_id", "created_at", "id"),
    sa.CheckConstraint("base_generation >= 1 AND base_generation <= 2147483647", name="ck_agent_proposal_generation"),
    sa.CheckConstraint("field = 'work_policy.enabled'", name="ck_agent_proposal_field"),
    sa.CheckConstraint("(decided_at IS NULL AND decided_by IS NULL AND operation_id IS NULL) OR (decided_at IS NOT NULL AND decided_by IS NOT NULL)", name="ck_agent_proposal_decision"),
)


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _table.name not in inspector.get_table_names():
        _table.create(bind)
        return
    import_module("deerflow.persistence.migrations.versions.0035_agent_conversations")._validate_existing(inspector, _table)
    indexes = [i for i in inspector.get_indexes(_table.name) if not i.get("duplicates_constraint")]
    if [(i["name"], i["column_names"], bool(i["unique"])) for i in indexes] != [("ix_agent_proposal_page", ["instance_id", "created_at", "id"], False)]:
        raise RuntimeError("Incompatible agent proposal indexes")


def downgrade():
    bind = op.get_bind()
    if _table.name not in sa.inspect(bind).get_table_names():
        return
    # Operators stop the new Gateway before rollback. Serialize the preflight
    # with any last in-flight writer before advancing the Alembic marker.
    if bind.dialect.name == "postgresql":
        bind.execute(sa.text("LOCK TABLE agent_lifecycle_operations, agent_configuration_proposals IN SHARE ROW EXCLUSIVE MODE"))
    elif bind.dialect.name == "sqlite":
        bind.execute(_table.update().where(sa.false()).values(id=_table.c.id))
    unresolved = bind.execute(
        sa.text("""
        SELECT 1 FROM agent_configuration_proposals p
        LEFT JOIN agent_lifecycle_operations l
          ON l.instance_id = p.instance_id AND l.operation_id = p.operation_id
        WHERE p.operation_id IS NOT NULL AND (l.phase IS NULL OR l.phase NOT IN ('complete','abandoned'))
        LIMIT 1
    """)
    ).first()
    if unresolved:
        raise RuntimeError("Resolve linked configuration proposal lifecycle operations before rollback")
    if bind.execute(sa.select(1).select_from(_table).limit(1)).first() is None:
        _table.drop(bind)
    # Historical proposals remain instance-owned audit records. The predecessor
    # ignores this additive table and a subsequent upgrade validates its shape.
