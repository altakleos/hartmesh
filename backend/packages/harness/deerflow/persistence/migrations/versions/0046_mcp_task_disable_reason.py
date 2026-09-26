"""A durable MCP task can be cancelled because its owner was turned off.

Revision ID: 0046_mcp_task_disable_reason
Revises: 0045_refusal_sweep_reach
Create Date: 2026-09-26

alembic_version.version_num is VARCHAR(32); revision ids in this chain must
stay at or under that length or stamping/upgrading a database fails outright.

A task's cancellation records who asked and why (``cancel_actor_ref``,
``cancel_reason_code``). ``accounts disable`` cancels a turned-off person's
tasks as the deployer, so the reason codes gain ``account_disabled``; filing
it under ``user_api`` would say the person asked. The downgrade refuses while
any task carries the new reason, the way 0028's does for its rows, rather
than rewrite whose request it was.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0046_mcp_task_disable_reason"
down_revision: str | Sequence[str] | None = "0045_refusal_sweep_reach"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CONSTRAINT = "ck_mcp_tasks_cancel_intent_shape"
_SHAPE = "(cancel_actor_ref IS NULL OR length(cancel_actor_ref) = 64) AND (cancel_reason_code IS NULL OR cancel_reason_code IN ({codes}))"
_BEFORE = "'user_api', 'agent_tool'"
_AFTER = "'user_api', 'agent_tool', 'account_disabled'"


def _replace_shape(codes: str) -> None:
    bind = op.get_bind()
    if "mcp_tasks" not in sa.inspect(bind).get_table_names():
        return
    checks = {item["name"] for item in sa.inspect(bind).get_check_constraints("mcp_tasks") if isinstance(item.get("name"), str)}
    with op.batch_alter_table("mcp_tasks") as batch_op:
        if _CONSTRAINT in checks:
            batch_op.drop_constraint(_CONSTRAINT, type_="check")
        batch_op.create_check_constraint(_CONSTRAINT, _SHAPE.format(codes=codes))


def upgrade() -> None:
    _replace_shape(_AFTER)


def downgrade() -> None:
    bind = op.get_bind()
    if "mcp_tasks" in sa.inspect(bind).get_table_names():
        carried = bind.execute(sa.text("SELECT COUNT(*) FROM mcp_tasks WHERE cancel_reason_code = 'account_disabled'")).scalar_one()
        if carried:
            raise RuntimeError("mcp_task_disable_reason_rollback_blocked")
    _replace_shape(_BEFORE)
