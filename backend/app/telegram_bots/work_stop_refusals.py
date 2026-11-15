"""Conclusive Stop permission refusals retain every original command owner."""

from sqlalchemy import or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import thaw
from app.telegram_bots.work_execution_sources import act_facts
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_stop_retries import STOP_REPEATS, child_identity, parent_of
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


def permission_refusal(act):
    return act.kind == "stop" and act.state == "refused" and act.reason in {"denied", "ceiling", "grant-required", "not-visible", "agent-not-active"}


def refused_stop(act):
    scope = act.stop_dispatch
    return (
        permission_refusal(act)
        and parent_of(act) is None
        and act.attempts >= 0
        and act.cancel_requested_at is None
        and not act.operation_ids
        and not act.result
        and scope is not None
        and set(scope) == {"attempt_id", "thread_id", "run_id", "state", "attempts", "not_before"}
        and scope["state"] in {"awaiting_command", "ended"}
        and scope["attempts"] == 0
        and scope["not_before"] is None
        and scope["attempt_id"] == act.body["attempt_id"]
    )


def signature(act):
    return digest([act_facts(act), thaw(act.result)])


class StopRefusalProof:
    def __init__(self, act):
        self.identity, self.signature = act.id, signature(act)

    def extend(self, query):
        self.column = len(query.column_descriptions)
        children = [child_identity(self.identity, ordinal) for ordinal in range(2, STOP_REPEATS + 1)]
        ids = [self.identity, *children]
        child = aliased(TelegramActRow)
        return query.add_columns(
            select(child.id).where(or_(child.id.in_(children), child.continuation["stop_retry"]["parent_id"].as_string() == self.identity)).exists(),
            select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id.in_(ids)).exists(),
            select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id.in_(ids)).exists(),
        )

    def check(self, rows, act):
        if act.id != self.identity or signature(act) != self.signature or not refused_stop(act) or any(any(row[self.column : self.column + 3]) for row in rows):
            raise ProductSourceRetired()
