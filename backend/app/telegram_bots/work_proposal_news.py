"""A derived Work event names its source attempt without activating the child."""

from sqlalchemy import exists, select
from sqlalchemy.orm import aliased

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.result_notice_sources import fields
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_event_source import event_signature
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow


class ProposalNews:
    def __init__(self, link, item):
        self.link, self.item = link, item

    def extend(self, query):
        self.column = len(query.column_descriptions)
        source, child_attempt = aliased(AgentWorkRow), aliased(AgentWorkAttemptRow)
        started = exists(select(child_attempt.id).where(child_attempt.work_id == AgentWorkRow.id).correlate(AgentWorkRow))
        return query.add_columns(source, started).outerjoin(source, source.id == AgentWorkAttemptRow.work_id)

    def qualify(self, rows, event, work, instance, attempt, activation, intent):
        source, started = rows[0][self.column : self.column + 2]
        if (
            source is None
            or attempt is None
            or activation is None
            or source.id == work.id
            or source.instance_id != work.instance_id
            or not same_body(event.request, {"source_work_id": source.id, "attempt_id": attempt.id})
            or (event.actor_kind, event.actor_id, work.creator_id) != ("nonhuman", instance.principal_id, instance.principal_id)
            or (activation.action, activation.work_id, activation.instance_id, activation.operation_id) != ("activate", source.id, work.instance_id, attempt.activation_id)
            or (attempt.work_id, attempt.requester_id) != (source.id, self.link.owner_user_id)
            or event.revision != 1
            or event.assignment_revision != 1
            or started
            or work.status != "open"
            or not isinstance(work.objective, str)
            or not isinstance(work.success_criteria, str)
        ):
            raise ProductSourceRetired()
        # The source attempt may have settled or advanced; its identity and
        # requester remain the provenance. The child has no start in chat.
        signature = digest([event_signature(event, None, intent), fields(attempt, ("id", "work_id", "activation_id", "requester_id")), event_signature(activation, None, {})])
        return signature, {"objective_full": work.objective, "success_criteria": work.success_criteria}
