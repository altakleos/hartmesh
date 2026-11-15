"""The latest stored suggestion keeps its canonical nonhuman attempt provenance."""

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.producer import ProductSourceRetired
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow


class WorkSuggestionProjection:
    def __init__(self, work):
        self.work = work

    def extend(self, query):
        event, candidate, attempt = aliased(AgentWorkEventRow), aliased(AgentWorkEventRow), aliased(AgentWorkAttemptRow)
        latest = select(candidate.id).where(candidate.work_id == self.work["id"], candidate.action == "suggest", candidate.actor_kind == "nonhuman").order_by(candidate.revision.desc()).limit(1).scalar_subquery()
        self.column = len(query.column_descriptions)
        return query.add_columns(event, attempt).outerjoin(event, event.id == latest).outerjoin(attempt, and_(attempt.id == event.request["attempt_id"].as_string(), attempt.work_id == self.work["id"]))

    def check(self, rows):
        event, attempt = rows[0][self.column : self.column + 2]
        expected = None if event is None else {"id": event.id, "statement": event.request.get("statement")}
        instance = rows[0][1]
        if self.work.get("suggestion") != expected:
            raise ProductSourceRetired()
        if event is not None and (
            attempt is None
            or (event.instance_id, event.work_id, event.actor_kind, event.actor_id, event.action) != (instance.id, self.work["id"], "nonhuman", instance.principal_id, "suggest")
            or event.assignment_revision != attempt.assignment_revision
            or event.request.get("action") != "suggest"
            or event.result.get("id") != self.work["id"]
            or event.result.get("instance_id") != instance.id
        ):
            raise ProductSourceRetired()
