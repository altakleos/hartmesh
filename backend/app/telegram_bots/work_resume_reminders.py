"""Saved-answer reminders qualify unchanged, unresumed information requests."""

from datetime import timedelta

from sqlalchemy import exists, select
from sqlalchemy.orm import aliased

from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_answer_news import AnswerNews
from app.telegram_bots.work_result_identity import digest
from deerflow.agent_instances.human_input import HumanInput
from deerflow.persistence.agent_instances.work import AgentWorkEventRow


class ResumeReminder(AnswerNews):
    def __init__(self, link, item):
        super().__init__(item)
        self.link = link

    def extend(self, query):
        result = super().extend(query)
        self.later_column = len(result.column_descriptions)
        later, event = aliased(AgentWorkEventRow), AgentWorkEventRow
        resumed = exists(select(later.id).where(later.work_id == event.work_id, later.revision > event.revision, later.action.in_(("activate", "stop", "edit", "cancel", "reopen"))).correlate(event))
        return result.add_columns(resumed)

    def qualify(self, rows, event, work, intent):
        signature = super().qualify(rows, event, work, intent)
        _receipt, request, response, unresolved = rows[0][self.column : self.column + 4]
        if response.actor_id != self.link.owner_user_id:
            raise ProductSourceRetired()
        if rows[0][self.later_column] or unresolved or request.state != "answered" or not HumanInput._source_current(request, work):
            raise ProductSourceRetired("resumed")
        self.due = aware(response.created_at) + timedelta(minutes=60)
        return digest([signature, self.due.isoformat()])
