"""Canonical closure/routing evidence, independent of question actionability."""

from sqlalchemy import String, and_, cast, false
from sqlalchemy.orm import aliased

from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.settings import _snapshot, format_time
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_answer_news import AnswerNews
from app.telegram_bots.work_event_source import event_signature
from app.telegram_bots.work_result_identity import digest
from deerflow.agent_instances.human_input_contract import RequestCommand
from deerflow.agent_instances.work_execution_contract import ReportWork
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputRequestRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.persistence.user.model import UserRow

UPDATE_KEYS = {
    "request.answered_elsewhere": "human_response",
    "request.answered_by": "human_response",
    "request.rerouted": "request_route",
    "request.withdrawn": "request_withdraw",
    "request.assessed": "assess_input",
}


class QuestionUpdateProjection:
    def __init__(self, link, item, clock, *, source=None):
        self.link, self.item, self.clock, self.source = link, item, clock, source

    def extend(self, query):
        self.column = len(query.column_descriptions)
        work, event, delivery = AgentWorkRow, AgentWorkEventRow, TelegramOutboxRow
        receipt, request, response = HumanInputEventRow, HumanInputRequestRow, HumanInputResponseRow
        actor, recipient = aliased(UserRow), aliased(UserRow)
        assessment = aliased(HumanInputEventRow)
        return (
            query.add_columns(work, event, delivery, receipt, request, response, false(), actor.email, recipient.email, TelegramPersonSettingsRow, AgentWorkAttemptRow, assessment)
            .join(work, work.id == self.item.work_id)
            .join(event, event.id == self.item.event_id)
            .join(delivery, delivery.id == self.item.id)
            .outerjoin(receipt, and_(receipt.instance_id == event.instance_id, receipt.operation_id == event.operation_id))
            .join(request, request.id == self.item.request_id)
            .outerjoin(response, response.id == receipt.receipt["response_id"].as_string())
            .outerjoin(actor, actor.id == event.actor_id)
            .outerjoin(recipient, recipient.id == event.request["recipient_id"].as_string())
            .join(TelegramPersonSettingsRow, TelegramPersonSettingsRow.owner_user_id == TelegramBotLinkRow.owner_user_id)
            .outerjoin(AgentWorkAttemptRow, AgentWorkAttemptRow.id == event.request["attempt_id"].as_string())
            .outerjoin(assessment, and_(assessment.request_id == request.id, assessment.action == "assess", cast(assessment.revision - 1, String) == cast(event.request["request_basis"]["revision"].as_string(), String)))
        )

    def check(self, rows):
        instance, policy = rows[0][1], rows[0][3]
        work, event, delivery, receipt, request, response, _unused, actor, recipient, preferences, attempt, assessment = rows[0][self.column : self.column + 12]
        intent = self.item.payload.get("work_event_intent", {})
        key = intent.get("key")
        if (
            key not in UPDATE_KEYS
            or type(intent.get("version")) is not int
            or intent["version"] != 1
            or any(type(intent.get(field)) is not int for field in ("event_revision", "assignment_revision", "request_revision", "edit_request_revision"))
            or (event.action, event.instance_id, event.work_id, event.revision, event.assignment_revision) != (UPDATE_KEYS[key], instance.id, work.id, intent["event_revision"], intent["assignment_revision"])
            or (request.instance_id, request.work_id, request.assignment_revision) != (instance.id, work.id, event.assignment_revision)
            or work.instance_id != instance.id
            or (event.result.get("id"), event.result.get("instance_id"), event.result.get("creator_id")) != (work.id, instance.id, work.creator_id)
            or type(event.result.get("revision")) is not int
            or event.result["revision"] != event.revision
            or type(event.result.get("assignment_revision")) is not int
            or event.result["assignment_revision"] != event.assignment_revision
            or _item(delivery) != self.item
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.class_, delivery.dedupe_key)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, instance.id, "question", "edit:" + event.id)
            or delivery.silent is not True
        ):
            raise ProductSourceRetired()
        settings = _snapshot(preferences)
        if settings.locale != self.clock.settings.locale or settings.timezone != self.clock.settings.timezone:
            raise ProductSourceRetired()
        values = {"name": instance.name, "person": recipient if key == "request.rerouted" else actor}
        if event.action == "human_response":
            answer = AnswerNews(self.item)
            answer.column = self.column + 3
            signature = answer.qualify(rows, event, work, {**intent, "response_id": response.id if response else None})
            signature = digest([signature, aware(response.created_at).isoformat()])
            if (event.actor_id == self.link.owner_user_id) != (key == "request.answered_elsewhere") or intent["edit_request_revision"] != intent["request_revision"]:
                raise ProductSourceRetired()
            values["time"] = format_time(aware(response.created_at), settings=settings, now=self.clock.now(), zone=self.clock.zone)
        elif event.action == "assess_input":
            body = {name: value for name, value in event.request.items() if name != "attempt_id"}
            try:
                command = ReportWork.model_validate(body)
            except ValueError:
                raise ProductSourceRetired() from None
            if (
                not same_body(command.model_dump(mode="json", exclude_unset=True), body)
                or command.action != "assess_input"
                or command.operation_id != event.operation_id
                or command.expected_revision + 1 != event.revision
                or (event.actor_kind, event.actor_id) != ("nonhuman", instance.principal_id)
                or attempt is None
                or (attempt.id, attempt.work_id, attempt.assignment_revision) != (intent.get("attempt_id"), work.id, event.assignment_revision)
                or command.request_basis is None
                or command.request_basis.id != request.id
                or command.request_basis.request_revision != intent["edit_request_revision"]
                or intent["request_revision"] != intent["edit_request_revision"]
                or command.request_basis.revision >= request.revision
                or request.request_revision < command.request_basis.request_revision
                or receipt is not None
                or assessment is None
                or (request.purpose, request.state, request.closed_reason) != ("information", "closed", "resolved")
                or assessment.revision != command.request_basis.revision + 1
                or (assessment.instance_id, assessment.request_id, assessment.actor_kind, assessment.actor_id) != (instance.id, request.id, "nonhuman", instance.principal_id)
                or not same_body(assessment.request, command.model_dump(mode="json"))
                or not same_body(assessment.receipt, {"actor_kind": "nonhuman", "actor_id": instance.principal_id})
            ):
                raise ProductSourceRetired()
            signature = event_signature(event, assessment, intent)
        else:
            self._command(event, request, receipt, intent)
            signature = event_signature(event, receipt, intent)
        fresh = {
            "key": "stale.not_waiting" if key == "request.rerouted" and event.request.get("recipient_id") is None else key,
            "values": values,
            "event_signature": signature,
            "protected": instance.custody != "personal" and policy == "protected",
            "request_id": request.id,
            "work_id": work.id,
            "event_revision": event.revision,
            "request_revision": intent["edit_request_revision"],
            "assignment_revision": event.assignment_revision,
            "locale": settings.locale,
            "timezone": settings.timezone,
        }
        if self.source is not None and not same_body(self.source, fresh):
            raise ProductSourceRetired()
        self.source = fresh

    @staticmethod
    def _command(event, request, receipt, intent):
        try:
            command = RequestCommand.model_validate(event.request)
        except ValueError:
            raise ProductSourceRetired() from None
        revision = command.expected_request_revision + (command.action == "route")
        if (
            not same_body(command.model_dump(mode="json", exclude_unset=True), event.request)
            or receipt is None
            or not same_body(receipt.request, event.request)
            or event.action != "request_" + command.action
            or command.operation_id != event.operation_id
            or event.actor_kind != "human"
            or (receipt.instance_id, receipt.request_id, receipt.operation_id, receipt.actor_kind, receipt.actor_id, receipt.action, receipt.revision)
            != (event.instance_id, request.id, event.operation_id, "human", event.actor_id, command.action, command.expected_revision + 1)
            or (intent["edit_request_revision"], intent["request_revision"]) != (command.expected_request_revision, revision)
            or request.revision < receipt.revision
            or request.request_revision < revision
            or not same_body(
                receipt.receipt, {"operation_id": event.operation_id, "request_id": request.id, "revision": receipt.revision, "request_revision": revision, "actor_id": event.actor_id, "actor_kind": "human", "response_id": None}
            )
        ):
            raise ProductSourceRetired()
