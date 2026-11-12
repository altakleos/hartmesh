"""Information-answer provenance joins the exact receipt and response."""

from sqlalchemy import and_, exists

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.result_notice_sources import fields
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_event_source import event_signature
from app.telegram_bots.work_result_identity import digest
from deerflow.agent_instances.human_input_contract import Respond
from deerflow.agent_instances.work import UNRESOLVED
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputRequestRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow


class AnswerNews:
    def __init__(self, item):
        self.item = item

    def extend(self, query):
        self.column = len(query.column_descriptions)
        receipt, request, response, event = HumanInputEventRow, HumanInputRequestRow, HumanInputResponseRow, AgentWorkEventRow
        return (
            query.add_columns(receipt, request, response, exists().where(AgentWorkAttemptRow.work_id == event.work_id, AgentWorkAttemptRow.status.in_(UNRESOLVED)).correlate(event))
            .outerjoin(receipt, and_(receipt.instance_id == event.instance_id, receipt.operation_id == event.operation_id))
            .outerjoin(request, request.id == receipt.request_id)
            .outerjoin(response, response.id == receipt.receipt["response_id"].as_string())
        )

    def qualify(self, rows, event, work, intent):
        receipt, request, response, unresolved = rows[0][self.column : self.column + 4]
        self.open_assignment = not unresolved
        try:
            command = Respond.model_validate(event.request)
            if (
                receipt is None
                or request is None
                or response is None
                or not same_body(command.model_dump(mode="json", exclude_unset=True), event.request)
                or not same_body(receipt.request, event.request)
                or event.action != "human_response"
                or event.actor_kind != "human"
                or command.operation_id != event.operation_id
                or command.disposition != "supplied"
                or (receipt.instance_id, receipt.operation_id, receipt.action, receipt.actor_kind, receipt.actor_id) != (event.instance_id, event.operation_id, "respond", "human", event.actor_id)
                or (request.id, request.instance_id, request.work_id, request.purpose, request.assignment_revision) != (self.item.request_id, event.instance_id, work.id, "information", event.assignment_revision)
                or receipt.request_id != request.id
                or (response.id, response.request_id, response.actor_id, response.request_revision, response.assignment_revision, response.disposition)
                != (intent.get("response_id"), request.id, event.actor_id, command.expected_request_revision, command.expected_assignment_revision, "supplied")
                or type(intent.get("request_revision")) is not int
                or intent["request_revision"] != response.request_revision
                or response.assignment_revision != event.assignment_revision
                or request.request_revision < response.request_revision
                or request.revision < receipt.revision
                or not same_body(
                    receipt.receipt,
                    {"operation_id": event.operation_id, "request_id": request.id, "revision": receipt.revision, "request_revision": response.request_revision, "actor_id": event.actor_id, "actor_kind": "human", "response_id": response.id},
                )
                or not same_body([response.text, response.choice, response.sources], [command.text, command.choice, [source.model_dump(mode="json") for source in command.sources]])
            ):
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            raise ProductSourceRetired() from None
        return digest(
            [
                event_signature(event, receipt, intent),
                fields(response, ("id", "request_id", "actor_id", "request_revision", "assignment_revision", "disposition", "text", "choice", "sources")),
                fields(request, ("id", "instance_id", "work_id", "purpose", "assignment_revision")),
            ]
        )
