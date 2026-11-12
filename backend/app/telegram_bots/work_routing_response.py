"""Exact attributed response evidence in every routing presentation snapshot."""

from datetime import datetime

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from deerflow.agent_instances.human_input_contract import Respond
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow


class RoutingResponseProof:
    def __init__(self, source, response_id):
        self.source, self.response_id = source, response_id
        self.column = None

    def extend(self, query):
        if self.response_id is None:
            return query
        self.column = len(query.column_descriptions)
        response, receipt, event = aliased(HumanInputResponseRow), aliased(HumanInputEventRow), aliased(AgentWorkEventRow)
        return (
            query.add_columns(response, receipt, event)
            .outerjoin(response, response.id == self.response_id)
            .outerjoin(receipt, and_(receipt.instance_id == self.source.request["instance_id"], receipt.request_id == response.request_id, receipt.receipt["response_id"].as_string() == response.id))
            .outerjoin(event, and_(event.instance_id == receipt.instance_id, event.operation_id == receipt.operation_id))
        )

    def check(self, rows):
        if self.response_id is None:
            return
        try:
            identities = set()
            shown = next(item for item in self.source.responses if item["id"] == self.response_id)
            for row in rows:
                response, receipt, event = row[self.column : self.column + 3]
                if response is None or receipt is None or event is None:
                    raise ValueError()
                identities.add((response.id, receipt.id, event.id))
                proof = receipt.receipt
                command = Respond.model_validate(event.request)
                if (
                    (event.instance_id, event.work_id, event.action, event.actor_kind, event.actor_id) != (self.source.request["instance_id"], self.source.work["id"], "human_response", "human", response.actor_id)
                    or (receipt.request_id, receipt.action, receipt.actor_kind, receipt.actor_id) != (response.request_id, "respond", "human", response.actor_id)
                    or response.request_id != self.source.request["id"]
                    or response.id != self.response_id
                    or (proof.get("operation_id"), proof.get("request_id"), proof.get("actor_id"), proof.get("actor_kind"), proof.get("response_id")) != (event.operation_id, response.request_id, response.actor_id, "human", response.id)
                    or type(proof.get("revision")) is not int
                    or proof["revision"] != receipt.revision
                    or type(proof.get("request_revision")) is not int
                    or proof["request_revision"] != response.request_revision
                    or not same_body(receipt.request, event.request)
                    or command.operation_id != event.operation_id
                    or (command.expected_request_revision, command.expected_assignment_revision, command.disposition, command.text, command.choice)
                    != (response.request_revision, response.assignment_revision, response.disposition, response.text, response.choice)
                    or event.assignment_revision != response.assignment_revision
                    or not same_body([item.model_dump() for item in command.sources], response.sources)
                ):
                    raise ValueError()
                fields = ("id", "request_id", "actor_id", "request_revision", "assignment_revision", "disposition", "text", "choice")
                if not same_body({key: shown.get(key) for key in fields}, {key: getattr(response, key) for key in fields}) or aware(datetime.fromisoformat(shown["created_at"])) != aware(response.created_at):
                    raise ValueError()
                # HumanInput.responses intentionally projects each canonical
                # reference for this recipient. Unreadable references contain
                # no locator; readable ones explicitly disclaim a content read.
                projected = shown.get("sources")
                if type(projected) is not list or len(projected) != len(response.sources):
                    raise ValueError()
                for raw, visible in zip(response.sources, projected, strict=True):
                    if not same_body(visible, {**raw, "current_contents": "not_checked"}) and not same_body(visible, {"kind": "unavailable", "current_contents": "not_checked"}):
                        raise ValueError()
            if len(identities) != 1:
                raise ValueError()
        except (KeyError, ValueError, TypeError, AttributeError, StopIteration):
            raise ProductSourceRetired() from None
