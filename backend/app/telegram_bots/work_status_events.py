"""Canonical event proofs used by the dormant Work status reader."""

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from deerflow.agent_instances.human_input_contract import CreateRequest
from deerflow.agent_instances.work_execution_contract import ReportWork


def snapshot(event, work, instance):
    if (
        (event.work_id, event.instance_id) != (work.id, instance.id)
        or any(type(event.result.get(key)) is not int for key in ("revision", "assignment_revision"))
        or (event.result.get("id"), event.result.get("instance_id"), event.result.get("revision"), event.result.get("assignment_revision")) != (work.id, instance.id, event.revision, event.assignment_revision)
        or event.result.get("creator_id") != work.creator_id
    ):
        raise ProductSourceRetired()


def report(event, attempt, instance):
    body = {key: value for key, value in event.request.items() if key != "attempt_id"}
    try:
        command = ReportWork.model_validate(body)
    except ValueError:
        raise ProductSourceRetired() from None
    if (
        not same_body(command.model_dump(mode="json", exclude_unset=True), body)
        or event.request.get("attempt_id") != attempt.id
        or (event.actor_kind, event.actor_id) != ("nonhuman", instance.principal_id)
        or (command.operation_id, command.action, command.expected_revision + 1) != (event.operation_id, event.action, event.revision)
        or event.assignment_revision != attempt.assignment_revision
    ):
        raise ProductSourceRetired()
    return command


def question(event, attempt, instance, request, receipt, prior_activation, intent, item):
    if (
        request is None
        or (request.id, request.instance_id, request.work_id, request.assignment_revision, request.purpose) != (item.request_id, instance.id, attempt.work_id, event.assignment_revision, "information")
        or type(intent["request_revision"]) is not int
        or intent["request_revision"] != 1
    ):
        raise ProductSourceRetired()
    blocker = event.result.get("blocker") or {}
    if type(blocker.get("revision")) is not int or (blocker.get("id"), blocker["revision"], blocker.get("assignment_revision"), blocker.get("kind")) != (
        request.basis_id,
        request.basis_revision,
        request.assignment_revision,
        request.purpose,
    ):
        raise ProductSourceRetired()
    if event.actor_kind == "human":
        try:
            command = CreateRequest.model_validate(event.request)
        except ValueError:
            raise ProductSourceRetired() from None
        expected_receipt = {"operation_id": event.operation_id, "request_id": request.id, "revision": 1, "request_revision": 1, "actor_id": event.actor_id, "actor_kind": "human", "response_id": None}
        if (
            event.action != "request_input"
            or not same_body(command.model_dump(mode="json", exclude_unset=True), event.request)
            or (command.operation_id, command.expected_work_revision + 1, command.expected_assignment_revision, command.purpose) != (event.operation_id, event.revision, event.assignment_revision, "information")
            or prior_activation != attempt.activation_id
            or receipt is None
            or (receipt.instance_id, receipt.request_id, receipt.actor_kind, receipt.actor_id, receipt.action, receipt.operation_id, receipt.revision) != (instance.id, request.id, "human", event.actor_id, "create", event.operation_id, 1)
            or not same_body(receipt.request, event.request)
            or not same_body(receipt.receipt, expected_receipt)
        ):
            raise ProductSourceRetired()
    else:
        command = report(event, attempt, instance)
        if command.purpose != "information":
            raise ProductSourceRetired()
