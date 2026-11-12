"""Resolve immutable event identities without substituting a later attempt."""

from sqlalchemy import select

from app.telegram_bots.work_actions import same_body
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputRequestRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow


async def event_attempt(writer, event):
    work_id = event.request.get("source_work_id") if event.action == "derived" else event.work_id
    identity = event.request.get("attempt_id")
    if identity is not None:
        attempt = await writer.get(AgentWorkAttemptRow, identity)
        work = await writer.get(AgentWorkRow, work_id) if work_id else None
        if attempt is None or attempt.work_id != work_id or work is None or work.instance_id != event.instance_id:
            raise ValueError("work-event-attempt-missing")
        return attempt
    activation = (
        event
        if event.action == "activate"
        else await writer.scalar(
            select(AgentWorkEventRow)
            .where(AgentWorkEventRow.instance_id == event.instance_id, AgentWorkEventRow.work_id == event.work_id, AgentWorkEventRow.action == "activate", AgentWorkEventRow.revision < event.revision)
            .order_by(AgentWorkEventRow.revision.desc())
            .limit(1)
        )
    )
    if activation is None:
        return None
    attempt = await writer.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == work_id, AgentWorkAttemptRow.activation_id == activation.operation_id))
    if attempt is None:
        raise ValueError("work-event-activation-missing")
    return attempt


async def event_request(writer, event):
    receipt = await writer.scalar(select(HumanInputEventRow).where(HumanInputEventRow.instance_id == event.instance_id, HumanInputEventRow.operation_id == event.operation_id))
    if receipt is None and (event.action in {"human_response", "request_route", "request_withdraw"} or event.action == "request_input" and event.actor_kind == "human"):
        raise ValueError("work-event-request-receipt-missing")
    if receipt is not None:
        action = {"request_input": "create", "human_response": "respond", "request_route": "route", "request_withdraw": "withdraw"}.get(event.action, event.action)
        proof = receipt.receipt
        if (
            event.actor_kind != "human"
            or (receipt.action, receipt.actor_kind, receipt.actor_id) != (action, event.actor_kind, event.actor_id)
            or not same_body(receipt.request, event.request)
            or (proof.get("operation_id"), proof.get("request_id"), proof.get("actor_id"), proof.get("actor_kind")) != (event.operation_id, receipt.request_id, event.actor_id, event.actor_kind)
            or type(proof.get("revision")) is not int
            or proof["revision"] != receipt.revision
            or type(proof.get("request_revision")) is not int
            or proof["request_revision"] < 1
        ):
            raise ValueError("work-event-request-receipt-mismatch")
        request = await writer.get(HumanInputRequestRow, receipt.request_id)
    elif event.action == "assess_input":
        request = await writer.get(HumanInputRequestRow, (event.request.get("request_basis") or {}).get("id"))
    else:
        blocker = event.result.get("blocker") or {}
        basis = blocker.get("id") or event.result.get("outcome_id")
        basis_revision = blocker.get("revision") if blocker else (event.result.get("outcome") or {}).get("evidence_revision")
        request = (
            None
            if basis is None
            else (
                await writer.scalars(
                    select(HumanInputRequestRow).where(
                        HumanInputRequestRow.instance_id == event.instance_id,
                        HumanInputRequestRow.work_id == event.work_id,
                        HumanInputRequestRow.basis_id == basis,
                        HumanInputRequestRow.basis_revision == basis_revision,
                        HumanInputRequestRow.assignment_revision == event.assignment_revision,
                    )
                )
            ).one_or_none()
        )
    if request is not None and (request.instance_id != event.instance_id or request.work_id != event.work_id):
        raise ValueError("work-event-request-scope")
    if event.action in {"request_input", "revise_input", "human_response", "request_route", "request_withdraw", "assess_input"} and request is None:
        raise ValueError("work-event-request-missing")
    return request, receipt


async def event_response(writer, event, request, receipt):
    if receipt is None or receipt.action != "respond" or receipt.actor_id != event.actor_id or receipt.receipt.get("operation_id") != event.operation_id or receipt.receipt.get("request_id") != request.id:
        raise ValueError("work-event-response-receipt")
    response = await writer.get(HumanInputResponseRow, receipt.receipt.get("response_id"))
    if response is None or (response.request_id, response.actor_id, response.assignment_revision, response.request_revision) != (request.id, event.actor_id, event.assignment_revision, receipt.receipt.get("request_revision")):
        raise ValueError("work-event-response-missing")
    if (response.disposition, response.text, response.choice) != (event.request.get("disposition", "supplied"), event.request.get("text"), event.request.get("choice")):
        raise ValueError("work-event-response-body-mismatch")
    return response
