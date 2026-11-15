"""Read-only current readiness for an answered, effect-free Start refusal."""

from dataclasses import dataclass

from sqlalchemy import select

from app.gateway import storage_spaces
from app.telegram_bots.acts import _snapshot
from app.telegram_bots.instance_refusals import describe_send_refusal
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import thaw
from app.telegram_bots.work_execution_sources import act_facts, require_execution_scope
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_start_actions import answered_calls
from app.telegram_bots.work_start_conversations import context_plan, require_context_child
from deerflow.agent_instances.contract import ExecutionUnavailable
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow

NO_EFFECT_REASONS = frozenset({"denied", "ceiling", "grant-required", "not-visible", "agent-not-active", "execution-unavailable"})


def conclusive_refusal(root, effective):
    if root.cancel_requested_at is not None or effective.cancel_requested_at is not None or effective.state not in {"refused", "conflict"} or effective.reason not in NO_EFFECT_REASONS:
        return False
    if root.id != effective.id:
        require_context_child(root, effective)
        marker = context_plan(root)
        if root.state not in {"refused", "conflict", "uncertain"} or root.state == "uncertain" and marker["phase"] != "ended":
            return False
    for act in (root, effective):
        answered = answered_calls(act)
        if act.attempts < 0 or act.attempts == 0 and answered != 0 or act.attempts > 0 and answered < 1:
            return False
    return True


@dataclass(frozen=True)
class RefusalClock:
    settings: object
    zone: object
    now: object
    signature: str
    failure: object


def readiness_signature(root, effective):
    return digest([act_facts(root), thaw(root.result), act_facts(effective), thaw(effective.result)])


async def prepare_refusal_clock(reader, link, item, clock):
    async with reader.links.session_factory() as session:
        row = await session.get(TelegramActRow, item.payload["work_execution_intent"]["act_id"])
        if row is None:
            raise ProductSourceRetired()
        root = _snapshot(row)
        require_execution_scope(link, root)
        if root.kind != "activate":
            return clock
        marker = context_plan(root) if (root.continuation or {}).get("activation_context") is not None else None
        if marker is not None:
            child = await session.get(TelegramActRow, marker["child_id"])
            if child is None:
                raise ProductSourceRetired()
            effective = _snapshot(child)
            require_context_child(root, effective)
        else:
            effective = root
        if not conclusive_refusal(root, effective):
            return clock
        ids = {root.id, effective.id}
        owned = await session.execute(select(select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id.in_(ids)).exists(), select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id.in_(ids)).exists()))
        if any(owned.one()):
            return clock
    await reader.authority.instance_view(link)
    try:
        await storage_spaces.require_execution(reader.authority.app, link.agent_ref)
    except ExecutionUnavailable as error:
        failure = await describe_send_refusal(reader.authority, link, error)
    else:
        failure = None
    return RefusalClock(clock.settings, clock.zone, clock.now, readiness_signature(root, effective), failure)
