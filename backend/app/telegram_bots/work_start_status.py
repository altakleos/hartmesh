"""Chat activation seeds the reader's one canonical status root in its writer."""

from sqlalchemy import select

from app.telegram_bots.links import BotLink
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_event_intents import WorkEventIntents
from deerflow.agent_instances.human_input import HumanInput
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow


async def seed_activation_status(acts, writer, link, act, result):
    if link is None or (link.id, link.owner_user_id, link.bot_id, link.binding_generation, link.telegram_user_id, link.chat_id, link.agent_ref, link.state, link.health) != (
        act.link_id,
        act.owner_user_id,
        act.bot_id,
        act.binding_generation,
        act.telegram_user_id,
        act.chat_id,
        act.instance_id,
        "connected",
        "ok",
    ):
        return
    event = await writer.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.instance_id == act.instance_id, AgentWorkEventRow.operation_id == act.id))
    attempt = await writer.scalar(select(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id == act.work_id, AgentWorkAttemptRow.activation_id == act.id))
    if (
        event is None
        or attempt is None
        or (event.work_id, event.action, event.actor_kind, event.actor_id) != (act.work_id, "activate", "human", act.owner_user_id)
        or not same_body(event.request, act.body)
        or attempt.requester_id != act.owner_user_id
        or not same_body(attempt.request, act.body)
        or result.get("activation_receipt") != {"operation_id": act.id, "attempt_id": attempt.id}
    ):
        raise ValueError("invalid-activation-status-receipt")
    # The reader locks this same bot before inserting the same dedupe key.
    # Whichever observes the admitted activation first owns the single root.
    await WorkEventIntents(links=acts.links, human=HumanInput(acts.work)).project(writer, BotLink.from_row(link), event)
