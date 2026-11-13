"""Close only conclusively superseded, still-unmaterialized question intents."""

from sqlalchemy import select

from app.telegram_bots.attention_authority import _SCOPE
from app.telegram_bots.producer import _item
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


async def retire_obsolete_question(outbox, link, item, *, available):
    intent = item.payload.get("work_event_intent")
    if (
        not available()
        or type(intent) is not dict
        or type(intent.get("version")) is not int
        or intent["version"] != 1
        or intent.get("key") not in {"ask.info", "ask.decision", "review.ready", "request.unrouted"}
        or item.class_ != "question"
        or item.dedupe_key != f"event:{item.event_id}:question"
        or item.state != "pending"
        or item.parts
        or any(type(intent.get(key)) is not int or intent[key] < 1 for key in ("request_revision", "assignment_revision"))
    ):
        return False
    async with outbox._writer(link.id) as (writer, binding):
        if not available() or binding is None or any(getattr(binding, key) != getattr(link, key) for key in _SCOPE):
            return False
        row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
        if row is None or _item(row) != item or any(getattr(row, key) is not None for key in ("claim_owner", "claim_id", "claimed_at")):
            return False
        facts = (
            await writer.execute(
                select(HumanInputRequestRow, AgentWorkRow)
                .join(AgentWorkRow, AgentWorkRow.id == HumanInputRequestRow.work_id)
                .where(HumanInputRequestRow.id == row.request_id, AgentWorkRow.id == row.work_id, HumanInputRequestRow.instance_id == link.agent_ref, AgentWorkRow.instance_id == link.agent_ref)
            )
        ).first()
        if facts is None:
            return False  # Missing or unreadable evidence is not a conclusive disposition.
        request, work = facts
        obsolete = request.state in {"answered", "closed"} or request.request_revision > intent["request_revision"] or request.assignment_revision > intent["assignment_revision"] or work.assignment_revision > intent["assignment_revision"]
        if not obsolete or not available():
            return False
        row.state, row.skip_reason, row.closed_at = "skipped", "superseded", outbox.clock()
        return True
