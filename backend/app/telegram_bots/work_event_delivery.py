"""Materialize information/decision intents through existing Work sources.

This callable is not installed by the supervisor. Result files, routing, edits,
status and summaries retain their own pending materialization paths.
"""

from datetime import datetime, timedelta

from app.telegram_bots.attention_authority import _SCOPE, prepare_attention
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired, _item
from app.telegram_bots.work_event_facts import event_attempt, event_request
from app.telegram_bots.work_event_source import event_signature, require_event_source
from app.telegram_bots.work_request_recovery import WorkRequestRecovery, _RecoveryProjection
from deerflow.agent_instances.work import UNRESOLVED
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkEventDelivery:
    def __init__(self, plans):
        self.plans = plans
        self.outbox, self.reader = plans.outbox, plans.reader
        self.recovery = WorkRequestRecovery(plans)

    async def materialize(self, link, identity):
        return await await_drained(self._materialize(link, identity))

    async def _materialize(self, link, identity):
        async with self.outbox.sessions() as reader:
            stored = await reader.get(TelegramOutboxRow, identity)
            if stored is None:
                raise ProductSourceRetired()
            if not stored.parts and stored.claim_id is not None:
                raise ProductSourceRetired()
            captured = _item(stored)
        intent = captured.payload.get("work_event_intent")
        if type(intent) is not dict or intent.get("key") not in {"ask.info", "ask.decision"} or captured.dedupe_key.startswith("edit:"):
            return False
        if (
            (captured.link_id, captured.owner_user_id, captured.binding_generation, captured.chat_id, captured.instance_id, captured.class_) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref, "question")
            or intent.get("version") != 1
            or type(intent.get("version")) is not int
            or not captured.event_id
            or not captured.request_id
        ):
            raise ProductSourceRetired()
        if captured.parts:
            if "work_request_basis" not in captured.payload or "work_event_source" not in captured.payload:
                raise ProductSourceRetired()
            return True
        if captured.state != "pending" or captured.payload.get("presentation_version") != 0:
            raise ProductSourceRetired()
        source, settings = await self.recovery._source(link, captured.request_id)
        if (
            source.request["purpose"] != ("information" if intent["key"] == "ask.info" else "decision")
            or type(intent.get("request_revision")) is not int
            or type(intent.get("assignment_revision")) is not int
            or (source.seen["request_revision"], source.seen["assignment_revision"]) != (intent["request_revision"], intent["assignment_revision"])
        ):
            raise ProductSourceRetired()
        admission = await prepare_attention(self.reader, link)
        async with self.outbox._writer(link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
            if binding is None or any(getattr(binding, key) != getattr(link, key) for key in _SCOPE) or row is None or row.claim_id is not None or _item(row) != captured:
                raise DeliveryRetry(timedelta(seconds=5))
            event = await writer.get(AgentWorkEventRow, captured.event_id)
            if event is None:
                raise DeliveryRetry(timedelta(seconds=5))
            if (
                (event.instance_id, event.work_id, event.revision, event.assignment_revision) != (link.agent_ref, captured.work_id, intent.get("event_revision"), intent["assignment_revision"])
                or event.action not in {"request_input", "revise_input", "request_route"}
                or type(intent.get("event_revision")) is not int
            ):
                raise ProductSourceRetired()
            request, receipt = await event_request(writer, event)
            if request is None or request.id != captured.request_id:
                raise ProductSourceRetired()
            revision = receipt.receipt["request_revision"] if receipt is not None else 1
            if revision != intent["request_revision"]:
                raise ProductSourceRetired()
            attempt = await event_attempt(writer, event)
            if (attempt.id if attempt else None) != intent.get("attempt_id"):
                raise ProductSourceRetired()
            if intent["key"] == "ask.decision" and attempt is not None and attempt.status in UNRESOLVED:
                deadline = datetime.fromisoformat(intent["hold_until"])
                if deadline.tzinfo is None:
                    raise ProductSourceRetired()
                if self.outbox.clock() < aware(deadline):
                    return False
            signature = event_signature(event, receipt, intent)
            await self.recovery._admit(admission, writer, source)
            row.payload = {**row.payload, "work_event_source": {"version": 1, "signature": signature}}
            await self.plans._freeze(writer, link, row, source, settings, pending=True)
            await writer.flush()
            held, _target = await admission(session=writer, projection=_MaterializationProjection(source, _item(row)))
            if "agents:write" not in held:
                raise ProductSourceRetired()
            return True


class _MaterializationProjection(_RecoveryProjection):
    def __init__(self, source, item):
        super().__init__(source)
        self.item = item

    def extend(self, query):
        delivery, event, receipt = TelegramOutboxRow, AgentWorkEventRow, HumanInputEventRow
        return (
            super()
            .extend(query)
            .add_columns(delivery, event, receipt)
            .join(delivery, delivery.id == self.item.id)
            .outerjoin(event, event.id == delivery.event_id)
            .outerjoin(receipt, (receipt.instance_id == event.instance_id) & (receipt.operation_id == event.operation_id))
        )

    def check(self, rows):
        super().check(rows)
        delivery, event, receipt = rows[0][11:14]
        if delivery.claim_id is not None or _item(delivery) != self.item:
            raise ProductSourceRetired()
        require_event_source(self.item, self.source, event=event, receipt=receipt, final=True)
