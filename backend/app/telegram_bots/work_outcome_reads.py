"""Read exact action receipts and current Work context without issuing commands."""

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import current_attention
from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from deerflow.agent_instances.work import UNRESOLVED
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class WorkOutcomeRead:
    act: object
    receipt: dict | None
    source: object
    permissions: int
    protected: bool
    attempt_status: str | None
    continued: bool

    @property
    def attempt_unresolved(self):
        return self.attempt_status in UNRESOLVED


class WorkOutcomeReader:
    def __init__(self, requests, acts):
        self.requests, self.acts = requests, acts
        if requests.links.session_factory.kw.get("bind") is not acts.session_factory.kw.get("bind"):
            raise ValueError("work-outcome-requires-shared-storage")

    async def read(self, link, act_id):
        async def perform():
            act = await self.acts._work_record(act_id)
            if (act.link_id, act.owner_user_id, act.instance_id, act.binding_generation, act.bot_id, act.telegram_user_id, act.chat_id) != (
                link.id,
                link.owner_user_id,
                link.agent_ref,
                link.binding_generation,
                link.bot_id,
                link.telegram_user_id,
                link.chat_id,
            ):
                raise ProductSourceRetired()
            # Receipt lookup has the service's actor/body/target checks. It is
            # read-only even for an uncertain action; only explicit Retry or
            # the bounded worker may attempt the original operation again.
            receipt = await self.acts.receipt_for(act)
            if act.state == "done" and receipt is None:
                raise ProductSourceRetired()
            if act.state != "done" and receipt is not None:
                # Recovery must first settle the journal from this receipt.
                raise DeliveryRetry(timedelta(seconds=5))
            source = await self.requests.read_request(link, act.request_id)
            if source.work["id"] != act.work_id:
                raise ProductSourceRetired()
            projection = _OutcomeProjection(act, source, receipt)
            _held, target = await current_attention(self.requests, link, projection=projection)
            return WorkOutcomeRead(act, receipt, source, target[0], target[1] == "protected", (source.work.get("attempt") or {}).get("status"), projection.continued)

        return await await_drained(perform())


class _OutcomeProjection:
    """One final joined account/role/claim-free action and source read."""

    def __init__(self, act, source, receipt):
        self.act, self.source, self.receipt = act, source, receipt
        self.continued = False

    def extend(self, query):
        act, work, request = TelegramActRow, AgentWorkRow, HumanInputRequestRow
        event = HumanInputEventRow if self.act.kind in {"respond", "route"} else AgentWorkEventRow
        effect = aliased(AgentWorkEventRow)
        other, attempt = aliased(AgentWorkAttemptRow), AgentWorkAttemptRow
        latest = select(other.id).where(other.work_id == work.id).order_by(other.created_at.desc(), other.id.desc()).limit(1).scalar_subquery()
        return (
            query.add_columns(act, work, request, event, attempt, effect)
            .join(act, act.id == self.act.id)
            .join(work, work.id == self.act.work_id)
            .join(request, request.id == self.act.request_id)
            .outerjoin(event, and_(event.instance_id == act.instance_id, event.operation_id == act.id))
            .outerjoin(attempt, attempt.id == latest)
            .outerjoin(effect, and_(effect.instance_id == act.instance_id, effect.operation_id == act.id))
        )

    def check(self, rows):
        from app.telegram_bots.acts import _snapshot

        bot, target, permissions, policy = rows[0][:4]
        saved, work, request, event, attempt, effect = rows[0][8:14]
        # Journal transitions during read must be observed before wording is
        # chosen. This also covers a completed callback replay after restart.
        if _snapshot(saved) != self.act or not same_body(saved.body, self.act.body) or not same_body(saved.result, self.act.result):
            raise DeliveryRetry(timedelta(seconds=5))
        if (work.instance_id, request.instance_id, request.work_id) != (bot.agent_ref, bot.agent_ref, work.id):
            raise ProductSourceRetired()
        previous_attempt = self.source.work.get("attempt") or {}
        if (
            work.revision != self.source.work["revision"]
            or request.revision != self.source.request["revision"]
            or target.name != self.source.request["instance_name"]
            or (attempt.id if attempt else None, attempt.status if attempt else None) != (previous_attempt.get("id"), previous_attempt.get("status"))
            or (target.custody == "company" and policy == "protected") != self.source.protected
        ):
            raise DeliveryRetry(timedelta(seconds=5))
        if self.act.state != "done":
            if event is not None or effect is not None:
                raise DeliveryRetry(timedelta(seconds=5))
            return
        if event is None or (event.actor_kind, event.actor_id, event.action) != ("human", bot.owner_user_id, self.act.kind) or not same_body(event.request, self.act.body):
            raise ProductSourceRetired()
        if self.act.kind in {"respond", "route"}:
            if event.request_id != request.id or not same_body(event.receipt, self.receipt.get("receipt")):
                raise ProductSourceRetired()
        elif event.work_id != work.id or self.act.kind == "accept" and not same_body(event.result.get("review"), self.receipt.get("review")):
            raise ProductSourceRetired()

        action = {"respond": "human_response", "route": "request_route"}.get(self.act.kind, self.act.kind)
        if effect is None or (effect.work_id, effect.actor_kind, effect.actor_id, effect.action) != (work.id, "human", bot.owner_user_id, action) or not same_body(effect.request, self.act.body):
            raise ProductSourceRetired()
        # The activation's expected Work revision provides logical ordering,
        # independent of wall-clock skew or current execution availability.
        if attempt is not None:
            revision = attempt.request.get("expected_revision")
            if type(revision) is not int or revision < 1:
                raise ProductSourceRetired()
            self.continued = revision >= effect.revision
