"""Current recipient reads for colleague Work command notifications."""

from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention import AttentionReader
from app.telegram_bots.attention_authority import _SCOPE, prepare_attention
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.result_notice_sources import fields
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_answer_news import AnswerNews
from app.telegram_bots.work_event_source import event_signature
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_stop_news import STOP_KEYS, StopNews, prepare_news_clock
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.work_contract import WorkCommand
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained

COMMAND_KEYS = {"work.changed": "edit", "work.cancelled": "cancel", "work.reopened": "reopen", "result.accepted": "accept", "result.changes": "changes_requested", "decision.recorded": "decide"}
_WORK = ("id", "instance_id", "objective", "creator_id", "revision", "assignment_revision", "status")
_ATTEMPT = ("id", "work_id", "requester_id", "activation_id", "assignment_revision")


@dataclass(frozen=True)
class WorkNewsRead:
    work_id: str
    key: str
    values: dict
    protected: bool
    work: dict
    event_signature: str
    signature: str


class WorkNewsReader(AttentionReader):
    def __init__(self, *, work, authority, links, clock=None):
        super().__init__(work=work, authority=authority, links=links)
        self.clock = clock or (lambda: datetime.now(UTC))

    async def read(self, link, item):
        try:
            return await await_drained(self._read(link, item))
        except (AccessOff, CeilingRefused, AgentConflict, AgentDenied):
            raise ProductSourceRetired() from None

    async def prepare_clock(self, link, item):
        return await prepare_news_clock(self, link, item)

    async def _read(self, link, item):
        authority = await self._current(link)
        current = await self.work.get(actor=PrincipalRef("human", link.owner_user_id), instance_id=link.agent_ref, work_id=item.work_id)
        work = {key: current[key] for key in _WORK}
        projection = WorkNewsProjection(self, link, item, work=work, protected=authority[1][1] == "protected", clock=await self.prepare_clock(link, item))
        admission = await prepare_attention(self, link)
        await admission(projection=projection)
        return projection.source


class WorkNewsProjection:
    def __init__(self, reader, link, item, source=None, *, work=None, protected=None, clock=None):
        self.reader, self.link, self.item, self.source = reader, link, item, source
        self.work = source.work if source else work
        self.protected = source.protected if source else protected
        self.stop = StopNews(link, item, clock) if item.payload.get("work_event_intent", {}).get("key") in STOP_KEYS else None
        self.answer = AnswerNews(item) if item.payload.get("work_event_intent", {}).get("key") == "request.answered" else None

    def extend(self, query):
        self.column = len(query.column_descriptions)
        work, event, attempt, delivery = AgentWorkRow, AgentWorkEventRow, AgentWorkAttemptRow, TelegramOutboxRow
        actor, activation, candidate = aliased(UserRow), aliased(AgentWorkEventRow), aliased(AgentWorkEventRow)
        prior = (
            select(candidate.id)
            .where(candidate.work_id == event.work_id, candidate.instance_id == event.instance_id, candidate.action == "activate", candidate.revision < event.revision)
            .order_by(candidate.revision.desc())
            .limit(1)
            .correlate(event)
            .scalar_subquery()
        )
        result = query.add_columns(work, event, attempt, activation, actor.email, delivery).join(work, work.id == self.item.work_id).join(event, event.id == self.item.event_id)
        if self.stop:
            result = result.outerjoin(attempt, and_(attempt.id == event.request["attempt_id"].as_string(), attempt.work_id == work.id)).outerjoin(
                activation, and_(activation.work_id == work.id, activation.operation_id == attempt.activation_id)
            )
        else:
            result = result.outerjoin(activation, activation.id == prior).outerjoin(attempt, and_(attempt.work_id == work.id, attempt.activation_id == activation.operation_id))
        result = result.outerjoin(actor, actor.id == event.actor_id).join(delivery, delivery.id == self.item.id)
        return self.answer.extend(result) if self.answer else self.stop.extend(result, activation) if self.stop else result

    def check(self, rows):
        bot, instance, _permission, policy = rows[0][:4]
        work, event, attempt, activation, person, delivery = rows[0][self.column : self.column + 6]
        intent = self.item.payload.get("work_event_intent", {})
        key = intent.get("key")
        answer_signature = self.answer.qualify(rows, event, work, intent) if self.answer else None
        stop_signature, stop_values = self.stop.qualify(rows, event, work, instance, attempt, activation, intent) if self.stop else (None, {})
        if self.answer is None and self.stop is None:
            try:
                command = WorkCommand.model_validate(event.request)
            except ValueError:
                raise ProductSourceRetired() from None
            expected_assignment = command.expected_assignment_revision + (event.action in {"edit", "cancel", "reopen", "changes_requested"})
            if (
                not same_body(command.model_dump(mode="json", exclude_unset=True), event.request)
                or command.operation_id != event.operation_id
                or command.action != event.action
                or command.expected_revision + 1 != event.revision
                or expected_assignment != event.assignment_revision
                or type(event.result.get("revision")) is not int
                or event.result["revision"] != event.revision
                or type(event.result.get("assignment_revision")) is not int
                or event.result["assignment_revision"] != event.assignment_revision
                or event.result.get("creator_id") != work.creator_id
            ):
                raise ProductSourceRetired()
        if (
            type(event.result.get("revision")) is not int
            or event.result["revision"] != event.revision
            or type(event.result.get("assignment_revision")) is not int
            or event.result["assignment_revision"] != event.assignment_revision
            or event.result.get("creator_id") != work.creator_id
        ):
            raise ProductSourceRetired()
        if (
            key not in COMMAND_KEYS
            and key != "request.answered"
            and key not in STOP_KEYS
            or type(intent.get("version")) is not int
            or intent["version"] != 1
            or any(type(intent.get(name)) is not int for name in ("event_revision", "assignment_revision"))
            or (event.instance_id, event.work_id, event.revision, event.assignment_revision, event.action)
            != (self.link.agent_ref, work.id, intent["event_revision"], intent["assignment_revision"], ("human_response" if self.answer else "attempt_settled" if self.stop else COMMAND_KEYS[key]))
            or (event.actor_kind, event.actor_id) == ("human", self.link.owner_user_id)
            or self.stop is None
            and event.actor_kind != "human"
            or self.answer is None
            and self.stop is None
            and event.request.get("action") != event.action
            or (event.result.get("id"), event.result.get("instance_id")) != (work.id, self.link.agent_ref)
            or work.instance_id != self.link.agent_ref
            or any(getattr(bot, field) != getattr(self.link, field) for field in _SCOPE)
            or _item(delivery) != self.item
            or delivery.state != "sending"
            and any(getattr(delivery, field) is not None for field in ("claim_owner", "claim_id", "claimed_at"))
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.class_)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, "reply" if self.item.payload.get("work_news_basis", {}).get("reveal") else "info")
            or delivery.dedupe_key != "event:" + event.id + ":info"
            or (attempt.id if attempt else None) != intent.get("attempt_id")
            or (event.request.get("attempt_id") is not None or activation is not None)
            and attempt is None
            or not same_body(self.work, fields(work, _WORK))
            or self.protected != (instance.custody != "personal" and policy == "protected")
        ):
            raise ProductSourceRetired()
        requester = attempt.requester_id if attempt else None
        recipients = {work.creator_id} if key in {"work.changed", "work.reopened"} else {requester, work.creator_id} if key in {"result.accepted", "work.cancelled"} else {requester}
        if self.link.owner_user_id not in recipients:
            raise ProductSourceRetired()
        values = {"objective": work.objective, "person": person, **stop_values}
        if self.answer:
            values["open_assignment"] = self.answer.open_assignment
        if key == "result.changes":
            values["text"] = event.request["note"]
        elif key == "decision.recorded":
            # Work stores one decision statement, not independent choice/note
            # fields. Do not invent or duplicate a second annotation.
            values.update(decision=event.request["note"], note=None)
        signature = answer_signature or stop_signature or event_signature(event, None, intent)
        fresh = WorkNewsRead(work.id, key, values, self.protected, self.work, signature, digest([signature, self.work, values, fields(attempt, _ATTEMPT), None if activation is None else event_signature(activation, None, {})]))
        if self.source is not None and self.source != fresh:
            raise ProductSourceRetired()
        self.source = fresh
