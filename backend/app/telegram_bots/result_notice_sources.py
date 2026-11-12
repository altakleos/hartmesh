"""Canonical source reads for result recipients without review controls."""

from dataclasses import dataclass
from types import SimpleNamespace

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention import AttentionReader
from app.telegram_bots.attention_authority import _SCOPE, prepare_attention
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_event_source import event_signature
from app.telegram_bots.work_result_identity import digest, qualify_identity, result_identity
from app.telegram_bots.work_result_navigation import NavigationOrigin, qualify_navigation, require_navigation
from app.telegram_bots.work_source_spaces import ReferencedSpaces
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained

_WORK_FIELDS = ("id", "instance_id", "objective", "revision", "assignment_revision", "status", "creator_id", "review_required", "review_state", "outcome_id")
_ATTEMPT_FIELDS = ("id", "work_id", "requester_id", "thread_id", "status", "assignment_revision", "definition_revision")
_REQUEST_FIELDS = ("id", "instance_id", "work_id", "purpose", "recipient_id", "state", "revision", "request_revision", "assignment_revision", "basis_id", "basis_revision")


def fields(row, names):
    return None if row is None else {name: getattr(row, name) for name in names}


@dataclass(frozen=True)
class ResultNoticeRead:
    work: dict
    key: str
    name: str
    person: str | None
    protected: bool
    identity: dict
    navigation: dict
    signature: str
    event_signature: str


class ResultNoticeReader(AttentionReader):
    async def read(self, link, item):
        try:
            return await await_drained(self._read(link, item))
        except (AccessOff, CeilingRefused, AgentConflict, AgentDenied):
            raise ProductSourceRetired() from None

    async def _read(self, link, item):
        authority = await self._current(link)
        work = await self.work.get(actor=PrincipalRef("human", link.owner_user_id), instance_id=link.agent_ref, work_id=item.work_id)
        work = {name: work.get(name) for name in (*_WORK_FIELDS, "outcome", "suggestion")}
        projection = ResultNoticeProjection(self, link, item, work=work, protected=authority[1][1] == "protected")
        admission = await prepare_attention(self, link)
        await admission(projection=projection)
        return projection.source


class ResultNoticeProjection:
    """One provider-final snapshot, reusable by materialization and upload."""

    def __init__(self, reader, link, item, source=None, *, work=None, protected=None):
        self.reader, self.link, self.item, self.source = reader, link, item, source
        self.work = source.work if source is not None else work
        self.protected = source.protected if source is not None else protected
        self.origin = NavigationOrigin()
        self.spaces = ReferencedSpaces(reader, reader.links.session_factory, SimpleNamespace(request={}, work=self.work, responses=()))

    def extend(self, query):
        self.column = len(query.column_descriptions)
        work, event, attempt, request, delivery = AgentWorkRow, AgentWorkEventRow, AgentWorkAttemptRow, HumanInputRequestRow, TelegramOutboxRow
        person, suggestion, candidate, suggestion_attempt = aliased(UserRow), aliased(AgentWorkEventRow), aliased(AgentWorkEventRow), aliased(AgentWorkAttemptRow)
        latest = select(candidate.id).where(candidate.work_id == work.id, candidate.action == "suggest", candidate.actor_kind == "nonhuman").order_by(candidate.revision.desc()).limit(1).correlate(work).scalar_subquery()
        return (
            query.add_columns(work, event, attempt, request, person.email, suggestion, delivery, self.spaces.expression(), suggestion_attempt)
            .join(work, and_(work.id == self.item.work_id, work.instance_id == TelegramBotLinkRow.agent_ref))
            .join(event, event.id == self.item.event_id)
            .join(attempt, and_(attempt.id == self.item.payload["work_event_intent"].get("attempt_id"), attempt.work_id == work.id))
            .outerjoin(request, request.id == self.item.request_id)
            .outerjoin(person, person.id == request.recipient_id)
            .outerjoin(suggestion, suggestion.id == latest)
            .outerjoin(suggestion_attempt, and_(suggestion_attempt.id == suggestion.request["attempt_id"].as_string(), suggestion_attempt.work_id == work.id))
            .join(delivery, delivery.id == self.item.id)
        )

    def check(self, rows):
        bot, instance, _grant, policy = rows[0][:4]
        work, event, attempt, request, person, suggestion, delivery, spaces = rows[0][self.column : self.column + 8]
        suggestion_attempt = rows[0][self.column + 8]
        intent = self.item.payload.get("work_event_intent", {})
        key = intent.get("key")
        from app.telegram_bots.work_navigation_sources import delivery_scope

        expected_class, expected_key = delivery_scope(self.item, "result_notice_basis", "reply" if self.item.payload.get("result_notice_basis", {}).get("reveal") else "result", "event:" + event.id + ":result")
        if (
            key not in {"result.done", "result.waiting_review"}
            or type(intent.get("version")) is not int
            or intent["version"] != 1
            or type(intent.get("event_revision")) is not int
            or type(intent.get("assignment_revision")) is not int
            or any(getattr(bot, name) != getattr(self.link, name) for name in _SCOPE)
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.class_)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, expected_class)
            or _item(delivery) != self.item
            or (event.instance_id, event.work_id, event.revision, event.assignment_revision, event.action) != (self.link.agent_ref, work.id, intent["event_revision"], intent["assignment_revision"], "attempt_settled")
            or event.request.get("status") != "succeeded"
            or event.result.get("status") != ("completed" if key == "result.done" else "submitted")
            or event.actor_kind == "human"
            and event.actor_id == self.link.owner_user_id
            or delivery.dedupe_key != expected_key
            or event.request.get("attempt_id") != attempt.id
            or attempt.status != "succeeded"
            or work.outcome is None
            or work.outcome.get("attempt_id") != attempt.id
            or event.result.get("outcome_id") != work.outcome_id
            or digest(event.result.get("outcome")) != digest(work.outcome)
            or digest({name: value for name, value in self.work["outcome"].items() if name != "sources"}) != digest({name: value for name, value in work.outcome.items() if name != "sources"})
            or any(self.work.get(name) != getattr(work, name) for name in _WORK_FIELDS)
            or self.protected != (instance.custody != "personal" and policy == "protected")
        ):
            raise ProductSourceRetired()
        if key == "result.done":
            if work.status != "completed" or work.review_required or self.link.owner_user_id not in {attempt.requester_id, work.creator_id} or request is not None:
                raise ProductSourceRetired()
            person = None
        elif (
            work.status != "submitted"
            or request is None
            or request.purpose != "review"
            or request.state != "pending"
            or (request.instance_id, request.work_id, request.assignment_revision, request.basis_id, request.basis_revision) != (work.instance_id, work.id, work.assignment_revision, work.outcome_id, work.outcome["evidence_revision"])
            or self.link.owner_user_id != attempt.requester_id
            or self.link.owner_user_id == request.recipient_id
        ):
            raise ProductSourceRetired()
        identity = result_identity(work)
        qualify_identity(identity, SimpleNamespace(work=self.work))
        suggested = None if suggestion is None else {"id": suggestion.id, "statement": suggestion.request["statement"]}
        if self.work.get("suggestion") != suggested:
            raise ProductSourceRetired()
        if suggestion is not None and (
            suggestion_attempt is None
            or (suggestion.instance_id, suggestion.work_id, suggestion.actor_kind, suggestion.actor_id, suggestion.action) != (work.instance_id, work.id, "nonhuman", instance.principal_id, "suggest")
            or suggestion.assignment_revision != suggestion_attempt.assignment_revision
            or suggestion.request.get("action") != "suggest"
            or suggestion.result.get("id") != work.id
            or suggestion.result.get("instance_id") != work.instance_id
        ):
            raise ProductSourceRetired()
        navigation = {"version": 1, "agent_ref": self.link.agent_ref, "attempt_id": attempt.id, "thread_id": attempt.thread_id, "public_base_url": self.origin.base}
        signature = digest(
            [
                fields(work, _WORK_FIELDS),
                identity,
                fields(attempt, _ATTEMPT_FIELDS),
                fields(request, _REQUEST_FIELDS),
                instance.name,
                person,
                suggested,
                None if suggestion is None else event_signature(suggestion, None, {}),
                fields(suggestion_attempt, _ATTEMPT_FIELDS),
                event_signature(event, None, intent),
            ]
        )
        fresh = ResultNoticeRead(self.work, key, instance.name, person, self.protected, identity, navigation, signature, event_signature(event, None, intent))
        if self.source is not None and self.source != fresh:
            raise ProductSourceRetired()
        self.source = fresh
        qualify_navigation(navigation, self.link, fresh)
        require_navigation(navigation, attempt, work.id)
        self.origin.check(navigation)
        self.spaces.check(spaces)
