"""Complete summary selection qualified again in the final account snapshot."""

from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import literal, select, true, union_all
from sqlalchemy.orm import aliased

from app.telegram_bots.attention import AttentionReader
from app.telegram_bots.attention_authority import current_attention, prepare_attention
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.summary import sections
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_projection import _EntityArray
from deerflow.agent_instances.human_input import HumanInput
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow
from deerflow.utils.file_io import await_drained

_WORK = ("id", "instance_id", "creator_id", "objective", "status", "assignment_revision", "revision", "created_at", "outcome_id", "blocker", "outcome")
_REQUEST = ("id", "instance_id", "work_id", "purpose", "state", "recipient_id", "created_at", "assignment_revision", "basis_id", "basis_revision", "revision", "request_revision")
_ATTEMPT = ("id", "work_id", "status", "assignment_revision", "settled_at")
_FIELDS = {"work": _WORK, "request": _REQUEST, "attempt": _ATTEMPT}


def selection_query(link, as_of):
    """A union keeps source cardinality additive, independent of display caps."""
    work, request, attempt = AgentWorkRow, HumanInputRequestRow, AgentWorkAttemptRow
    other = aliased(AgentWorkAttemptRow)
    latest = select(other.id).where(other.work_id == work.id).order_by(other.created_at.desc(), other.id.desc()).limit(1).scalar_subquery()

    def columns(kind, model, fields):
        return literal(kind).label("kind"), _EntityArray(*(getattr(model, key) for key in fields)).label("body")

    works = select(*columns("work", work, _WORK)).where(work.instance_id == link.agent_ref, work.created_at <= as_of)
    attempts = select(*columns("attempt", attempt, _ATTEMPT)).join(work, work.id == attempt.work_id).where(work.instance_id == link.agent_ref, work.created_at <= as_of, attempt.id == latest)
    requests = select(*columns("request", request, _REQUEST)).where(request.instance_id == link.agent_ref, request.created_at <= as_of)
    return union_all(works, attempts, requests)


def _attention(facts, *, owner_user_id, permissions, can_write):
    values = {key: {} for key in _FIELDS}
    for kind, body in facts:
        if kind is None and body is None:
            continue
        if kind not in _FIELDS or type(body) is not list or len(body) != len(_FIELDS[kind]):
            raise ProductSourceRetired()
        row = dict(zip(_FIELDS[kind], body, strict=True))
        for key in ("created_at", "settled_at"):
            if row.get(key) is not None:
                row[key] = aware(datetime.fromisoformat(row[key])).isoformat()
        previous = values[kind].get(row["id"])
        if previous is not None and not same_body(previous, row):
            raise ProductSourceRetired()
        values[kind][row["id"]] = row
    attempts = {row["work_id"]: row for row in values["attempt"].values()}
    works = values["work"]
    for row in works.values():
        row["attempt"] = attempts.get(row["id"])
    pending = []
    for row in values["request"].values():
        work = works.get(row["work_id"])
        required = 3 if row["purpose"] == "information" else 6
        addressed = row["recipient_id"] == owner_user_id
        eligible = addressed and permissions & required == required
        row["needs_routing"] = row["state"] != "closed" and not eligible
        if work is not None and addressed and eligible and can_write and row["state"] == "pending" and HumanInput._source_current(SimpleNamespace(**row), SimpleNamespace(**work)):
            pending.append(row)
    return {"work": list(works.values()), "requests": list(values["request"].values()), "pending": pending, "routing": []}


@dataclass(frozen=True)
class SummaryRead:
    owner: object
    link: object
    as_of: datetime
    since: datetime
    sections: dict
    protected: bool

    async def authorize(self, *, session=None, projection=None):
        async def perform():
            admission = await prepare_attention(self.owner, self.link)
            return await admission(session=session, projection=_SummaryAuthorization(self, projection))

        return await await_drained(perform())


class _SummaryAuthorization:
    """Additional delivery guards cannot replace the mandatory source guard."""

    def __init__(self, source, additional):
        self.source = SummaryProjection(source)
        self.additional = additional

    def extend(self, query):
        query = self.source.extend(query)
        return self.additional.extend(query) if self.additional is not None else query

    def check_with_permissions(self, rows, held):
        self.source.check_with_permissions(rows, held)
        if self.additional is not None:
            qualified = getattr(self.additional, "check_with_permissions", None)
            if qualified is not None:
                qualified(rows, held)
            else:
                self.additional.check(rows)


class SummaryReader(AttentionReader):
    async def read(self, link, *, as_of, since):
        async def perform():
            cutoff, beginning = aware(as_of), aware(since)
            if beginning > cutoff:
                raise ProductSourceRetired()
            attention = await self.collect(link, as_of=cutoff)
            _held, target = await current_attention(self, link)
            result = SummaryRead(self, link, cutoff, beginning, sections(attention, owner_user_id=link.owner_user_id, since=beginning), target[1] == "protected")
            await result.authorize()
            return result

        return await await_drained(perform())


class SummaryProjection:
    def __init__(self, source):
        self.source = source

    def extend(self, query):
        self.column = len(query.column_descriptions)
        facts = selection_query(self.source.link, self.source.as_of).subquery("summary_facts")
        return query.add_columns(facts.c.kind, facts.c.body).outerjoin(facts, true())

    def check_with_permissions(self, rows, held):
        _bot, instance, permissions, policy = rows[0][:4]
        if (instance.custody == "company" and policy == "protected") != self.source.protected:
            raise ProductSourceRetired()
        attention = _attention([row[self.column : self.column + 2] for row in rows], owner_user_id=self.source.link.owner_user_id, permissions=permissions, can_write="agents:write" in held)
        current = sections(attention, owner_user_id=self.source.link.owner_user_id, since=self.source.since)
        if not same_body(current, self.source.sections):
            raise ProductSourceRetired()
