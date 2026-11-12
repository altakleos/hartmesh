"""Canonical digest navigation sources, independent of delivery claims."""

from dataclasses import dataclass, replace
from types import SimpleNamespace

from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_text import DigestEntry
from app.telegram_bots.work_event_source import require_event_source
from app.telegram_bots.work_request_recovery import _RecoveryProjection
from app.telegram_bots.work_request_source import frozen
from app.telegram_bots.work_source_spaces import ReferencedSpaces
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@dataclass(frozen=True)
class DigestMember:
    item: object
    entry: DigestEntry
    source: object
    protected: bool
    projection: object


def claim_of(row):
    return row.claim_owner, row.claim_id, aware(row.claimed_at) if row.claimed_at else None


_REQUEST_FIELDS = ("id", "instance_id", "work_id", "purpose", "question", "reason", "expected_response", "choices", "recipient_id", "request_revision", "assignment_revision", "basis_id", "basis_revision")


def require_request_origin(previous, current):
    if not same_body({key: previous.request.get(key) for key in _REQUEST_FIELDS}, {key: current.request.get(key) for key in _REQUEST_FIELDS}) or not same_body(
        {key: previous.work.get(key) for key in ("id", "instance_id", "objective", "assignment_revision")}, {key: current.work.get(key) for key in ("id", "instance_id", "objective", "assignment_revision")}
    ):
        raise ProductSourceRetired()


class DigestMembers:
    def __init__(self, *, requests, news=None, results=None, routing=None):
        self.requests, self.news, self.results, self.routing = requests, news, results, routing
        engine = requests.links.session_factory.kw["bind"]
        if any(reader is not None and reader.links.session_factory.kw["bind"] is not engine for reader in (news, results, routing)):
            raise ValueError("digest-requires-shared-storage")

    async def read(self, link, item, *, locale, claim=(None, None, None)):
        if type(item.payload) is not dict or type(item.parts) is not list or any(type(part) is not dict for part in item.parts):
            raise ProductSourceRetired()
        families = {"work_request_basis", "work_news_basis", "result_notice_basis"}.intersection(item.payload)
        if (
            len(families) != 1
            or not item.parts
            or item.message_thread_id is not None
            or any(part.get("state") != "pending" or part.get("message_ids") or part.get("dispatched_at") or part.get("sent_at") or part.get("kind") == "edit" for part in item.parts)
        ):
            raise ProductSourceRetired()
        if "work_news_basis" in families:
            return await self._news(link, item, locale, claim)
        if "result_notice_basis" in families:
            return await self._result(link, item, locale, claim)
        basis = item.payload.get("work_request_basis")
        if type(basis) is not dict or type(basis.get("version")) is not int:
            raise ProductSourceRetired()
        if type(basis) is dict and basis.get("version") in {5, 6}:
            return await self._routing(link, item, locale, claim)
        if type(basis) is not dict or basis.get("version") not in {1, 2} or basis.get("reveal") or basis.get("reply_origin"):
            raise ProductSourceRetired()
        if item.class_ != "question" or not item.parts or item.message_thread_id is not None:
            raise ProductSourceRetired()
        _, previous, _ = frozen(SimpleNamespace(link=link, item=item, part=item.parts[0]), pending=True)
        source = await self.requests.read_request(link, item.request_id)
        require_request_origin(previous, source)
        require_event_source(item, source)
        if source.request["state"] != "pending" or source.request["needs_routing"] or not source.request["can_respond" if source.request["purpose"] == "information" else "can_manage"]:
            raise ProductSourceRetired()
        entry = DigestEntry(item.id, item.class_, {"information": "question", "decision": "decision", "review": "review"}[source.request["purpose"]], objective=source.work["objective"])
        base = None
        if "result_files" in basis:
            from app.telegram_bots.work_result_recovery import _ResultRecoveryProjection

            source = replace(source, work={**source.work, "result_identity": item.payload["work_result_identity"], "result_navigation": item.payload["work_result_navigation"]})
            base = _ResultRecoveryProjection(source)
        return DigestMember(item, entry, source, source.protected, _QuestionProjection(self.requests, link, item, source, claim, base=base))

    async def _routing(self, link, item, locale, claim):
        from app.telegram_bots.work_result_navigation import NavigationOrigin
        from app.telegram_bots.work_routing import present_routing
        from app.telegram_bots.work_routing_recovery import _RoutingRecoveryProjection, _RoutingSource

        if self.routing is None or item.class_ != "question":
            raise ProductSourceRetired()
        basis, previous, _ = frozen(SimpleNamespace(link=link, item=item, part=item.parts[0]), pending=True)
        if basis.get("reveal") or basis.get("reply_origin") or basis.get("choice_origin"):
            raise ProductSourceRetired()
        value = await self.routing.read_routing(link, item.request_id, response_id=item.payload.get("work_event_intent", {}).get("response_id"))
        source = _RoutingSource(value, link, NavigationOrigin().base)
        require_request_origin(previous, source)
        require_event_source(item, source)
        view = present_routing(value, locale=locale, link=link, public_base_url=source.public_base_url)
        entry = DigestEntry(item.id, item.class_, "news", first_line=view.parts[0]["visible_text"].splitlines()[0])
        base = _RoutingRecoveryProjection(self.routing.links.session_factory, source)
        return DigestMember(item, entry, source, source.protected, _QuestionProjection(self.routing, link, item, source, claim, base=base))

    async def _news(self, link, item, locale, claim):
        from app.telegram_bots.work_news import present_news
        from app.telegram_bots.work_news_delivery import frozen_news
        from app.telegram_bots.work_news_sources import WorkNewsProjection, same_news_origin

        if self.news is None:
            raise ProductSourceRetired()
        basis, previous, _ = frozen_news(SimpleNamespace(link=link, item=item, part=None), pending=True)
        if basis.get("reveal") or item.class_ in {"reply", "progress"}:
            raise ProductSourceRetired()
        source = await self.news.read(link, item)
        if not same_news_origin(previous, source):
            raise ProductSourceRetired()
        view = present_news(source.key, values=source.values, locale=locale, protected=source.protected)
        entry = DigestEntry(item.id, item.class_, "news", first_line=view.parts[0]["visible_text"].splitlines()[0])
        projection = WorkNewsProjection(self.news, link, item, source, clock=await self.news.prepare_clock(link, item))
        return DigestMember(item, entry, source, source.protected, _ClaimProjection(projection, claim, lambda: projection.delivery_column))

    async def _result(self, link, item, locale, claim):
        from app.telegram_bots.render import render_plain
        from app.telegram_bots.result_notice_delivery import frozen_notice
        from app.telegram_bots.result_notice_sources import ResultNoticeProjection

        if self.results is None:
            raise ProductSourceRetired()
        basis, previous, _ = frozen_notice(SimpleNamespace(link=link, item=item, part=None), pending=True)
        if basis.get("reveal") or item.class_ != "result":
            raise ProductSourceRetired()
        source = await self.results.read(link, item)
        if source.event_signature != previous.event_signature or source.identity != previous.identity:
            raise ProductSourceRetired()
        title = render_plain(source.key, locale, objective=source.work["objective"], name=source.name, statement=source.work["outcome"]["statement"], person=source.person or render_plain("label.colleague", locale)).splitlines()[0]
        entry = DigestEntry(item.id, item.class_, "news", first_line=title)
        projection = ResultNoticeProjection(self.results, link, item, source)
        return DigestMember(item, entry, source, source.protected, _ClaimProjection(projection, claim, lambda: projection.column + 6))


class _ClaimProjection:
    def __init__(self, source, claim, column):
        self.source, self.claim, self.column = source, claim, column

    def extend(self, query):
        return self.source.extend(query)

    def check_with_permissions(self, rows, held):
        if claim_of(rows[0][self.column()]) != self.claim:
            raise ProductSourceRetired()
        qualified = getattr(self.source, "check_with_permissions", None)
        if qualified is not None:
            qualified(rows, held)
        else:
            self.source.check(rows)


class _QuestionProjection:
    def __init__(self, reader, link, item, source, claim, *, base=None):
        self.source, self.base = source, base or _RecoveryProjection(source)
        self.link, self.item = link, item
        self.claim = claim
        self.spaces = ReferencedSpaces(reader, reader.links.session_factory, source)

    def extend(self, query):
        delivery, event, receipt = TelegramOutboxRow, AgentWorkEventRow, HumanInputEventRow
        result = self.base.extend(query)
        self.column = len(result.column_descriptions)
        return (
            result.add_columns(delivery, event, receipt, self.spaces.expression())
            .join(delivery, delivery.id == self.item.id)
            .outerjoin(event, event.id == delivery.event_id)
            .outerjoin(receipt, (receipt.instance_id == event.instance_id) & (receipt.operation_id == event.operation_id))
        )

    def check_with_permissions(self, rows, held):
        if "agents:write" not in held:
            raise ProductSourceRetired()
        self.check(rows)

    def check(self, rows):
        self.base.check([row[: self.column] for row in rows])
        work, request = rows[0][8:10]
        delivery, event, receipt, spaces = rows[0][self.column : self.column + 4]
        if (
            _item(delivery) != self.item
            or claim_of(delivery) != self.claim
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.work_id, delivery.request_id)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, work.id, request.id)
            or work.objective != self.source.work["objective"]
            or not same_body({key: getattr(request, key) for key in _REQUEST_FIELDS}, {key: self.source.request.get(key) for key in _REQUEST_FIELDS})
            or rows[0][1].name != self.source.request["instance_name"]
        ):
            raise ProductSourceRetired()
        require_event_source(self.item, self.source, event=event, receipt=receipt, final=True)
        self.spaces.check(spaces)
