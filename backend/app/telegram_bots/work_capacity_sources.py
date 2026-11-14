"""Current full-consent capacity offers use the ordinary protected news owner."""

from dataclasses import asdict, dataclass, replace
from datetime import datetime

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.render import render
from app.telegram_bots.split import split_html, visible_text
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_details import WorkDetailProjection
from app.telegram_bots.work_execution_sources import act_facts, qualify_execution_origin
from app.telegram_bots.work_news import present_news
from app.telegram_bots.work_news_sources import WorkNewsRead
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_sources import WorkViewReader, detail_read, present_view
from app.telegram_bots.work_views import WorkViewPresentation, work_buttons
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class CapacityClock:
    settings: object
    zone: object
    now: object
    capacity: object


def offer_intent(item):
    value = item.payload.get("capacity_offer_intent")
    if (
        type(value) is not dict
        or set(value) != {"version", "act_id", "ordinal", "origin"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or type(value["ordinal"]) is not int
        or value["ordinal"] < 1
        or type(value["act_id"]) is not str
        or len(value["act_id"]) != 32
    ):
        raise ProductSourceRetired()
    return value


def startable_detail(detail, link):
    return any(
        item.get("kind") == "work_start"
        for row in work_buttons(
            detail.record, owner_id=link.owner_user_id, permissions=detail.permissions, held=detail.held, request=detail.request, lifecycle_pending=detail.lifecycle_pending, can_reuse_conversation=detail.can_reuse_conversation
        )
        for item in row
    )


def qualify_waiting(link, journal, attempt, callback, parent, controls, origin, *, expected=None, transfer=None, activation_parent=None):
    if journal is None or attempt is not None or journal.cancel_requested_at is not None:
        raise ProductSourceRetired()
    act = _snapshot(journal)
    from app.telegram_bots.work_start_actions import answered_calls

    if act.attempts < 1 or answered_calls(act) != act.attempts:
        raise ProductSourceRetired()
    actual = qualify_execution_origin(link, act, callback, parent, controls, activation_parent=activation_parent)
    if not same_body(actual, origin):
        raise ProductSourceRetired()
    if expected is not None:
        facts = act_facts(act)
        if transfer is not None and (act.state, act.reason, act.result) == ("refused", "superseded", transfer):
            facts = {**facts, "state": "waiting", "reason": "work-limit"}
        if not same_body(facts, expected):
            raise ProductSourceRetired()
    if (act.state, act.reason) != ("waiting", "work-limit") and (transfer is None or (act.state, act.reason, act.result) != ("refused", "superseded", transfer)):
        raise ProductSourceRetired()
    return act


def present_capacity_offer(source, basis, identity, link):
    if basis["concealed"]:
        # The service teaser contains no assignment or occupant detail.
        return present_news("work.changed", values={}, locale=basis["locale"], protected=source.protected, concealed=True, outbox_id=identity)
    view = present_view(replace(source, key="view.detail"), basis, link)
    html = render("busy.place_free", basis["locale"], objective=source.work["detail"]["record"]["objective"]) + "\n\n" + "".join(part["html"] for part in view.parts)
    parts = tuple(dict(kind="text", html=chunk, visible_text=visible_text(chunk), protect_content=source.protected) for chunk in split_html(html))
    return WorkViewPresentation(parts, view.keyboard)


class WorkCapacityOfferReader(WorkViewReader):
    def __init__(self, *, capacity, **kwargs):
        super().__init__(**kwargs)
        self.capacity = capacity

    async def prepare_clock(self, link, item):
        clock = await super().prepare_clock(link, item)
        capacity = await self.capacity.read(link)
        return CapacityClock(clock.settings, clock.zone, clock.now, capacity)

    async def read(self, link, item):
        async def perform():
            intent = offer_intent(item)
            detail = await self.detail(link, item.work_id, ignore_activation=intent["act_id"])
            projection = WorkCapacityOfferProjection(self, link, item, detail=detail, clock=await self.prepare_clock(link, item))
            await projection.prepare_source()
            admission = await prepare_attention(self, link)
            await admission(projection=projection)
            return projection.source

        return await await_drained(perform())


class WorkCapacityOfferProjection:
    def __init__(self, reader, link, item, source=None, *, detail=None, clock):
        self.reader, self.link, self.item, self.source, self.clock = reader, link, item, source, clock
        self.intent = offer_intent(item)
        self.detail = detail or detail_read(source.work["detail"])
        self.projection = WorkDetailProjection(reader, link, self.detail.record, self.detail.request, reusable=self.detail.can_reuse_conversation, expected=self.detail, ignore_activation=self.intent["act_id"])
        self.activation_parent = None

    async def prepare_source(self):
        from app.telegram_bots.work_continuation_sources import ActivationParentProjection

        async with self.reader.links.session_factory() as reader:
            act = await reader.get(TelegramActRow, self.intent["act_id"])
            if act is None or act.kind != "activate":
                raise ProductSourceRetired()
            captured = _snapshot(act)
        self.activation_parent = ActivationParentProjection(captured, for_effect=False)
        try:
            await self.activation_parent.prepare(self.reader.acts, self.link)
        except (ValueError, KeyError, TypeError):
            raise ProductSourceRetired() from None
        return self

    def with_item(self, item):
        from copy import copy

        current = copy(self)
        current.item = item
        return current

    def extend(self, query):
        query = self.projection.extend(query)
        self.delivery_column = len(query.column_descriptions)
        delivery, act, callback, parent, control, attempt, settings = (
            TelegramOutboxRow,
            aliased(TelegramActRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramOutboxRow),
            aliased(TelegramCallbackRow),
            aliased(AgentWorkAttemptRow),
            aliased(TelegramPersonSettingsRow),
        )
        latest = aliased(TelegramOutboxRow)
        newest = select(latest.id).where(latest.payload["capacity_offer_intent"]["act_id"].as_string().is_not(None)).order_by(latest.created_at.desc(), latest.id.desc()).limit(1).scalar_subquery()
        result = (
            query.add_columns(delivery, act, callback, parent, control, attempt, settings, newest)
            .join(delivery, delivery.id == self.item.id)
            .join(act, act.id == self.intent["act_id"])
            .outerjoin(callback, callback.id == self.intent["origin"]["callback_id"])
            .outerjoin(parent, parent.id == callback.payload["anchor_outbox_id"].as_string())
            .outerjoin(control, control.group_id == callback.group_id)
            .outerjoin(attempt, and_(attempt.work_id == act.work_id, attempt.activation_id == act.id))
            .join(settings, settings.owner_user_id == self.link.owner_user_id)
        )
        if self.activation_parent is None:
            raise ProductSourceRetired()
        return self.activation_parent.extend(result)

    def check(self, rows):
        raise ProductSourceRetired()

    def check_with_permissions(self, rows, held):
        self.projection.check_with_permissions(rows, held)
        delivery, journal, callback, parent, _control, attempt, settings = rows[0][self.delivery_column : self.delivery_column + 7]
        controls = list({r[self.delivery_column + 4].id: r[self.delivery_column + 4] for r in rows if r[self.delivery_column + 4] is not None}.values())
        try:
            self.activation_parent.check(rows, journal, held)
        except (ValueError, KeyError, TypeError):
            raise ProductSourceRetired() from None
        primary = rows[0][self.activation_parent.column]
        act = qualify_waiting(self.link, journal, attempt, callback, parent, controls, self.intent["origin"], activation_parent=_snapshot(primary) if primary is not None else None)
        from app.telegram_bots.work_capacity_reads import capacity_scope

        if (
            not self.clock.capacity.capacity.available
            or self.clock.capacity.scope != capacity_scope(self.link)
            or capacity_scope(rows[0][0]) != capacity_scope(self.link)
            or rows[0][self.delivery_column + 7] != self.item.id
            or _item(delivery) != self.item
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.work_id, delivery.event_id, delivery.request_id)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, act.work_id, None, None)
            or delivery.dedupe_key != "free:" + act.id + ":" + str(self.intent["ordinal"])
            or delivery.class_ != ("reply" if self.item.payload.get("work_news_basis", {}).get("reveal") else "service")
            or parent.message_thread_id != delivery.message_thread_id
            or settings.locale != self.clock.settings.locale
            or settings.timezone != self.clock.settings.timezone
            or not startable_detail(self.projection.source, self.link)
        ):
            raise ProductSourceRetired()
        record = self.projection.source.record

        def date(value):
            return datetime.fromisoformat(value).astimezone(self.clock.zone).strftime("%Y-%m-%d %H:%M") if value else None

        data = thaw({"detail": asdict(self.projection.source), "waiting_act": act_facts(act), "origin": self.intent["origin"]})
        values = {"date": date(record["created_at"]), "due": date(record["due_at"])}
        source = WorkNewsRead(act.work_id, "capacity.offer", values, self.projection.source.protected, data, digest([act.id, self.intent, thaw(act.body)]), digest([data, values]))
        if self.source is not None and not same_body(asdict(source), asdict(self.source)):
            raise ProductSourceRetired()
        self.source = source
