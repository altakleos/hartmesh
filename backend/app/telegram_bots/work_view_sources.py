"""Current full Work views share the durable notice sender and SDK fencing."""

import asyncio
from dataclasses import asdict
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import and_, false, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.settings import SettingsRepository
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_details import WorkDetailProjection, WorkDetailRead
from app.telegram_bots.work_news_sources import WorkNewsRead
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_stop_news import NewsClock
from app.telegram_bots.work_view_origins import qualify_origin
from app.telegram_bots.work_view_reads import WorkPageProjection, WorkPageRead, WorkViewsReader
from app.telegram_bots.work_views import present_work_detail, present_work_page
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained

VIEW_KEYS = frozenset({"view.list", "view.detail"})


def detail_read(value):
    return WorkDetailRead(**{**value, "held": tuple(value["held"])})


def page_read(value):
    return WorkPageRead(**{**value, "details": tuple(detail_read(item) for item in value["details"])})


def present_view(source, basis, link):
    if source.key == "view.list":
        page = page_read(source.work["page"])
        return present_work_page(page.frame, link=link, owner_id=link.owner_user_id, name=page.name, locale=basis["locale"], protected=source.protected, public_base_url=basis["public_base_url"])
    detail = detail_read(source.work["detail"])
    return present_work_detail(
        detail.record,
        owner_id=link.owner_user_id,
        permissions=detail.permissions,
        held=detail.held,
        locale=basis["locale"],
        person=detail.person,
        recipient=detail.recipient,
        request=detail.request,
        date=source.values["date"],
        due=source.values["due"],
        protected=source.protected,
        lifecycle_pending=detail.lifecycle_pending,
        can_reuse_conversation=detail.can_reuse_conversation,
        link=link,
        public_base_url=basis["public_base_url"],
    )


class WorkViewReader(WorkViewsReader):
    async def prepare_clock(self, link, item):
        settings = await SettingsRepository(self.links.session_factory, self.links.config).get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        return NewsClock(settings, await asyncio.to_thread(ZoneInfo, settings.timezone), self.clock)

    async def read(self, link, item):
        async def perform():
            intent = item.payload.get("work_view_intent", {})
            if intent.get("version") != 1 or type(intent.get("version")) is not int or intent.get("key") not in VIEW_KEYS:
                raise ProductSourceRetired()
            clock = await self.prepare_clock(link, item)
            if intent["key"] == "view.list":
                page = await self.read_page(link, intent["cursor"])
                data, protected, values = {"page": asdict(page)}, page.protected, {}
            else:
                detail = await self.detail(link, intent["work_id"])

                def date(value):
                    return datetime.fromisoformat(value).astimezone(clock.zone).strftime("%Y-%m-%d %H:%M") if value else None

                data, protected, values = {"detail": asdict(detail)}, detail.protected, {"date": date(detail.record["created_at"]), "due": date(detail.record["due_at"])}
            data.update(timezone=clock.settings.timezone, origin=intent["origin"])
            data = thaw(data)
            source = WorkNewsRead(item.work_id, intent["key"], values, protected, data, digest([link.id, link.binding_generation, intent]), digest([data, values]))
            projection = WorkViewProjection(self, link, item, source, clock=clock)
            admission = await prepare_attention(self, link)
            await admission(projection=projection)
            return projection.source

        return await await_drained(perform())


class WorkViewProjection:
    def __init__(self, reader, link, item, source=None, *, clock):
        if source is None:
            raise ProductSourceRetired()
        self.reader, self.link, self.item, self.source, self.clock = reader, link, item, source, clock
        if source.key == "view.detail":
            detail = detail_read(source.work["detail"])
            self.projection = WorkDetailProjection(reader, link, detail.record, detail.request, reusable=detail.can_reuse_conversation, expected=detail)
        elif source.key == "view.list":
            page = page_read(source.work["page"])
            self.projection = WorkPageProjection(reader, link, page.frame, page.details, page.snapshot_signature, expected=page)
        else:
            raise ProductSourceRetired()

    def extend(self, query):
        result = self.projection.extend(query)
        self.delivery_column = len(result.column_descriptions)
        delivery, mark, gesture, parent, control, settings = TelegramOutboxRow, aliased(TelegramInboundMarkRow), aliased(TelegramCallbackRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), aliased(TelegramPersonSettingsRow)
        origin = self.source.work["origin"]
        return (
            result.add_columns(delivery, mark, gesture, parent, control, settings)
            .join(delivery, delivery.id == self.item.id)
            .outerjoin(mark, and_(mark.link_id == self.link.id, mark.chat_id == self.link.chat_id, mark.message_id == origin.get("message_id")) if origin.get("kind") == "inbox" else false())
            .outerjoin(gesture, gesture.id == origin.get("gesture_id") if origin.get("kind") == "callback" else false())
            .outerjoin(parent, parent.id == origin.get("anchor_outbox_id") if origin.get("kind") == "callback" else false())
            .outerjoin(
                control,
                and_(
                    control.group_id == select(TelegramCallbackRow.group_id).where(TelegramCallbackRow.id == origin.get("callback_id")).scalar_subquery(),
                    control.payload["anchor_outbox_id"].as_string() == parent.id,
                    control.payload["presentation_version"].as_integer() == origin.get("presentation_version"),
                ),
            )
            .join(settings, settings.owner_user_id == self.link.owner_user_id)
        )

    def check_with_permissions(self, rows, held):
        self.projection.check_with_permissions(rows, held)
        self.check_delivery(rows)

    def check(self, rows):
        raise ProductSourceRetired()

    def check_delivery(self, rows):
        delivery = rows[0][self.delivery_column]
        intent = self.item.payload.get("work_view_intent", {})
        actual = self.projection.source
        if self.source.protected is not actual.protected or self.source.key == "view.list" and actual.frame["cursor"] != intent.get("cursor") or self.source.key == "view.detail" and actual.record["id"] != intent.get("work_id"):
            raise ProductSourceRetired()
        if (
            _item(delivery) != self.item
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.class_)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, "reply")
            or delivery.work_id != intent.get("work_id")
            or delivery.request_id is not None
            or delivery.event_id is not None
            or source_key(intent) != self.source.key
            or not same_body(intent.get("origin"), self.source.work["origin"])
            or rows[0][self.delivery_column + 5].timezone != self.source.work["timezone"]
        ):
            raise ProductSourceRetired()
        if self.source.event_signature != digest([self.link.id, self.link.binding_generation, intent]) or self.source.signature != digest([self.source.work, self.source.values]):
            raise ProductSourceRetired()
        qualify_origin(self.link, self.item, intent["origin"], rows, self.delivery_column + 1, self.clock.now)


def source_key(intent):
    key = intent.get("key")
    if key not in VIEW_KEYS or type(intent.get("version")) is not int or intent["version"] != 1 or set(intent) != {"version", "key", "origin", "cursor" if key == "view.list" else "work_id"}:
        raise ProductSourceRetired()
    return key
