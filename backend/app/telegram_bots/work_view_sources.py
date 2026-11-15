"""Current full Work views share the durable notice sender and SDK fencing."""

import asyncio
from dataclasses import asdict, replace
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import and_, false, select, true
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.render import render
from app.telegram_bots.settings import SettingsRepository
from app.telegram_bots.split import split_html, visible_text
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
    shown = present_work_detail(
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
    if source.work.get("origin", {}).get("refusal") == "start.changed":
        html = render("start.changed", basis["locale"]) + "\n\n" + "".join(part["html"] for part in shown.parts)
        return replace(shown, parts=tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": source.protected} for chunk in split_html(html)))
    return shown


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
                detail = await self.detail(link, intent["work_id"], ignore_activation=intent["origin"].get("waiting_act_id"))

                def date(value):
                    return datetime.fromisoformat(value).astimezone(clock.zone).strftime("%Y-%m-%d %H:%M") if value else None

                data, protected, values = {"detail": asdict(detail)}, detail.protected, {"date": date(detail.record["created_at"]), "due": date(detail.record["due_at"])}
            data.update(timezone=clock.settings.timezone, origin=intent["origin"])
            data = thaw(data)
            source = WorkNewsRead(item.work_id, intent["key"], values, protected, data, digest([link.id, link.binding_generation, intent]), digest([data, values]))
            projection = WorkViewProjection(self, link, item, source, clock=clock)
            await projection.prepare_source()
            admission = await prepare_attention(self, link)
            await admission(projection=projection)
            return projection.source

        return await await_drained(perform())


class WorkViewProjection:
    def __init__(self, reader, link, item, source=None, *, clock, origin_only=False):
        if source is None:
            raise ProductSourceRetired()
        self.reader, self.link, self.item, self.source, self.clock = reader, link, item, source, clock
        self.origin_only, self.original_projection = origin_only, None
        self.refresh = None
        if source.work["origin"].get("refusal") is not None:
            from app.telegram_bots.work_start_refresh import UnadmittedStartProjection

            self.refresh = UnadmittedStartProjection(link, source.work["origin"], item)
        if source.key == "view.detail":
            detail = detail_read(source.work["detail"])
            self.projection = WorkDetailProjection(reader, link, detail.record, detail.request, reusable=detail.can_reuse_conversation, expected=detail, ignore_activation=source.work["origin"].get("waiting_act_id"))
        elif source.key == "view.list":
            page = page_read(source.work["page"])
            self.projection = WorkPageProjection(reader, link, page.frame, page.details, page.snapshot_signature, expected=page)
        else:
            raise ProductSourceRetired()

    async def prepare_source(self, *, trail=(), activation_id=None):
        if self.refresh is not None:
            from app.telegram_bots.work_start_refresh import refresh_origin_projection

            self.original_projection = await refresh_origin_projection(self.reader, self.link, self.item, trail=trail, activation_id=activation_id)
        return self

    def with_item(self, item):
        from copy import copy

        current = copy(self)
        current.item = item
        if self.refresh is not None:
            from app.telegram_bots.work_start_refresh import UnadmittedStartProjection

            current.refresh = UnadmittedStartProjection(self.link, self.source.work["origin"], item)
        return current

    def extend(self, query):
        if self.origin_only:
            from app.telegram_bots.work_digest_projection import _member_base, _source_columns

            local = self._extend_local(_member_base(self.link))
            nested = local.subquery()
            columns, self.local_decoders = _source_columns(local, nested)
            self.local_offset = len(query.column_descriptions)
            result = query.add_columns(*columns).outerjoin(nested, true())
        else:
            result = self._extend_local(query)
        if self.refresh is not None and self.original_projection is None:
            raise ProductSourceRetired()
        return self.original_projection.extend(result) if self.original_projection is not None else result

    def _extend_local(self, query):
        from app.telegram_bots.work_untied_delivery import callback_facts

        result = query if self.origin_only else self.projection.extend(query)
        self.delivery_column = len(result.column_descriptions)
        delivery, mark, gesture, parent, settings = (
            aliased(TelegramOutboxRow),
            aliased(TelegramInboundMarkRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramOutboxRow),
            aliased(TelegramPersonSettingsRow),
        )
        origin = self.source.work["origin"]
        summary_root, summary_control = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        locator = aliased(TelegramCallbackRow)
        group = select(locator.group_id).where(locator.id == origin.get("callback_id")).scalar_subquery()
        controls = callback_facts(
            self.reader.links.session_factory,
            and_(TelegramCallbackRow.group_id == group, TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == parent.id, TelegramCallbackRow.payload["presentation_version"].as_integer() == origin.get("presentation_version")),
        )
        summary_group = select(summary_control.group_id).where(summary_control.id == parent.payload["summary_view_basis"]["callback_id"].as_string()).correlate(parent).scalar_subquery()
        summary_controls = callback_facts(self.reader.links.session_factory, TelegramCallbackRow.group_id == summary_group)
        result = (
            result.add_columns(delivery, mark, gesture, parent, controls, settings, summary_root, summary_controls)
            .join(delivery, delivery.id == self.item.id)
            .outerjoin(mark, and_(mark.link_id == self.link.id, mark.chat_id == self.link.chat_id, mark.message_id == origin.get("message_id")) if origin.get("kind") == "inbox" else false())
            .outerjoin(gesture, gesture.id == origin.get("gesture_id") if origin.get("kind") == "callback" else false())
            .outerjoin(parent, parent.id == origin.get("anchor_outbox_id") if origin.get("kind") == "callback" else false())
            .join(settings, settings.owner_user_id == self.link.owner_user_id)
            .outerjoin(summary_root, summary_root.id == parent.payload["summary_view_basis"]["parent_id"].as_string())
        )
        result = self.refresh.extend(result) if self.refresh is not None else result
        return result

    def check_with_permissions(self, rows, held):
        local = rows
        if self.origin_only:
            width = len(self.local_decoders)
            local = [(*row[:8], *(decode(value) for decode, value in zip(self.local_decoders, row[self.local_offset : self.local_offset + width], strict=True))) for row in rows]
            if not local or local[0][self.delivery_column] is None:
                raise ProductSourceRetired()
        if not self.origin_only:
            self.projection.check_with_permissions(local, held)
        self.check_delivery(local)
        if self.refresh is not None:
            self.refresh.check(local)
        if self.original_projection is not None:
            self.original_projection.check_with_permissions(rows, held)

    def check(self, rows):
        raise ProductSourceRetired()

    def check_delivery(self, rows):
        delivery = rows[0][self.delivery_column]
        intent = self.item.payload.get("work_view_intent", {})
        actual = self.projection.source
        if not self.origin_only and (
            self.source.protected is not actual.protected or self.source.key == "view.list" and actual.frame["cursor"] != intent.get("cursor") or self.source.key == "view.detail" and actual.record["id"] != intent.get("work_id")
        ):
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
            or not self.origin_only
            and rows[0][self.delivery_column + 5].timezone != self.source.work["timezone"]
        ):
            raise ProductSourceRetired()
        if self.source.event_signature != digest([self.link.id, self.link.binding_generation, intent]) or self.source.signature != digest([self.source.work, self.source.values]):
            raise ProductSourceRetired()
        qualify_origin(self.link, self.item, intent["origin"], rows, self.delivery_column + 1, self.clock.now, historical=self.origin_only)
        parent = rows[0][self.delivery_column + 3]
        if parent is not None and "summary_view_basis" in parent.payload:
            from app.telegram_bots.work_request_source import _RequestProjection
            from app.telegram_bots.work_summary_origins import qualify_summary_parent

            controls = {control.id: control for row in rows for control in _RequestProjection.objects(row[self.delivery_column + 7])}
            qualify_summary_parent(self.link, _item(parent), rows[0][self.delivery_column + 6], controls, self.clock.now)


def source_key(intent):
    key = intent.get("key")
    if key not in VIEW_KEYS or type(intent.get("version")) is not int or intent["version"] != 1 or set(intent) != {"version", "key", "origin", "cursor" if key == "view.list" else "work_id"}:
        raise ProductSourceRetired()
    return key
