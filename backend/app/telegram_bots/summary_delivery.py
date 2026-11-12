"""Daily summary persistence and source-owned delivery, dormant until installed."""

import asyncio
from datetime import date, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4
from zoneinfo import ZoneInfo

from sqlalchemy import String, cast, select
from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.notifications import aware, preference_skip
from app.telegram_bots.pages import page_address
from app.telegram_bots.producer import DeliveryClaimRetired, PermanentDeliveryError, ProductSourceRetired, _item, _safe_json
from app.telegram_bots.settings import SettingsRepository, _snapshot
from app.telegram_bots.summary import due_date
from app.telegram_bots.summary_sources import SummaryRead
from app.telegram_bots.summary_text import present_summary
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_navigation import _same_item
from app.telegram_bots.work_digest_text import _identity
from app.telegram_bots.work_result_navigation import NavigationOrigin
from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def recipes(basis, link, identity):
    if not any(basis["sections"].values()):
        return [], []
    minute = basis["summary_minute"]
    shown = present_summary(
        basis["sections"],
        locale=basis["locale"],
        protected=basis["protected"],
        concealed=basis["concealed"],
        outbox_id=identity,
        link_id=link.id,
        time=f"{minute // 60:02d}:{minute % 60:02d}",
        work_url=page_address("web.work", link, permissions=AgentPermission.INSPECT, public_base_url=basis["public_base_url"]),
    )
    keyboard = [list(line) for line in shown.keyboard]
    parts = [{**part, "silent": True, "stored_keyboard": True, "source_ref": {"key": "summary.presentation", "values": {"outbox_id": identity}}} for part in shown.parts]
    return parts, keyboard


def _validate_sections(value):
    if type(value) is not dict or set(value) != {"finished", "working", "interrupted", "waiting"}:
        raise ValueError()
    for section, entries in value.items():
        if type(entries) is not list:
            raise ValueError()
        seen = set()
        for entry in entries:
            if type(entry) is not dict or set(entry) != {"kind", "work_id", "request_id", "objective"}:
                raise ValueError()
            kinds = {"question", "decision", "review", "resume", "start"} if section == "waiting" else {section}
            if type(entry["kind"]) is not str or entry["kind"] not in kinds or not _identity(entry["work_id"]) or type(entry["objective"]) is not str:
                raise ValueError()
            requested = entry["kind"] in {"question", "decision", "review"}
            if requested and not _identity(entry["request_id"]) or not requested and entry["request_id"] is not None:
                raise ValueError()
            key = (entry["kind"], entry["work_id"], entry["request_id"])
            if key in seen:
                raise ValueError()
            seen.add(key)


def frozen_summary(captured, *, pending=False, acknowledged=False):
    try:
        item, basis, link = captured.item, captured.item.payload["summary_basis"], captured.link
        if type(basis) is not dict or set(basis) != {"version", "date", "as_of", "since", "sections", "protected", "concealed", "locale", "timezone", "summary_minute", "public_base_url", "parts", "keyboard"}:
            raise ValueError()
        if (
            type(basis["version"]) is not int
            or basis["version"] != 1
            or date.fromisoformat(basis["date"]).isoformat() != basis["date"]
            or datetime.fromisoformat(basis["as_of"]).utcoffset() is None
            or datetime.fromisoformat(basis["since"]).utcoffset() is None
            or datetime.fromisoformat(basis["since"]) > datetime.fromisoformat(basis["as_of"])
            or item.created_at is None
            or aware(item.created_at) != datetime.fromisoformat(basis["as_of"])
            or "summary_dispatch_conflict" in item.payload
            or item.silent is not True
            or basis["locale"] not in {"en-US", "zh-CN"}
            or type(basis["timezone"]) is not str
            or not basis["timezone"]
            or basis["public_base_url"] is not None
            and type(basis["public_base_url"]) is not str
            or type(basis["protected"]) is not bool
            or basis["concealed"] is not basis["protected"]
            or type(basis["summary_minute"]) is not int
            or not 0 <= basis["summary_minute"] < 1440
            or type(item.payload["presentation_version"]) is not int
            or item.payload["presentation_version"] != 0
            or item.class_ != "summary"
            or item.dedupe_key != "summary:" + basis["date"]
            or (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.instance_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref)
            or any(value is not None for value in (item.work_id, item.request_id, item.event_id, item.message_thread_id))
        ):
            raise ValueError()
        _validate_sections(basis["sections"])
        expected, keyboard = recipes(basis, link, item.id)
        if not same_body(expected, basis["parts"]) or not same_body(keyboard, basis["keyboard"]) or not same_body(keyboard, item.payload["product_keyboard_basis"]) or len(item.parts) != len(expected):
            raise ValueError()
        if not expected:
            if item.state != "skipped" or captured.part is not None:
                raise ValueError()
            return basis, None
        part = item.parts[0]
        metadata = {"part_id", "ordinal", "presentation_version", "state", "message_ids", "sent_at", "dispatched_at", "plain_fallback", "url_button_fallback", "invalid_token_verification_pending", "callback_group_id"}
        if (
            not same_body({key: value for key, value in part.items() if key not in metadata}, expected[0])
            or any(type(part[key]) is not int or part[key] != 0 for key in ("ordinal", "presentation_version"))
            or any(type(part.get(key)) is not str or len(part[key]) != 32 or any(character not in "0123456789abcdef" for character in part[key]) for key in ("part_id", "callback_group_id"))
            or part["state"] != ("sent" if acknowledged else "pending" if pending else "sending")
            or captured.part is not None
            and not same_body(captured.part, part)
            or type(part["message_ids"]) is not list
            or any(type(part[key]) is not bool for key in ("plain_fallback", "url_button_fallback") if key in part)
        ):
            raise ValueError()
        if acknowledged:
            if len(part["message_ids"]) != 1 or type(part["message_ids"][0]) is not int or part["message_ids"][0] <= 0 or datetime.fromisoformat(part["sent_at"]).utcoffset() is None:
                raise ValueError()
        elif part["message_ids"] or "sent_at" in part:
            raise ValueError()
        if "dispatched_at" in part and datetime.fromisoformat(part["dispatched_at"]).utcoffset() is None:
            raise ValueError()
        return basis, expected[0]
    except (ValueError, TypeError, KeyError, IndexError):
        raise PermanentDeliveryError("invalid-summary-presentation") from None


def source_from_basis(reader, link, basis):
    return SummaryRead(reader, link, aware(datetime.fromisoformat(basis["as_of"])), aware(datetime.fromisoformat(basis["since"])), basis["sections"], basis["protected"])


class SummaryPlans:
    def __init__(self, *, reader, outbox, callbacks, available=lambda: True):
        self.reader, self.outbox, self.callbacks, self.available = reader, outbox, callbacks, available
        self.settings = SettingsRepository(outbox.sessions, outbox.config)
        if any(sessions.kw["bind"] is not outbox.sessions.kw["bind"] for sessions in (reader.links.session_factory, callbacks.sessions)):
            raise ValueError("summary-requires-shared-storage")

    async def plan(self, link):
        async def perform():
            if not self.available():
                return None
            settings = await self.settings.get(link.owner_user_id)
            if settings is None:
                return None
            now = aware(self.outbox.clock())
            zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
            scheduled = due_date(now, settings, zone)
            if scheduled is None:
                return None
            key = "summary:" + scheduled
            async with self.outbox.sessions() as reader:
                existing = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key))
                if existing is not None:
                    return existing.id
                previous = await reader.scalar(
                    select(TelegramOutboxRow)
                    .where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.class_ == "summary", TelegramOutboxRow.created_at < now, cast(TelegramOutboxRow.payload["summary_basis"]["version"].as_string(), String) == "1")
                    .order_by(TelegramOutboxRow.created_at.desc())
                    .limit(1)
                )
            beginning = now - timedelta(days=1)
            if previous is not None:
                try:
                    part = previous.parts[-1] if previous.parts else None
                    frozen_summary(SimpleNamespace(link=link, item=_item(previous), part=part), pending=part is not None and part["state"] == "pending", acknowledged=part is not None and part["state"] == "sent")
                except (PermanentDeliveryError, KeyError, TypeError, IndexError):
                    pass
                else:
                    beginning = aware(previous.created_at)
            source = await self.reader.read(link, as_of=now, since=beginning)
            origin = NavigationOrigin()
            basis = {
                "version": 1,
                "date": scheduled,
                "as_of": now.isoformat(),
                "since": source.since.isoformat(),
                "sections": source.sections,
                "protected": source.protected,
                "concealed": source.protected,
                "locale": settings.locale,
                "timezone": settings.timezone,
                "summary_minute": settings.summary_minute,
                "public_base_url": origin.base,
            }
            async with self.outbox._writer(link.id) as (writer, _binding):
                projection = _SummaryWriteProjection(self, source, basis, zone)
                await source.authorize(session=writer, projection=projection)
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key))
                if existing is not None:
                    return existing.id
                identity = uuid4().hex
                parts, keyboard = recipes(basis, link, identity)
                row = TelegramOutboxRow(
                    id=identity,
                    link_id=link.id,
                    owner_user_id=link.owner_user_id,
                    binding_generation=link.binding_generation,
                    dedupe_key=key,
                    class_="summary",
                    instance_id=link.agent_ref,
                    chat_id=link.chat_id,
                    payload=_safe_json({"presentation_version": 0, "summary_basis": {**basis, "parts": parts, "keyboard": keyboard}, "product_keyboard_basis": keyboard}),
                    parts=self.outbox._parts(parts, 0) if parts else [],
                    state="pending",
                    silent=True,
                    created_at=now,
                    not_before=now,
                )
                writer.add(row)
                await writer.flush()
                if parts:
                    group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
                    row.parts = [{**row.parts[0], "callback_group_id": group.group_id}]
                    for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                        control.payload = {**control.payload, "presentation_pending": True}
                        control.created_at = now
                else:
                    row.state, row.skip_reason, row.closed_at = "skipped", "nothing-to-say", self.outbox.clock()
                await writer.flush()
                frozen_summary(SimpleNamespace(link=link, item=_item(row), part=row.parts[0] if row.parts else None), pending=True)
                projection.item = _item(row)
                await source.authorize(session=writer, projection=projection)
                origin.check(basis)
                return identity

        return await await_drained(perform())


class _SummaryWriteProjection:
    def __init__(self, plans, source, basis, zone, *, captured=None, silent=True, now=None):
        self.source = source
        self.plans, self.basis, self.zone = plans, basis, zone
        self.item, self.captured, self.silent, self.now = captured.item if captured else None, captured, silent, now or source.as_of
        self.origin = NavigationOrigin()

    def extend(self, query):
        self.delivery_column = len(query.column_descriptions)
        settings, delivery, control = TelegramPersonSettingsRow, aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        query = query.add_columns(settings).join(settings, settings.owner_user_id == self.source.link.owner_user_id)
        if self.item is not None:
            groups = [part["callback_group_id"] for part in self.item.parts if "callback_group_id" in part]
            query = query.add_columns(delivery, control).join(delivery, delivery.id == self.item.id).outerjoin(control, control.group_id.in_(groups))
        return query

    def check_with_permissions(self, rows, held):
        self.origin.check(self.basis)
        if not self.plans.available():
            raise ProductSourceRetired()
        settings = _snapshot(rows[0][self.delivery_column])
        if (settings.locale, settings.timezone, settings.summary_minute) != (self.basis["locale"], self.basis["timezone"], self.basis["summary_minute"]) or not settings.summary_enabled:
            raise ProductSourceRetired()
        if self.captured is None and due_date(self.source.as_of, settings, self.zone) != self.basis["date"]:
            raise ProductSourceRetired()
        if self.item is None:
            return
        row = rows[0][self.delivery_column + 1]
        if not _same_item(row, self.item):
            raise DeliveryClaimRetired()
        if self.captured is not None:
            captured = self.captured
            current = self.plans.outbox._sending(rows[0][0], row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
            if current != captured or self.silent is not True or preference_skip(captured.item, settings, self.now):
                raise DeliveryClaimRetired()
        elif claim_of(row) != (None, None, None):
            raise DeliveryClaimRetired()
        if not row.parts:
            return
        controls = list({control.id: control for record in rows if (control := record[self.delivery_column + 2]) is not None}.values())
        try:
            _validate_rows(row, controls, allow_pending=True)
            for control in controls:
                self.plans.callbacks._usable(control)
                if aware(control.created_at) != aware(row.created_at) or control.message_id is not None or control.payload.get("presentation_pending") is not True:
                    raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None


class SummarySources:
    def __init__(self, plans):
        self.plans = plans

    async def read_part(self, captured):
        basis, part = frozen_summary(captured)
        source = source_from_basis(self.plans.reader, captured.link, basis)
        await source.authorize()
        return part["html"], part["visible_text"]

    async def authorize(self, captured, *, silent, now):
        async def perform():
            basis, _part = frozen_summary(captured)
            source = source_from_basis(self.plans.reader, captured.link, basis)
            zone = await asyncio.to_thread(ZoneInfo, basis["timezone"])
            await source.authorize(projection=_SummaryWriteProjection(self.plans, source, basis, zone, captured=captured, silent=silent, now=now))

        return await await_drained(perform())


async def acknowledge_summary(session, row):
    from app.telegram_bots.links import BotLink

    binding = await session.get(TelegramBotLinkRow, row.link_id)
    if binding is None:
        raise ProductSourceRetired()
    frozen_summary(SimpleNamespace(link=BotLink.from_row(binding), item=_item(row), part=row.parts[-1]), acknowledged=True)
    controls = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
    try:
        _validate_rows(row, controls, allow_pending=True)
        if any(aware(control.created_at) != aware(row.created_at) or control.message_id is not None or control.payload.get("presentation_pending") is not True for control in controls):
            raise CallbackRefused()
    except CallbackRefused:
        raise ProductSourceRetired() from None
