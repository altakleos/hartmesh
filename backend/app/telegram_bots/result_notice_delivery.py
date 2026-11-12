"""Dormant result notification plans and current SDK source admission."""

import asyncio
from dataclasses import asdict
from datetime import timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import false, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import _SCOPE, current_attention, prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.notifications import aware, preference_skip
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, PermanentDeliveryError, ProductSourceRetired, _item, _safe_json
from app.telegram_bots.result_notice_sources import ResultNoticeProjection, ResultNoticeRead
from app.telegram_bots.result_notices import present_result
from app.telegram_bots.settings import SettingsRepository, _snapshot
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_request_source import cleanup_ready, require_show
from app.telegram_bots.work_result_files import result_options
from app.telegram_bots.work_result_history import qualified_history
from app.telegram_bots.work_result_identity import qualify_identity
from app.telegram_bots.work_result_ledger import initial_ledger, qualify_result_parts
from app.telegram_bots.work_result_navigation import qualify_navigation
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def presentation(source, basis, identity):
    from app.telegram_bots.render import render_plain

    return present_result(
        source.work,
        key=source.key,
        name=source.name,
        person=source.person or render_plain("label.colleague", basis["locale"]),
        protected=source.protected,
        locale=basis["locale"],
        concealed=basis["concealed"],
        outbox_id=identity,
        **result_options(basis["result_files"]),
    )


def recipes(source, basis, identity):
    view = presentation(source, basis, identity)
    controls = [list(line) for line in view.keyboard]
    parts = [{**part, "source_ref": {"key": "result.presentation", "values": {"work_id": source.work["id"], "part": index}}} for index, part in enumerate(view.parts)]
    if controls:
        parts[-1]["stored_keyboard"] = True
    return parts, controls


def frozen_notice(captured, *, acknowledged=False, pending=False):
    try:
        item, basis = captured.item, captured.item.payload["result_notice_basis"]
        if (
            type(basis) is not dict
            or set(basis)
            != {"version", "source", "locale", "concealed", "result_files", "parts", "keyboard"} | ({"reveal", "cleanup_id"} if basis.get("version") == 2 else set()) | ({"navigation_origin"} if "navigation_origin" in basis else set())
            or type(basis["version"]) is not int
            or basis["version"] not in {1, 2}
        ):
            raise ValueError()
        reveal = basis.get("reveal")
        navigation = basis.get("navigation_origin")
        if "navigation_origin" in basis:
            from app.telegram_bots.work_navigation_origin import validate_origin

            validate_origin(navigation)
            if basis["concealed"] or item.dedupe_key != "notice:work-navigation:" + navigation["query_key"]:
                raise ValueError()
        if (
            type(item.payload["presentation_version"]) is not int
            or (item.payload["presentation_version"] != 0 if basis["version"] == 1 else item.payload["presentation_version"] < 1)
            or item.class_ != ("reply" if reveal or navigation else "result")
        ):
            raise ValueError()
        if basis["version"] == 2 and (
            (reveal is None) != (basis["cleanup_id"] is None)
            or reveal is not None
            and (any(type(value) is not str or len(value) != 32 or any(char not in "0123456789abcdef" for char in value) for value in (reveal, basis["cleanup_id"])) or basis["concealed"])
        ):
            raise ValueError()
        source = ResultNoticeRead(**basis["source"])
        if (
            source.key != item.payload["work_event_intent"]["key"]
            or (source.work["instance_id"], source.work["id"]) != (captured.link.agent_ref, item.work_id)
            or type(source.protected) is not bool
            or type(basis["concealed"]) is not bool
            or basis["concealed"] != (source.protected and reveal is None and navigation is None)
            or source.identity != item.payload["work_result_identity"]
            or source.navigation != item.payload["work_result_navigation"]
        ):
            raise ValueError()
        qualify_identity(source.identity, source)
        qualify_navigation(source.navigation, captured.link, source)
        parts, controls = recipes(source, basis, item.id)
        if not same_body(parts, basis["parts"]) or not same_body(controls, basis["keyboard"]) or not same_body(controls, item.payload["product_keyboard_basis"]):
            raise ValueError()
        carried, hashes = qualified_history(item, source)
        recipe = qualify_result_parts(item, parts, selected=captured.part, acknowledged=acknowledged, pending=pending, carried=carried, known_hashes=hashes)
        return basis, source, recipe
    except (KeyError, TypeError, ValueError, IndexError):
        raise PermanentDeliveryError("invalid-result-notice-source") from None


class ResultNoticePlans:
    def __init__(self, *, reader, outbox, callbacks):
        self.reader, self.outbox, self.callbacks = reader, outbox, callbacks
        self.settings = SettingsRepository(outbox.sessions, outbox.config)
        if reader.links.session_factory.kw.get("bind") is not outbox.sessions.kw.get("bind") or callbacks.sessions.kw.get("bind") is not outbox.sessions.kw.get("bind"):
            raise ValueError("result-notice-requires-shared-storage")

    async def materialize(self, link, identity):
        return await await_drained(self._materialize(link, identity))

    async def _materialize(self, link, identity):
        async with self.outbox.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            if row is None or not row.parts and any(getattr(row, field) is not None for field in ("claim_owner", "claim_id", "claimed_at")):
                raise ProductSourceRetired()
            item = _item(row)
        if item.parts:
            frozen_notice(SimpleNamespace(link=link, item=item, part=None))
            return True
        if item.state != "pending" or item.payload.get("presentation_version") != 0:
            raise ProductSourceRetired()
        source = await self.reader.read(link, item)
        settings = await self.settings.get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        admission = await prepare_attention(self.reader, link)
        async with self.outbox._writer(link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
            if binding is None or any(getattr(binding, name) != getattr(link, name) for name in _SCOPE) or row is None or _item(row) != item or any(getattr(row, field) is not None for field in ("claim_owner", "claim_id", "claimed_at")):
                raise DeliveryRetry(timedelta(seconds=5))
            await admission(session=writer, projection=_MaterializationProjection(self.reader, link, item, source))
            basis = {
                "version": 1,
                "source": asdict(source),
                "locale": settings.locale,
                "concealed": source.protected,
                "result_files": {"version": 1, "capacity": self.outbox.config.files.in_memory_max_bytes, "max_bytes": self.outbox.config.files.max_outbound_bytes},
            }
            parts, controls = recipes(source, basis, identity)
            row.parts = self.outbox._parts(parts, 0)
            row.payload = _safe_json(
                {**row.payload, "result_notice_basis": {**basis, "parts": parts, "keyboard": controls}, "product_keyboard_basis": controls, "work_result_identity": source.identity, "work_result_navigation": source.navigation}
            )
            if controls:
                group = await self.callbacks.write_group(link, controls, anchor_outbox_id=identity, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
                row.parts = [*row.parts[:-1], {**row.parts[-1], "callback_group_id": group.group_id}]
                for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                    control.payload = {**control.payload, "presentation_pending": True}
            row.payload = {**row.payload, "work_result_ledger": initial_ledger(row.parts)}
            await writer.flush()
            await admission(session=writer, projection=_MaterializationProjection(self.reader, link, _item(row), source))
            return True


class _MaterializationProjection(ResultNoticeProjection):
    def check(self, rows):
        super().check(rows)
        delivery = rows[0][self.column + 6]
        if any(getattr(delivery, field) is not None for field in ("claim_owner", "claim_id", "claimed_at")):
            raise ProductSourceRetired()


class ResultNoticeSources:
    require_write = False

    def __init__(self, plans, *, navigation=None):
        from app.telegram_bots.result_notice_recovery import ResultNoticeRecovery

        self.reader, self.outbox, self.callbacks, self.settings = plans.reader, plans.outbox, plans.callbacks, plans.settings
        self.recovery = ResultNoticeRecovery(plans, navigation=navigation)
        self.navigation = navigation

    async def _prepare(self, captured):
        basis, source, part = frozen_notice(captured)
        current = await self.reader.read(captured.link, captured.item)
        if current.event_signature != source.event_signature or current.identity != source.identity:
            raise ProductSourceRetired()
        settings = await self.settings.get(captured.link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        if not same_body(asdict(current), asdict(source)) or settings.locale != basis["locale"]:
            await self.recovery.refresh(captured)
            raise DeliveryClaimRetired()
        return basis, source, part

    async def read_part(self, captured):
        async def read():
            _basis, _source, part = await self._prepare(captured)
            return ("", "") if part["kind"] in {"document", "photo", "album"} or "result_file_warning" in part else (part["html"], part["visible_text"])

        return await await_drained(read())

    async def file_projection(self, captured, *, now, silent=True):
        basis, source, _part = await self._prepare(captured)
        settings = await self.settings.get(captured.link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
        from app.telegram_bots.work_navigation_sources import qualify_navigation

        return await qualify_navigation(self.navigation, captured, basis, source, _SendProjection(self, captured, source, basis, settings, zone, now, silent), family="result_notice_basis")

    async def authorize(self, captured, *, silent, now):
        async def read():
            projection = await self.file_projection(captured, now=now, silent=silent)
            await current_attention(self.reader, captured.link, projection=projection)

        return await await_drained(read())


class _SendProjection(ResultNoticeProjection):
    def __init__(self, host, captured, source, basis, settings, zone, now, silent):
        super().__init__(host.reader, captured.link, captured.item, source)
        self.host, self.captured, self.basis, self.settings, self.zone, self.now, self.silent = host, captured, basis, settings, zone, now, silent
        self.group = next((part["callback_group_id"] for part in captured.item.parts if part.get("stored_keyboard") and part["presentation_version"] == captured.item.payload["presentation_version"]), None)

    def extend(self, query):
        result = super().extend(query)
        self.send_column = len(result.column_descriptions)
        cleanup = aliased(TelegramOutboxRow)
        return (
            result.add_columns(TelegramPersonSettingsRow, TelegramCallbackRow, cleanup)
            .join(TelegramPersonSettingsRow, TelegramPersonSettingsRow.owner_user_id == TelegramBotLinkRow.owner_user_id)
            .outerjoin(TelegramCallbackRow, or_(TelegramCallbackRow.group_id == self.group if self.group else false(), TelegramCallbackRow.id == self.basis.get("reveal") if self.basis.get("reveal") else false()))
            .outerjoin(cleanup, cleanup.id == self.basis.get("cleanup_id") if self.basis.get("cleanup_id") else false())
        )

    def check(self, rows):
        super().check(rows)
        delivery = rows[0][self.column + 6]
        current = self.host.outbox._sending(
            rows[0][0], delivery, link_id=self.link.id, part_id=self.captured.part["part_id"], claim_owner=self.captured.claim_owner, claim_id=self.captured.claim_id, token_revision=self.captured.token_revision
        )
        if current != self.captured:
            raise DeliveryClaimRetired()
        actual = _snapshot(rows[0][self.send_column])
        if actual.locale != self.basis["locale"] or actual.timezone != self.settings.timezone or preference_skip(self.item, actual, self.now):
            raise ProductSourceRetired()
        local = self.now.astimezone(self.zone)
        minute, start, end = local.hour * 60 + local.minute, actual.quiet_start_minute, actual.quiet_end_minute
        quiet = actual.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
        recent = rows[0][0].last_update_at is not None and self.now - aware(rows[0][0].last_update_at) <= timedelta(seconds=self.host.outbox.config.outbox.recent_act_seconds)
        if bool(delivery.silent or self.captured.part.get("silent") or quiet and not recent) != bool(self.silent) or self.source.protected and not self.captured.part.get("protect_content"):
            raise DeliveryClaimRetired()
        controls = list({control.id: control for row in rows if (control := row[self.send_column + 1]) is not None}.values())
        if self.basis.get("reveal"):
            reveal = next((row for row in controls if row.id == self.basis["reveal"]), None)
            require_show(reveal, delivery)
            cleanup = rows[0][self.send_column + 2]
            if not cleanup_ready(delivery, cleanup):
                raise DeliveryRetry(timedelta(seconds=5))
            if cleanup.payload["target_message_id"] != reveal.message_id:
                raise ProductSourceRetired()
        if self.group:
            controls = [row for row in controls if row.group_id == self.group]
            try:
                _validate_rows(delivery, controls, allow_pending=True)
            except CallbackRefused:
                raise ProductSourceRetired() from None
            age_now = self.host.callbacks.clock()
            if any(
                control.consumed_at is not None or control.expires_at is not None and aware(control.expires_at) <= age_now or age_now >= aware(control.created_at) + timedelta(days=self.host.callbacks.config.callbacks.max_age_days)
                for control in controls
            ):
                raise ProductSourceRetired()


async def acknowledge_notice(session, row):
    from app.telegram_bots.links import BotLink

    binding = await session.get(TelegramBotLinkRow, row.link_id)
    if binding is None:
        raise ProductSourceRetired()
    frozen_notice(SimpleNamespace(link=BotLink.from_row(binding), item=_item(row), part=row.parts[-1]), acknowledged=True)
