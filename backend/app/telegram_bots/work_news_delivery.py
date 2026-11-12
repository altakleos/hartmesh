"""Dormant source-owned colleague news plans and final SDK admission."""

import asyncio
from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import false, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attempt_text import ATTEMPT_KEYS
from app.telegram_bots.attention_authority import _SCOPE, current_attention, prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows, canonical_keyboard
from app.telegram_bots.notifications import aware, preference_skip
from app.telegram_bots.pages import page_address
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, PermanentDeliveryError, ProductSourceRetired, _item, _safe_json
from app.telegram_bots.settings import SettingsRepository, _snapshot
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_news import news_class, present_news
from app.telegram_bots.work_news_sources import WorkNewsProjection, WorkNewsRead, same_news_origin
from app.telegram_bots.work_request_source import cleanup_ready, require_show
from app.telegram_bots.work_result_navigation import NavigationOrigin
from app.telegram_bots.work_stop_news import NewsClock
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def recipes(source, basis, identity, link):
    view = present_news(source.key, values=source.values, locale=basis["locale"], protected=source.protected, concealed=basis["concealed"], outbox_id=identity)
    keyboard = [list(line) for line in view.keyboard]
    if not basis["concealed"] and source.key in ATTEMPT_KEYS and source.values.get("open_files"):
        address = page_address("web.files", link, permissions=3, public_base_url=basis["public_base_url"])
        if address:
            keyboard.append([{"label": "label.open_files", "url": address}])
    if not basis["concealed"] and (
        source.key in {"decision.recorded", "result.changes", "attempt.stopped", "attempt.stopped_changed", "work.proposed", "remind.ready_to_resume"} | ATTEMPT_KEYS
        or source.key == "request.answered"
        and source.values.get("open_assignment") is True
    ):
        address = page_address("web.work", link, work_id=source.work_id, permissions=3, public_base_url=basis["public_base_url"])
        if address:
            keyboard.append([{"label": "label.open_assignment", "url": address}])
    if not basis["concealed"] and source.key == "work.proposed":
        keyboard.append([{"label": "label.not_now", "kind": "dismiss", "target_kind": "outbox", "target_id": identity, "payload": {}}])
    if not basis["concealed"] and source.key == "attempt.no_result":
        address = page_address("web.conversation", link, thread_id=source.values.get("thread_id"), public_base_url=basis["public_base_url"])
        if address:
            keyboard.append([{"label": "label.open_conversation", "url": address}])
    keyboard = canonical_keyboard(keyboard)
    parts = [{**part, "source_ref": {"key": "work.news.presentation", "values": {"work_id": source.work_id, "part": index}}} for index, part in enumerate(view.parts)]
    if keyboard:
        parts[-1]["stored_keyboard"] = True
    return parts, keyboard


def frozen_news(captured, *, acknowledged=False, pending=False, _history=None):
    try:
        item = captured.item
        basis = item.payload["work_news_basis"]
        if (
            type(basis) is not dict
            or set(basis)
            != {"version", "source", "locale", "concealed", "public_base_url", "parts", "keyboard"}
            | ({"reveal", "cleanup_id", "origin"} if basis.get("version") == 2 else {"reveal", "cleanup_id", "history", "teaser_version", "edit_target"} if basis.get("version") == 3 else set())
            | ({"navigation_origin"} if basis.get("version") == 1 and "navigation_origin" in basis else set())
            or type(basis["version"]) is not int
            or basis["version"] not in {1, 2, 3}
        ):
            raise ValueError()
        source = WorkNewsRead(**basis["source"])
        if type(basis["locale"]) is not str or basis["public_base_url"] is not None and type(basis["public_base_url"]) is not str or type(source.values) is not dict or type(source.work) is not dict:
            raise ValueError()
        if any(type(value) is not str or len(value) != 64 or any(char not in "0123456789abcdef" for char in value) for value in (source.signature, source.event_signature)):
            raise ValueError()
        version = item.payload["presentation_version"]
        reveal = basis.get("reveal")
        navigation = basis.get("navigation_origin")
        if "navigation_origin" in basis:
            from app.telegram_bots.work_navigation_origin import validate_origin

            validate_origin(navigation)
            if basis["concealed"] or item.dedupe_key != "notice:work-navigation:" + navigation["query_key"]:
                raise ValueError()
        if (
            type(item.payload["presentation_version"]) is not int
            or version < 0
            or basis["version"] < 3
            and version != basis["version"] - 1
            or basis["version"] == 3
            and version < 1
            or item.class_ != ("reply" if reveal or navigation else news_class(source.key))
            or type(source.protected) is not bool
            or type(basis["concealed"]) is not bool
            or basis["concealed"] != (source.protected and reveal is None and navigation is None)
        ):
            raise ValueError()
        historical = [part for part in item.parts if part["presentation_version"] < version]
        parts = [part for part in item.parts if part["presentation_version"] == version]
        if len(parts) + len(historical) != len(item.parts):
            raise ValueError()
        if basis["version"] == 3:
            archive = basis["history"]
            if type(archive) is not list or (_history is not None and archive) or (_history is None and len(archive) != version):
                raise ValueError()
            qualified = {} if _history is None else _history
            for index, previous in enumerate(archive):
                if type(previous) is not dict or set(previous) != {"basis", "parts", "class", "presentation_version"} or type(previous["presentation_version"]) is not int or previous["presentation_version"] != index:
                    raise ValueError()
                prefix = [part for part in historical if part["presentation_version"] < index]
                old = replace(
                    item,
                    class_=previous["class"],
                    parts=[*prefix, *previous["parts"]],
                    payload={**item.payload, "presentation_version": index, "work_news_basis": previous["basis"], "product_keyboard_basis": previous["basis"]["keyboard"]},
                )
                _prior, prior_source, _recipe = frozen_news(SimpleNamespace(link=captured.link, item=old, part=None), _history=qualified)
                retired = deepcopy(previous["parts"])
                for part in retired:
                    if part["state"] in {"pending", "sending"}:
                        part["state"] = "retired"
                actual = [part for part in historical if part["presentation_version"] == index]
                if not same_body(retired, actual) or prior_source.event_signature != source.event_signature or prior_source.work_id != source.work_id:
                    raise ValueError()
                qualified[index] = previous["basis"]
            if reveal is not None:
                if type(reveal) is not str or len(reveal) != 32 or any(char not in "0123456789abcdef" for char in reveal) or type(basis["teaser_version"]) is not int or not 0 <= basis["teaser_version"] < version:
                    raise ValueError()
                if not qualified[basis["teaser_version"]]["concealed"]:
                    raise ValueError()
            elif basis["teaser_version"] is not None or basis["edit_target"] is not None:
                raise ValueError()
        elif version:
            if type(basis["reveal"]) is not str or len(basis["reveal"]) != 32 or any(char not in "0123456789abcdef" for char in basis["reveal"]) or len(historical) != 1:
                raise ValueError()
            origin = basis["origin"]
            old = replace(item, class_=news_class(source.key), parts=historical, payload={**item.payload, "presentation_version": 0, "work_news_basis": origin, "product_keyboard_basis": origin["keyboard"]})
            _origin, previous, _recipe = frozen_news(SimpleNamespace(link=captured.link, item=old, part=historical[-1]), acknowledged=True)
            if not origin["concealed"] or previous.event_signature != source.event_signature or previous.work_id != source.work_id:
                raise ValueError()
        expected, keyboard = recipes(source, basis, item.id, captured.link)
        if not same_body(expected, basis["parts"]) or not same_body(keyboard, basis["keyboard"]) or not same_body(keyboard, item.payload["product_keyboard_basis"]) or len(parts) != len(expected):
            raise ValueError()
        if version:
            cleanup = basis["cleanup_id"]
            if cleanup is not None and (type(cleanup) is not str or len(cleanup) != 32 or any(char not in "0123456789abcdef" for char in cleanup)):
                raise ValueError()
            if basis["version"] == 2 and ((len(expected) == 1) != (cleanup is None)):
                raise ValueError()
            if basis["version"] == 3 and reveal is not None and ((basis["edit_target"] is None) == (cleanup is None)):
                raise ValueError()
        teaser_version = basis.get("teaser_version", 0)
        teasers = [part for part in historical if part["presentation_version"] == teaser_version]
        edit = version and (basis["version"] == 2 and len(expected) == 1 or basis.get("edit_target") is not None)
        if edit and (len(teasers) != 1 or teasers[0]["state"] != "sent" or basis["version"] == 3 and basis["edit_target"] != teasers[0]["part_id"]):
            raise ValueError()
        metadata = {
            "part_id",
            "ordinal",
            "presentation_version",
            "state",
            "message_ids",
            "sent_at",
            "dispatched_at",
            "plain_fallback",
            "url_button_fallback",
            "callback_group_id",
            "invalid_token_verification_pending",
            "target_message_id",
            "target_protect_content",
        }
        ids, messages, selected, unfinished = {part["part_id"] for part in historical}, set(), None, False
        for ordinal, (part, recipe) in enumerate(zip(parts, expected, strict=True)):
            identity = part["part_id"]
            if type(identity) is not str or len(identity) != 32 or any(char not in "0123456789abcdef" for char in identity) or identity in ids:
                raise ValueError()
            ids.add(identity)
            body = {key: value for key, value in part.items() if key not in metadata}
            if part["kind"] == "edit":
                if not edit or len(expected) != 1 or type(part.get("target_message_id")) is not int or part["target_message_id"] != teasers[0]["message_ids"][0] or part.get("target_protect_content") is not teasers[0]["protect_content"]:
                    raise ValueError()
                body["kind"] = "text"
            elif "target_message_id" in part or "target_protect_content" in part or edit:
                raise ValueError()
            if type(part["ordinal"]) is not int or part["ordinal"] != ordinal or type(part["presentation_version"]) is not int or part["presentation_version"] != version or not same_body(body, recipe):
                raise ValueError()
            if (
                part["state"] not in {"pending", "sending", "sent"}
                or any(type(part[key]) is not bool for key in ("plain_fallback", "url_button_fallback") if key in part)
                or part.get("callback_group_id")
                and not recipe.get("stored_keyboard")
            ):
                raise ValueError()
            receipts = part["message_ids"]
            if type(receipts) is not list:
                raise ValueError()
            if part["state"] == "sent":
                if part["kind"] == "edit" and not same_body(receipts, [part["target_message_id"]]):
                    raise ValueError()
                if unfinished or len(receipts) != 1 or type(receipts[0]) is not int or receipts[0] <= 0 or receipts[0] in messages or datetime.fromisoformat(part["sent_at"]).utcoffset() is None:
                    raise ValueError()
                messages.add(receipts[0])
            elif receipts or "sent_at" in part or acknowledged:
                raise ValueError()
            if captured.part is not None and same_body(part, captured.part):
                if unfinished or part["state"] != ("sent" if acknowledged else "pending" if pending else "sending"):
                    raise ValueError()
                selected = recipe
            unfinished |= part["state"] != "sent"
        if captured.part is not None and selected is None:
            raise ValueError()
        return basis, source, selected
    except (KeyError, TypeError, ValueError, IndexError):
        raise PermanentDeliveryError("invalid-work-news-source") from None


def require_teaser(callback, parent):
    version = parent.payload["presentation_version"] if parent.payload["work_news_basis"]["concealed"] else parent.payload["work_news_basis"].get("teaser_version", 0)
    historical = [part for part in parent.parts if part["presentation_version"] == version]
    if (
        callback is None
        or len(historical) != 1
        or historical[0]["state"] != "sent"
        or (callback.group_id, callback.message_id, callback.message_thread_id) != (historical[0].get("callback_group_id"), historical[0]["message_ids"][0], parent.message_thread_id)
        or type(callback.payload.get("presentation_version")) is not int
        or callback.payload["presentation_version"] != version
        or callback.payload.get("presentation_pending") is not False
    ):
        raise ProductSourceRetired()


class WorkNewsPlans:
    def __init__(self, *, reader, outbox, callbacks, projection=WorkNewsProjection):
        self.reader, self.outbox, self.callbacks = reader, outbox, callbacks
        self.projection = projection
        self.settings = SettingsRepository(outbox.sessions, outbox.config)
        if reader.links.session_factory.kw.get("bind") is not outbox.sessions.kw.get("bind") or callbacks.sessions.kw.get("bind") is not outbox.sessions.kw.get("bind"):
            raise ValueError("work-news-requires-shared-storage")

    async def materialize(self, link, identity):
        return await await_drained(self._materialize(link, identity))

    async def _materialize(self, link, identity):
        async with self.outbox.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            if row is None or any(getattr(row, field) is not None for field in ("claim_owner", "claim_id", "claimed_at")):
                raise ProductSourceRetired()
            item = _item(row)
        if item.parts:
            frozen_news(SimpleNamespace(link=link, item=item, part=None))
            return True
        if item.state != "pending" or item.payload.get("presentation_version") != 0:
            raise ProductSourceRetired()
        source = await self.reader.read(link, item)
        settings = await self.settings.get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        origin = NavigationOrigin()
        admission = await prepare_attention(self.reader, link)
        clock = await self.reader.prepare_clock(link, item)
        async with self.outbox._writer(link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
            if binding is None or any(getattr(binding, field) != getattr(link, field) for field in _SCOPE) or row is None or _item(row) != item:
                raise DeliveryClaimRetired()
            await admission(session=writer, projection=self.projection(self.reader, link, item, source, clock=clock))
            basis = {"version": 1, "source": asdict(source), "locale": settings.locale, "concealed": source.protected, "public_base_url": origin.base}
            parts, keyboard = recipes(source, basis, identity, link)
            row.parts = self.outbox._parts(parts, 0)
            row.payload = _safe_json({**row.payload, "work_news_basis": {**basis, "parts": parts, "keyboard": keyboard}, "product_keyboard_basis": keyboard})
            if source.key == "remind.ready_to_resume":
                row.payload = {**row.payload, "kind": "resume_reminder"}
            if keyboard:
                group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=row.message_thread_id, session=writer)
                row.parts = [*row.parts[:-1], {**row.parts[-1], "callback_group_id": group.group_id}]
                for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                    control.payload = {**control.payload, "presentation_pending": True}
            await writer.flush()
            await admission(session=writer, projection=self.projection(self.reader, link, _item(row), source, clock=clock))
            origin.check(basis)
            return True


class WorkNewsSources:
    def __init__(self, plans, *, navigation=None):
        from app.telegram_bots.work_news_recovery import WorkNewsRecovery

        self.reader, self.outbox, self.callbacks, self.settings = plans.reader, plans.outbox, plans.callbacks, plans.settings
        self.projection = plans.projection
        self.recovery = WorkNewsRecovery(plans)
        self.navigation = navigation

    async def read_part(self, captured):
        async def read():
            basis, source, part = frozen_news(captured)
            current = await self.reader.read(captured.link, captured.item)
            if not same_news_origin(source, current):
                raise ProductSourceRetired()
            settings = await self.settings.get(captured.link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            if not same_body(asdict(current), asdict(source)) or settings.locale != basis["locale"] or NavigationOrigin().base != basis["public_base_url"]:
                if "navigation_origin" in basis:
                    raise ProductSourceRetired()
                await self.recovery.refresh(captured)
                raise DeliveryClaimRetired()
            return part["html"], part["visible_text"]

        return await await_drained(read())

    async def authorize(self, captured, *, silent, now):
        async def read():
            basis, source, _part = frozen_news(captured)
            settings = await self.settings.get(captured.link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
            from app.telegram_bots.work_navigation_sources import qualify_navigation

            projection = await qualify_navigation(self.navigation, captured, basis, source, _SendProjection(self, captured, source, basis, settings, zone, now, silent), family="work_news_basis")
            try:
                await current_attention(self.reader, captured.link, projection=projection)
            except ProductSourceRetired:
                # The failed final fence has admitted no SDK call. Recheck
                # authority before distinguishing display drift from retirement.
                current = await self.reader.read(captured.link, captured.item)
                if same_news_origin(source, current) and (not same_body(asdict(current), asdict(source)) or NavigationOrigin().base != basis["public_base_url"]):
                    raise DeliveryRetry(timedelta(seconds=1)) from None
                raise

        return await await_drained(read())


class _SendProjection:
    def __init__(self, host, captured, source, basis, settings, zone, now, silent):
        self.projection = host.projection(host.reader, captured.link, captured.item, source, clock=NewsClock(settings, zone, host.reader.clock))
        self.link, self.item, self.source = captured.link, captured.item, source
        self.host, self.captured, self.basis, self.settings, self.zone, self.now, self.silent = host, captured, basis, settings, zone, now, silent
        self.group = captured.item.parts[-1].get("callback_group_id")
        self.origin = NavigationOrigin()

    def extend(self, query):
        result = self.projection.extend(query)
        self.send_column = len(result.column_descriptions)
        cleanup = aliased(TelegramOutboxRow)
        return (
            result.add_columns(TelegramPersonSettingsRow, TelegramCallbackRow, cleanup)
            .join(TelegramPersonSettingsRow, TelegramPersonSettingsRow.owner_user_id == TelegramBotLinkRow.owner_user_id)
            .outerjoin(TelegramCallbackRow, or_(TelegramCallbackRow.group_id == self.group if self.group else false(), TelegramCallbackRow.id == self.basis.get("reveal") if self.basis.get("reveal") else false()))
            .outerjoin(cleanup, cleanup.id == self.basis.get("cleanup_id") if self.basis.get("cleanup_id") else false())
        )

    def check(self, rows):
        self.projection.check(rows)
        self.check_delivery(rows)

    def check_with_permissions(self, rows, held):
        qualified = getattr(self.projection, "check_with_permissions", None)
        if qualified is not None:
            qualified(rows, held)
        else:
            self.projection.check(rows)
        self.check_delivery(rows)

    def check_delivery(self, rows):
        delivery = rows[0][self.projection.delivery_column]
        latest = self.host.outbox._sending(
            rows[0][0], delivery, link_id=self.link.id, part_id=self.captured.part["part_id"], claim_owner=self.captured.claim_owner, claim_id=self.captured.claim_id, token_revision=self.captured.token_revision
        )
        if latest != self.captured:
            raise DeliveryClaimRetired()
        self.origin.check(self.basis)
        actual = _snapshot(rows[0][self.send_column])
        if preference_skip(self.item, actual, self.now):
            raise ProductSourceRetired()
        if actual.locale != self.basis["locale"] or actual.timezone != self.settings.timezone:
            raise DeliveryRetry(timedelta(seconds=1))
        local = self.now.astimezone(self.zone)
        minute, start, end = local.hour * 60 + local.minute, actual.quiet_start_minute, actual.quiet_end_minute
        quiet = actual.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
        if self.source.key == "remind.ready_to_resume":
            now = self.host.outbox.clock()
            due = datetime.fromisoformat(self.source.values["due"])
            if now < due:
                raise DeliveryRetry(due - now)
            local = now.astimezone(self.zone)
            minute = local.hour * 60 + local.minute
            reminder_quiet = actual.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
            if not self.basis.get("reveal") and not self.basis.get("navigation_origin") and reminder_quiet and actual.summary_enabled:
                raise ProductSourceRetired("quiet-hours")
        recent = rows[0][0].last_update_at is not None and self.now - aware(rows[0][0].last_update_at) <= timedelta(seconds=self.host.outbox.config.outbox.recent_act_seconds)
        if bool(delivery.silent or self.captured.part.get("silent") or quiet and not recent) != bool(self.silent):
            raise DeliveryClaimRetired()
        controls = list({row[self.send_column + 1].id: row[self.send_column + 1] for row in rows if row[self.send_column + 1] is not None}.values())
        if self.basis.get("reveal"):
            reveal = next((control for control in controls if control.id == self.basis["reveal"]), None)
            require_show(reveal, delivery)
            require_teaser(reveal, delivery)
            if self.basis.get("cleanup_id"):
                cleanup = rows[0][self.send_column + 2]
                if not cleanup_ready(delivery, cleanup):
                    raise DeliveryRetry(timedelta(seconds=5))
                if cleanup.payload["target_message_id"] != reveal.message_id:
                    raise ProductSourceRetired()
            elif self.captured.part.get("target_message_id") != reveal.message_id:
                raise ProductSourceRetired()
        controls = [control for control in controls if control.group_id == self.group]
        try:
            _validate_rows(delivery, controls, allow_pending=True)
        except CallbackRefused:
            raise ProductSourceRetired() from None
        now = self.host.callbacks.clock()
        if any(
            control.consumed_at is not None or control.expires_at is not None and aware(control.expires_at) <= now or now >= aware(control.created_at) + timedelta(days=self.host.callbacks.config.callbacks.max_age_days)
            for control in controls
        ):
            raise ProductSourceRetired()


async def acknowledge_news(session, row):
    from app.telegram_bots.links import BotLink

    binding = await session.get(TelegramBotLinkRow, row.link_id)
    if binding is None:
        raise ProductSourceRetired()
    frozen_news(SimpleNamespace(link=BotLink.from_row(binding), item=_item(row), part=row.parts[-1]), acknowledged=True)
