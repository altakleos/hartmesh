"""Fixed callback refusals read current identity, never protected Work content."""

import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.assignment_controls import assignment_permissions, displayed_assignment
from app.telegram_bots.assignment_drafts import draft_scope
from app.telegram_bots.assignment_lineage import FreshDraft
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _row, _validate_rows
from app.telegram_bots.notifications import aware, preference_skip
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, Outbox, ProductSourceRetired
from app.telegram_bots.publication import fixed_source
from app.telegram_bots.render import render, render_plain
from app.telegram_bots.settings import _snapshot
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_details import WorkDetailProjection
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_start_controls import displayed_start
from app.telegram_bots.work_stop_controls import displayed_stop
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from app.telegram_bots.work_views import work_management_permissions, work_start_permissions
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.persistence.user.model import UserRow
from deerflow.utils.file_io import await_drained

_ASSIGNMENT = {"assign_start", "assign_save"}
_MANAGEMENT = {"work_change", "work_withdraw"}
_WORK = {"work_start", "stop_attempt"} | _MANAGEMENT


def displayed_control(link, parent, callback):
    if callback.kind in _ASSIGNMENT:
        return displayed_assignment(link, parent, callback)
    if callback.kind == "work_start":
        return displayed_start(link, parent, callback)
    from app.telegram_bots.work_detail_cards import full_detail
    from app.telegram_bots.work_status_control_origins import carrier_source

    if callback.kind == "stop_attempt":
        from app.telegram_bots.work_status_control_origins import status_record

        original, _work_id, _attempt_id = displayed_stop(link, parent, callback)
        source = carrier_source(original)
        return original, status_record(source) if source["key"].startswith("status.") else full_detail(source)["record"]
    original = acknowledged_view(link, parent, callback.payload.get("presentation_version"))
    record = full_detail(carrier_source(original))["record"]
    if callback.kind not in _MANAGEMENT or callback.target_kind != "work" or (callback.target_id, callback.payload.get("work_id")) != (record["id"], record["id"]) or original.payload.get("work_news_basis", {}).get("concealed"):
        raise CallbackRefused()
    return original, record


def denial_key(kind, reason):
    if kind == "stop_attempt":
        return "stop.not_yours" if reason == "manage" else "work.need_inspect" if reason == "inspect" else "denied.other"
    return {"work_change": "denied.change", "work_withdraw": "denied.withdraw"}.get(kind) or ("denied.start" if kind == "work_start" else "assign.no_access" if reason == "inspect" else "denied.other")


def permissions_for(kind, detail):
    if kind == "stop_attempt":
        return frozenset({"agents:write", "runs:cancel"})
    if kind == "work_start":
        return work_start_permissions(detail.record, can_reuse_conversation=detail.can_reuse_conversation)
    if kind in _MANAGEMENT:
        return work_management_permissions(detail.record)
    return assignment_permissions(start=kind == "assign_start")


def callback_facts(row):
    value = _row(row)
    return {key: thaw(getattr(value, key)) for key in ("id", "group_id", "link_id", "chat_id", "binding_generation", "message_id", "message_thread_id", "kind", "target_kind", "target_id", "payload")} | {
        "created_at": value.created_at.isoformat(),
        "expires_at": value.expires_at.isoformat() if value.expires_at else None,
    }


def recipe(intent):
    return {"kind": "text", "source_ref": {"key": intent["key"], "values": {}}, "reply_to_message_id": intent["callback"]["message_id"], "protect_content": intent["protected"]}


def validate_intent(intent):
    if (
        type(intent) is not dict
        or set(intent) != {"version", "callback", "signature", "query_id", "key", "protected", "reason", "required"}
        or type(intent["version"]) is not int
        or intent["version"] != 1
        or type(intent["callback"]) is not dict
        or type(intent["protected"]) is not bool
    ):
        raise ProductSourceRetired()
    kind = intent["callback"].get("kind")
    if (
        type(kind) is not str
        or kind not in _WORK | _ASSIGNMENT
        or type(intent["reason"]) is not str
        or intent["reason"] not in {"inspect", "capability", "manage"}
        or type(intent["query_id"]) is not str
        or not 1 <= len(intent["query_id"]) <= 256
    ):
        raise ProductSourceRetired()
    expected_key = denial_key(kind, intent["reason"])
    required = intent["required"]
    if intent["key"] != expected_key or type(required) is not list or any(type(value) is not str for value in required) or sorted(set(required)) != required:
        raise ProductSourceRetired()
    if intent["reason"] in {"inspect", "manage"}:
        if required or intent["reason"] == "manage" and kind not in _MANAGEMENT | {"stop_attempt"}:
            raise ProductSourceRetired()
    elif kind == "work_start":
        if set(required) not in ({"agents:write", "runs:create"}, {"agents:write", "runs:create", "threads:write"}):
            raise ProductSourceRetired()
    elif kind == "stop_attempt":
        if set(required) != {"agents:write", "runs:cancel"}:
            raise ProductSourceRetired()
    elif kind in _MANAGEMENT:
        if set(required) not in ({"agents:write"}, {"agents:write", "runs:cancel"}):
            raise ProductSourceRetired()
    elif set(required) != assignment_permissions(start=kind == "assign_start"):
        raise ProductSourceRetired()
    callback = intent["callback"]
    if set(callback) != {"id", "group_id", "link_id", "chat_id", "binding_generation", "message_id", "message_thread_id", "kind", "target_kind", "target_id", "payload", "created_at", "expires_at"}:
        raise ProductSourceRetired()
    if (
        any(type(callback[key]) is not str or not 1 <= len(callback[key]) <= 128 for key in ("id", "group_id", "link_id", "target_id"))
        or callback["target_kind"] != ("draft" if kind in _ASSIGNMENT else "attempt" if kind == "stop_attempt" else "work")
        or any(type(callback[key]) is not int or callback[key] <= 0 for key in ("chat_id", "binding_generation", "message_id"))
        or callback["message_thread_id"] is not None
        and (type(callback["message_thread_id"]) is not int or callback["message_thread_id"] <= 0)
        or type(callback["payload"]) is not dict
        or type(callback["payload"].get("anchor_outbox_id")) is not str
        or not callback["payload"]["anchor_outbox_id"]
        or type(callback["payload"].get("presentation_version")) is not int
        or callback["payload"]["presentation_version"] < 0
        or type(intent["signature"]) is not str
        or len(intent["signature"]) != 64
        or any(character not in "0123456789abcdef" for character in intent["signature"])
    ):
        raise ProductSourceRetired()
    try:
        for key in ("created_at", "expires_at"):
            value = callback[key]
            if key == "expires_at" and value is None:
                continue
            if type(value) is not str or datetime.fromisoformat(value).tzinfo is None:
                raise ValueError()
    except (TypeError, ValueError):
        raise ProductSourceRetired() from None
    return intent


def source_key(link, intent):
    return "work-callback-denial:" + digest([link.id, link.binding_generation, intent["callback"]["id"], intent["query_id"]])


class DenialProjection:
    def __init__(self, owner, link, intent, *, captured=None, metadata=None, preferences=None, detail=None, person=None):
        self.owner, self.link, self.intent = owner, link, validate_intent(intent)
        self.captured, self.metadata = captured, metadata
        self.preferences = preferences
        self.person = person
        self.detail = WorkDetailProjection(owner.chat.views.reader, link, detail.record, detail.request, reusable=detail.can_reuse_conversation, expected=detail) if detail is not None else None

    def extend(self, query):
        if self.detail is not None:
            query = self.detail.extend(query)
        self.column = len(query.column_descriptions)
        callback, parent, member, action, attempt, event, root, settings, supervisor = (
            aliased(TelegramCallbackRow),
            aliased(TelegramOutboxRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramActRow),
            aliased(AgentWorkAttemptRow),
            aliased(AgentWorkEventRow),
            aliased(TelegramActRow),
            aliased(TelegramPersonSettingsRow),
            aliased(UserRow),
        )
        query = (
            query.add_columns(callback, parent, member, action, attempt, event, root, settings, supervisor.email)
            .outerjoin(callback, callback.id == self.intent["callback"]["id"])
            .outerjoin(parent, parent.id == callback.payload["anchor_outbox_id"].as_string())
            .outerjoin(member, member.group_id == callback.group_id)
            .outerjoin(action, action.id == member.id)
            .outerjoin(attempt, attempt.activation_id == member.id)
            .outerjoin(event, event.operation_id == member.id)
            .outerjoin(root, and_(callback.kind.in_(["assign_start", "assign_save"]), root.id == callback.target_id))
            .outerjoin(settings, settings.owner_user_id == self.link.owner_user_id)
            .outerjoin(supervisor, supervisor.id == query.column_descriptions[1]["entity"].supervisor_id)
        )
        opening, gesture, draft = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), aliased(TelegramActRow)
        management_key = "notice:work-management:" + digest([self.link.id, self.link.binding_generation, self.intent["callback"]["id"], self.intent["query_id"]])
        self.management_column = len(query.column_descriptions)
        query = query.add_columns(
            select(opening.id).where(opening.payload["work_management_intent"]["origin"]["callback_id"].as_string() == member.id).exists(),
            select(gesture.id).where(gesture.kind == "work_management_gesture", gesture.payload["callback_id"].as_string() == member.id).exists(),
            select(draft.id).where(draft.id == digest([management_key, "draft"])[:32]).exists(),
        )
        native, work, requester = aliased(AgentWorkAttemptRow), aliased(AgentWorkRow), aliased(UserRow)
        self.stop_column = len(query.column_descriptions)
        query = (
            query.add_columns(native, work.instance_id, requester.email)
            .outerjoin(native, and_(callback.kind == "stop_attempt", native.id == callback.target_id))
            .outerjoin(work, work.id == native.work_id)
            .outerjoin(requester, requester.id == native.requester_id)
        )
        self.fresh = None
        if self.intent["callback"]["kind"] in _ASSIGNMENT:
            self.fresh = FreshDraft(self.intent["callback"]["target_id"], self.intent["callback"]["payload"]["anchor_outbox_id"])
            query = self.fresh.extend(query)
        self.delivery_column = len(query.column_descriptions)
        if self.captured is not None:
            delivery = aliased(TelegramOutboxRow)
            query = query.add_columns(delivery).join(delivery, delivery.id == self.captured.item.id)
        return query

    def check(self, rows):
        raise ProductSourceRetired()

    def original_control(self, row):
        return row.consumed_at is None or (self.captured is not None and row.closed_reason == "superseded" and aware(row.consumed_at) >= aware(self.captured.item.created_at))

    def check_with_permissions(self, rows, held):
        if self.detail is not None:
            self.detail.check_with_permissions(rows, held)
        callback, parent, _member, _action, _attempt, _event, root, settings, supervisor = rows[0][self.column : self.column + 9]
        if callback is None or parent is None or callback_facts(callback) != self.intent["callback"] or not self.original_control(callback):
            raise ProductSourceRetired()
        if (callback.link_id, callback.binding_generation, callback.chat_id) != (self.link.id, self.link.binding_generation, self.link.chat_id):
            raise ProductSourceRetired()
        if self.intent["reason"] == "inspect":
            if rows[0][2] & 2:
                raise ProductSourceRetired()
        elif self.intent["reason"] == "manage":
            if not rows[0][2] & 2 or rows[0][2] & 4:
                raise ProductSourceRetired()
        else:
            if not rows[0][2] & 2:
                raise ProductSourceRetired()
            if callback.kind in _WORK:
                if self.detail is None:
                    raise ProductSourceRetired()
                required = permissions_for(callback.kind, self.detail.source)
            else:
                required = assignment_permissions(start=callback.kind == "assign_start")
            if sorted(required) != self.intent["required"] or required.issubset(held):
                raise ProductSourceRetired()
        policy = "allowed" if rows[0][1].custody == "personal" or rows[0][3] is None else rows[0][3]
        if (policy == "protected") is not self.intent["protected"]:
            raise ProductSourceRetired()
        members = {row[self.column + 2].id: row[self.column + 2] for row in rows if row[self.column + 2] is not None}
        if not members or any(not self.original_control(member) or "work_action_id" in member.payload for member in members.values()) or any(any(row[self.column + index] is not None for index in (3, 4, 5)) for row in rows):
            raise ProductSourceRetired()
        if callback.kind in _MANAGEMENT and any(any(row[self.management_column : self.management_column + 3]) for row in rows):
            raise ProductSourceRetired()
        try:
            original, original_record = displayed_control(self.link, parent, callback)
            _validate_rows(original, list(members.values()))
            if anchor_signature(original) != self.intent["signature"]:
                raise ProductSourceRetired()
            if callback.kind == "stop_attempt":
                native, instance_id, person = rows[0][self.stop_column : self.stop_column + 3]
                shown = original_record.get("attempt") or {}
                if (
                    native is None
                    or instance_id != self.link.agent_ref
                    or (native.id, native.work_id, native.requester_id) != (callback.target_id, callback.payload.get("work_id"), shown.get("requester_id"))
                    or native.status not in {"starting", "running"}
                    or self.intent["reason"] == "manage"
                    and native.requester_id == self.link.owner_user_id
                ):
                    raise ProductSourceRetired()
                if self.person is not None and person != self.person:
                    raise DeliveryRetry(timedelta(seconds=1))
            if self.fresh is not None:
                if (
                    root is None
                    or any(getattr(root, key) != value for key, value in draft_scope(self.link).items())
                    or root.state != "draft"
                    or root.operation_ids
                    or root.continuation is not None
                    or root.stop_dispatch is not None
                    or root.cancel_requested_at is not None
                ):
                    raise ProductSourceRetired()
                self.fresh.check(rows, parent)
            if self.metadata is not None and (settings is None or (settings.locale, rows[0][1].name, supervisor) != self.metadata):
                raise DeliveryRetry(timedelta(seconds=1))
            if self.captured is not None:
                saved = rows[0][self.delivery_column]
                current = Outbox._sending(rows[0][0], saved, link_id=self.link.id, part_id=self.captured.part["part_id"], claim_owner=self.captured.claim_owner, claim_id=self.captured.claim_id, token_revision=self.captured.token_revision)
                if current != self.captured:
                    raise DeliveryClaimRetired()
                if self.preferences is not None:
                    timezone, zone, silent, now = self.preferences
                    actual = _snapshot(settings)
                    if actual.timezone != timezone or preference_skip(self.captured.item, actual, now):
                        raise DeliveryClaimRetired()
                    local = now.astimezone(zone)
                    minute, start, end = local.hour * 60 + local.minute, actual.quiet_start_minute, actual.quiet_end_minute
                    quiet = actual.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
                    recent = rows[0][0].last_update_at is not None and now - aware(rows[0][0].last_update_at) <= timedelta(seconds=self.owner.chat.host.config.outbox.recent_act_seconds)
                    if bool(saved.silent or self.captured.part.get("silent") or quiet and not recent) != bool(silent):
                        raise DeliveryClaimRetired()
            self.owner.chat.host.callbacks._unexpired(callback)
        except (CallbackRefused, KeyError, ValueError, TypeError):
            raise ProductSourceRetired() from None


class WorkCallbackDenials:
    intent_field = "work_callback_denial"
    parse_intent = staticmethod(validate_intent)
    source_id = staticmethod(source_key)
    projection = DenialProjection

    def __init__(self, chat):
        self.chat = chat

    async def offer(self, link, row, *, guard, query_id):
        async def perform():
            if row.kind not in _WORK | _ASSIGNMENT or type(query_id) is not str or not 1 <= len(query_id) <= 256:
                return False
            current = await prepare_attention(self.chat.views.reader, link, require_inspect=False)
            host = self.chat.host
            held, basis = await current()
            reason = "inspect" if not basis[0] & 2 else "manage" if row.kind in _MANAGEMENT | {"stop_attempt"} and not basis[0] & 4 else "capability"
            if row.kind == "stop_attempt" and basis[0] & 2 and not permissions_for(row.kind, None).issubset(held):
                reason = "capability"
            work_id = row.payload.get("work_id") if row.kind == "stop_attempt" else row.target_id
            detail = await self.chat.views.reader.detail(link, work_id) if row.kind in _WORK and (reason == "capability" or row.kind == "stop_attempt" and reason == "manage") else None
            required = permissions_for(row.kind, detail) if reason == "capability" else frozenset()
            if reason == "capability" and required.issubset(held):
                return False
            async with host.callbacks.writer(link, guard) as writer:
                group = await host.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                selected = await writer.get(TelegramCallbackRow, row.id, populate_existing=True)
                if _row(selected) != row:
                    raise ProductSourceRetired()
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                original, _source = displayed_control(link, parent, selected)
                _held, basis = await current(session=writer)
                key = denial_key(row.kind, reason)
                intent = dict(version=1, callback=callback_facts(selected), signature=anchor_signature(original), query_id=query_id, key=key, protected=basis[1] == "protected", reason=reason, required=sorted(required))
                projection = DenialProjection(self, link, intent, detail=detail)
                await current(session=writer, projection=projection)
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "notice:" + source_key(link, intent)).with_for_update())
                if existing is not None and not same_body(existing.payload.get("work_callback_denial"), intent):
                    # A replay never relabels the first committed reply owner.
                    raise CallbackRefused(None)
                identity = await host.outbox.plan_product(link, source_id=source_key(link, intent), parts=[recipe(intent)], message_thread_id=row.message_thread_id, session=writer)
                if existing is None:
                    delivery = await writer.get(TelegramOutboxRow, identity)
                    delivery.payload = {**delivery.payload, "work_callback_denial": intent}
                await current(session=writer, projection=projection)
                guard.current()
            return True

        return await await_drained(perform())

    def intent(self, captured):
        item = captured.item
        intent = self.parse_intent(item.payload.get(self.intent_field))
        expected = recipe(intent)
        basis = {"origins": [], "chat_id": captured.link.chat_id, "message_thread_id": intent["callback"]["message_thread_id"], "finish_route": False, "parts": [expected]}
        if (
            item.class_ != "reply"
            or set(item.payload) - {"product_basis", "presentation_version", self.intent_field, "notification_reserved_at"}
            or item.payload.get("presentation_version") != 0
            or type(item.payload.get("presentation_version")) is not int
            or len(item.parts) != 1
            or item.work_id is not None
            or item.instance_id is not None
            or item.request_id is not None
            or item.event_id is not None
            or item.dedupe_key != "notice:" + self.source_id(captured.link, intent)
            or item.message_thread_id != basis["message_thread_id"]
            or not same_body(item.payload.get("product_basis"), basis)
        ):
            raise ProductSourceRetired()
        if (
            any(captured.part.get(key) != value for key, value in expected.items())
            or any(captured.part.get(key) is not None for key in ("url_button", "stored_keyboard", "callback_group_id"))
            or captured.part.get("force_reply") is not None
            and captured.part.get("force_reply") is not False
        ):
            raise ProductSourceRetired()
        return intent

    async def prepare_person(self, captured, intent, values):
        return None

    async def read_part(self, captured, *, silent=None, now=None):
        async def read():
            intent = self.intent(captured)
            settings = await self.chat.host.settings.get(captured.link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
            source = await fixed_source(self.chat.host, captured.link, intent["key"])
            values = source["values"]
            metadata = (settings.locale, values["name"], values["supervisor"])
            kind, reason = intent["callback"]["kind"], intent["reason"]
            work_id = intent["callback"]["payload"].get("work_id") if kind == "stop_attempt" else intent["callback"]["target_id"]
            detail = await self.chat.views.reader.detail(captured.link, work_id) if kind in _WORK and (reason == "capability" or kind == "stop_attempt" and reason == "manage") else None
            person = None
            if kind == "stop_attempt" and reason == "manage":
                from app.telegram_bots.authority import get_local_provider

                attempt = detail.record.get("attempt") or {}
                if attempt.get("id") != intent["callback"]["target_id"]:
                    raise ProductSourceRetired()
                requester = await get_local_provider().get_user(attempt["requester_id"])
                if requester is None:
                    raise ProductSourceRetired()
                person = values["person"] = str(requester.email)
            if person is None:
                person = await self.prepare_person(captured, intent, values)
            current = await prepare_attention(self.chat.views.reader, captured.link, require_inspect=False)
            preferences = (settings.timezone, zone, silent, now) if now is not None else None
            projection = self.projection(self, captured.link, intent, captured=captured, metadata=metadata, preferences=preferences, detail=detail, person=person)
            prepare = getattr(projection, "prepare_source", None)
            if prepare is not None:
                await prepare()
            await current(projection=projection)
            return render(intent["key"], settings.locale, **values), render_plain(intent["key"], settings.locale, **values)

        return await await_drained(read())

    async def authorize(self, captured, *, silent, now, rendered):
        if await self.read_part(captured, silent=silent, now=now) != rendered:
            raise DeliveryRetry(timedelta(seconds=1))
