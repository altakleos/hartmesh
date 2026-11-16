"""Current admission feedback for a new Retry, preserving its uncertain owner."""

from datetime import timedelta

from sqlalchemy import false, select
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.notifications import aware, preference_skip
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, Outbox, ProductSourceRetired, _item
from app.telegram_bots.settings import _snapshot as settings_snapshot
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from app.telegram_bots.work_callback_denials import WorkCallbackDenials, callback_facts, recipe
from app.telegram_bots.work_execution_sources import act_facts
from app.telegram_bots.work_news_sources import WorkNewsRead
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_origins import acknowledged_view, anchor_signature
from deerflow.agent_instances.contract import AgentDenied
from deerflow.agent_instances.work import UNRESOLVED
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.persistence.user.model import UserRow
from deerflow.utils.file_io import await_drained

FAMILIES = {"management": "work_management_intent", "execution": "work_execution_intent", "assignment": "assignment_intent"}


def source_id(link, intent):
    return "work-retry-denial:" + digest([link.id, link.binding_generation, intent["callback"]["id"], intent["query_id"]])


def refusal(kind, reason):
    if kind == "activate":
        return "denied.start"
    if kind == "stop" and reason == "manage":
        return "stop.not_yours"
    if kind in {"edit", "cancel"}:
        return "denied.change" if kind == "edit" else "denied.withdraw"
    if kind == "delegate" and reason == "inspect":
        return "assign.no_access"
    return "work.need_inspect" if reason == "inspect" else "denied.other"


def validate_intent(value):
    if (
        type(value) is not dict
        or set(value) != {"version", "callback", "signature", "query_id", "key", "protected", "reason", "required", "act_id", "act_signature", "family", "kind"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or value["family"] not in FAMILIES
        or value["kind"] not in {"activate", "stop", "delegate", "edit", "cancel"}
        or value["reason"] not in {"inspect", "manage", "capability"}
        or value["key"] != refusal(value["kind"], value["reason"])
        or type(value["protected"]) is not bool
        or type(value["query_id"]) is not str
        or not 1 <= len(value["query_id"]) <= 256
        or type(value["required"]) is not list
        or value["required"] != sorted(set(value["required"]))
        or not set(value["required"]) <= {"agents:write", "runs:create", "runs:cancel", "threads:write"}
        or any(type(value[key]) is not str or len(value[key]) != size or any(char not in "0123456789abcdef" for char in value[key]) for key, size in (("act_id", 32), ("signature", 64), ("act_signature", 64)))
    ):
        raise ProductSourceRetired()
    callback = value["callback"]
    if (
        type(callback) is not dict
        or set(callback) != {"id", "group_id", "link_id", "chat_id", "binding_generation", "message_id", "message_thread_id", "kind", "target_kind", "target_id", "payload", "created_at", "expires_at"}
        or callback.get("kind") != "act_check"
        or callback.get("target_kind") != "act"
        or callback.get("target_id") != value["act_id"]
        or callback.get("payload", {}).get("act_id") != value["act_id"]
    ):
        raise ProductSourceRetired()
    if (
        any(type(callback[key]) is not str or len(callback[key]) != 32 or any(char not in "0123456789abcdef" for char in callback[key]) for key in ("id", "group_id", "link_id"))
        or any(type(callback[key]) is not int or callback[key] <= 0 for key in ("chat_id", "message_id", "binding_generation"))
        or callback["message_thread_id"] is not None
        and (type(callback["message_thread_id"]) is not int or callback["message_thread_id"] <= 0)
        or type(callback["payload"]) is not dict
        or type(callback["payload"].get("anchor_outbox_id")) is not str
        or type(callback["payload"].get("presentation_version")) is not int
    ):
        raise ProductSourceRetired()
    from datetime import datetime

    try:
        for key in ("created_at", "expires_at"):
            if callback[key] is not None and (type(callback[key]) is not str or datetime.fromisoformat(callback[key]).tzinfo is None):
                raise ValueError()
    except (TypeError, ValueError):
        raise ProductSourceRetired() from None
    return value


def action_requirements(source, act, owner_id, requester_id=None):
    required = {"agents:write"}
    manage = act.kind in {"edit", "cancel"}
    if act.kind == "activate":
        required.add("runs:create")
        if act.operation_ids or "activation_context" in (act.continuation or {}):
            required.add("threads:write")
    if act.kind == "stop" or act.kind in {"edit", "cancel"} and act.stop_dispatch is not None:
        required.add("runs:cancel")
    if act.kind == "stop":
        manage = (source["work"].get("original") or {}).get("requester_id") != owner_id
    if act.kind in {"edit", "cancel"} and act.state == "done":
        manage = requester_id != owner_id
    return sorted(required), manage


def denial_reason(permissions, held, required, manage):
    if not permissions & 2:
        return "inspect"
    if not set(required) <= held:
        return "capability"
    return "manage" if manage and not permissions & 4 else None


def remaining_scope(act, native):
    scope = act.stop_dispatch
    if (
        native is None
        or scope is None
        or native.work_id != act.work_id
        or (native.id, native.thread_id) != (scope["attempt_id"], scope["thread_id"])
        or scope["run_id"] is not None
        and native.run_id != scope["run_id"]
        or native.status not in UNRESOLVED
    ):
        raise ProductSourceRetired()
    return {key: getattr(native, key) for key in ("id", "work_id", "thread_id", "run_id", "status", "requester_id")}


async def requester_for(reader, act):
    if act.kind not in {"edit", "cancel"} or act.state != "done" or act.stop_dispatch is None:
        return None
    native = await reader.get(AgentWorkAttemptRow, act.stop_dispatch["attempt_id"])
    return remaining_scope(act, native)["requester_id"]


async def qualify_retry(owner, link, row, original, *, guard):
    """A read-only receipt settlement still needs its actual live Retry source."""
    host = owner.chat.host
    async with host.callbacks.writer(link, guard) as writer:
        group = await host.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
        selected = await writer.get(TelegramCallbackRow, row.id, populate_existing=True)
        parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
        actual = await writer.get(TelegramActRow, original.id, populate_existing=True)
        if callback_facts(selected) != callback_facts(row) or actual is None or digest(act_facts(_snapshot(actual))) != digest(act_facts(original)):
            raise ProductSourceRetired()
        shown = acknowledged_view(link, parent, group.presentation_version)
        _validate_rows(shown, group.rows)
        source = shown.payload["work_news_basis"]["source"]
        expected = source["work"].get("act")
        if (
            expected is None
            or expected["id"] != actual.id
            or row.target_id != actual.id
            or row.payload.get("act_id") != actual.id
            or source["work"].get("notice", {}).get("retry") is not True
            or any(not same_body(expected.get(key), getattr(actual, key)) for key in ("id", *_ACT_SCOPE, "body", "state", "attempts", "continuation", "stop_dispatch", "operation_ids"))
            or (actual.link_id, actual.owner_user_id, actual.bot_id, actual.binding_generation, actual.telegram_user_id, actual.chat_id, actual.instance_id)
            != (link.id, link.owner_user_id, link.bot_id, link.binding_generation, link.telegram_user_id, link.chat_id, link.agent_ref)
            or any(member.consumed_at is not None or "work_action_id" in member.payload for member in group.rows)
        ):
            raise ProductSourceRetired()
        effective = source["work"].get("effective") or source["work"].get("child")
        if effective is not None and effective["id"] != actual.id:
            child = await writer.get(TelegramActRow, effective["id"], populate_existing=True)
            if child is None or effective["id"] not in actual.operation_ids or any(not same_body(effective.get(key), getattr(child, key)) for key in ("id", *_ACT_SCOPE, "body", "state", "attempts", "continuation", "operation_ids")):
                raise ProductSourceRetired()
        members = [member.id for member in group.rows]
        for table, column in ((TelegramActRow, TelegramActRow.id), (AgentWorkAttemptRow, AgentWorkAttemptRow.activation_id), (AgentWorkEventRow, AgentWorkEventRow.operation_id)):
            if await writer.scalar(select(column).select_from(table).where(column.in_(members)).limit(1)) is not None:
                raise ProductSourceRetired()
        guard.current()


class RetryDenialProjection:
    def __init__(self, owner, link, intent, *, captured=None, metadata=None, preferences=None, **_unused):
        self.owner, self.link, self.intent = owner, link, validate_intent(intent)
        self.captured, self.metadata, self.preferences = captured, metadata, preferences
        self.person = _unused.get("person")
        self.companion = None

    async def prepare_source(self):
        host, intent = self.owner.chat.host, self.intent
        async with host.outbox.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, intent["callback"]["payload"].get("anchor_outbox_id"))
            if parent is None or FAMILIES[intent["family"]] not in parent.payload:
                raise ProductSourceRetired()
            original = acknowledged_view(self.link, parent, intent["callback"]["payload"].get("presentation_version"))
            if anchor_signature(original) != intent["signature"]:
                raise ProductSourceRetired()
            self.source = original.payload["work_news_basis"]["source"]
            act = await reader.get(TelegramActRow, intent["act_id"])
            if act is None or digest(act_facts(_snapshot(act))) != intent["act_signature"]:
                raise ProductSourceRetired()
            self.required, self.manage = action_requirements(self.source, _snapshot(act), self.link.owner_user_id, await requester_for(reader, _snapshot(act)))
            self.remaining_basis = None
            if act.kind in {"edit", "cancel"} and act.state == "done" and act.stop_dispatch is not None:
                self.remaining_basis = remaining_scope(_snapshot(act), await reader.get(AgentWorkAttemptRow, act.stop_dispatch["attempt_id"]))
        plans = getattr(self.owner.chat, {"management": "management", "execution": "executions", "assignment": "assignments"}[intent["family"]])
        clock = await plans.reader.prepare_clock(self.link, _item(parent))
        if intent["family"] == "management":
            from app.telegram_bots.work_management_sources import WorkManagementProjection

            self.companion = WorkManagementProjection(plans.reader, self.link, _item(parent), WorkNewsRead(**self.source), clock=clock, origin_only=True)
        elif intent["family"] == "assignment":
            from app.telegram_bots.assignment_sources import AssignmentProjection

            self.companion = AssignmentProjection(plans.reader, self.link, _item(parent), WorkNewsRead(**self.source), clock=clock, origin_only=True, recovery_only=True)
        else:
            from app.telegram_bots.work_execution_sources import WorkExecutionProjection

            self.companion = WorkExecutionProjection(plans.reader, self.link, _item(parent), WorkNewsRead(**self.source), clock=clock, origin_only=True, recovery_only=True)
            await self.companion.prepare_source()

    def extend(self, query):
        if self.companion is None:
            raise ProductSourceRetired()
        callback, parent, member, action, native, event, original, settings, supervisor, delivery = (
            aliased(TelegramCallbackRow),
            aliased(TelegramOutboxRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramActRow),
            aliased(AgentWorkAttemptRow),
            aliased(AgentWorkEventRow),
            aliased(TelegramActRow),
            aliased(TelegramPersonSettingsRow),
            aliased(UserRow),
            aliased(TelegramOutboxRow),
        )
        self.column = len(query.column_descriptions)
        result = (
            query.add_columns(callback, parent, member, action, native, event, original, settings, supervisor.email, delivery)
            .outerjoin(callback, callback.id == self.intent["callback"]["id"])
            .outerjoin(parent, parent.id == callback.payload["anchor_outbox_id"].as_string())
            .outerjoin(member, member.group_id == callback.group_id)
            .outerjoin(action, action.id == member.id)
            .outerjoin(native, native.activation_id == member.id)
            .outerjoin(event, event.operation_id == member.id)
            .outerjoin(original, original.id == self.intent["act_id"])
            .outerjoin(settings, settings.owner_user_id == self.link.owner_user_id)
            .outerjoin(supervisor, supervisor.id == query.column_descriptions[1]["entity"].supervisor_id)
            .outerjoin(delivery, delivery.id == self.captured.item.id if self.captured is not None else false())
        )
        remaining = aliased(AgentWorkAttemptRow)
        self.remaining_column = len(result.column_descriptions)
        result = result.add_columns(remaining).outerjoin(remaining, remaining.id == original.stop_dispatch["attempt_id"].as_string())
        result = self.companion.extend(result)
        self.person_column = None
        if self.intent["family"] == "execution" and self.intent["kind"] == "stop":
            requester = aliased(UserRow)
            native = result.column_descriptions[self.companion.delivery_column + 5]["entity"]
            self.person_column = len(result.column_descriptions)
            result = result.add_columns(requester.email).outerjoin(requester, requester.id == native.requester_id)
        return result

    def check_with_permissions(self, rows, held):
        callback, parent, _member, _action, _native, _event, original, settings, supervisor, delivery = rows[0][self.column : self.column + 10]
        if callback is None or parent is None or original is None or callback_facts(callback) != self.intent["callback"] or callback.consumed_at is not None:
            raise ProductSourceRetired()
        if digest(act_facts(_snapshot(original))) != self.intent["act_signature"] or original.kind != self.intent["kind"]:
            raise ProductSourceRetired()
        if (callback.link_id, callback.binding_generation, callback.chat_id) != (self.link.id, self.link.binding_generation, self.link.chat_id) or parent.payload.get("presentation_version") != callback.payload.get("presentation_version"):
            raise ProductSourceRetired()
        members = {row[self.column + 2].id: row[self.column + 2] for row in rows if row[self.column + 2] is not None}
        if (
            not members
            or any(member.consumed_at is not None or "work_action_id" in member.payload or member.id == original.id for member in members.values())
            or any(any(row[self.column + index] is not None for index in (3, 4, 5)) for row in rows)
        ):
            raise ProductSourceRetired()
        shown = acknowledged_view(self.link, parent, callback.payload.get("presentation_version"))
        _validate_rows(shown, list(members.values()))
        if anchor_signature(shown) != self.intent["signature"] or self.source["work"].get("notice", {}).get("retry") is not True or self.source["work"]["act"]["id"] != original.id:
            raise ProductSourceRetired()
        if self.intent["family"] == "management":
            self.companion.check_ownership(rows)
            event = rows[0][self.companion.column + 2]
            if original.state != "done" and event is not None:
                raise ProductSourceRetired()
            if original.state == "done":
                from app.telegram_bots.work_management_consent import management_receipt

                management_receipt(_snapshot(original), event)
        else:
            self.companion.check_with_permissions(rows, held)
            if self.intent["family"] == "execution" and original.state != "done":
                native, event = rows[0][self.companion.delivery_column + 5], rows[0][self.companion.delivery_column + 7]
                if original.kind == "stop" and event is not None:
                    raise ProductSourceRetired()
                if original.kind == "activate":
                    effective = self.source["work"]["effective"]
                    if native is not None and (native.run_id is not None or native.status not in UNRESOLVED):
                        raise ProductSourceRetired()
                    if event is not None and (
                        native is None
                        or (event.instance_id, event.work_id, event.operation_id, event.actor_kind, event.actor_id, event.action) != (self.link.agent_ref, original.work_id, effective["id"], "human", self.link.owner_user_id, "activate")
                        or not same_body(event.request, effective["body"])
                    ):
                        raise ProductSourceRetired()
                    if native is not None and event is None:
                        raise ProductSourceRetired()
        if self.person is not None and (self.person_column is None or rows[0][self.person_column] != self.person):
            raise DeliveryRetry(timedelta(seconds=1))
        permissions = rows[0][2]
        remaining = rows[0][self.remaining_column]
        if self.remaining_basis is not None and remaining_scope(_snapshot(original), remaining) != self.remaining_basis:
            raise ProductSourceRetired()
        required, manage = action_requirements(self.source, _snapshot(original), self.link.owner_user_id, remaining.requester_id if remaining is not None else None)
        if required != self.required or manage != self.manage:
            raise ProductSourceRetired()
        reason = denial_reason(permissions, held, required, manage)
        if reason != self.intent["reason"] or self.required != self.intent["required"]:
            raise ProductSourceRetired()
        policy = "allowed" if rows[0][1].custody == "personal" or rows[0][3] is None else rows[0][3]
        if (policy == "protected") is not self.intent["protected"]:
            raise ProductSourceRetired()
        self.owner.chat.host.callbacks._unexpired(callback)
        if self.metadata is not None and (settings is None or (settings.locale, rows[0][1].name, supervisor) != self.metadata):
            raise DeliveryRetry(timedelta(seconds=1))
        if self.captured is not None:
            current = Outbox._sending(rows[0][0], delivery, link_id=self.link.id, part_id=self.captured.part["part_id"], claim_owner=self.captured.claim_owner, claim_id=self.captured.claim_id, token_revision=self.captured.token_revision)
            if current != self.captured:
                raise DeliveryClaimRetired()
            if self.preferences is not None:
                timezone, zone, silent, now = self.preferences
                actual = settings_snapshot(settings)
                if actual.timezone != timezone or preference_skip(self.captured.item, actual, now):
                    raise DeliveryClaimRetired()
                local = now.astimezone(zone)
                minute, start, end = local.hour * 60 + local.minute, actual.quiet_start_minute, actual.quiet_end_minute
                quiet = actual.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
                recent = rows[0][0].last_update_at is not None and now - aware(rows[0][0].last_update_at) <= timedelta(seconds=self.owner.chat.host.config.outbox.recent_act_seconds)
                if bool(delivery.silent or self.captured.part.get("silent") or quiet and not recent) != bool(silent):
                    raise DeliveryClaimRetired()


class WorkRetryDenials(WorkCallbackDenials):
    intent_field = "work_retry_denial"
    parse_intent = staticmethod(validate_intent)
    source_id = staticmethod(source_id)
    projection = RetryDenialProjection

    async def prepare_person(self, captured, intent, values):
        if intent["kind"] != "stop" or intent["reason"] != "manage":
            return None
        from app.telegram_bots.authority import get_local_provider

        async with self.chat.host.outbox.sessions() as reader:
            act = await reader.get(TelegramActRow, intent["act_id"])
            native = await reader.get(AgentWorkAttemptRow, act.body["attempt_id"]) if act is not None else None
            if native is None or native.work_id != act.work_id:
                raise ProductSourceRetired()
            requester_id = native.requester_id
        person = await get_local_provider().get_user(requester_id)
        if person is None:
            raise ProductSourceRetired()
        values["person"] = str(person.email)
        return values["person"]

    async def offer(self, link, row, *, guard, query_id):
        async def perform():
            if row.kind != "act_check" or type(query_id) is not str or not 1 <= len(query_id) <= 256:
                return False
            host = self.chat.host
            async with host.outbox.sessions() as reader:
                parent = await reader.get(TelegramOutboxRow, row.payload.get("anchor_outbox_id"))
                original = await reader.get(TelegramActRow, row.target_id)
                if parent is None or original is None:
                    return False
                families = [family for family, field in FAMILIES.items() if field in parent.payload]
                if len(families) != 1:
                    raise ProductSourceRetired()
                shown = acknowledged_view(link, parent, row.payload.get("presentation_version"))
                source = shown.payload["work_news_basis"]["source"]
                original = _snapshot(original)
                required, manage = action_requirements(source, original, link.owner_user_id, await requester_for(reader, original))
            try:
                current = await prepare_attention(self.chat.views.reader, link, require_inspect=False)
                held, basis = await current()
            except (AgentDenied, CeilingRefused, AccessOff):
                key, _identity = await host.authority.instance_lifecycle(link)
                notice = getattr(host, "notice_instance_unavailable", None)
                if key not in {"agent.paused", "agent.retired", "access.agent_off"} or notice is None:
                    raise
                # This is a new qualified Retry gesture. The existing minimal
                # status owner repeats current lifecycle/access at the SDK fence;
                # the uncertain original keeps its operation and Native owner.
                await qualify_retry(self, link, row, original, guard=guard)
                try:
                    receipt = await host.acts.receipt_for(original, allow_inactive=True)
                except (AgentDenied, CeilingRefused, AccessOff):
                    receipt = None
                if receipt is not None and original.state != "done":
                    await qualify_retry(self, link, row, original, guard=guard)
                    await host.acts.reconcile_receipt(original.id, allow_inactive=True)
                await notice(link)
                return True
            reason = denial_reason(basis[0], held, required, manage)
            if reason is None:
                return False
            await qualify_retry(self, link, row, original, guard=guard)
            # A known committed result takes precedence over new-admission
            # restrictions. Unreadable receipts never authorize a retry.
            try:
                receipt = await host.acts.receipt_for(original)
            except (AgentDenied, CeilingRefused, AccessOff):
                receipt = None
            if receipt is not None and original.state != "done":
                await qualify_retry(self, link, row, original, guard=guard)
                await host.acts.reconcile_receipt(original.id)
                plans = getattr(self.chat, {"management": "management", "execution": "executions", "assignment": "assignments"}[families[0]])
                await plans.observe(link, source["work"].get("draft_id") if families[0] == "assignment" else parent.id if families[0] == "management" else original.id, guard=guard)
                return True
            intent = dict(
                version=1,
                callback=callback_facts(row),
                signature=anchor_signature(shown),
                query_id=query_id,
                key=refusal(original.kind, reason),
                protected=basis[1] == "protected",
                reason=reason,
                required=required,
                act_id=original.id,
                act_signature=digest(act_facts(original)),
                family=families[0],
                kind=original.kind,
            )
            proof = self.projection(self, link, intent)
            await proof.prepare_source()
            async with host.callbacks.writer(link, guard) as writer:
                await host.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                await current(session=writer, projection=proof)
                key = self.source_id(link, intent)
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "notice:" + key))
                if existing is not None and not same_body(existing.payload.get(self.intent_field), intent):
                    raise CallbackRefused(None)
                identity = await host.outbox.plan_product(link, source_id=key, parts=[recipe(intent)], message_thread_id=row.message_thread_id, session=writer)
                if existing is None:
                    delivery = await writer.get(TelegramOutboxRow, identity)
                    delivery.payload = {**delivery.payload, self.intent_field: intent}
                await current(session=writer, projection=proof)
            return True

        return await await_drained(perform())
