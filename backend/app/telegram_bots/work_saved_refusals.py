"""Fixed saved-Start refusals retain actual journals, SDK origins and receipts."""

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import and_, false, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.authority import get_local_provider
from app.telegram_bots.notifications import aware, preference_skip
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, Outbox, ProductSourceRetired, _item
from app.telegram_bots.render import render, render_plain
from app.telegram_bots.route_permissions import prepare_routes
from app.telegram_bots.settings import _snapshot as settings_snapshot
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_execution_readiness import conclusive_refusal, readiness_signature
from app.telegram_bots.work_execution_sources import act_facts, execution_origin_rows, qualify_execution_origin, require_execution_scope
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_start_conversations import context_plan
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.persistence.user.access import DisabledIdentityRow, RoleLimitRow
from deerflow.persistence.user.model import UserRow
from deerflow.utils.file_io import await_drained

_LIFECYCLE = {"agent.paused", "agent.retired", "access.agent_off"}


def primary_signature(primary):
    return digest([act_facts(primary), thaw(primary.result)]) if primary is not None else None


@dataclass(frozen=True)
class Metadata:
    key: str
    values: dict
    settings: object
    zone: object
    live_owner: object
    can_read: bool
    decision: object


class SavedStartProof:
    assignment = False

    def __init__(self, owner, link, item, root, effective, origin, callback, parent, primary):
        self.owner, self.link, self.item = owner, link, item
        self.root_id, self.effective_id = root.id, effective.id
        self.signature = readiness_signature(root, effective)
        self.origin, self.group_id, self.parent_id = origin, callback.group_id, parent.id
        self.primary_id = primary.id if primary is not None else None
        self.primary_signature = primary_signature(primary)
        self.metadata = self.captured = None
        self.silent = self.now = None

    @classmethod
    async def read(cls, owner, link, item):
        if cls is SavedStartProof and "assignment_intent" in item.payload:
            from app.telegram_bots.work_saved_assignment_refusals import SavedAssignmentProof

            return await SavedAssignmentProof.read(owner, link, item)
        intent = item.payload.get("work_execution_intent")
        if type(intent) is not dict or set(intent) != {"version", "act_id", "origin"} or type(intent["version"]) is not int or intent["version"] != 1:
            raise ProductSourceRetired()
        async with owner.host.outbox.sessions() as session:
            row = await session.get(TelegramActRow, intent["act_id"])
            if row is None:
                raise ProductSourceRetired()
            root = _snapshot(row)
            require_execution_scope(link, root)
            if root.kind != "activate":
                return None
            marker = context_plan(root) if (root.continuation or {}).get("activation_context") is not None else None
            child = await session.get(TelegramActRow, marker["child_id"]) if marker is not None else row
            if child is None:
                return None
            effective = _snapshot(child)
            if not conclusive_refusal(root, effective):
                return None
            callback, parent, controls, primary, _context = await execution_origin_rows(session, row)
            if callback is None or parent is None:
                raise ProductSourceRetired()
            origin = qualify_execution_origin(link, root, callback, parent, controls, activation_parent=primary)
            if not same_body(origin, intent["origin"]):
                raise ProductSourceRetired()
        return cls(owner, link, item, root, effective, origin, callback, parent, primary)

    def extend(self, query):
        self.column = len(query.column_descriptions)
        root, effective, canonical, parent, control, primary, instance, settings, supervisor, user, disabled, connection, grant, delivery, limit = (
            aliased(TelegramActRow),
            aliased(TelegramActRow),
            aliased(TelegramOutboxRow),
            aliased(TelegramOutboxRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramActRow),
            aliased(AgentInstanceRow),
            aliased(TelegramPersonSettingsRow),
            aliased(UserRow),
            aliased(UserRow),
            aliased(DisabledIdentityRow),
            aliased(ChannelConnectionRow),
            aliased(AgentInstanceGrantRow),
            aliased(TelegramOutboxRow),
            aliased(RoleLimitRow),
        )
        ids = {self.root_id, self.effective_id}
        return (
            query.add_columns(
                root,
                effective,
                canonical,
                parent,
                control,
                primary,
                instance,
                settings,
                supervisor.email,
                user,
                disabled,
                connection,
                grant.permissions,
                delivery,
                select(AgentWorkAttemptRow.id).where(AgentWorkAttemptRow.activation_id.in_(ids)).exists(),
                select(AgentWorkEventRow.id).where(AgentWorkEventRow.operation_id.in_(ids)).exists(),
                limit,
            )
            .join(root, root.id == self.root_id)
            .join(effective, effective.id == self.effective_id)
            .join(canonical, canonical.id == self.item.id)
            .join(parent, parent.id == self.parent_id)
            .outerjoin(control, control.group_id == self.group_id)
            .outerjoin(primary, primary.id == self.primary_id if self.primary_id is not None else false())
            .join(instance, instance.id == self.link.agent_ref)
            .join(settings, settings.owner_user_id == self.link.owner_user_id)
            .outerjoin(supervisor, supervisor.id == instance.supervisor_id)
            .join(user, user.id == self.link.owner_user_id)
            .outerjoin(disabled, or_(disabled.subject == user.id, disabled.subject == user.oauth_id))
            .outerjoin(limit, limit.subject == user.oauth_id)
            .join(connection, connection.id == self.link.connection_id)
            .outerjoin(grant, and_(grant.instance_id == instance.id, grant.user_id == self.link.owner_user_id))
            .outerjoin(delivery, delivery.id == self.captured.item.id if self.captured is not None else false())
        )

    def check_source(self, rows, root, effective, canonical, parent, primary, native, event):
        require_execution_scope(self.link, root)
        if not conclusive_refusal(root, effective) or readiness_signature(root, effective) != self.signature or native is not False or event is not False:
            raise ProductSourceRetired()
        if primary_signature(_snapshot(primary) if primary is not None else None) != self.primary_signature:
            raise ProductSourceRetired()
        if (
            (canonical.link_id, canonical.owner_user_id, canonical.binding_generation, canonical.chat_id, canonical.work_id, canonical.message_thread_id, canonical.instance_id)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, root.work_id, self.item.message_thread_id, self.link.agent_ref)
            or canonical.dedupe_key != "notice:work-execution:" + root.id
            or not same_body(canonical.payload.get("work_execution_intent"), self.item.payload.get("work_execution_intent"))
        ):
            raise ProductSourceRetired()
        controls = list({control.id: control for row in rows if (control := row[self.column + 4]) is not None}.values())
        selected = next((control for control in controls if control.id == self.origin["callback_id"]), None)
        if selected is None or not same_body(qualify_execution_origin(self.link, root, selected, parent, controls, activation_parent=_snapshot(primary) if primary is not None else None), self.origin):
            raise ProductSourceRetired()

    def check(self, rows):
        root, effective, canonical, parent, _control, primary, instance, settings, supervisor, user, _disabled, connection, permissions, delivery, native, event = rows[0][self.column : self.column + 16]
        root, effective = _snapshot(root), _snapshot(effective)
        self.check_source(rows, root, effective, canonical, parent, primary, native, event)
        if (connection.owner_user_id, connection.provider, connection.status, connection.external_account_id, connection.workspace_id, connection.bot_user_id) != (
            self.link.owner_user_id,
            "telegram",
            "connected",
            str(self.link.telegram_user_id),
            str(self.link.bot_id),
            str(self.link.bot_id),
        ):
            raise ProductSourceRetired()
        if self.metadata is not None:
            meta = self.metadata
            meta.live_owner.check_snapshot(user, [row[self.column + 10] for row in rows])
            meta.decision.check_snapshot(user, [row[self.column + 16] for row in rows])
            if instance.name != meta.values["name"] or supervisor != meta.values.get("supervisor", supervisor) or (settings.locale, settings.timezone) != (meta.settings.locale, meta.settings.timezone):
                raise DeliveryRetry(timedelta(seconds=1))
            if meta.key in {"denied.start", "assign.no_access"}:
                if instance.status != "active" or type(permissions) is not int or not permissions & 1 or permissions & 2:
                    raise ProductSourceRetired()
            elif meta.key == "denied.other":
                if instance.status != "active" or type(permissions) is not int or permissions & 3 != 3 or "agents:write" in meta.decision.held:
                    raise ProductSourceRetired()
            elif meta.key in {"agent.paused", "agent.retired"}:
                if instance.status != {"agent.paused": "suspended", "agent.retired": "archived"}[meta.key] or type(permissions) is not int or not permissions & 1:
                    raise ProductSourceRetired()
            elif meta.key == "access.agent_off" and meta.can_read and type(permissions) is int and permissions & 1:
                raise ProductSourceRetired()
        if self.captured is not None:
            current = Outbox._sending(rows[0][0], delivery, link_id=self.link.id, part_id=self.captured.part["part_id"], claim_owner=self.captured.claim_owner, claim_id=self.captured.claim_id, token_revision=self.captured.token_revision)
            if current != self.captured:
                raise DeliveryClaimRetired()
            if self.now is not None:
                actual = settings_snapshot(settings)
                if preference_skip(self.captured.item, actual, self.now):
                    raise ProductSourceRetired()
                local = self.now.astimezone(self.metadata.zone)
                minute, start, end = local.hour * 60 + local.minute, actual.quiet_start_minute, actual.quiet_end_minute
                quiet = actual.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
                recent = rows[0][0].last_update_at is not None and self.now - aware(rows[0][0].last_update_at) <= timedelta(seconds=self.owner.host.config.outbox.recent_act_seconds)
                if bool(delivery.silent or self.captured.part.get("silent") or quiet and not recent) != bool(self.silent):
                    raise DeliveryClaimRetired()


class WorkSavedRefusals:
    def __init__(self, chat):
        self.chat, self.host = chat, chat.host

    async def metadata(self, link, *, assignment=False):
        routes = await prepare_routes(self.host.links.session_factory)
        decision = await routes.evaluate(await routes.users.get_user_by_id(link.owner_user_id))
        key, identity = await self.host.authority.instance_lifecycle(link)
        if key not in _LIFECYCLE:
            view = await self.host.authority.instance_view(link)
            if view.status != "active":
                return None
            if not view.permissions & 2:
                key = "assign.no_access" if assignment else "denied.start"
            elif assignment and "agents:write" not in decision.held:
                key = "denied.other"
            else:
                return None
            identity = view
        settings = await self.host.settings.get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
        values = {"name": identity.name, "product": self.host.product_name}
        if key in {"denied.start", "assign.no_access", "denied.other", "access.agent_off"}:
            person = await get_local_provider().get_user(identity.supervisor.subject_id)
            values["supervisor"] = person.email if person is not None else None
        live = await self.host.authority.prepare_live_owner(link.owner_user_id)
        can_read = "agents:read" in decision.held
        if key in {"agent.paused", "agent.retired"} and not can_read:
            raise DeliveryRetry(timedelta(seconds=1))
        return Metadata(key, values, settings, zone, live, can_read, decision)

    @staticmethod
    def recipe(key):
        return {"kind": "text", "source_ref": {"key": key, "values": {}}}

    async def offer(self, link, item, *, guard):
        async def perform():
            proof = await SavedStartProof.read(self, link, item)
            if proof is None:
                return None
            metadata = await self.metadata(link, assignment=proof.assignment)
            if metadata is None:
                return None
            proof.metadata = metadata
            intent = {"version": 1, "act_id": proof.root_id, "canonical_id": item.id, "signature": proof.signature, "key": metadata.key}
            source = "work-saved-refusal:" + digest([link.id, link.binding_generation, proof.root_id, metadata.key])
            async with self.host.callbacks.writer(link, guard) as writer:
                await guard.check_projection(writer, proof)
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "notice:" + source).with_for_update())
                if existing is not None and not same_body(existing.payload.get("work_saved_refusal"), intent):
                    raise ProductSourceRetired()
                planner = self.host.outbox.plan_status_notice if metadata.key in _LIFECYCLE else self.host.outbox.plan_product
                identity = await planner(link, source_id=source, parts=[self.recipe(metadata.key)], message_thread_id=item.message_thread_id, session=writer)
                if existing is None:
                    row = await writer.get(TelegramOutboxRow, identity)
                    row.payload = {**row.payload, "work_saved_refusal": intent}
                await guard.check_projection(writer, proof)
            return identity

        return await await_drained(perform())

    async def read_part(self, captured, *, silent=None, now=None):
        async def read():
            item, link = captured.item, captured.link
            intent = item.payload.get("work_saved_refusal")
            if (
                type(intent) is not dict
                or set(intent) != {"version", "act_id", "canonical_id", "signature", "key"}
                or type(intent["version"]) is not int
                or intent["version"] != 1
                or type(intent["key"]) is not str
                or intent["key"] not in _LIFECYCLE | {"denied.start", "assign.no_access", "denied.other"}
            ):
                raise ProductSourceRetired()
            if (
                any(type(intent[key]) is not str or not 1 <= len(intent[key]) <= 128 for key in ("act_id", "canonical_id"))
                or type(intent["signature"]) is not str
                or len(intent["signature"]) != 64
                or any(value not in "0123456789abcdef" for value in intent["signature"])
            ):
                raise ProductSourceRetired()
            async with self.host.outbox.sessions() as session:
                canonical = await session.get(TelegramOutboxRow, intent["canonical_id"])
                if canonical is None:
                    raise ProductSourceRetired()
                canonical_item = _item(canonical)
            proof = await SavedStartProof.read(self, link, canonical_item)
            if proof is None or (proof.root_id, proof.signature) != (intent["act_id"], intent["signature"]):
                raise ProductSourceRetired()
            metadata = await self.metadata(link, assignment=proof.assignment)
            if metadata is None or metadata.key != intent["key"]:
                raise ProductSourceRetired()
            recipe = self.recipe(intent["key"])
            source = "notice:work-saved-refusal:" + digest([link.id, link.binding_generation, proof.root_id, metadata.key])
            scope = {name: getattr(link, name) for name in ("owner_user_id", "agent_kind", "agent_ref", "bot_id", "connection_id", "telegram_user_id", "binding_generation", "chat_id")}
            status = metadata.key in _LIFECYCLE
            allowed = {"presentation_version", "work_saved_refusal", "notification_reserved_at"} | ({"delivery_kind", "status_basis"} if status else {"product_basis"})
            if (
                set(item.payload) - allowed
                or type(item.payload.get("presentation_version")) is not int
                or item.payload["presentation_version"] != 0
                or any(getattr(item, key) is not None for key in ("instance_id", "work_id", "request_id", "event_id"))
            ):
                raise ProductSourceRetired()
            expected = (
                {"scope": scope, "origins": [], "message_thread_id": captured.item.message_thread_id, "finish_route": False, "parts": [recipe]}
                if status
                else {"origins": [], "chat_id": link.chat_id, "message_thread_id": captured.item.message_thread_id, "finish_route": False, "parts": [recipe]}
            )
            if (
                item.class_ != "reply"
                or item.dedupe_key != source
                or item.message_thread_id != proof.item.message_thread_id
                or not same_body(item.payload.get("status_basis" if status else "product_basis"), expected)
                or (item.payload.get("delivery_kind") == "status_notice") is not status
            ):
                raise ProductSourceRetired()
            if (
                len(item.parts) != 1
                or any(captured.part.get(key) != value for key, value in recipe.items())
                or any(captured.part.get(key) is not None for key in ("url_button", "callback_group_id", "stored_keyboard", "reply_to_message_id"))
                or captured.part.get("force_reply") is not None
                and captured.part.get("force_reply") is not False
            ):
                raise ProductSourceRetired()
            proof.metadata, proof.captured, proof.silent, proof.now = metadata, captured, silent, now
            policy = await self.host.authority.instance_send_policy(link, require_use=not status, expected_lifecycle=metadata.key if status else None, projection=proof)
            if policy in {"off", "restoring", "protected"} and not captured.part.get("protect_content"):
                raise DeliveryClaimRetired()
            return render(metadata.key, metadata.settings.locale, **metadata.values), render_plain(metadata.key, metadata.settings.locale, **metadata.values)

        return await await_drained(read())

    async def authorize(self, captured, *, silent, now, rendered):
        if await self.read_part(captured, silent=silent, now=now) != rendered:
            raise DeliveryRetry(timedelta(seconds=1))
