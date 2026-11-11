"""Source proof and one final SQL boundary for saved Work request delivery."""

import asyncio
import json
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import JSON, and_, false, func, select

from app.telegram_bots.attention_authority import current_attention
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.notifications import aware, preference_skip
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, PermanentDeliveryError, ProductSourceRetired
from app.telegram_bots.settings import SettingsRepository, _snapshot
from app.telegram_bots.work_requests import WorkRequestRead, present_request, seen_basis
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.spaces.model import SpaceGrantRow, SpaceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.spaces.contract import Permission
from deerflow.utils.file_io import await_drained


def content(source):
    request_fields = (
        "id",
        "instance_id",
        "work_id",
        "purpose",
        "question",
        "reason",
        "expected_response",
        "choices",
        "sources",
        "recipient_id",
        "revision",
        "request_revision",
        "assignment_revision",
        "state",
        "basis_id",
        "basis_revision",
        "instance_name",
    )
    work_fields = ("id", "instance_id", "objective", "assignment_revision", "outcome", "outcome_id", "blocker", "status")
    return ({key: source.request.get(key) for key in request_fields}, {key: source.work.get(key) for key in work_fields}, source.responses, source.people)


def _references(value):
    if isinstance(value, dict):
        if value.get("kind") == "space_file":
            yield value["space_id"]
        for child in value.values():
            yield from _references(child)
    elif isinstance(value, (list, tuple)):
        for child in value:
            yield from _references(child)


def frozen(captured):
    """A class label or literal template cannot manufacture a Work source."""
    try:
        basis = captured.item.payload["work_request_basis"]
        if type(basis) is not dict or set(basis) != {"version", "source", "locale", "concealed", "parts", "keyboard"} or type(basis["version"]) is not int or basis["version"] != 1:
            raise ValueError()
        if (
            captured.item.class_ != "question"
            or type(basis["concealed"]) is not bool
            or type(basis["locale"]) is not str
            or type(captured.item.payload.get("presentation_version")) is not int
            or captured.item.payload["presentation_version"] != 0
        ):
            raise ValueError()
        data = basis["source"]
        source = WorkRequestRead(**{**data, "responses": tuple(data["responses"])})
        if source.request["instance_id"] != captured.link.agent_ref or captured.link.agent_kind != "instance" or source.request["work_id"] != source.work["id"] or source.request["recipient_id"] != captured.link.owner_user_id:
            raise ValueError()
        if type(source.protected) is not bool or json.dumps(source.seen, sort_keys=True) != json.dumps(seen_basis(source.work, source.request, source.responses), sort_keys=True):
            raise ValueError()
        presentation = present_request(source, locale=basis["locale"], concealed=basis["concealed"], outbox_id=captured.item.id)
        expected = []
        for index, part in enumerate(presentation.parts):
            recipe = {**part, "source_ref": {"key": "work.request.presentation", "values": {"request_id": source.request["id"], "part": index}}}
            if presentation.keyboard and index == len(presentation.parts) - 1:
                recipe["stored_keyboard"] = True
            expected.append(recipe)
        if basis["parts"] != expected or basis["keyboard"] != [list(line) for line in presentation.keyboard] or captured.item.payload.get("product_keyboard_basis") != basis["keyboard"]:
            raise ValueError()
        index = captured.part["source_ref"]["values"]["part"]
        if type(index) is not int or not 0 <= index < len(expected):
            raise ValueError()
        parts = captured.item.parts
        if len(parts) != len(expected) or parts[index] != captured.part or captured.part.get("state") != "sending":
            raise ValueError()
        identities = set()
        receipts = {"part_id", "presentation_version", "ordinal", "state", "message_ids", "callback_group_id", "sent_at", "dispatched_at", "plain_fallback", "invalid_token_verification_pending"}
        for ordinal, (part, recipe) in enumerate(zip(parts, expected, strict=True)):
            identity = part.get("part_id")
            if type(identity) is not str or len(identity) != 32 or identity in identities or any(ch not in "0123456789abcdef" for ch in identity):
                raise ValueError()
            identities.add(identity)
            if type(part.get("ordinal")) is not int or part["ordinal"] != ordinal or type(part.get("presentation_version")) is not int or part["presentation_version"] != 0:
                raise ValueError()
            if set(part) - set(recipe) - receipts or any(part.get(key) != value for key, value in recipe.items() if key != "protect_content"):
                raise ValueError()
            if source.protected and not part.get("protect_content") or ordinal < len(parts) - 1 and part.get("callback_group_id"):
                raise ValueError()
            if ordinal < index and (part["state"] != "sent" or not part.get("sent_at") or not part.get("message_ids") or any(type(identity) is not int or identity <= 0 for identity in part["message_ids"])):
                raise ValueError()
        recipe = expected[index]
        return basis, source, recipe
    except (KeyError, TypeError, ValueError, IndexError):
        raise PermanentDeliveryError("invalid-work-request-source") from None


class WorkRequestSources:
    def __init__(self, *, reader, outbox, callbacks):
        self.reader, self.outbox, self.callbacks = reader, outbox, callbacks
        self.settings = SettingsRepository(outbox.sessions, outbox.config)
        if reader.links.session_factory.kw.get("bind") is not outbox.sessions.kw.get("bind") or callbacks.sessions.kw.get("bind") is not outbox.sessions.kw.get("bind"):
            raise ValueError("work-source-requires-shared-storage")

    async def _prepare(self, captured):
        basis, source, part = frozen(captured)
        current = await self.reader.read_request(captured.link, source.request["id"])
        if content(current) != content(source) or current.request["state"] != "pending" or current.request["needs_routing"]:
            raise ProductSourceRetired()
        if not (current.request["can_respond"] if current.request["purpose"] == "information" else current.request["can_manage"]):
            raise ProductSourceRetired()
        if current.seen != source.seen:
            raise DeliveryRetry(timedelta(seconds=5))
        if current.protected != basis["concealed"]:
            # Privacy transitions require a durable replacement, never merely
            # a transport flag on the formerly visible message body.
            raise DeliveryRetry(timedelta(seconds=5))
        return basis, current, part

    async def read_part(self, captured):
        async def read():
            _basis, _current, part = await self._prepare(captured)
            return part["html"], part["visible_text"]

        try:
            return await await_drained(read())
        except (AccessOff, CeilingRefused, AgentDenied, AgentConflict):
            raise ProductSourceRetired() from None

    async def authorize(self, captured, *, silent, now):
        async def read():
            basis, source, _part = await self._prepare(captured)
            settings = await self.settings.get(captured.link.owner_user_id)
            if settings is None:
                raise DeliveryClaimRetired()
            zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
            projection = _RequestProjection(self, captured, basis, source, settings, zone, silent, now)
            held, _target = await current_attention(self.reader, captured.link, projection=projection)
            if "agents:write" not in held:
                raise ProductSourceRetired()

        try:
            await await_drained(read())
        except (AccessOff, CeilingRefused, AgentDenied, AgentConflict, CallbackRefused):
            raise ProductSourceRetired() from None


class _RequestProjection:
    """Synchronous projection of facts joined to the final account/grant read."""

    def __init__(self, host, captured, basis, source, settings, zone, silent, now):
        self.host, self.captured, self.basis, self.source = host, captured, basis, source
        self.settings, self.zone, self.silent, self.now = settings, zone, silent, now
        self.spaces = set(_references((source.request, source.work, source.responses)))

    def extend(self, query):
        bot, delivery, settings = TelegramBotLinkRow, TelegramOutboxRow, TelegramPersonSettingsRow
        request, work, space, grant, callback = HumanInputRequestRow, AgentWorkRow, SpaceRow, SpaceGrantRow, TelegramCallbackRow
        sqlite = self.host.outbox.sessions.kw["bind"].dialect.name == "sqlite"

        def aggregate(fields):
            args = []
            for key, column in fields.items():
                args.extend((key, func.json(column) if sqlite and isinstance(column.type, JSON) else column))
            return func.json_group_array(func.json_object(*args)) if sqlite else func.json_agg(func.json_build_object(*args))

        # Correlated compact arrays keep the complete outbox JSON out of the
        # Space x callback product. These facts still share the final statement.
        fields = {column.name: column for column in space.__table__.columns if column.name not in {"created_at", "updated_at"}}
        spaces = (
            select(aggregate({**fields, "permissions": grant.permissions}))
            .select_from(space)
            .outerjoin(grant, and_(grant.space_id == space.id, grant.principal_kind == "human", grant.subject_id == bot.owner_user_id))
            .where(space.id.in_(self.spaces) if self.spaces else false())
            .correlate(bot)
            .scalar_subquery()
        )
        callbacks = (
            select(aggregate({column.name: column for column in callback.__table__.columns}))
            .where(callback.group_id == self.captured.part.get("callback_group_id") if self.captured.part.get("callback_group_id") else false())
            .scalar_subquery()
        )
        return (
            query.add_columns(work, request, delivery, settings, spaces, callbacks)
            .join(work, and_(work.id == self.source.work["id"], work.instance_id == bot.agent_ref))
            .join(request, and_(request.id == self.source.request["id"], request.instance_id == bot.agent_ref, request.work_id == work.id))
            .join(delivery, and_(delivery.id == self.captured.item.id, delivery.link_id == bot.id))
            .join(settings, settings.owner_user_id == bot.owner_user_id)
        )

    @staticmethod
    def objects(value):
        values = json.loads(value) if isinstance(value, str) else value or []
        result = []
        for fields in values:
            for key in ("created_at", "expires_at", "consumed_at"):
                if isinstance(fields.get(key), str):
                    fields[key] = datetime.fromisoformat(fields[key])
            result.append(SimpleNamespace(**fields))
        return result

    def check(self, rows):
        current, target, permissions, policy = rows[0][:4]
        work, request, delivery, saved_settings = rows[0][8:12]
        latest = self.host.outbox._sending(current, delivery, link_id=current.id, part_id=self.captured.part["part_id"], claim_owner=self.captured.claim_owner, claim_id=self.captured.claim_id, token_revision=self.captured.token_revision)
        if latest != self.captured:
            raise DeliveryClaimRetired()
        if (request.revision, request.request_revision, request.state, request.recipient_id) != (
            self.source.request["revision"],
            self.source.request["request_revision"],
            "pending",
            current.owner_user_id,
        ):
            raise ProductSourceRetired()
        if work.revision != self.source.work["revision"]:
            raise DeliveryRetry(timedelta(seconds=5))
        if self.source.request["purpose"] != "information" and permissions & 6 != 6:
            raise ProductSourceRetired()
        protected = target.custody == "company" and policy == "protected"
        if protected != self.basis["concealed"] or protected and not self.captured.part.get("protect_content"):
            raise DeliveryRetry(timedelta(seconds=5))
        actual = _snapshot(saved_settings)
        if actual.locale != self.basis["locale"] or actual.timezone != self.settings.timezone:
            raise DeliveryRetry(timedelta(seconds=5))
        if preference_skip(latest.item, actual, self.now):
            raise ProductSourceRetired()
        local = self.now.astimezone(self.zone)
        minute, start, end = local.hour * 60 + local.minute, actual.quiet_start_minute, actual.quiet_end_minute
        quiet = actual.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
        recent = current.last_update_at is not None and self.now - aware(current.last_update_at) <= timedelta(seconds=self.host.outbox.config.outbox.recent_act_seconds)
        if bool(delivery.silent or self.captured.part.get("silent") or quiet and not recent) != bool(self.silent):
            raise DeliveryClaimRetired()
        seen = set()
        for space in self.objects(rows[0][12]):
            if space.status not in {"active", "archived"} or space.permissions is None or not self.host.reader.work.agents.files.registry._view(space, space.permissions).permissions & Permission.READ:
                raise ProductSourceRetired()
            seen.add(space.id)
        if seen != self.spaces:
            raise ProductSourceRetired()
        if self.captured.part.get("stored_keyboard"):
            callbacks = {row.id: row for row in self.objects(rows[0][13])}
            _validate_rows(delivery, list(callbacks.values()), part=self.captured.part)
            for callback in callbacks.values():
                if (
                    callback.link_id != current.id
                    or callback.binding_generation != current.binding_generation
                    or callback.chat_id != current.chat_id
                    or callback.message_thread_id != delivery.message_thread_id
                    or callback.payload.get("anchor_outbox_id") != delivery.id
                    or callback.payload.get("presentation_version") != delivery.payload["presentation_version"]
                    or callback.payload.get("presentation_pending") is not False
                ):
                    raise DeliveryClaimRetired()
                self.host.callbacks._usable(callback)
