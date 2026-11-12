"""Dormant, source-owned digest navigation and whole-batch SDK admission."""

import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import false, select

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.digests import _members, validate
from app.telegram_bots.notifications import aware, preference_skip
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryRetry, PermanentDeliveryError, ProductSourceRetired, _item, _safe_json
from app.telegram_bots.settings import SettingsRepository, _snapshot
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_projection import DigestProjection
from app.telegram_bots.work_digest_text import DigestEntry, present_digest
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def member_proof(member):
    source = member.source
    body = {"value": asdict(source.value), "routing": source.routing} if hasattr(source, "value") else asdict(source)
    origin = getattr(source, "event_signature", None) or member.item.payload.get("work_event_source")
    return {"id": member.item.id, "origin": digest([origin, member.item.payload.get("work_result_identity")]), "source": digest([body, member.item.silent, [part.get("silent", False) for part in member.item.parts]])}


def recipes(basis, identity):
    view = present_digest([DigestEntry(**entry) for entry in basis["entries"]], locale=basis["locale"], protected=basis["protected"], concealed=basis["concealed"], outbox_id=identity)
    parts = [{**part, "stored_keyboard": True, "source_ref": {"key": "work.digest.presentation", "values": {"outbox_id": identity}}} for part in view.parts]
    return parts, [list(line) for line in view.keyboard]


def frozen_digest(captured, *, pending=False, acknowledged=False):
    try:
        item = captured.item
        identities, kinds, _ = validate(item)
        basis = item.payload["work_digest_basis"]
        if (
            type(basis) is not dict
            or set(basis) != {"version", "locale", "protected", "concealed", "entries", "members", "parts", "keyboard"}
            or type(basis["version"]) is not int
            or basis["version"] != 1
            or type(basis["locale"]) is not str
            or type(basis["protected"]) is not bool
            or basis["concealed"] is not basis["protected"]
            or item.payload["presentation_version"] != 0
            or type(item.payload["presentation_version"]) is not int
            or type(basis["members"]) is not list
            or len(basis["members"]) != len(identities)
            or type(basis["entries"]) is not list
            or len(basis["entries"]) != len(identities)
        ):
            raise ValueError()
        if [(entry["outbox_id"], entry["class_"]) for entry in basis["entries"]] != list(zip(identities, kinds, strict=True)):
            raise ValueError()
        for identity, proof in zip(identities, basis["members"], strict=True):
            if (
                type(proof) is not dict
                or set(proof) != {"id", "origin", "source"}
                or proof["id"] != identity
                or any(type(proof[key]) is not str or len(proof[key]) != 64 or any(char not in "0123456789abcdef" for char in proof[key]) for key in ("origin", "source"))
            ):
                raise ValueError()
        parts, keyboard = recipes(basis, item.id)
        if not same_body(parts, basis["parts"]) or not same_body(keyboard, basis["keyboard"]) or not same_body(keyboard, item.payload["product_keyboard_basis"]) or len(item.parts) != 1:
            raise ValueError()
        part = item.parts[0]
        metadata = {"part_id", "ordinal", "presentation_version", "state", "message_ids", "sent_at", "dispatched_at", "plain_fallback", "invalid_token_verification_pending", "callback_group_id"}
        if (
            not same_body({key: value for key, value in part.items() if key not in metadata}, parts[0])
            or any(type(part[key]) is not int or part[key] != 0 for key in ("ordinal", "presentation_version"))
            or any(type(part.get(key)) is not str or len(part[key]) != 32 or any(char not in "0123456789abcdef" for char in part[key]) for key in ("part_id", "callback_group_id"))
            or part["state"] != ("sent" if acknowledged else "pending" if pending else "sending")
            or captured.part is not None
            and not same_body(captured.part, part)
            or type(part["message_ids"]) is not list
            or any(type(part[key]) is not bool for key in ("plain_fallback", "invalid_token_verification_pending") if key in part)
        ):
            raise ValueError()
        if acknowledged:
            if len(part["message_ids"]) != 1 or type(part["message_ids"][0]) is not int or part["message_ids"][0] <= 0 or datetime.fromisoformat(part["sent_at"]).utcoffset() is None:
                raise ValueError()
        elif part["message_ids"] or "sent_at" in part:
            raise ValueError()
        if "dispatched_at" in part and datetime.fromisoformat(part["dispatched_at"]).utcoffset() is None:
            raise ValueError()
        return basis, parts[0]
    except (KeyError, TypeError, ValueError, IndexError):
        raise PermanentDeliveryError("invalid-digest-presentation") from None


class DigestPlans:
    def __init__(self, *, outbox, callbacks, members):
        self.outbox, self.callbacks, self.members, self.reader = outbox, callbacks, members, members.requests
        self.settings = SettingsRepository(outbox.sessions, outbox.config)
        if any(sessions.kw["bind"] is not outbox.sessions.kw["bind"] for sessions in (callbacks.sessions, self.reader.links.session_factory)):
            raise ValueError("digest-requires-shared-storage")

    async def load(self, link, identity, *, captured=None):
        async with self.outbox.sessions() as reader:
            row = await reader.get(TelegramOutboxRow, identity)
            if row is None or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.instance_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref):
                raise ProductSourceRetired()
            rows = await _members(reader, row)
            claim = claim_of(row)
            if captured is None:
                if row.state != "pending" or claim != (None, None, None):
                    raise ProductSourceRetired()
            elif _item(row) != captured.item or claim[:2] != (captured.claim_owner, captured.claim_id):
                raise DeliveryClaimRetired()
            if any(member.state != row.state or claim_of(member) != claim for member in rows):
                raise DeliveryClaimRetired()
            item, items = _item(row), [_item(member) for member in rows]
        settings = await self.settings.get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        members = [await self.members.read(link, member, locale=settings.locale, claim=claim) for member in items]
        silent = all(member.item.silent or all(part.get("silent") is True for part in member.item.parts) or member.item.class_ in {"summary", "reminder"} for member in members)
        if type(item.silent) is not bool or item.silent != silent:
            raise ProductSourceRetired()
        return item, settings, members, claim

    async def materialize(self, link, identity):
        return await await_drained(self._materialize(link, identity))

    async def _materialize(self, link, identity):
        item, settings, members, claim = await self.load(link, identity)
        if item.parts:
            frozen_digest(SimpleNamespace(link=link, item=item, part=None), pending=True)
            return True
        basis = {
            "version": 1,
            "locale": settings.locale,
            "protected": any(member.protected for member in members),
            "concealed": any(member.protected for member in members),
            "entries": [asdict(member.entry) for member in members],
            "members": [member_proof(member) for member in members],
        }
        parts, keyboard = recipes(basis, identity)
        basis.update(parts=parts, keyboard=keyboard)
        admission = await prepare_attention(self.reader, link)
        zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
        async with self.outbox._writer(link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
            if not self.outbox._binding(binding, link) or row is None or _item(row) != item or claim_of(row) != claim:
                raise DeliveryClaimRetired()
            await admission(session=writer, projection=_BatchProjection(self, link, item, members, settings, claim, zone=zone))
            row.parts = self.outbox._parts(parts, 0)
            row.payload = _safe_json({**row.payload, "work_digest_basis": basis, "product_keyboard_basis": keyboard})
            group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
            row.parts = [{**row.parts[0], "callback_group_id": group.group_id}]
            for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                control.payload = {**control.payload, "presentation_pending": True}
            await writer.flush()
            await admission(session=writer, projection=_BatchProjection(self, link, _item(row), members, settings, claim, zone=zone))
            frozen_digest(SimpleNamespace(link=link, item=_item(row), part=None), pending=True)
            return True


class DigestSources:
    def __init__(self, plans):
        self.plans = plans

    async def prepare(self, captured):
        basis, part = frozen_digest(captured)
        item, settings, members, claim = await self.plans.load(captured.link, captured.item.id, captured=captured)
        if (
            basis["locale"] != settings.locale
            or not same_body(basis["members"], [member_proof(member) for member in members])
            or not same_body(basis["entries"], [asdict(member.entry) for member in members])
            or basis["protected"] != any(member.protected for member in members)
        ):
            # The whole pre-ACK group is explicitly retired by the sender;
            # membership is never silently replaced during a retry.
            raise ProductSourceRetired()
        return item, settings, members, claim, part

    async def read_part(self, captured):
        async def read():
            *_, part = await self.prepare(captured)
            return part["html"], part["visible_text"]

        return await await_drained(read())

    async def authorize(self, captured, *, silent, now):
        async def read():
            item, settings, members, claim, _ = await self.prepare(captured)
            zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
            admission = await prepare_attention(self.plans.reader, captured.link)
            await admission(projection=_BatchProjection(self.plans, captured.link, item, members, settings, claim, captured=captured, silent=silent, now=now, zone=zone))

        return await await_drained(read())


class _BatchProjection(DigestProjection):
    def __init__(self, host, link, item, members, settings, claim, *, captured=None, silent=None, now=None, zone=None):
        super().__init__(link, [member.projection for member in members])
        self.host, self.item, self.settings, self.claim = host, item, settings, claim
        self.source_members = members
        self.captured, self.silent, self.now, self.zone = captured, silent, now, zone

    def extend(self, query):
        query = super().extend(query)
        self.column = len(query.column_descriptions)
        group = self.item.parts[0].get("callback_group_id") if self.item.parts else None
        return (
            query.add_columns(TelegramOutboxRow, TelegramPersonSettingsRow, TelegramCallbackRow)
            .join(TelegramOutboxRow, TelegramOutboxRow.id == self.item.id)
            .join(TelegramPersonSettingsRow, TelegramPersonSettingsRow.owner_user_id == TelegramBotLinkRow.owner_user_id)
            .outerjoin(TelegramCallbackRow, TelegramCallbackRow.group_id == group if group else false())
        )

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        row, settings = rows[0][self.column : self.column + 2]
        if _item(row) != self.item or claim_of(row) != self.claim:
            raise DeliveryClaimRetired()
        actual = _snapshot(settings)
        if actual.locale != self.settings.locale or actual.timezone != self.settings.timezone or preference_skip(self.item, actual, self.now or self.host.outbox.clock()):
            raise ProductSourceRetired()
        moment = self.host.outbox.clock()
        local = moment.astimezone(self.zone)
        minute, start, end = local.hour * 60 + local.minute, actual.quiet_start_minute, actual.quiet_end_minute
        quiet = actual.quiet_enabled and (minute >= start or minute < end if start > end else start <= minute < end)
        for member in self.source_members:
            if getattr(member.source, "key", None) == "remind.ready_to_resume":
                due = datetime.fromisoformat(member.source.values["due"])
                if moment < due:
                    raise DeliveryRetry(due - moment)
                if quiet and actual.summary_enabled:
                    raise ProductSourceRetired("quiet-hours")
        if self.captured is not None:
            latest = self.host.outbox._sending(
                rows[0][0], row, link_id=self.link.id, part_id=self.captured.part["part_id"], claim_owner=self.captured.claim_owner, claim_id=self.captured.claim_id, token_revision=self.captured.token_revision
            )
            if latest != self.captured:
                raise DeliveryClaimRetired()
            recent = rows[0][0].last_update_at is not None and moment - aware(rows[0][0].last_update_at) <= timedelta(seconds=self.host.outbox.config.outbox.recent_act_seconds)
            if bool(row.silent or quiet and not recent) != bool(self.silent):
                raise DeliveryClaimRetired()
        if row.parts:
            controls = list({control.id: control for *_, control in rows if control is not None}.values())
            try:
                _validate_rows(row, controls, allow_pending=True)
                for control in controls:
                    self.host.callbacks._usable(control)
            except CallbackRefused:
                raise ProductSourceRetired() from None


async def acknowledge_digest(session, row):
    from app.telegram_bots.links import BotLink

    binding = await session.get(TelegramBotLinkRow, row.link_id)
    if binding is None:
        raise ProductSourceRetired()
    frozen_digest(SimpleNamespace(link=BotLink.from_row(binding), item=_item(row), part=row.parts[0]), acknowledged=True)


def same_dispatch(expected, current):
    """ACK belongs to the manifest admitted before the external SDK await."""

    def parts(item):
        return [{key: value for key, value in part.items() if key not in {"plain_fallback", "invalid_token_verification_pending"}} for part in item.parts]

    return replace(expected, payload={}, parts=[]) == replace(current, payload={}, parts=[]) and same_body(expected.payload, current.payload) and same_body(parts(expected), parts(current))
