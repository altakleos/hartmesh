"""Explicit protected digest Show, preserving the original batch receipt."""

from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import DeliveryClaimRetired, PermanentDeliveryError, ProductSourceRetired, _item, _safe_json
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_delivery import member_proof
from app.telegram_bots.work_digest_navigation import DigestNavigation, _NavigationProjection
from app.telegram_bots.work_digest_text import DigestEntry, present_digest
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def parent_proof(item):
    return digest([item.id, item.dedupe_key, item.payload, item.parts])


def recipes(basis, identity):
    shown = present_digest([DigestEntry(**entry) for entry in basis["entries"]], locale=basis["locale"], protected=basis["protected"], outbox_id=identity)
    part = {**shown.parts[0], "kind": "edit", "target_message_id": basis["target"], "target_protect_content": True, "stored_keyboard": True, "source_ref": {"key": "work.digest.view", "values": {"outbox_id": identity}}}
    return part, [list(line) for line in shown.keyboard]


def frozen_view(captured, *, pending=False, acknowledged=False):
    try:
        item = captured.item
        basis = item.payload["work_digest_view_basis"]
        if (
            type(basis) is not dict
            or set(basis) != {"version", "parent_id", "parent_proof", "callback_id", "locale", "protected", "target", "entries", "members", "parts", "keyboard"}
            or type(basis["version"]) is not int
            or basis["version"] != 1
            or type(item.payload["presentation_version"]) is not int
            or item.payload["presentation_version"] != 0
            or item.class_ != "reply"
            or "digest_dispatch_conflict" in item.payload
            or item.message_thread_id is not None
            or item.dedupe_key != "notice:digest-show:" + basis["callback_id"]
            or type(basis["target"]) is not int
            or basis["target"] <= 0
            or basis["protected"] is not True
            or type(basis["locale"]) is not str
            or any(type(basis[key]) is not str or len(basis[key]) != length or any(char not in "0123456789abcdef" for char in basis[key]) for key, length in (("parent_id", 32), ("callback_id", 32), ("parent_proof", 64)))
            or len(item.parts) != 1
        ):
            raise ValueError()
        recipe, keyboard = recipes(basis, item.id)
        if not same_body([recipe], basis["parts"]) or not same_body(keyboard, basis["keyboard"]) or not same_body(keyboard, item.payload["product_keyboard_basis"]):
            raise ValueError()
        part = item.parts[0]
        metadata = {"part_id", "ordinal", "presentation_version", "state", "message_ids", "sent_at", "dispatched_at", "plain_fallback", "invalid_token_verification_pending", "callback_group_id"}
        if (
            not same_body({key: value for key, value in part.items() if key not in metadata}, recipe)
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
            if not same_body(part["message_ids"], [basis["target"]]) or datetime.fromisoformat(part["sent_at"]).utcoffset() is None:
                raise ValueError()
        elif part["message_ids"] or "sent_at" in part:
            raise ValueError()
        if "dispatched_at" in part and datetime.fromisoformat(part["dispatched_at"]).utcoffset() is None:
            raise ValueError()
        return basis, recipe
    except (KeyError, TypeError, ValueError, IndexError):
        raise PermanentDeliveryError("invalid-digest-view") from None


class DigestViews:
    def __init__(self, plans):
        self.plans, self.navigation = plans, DigestNavigation(plans)
        self.outbox, self.callbacks = plans.outbox, plans.callbacks

    async def execute_callback(self, channel, row, **_options):
        if row.kind != "reveal":
            return False
        async with self.outbox.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, row.payload.get("anchor_outbox_id"))
            if parent is None or "work_digest_basis" not in parent.payload:
                return False
        link = deepcopy(channel.link)
        guard = channel._services.commands._guard(channel, link)
        try:
            await self.reveal(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard)
        except ProductSourceRetired:
            raise CallbackRefused("stale.not_waiting") from None
        return True

    async def reveal(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            key = "notice:digest-show:" + callback_id
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None or callback.kind != "reveal" or callback.link_id != link.id:
                    raise CallbackRefused()
                identity = callback.payload.get("anchor_outbox_id")
                existing = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key))
            snapshot = await self.navigation.read(link, identity, callback_id=callback_id, closed_reason="revealed" if existing is not None else None)
            if (chat_id, message_id, message_thread_id) != (link.chat_id, snapshot.item.parts[0]["message_ids"][0], None):
                raise CallbackRefused()
            if existing is not None:
                basis, _ = frozen_view(SimpleNamespace(link=link, item=_item(existing), part=None), pending=existing.state == "pending", acknowledged=existing.state == "sent")
                if basis["parent_id"] != identity or basis["parent_proof"] != parent_proof(snapshot.item):
                    raise CallbackRefused()
                return existing.id
            composed = _ViewGuard(guard, snapshot)
            async with self.callbacks.writer(link, composed) as writer:
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                if group.anchor_outbox_id != identity:
                    raise CallbackRefused()
                if await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key)):
                    raise CallbackRefused()
                identity = uuid4().hex
                row = TelegramOutboxRow(
                    id=identity,
                    link_id=link.id,
                    owner_user_id=link.owner_user_id,
                    binding_generation=link.binding_generation,
                    chat_id=link.chat_id,
                    instance_id=link.agent_ref,
                    dedupe_key=key,
                    class_="reply",
                    payload={"presentation_version": 0},
                    parts=[],
                    state="pending",
                    silent=True,
                    not_before=self.outbox.clock(),
                    created_at=self.outbox.clock(),
                )
                writer.add(row)
                await writer.flush()
                basis = {
                    "version": 1,
                    "parent_id": snapshot.item.id,
                    "parent_proof": parent_proof(snapshot.item),
                    "callback_id": callback_id,
                    "locale": snapshot.locale,
                    "protected": True,
                    "target": message_id,
                    "entries": [asdict(member.entry) for member in snapshot.members],
                    "members": [member_proof(member) for member in snapshot.members],
                }
                part, keyboard = recipes(basis, identity)
                basis.update(parts=[part], keyboard=keyboard)
                row.payload = _safe_json({**row.payload, "work_digest_view_basis": basis, "product_keyboard_basis": keyboard})
                row.parts = self.outbox._parts([part], 0)
                fresh = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=chat_id, message_id=None, message_thread_id=None, session=writer)
                row.parts = [{**row.parts[0], "callback_group_id": fresh.group_id}]
                for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == fresh.group_id)):
                    control.created_at = group.selected.created_at
                    control.payload = {**control.payload, "presentation_pending": True}
                await self.callbacks.consume_group(group, reason="revealed", session=writer)
                composed.snapshot = replace(snapshot, closed_reason="revealed")
                frozen_view(SimpleNamespace(link=link, item=_item(row), part=None), pending=True)
                return identity

        return await await_drained(perform())


class _ViewGuard:
    def __init__(self, base, snapshot):
        self.base, self.snapshot = base, snapshot

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        await self.snapshot.authorize(session=writer)
        self.base.current()
        return current


class DigestViewSources:
    def __init__(self, views):
        self.views = views

    async def prepare(self, captured):
        basis, part = frozen_view(captured)
        snapshot = await self.views.navigation.read(captured.link, basis["parent_id"], callback_id=basis["callback_id"], closed_reason="revealed")
        if (
            basis["parent_proof"] != parent_proof(snapshot.item)
            or basis["locale"] != snapshot.locale
            or not same_body(basis["members"], [member_proof(member) for member in snapshot.members])
            or not same_body(basis["entries"], [asdict(member.entry) for member in snapshot.members])
        ):
            raise ProductSourceRetired()
        if basis["target"] != snapshot.item.parts[0]["message_ids"][0] or snapshot.item.parts[0]["protect_content"] is not True:
            raise ProductSourceRetired()
        return snapshot, part

    async def read_part(self, captured):
        async def read():
            _, part = await self.prepare(captured)
            return part["html"], part["visible_text"]

        return await await_drained(read())

    async def authorize(self, captured, *, silent, now):
        async def read():
            snapshot, _ = await self.prepare(captured)
            admission = await prepare_attention(self.views.plans.reader, captured.link)
            await admission(projection=_ViewProjection(self, snapshot, captured, silent))

        return await await_drained(read())


class _ViewProjection(_NavigationProjection):
    def __init__(self, host, snapshot, captured, silent):
        super().__init__(snapshot)
        self.host, self.captured, self.silent = host, captured, silent

    def extend(self, query):
        query = super().extend(query)
        self.view_column = len(query.column_descriptions)
        row, control = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        return query.add_columns(row, control).join(row, row.id == self.captured.item.id).outerjoin(control, control.group_id == self.captured.part["callback_group_id"])

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        row = rows[0][self.view_column]
        captured = self.captured
        latest = self.host.views.outbox._sending(rows[0][0], row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
        if latest != captured or row.silent is not True or self.silent is not True:
            raise DeliveryClaimRetired()
        controls = list({control.id: control for *_, control in rows if control is not None}.values())
        try:
            _validate_rows(row, controls, allow_pending=True)
            for control in controls:
                self.host.views.callbacks._usable(control)
        except CallbackRefused:
            raise ProductSourceRetired() from None


async def acknowledge_view(session, row):
    from app.telegram_bots.links import BotLink

    binding = await session.get(TelegramBotLinkRow, row.link_id)
    if binding is None:
        raise ProductSourceRetired()
    frozen_view(SimpleNamespace(link=BotLink.from_row(binding), item=_item(row), part=None), acknowledged=True)
