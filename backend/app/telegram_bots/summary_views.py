"""Explicit protected summary Show with its own source-owned reply and ACK."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.notifications import aware
from app.telegram_bots.pages import page_address
from app.telegram_bots.producer import DeliveryClaimRetired, PermanentDeliveryError, ProductSourceRetired, _item, _safe_json
from app.telegram_bots.summary_delivery import _validate_sections, frozen_summary
from app.telegram_bots.summary_navigation import SummaryNavigation, SummaryNavigationProjection
from app.telegram_bots.summary_text import present_summary
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_navigation import _same_item
from app.telegram_bots.work_digest_text import _identity
from app.telegram_bots.work_digest_views import parent_proof
from app.telegram_bots.work_request_source import require_show
from app.telegram_bots.work_result_navigation import NavigationOrigin
from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def summary_basis(snapshot):
    original, settings = snapshot.item.payload["summary_basis"], snapshot.settings
    return {
        "date": original["date"],
        "as_of": original["as_of"],
        "since": original["since"],
        "sections": snapshot.source.sections,
        "protected": snapshot.source.protected,
        "locale": settings.locale,
        "timezone": settings.timezone,
        "summary_minute": settings.summary_minute,
        "public_base_url": NavigationOrigin().base,
    }


def recipes(basis, link, identity):
    summary = basis["summary"]
    minute = summary["summary_minute"]
    shown = present_summary(
        summary["sections"],
        locale=summary["locale"],
        protected=True,
        concealed=False,
        outbox_id=identity,
        link_id=link.id,
        time=f"{minute // 60:02d}:{minute % 60:02d}",
        work_url=page_address("web.work", link, permissions=AgentPermission.INSPECT, public_base_url=summary["public_base_url"]),
    )
    part = {**shown.parts[0], "kind": "edit", "target_message_id": basis["target"], "target_protect_content": True, "silent": True, "stored_keyboard": True, "source_ref": {"key": "summary.view", "values": {"outbox_id": identity}}}
    return part, [list(line) for line in shown.keyboard]


def frozen_view(captured, *, pending=False, acknowledged=False):
    try:
        item, link = captured.item, captured.link
        basis = item.payload["summary_view_basis"]
        if type(basis) is not dict or set(basis) != {"version", "parent_id", "parent_proof", "callback_id", "target", "summary", "parts", "keyboard"}:
            raise ValueError()
        summary = basis["summary"]
        if (
            type(basis["version"]) is not int
            or basis["version"] != 1
            or type(item.payload["presentation_version"]) is not int
            or item.payload["presentation_version"] != 0
            or "summary_dispatch_conflict" in item.payload
            or item.class_ != "reply"
            or item.silent is not True
            or item.dedupe_key != "notice:summary-show:" + basis["callback_id"]
            or not _identity(basis["parent_id"])
            or not _identity(basis["callback_id"])
            or type(basis["parent_proof"]) is not str
            or len(basis["parent_proof"]) != 64
            or any(char not in "0123456789abcdef" for char in basis["parent_proof"])
            or type(basis["target"]) is not int
            or basis["target"] <= 0
            or (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.instance_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref)
            or any(value is not None for value in (item.work_id, item.request_id, item.event_id, item.message_thread_id))
            or type(summary) is not dict
            or set(summary) != {"date", "as_of", "since", "sections", "protected", "locale", "timezone", "summary_minute", "public_base_url"}
            or type(summary["protected"]) is not bool
            or summary["locale"] not in {"en-US", "zh-CN"}
            or type(summary["summary_minute"]) is not int
            or not 0 <= summary["summary_minute"] < 1440
            or datetime.fromisoformat(summary["as_of"]).utcoffset() is None
            or datetime.fromisoformat(summary["since"]).utcoffset() is None
            or datetime.fromisoformat(summary["since"]) > datetime.fromisoformat(summary["as_of"])
            or len(item.parts) != 1
        ):
            raise ValueError()
        _validate_sections(summary["sections"])
        recipe, keyboard = recipes(basis, link, item.id)
        if not same_body([recipe], basis["parts"]) or not same_body(keyboard, basis["keyboard"]) or not same_body(keyboard, item.payload["product_keyboard_basis"]):
            raise ValueError()
        part = item.parts[0]
        metadata = {"part_id", "ordinal", "presentation_version", "state", "message_ids", "sent_at", "dispatched_at", "plain_fallback", "url_button_fallback", "invalid_token_verification_pending", "callback_group_id"}
        if (
            not same_body({key: value for key, value in part.items() if key not in metadata}, recipe)
            or any(type(part[key]) is not int or part[key] != 0 for key in ("ordinal", "presentation_version"))
            or any(not _identity(part.get(key)) for key in ("part_id", "callback_group_id"))
            or part["state"] != ("sent" if acknowledged else "pending" if pending else "sending")
            or captured.part is not None
            and not same_body(captured.part, part)
            or type(part["message_ids"]) is not list
            or any(type(part[key]) is not bool for key in ("plain_fallback", "url_button_fallback") if key in part)
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
    except (KeyError, ValueError, TypeError, IndexError):
        raise PermanentDeliveryError("invalid-summary-view") from None


class SummaryViews:
    def __init__(self, plans):
        self.plans, self.outbox, self.callbacks = plans, plans.outbox, plans.callbacks
        self.navigation = SummaryNavigation(plans)

    async def execute_callback(self, channel, row, **_options):
        if row.kind != "reveal":
            return False
        async with self.outbox.sessions() as reader:
            parent = await reader.get(TelegramOutboxRow, row.payload.get("anchor_outbox_id"))
            if parent is None or "summary_basis" not in parent.payload:
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
            key = "notice:summary-show:" + callback_id
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None or callback.kind != "reveal" or callback.link_id != link.id:
                    raise CallbackRefused()
                parent_id = callback.payload.get("anchor_outbox_id")
                existing = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key))
            snapshot = await self.navigation.read(link, parent_id, callback_id=callback_id, closed_reason="revealed" if existing is not None else None)
            if type(message_id) is not int or (chat_id, message_id, message_thread_id) != (link.chat_id, snapshot.item.parts[-1]["message_ids"][0], None):
                raise CallbackRefused()
            if existing is not None:
                basis, _ = frozen_view(SimpleNamespace(link=link, item=_item(existing), part=None), pending=existing.state == "pending", acknowledged=existing.state == "sent")
                if basis["parent_id"] != parent_id or basis["parent_proof"] != parent_proof(snapshot.item):
                    raise CallbackRefused()
                return existing.id
            composed = _ViewGuard(guard, snapshot)
            async with self.callbacks.writer(link, composed) as writer:
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                if group.anchor_outbox_id != parent_id or await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == key)):
                    raise CallbackRefused()
                row = TelegramOutboxRow(
                    id=uuid4().hex,
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
                basis = {"version": 1, "parent_id": parent_id, "parent_proof": parent_proof(snapshot.item), "callback_id": callback_id, "target": message_id, "summary": summary_basis(snapshot)}
                part, keyboard = recipes(basis, link, row.id)
                basis.update(parts=[part], keyboard=keyboard)
                row.payload = _safe_json({"presentation_version": 0, "summary_view_basis": basis, "product_keyboard_basis": keyboard})
                row.parts = self.outbox._parts([part], 0)
                fresh = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=0, chat_id=chat_id, message_id=None, message_thread_id=None, session=writer)
                row.parts = [{**row.parts[0], "callback_group_id": fresh.group_id}]
                for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == fresh.group_id)):
                    control.created_at = group.selected.created_at
                    control.payload = {**control.payload, "presentation_pending": True}
                await self.callbacks.consume_group(group, reason="revealed", session=writer)
                composed.snapshot = replace(snapshot, closed_reason="revealed")
                frozen_view(SimpleNamespace(link=link, item=_item(row), part=None), pending=True)
                composed.item = _item(row)
                return row.id

        return await await_drained(perform())


class _ViewGuard:
    def __init__(self, base, snapshot):
        self.base, self.snapshot, self.item = base, snapshot, None

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        projection = SummaryNavigationProjection(self.snapshot) if self.item is None else _ViewProjection(self.snapshot, self.item)
        await self.snapshot.source.authorize(session=writer, projection=projection)
        self.base.current()
        return current


class _ViewProjection(SummaryNavigationProjection):
    def __init__(self, snapshot, item, *, captured=None, silent=True):
        super().__init__(snapshot)
        self.item, self.captured, self.silent = item, captured, silent
        self.origin = NavigationOrigin()

    def extend(self, query):
        query = super().extend(query)
        self.view_column = len(query.column_descriptions)
        row, control = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        return query.add_columns(row, control).join(row, row.id == self.item.id).outerjoin(control, control.group_id == self.item.parts[-1]["callback_group_id"])

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        row = rows[0][self.view_column]
        basis = self.item.payload["summary_view_basis"]
        self.origin.check(basis["summary"])
        if not _same_item(row, self.item) or basis["parent_proof"] != parent_proof(self.snapshot.item) or not same_body(basis["summary"], summary_basis(self.snapshot)):
            raise ProductSourceRetired()
        if self.captured is None:
            if claim_of(row) != (None, None, None):
                raise DeliveryClaimRetired()
        else:
            captured = self.captured
            latest = self.snapshot.owner.plans.outbox._sending(
                rows[0][0], row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision
            )
            if latest != captured or self.silent is not True:
                raise DeliveryClaimRetired()
        controls = list({control.id: control for record in rows if (control := record[self.view_column + 1]) is not None}.values())
        try:
            _validate_rows(row, controls, allow_pending=True)
            for control in controls:
                self.snapshot.owner.plans.callbacks._usable(control)
                if control.message_id is not None or control.payload.get("presentation_pending") is not True or aware(control.created_at) != aware(self.snapshot.item.created_at):
                    raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None


class SummaryViewSources:
    def __init__(self, views):
        self.views = views

    async def prepare(self, captured):
        basis, part = frozen_view(captured)
        snapshot = await self.views.navigation.read(captured.link, basis["parent_id"], callback_id=basis["callback_id"], closed_reason="revealed")
        if basis["parent_proof"] != parent_proof(snapshot.item) or basis["target"] != snapshot.item.parts[-1]["message_ids"][0] or not same_body(basis["summary"], summary_basis(snapshot)):
            raise ProductSourceRetired()
        return snapshot, part

    async def read_part(self, captured):
        snapshot, part = await self.prepare(captured)
        await snapshot.authorize()
        return part["html"], part["visible_text"]

    async def authorize(self, captured, *, silent, now):
        snapshot, _part = await self.prepare(captured)
        await snapshot.source.authorize(projection=_ViewProjection(snapshot, captured.item, captured=captured, silent=silent))


async def acknowledge_view(session, row):
    from app.telegram_bots.links import BotLink

    binding = await session.get(TelegramBotLinkRow, row.link_id)
    if binding is None:
        raise ProductSourceRetired()
    basis, _ = frozen_view(SimpleNamespace(link=BotLink.from_row(binding), item=_item(row), part=None), acknowledged=True)
    parent = await session.get(TelegramOutboxRow, basis["parent_id"])
    if parent is None or parent_proof(_item(parent)) != basis["parent_proof"]:
        raise ProductSourceRetired()
    frozen_summary(SimpleNamespace(link=BotLink.from_row(binding), item=_item(parent), part=None), acknowledged=True)
    if parent.state != "sent" or claim_of(parent) != (None, None, None) or parent.message_id != basis["target"]:
        raise ProductSourceRetired()
    original = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"])))
    selected = next((control for control in original if control.id == basis["callback_id"]), None)
    require_show(selected, parent)
    controls = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.parts[-1]["callback_group_id"])))
    try:
        _validate_rows(parent, original)
        if any(control.message_id != parent.message_id or aware(control.created_at) != aware(parent.created_at) for control in original):
            raise CallbackRefused()
        _validate_rows(row, controls, allow_pending=True)
        if any(control.message_id is not None or control.payload.get("presentation_pending") is not True or aware(control.created_at) != aware(parent.created_at) for control in controls):
            raise CallbackRefused()
    except CallbackRefused:
        raise ProductSourceRetired() from None
