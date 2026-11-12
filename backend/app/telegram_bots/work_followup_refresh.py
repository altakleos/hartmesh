"""Explicit recipient refresh preserves the already completed response."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace

from sqlalchemy import and_, false, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _topic
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_outcome_delivery import _ContextProjection, _followup_candidates, context, materialize, validate_input
from app.telegram_bots.work_routing_followups import FollowupAdmission, acknowledged_followup, read_followup, same_followup
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def refresh_identity(basis):
    value = basis["refresh"]
    if type(value) is not dict or set(value) != {"callback_id", "anchor_outbox_id", "input_outbox_id"}:
        raise ProductSourceRetired()
    if any(type(item) is not str or len(item) != 32 or any(ch not in "0123456789abcdef" for ch in item) for item in value.values()):
        raise ProductSourceRetired()
    return value


def validate_refresh(callback, parent, basis, row):
    value = refresh_identity(basis)
    if callback is None or parent is None or (callback.id, parent.id) != (value["callback_id"], value["anchor_outbox_id"]):
        raise ProductSourceRetired()
    link = SimpleNamespace(id=row.link_id, owner_user_id=row.owner_user_id, chat_id=row.chat_id, binding_generation=row.binding_generation, agent_kind="instance", agent_ref=row.instance_id)
    acknowledged_followup(link, parent)
    old = parent.payload["work_outcome_basis"]
    if (
        callback.kind != "route_to"
        or (callback.link_id, callback.chat_id, callback.binding_generation, callback.message_thread_id, callback.payload.get("anchor_outbox_id")) != (row.link_id, row.chat_id, row.binding_generation, None, parent.id)
        or callback.consumed_at is None
        or callback.closed_reason != "routing-refresh"
        or callback.payload.get("work_followup_outbox_id") != row.id
        or not same_body(old["context"]["act"], basis["context"]["act"])
        or not same_body(old["input"], basis["input"])
        or not same_body(old["origin"], basis["origin"])
        or value["input_outbox_id"] != old.get("refresh", {}).get("input_outbox_id", parent.id)
        or not any(part.get("callback_group_id") == callback.group_id and part["message_ids"] == [callback.message_id] for part in parent.parts)
    ):
        raise ProductSourceRetired()


class WorkRoutingFollowupRecovery:
    def __init__(self, plans):
        self.plans, self.outbox, self.callbacks = plans, plans.outbox, plans.callbacks

    async def refresh(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        if type(chat_id) is not int or type(message_id) is not int or message_id <= 0:
            raise CallbackRefused()
        topic = _topic(message_thread_id)

        async def perform():
            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if (
                    callback is None
                    or (callback.link_id, callback.chat_id, callback.message_id, callback.message_thread_id, callback.binding_generation) != (link.id, chat_id, message_id, topic, link.binding_generation)
                    or callback.kind != "route_to"
                ):
                    raise CallbackRefused()
                parent = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id"))
                if parent is None:
                    raise CallbackRefused()
                acknowledged_followup(link, parent)
                old_basis = deepcopy(parent.payload["work_outcome_basis"])
                prior = callback.payload.get("work_followup_outbox_id")
            repairing = None
            if prior is not None:
                # Replaying this refresh only returns its already published
                # identity. It cannot create a new reply after collection.
                async with self.callbacks.writer(link, guard) as writer:
                    original = await writer.get(TelegramCallbackRow, callback_id, with_for_update=True, populate_existing=True)
                    saved_parent = await writer.get(TelegramOutboxRow, parent.id, populate_existing=True)
                    row = await writer.get(TelegramOutboxRow, prior, populate_existing=True)
                    if row is None:
                        raise CallbackRefused()
                    validate_refresh(original, saved_parent, row.payload["work_outcome_basis"], row)
                    if row.state == "failed" and row.failed_reason == "work-outcome-refresh-required" and row.claim_id is None:
                        repairing = row.id
                    else:
                        return row.id
            read = await self.plans.reader.read(link, old_basis["context"]["act"]["id"])
            routing = await read_followup(self.plans.routing, link, read)
            if routing is None:
                raise ProductSourceRetired()
            settings = await self.plans.settings.get(link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            data = context(read)
            parts, keyboard, _ = await asyncio.to_thread(materialize, link, data, locale=settings.locale, timezone=settings.timezone, product=self.plans.product, routing=routing)
            prepared = await prepare_attention(self.plans.plans.reader, link)
            projection = _RefreshProjection(read, FollowupAdmission(link, parent), _followup_candidates(self.plans, link, data, routing), callback_id)
            composed = _RefreshGuard(guard, prepared, projection)
            async with self.callbacks.writer(link, composed) as writer:
                if repairing is None:
                    group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=topic, guard=composed, session=writer)
                    row = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, with_for_update=True, populate_existing=True)
                    controls = [await writer.get(TelegramCallbackRow, item.id, populate_existing=True) for item in group.rows]
                else:
                    row, controls, original = await self.callbacks._locked(writer, link, callback_id)
                    obsolete = await writer.get(TelegramOutboxRow, repairing, with_for_update=True, populate_existing=True)
                    if obsolete is None or obsolete.state != "failed" or obsolete.failed_reason != "work-outcome-refresh-required" or obsolete.claim_id is not None:
                        raise CallbackRefused()
                    validate_refresh(original, row, obsolete.payload["work_outcome_basis"], obsolete)
                    await self.plans._retire(writer, link, obsolete)
                if row.id != parent.id or not same_body(row.payload["work_outcome_basis"], old_basis):
                    raise ProductSourceRetired()
                for item in controls:
                    if item.payload.get("work_action_id") is not None:
                        action = await writer.get(TelegramActRow, item.payload["work_action_id"], with_for_update=True, populate_existing=True)
                        if action is None or action.kind != "route" or action.state != "refused" or action.reason != "recipient-ineligible":
                            raise CallbackRefused(None)
                if (
                    repairing is None
                    and all(item.payload.get("work_action_id") is None for item in controls)
                    and same_followup(routing, old_basis["routing"])
                    and (settings.locale, settings.timezone) == (old_basis["locale"], old_basis["timezone"])
                ):
                    return row.id
                basis = {
                    "version": 4,
                    "context": data,
                    "locale": settings.locale,
                    "timezone": settings.timezone,
                    "parts": parts,
                    "keyboard": keyboard,
                    "routing": routing,
                    "destination": {"chat_id": link.chat_id, "message_thread_id": None},
                    "input": old_basis["input"],
                    "origin": old_basis["origin"],
                    "refresh": {"callback_id": callback_id, "anchor_outbox_id": row.id, "input_outbox_id": old_basis.get("refresh", {}).get("input_outbox_id", row.id)},
                }
                identity, _ = await self.outbox._add(
                    link,
                    dedupe_key="notice:work-routing-refresh:" + callback_id + ("." + repairing if repairing else ""),
                    class_="reply",
                    payload={"parts": parts, "product_keyboard_basis": keyboard, "work_outcome_basis": basis},
                    instance_id=link.agent_ref,
                    work_id=row.work_id,
                    request_id=row.request_id,
                    message_thread_id=None,
                    session=writer,
                )
                fresh = await writer.get(TelegramOutboxRow, identity)
                projection.published_id = identity
                saved_parts = self.outbox._parts(parts, 0)
                if keyboard:
                    new_group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=identity, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
                    saved_parts[-1]["callback_group_id"] = new_group.group_id
                    for item in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == new_group.group_id)):
                        item.created_at = callback.created_at
                        item.payload = {**item.payload, "presentation_pending": True}
                fresh.parts = saved_parts
                await self.plans._retire_controls(writer, link, row, "routing-refresh")
                for item in controls:
                    item.payload = {**item.payload, "work_followup_outbox_id": identity}
                validate_refresh(next(item for item in controls if item.id == callback_id), row, basis, fresh)
                if basis["input"] is not None:
                    mark = basis["input"]
                    saved_input = await writer.get(TelegramInboundMarkRow, (mark["link_id"], mark["message_id"], mark["chat_id"]), with_for_update=True, populate_existing=True)
                    validate_input(saved_input, basis, fresh)
                return identity

        return await await_drained(perform())


class _RefreshProjection(_ContextProjection):
    def __init__(self, read, origin, candidates, callback_id):
        super().__init__(read)
        self.origin, self.candidates, self.callback_id = origin, candidates, callback_id
        self.published_id = None

    def extend(self, query):
        callback, fresh, mark, reserved = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow), aliased(TelegramInboundMarkRow), aliased(TelegramActRow)
        identity = self.origin.basis["input"]
        result = (
            self.origin.extend(super().extend(query))
            .add_columns(callback, fresh, mark, reserved)
            .outerjoin(callback, callback.id == self.callback_id)
            .outerjoin(fresh, fresh.id == self.published_id if self.published_id else false())
            .outerjoin(mark, and_(mark.link_id == identity["link_id"], mark.chat_id == identity["chat_id"], mark.message_id == identity["message_id"]) if identity else false())
            .outerjoin(reserved, reserved.id == callback.payload["work_action_id"].as_string())
        )
        return self.candidates.extend(result)

    def check(self, rows):
        super().check(rows)
        self.origin.check([row[:21] for row in rows])
        callback, fresh, mark, reserved = rows[0][21:25]
        if callback is None:
            raise ProductSourceRetired()
        if callback.payload.get("work_action_id") is not None and (reserved is None or reserved.kind != "route" or reserved.state != "refused" or reserved.reason != "recipient-ineligible"):
            raise CallbackRefused(None)
        if self.published_id is not None:
            if fresh is None:
                raise ProductSourceRetired()
            validate_refresh(callback, rows[0][20], fresh.payload["work_outcome_basis"], fresh)
            if fresh.payload["work_outcome_basis"]["input"] is not None:
                validate_input(mark, fresh.payload["work_outcome_basis"], fresh)
        self.candidates.check(rows)


class _RefreshGuard:
    def __init__(self, base, prepared, projection):
        self.base, self.prepared, self.projection = base, prepared, projection

    async def prepare(self):
        await prepare_publication_guard(self.base)

    def current(self):
        self.base.current()

    async def check(self, writer):
        current = await self.base.check(writer)
        await self.prepared(session=writer, projection=self.projection)
        self.base.current()
        return current
