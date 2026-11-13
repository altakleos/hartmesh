"""Explicit recovery owns a frozen cohort, never just its quoted member."""

import re
from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import select

from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_album_guard import AlbumGuard
from app.telegram_bots.work_album_inputs import AlbumInputProof, album_context, input_packet, inputs_digest, member_packet
from app.telegram_bots.work_file_albums import album_manifest
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def recovery_context(mark):
    value = mark.route_basis.get("work_album_recovery")
    keys = {"version", "member_ids", "inputs_sha256", "source_outbox_id", "request_id", "work_id", "prompt_id", "notice_id", "operation_ids", "basis_sha256"}
    if type(value) is not dict or set(value) != keys or type(value["version"]) is not int or value["version"] != 1:
        raise CallbackRefused()
    ids = value["member_ids"]
    if type(ids) is not list or not 1 <= len(ids) <= 64 or any(type(item) is not int or not 0 < item < 2**63 for item in ids) or len(set(ids)) != len(ids) or mark.message_id not in ids:
        raise CallbackRefused()
    operations = value["operation_ids"]
    if type(operations) is not list or len(operations) > 64 or operations != sorted(set(operations)) or any(type(item) is not str or re.fullmatch(r"[0-9a-f]{32}", item) is None for item in operations):
        raise CallbackRefused()
    for key in ("source_outbox_id", "request_id", "work_id", "prompt_id", "notice_id", "inputs_sha256", "basis_sha256"):
        item = value[key]
        if item is None and key in {"source_outbox_id", "request_id", "work_id"}:
            continue
        if type(item) is not str or re.fullmatch(r"[0-9a-f]{64}" if key in {"inputs_sha256", "basis_sha256"} else r"[0-9a-f]{32}", item) is None:
            raise CallbackRefused()
    if (value["request_id"] is None) != (value["work_id"] is None) or value["request_id"] is not None and value["source_outbox_id"] is None:
        raise CallbackRefused()
    return value


def recovery_inputs(marks):
    packets = []
    for mark in marks:
        stamp = datetime.fromisoformat(mark.accepted_at) if type(mark.accepted_at) is str else mark.accepted_at
        if not isinstance(stamp, datetime):
            raise CallbackRefused()
        stamp = stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp.astimezone(UTC)
        packets.append({**input_packet(mark), "accepted_at": stamp.isoformat()})
    return inputs_digest(packets)


def recovery_basis(marks):
    from app.telegram_bots.inbox import _basis

    return inputs_digest([{key: value for key, value in _basis(mark).items() if key not in {"work_album_recovery", "work_album_handoff"}} for mark in marks])


async def cohort(inbox, writer, link, origin):
    """Read the stored cohort without adopting later Telegram members."""
    first = (await inbox._rows(writer, [origin]))[0]
    inbox._current(link, first)
    existing = first.route_basis.get("work_album_recovery")
    if existing is not None:
        existing = recovery_context(first)
        identities = existing["member_ids"]
    elif "work_album" in first.route_basis:
        identities = album_context(first)["member_ids"]
    else:
        identities = album_manifest(first, link)["message_ids"]
    row = TelegramInboundMarkRow
    marks = list(await writer.scalars(select(row).where(row.link_id == link.id, row.chat_id == link.chat_id, row.message_id.in_(identities)).with_for_update().execution_options(populate_existing=True)))
    indexed = {mark.message_id: mark for mark in marks}
    if len(indexed) != len(identities):
        raise CallbackRefused()
    ordered = [indexed[identity] for identity in identities]
    for mark in ordered:
        inbox._current(link, mark)
        if (
            mark.message_thread_id != first.message_thread_id
            or mark.route_complete
            or any(getattr(mark, key) is not None for key in ("turn_id", "thread_id", "run_id", "outbox_id", "assistant_message_id"))
            or "work_action_id" in mark.route_basis
        ):
            raise CallbackRefused()
        if existing is not None:
            if mark.route_kind != "work_album_hold" or not same_body(mark.route_basis.get("work_album_recovery"), existing):
                raise CallbackRefused()
        elif not same_body(album_manifest(mark, link), album_manifest(first, link)):
            raise CallbackRefused()
        for identity in mark.operation_ids:
            if await writer.get(TelegramActRow, identity) is not None:
                raise CallbackRefused()
    if existing is None and "work_album" in first.route_basis:
        root = next(mark for mark in ordered if mark.message_id == album_context(first)["root_message_id"])
        AlbumInputProof(writer.get_bind().dialect.name == "sqlite", InboxRow.from_row(root)).check([member_packet(mark) for mark in ordered])
    if existing is not None and (
        recovery_inputs(ordered) != existing["inputs_sha256"] or recovery_basis(ordered) != existing["basis_sha256"] or sorted({identity for mark in ordered for identity in mark.operation_ids}) != existing["operation_ids"]
    ):
        raise CallbackRefused()
    return ordered


class WorkAlbumRecovery:
    def __init__(self, files):
        self.files, self.services, self.inbox = files, files.services, files.inbox
        self.closed = files.inputs.callbacks.closed
        self.callbacks = self.services.callbacks

    async def offer(self, channel, origin, *, too_long=False, recovery_guard=None):
        async def perform():
            link = deepcopy(channel.link)
            guard = recovery_guard or self.services.commands._guard(channel, link)
            async with self.callbacks.writer(link, guard) as writer:
                marks = await cohort(self.inbox, writer, link, origin)
                if "work_album_recovery" in marks[0].route_basis:
                    return marks[0].route_basis["work_album_recovery"]["notice_id"]
                manifest = album_manifest(marks[0], link)
                source_id = manifest["source_ids"][0] if len(manifest["source_ids"]) == 1 and manifest["state"] != "ambiguous" else None
                parent = await writer.get(TelegramOutboxRow, source_id) if source_id else None
                request_id = work_id = None
                if parent is not None:
                    if (parent.link_id, parent.owner_user_id, parent.chat_id, parent.binding_generation, parent.message_thread_id) != (link.id, link.owner_user_id, link.chat_id, link.binding_generation, origin.message_thread_id):
                        raise CallbackRefused()
                    request_id, work_id = parent.request_id, parent.work_id
                originals = [InboxRow.from_row(mark) for mark in marks]
            available = False
            if request_id is not None:
                try:
                    source, _settings = await self.closed.recovery._source(link, request_id)
                    available = source.work["id"] == work_id
                except (AgentDenied, ProductSourceRetired):
                    pass
            guard = AlbumGuard(guard, originals, reader=self.closed.plans.reader)
            async with self.callbacks.writer(link, guard) as writer:
                marks = await cohort(self.inbox, writer, link, origin)
                for mark, original in zip(marks, originals, strict=True):
                    self.inbox._publication_snapshot_matches(mark, original)
                prompt_id = uuid4().hex
                writer.add(
                    TelegramCallbackRow(
                        id=prompt_id,
                        group_id=uuid4().hex,
                        link_id=link.id,
                        chat_id=link.chat_id,
                        binding_generation=link.binding_generation,
                        message_thread_id=origin.message_thread_id,
                        message_id=None,
                        kind="prompt",
                        target_kind=None,
                        target_id=None,
                        payload={"purpose": "retired_presentation", "anchor_outbox_id": source_id, "work_request_id": request_id},
                        created_at=self.callbacks.clock(),
                        consumed_at=self.callbacks.clock(),
                        closed_reason="answered",
                    )
                )
                await writer.flush()
                payload = {"prompt_id": prompt_id, "message_id": marks[0].message_id}
                choices = [("prompt_send_new", "label.send_new"), ("prompt_discard", "label.discard")]
                if available:
                    choices.insert(0, ("prompt_reopen", "label.reopen_answer"))
                notice_id = await self.services.outbox.plan_product_keyboard(
                    link,
                    source_id="work-album-closed:" + prompt_id,
                    parts=[{"kind": "text", "source_ref": {"key": "ask.answer_too_long" if available and too_long else "prompt.closed" if available else "prompt.unavailable", "values": {}}, "reply_to_message_id": marks[0].message_id}],
                    keyboard=[[{"kind": kind, "label": label, "target_kind": "request" if request_id else None, "target_id": request_id, "payload": payload}] for kind, label in choices],
                    callbacks=self.callbacks,
                    guard=guard,
                    inbox_rows=originals,
                    finish_route=False,
                    message_thread_id=origin.message_thread_id,
                    session=writer,
                )
                context = {
                    "version": 1,
                    "member_ids": [mark.message_id for mark in marks],
                    "inputs_sha256": recovery_inputs(marks),
                    "source_outbox_id": source_id,
                    "request_id": request_id,
                    "work_id": work_id,
                    "prompt_id": prompt_id,
                    "notice_id": notice_id,
                    "operation_ids": sorted({identity for mark in marks for identity in mark.operation_ids}),
                    "basis_sha256": recovery_basis(marks),
                }
                for mark in marks:
                    mark.route_kind = "work_album_hold"
                    mark.route_basis = {**mark.route_basis, "work_album_recovery": context}
                guard.expect([InboxRow.from_row(mark) for mark in marks])
                await writer.flush()
                return notice_id

        return await await_drained(perform())

    async def execute_callback(self, channel, row, *, update_id):
        if row.kind not in {"prompt_reopen", "prompt_send_new", "prompt_discard"}:
            return False

        async def perform():
            link = deepcopy(channel.link)
            guard = self.services.commands._guard(channel, link)
            async with self.callbacks.writer(link, guard) as writer:
                mark = await writer.get(TelegramInboundMarkRow, (link.id, row.payload.get("message_id"), link.chat_id))
                if mark is None or "work_album_recovery" not in mark.route_basis:
                    return False
                origin = InboxRow.from_row(mark)
            if row.kind == "prompt_reopen":
                from app.telegram_bots.work_album_reopen import reopen_album

                return await reopen_album(self, channel, link, row, origin, guard=guard)
            if row.kind == "prompt_send_new":
                from app.telegram_bots.work_album_handoff import send_album_as_new

                return await send_album_as_new(self, channel, link, row, update_id=update_id, guard=guard)
            async with self.callbacks.writer(link, guard) as writer:
                originals = [InboxRow.from_row(mark) for mark in await cohort(self.inbox, writer, link, origin)]
            guard = AlbumGuard(guard, originals, reader=self.closed.plans.reader, callback_id=row.id)
            async with self.callbacks.writer(link, guard) as writer:
                group = await self.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                marks = await cohort(self.inbox, writer, link, origin)
                context = marks[0].route_basis["work_album_recovery"]
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id)
                if (
                    group.selected.payload.get("prompt_id") != context["prompt_id"]
                    or context["notice_id"] != group.anchor_outbox_id
                    or parent.payload["product_basis"]["origins"] != sorted([[link.id, mark.message_id, link.chat_id] for mark in marks])
                ):
                    raise CallbackRefused()
                if any("publication_owner" in control.payload for control in group.rows):
                    raise CallbackRefused()
                if row.kind == "prompt_discard":
                    guard.allow_finish = True
                    await self.callbacks.consume_group(group, session=writer)
                    await self.services.outbox.plan_product_keyboard(
                        link,
                        source_id="work-album-discard:" + row.id,
                        parts=[{"kind": "edit", "source_ref": {"key": "prompt.discarded", "values": {}}, "target_message_id": row.message_id}],
                        keyboard=[],
                        callbacks=self.callbacks,
                        guard=guard,
                        inbox_rows=[InboxRow.from_row(mark) for mark in marks],
                        finish_route=True,
                        message_thread_id=row.message_thread_id,
                        session=writer,
                    )
                    guard.expect([InboxRow.from_row(mark) for mark in marks])
                    guard.allow_finish = False
                    return True
                raise CallbackRefused()

        return await await_drained(perform())

    async def unavailable(self, channel, link, selected, origin, *, guard):
        async with self.callbacks.writer(link, guard) as writer:
            originals = [InboxRow.from_row(mark) for mark in await cohort(self.inbox, writer, link, origin)]
        guard = AlbumGuard(guard, originals, reader=self.closed.plans.reader, callback_id=selected.id)
        async with self.callbacks.writer(link, guard) as writer:
            group = await self.callbacks.lock_current(link, selected.id, chat_id=selected.chat_id, message_id=selected.message_id, message_thread_id=selected.message_thread_id, guard=guard, session=writer)
            marks = await cohort(self.inbox, writer, link, origin)
            context = recovery_context(marks[0])
            if (
                selected.kind != "prompt_reopen"
                or group.anchor_outbox_id != context["notice_id"]
                or selected.payload.get("prompt_id") != context["prompt_id"]
                or selected.payload.get("message_id") != marks[0].message_id
                or any("publication_owner" in row.payload for row in group.rows)
            ):
                raise CallbackRefused()
            await self.callbacks.consume_group(group, session=writer)
            payload = {"prompt_id": context["prompt_id"], "message_id": marks[0].message_id}
            notice = await self.services.outbox.plan_product_keyboard(
                link,
                source_id="work-album-unavailable:" + selected.id,
                parts=[{"kind": "edit", "source_ref": {"key": "prompt.unavailable", "values": {}}, "target_message_id": selected.message_id}],
                keyboard=[[{"kind": kind, "label": label, "target_kind": "request", "target_id": context["request_id"], "payload": payload}] for kind, label in [("prompt_send_new", "label.send_new"), ("prompt_discard", "label.discard")]],
                callbacks=self.callbacks,
                guard=guard,
                inbox_rows=[InboxRow.from_row(mark) for mark in marks],
                finish_route=False,
                message_thread_id=origin.message_thread_id,
                session=writer,
            )
            for mark in marks:
                mark.route_basis = {**mark.route_basis, "work_album_recovery": {**context, "notice_id": notice}}
            guard.expect([InboxRow.from_row(mark) for mark in marks])
            await writer.flush()
            return True
