"""Capture an explicit reply to a complete Work card before response admission."""

from dataclasses import replace
from datetime import datetime
from uuid import uuid4

from sqlalchemy import and_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackGroup, CallbackRefused, _aware, _row, _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import keyboard_cleanup_row
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from app.telegram_bots.work_input_capture import _TEXT
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.human_input_contract import Respond
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def reply_basis(link, parent, mark, controls, *, consumed):
    source = acknowledged_source(link, parent)
    basis = parent.payload["work_request_basis"]
    if basis["version"] not in {1, 2, 3} or basis.get("interaction", {}).get("view") == "prompt":
        raise CallbackRefused()
    raw = mark.sanitized_input
    if type(raw) is not dict or set(raw) - {"type", "text", "reply_to_message_id"} or raw.get("type", "text") != "text":
        raise CallbackRefused()
    quoted = raw.get("reply_to_message_id")
    if type(quoted) is not int or (parent.link_id, parent.chat_id, parent.binding_generation, parent.message_thread_id) != (mark.link_id, mark.chat_id, mark.binding_generation, mark.message_thread_id):
        raise CallbackRefused()
    parts = [part for part in parent.parts if part["presentation_version"] == parent.payload["presentation_version"]]
    messages = set()
    for part in parts:
        if len(part["message_ids"]) != 1 or part["message_ids"][0] in messages or datetime.fromisoformat(part["sent_at"]).utcoffset() is None:
            raise CallbackRefused()
        messages.update(part["message_ids"])
    groups = {part.get("callback_group_id") for part in parts} - {None}
    if quoted not in messages or len(groups) != 1 or not controls or {row.group_id for row in controls} != groups:
        raise CallbackRefused()
    _validate_rows(parent, controls)
    for control in controls:
        part = next(part for part in parts if part.get("callback_group_id") == control.group_id)
        if type(control.message_id) is not int or part["message_ids"] != [control.message_id]:
            raise CallbackRefused()
    if any("work_action_id" in row.payload or (row.consumed_at is not None) is not consumed or consumed and row.closed_reason != "work-reply" for row in controls):
        raise CallbackRefused()
    return {
        "version": 2,
        "source_outbox_id": parent.id,
        "presentation_version": parent.payload["presentation_version"],
        "group_id": controls[0].group_id,
        "reply_to_message_id": quoted,
        "request_id": source.request["id"],
        "work_id": source.work["id"],
        "purpose": "answer" if source.request["purpose"] == "information" else "comment",
        "seen": source.seen,
        "text": _TEXT.validate_python(raw.get("text")),
    }


def response(captured):
    data = captured.route_basis["work_input"]
    if data.get("version") != 2 or data.get("purpose") not in {"answer", "comment"} or len(captured.operation_ids) != 1:
        raise CallbackRefused()
    return Respond(operation_id=captured.operation_ids[0], expected_request_revision=data["seen"]["request_revision"], expected_assignment_revision=data["seen"]["assignment_revision"], text=data["text"])


class WorkCardInputs:
    def __init__(self, controls):
        self.controls, self.plans = controls, controls.plans
        self.inbox, self.callbacks = self.plans.outbox.inbox, self.plans.callbacks

    async def capture(self, link, origin, *, source_outbox_id, guard):
        async def perform():
            admission = await prepare_attention(self.plans.reader, link)
            composed = _CardGuard(guard, admission, link)
            async with self.callbacks.writer(link, composed) as writer:
                parent = await writer.get(TelegramOutboxRow, source_outbox_id, with_for_update=True, populate_existing=True)
                if parent is None:
                    raise CallbackRefused()
                groups = {part.get("callback_group_id") for part in parent.parts if part["presentation_version"] == parent.payload["presentation_version"]} - {None}
                rows = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups)).order_by(TelegramCallbackRow.id).with_for_update()))
                mark = (await self.inbox._rows(writer, [origin]))[0]
                self.inbox._current(link, mark)
                if origin.link_id != link.id:
                    raise CallbackRefused()
                data = reply_basis(link, parent, mark, rows, consumed=mark.route_kind is not None)
                if mark.route_kind is not None:
                    if mark.route_kind != "work_input" or mark.route_target_id != data["request_id"] or not same_body(mark.route_basis.get("work_input"), data) or len(mark.operation_ids) != 1:
                        raise CallbackRefused()
                else:
                    for row in rows:
                        self.callbacks._usable(row)
                    operation = uuid4().hex
                    composed.captured = replace(InboxRow.from_row(mark), route_kind="work_input", route_target_id=data["request_id"], route_basis={"work_input": data}, operation_ids=[operation], state="reply_pending")
                    await self.inbox.route(InboxRow.from_row(mark), kind="work_input", target_id=data["request_id"], basis={"work_input": data}, operation_ids=[operation], session=writer)
                    group = CallbackGroup(_row(rows[0]), tuple(_row(row) for row in rows), parent.id, parent.payload["presentation_version"])
                    await self.callbacks.consume_group(group, reason="work-reply", session=writer)
                    cleanup = keyboard_cleanup_row(rows[0], owner_user_id=link.owner_user_id, now=self.plans.outbox.clock(), dedupe_key="notice:work-reply-cleanup:" + rows[0].group_id)
                    cleanup.payload = {**cleanup.payload, "work_request_parent_id": parent.id}
                    writer.add(cleanup)
                    await self.inbox.register_delivery([origin], outbox_id=parent.id, primary=False, finish=False, session=writer)
                if composed.captured is None:
                    composed.captured = InboxRow.from_row(mark)
                return composed.captured

        return await await_drained(perform())

    async def stage(self, link, origin, *, guard):
        async def perform():
            async with self.plans.outbox.sessions() as reader:
                mark = (await self.inbox._rows(reader, [origin]))[0]
                if mark.route_kind != "work_input" or mark.route_basis.get("work_input", {}).get("version") != 2:
                    raise CallbackRefused()
                parent_id = mark.route_basis["work_input"]["source_outbox_id"]
            captured = await self.capture(link, origin, source_outbox_id=parent_id, guard=guard)
            request = response(captured)
            data = captured.route_basis["work_input"]
            await self.controls.acts._work_link(link)
            owner = await self.controls.acts.authority.prepare_live_owner(link.owner_user_id)
            admission = await prepare_attention(self.plans.reader, link)
            composed = _CardGuard(guard, admission, link, captured=captured)
            async with self.callbacks.writer(link, composed) as writer:
                existing = await writer.get(TelegramActRow, request.operation_id, with_for_update=True, populate_existing=True)
                if "work_action_id" in captured.route_basis and (captured.route_basis["work_action_id"] != request.operation_id or existing is None):
                    raise CallbackRefused()
                composed.require_current = existing is None
                await composed.check(writer)
                result = await self.controls.acts._stage_work_act(link, kind="respond", request_id=data["request_id"], work_id=data["work_id"], request=request, owner=owner, session=writer)
                mark = (await self.inbox._rows(writer, [origin]))[0]
                mark.route_basis = {**mark.route_basis, "work_action_id": result.id}
                composed.captured, composed.action = replace(captured, route_basis={**captured.route_basis, "work_action_id": result.id}), result
                return result

        return await await_drained(perform())


class _CardGuard:
    def __init__(self, base, admission, link, *, captured=None):
        self.base, self.admission, self.link, self.captured = base, admission, link, captured
        self.require_current, self.action = False, None

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        held, _target = await self.admission(session=writer, projection=_CardProjection(self) if self.captured is not None else None)
        if "agents:write" not in held:
            raise AgentDenied("The reply is unavailable")
        self.base.current()
        return current


class _CardProjection:
    def __init__(self, guard):
        self.guard = guard

    def extend(self, query):
        captured = self.guard.captured
        data = captured.route_basis["work_input"]
        work, request, parent, mark, callback, saved = AgentWorkRow, HumanInputRequestRow, TelegramOutboxRow, TelegramInboundMarkRow, TelegramCallbackRow, aliased(TelegramActRow)
        return (
            query.add_columns(work, request, parent, mark, callback, saved)
            .join(work, and_(work.id == data["work_id"], work.instance_id == TelegramBotLinkRow.agent_ref))
            .join(request, and_(request.id == data["request_id"], request.work_id == work.id, request.instance_id == TelegramBotLinkRow.agent_ref))
            .join(parent, parent.id == data["source_outbox_id"])
            .join(mark, and_(mark.link_id == captured.link_id, mark.chat_id == captured.chat_id, mark.message_id == captured.message_id))
            .outerjoin(callback, callback.group_id == data["group_id"])
            .outerjoin(saved, saved.id == captured.operation_ids[0])
        )

    def check(self, rows):
        work, request, parent, mark, _callback, saved = rows[0][8:14]
        guard, captured = self.guard, self.guard.captured
        controls = list({row[12].id: row[12] for row in rows if row[12] is not None}.values())
        data = reply_basis(guard.link, parent, mark, controls, consumed=True)
        if (
            not same_body(data, captured.route_basis["work_input"])
            or not same_body(mark.route_basis, captured.route_basis)
            or not same_body(mark.sanitized_input, captured.sanitized_input)
            or mark.operation_ids != captured.operation_ids
            or (mark.state, mark.route_kind, mark.route_target_id, mark.update_id, mark.route_complete) != (captured.state, captured.route_kind, captured.route_target_id, captured.update_id, captured.route_complete)
            or _aware(mark.accepted_at) != _aware(captured.accepted_at)
        ):
            raise CallbackRefused()
        if "work_action_id" in mark.route_basis and (mark.route_basis["work_action_id"] != captured.operation_ids[0] or saved is None):
            raise CallbackRefused()
        if guard.action is not None and (saved is None or any(getattr(saved, key) != getattr(guard.action, key) for key in _ACT_SCOPE) or not same_body(saved.body, guard.action.body)):
            raise CallbackRefused()
        if guard.require_current and (mark.route_complete or mark.state == "done"):
            raise CallbackRefused()
        if guard.require_current and (
            request.state == "closed" or not HumanInput._source_current(request, work) or (request.request_revision, request.assignment_revision) != (data["seen"]["request_revision"], data["seen"]["assignment_revision"])
        ):
            raise AgentConflict("The request changed; reload before replying")
