"""A Reopen press shows a fresh Work prompt without using the retained answer."""

from copy import deepcopy

from sqlalchemy import JSON, and_, func, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _aware, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_interaction_delivery import WorkInteractionPlans
from app.telegram_bots.work_request_recovery import _RecoveryProjection
from app.telegram_bots.work_typing import PURPOSES
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def identity(mark):
    return {key: getattr(mark, key) for key in ("link_id", "chat_id", "message_id", "update_id", "binding_generation")}


def validate_reopen(value):
    if type(value) is not dict or set(value) != ({"version", "input", "prompt_id", "album"} if value.get("version") == 2 else {"version", "input", "prompt_id"}) or type(value["version"]) is not int or value["version"] not in {1, 2}:
        raise ProductSourceRetired()
    origin = value["input"]
    if type(origin) is not dict or set(origin) != {"link_id", "chat_id", "message_id", "update_id", "binding_generation"}:
        raise ProductSourceRetired()
    if any(type(origin[key]) is not int or not 0 <= origin[key] < 2**63 for key in ("chat_id", "message_id", "update_id", "binding_generation")):
        raise ProductSourceRetired()
    if type(origin["link_id"]) is not str or type(value["prompt_id"]) is not str or len(value["prompt_id"]) != 32:
        raise ProductSourceRetired()
    if value["version"] == 2:
        from types import SimpleNamespace

        from app.telegram_bots.work_album_recovery import recovery_context

        recovery_context(SimpleNamespace(route_basis={"work_album_recovery": value["album"]}, message_id=origin["message_id"]))
    return value


def check_reopen_origin(link, parent, callbacks, mark, basis, source, outbox_id):
    from app.telegram_bots.work_closed_inputs import validate_closed_voice

    reopened = validate_reopen(basis["reopen"])
    if reopened["version"] == 2:
        from app.telegram_bots.work_album_reopen import validate_reopened_group

        return validate_reopened_group(link, parent, callbacks, mark, basis, source, outbox_id)
    origin = basis["origin"]
    selected = next((row for row in callbacks if row.id == origin["callback_id"]), None)
    old_prompt = next((row for row in callbacks if row.id == reopened["prompt_id"]), None)
    if mark is None or parent is None or selected is None or old_prompt is None:
        raise ProductSourceRetired()
    validate_closed_voice(parent, mark)
    closed = mark.route_basis.get("work_closed", {})
    scope = (link.id, link.chat_id, link.binding_generation, mark.message_thread_id)
    if (
        not same_body(identity(mark), reopened["input"])
        or mark.route_kind != "work_card_hold"
        or not mark.route_complete
        or mark.operation_ids
        or mark.thread_id is not None
        or mark.turn_id is not None
        or outbox_id not in mark.delivery_ids
        or closed.get("notice_id") != parent.id
        or closed.get("prompt_id") != old_prompt.id
        or (closed.get("request_id"), closed.get("work_id")) != (source.request["id"], source.work["id"])
        or selected.kind != "prompt_reopen"
        or selected.payload.get("prompt_id") != old_prompt.id
        or selected.payload.get("message_id") != mark.message_id
        or old_prompt.kind != "prompt"
        or old_prompt.payload.get("anchor_outbox_id") != closed.get("source_outbox_id")
        or (old_prompt.link_id, old_prompt.chat_id, old_prompt.binding_generation, old_prompt.message_thread_id) != scope
        or (parent.link_id, parent.chat_id, parent.binding_generation, parent.message_thread_id) != scope
        or parent.owner_user_id != link.owner_user_id
        or parent.id != origin["anchor_outbox_id"]
        or parent.payload["presentation_version"] != origin["presentation_version"]
        or parent.state != "sent"
        or parent.payload.get("product_basis", {}).get("origins") != [[link.id, mark.message_id, mark.chat_id]]
    ):
        raise ProductSourceRetired()
    group = [row for row in callbacks if row.group_id == selected.group_id]
    _validate_rows(parent, group)
    if any(row.consumed_at is None or row.closed_reason != "work-preview" or "publication_owner" in row.payload or row.message_id != origin["message_id"] for row in group):
        raise ProductSourceRetired()
    part = next((part for part in parent.parts if part.get("callback_group_id") == selected.group_id and part["presentation_version"] == origin["presentation_version"]), None)
    if part is None or part["state"] != "sent" or part["message_ids"] != [origin["message_id"]]:
        raise ProductSourceRetired()


class WorkReopen:
    def __init__(self, closed):
        self.closed, self.interactions = closed, WorkInteractionPlans(closed.plans)

    async def open(self, channel, link, selected, origin, *, guard):
        async def perform():
            closed = origin.route_basis["work_closed"]
            try:
                source, settings = await self.closed.recovery._source(link, closed["request_id"])
            except (AgentDenied, ProductSourceRetired):
                return await self.closed.unavailable(link, selected, guard=guard)
            admission = await prepare_attention(self.closed.plans.reader, link)
            composed = _ReopenGuard(guard, admission, source, origin, selected.id, sqlite=self.closed.callbacks.sessions.kw["bind"].dialect.name == "sqlite")
            async with self.closed.callbacks.writer(link, composed) as writer:
                group = await self.closed.callbacks.lock_current(link, selected.id, chat_id=selected.chat_id, message_id=selected.message_id, message_thread_id=selected.message_thread_id, guard=composed, session=writer)
                parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
                prompt = await writer.get(TelegramCallbackRow, closed["prompt_id"], populate_existing=True)
                if parent.id != closed["notice_id"] or prompt is None or prompt.payload.get("anchor_outbox_id") != closed["source_outbox_id"]:
                    raise CallbackRefused()
                composed.parent = _item(parent)
                composed.prompt_basis = prompt_basis(prompt)
                purpose = next((key for key, value in PURPOSES.items() if value == prompt.payload.get("purpose")), None)
                state = {"view": "prompt", "purpose": purpose or ("answer" if source.request["purpose"] == "information" else "comment")}
                if state["purpose"] == "decision_note":
                    state["decision"] = prompt.payload["interaction"]["decision"]
                output = await self.interactions._publish(
                    writer,
                    link,
                    group,
                    parent,
                    source,
                    settings,
                    state,
                    key="notice:work-reopen:" + selected.id,
                    chat_id=selected.chat_id,
                    message_id=selected.message_id,
                    message_thread_id=selected.message_thread_id,
                    guard=composed,
                    reopen_origin={"version": 1, "input": identity(origin), "prompt_id": prompt.id},
                )
                await self.closed.inbox.register_delivery([origin], outbox_id=output, primary=True, finish=True, session=writer)
                composed.completed = output
                return output

        return await await_drained(perform())


def prompt_basis(prompt):
    return deepcopy({key: prompt.payload.get(key) for key in ("purpose", "anchor_outbox_id", "interaction", "request_id", "work_id")})


class _ReopenGuard:
    def __init__(self, base, admission, source, origin, callback_id, *, sqlite):
        self.base, self.admission, self.source, self.origin, self.callback_id = base, admission, source, origin, callback_id
        self.sqlite, self.parent, self.prompt_basis = sqlite, None, None
        self.completed = None

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        held, _target = await self.admission(session=writer, projection=_ReopenProjection(self))
        if "agents:write" not in held:
            raise AgentDenied("The request is unavailable")
        self.base.current()
        return current


class _ReopenProjection(_RecoveryProjection):
    def __init__(self, guard):
        super().__init__(guard.source)
        self.guard = guard

    def extend(self, query):
        mark, callback, parent = aliased(TelegramInboundMarkRow), aliased(TelegramCallbackRow), aliased(TelegramOutboxRow)
        origin = self.guard.origin
        closed = origin.route_basis["work_closed"]
        control = TelegramCallbackRow
        args = []
        for column in control.__table__.columns:
            args.extend((column.name, func.json(column) if self.guard.sqlite and isinstance(column.type, JSON) else column))
        aggregate = func.json_group_array(func.json_object(*args)) if self.guard.sqlite else func.json_agg(func.json_build_object(*args))
        group_id = select(control.group_id).where(control.id == self.guard.callback_id).scalar_subquery()
        controls = select(aggregate).where(or_(control.group_id == group_id, control.id == closed["prompt_id"])).scalar_subquery()
        return (
            super()
            .extend(query)
            .add_columns(mark, callback, parent, controls)
            .outerjoin(mark, and_(mark.link_id == origin.link_id, mark.chat_id == origin.chat_id, mark.message_id == origin.message_id))
            .outerjoin(callback, callback.id == self.guard.callback_id)
            .outerjoin(parent, parent.id == closed["notice_id"])
        )

    def check(self, rows):
        super().check(rows)
        mark, callback = rows[0][11:13]
        original, completed = self.guard.origin, self.guard.completed
        if (
            mark is None
            or callback is None
            or not same_body(identity(mark), identity(original))
            or mark.message_thread_id != original.message_thread_id
            or _aware(mark.accepted_at) != _aware(original.accepted_at)
            or mark.state != original.state
            or not same_body(mark.sanitized_input, original.sanitized_input)
            or not same_body(mark.route_basis, original.route_basis)
            or mark.route_kind != "work_card_hold"
            or mark.route_target_id != original.route_target_id
            or mark.operation_ids
            or mark.thread_id is not None
            or mark.turn_id is not None
            or mark.route_complete is not bool(completed)
            or completed is not None
            and completed not in mark.delivery_ids
            or callback.kind != "prompt_reopen"
            or callback.payload.get("prompt_id") != original.route_basis["work_closed"]["prompt_id"]
            or callback.payload.get("message_id") != original.message_id
            or "publication_owner" in callback.payload
            or (callback.consumed_at is not None) is not bool(completed)
            or completed is not None
            and callback.closed_reason != "work-preview"
        ):
            raise CallbackRefused()
        if self.guard.parent is None:
            return
        from app.telegram_bots.work_request_source import _RequestProjection

        parent, controls = rows[0][13], _RequestProjection.objects(rows[0][14])
        prompt = next((row for row in controls if row.id == original.route_basis["work_closed"]["prompt_id"]), None)
        if parent is None or _item(parent) != self.guard.parent or prompt is None or not same_body(prompt_basis(prompt), self.guard.prompt_basis):
            raise CallbackRefused()
        group = [row for row in controls if row.group_id == callback.group_id]
        _validate_rows(parent, group)
        if any("publication_owner" in row.payload or (row.consumed_at is not None) is not bool(completed) or completed is not None and row.closed_reason != "work-preview" for row in group):
            raise CallbackRefused()
        if completed is not None:
            basis = {
                "reopen": {"version": 1, "input": identity(original), "prompt_id": prompt.id},
                "origin": {"callback_id": callback.id, "anchor_outbox_id": parent.id, "presentation_version": parent.payload["presentation_version"], "message_id": callback.message_id},
            }
            check_reopen_origin(rows[0][0], parent, controls, mark, basis, self.source, completed)
