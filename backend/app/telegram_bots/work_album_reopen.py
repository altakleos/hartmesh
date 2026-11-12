"""Reopen displays a new prompt; the retained group is never its answer."""

from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import JSON, func, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_album_guard import normalized
from app.telegram_bots.work_album_handoff import projection_packet
from app.telegram_bots.work_album_recovery import cohort, recovery_basis, recovery_context, recovery_inputs
from app.telegram_bots.work_interaction_delivery import WorkInteractionPlans
from app.telegram_bots.work_reopen import identity
from app.telegram_bots.work_request_recovery import _RecoveryProjection
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class RecoveryGroupProof:
    def __init__(self, origin, *, sqlite):
        self.origin, self.sqlite = origin, sqlite

    def expression(self):
        row = aliased(TelegramInboundMarkRow)
        args = []
        for key in (
            "link_id",
            "chat_id",
            "binding_generation",
            "message_thread_id",
            "message_id",
            "update_id",
            "sanitized_input",
            "input_profile",
            "route_basis",
            "route_kind",
            "route_target_id",
            "operation_ids",
            "route_complete",
            "turn_id",
            "thread_id",
            "run_id",
            "outbox_id",
            "assistant_message_id",
            "delivery_ids",
            "state",
            "accepted_at",
            "closed_at",
        ):
            column = row.route_basis["input_profile"] if key == "input_profile" else getattr(row, key)
            args.extend((key, func.json(column) if self.sqlite and isinstance(column.type, JSON) else column))
        blocked = select(func.count()).select_from(TelegramActRow).where(TelegramActRow.id.in_(recovery_context(self.origin)["operation_ids"])).scalar_subquery()
        args.extend(("blocked_actions", blocked))
        source = aliased(TelegramOutboxRow)
        for name in ("request_id", "work_id"):
            value = (
                select(getattr(source, name))
                .where(
                    source.id == recovery_context(self.origin)["source_outbox_id"],
                    source.link_id == row.link_id,
                    source.chat_id == row.chat_id,
                    source.binding_generation == row.binding_generation,
                    source.message_thread_id.is_not_distinct_from(row.message_thread_id),
                )
                .correlate(row)
                .scalar_subquery()
            )
            args.extend(("source_" + name, value))
        packet = func.json_object(*args) if self.sqlite else func.json_build_object(*args)
        aggregate = func.json_group_array(packet, type_=JSON) if self.sqlite else func.json_agg(packet, type_=JSON)
        return select(aggregate).where(row.link_id == self.origin.link_id, row.chat_id == self.origin.chat_id, row.message_id.in_(recovery_context(self.origin)["member_ids"])).scalar_subquery()


def validate_reopened_group(link, parent, callbacks, packets, basis, source, outbox_id):
    reopened, origin = basis["reopen"], basis["origin"]
    context = reopened["album"]
    if type(packets) is not list or len(packets) != len(context["member_ids"]):
        raise ProductSourceRetired()
    indexed = {packet["message_id"]: packet for packet in packets}
    if set(indexed) != set(context["member_ids"]):
        raise ProductSourceRetired()
    ordered = [SimpleNamespace(**indexed[identity]) for identity in context["member_ids"]]
    if recovery_inputs(ordered) != context["inputs_sha256"] or recovery_basis(ordered) != context["basis_sha256"] or sorted({identity for mark in ordered for identity in mark.operation_ids}) != context["operation_ids"]:
        raise ProductSourceRetired()
    for mark in ordered:
        if (
            (mark.source_request_id, mark.source_work_id) != (context["request_id"], context["work_id"])
            or mark.blocked_actions
            or not same_body(recovery_context(mark), context)
            or mark.route_kind != "work_album_hold"
            or mark.state not in {"reply_pending", "done"}
            or mark.route_target_id != (context["request_id"] if "work_album" in mark.route_basis else context["source_outbox_id"])
            or mark.outbox_id != outbox_id
            or mark.operation_ids != ([mark.route_basis["work_album"]["operation_id"]] if "work_album" in mark.route_basis and mark.message_id == mark.route_basis["work_album"]["root_message_id"] else [])
            or not mark.route_complete
            or outbox_id not in mark.delivery_ids
            or "work_action_id" in mark.route_basis
            or any(getattr(mark, key) is not None for key in ("turn_id", "thread_id", "run_id", "assistant_message_id"))
        ):
            raise ProductSourceRetired()
    selected = next((row for row in callbacks if row.id == origin["callback_id"]), None)
    prompt = next((row for row in callbacks if row.id == reopened["prompt_id"]), None)
    first = ordered[0]
    scope = (link.id, link.chat_id, link.binding_generation, first.message_thread_id)
    if (
        selected is None
        or prompt is None
        or parent is None
        or parent.id != context["notice_id"]
        or (context["request_id"], context["work_id"]) != (source.request["id"], source.work["id"])
        or not same_body(identity(first), reopened["input"])
        or parent.id != origin["anchor_outbox_id"]
        or parent.payload["presentation_version"] != origin["presentation_version"]
        or parent.state != "sent"
        or parent.owner_user_id != link.owner_user_id
        or (parent.link_id, parent.chat_id, parent.binding_generation, parent.message_thread_id) != scope
        or parent.payload["product_basis"]["origins"] != sorted([[link.id, mark.message_id, link.chat_id] for mark in ordered])
        or selected.kind != "prompt_reopen"
        or selected.target_kind != "request"
        or selected.target_id != context["request_id"]
        or selected.payload.get("prompt_id") != prompt.id
        or selected.payload.get("message_id") != first.message_id
        or prompt.payload.get("work_request_id") != context["request_id"]
        or prompt.id != context["prompt_id"]
        or prompt.kind != "prompt"
        or prompt.payload.get("anchor_outbox_id") != context["source_outbox_id"]
        or (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id) != scope
    ):
        raise ProductSourceRetired()
    group = [row for row in callbacks if row.group_id == selected.group_id]
    _validate_rows(parent, group)
    if any(row.consumed_at is None or row.closed_reason != "work-preview" or "publication_owner" in row.payload or row.message_id != origin["message_id"] for row in group):
        raise ProductSourceRetired()
    part = next((part for part in parent.parts if part.get("callback_group_id") == selected.group_id and part["presentation_version"] == origin["presentation_version"]), None)
    if part is None or part["state"] != "sent" or part["message_ids"] != [origin["message_id"]]:
        raise ProductSourceRetired()


async def reopen_album(owner, channel, link, selected, origin, *, guard):
    async def perform():
        context = recovery_context(origin)
        try:
            source, settings = await owner.closed.recovery._source(link, context["request_id"])
        except (AgentDenied, ProductSourceRetired):
            return await owner.unavailable(channel, link, selected, origin, guard=guard)
        admission = await prepare_attention(owner.closed.plans.reader, link)
        composed = AlbumReopenGuard(guard, admission, source, origin, selected.id, sqlite=owner.callbacks.sessions.kw["bind"].dialect.name == "sqlite")
        async with owner.callbacks.writer(link, composed) as writer:
            group = await owner.callbacks.lock_current(link, selected.id, chat_id=selected.chat_id, message_id=selected.message_id, message_thread_id=selected.message_thread_id, guard=composed, session=writer)
            marks = await cohort(owner.inbox, writer, link, origin)
            parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
            prompt = await writer.get(TelegramCallbackRow, context["prompt_id"], populate_existing=True)
            if (
                parent.id != context["notice_id"]
                or prompt is None
                or prompt.payload.get("work_request_id") != context["request_id"]
                or group.selected.target_kind != "request"
                or group.selected.target_id != context["request_id"]
                or group.selected.kind != "prompt_reopen"
                or group.selected.payload.get("prompt_id") != context["prompt_id"]
                or group.selected.payload.get("message_id") != marks[0].message_id
            ):
                raise CallbackRefused()
            composed.originals = tuple(InboxRow.from_row(mark) for mark in marks)
            reopened = {"version": 2, "input": identity(marks[0]), "prompt_id": prompt.id, "album": deepcopy(context)}
            output = await WorkInteractionPlans(owner.closed.plans)._publish(
                writer,
                link,
                group,
                parent,
                source,
                settings,
                {"view": "prompt", "purpose": "answer" if source.request["purpose"] == "information" else "comment"},
                key="notice:work-album-reopen:" + selected.id,
                chat_id=selected.chat_id,
                message_id=selected.message_id,
                message_thread_id=selected.message_thread_id,
                guard=composed,
                reopen_origin=reopened,
            )
            await owner.inbox.register_delivery(composed.originals, outbox_id=output, primary=True, finish=True, session=writer)
            composed.completed = output
            composed.basis = {"reopen": reopened, "origin": {"callback_id": selected.id, "anchor_outbox_id": parent.id, "presentation_version": parent.payload["presentation_version"], "message_id": selected.message_id}}
            return True

    return await await_drained(perform())


class AlbumReopenGuard:
    def __init__(self, base, admission, source, origin, callback_id, *, sqlite):
        self.base, self.admission, self.source, self.origin, self.callback_id, self.sqlite = base, admission, source, origin, callback_id, sqlite
        self.completed, self.originals, self.basis = None, None, None

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        held, _target = await self.admission(session=writer, projection=AlbumReopenProjection(self))
        if "agents:write" not in held:
            raise AgentDenied("The request is unavailable")
        self.base.current()
        return current


class AlbumReopenProjection(_RecoveryProjection):
    def __init__(self, guard):
        super().__init__(guard.source)
        self.guard = guard

    def extend(self, query):
        guard = self.guard
        context = recovery_context(guard.origin)
        row = TelegramCallbackRow
        args = []
        for column in row.__table__.columns:
            args.extend((column.name, func.json(column) if guard.sqlite and isinstance(column.type, JSON) else column))
        aggregate = func.json_group_array(func.json_object(*args)) if guard.sqlite else func.json_agg(func.json_build_object(*args))
        group_id = select(row.group_id).where(row.id == guard.callback_id).scalar_subquery()
        controls = select(aggregate).where((row.group_id == group_id) | (row.id == context["prompt_id"])).scalar_subquery()
        parent = aliased(TelegramOutboxRow)
        return super().extend(query).add_columns(RecoveryGroupProof(guard.origin, sqlite=guard.sqlite).expression(), controls, parent).outerjoin(parent, parent.id == context["notice_id"])

    def check(self, rows):
        super().check(rows)
        from app.telegram_bots.work_request_source import _RequestProjection

        packets, controls, parent = rows[0][-3:]
        guard = self.guard
        if guard.originals is None:
            return
        if guard.completed is not None:
            validate_reopened_group(rows[0][0], parent, _RequestProjection.objects(controls), packets, guard.basis, guard.source, guard.completed)
            return
        indexed = {packet["message_id"]: packet for packet in packets or []}
        if len(indexed) != len(guard.originals):
            raise CallbackRefused()
        for origin in guard.originals:
            packet = indexed.get(origin.message_id)
            if packet is None or packet["route_complete"] or packet["blocked_actions"]:
                raise CallbackRefused()
            for key in ("accepted_at", "closed_at"):
                if type(packet.get(key)) is str:
                    packet[key] = datetime.fromisoformat(packet[key])
            if not same_body(normalized({key: packet[key] for key in projection_packet(origin) if key != "route_complete"}), normalized({key: value for key, value in projection_packet(origin).items() if key != "route_complete"})):
                raise CallbackRefused()
