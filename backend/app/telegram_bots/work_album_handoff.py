"""One explicit new-message choice transfers a complete retained album."""

from copy import deepcopy

from sqlalchemy import JSON, func, select
from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.file_outcomes import FileOutcomes
from app.telegram_bots.file_puts import _file_journal, file_act_id, retain_captionless_file
from app.telegram_bots.inbox import InboxRow, chat_input_parts
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_album_guard import AlbumGuard, normalized
from app.telegram_bots.work_album_inputs import album_input_digest, input_packet
from app.telegram_bots.work_album_recovery import cohort, recovery_basis, recovery_context, recovery_inputs
from app.telegram_bots.work_input_lifetime import input_current
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def handoff(mark):
    value = mark.route_basis.get("work_album_handoff")
    if (
        type(value) is not dict
        or set(value) != {"version", "callback_id", "update_id"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or type(value["callback_id"]) is not str
        or len(value["callback_id"]) != 32
        or type(value["update_id"]) is not int
        or not 0 <= value["update_id"] < 2**63
    ):
        raise CallbackRefused()
    return value


async def handoff_members(inbox, writer, link, origin):
    context = recovery_context(origin)
    row = TelegramInboundMarkRow
    rows = list(await writer.scalars(select(row).where(row.link_id == link.id, row.chat_id == link.chat_id, row.message_id.in_(context["member_ids"])).with_for_update().execution_options(populate_existing=True)))
    by_id = {row.message_id: row for row in rows}
    if set(by_id) != set(context["member_ids"]):
        raise CallbackRefused()
    rows = [by_id[identity] for identity in context["member_ids"]]
    for mark in rows:
        inbox._current(link, mark)
        if (
            mark.route_kind not in {"work_file_new", "turn"}
            or mark.route_target_id != link.agent_ref
            or mark.operation_ids
            or mark.route_complete
            or any(getattr(mark, key) is not None for key in ("turn_id", "thread_id", "run_id", "outbox_id", "assistant_message_id"))
        ):
            raise CallbackRefused()
        if not same_body(mark.route_basis.get("work_album_recovery"), context) or not same_body(handoff(mark), handoff(origin)):
            raise CallbackRefused()
    if recovery_inputs(rows) != context["inputs_sha256"] or recovery_basis(rows) != context["basis_sha256"]:
        raise CallbackRefused()
    return rows


async def validate_staged_group(inbox, writer, link, group, marks, update_id):
    if not marks:
        raise CallbackRefused()
    origins = await handoff_members(inbox, writer, link, InboxRow.from_row(marks[0]))
    first, selected = origins[0], group.selected
    context, choice = first.route_basis["work_album_recovery"], handoff(first)
    parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id)
    if (selected.kind, selected.id, update_id, selected.payload.get("message_id"), selected.payload.get("prompt_id")) != ("prompt_send_new", choice["callback_id"], choice["update_id"], first.message_id, context["prompt_id"]):
        raise CallbackRefused()
    if (
        parent is None
        or parent.id != context["notice_id"]
        or parent.payload["product_basis"].get("origins") != sorted([[link.id, mark.message_id, link.chat_id] for mark in origins])
        or parent.payload["product_basis"].get("finish_route") is not False
        or any(mark.route_kind != "turn" for mark in origins)
    ):
        raise CallbackRefused()
    if any(part.get("source_ref", {}).get("key") not in {"prompt.closed", "prompt.unavailable", "ask.answer_too_long"} for part in parent.payload["product_basis"].get("parts", [])):
        raise CallbackRefused()
    return tuple(InboxRow.from_row(mark) for mark in origins)


def projection_packet(mark):
    return {
        **input_packet(mark),
        **{
            key: deepcopy(getattr(mark, key))
            for key in ("route_basis", "route_kind", "route_target_id", "operation_ids", "route_complete", "turn_id", "thread_id", "run_id", "outbox_id", "assistant_message_id", "accepted_at", "closed_at", "state", "delivery_ids")
        },
    }


class NewAlbumFileProjection:
    def __init__(self, origins, callback, controls, parent, *, sqlite):
        self.origins, self.sqlite = origins, sqlite
        self.controls = normalized([{column.name: thaw(getattr(control, column.name)) for column in TelegramCallbackRow.__table__.columns} for control in controls])
        self.parent = normalized({column.name: deepcopy(getattr(parent, "class_" if column.name == "class" else column.name)) for column in parent.__table__.columns})
        self.callback = {key: deepcopy(getattr(callback, key)) for key in ("id", "group_id", "link_id", "chat_id", "binding_generation", "message_thread_id", "message_id", "kind", "payload", "closed_reason")}

    def extend(self, query):
        row, control = aliased(TelegramInboundMarkRow), aliased(TelegramCallbackRow)
        args = []
        for name in projection_packet(self.origins[0]):
            column = row.route_basis["input_profile"] if name == "input_profile" else getattr(row, name)
            args.extend((name, func.json(column) if self.sqlite and isinstance(column.type, JSON) else column))
        packet = func.json_object(*args) if self.sqlite else func.json_build_object(*args)
        aggregate = func.json_group_array(packet, type_=JSON) if self.sqlite else func.json_agg(packet, type_=JSON)
        first = self.origins[0]
        members = select(aggregate).where(row.link_id == first.link_id, row.chat_id == first.chat_id, row.message_id.in_([origin.message_id for origin in self.origins])).scalar_subquery()
        sibling, parent = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow)
        args = []
        for column in TelegramCallbackRow.__table__.columns:
            field = getattr(sibling, column.name)
            args.extend((column.name, func.json(field) if self.sqlite and isinstance(column.type, JSON) else field))
        packet = func.json_object(*args) if self.sqlite else func.json_build_object(*args)
        aggregate = func.json_group_array(packet, type_=JSON) if self.sqlite else func.json_agg(packet, type_=JSON)
        controls = select(aggregate).where(sibling.group_id == self.callback["group_id"]).scalar_subquery()
        operations = recovery_context(self.origins[0])["operation_ids"]
        blocked = select(func.count()).select_from(TelegramActRow).where(TelegramActRow.id.in_(operations)).scalar_subquery()
        return query.add_columns(members, control, controls, parent, blocked).outerjoin(control, control.id == self.callback["id"]).outerjoin(parent, parent.id == self.parent["id"])

    def check(self, rows):
        from datetime import datetime

        packets, callback, controls, parent, blocked = rows[0][-5:]
        if blocked or parent is None or not same_body(normalized({column.name: deepcopy(getattr(parent, "class_" if column.name == "class" else column.name)) for column in parent.__table__.columns}), self.parent):
            raise CallbackRefused()
        if type(controls) is not list:
            raise CallbackRefused()
        for control in controls:
            for key in ("created_at", "expires_at", "consumed_at"):
                if type(control.get(key)) is str:
                    control[key] = datetime.fromisoformat(control[key])
        if not same_body(sorted(normalized(controls), key=lambda row: row["id"]), sorted(self.controls, key=lambda row: row["id"])):
            raise CallbackRefused()
        if callback is None or callback.consumed_at is not None or not same_body({key: getattr(callback, key) for key in self.callback}, self.callback):
            raise CallbackRefused()
        if type(packets) is not list or len(packets) != len(self.origins):
            raise CallbackRefused()
        expected = {origin.message_id: projection_packet(origin) for origin in self.origins}
        for packet in packets:
            # SQL JSON encodes SQLite booleans as 0/1, while their database
            # columns still retain the exact false requirement.
            if packet["route_complete"] not in (False, 0):
                raise CallbackRefused()
            packet["route_complete"] = False
            for key in ("accepted_at", "closed_at"):
                if type(packet.get(key)) is str:
                    packet[key] = datetime.fromisoformat(packet[key])
            if not same_body(normalized(packet), normalized(expected.get(packet["message_id"]))):
                raise CallbackRefused()


async def new_album_projection(journal, link, origin, access):
    async with journal.callbacks.writer(link, access.guard) as writer:
        marks = await handoff_members(journal.inbox, writer, link, origin)
        choice = handoff(marks[0])
        selected = await writer.get(TelegramCallbackRow, choice["callback_id"])
        if selected is None:
            raise CallbackRefused()
        group = await journal.callbacks.lock_current(link, selected.id, chat_id=link.chat_id, message_id=selected.message_id, message_thread_id=origin.message_thread_id, guard=access.guard, session=writer)
        context = recovery_context(origin)
        if selected.kind != "prompt_send_new" or group.anchor_outbox_id != context["notice_id"] or selected.payload.get("prompt_id") != context["prompt_id"] or selected.payload.get("message_id") != marks[0].message_id:
            raise CallbackRefused()
        if any(row.consumed_at is not None or "publication_owner" in row.payload for row in group.rows):
            raise CallbackRefused()
        parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id, populate_existing=True)
        return NewAlbumFileProjection(tuple(InboxRow.from_row(mark) for mark in marks), selected, group.rows, parent, sqlite=writer.get_bind().dialect.name == "sqlite")


async def send_album_as_new(owner, channel, link, row, *, update_id, guard, recover=False):
    async def perform():
        actual_guard = guard
        async with owner.callbacks.writer(link, actual_guard) as writer:
            selected = await writer.get(TelegramCallbackRow, row.id)
            if selected is None:
                raise CallbackRefused()
            if "publication_owner" not in selected.payload:
                mark = await writer.get(TelegramInboundMarkRow, (link.id, selected.payload.get("message_id"), link.chat_id))
                if mark is None:
                    raise CallbackRefused()
                initial = await (handoff_members if "work_album_handoff" in mark.route_basis else cohort)(owner.inbox, writer, link, InboxRow.from_row(mark))
                actual_guard = AlbumGuard(guard, [InboxRow.from_row(mark) for mark in initial], reader=owner.closed.plans.reader, callback_id=row.id)
        async with owner.callbacks.writer(link, actual_guard) as writer:
            selected = await writer.get(TelegramCallbackRow, row.id)
            if selected is None:
                raise CallbackRefused()
            if "publication_owner" in selected.payload:
                stage = await owner.inbox.stage_held_group(link, callback_id=row.id, update_id=update_id, origins=(), callbacks=owner.callbacks, guard=actual_guard, session=writer)
                origins = None
            else:
                group = await owner.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=actual_guard, session=writer)
                mark = await writer.get(TelegramInboundMarkRow, (link.id, selected.payload.get("message_id"), link.chat_id))
                if mark is None:
                    raise CallbackRefused()
                origins = await (handoff_members if "work_album_handoff" in mark.route_basis else cohort)(owner.inbox, writer, link, InboxRow.from_row(mark))
                context = origins[0].route_basis["work_album_recovery"]
                if (
                    selected.kind != "prompt_send_new"
                    or selected.payload.get("prompt_id") != context["prompt_id"]
                    or selected.payload.get("message_id") != origins[0].message_id
                    or group.anchor_outbox_id != context["notice_id"]
                    or any("publication_owner" in member.payload for member in group.rows)
                ):
                    raise CallbackRefused()
                previous = handoff(origins[0]) if "work_album_handoff" in origins[0].route_basis else None
                choice = previous or {"version": 1, "callback_id": row.id, "update_id": update_id}
                if choice["callback_id"] != row.id or type(choice["update_id"]) is not int or not 0 <= choice["update_id"] < 2**63:
                    raise CallbackRefused()
                for mark in origins:
                    file = mark.sanitized_input["files"][0]
                    native = await writer.get(TelegramActRow, file_act_id(link, file), with_for_update=True, populate_existing=True)
                    if native is not None and native.continuation.get("version") == 4:
                        _file_journal(native)
                        work = native.continuation["work"]
                        if (
                            work["version"] != 2
                            or not same_body(work["album"], mark.route_basis.get("work_album"))
                            or work["input_sha256"] != album_input_digest(mark)
                            or not same_body(native.continuation["file"], file)
                            or native.state not in {"done", "refused", "uncertain"}
                            or any(getattr(native, key) != value for key, value in owner.services.file_puts._scope(link).items())
                            or native.message_id != mark.message_id
                            or await writer.get(TelegramActRow, work["operation_id"]) is not None
                        ):
                            raise CallbackRefused()
                        native.continuation = {**{key: value for key, value in native.continuation.items() if key != "work"}, "version": 3}
                        if native.state == "done" and native.continuation["input"]["captionless"]:
                            await retain_captionless_file(writer, link, native, owner.callbacks.clock())
                    mark.route_kind, mark.route_target_id, mark.operation_ids = "work_file_new", link.agent_ref, []
                    mark.route_basis = {**mark.route_basis, "work_album_handoff": choice}
                actual_guard.expect([InboxRow.from_row(mark) for mark in origins])
                await writer.flush()
                origins = tuple(InboxRow.from_row(mark) for mark in origins)
                stage = None
        if origins is not None:
            saved = []
            for origin in origins:
                file = thaw(origin.sanitized_input["files"][0])
                if recover:
                    receipt = await owner.services.file_puts.reconcile_input(link, origin, file, origins=None, authority=owner.services.authority, current=lambda: input_current(channel, link, actual_guard))
                else:
                    receipt = await channel._own_instance_file(link, origin, file)
                if receipt.state not in {"done", "refused", "uncertain"}:
                    raise CallbackRefused()
                await FileOutcomes(owner.services).plan(channel, link, origin, file, current=lambda: input_current(channel, link, actual_guard))
                if receipt.state == "done":
                    saved.append((origin, file))
            if not any(chat_input_parts(origin)[0].strip() for origin in origins):

                async def settle(writer):
                    marks = await handoff_members(owner.inbox, writer, link, origins[0])
                    group = await owner.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=actual_guard, session=writer)
                    actual_guard.allow_finish = True
                    await owner.callbacks.consume_group(group, session=writer)
                    saved_ids = {origin.message_id for origin, _file in saved}
                    for mark in marks:
                        if mark.message_id not in saved_ids:
                            await owner.inbox.finish_route(InboxRow.from_row(mark), session=writer)
                    actual_guard.expect([InboxRow.from_row(mark) for mark in marks])
                    await actual_guard.check(writer)

                if saved:
                    await FileOutcomes(owner.services).question(channel, link, saved, current=lambda: input_current(channel, link, actual_guard), _settle=settle, _explicit_group=origins)
                else:
                    async with owner.callbacks.writer(link, actual_guard) as writer:
                        await settle(writer)
                return True
            async with owner.callbacks.writer(link, actual_guard) as writer:
                marks = await handoff_members(owner.inbox, writer, link, origins[0])
                for mark in marks:
                    mark.route_kind = "turn"
                actual_guard.expect([InboxRow.from_row(mark) for mark in marks])
                actual_guard.allow_finish = True
                await writer.flush()
                stage = await owner.inbox.stage_held_group(
                    link, callback_id=row.id, update_id=handoff(marks[0])["update_id"], origins=[InboxRow.from_row(mark) for mark in marks], callbacks=owner.callbacks, guard=actual_guard, session=writer
                )
                actual_guard.expect([InboxRow.from_row(mark) for mark in marks])
                actual_guard.allow_finish = False
        actual_guard.current()
        await owner.services.turns.accept_staged(channel, stage, admission_current=lambda: input_current(channel, link, actual_guard))
        return True

    return await await_drained(perform())
