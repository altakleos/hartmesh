"""An explicit new-message choice transfers actual files before chat publication."""

from copy import deepcopy
from dataclasses import replace

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused, _row
from app.telegram_bots.file_puts import _file_journal, file_act_id, retain_captionless_file, work_input_digest
from app.telegram_bots.inbox import InboxRow, chat_input_parts
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_input_lifetime import input_current
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramCallbackRow, TelegramInboundMarkRow
from deerflow.utils.file_io import await_drained


def handoff_basis(mark):
    value = mark.route_basis.get("work_file_handoff")
    if type(value) is not dict or set(value) != {"callback_id", "update_id"} or type(value["callback_id"]) is not str or len(value["callback_id"]) != 32 or type(value["update_id"]) is not int or not 0 <= value["update_id"] < 2**63:
        raise CallbackRefused()
    return value


async def recover_file_message(host, channel, link, origin, *, guard):
    async with host.callbacks.writer(link, guard) as writer:
        mark = (await host.inbox._rows(writer, [origin]))[0]
        host.inbox._current(link, mark)
        if mark.route_kind != "work_file_new" or mark.route_complete:
            raise CallbackRefused()
        handoff = handoff_basis(mark)
        callback = await writer.get(TelegramCallbackRow, handoff["callback_id"])
        if callback is None:
            raise CallbackRefused()
        saved = _row(callback)
    return await send_file_as_new(host, channel, link, saved, update_id=handoff["update_id"], guard=guard, recover=True)


class NewFileProjection:
    """A pending explicit handoff must survive through the native write."""

    def __init__(self, origin, callback):
        self.origin = origin
        self.callback = {key: deepcopy(getattr(callback, key)) for key in ("id", "group_id", "link_id", "chat_id", "binding_generation", "message_thread_id", "message_id", "kind", "payload", "closed_reason")}

    def extend(self, query):
        mark, callback = aliased(TelegramInboundMarkRow), aliased(TelegramCallbackRow)
        origin = self.origin
        return query.add_columns(mark, callback).outerjoin(mark, and_(mark.link_id == origin.link_id, mark.chat_id == origin.chat_id, mark.message_id == origin.message_id)).outerjoin(callback, callback.id == self.callback["id"])

    def check(self, rows):
        mark, callback = rows[0][-2:]
        if mark is None or callback is None or callback.consumed_at is not None or mark.route_complete or mark.route_kind != "work_file_new":
            raise CallbackRefused()
        captured = InboxRow.from_row(mark)
        for key in ("route_basis", "sanitized_input", "operation_ids", "route_kind", "route_target_id", "binding_generation", "message_thread_id", "update_id"):
            if not same_body(getattr(captured, key), getattr(self.origin, key)):
                raise CallbackRefused()
        if not same_body({key: getattr(callback, key) for key in self.callback}, self.callback):
            raise CallbackRefused()


async def new_file_projection(journal, link, origin, access):
    if "work_album_handoff" in origin.route_basis:
        from app.telegram_bots.work_album_handoff import new_album_projection

        return await new_album_projection(journal, link, origin, access)
    async with journal.callbacks.writer(link, access.guard) as writer:
        mark = (await journal.inbox._rows(writer, [origin]))[0]
        journal.inbox._publication_snapshot_matches(mark, origin)
        handoff = handoff_basis(mark)
        callback = await writer.get(TelegramCallbackRow, handoff.get("callback_id"))
        if mark.route_kind != "work_file_new" or mark.route_complete or callback is None:
            raise CallbackRefused()
        group = await journal.callbacks.lock_current(link, callback.id, chat_id=link.chat_id, message_id=callback.message_id, message_thread_id=mark.message_thread_id, guard=access.guard, session=writer)
        if callback.kind != "prompt_send_new" or group.anchor_outbox_id != mark.route_basis["work_closed"]["notice_id"] or callback.payload.get("message_id") != mark.message_id or mark.operation_ids:
            raise CallbackRefused()
        return NewFileProjection(origin, callback)


async def send_file_as_new(host, channel, link, row, *, update_id, guard, recover=False):
    if type(update_id) is not int or not 0 <= update_id < 2**63:
        raise CallbackRefused()

    async def perform():
        async with host.callbacks.writer(link, guard) as writer:
            group = await host.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            selected = group.selected
            mark = await writer.get(TelegramInboundMarkRow, (link.id, selected.payload.get("message_id"), link.chat_id), with_for_update=True, populate_existing=True)
            if mark is None:
                raise CallbackRefused()
            host.inbox._current(link, mark)
            closed = mark.route_basis.get("work_closed", {})
            if selected.kind != "prompt_send_new" or mark.message_thread_id != selected.message_thread_id or closed.get("notice_id") != group.anchor_outbox_id or closed.get("prompt_id") != selected.payload.get("prompt_id"):
                raise CallbackRefused()
            previous = handoff_basis(mark) if "work_file_handoff" in mark.route_basis else None
            if previous is not None and previous["callback_id"] != selected.id:
                raise CallbackRefused()
            owned_update = previous["update_id"] if previous is not None else update_id
            publishing = [member for member in group.rows if "publication_owner" in member.payload]
            if publishing:
                if not any(member.id == selected.id for member in publishing):
                    raise CallbackRefused()
                stage = await host.inbox.stage_prompt_message(
                    link, callback_id=selected.id, update_id=owned_update, chat_id=selected.chat_id, message_id=selected.message_id, message_thread_id=selected.message_thread_id, callbacks=host.callbacks, guard=guard, session=writer
                )
                origin = None
            else:
                if mark.route_complete or mark.route_kind not in {"work_card_hold", "work_file_new"} or mark.operation_ids:
                    raise CallbackRefused()
                handoff = {"callback_id": selected.id, "update_id": owned_update}
                if mark.route_kind == "work_file_new" and not same_body(mark.route_basis.get("work_file_handoff"), handoff):
                    raise CallbackRefused()
                _text, files = chat_input_parts(mark, bot_id=link.bot_id)
                for file in files:
                    native = await writer.get(TelegramActRow, file_act_id(link, file), with_for_update=True, populate_existing=True)
                    if native is None or native.continuation.get("version") != 4:
                        continue
                    _file_journal(native)
                    if any(getattr(native, key) != value for key, value in host.services.file_puts._scope(link).items()) or native.message_id != mark.message_id:
                        raise CallbackRefused()
                    work = native.continuation["work"]
                    captured = replace(InboxRow.from_row(mark), operation_ids=[work["operation_id"]])
                    if (
                        native.state not in {"done", "refused", "uncertain"}
                        or not same_body(native.continuation["file"], file)
                        or work["input_sha256"] != work_input_digest(captured)
                        or await writer.get(TelegramActRow, work["operation_id"]) is not None
                    ):
                        raise CallbackRefused()
                    # The press changes the purpose, never the native write's
                    # identity, body, selected child or original storage target.
                    native.continuation = {**{key: value for key, value in native.continuation.items() if key != "work"}, "version": 3}
                    if native.state == "done" and native.continuation["input"]["captionless"]:
                        await retain_captionless_file(writer, link, native, host.callbacks.clock())
                mark.route_kind, mark.route_target_id = "work_file_new", link.agent_ref
                mark.route_basis = {**mark.route_basis, "work_file_handoff": handoff, "input_profile": {"version": 2, "kind": "chat_input", "forwarded": False, "origin": None}}
                await writer.flush()
                origin, stage = InboxRow.from_row(mark), None
        if origin is not None:
            from app.telegram_bots.file_outcomes import FileOutcomes

            _text, files = chat_input_parts(origin, bot_id=link.bot_id)
            saved = []
            for file in files:
                if recover:
                    receipt = await host.services.file_puts.reconcile_input(link, origin, file, origins=None, authority=host.services.authority, current=lambda: input_current(channel, link, guard))
                else:
                    receipt = await channel._own_instance_file(link, origin, file)
                if receipt.state not in {"done", "refused", "uncertain"}:
                    raise CallbackRefused()
                await FileOutcomes(host.services).plan(channel, link, origin, file, current=lambda: input_current(channel, link, guard))
                if receipt.state == "done":
                    saved.append((origin, file))
            if not _text.strip():

                async def settle(writer):
                    selected = await host.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                    mark = (await host.inbox._rows(writer, [origin]))[0]
                    host.inbox._publication_snapshot_matches(mark, origin)
                    if mark.route_complete or mark.route_kind != "work_file_new":
                        raise CallbackRefused()
                    await host.callbacks.consume_group(selected, session=writer)
                    await host.prompts.close(link, link.chat_id, row.message_thread_id, reason="navigation", guard=guard, session=writer)

                if saved:
                    await FileOutcomes(host.services).question(channel, link, saved, current=lambda: input_current(channel, link, guard), _settle=settle)
                else:
                    async with host.callbacks.writer(link, guard) as writer:
                        await settle(writer)
                        await host.inbox.finish_route(origin, session=writer)
                return True
            async with host.callbacks.writer(link, guard) as writer:
                group = await host.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                mark = (await host.inbox._rows(writer, [origin]))[0]
                host.inbox._publication_snapshot_matches(mark, origin)
                if mark.route_kind != "work_file_new" or mark.route_complete or group.selected.kind != "prompt_send_new":
                    raise CallbackRefused()
                mark.route_kind = "turn"
                await writer.flush()
                stage = await host.inbox.stage_prompt_message(
                    link, callback_id=row.id, update_id=owned_update, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, callbacks=host.callbacks, guard=guard, session=writer
                )
                await host.prompts.close(link, link.chat_id, row.message_thread_id, reason="navigation", guard=guard, session=writer)
        guard.current()
        await host.services.turns.accept_staged(channel, stage, admission_current=lambda: input_current(channel, link, guard))
        return True

    return await await_drained(perform())
