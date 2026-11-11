"""Retain accepted text and its shown Work prompt before any domain submission."""

from uuid import uuid4

from pydantic import TypeAdapter

from app.telegram_bots.callbacks import CallbackRefused, Prompts, _aware
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_typing import validate_shown_prompt
from deerflow.agent_instances.work_contract import Statement
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

_TEXT = TypeAdapter(Statement)


class WorkInputCaptures:
    def __init__(self, inbox, callbacks):
        self.inbox, self.callbacks = inbox, callbacks
        if inbox.session_factory.kw.get("bind") is not callbacks.sessions.kw.get("bind"):
            raise ValueError("work-input-requires-shared-storage")

    async def capture(self, link, origin, *, prompt_id, guard):
        """Own the input route and close its shown prompt atomically; do not act."""

        async def perform():
            async with self.callbacks.writer(link, guard) as writer:
                if origin.link_id != link.id:
                    raise CallbackRefused()
                mark = (await self.inbox._rows(writer, [origin]))[0]
                self.inbox._current(link, mark)
                raw = mark.sanitized_input
                if set(raw) - {"text", "type", "reply_to_message_id"} or raw.get("type", "text") != "text":
                    raise CallbackRefused()
                text = _TEXT.validate_python(raw.get("text"))
                prompt = await writer.get(TelegramCallbackRow, prompt_id, populate_existing=True)
                if prompt is None or prompt.kind != "prompt" or (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id) != (link.id, mark.chat_id, mark.binding_generation, mark.message_thread_id):
                    raise CallbackRefused()
                parent = await writer.get(TelegramOutboxRow, prompt.payload.get("anchor_outbox_id"), with_for_update=True, populate_existing=True)
                # Match callback publication's anchor-before-control lock order.
                prompt = await writer.get(TelegramCallbackRow, prompt_id, with_for_update=True, populate_existing=True)
                if prompt is None:
                    raise CallbackRefused()
                source = acknowledged_source(link, parent)
                acknowledged_at = validate_shown_prompt(_item(parent), prompt)
                if raw.get("reply_to_message_id") not in {None, prompt.message_id}:
                    raise CallbackRefused()
                basis = {
                    "version": 1,
                    "prompt_id": prompt.id,
                    "source_outbox_id": parent.id,
                    "presentation_version": parent.payload["presentation_version"],
                    "request_id": source.request["id"],
                    "work_id": source.work["id"],
                    "purpose": prompt.payload["purpose"],
                    "interaction": prompt.payload["interaction"],
                    "seen": source.seen,
                    "text": text,
                }
                if mark.route_kind is not None:
                    if mark.route_kind != "work_input" or mark.route_target_id != source.request["id"] or not same_body(mark.route_basis.get("work_input"), basis) or len(mark.operation_ids) != 1:
                        raise CallbackRefused()
                    return InboxRow.from_row(mark)
                self.callbacks._usable(prompt)
                if raw.get("reply_to_message_id") is None and (_aware(mark.accepted_at) < acknowledged_at or mark.message_id <= prompt.message_id):
                    raise CallbackRefused()
                current = await Prompts(self.callbacks).current(link, mark.chat_id, mark.message_thread_id, guard=guard, session=writer)
                if current is None or current.id != prompt_id:
                    raise CallbackRefused()
                await self.inbox.route(InboxRow.from_row(mark), kind="work_input", target_id=source.request["id"], basis={"work_input": basis}, operation_ids=[uuid4().hex], session=writer)
                # This already-sent source pins the original prompt/seen proof
                # while the captured input still awaits its outcome delivery.
                await self.inbox.register_delivery([origin], outbox_id=parent.id, primary=False, finish=False, session=writer)
                prompt.consumed_at, prompt.closed_reason = self.callbacks.clock(), "answered"
                await writer.flush()
                return InboxRow.from_row(mark)

        return await await_drained(perform())
