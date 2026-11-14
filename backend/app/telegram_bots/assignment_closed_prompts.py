"""Retain late assignment typing under its actual shown prompt and input."""

from uuid import uuid4

from app.telegram_bots.assignment_input_ownership import release_hold
from app.telegram_bots.assignment_typing import shown_prompt
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_closed_inputs import closed_parts
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class ClosedTypingNotice:
    def __init__(self, origin, source):
        self.origin, self.source = origin, source


def selected_prompt(prompt, parent, *, ambiguous=False):
    if ambiguous:
        return {"prompt_id": None, "source_outbox_id": None, "ambiguous": True}
    intent = (parent.payload.get("assignment_prompt_intent") or parent.payload.get("management_prompt_intent") or {}) if parent is not None else {}
    return {"prompt_id": prompt.id if prompt is not None else intent.get("prompt_id"), "source_outbox_id": parent.id if parent is not None else prompt.payload.get("anchor_outbox_id") if prompt is not None else None, "ambiguous": False}


class AssignmentClosedPrompts:
    def __init__(self, work):
        self.work, self.host = work, work.host
        self.outbox, self.callbacks = self.host.outbox, self.host.callbacks

    async def capture(self, link, origin, *, source, guard, session=None):
        source = source or selected_prompt(None, None, ambiguous=True)
        async with self.callbacks.writer(link, guard, session=session) as writer:
            mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
            self.outbox.inbox._current(link, mark)
            if not same_body(mark.sanitized_input, origin.sanitized_input):
                raise ProductSourceRetired()
            if mark.route_kind == "work_card_hold" and "closed_typing" in mark.route_basis:
                return InboxRow.from_row(mark)
            if mark.route_kind == "work_card_hold" and "voice_note" in mark.route_basis and not mark.route_complete:
                from app.telegram_bots.assignment_voice_inputs import closed_voice_source, voice_basis

                voice = next((value for owner in ("assignment", "management") if (value := voice_basis(mark, owner=owner)) is not None), None)
                if (
                    voice is None
                    or voice["phase"] != "closed"
                    or not same_body(mark.route_basis["voice_note"].get("original"), voice["original"])
                    or not same_body(source, closed_voice_source(mark, voice))
                    or mark.operation_ids
                    or mark.thread_id is not None
                    or mark.turn_id is not None
                ):
                    raise ProductSourceRetired()
                mark.route_target_id = source["source_outbox_id"]
                mark.route_basis = {**mark.route_basis, "source_outbox_id": source["source_outbox_id"], "closed_typing": {"version": 1, **source}}
                await writer.flush()
                return InboxRow.from_row(mark)
            if mark.route_complete or mark.route_kind not in {None, "assignment_hold"}:
                raise CallbackRefused()
            if mark.route_kind == "assignment_hold":
                release_hold(mark)
            return await self.outbox.inbox.route(
                InboxRow.from_row(mark),
                kind="work_card_hold",
                target_id=source["source_outbox_id"],
                basis={"source_outbox_id": source["source_outbox_id"], "closed_typing": {"version": 1, **source}},
                operation_ids=[],
                session=writer,
            )

    async def offer(self, link, origin, *, source, guard):
        async def perform():
            incoming = await self.capture(link, origin, source=source, guard=guard)
            if incoming.route_basis.get("work_closed") is not None:
                return incoming.route_basis["work_closed"]["notice_id"]
            stored = incoming.route_basis["closed_typing"]
            actual_source = {key: stored[key] for key in ("prompt_id", "source_outbox_id", "ambiguous")}
            async with self.outbox.sessions() as reader:
                parent = await reader.get(TelegramOutboxRow, actual_source["source_outbox_id"]) if actual_source["source_outbox_id"] else None
                prompt = await reader.get(TelegramCallbackRow, actual_source["prompt_id"]) if actual_source["prompt_id"] else None
                item = _item(parent) if parent is not None else None
            available = False
            scope = (link.id, link.owner_user_id, link.binding_generation, origin.chat_id, origin.message_thread_id)
            if parent is not None and (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id, parent.message_thread_id) != scope:
                raise CallbackRefused()
            if prompt is not None and (prompt.link_id, prompt.binding_generation, prompt.chat_id, prompt.message_thread_id) != (link.id, link.binding_generation, origin.chat_id, origin.message_thread_id):
                raise CallbackRefused()
            if parent is not None and prompt is not None and not actual_source["ambiguous"]:
                try:
                    shown_prompt(item, prompt, accepted_at=origin.accepted_at)
                    from app.telegram_bots.assignment_prompt_reopen import prompt_plans

                    await prompt_plans(self, item).reader.read(link, item, allow_closed=True)
                    available = True
                except (CallbackRefused, ProductSourceRetired, PermanentDeliveryError, AgentDenied):
                    pass
            async with self.callbacks.writer(link, guard) as writer:
                mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
                self.outbox.inbox._current(link, mark)
                if not same_body(mark.sanitized_input, origin.sanitized_input):
                    raise ProductSourceRetired()
                if mark.route_kind == "work_card_hold" and "closed_typing" in mark.route_basis:
                    if not same_body(mark.route_basis["closed_typing"], stored):
                        raise CallbackRefused()
                    if "work_closed" in mark.route_basis:
                        return mark.route_basis["work_closed"]["notice_id"]
                if mark.route_complete or mark.route_kind != "work_card_hold":
                    raise CallbackRefused()
                incoming = InboxRow.from_row(mark)
                if "voice_note" in mark.route_basis:
                    from sqlalchemy import select

                    from app.telegram_bots.closed_voice_echo import echo_identity, qualify_echo

                    legacy = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == f"notice:voice-heard:{mark.chat_id}:{mark.message_id}"))
                    if legacy is not None:
                        if legacy.state != "sent":
                            return legacy.id
                        recorded = qualify_echo(link, mark, legacy)
                        await self.outbox.inbox.register_delivery([incoming], outbox_id=legacy.id, primary=False, finish=False, session=writer)
                        mark.route_basis = {**mark.route_basis, "closed_voice_echo": recorded}
                        incoming = InboxRow.from_row(mark)
                    elif echo_identity(mark) is not None:
                        raise ProductSourceRetired()
                if parent is not None:
                    await self.outbox.inbox.register_delivery([incoming], outbox_id=parent.id, primary=False, finish=False, session=writer)
                context_id = uuid4().hex
                context = TelegramCallbackRow(
                    id=context_id,
                    group_id=uuid4().hex,
                    link_id=link.id,
                    chat_id=mark.chat_id,
                    binding_generation=link.binding_generation,
                    message_id=None,
                    message_thread_id=mark.message_thread_id,
                    kind="prompt",
                    target_kind=None,
                    target_id=None,
                    payload={"purpose": "retired_presentation", "anchor_outbox_id": actual_source["source_outbox_id"], "closed_input": {"message_id": mark.message_id, "update_id": mark.update_id}},
                    created_at=self.callbacks.clock(),
                    consumed_at=self.callbacks.clock(),
                    closed_reason="answered",
                )
                writer.add(context)
                await writer.flush()
                choices = [("prompt_send_new", "label.send_new"), ("prompt_discard", "label.discard")]
                if available:
                    choices.insert(0, ("prompt_reopen", "label.reopen_answer"))
                payload = {"prompt_id": context_id, "message_id": mark.message_id}
                identity = await self.outbox.plan_product_keyboard(
                    link,
                    source_id="closed-typing:" + str(mark.chat_id) + ":" + str(mark.message_id),
                    parts=closed_parts(mark, available=available),
                    keyboard=[[{"kind": kind, "label": label, "target_kind": None, "target_id": None, "payload": payload}] for kind, label in choices],
                    callbacks=self.callbacks,
                    guard=guard,
                    inbox_rows=[incoming],
                    finish_route=False,
                    message_thread_id=mark.message_thread_id,
                    session=writer,
                )
                mark.route_basis = {
                    **mark.route_basis,
                    "work_closed": {"version": 1, "source_outbox_id": actual_source["source_outbox_id"], "prompt_id": context_id, "request_id": None, "work_id": parent.work_id if parent is not None else None, "notice_id": identity},
                }
                await writer.flush()
                return identity

        return await await_drained(perform())

    async def reopen(self, channel, link, row, origin, *, guard):
        from app.telegram_bots.assignment_prompt_reopen import reopen_assignment

        return await await_drained(reopen_assignment(self, link, row, origin, guard=guard))
