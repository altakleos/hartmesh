"""Capture explicit assignment words and route only assignment-owned typing."""

from sqlalchemy import select

from app.telegram_bots.assignment_drafts import AssignmentDraft, origin_facts
from app.telegram_bots.assignment_input_ownership import capture_basis, release_hold, require_owner, typed_basis
from app.telegram_bots.assignment_prompt_owners import quoted_assignment_sources
from app.telegram_bots.assignment_typing import shown_prompt
from app.telegram_bots.callbacks import CallbackRefused, normalize_callback_topic
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import PermanentDeliveryError, ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_input_capture import accepted_text
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


def typing_notice(origin, prompt, parent, callbacks, *, max_chars, ambiguous=False):
    try:
        if ambiguous or prompt is None or parent is None:
            raise CallbackRefused()
        callbacks._usable(prompt)
        acknowledged, messages = shown_prompt(_item(parent), prompt, accepted_at=origin.accepted_at)
        if len(origin.sanitized_input["text"]) > max_chars:
            return "assign.too_long"
        accepted_text(origin, prompt, acknowledged, message_ids=messages)
    except (CallbackRefused, PermanentDeliveryError, ProductSourceRetired, KeyError, TypeError, ValueError):
        return "prompt.closed"
    return None


class AssignmentCommands:
    def __init__(self, inputs, *, management=None):
        self.inputs, self.management = inputs, management
        self.outbox, self.callbacks = inputs.outbox, inputs.callbacks

    async def handle(self, channel, update, link, *, guard):
        command = update.message.text.lstrip().split(maxsplit=1)
        arguments = command[1] if len(command) > 1 else ""
        reply = update.message.reply_to_message
        own = reply is not None and reply.from_user is not None and reply.from_user.id == link.telegram_user_id and not reply.from_user.is_bot and reply.chat_id == link.chat_id and type(reply.text) is str
        words = "\n\n".join(part.strip() for part in ([reply.text] if own else []) + [arguments] if part.strip())
        origin, _created = await self.outbox.inbox.accept(link, update, sanitized_input={"command": "assign", "text": words})
        if origin.route_complete:
            return origin
        try:
            return await self.submit(link, origin, guard=guard)
        except AgentDenied as error:
            if origin.route_kind is not None:
                raise
            return await self.notice(link, origin, "assign.no_work" if error.reason == "work-off" else "assign.no_access", guard=guard)

    async def submit(self, link, origin, *, guard):
        await prepare_publication_guard(guard)
        if origin.sanitized_input.get("command") != "assign" or type(origin.sanitized_input.get("text")) is not str:
            raise ProductSourceRetired()
        words = origin.sanitized_input["text"]
        if not words:
            return await self.inputs.prompts.plan(link, origin, purpose="assign_words", guard=guard)
        if len(words) > self.outbox.config.assign.max_chars:
            return await self.notice(link, origin, "assign.too_long", guard=guard)

        async def admission(writer):
            await guard.check(writer)
            mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
            self.outbox.inbox._current(link, mark)
            if not same_body(origin_facts(InboxRow.from_row(mark)), origin_facts(origin)):
                raise ProductSourceRetired()
            if mark.route_kind is None:
                await self.inputs.prompts.prompts.close(link, mark.chat_id, mark.message_thread_id, reason="command", guard=guard, session=writer, shown_only=True)

        value = await self.inputs.requests.request(link, origin, words=words, admission=admission)
        if isinstance(value, AssignmentDraft):
            return await self.inputs.cards.plan(link, value.id, guard=guard)
        if value.phase == "awaiting_done":
            return await self.inputs.prompts.plan(link, origin, purpose="assign_done", draft_id=value.id, guard=guard)
        return value

    async def notice(self, link, origin, key, *, guard, session=None):
        button = None
        if key == "assign.no_work":
            from app.telegram_bots.pages import page_address
            from app.telegram_bots.presentation import page_permissions

            access = await page_permissions(self.inputs.cards.reader.authority, link)
            address = page_address("web.agentSettings", link, public_base_url=self.outbox.config.public_base_url, **access)
            if address is not None:
                button = {"page": "web.agentSettings", "url": address}
        async with self.callbacks.writer(link, guard, session=session) as writer:
            mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
            self.outbox.inbox._current(link, mark)
            if not same_body(origin_facts(InboxRow.from_row(mark)), origin_facts(origin)):
                raise ProductSourceRetired()
            if mark.route_kind == "assignment_hold":
                release_hold(mark)
            if mark.route_kind is None and mark.sanitized_input.get("command"):
                await self.inputs.prompts.prompts.close(link, mark.chat_id, mark.message_thread_id, reason="command", guard=guard, session=writer, shown_only=True)
            routed = await self.outbox.inbox.route(origin, kind="assignment_notice", target_id=key, basis={"key": key}, operation_ids=[], session=writer)
            return await self.outbox.plan_product(
                link,
                source_id=f"assignment-notice:{origin.chat_id}:{origin.message_id}",
                parts=[
                    {
                        "kind": "text",
                        "reply_to_message_id": origin.message_id,
                        "source_ref": {"key": key, "values": {"max": self.outbox.config.assign.max_chars} if key == "assign.too_long" else {}},
                        **({"url_button": button} if button else {}),
                    }
                ],
                inbox_rows=[routed],
                finish_route=True,
                session=writer,
            )

    async def handle_text(self, channel, update, link, *, guard):
        message = update.message
        if (
            link.agent_kind != "instance"
            or message is None
            or type(message.text) is not str
            or not message.text.strip()
            or message.text.startswith("/")
            or message.photo
            or message.document
            or message.voice
            or message.audio
            or message.video
            or message.media_group_id is not None
            or message.forward_origin is not None
            or message.is_automatic_forward is True
            or any(message.api_kwargs.get(key) is not None for key in ("forward_from", "forward_from_chat", "forward_sender_name", "forward_date"))
        ):
            return False

        async def perform():
            origin, _created = await self.outbox.inbox.accept(link, update, sanitized_input={"type": "text", "text": message.text})
            if origin.route_kind == "assignment_notice":
                return True
            if origin.route_kind in {"assignment_draft", "assignment_criteria"}:
                await self.inputs.answer(link, origin, guard=guard)
                return True
            if origin.route_kind == "management_edit" and self.management is not None:
                await self.management.answer(link, origin, guard=guard)
                return True
            if origin.route_kind == "assignment_hold":
                return await self.resume(link, origin, guard=guard)
            if origin.route_kind is not None:
                return await self.close_legacy_capture(link, origin, guard=guard)
            quoted = origin.sanitized_input.get("reply_to_message_id")
            intents = ("assignment_prompt_intent", "management_prompt_intent") if self.management is not None else ("assignment_prompt_intent",)
            sources = await quoted_assignment_sources(self.outbox, link, quoted, origin.message_thread_id, intents=intents) if quoted is not None else []
            async with self.callbacks.writer(link, guard) as writer:
                parent = None
                if sources:
                    parent = await writer.get(TelegramOutboxRow, sources[0])
                    candidates = [parent.payload[key] for key in intents if parent is not None and key in parent.payload]
                    intent = candidates[0] if len(candidates) == 1 else {}
                    prompt = await writer.get(TelegramCallbackRow, intent["prompt_id"]) if type(intent.get("prompt_id")) is str else None
                elif quoted is None:
                    prompt = await self.inputs.prompts.prompts.current(link, origin.chat_id, origin.message_thread_id, guard=guard, session=writer)
                else:
                    matches = list(
                        await writer.scalars(
                            select(TelegramCallbackRow).where(
                                TelegramCallbackRow.link_id == link.id,
                                TelegramCallbackRow.chat_id == origin.chat_id,
                                TelegramCallbackRow.binding_generation == link.binding_generation,
                                TelegramCallbackRow.message_thread_id == normalize_callback_topic(origin.message_thread_id),
                                TelegramCallbackRow.kind == "prompt",
                                TelegramCallbackRow.message_id == quoted,
                            )
                        )
                    )
                    prompt = matches[0] if len(matches) == 1 else None
                purposes = {"assign_words", "assign_done", "assign_edit"} | ({"work_edit"} if self.management is not None else set())
                if not sources and (prompt is None or prompt.payload.get("purpose") not in purposes):
                    return await self.close_legacy_capture(link, origin, guard=guard, session=writer)
                if parent is None and prompt is not None:
                    parent = await writer.get(TelegramOutboxRow, prompt.payload.get("anchor_outbox_id"))
                key = typing_notice(origin, prompt, parent, self.callbacks, max_chars=self.outbox.config.assign.max_chars, ambiguous=len(sources) > 1)
                if key is None:
                    mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
                    self.outbox.inbox._current(link, mark)
                    if not same_body(origin_facts(InboxRow.from_row(mark)), origin_facts(origin)):
                        raise ProductSourceRetired()
                    if mark.route_kind is not None:
                        return True
                    owner = capture_basis(mark, prompt, parent)
                    origin = await self.outbox.inbox.route(InboxRow.from_row(mark), kind="assignment_hold", target_id=parent.id, basis={"assignment_owner": owner}, operation_ids=[], session=writer)
                    require_owner(link, origin, prompt, parent)
                    await self.outbox.inbox.register_delivery([origin], outbox_id=parent.id, primary=False, finish=False, session=writer)
            if key is not None:
                await self.notice(link, origin, key, guard=guard)
            else:
                await self.resume(link, origin, guard=guard)
            return True

        return await await_drained(perform())

    async def close_legacy_capture(self, link, origin, *, guard, session=None):
        """Close only a proved unpublished old probe; never rewrite its input."""
        value = origin.sanitized_input
        if link.agent_kind != "instance" or type(value) is not dict or set(value) not in ({"text"}, {"text", "reply_to_message_id"}) or type(value.get("text")) is not str or not value["text"].strip():
            return False
        async with self.callbacks.writer(link, guard, session=session) as writer:
            mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
            self.outbox.inbox._current(link, mark)
            if not same_body(origin_facts(InboxRow.from_row(mark)), origin_facts(origin)):
                raise ProductSourceRetired()
            if mark.route_kind not in (None, "turn"):
                return False
            if mark.route_complete:
                return True
            if (
                mark.turn_id is not None
                or mark.thread_id is not None
                or mark.run_id is not None
                or mark.assistant_message_id is not None
                or mark.outbox_id is not None
                or mark.delivery_ids
                or mark.operation_ids
                or mark.grouped_message_ids
                or mark.route_kind is None
                and (mark.route_target_id is not None or mark.route_basis)
                or mark.route_kind == "turn"
                and (mark.route_target_id != link.agent_ref or not same_body(mark.route_basis, {"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}))
            ):
                return False
            incoming = InboxRow.from_row(mark)
            if mark.route_kind is None:
                incoming = await self.outbox.inbox.route(incoming, kind="assignment_notice", target_id="assign.resend", basis={"key": "assign.resend"}, operation_ids=[], session=writer)
            await self.outbox.plan_product(
                link,
                source_id=f"assignment-legacy-input:{origin.chat_id}:{origin.message_id}",
                parts=[{"kind": "text", "reply_to_message_id": origin.message_id, "source_ref": {"key": "assign.resend", "values": {}}}],
                inbox_rows=[incoming],
                finish_route=True,
                session=writer,
            )
            await guard.check(writer)
            return True

    async def resume(self, link, origin, *, guard):
        async def perform():
            async with self.callbacks.writer(link, guard) as writer:
                mark = (await self.outbox.inbox._rows(writer, [origin]))[0]
                self.outbox.inbox._current(link, mark)
                if mark.route_kind != "assignment_hold" or mark.route_complete:
                    return True
                owner = typed_basis(mark)
                if owner is None:
                    raise ProductSourceRetired()
                prompt = await writer.get(TelegramCallbackRow, owner["prompt_id"])
                parent = await writer.get(TelegramOutboxRow, owner["source_outbox_id"])
                captured = InboxRow.from_row(mark)
                key = typing_notice(captured, prompt, parent, self.callbacks, max_chars=self.outbox.config.assign.max_chars)
                if key is None:
                    try:
                        require_owner(link, mark, prompt, parent, expected=owner)
                    except (ProductSourceRetired, PermanentDeliveryError, KeyError, TypeError, ValueError):
                        key = "prompt.closed"
                if key is not None:
                    await self.notice(link, captured, key, guard=guard, session=writer)
                    return True
            processor = self.management if owner["owner"] == "management" else self.inputs
            if processor is None:
                raise ProductSourceRetired()
            result = await processor.answer(link, captured, prompt_id=owner["prompt_id"], guard=guard)
            if result is None:
                # A missing original prompt is definitive; asynchronous failures
                # propagate with the immutable hold still owning this input.
                async with self.callbacks.writer(link, guard) as writer:
                    mark = (await self.outbox.inbox._rows(writer, [captured]))[0]
                    if mark.route_kind == "assignment_hold":
                        prompt = await writer.get(TelegramCallbackRow, owner["prompt_id"])
                        parent = await writer.get(TelegramOutboxRow, owner["source_outbox_id"])
                        key = typing_notice(InboxRow.from_row(mark), prompt, parent, self.callbacks, max_chars=self.outbox.config.assign.max_chars)
                        if key is not None:
                            await self.notice(link, InboxRow.from_row(mark), key, guard=guard, session=writer)
            return True

        return await await_drained(perform())
