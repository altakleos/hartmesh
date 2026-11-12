"""Optional routing of accepted plain text through its shown Work prompt."""

import logging
from copy import deepcopy

from sqlalchemy import select

from app.telegram_bots.callbacks import CallbackRefused, Prompts, normalize_callback_topic
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_card_text import WorkCardText
from app.telegram_bots.work_input_admission import WorkInputAdmissions
from app.telegram_bots.work_input_capture import WorkInputCaptures
from app.telegram_bots.work_input_previews import WorkInputPreviews, blocked_input_actions, input_identity
from app.telegram_bots.work_typing import PURPOSES, shown_prompt_messages
from deerflow.agent_instances.contract import AgentConflict
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class WorkTextInputs:
    def __init__(self, services, callbacks):
        self.services, self.callbacks = services, callbacks
        self.cards = WorkCardText(services, callbacks, prompt_input=self._capture_prompt)
        self.inbox = services.outbox.inbox
        self.prompts = Prompts(services.callbacks)
        self.captures = WorkInputCaptures(self.inbox, services.callbacks)
        self.admissions = WorkInputAdmissions(callbacks.controls, self.captures)
        self.previews = WorkInputPreviews(callbacks.outcomes.plans, self.captures)

    async def handle_text(self, channel, update, *, captured_link=None):
        async def perform():
            link, message = deepcopy(captured_link or channel.link), update.message
            if link.agent_kind != "instance" or message is None or type(message.text) is not str or not message.text.strip() or not channel._callback_current(link):
                return False
            if message.photo or message.document or message.voice or message.audio or message.video or message.media_group_id is not None:
                return False
            if message.forward_origin is not None or message.is_automatic_forward is True or any(message.api_kwargs.get(key) is not None for key in ("forward_from", "forward_from_chat", "forward_sender_name", "forward_date")):
                return False
            if await self.cards.handle(channel, update, link=link):
                return True
            guard = self.services.commands._guard(channel, link)
            topic = normalize_callback_topic(message.message_thread_id)
            quoted = message.reply_to_message.message_id if message.reply_to_message else None
            async with self.services.callbacks.writer(link, guard) as writer:
                mark = await writer.get(TelegramInboundMarkRow, (link.id, message.message_id, message.chat_id))
                if mark is not None and mark.route_kind is not None:
                    if mark.route_kind != "work_input":
                        return False
                    if mark.route_complete:
                        return True
                    origin, prompt_id = InboxRow.from_row(mark), mark.route_basis["work_input"]["prompt_id"]
                else:
                    prompt = await self.prompts.current(link, message.chat_id, topic, guard=guard, session=writer)
                    if prompt is None or prompt.payload.get("purpose") not in PURPOSES.values() or not self.prompts.is_visible(prompt):
                        return False
                    parent = await writer.get(TelegramOutboxRow, prompt.payload.get("anchor_outbox_id"))
                    if parent is None or parent.payload.get("work_request_basis", {}).get("prompt_id") != prompt.id:
                        return False
                    if quoted is not None and quoted not in shown_prompt_messages(_item(parent)):
                        return False
                    origin, prompt_id = None, prompt.id
            if origin is None:
                origin, _ = await self.inbox.accept(link, update, sanitized_input={"type": "text", "text": message.text})
            captured = await self.captures.capture(link, origin, prompt_id=prompt_id, guard=guard)
            return await self._apply(channel, link, captured, guard)

        return await await_drained(perform())

    async def resume(self, channel, origin):
        """Resume the stored route; caller copies supply only lookup identity."""

        async def perform():
            link = deepcopy(channel.link)
            guard = self.services.commands._guard(channel, link)
            if await self.cards.resume(channel, link, origin, guard=guard):
                return True
            async with self.services.callbacks.writer(link, guard) as writer:
                stored = (await self.inbox._rows(writer, [origin]))[0]
                self.inbox._current(link, stored)
                if origin.link_id != link.id or stored.route_kind != "work_input":
                    raise CallbackRefused()
                if stored.route_complete:
                    return True
                original, prompt_id = InboxRow.from_row(stored), stored.route_basis["work_input"]["prompt_id"]
            captured = await self.captures.capture(link, original, prompt_id=prompt_id, guard=guard)
            return await self._apply(channel, link, captured, guard)

        return await await_drained(perform())

    async def _capture_prompt(self, channel, link, origin, prompt_id, guard):
        captured = await self.captures.capture(link, origin, prompt_id=prompt_id, guard=guard)
        return await self._apply(channel, link, captured, guard)

    async def _apply(self, channel, link, captured, guard):
        data = captured.route_basis["work_input"]
        if data["purpose"] in {"decide_own", "decide_note"}:
            async with self.services.callbacks.writer(link, guard) as writer:
                if await writer.scalar(select(blocked_input_actions(input_identity(captured)))):
                    # Its confirmation owns progress now; receipt observation
                    # must settle that operation before a new preview is safe.
                    return True
            await self.previews.open(link, captured, guard=guard)
            return True
        try:
            staged = await self.admissions.stage(link, captured, guard=guard)
        except ProductSourceRetired:
            if data["purpose"] == "request_changes" and await self.callbacks.review_recovery.recover(link, captured, guard=guard):
                return True
            raise
        except AgentConflict:
            if data["purpose"] != "request_changes":
                raise
            await self.previews.open(link, captured, guard=guard)
            return True
        # Replayed input is never a fresh Retry gesture. The action worker
        # owns automatic attempts; explicit Retry owns user-requested ones.
        try:
            if staged.attempts == 0:
                expected = {"id": staged.id, "kind": staged.kind, "body": staged.body, "attempts": staged.attempts}
                result = await self.callbacks.acts.check(staged.id, expected=expected, attempt_current=lambda: channel._callback_current(link))
            else:
                result = await self.callbacks.acts.reconcile_receipt(staged.id)
            if result.kind == "changes_requested" and result.state == "conflict":
                try:
                    await self.previews.open(link, captured, guard=guard, conclusive_act=result)
                except ProductSourceRetired:
                    if not await self.callbacks.review_recovery.recover(link, captured, guard=guard, conclusive_act=result):
                        raise
            else:
                await self.callbacks._observe(link, result.id, guard=guard)
        except Exception as error:
            logger.warning("Work input observation deferred (%s)", type(error).__name__)
        return True
