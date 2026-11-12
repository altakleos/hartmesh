"""Optional plain-text routing for explicit replies to shown Work cards."""

import logging

from sqlalchemy import select

from app.telegram_bots.callbacks import CallbackRefused, Prompts, normalize_callback_topic
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.work_card_inputs import WorkCardInputs
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class WorkCardText:
    def __init__(self, services, callbacks, *, prompt_input):
        self.services, self.callbacks = services, callbacks
        self.cards = WorkCardInputs(callbacks.controls)
        self.prompt_input = prompt_input

    async def handle(self, channel, update, *, link):
        async def perform():
            message = update.message
            guard = self.services.commands._guard(channel, link)
            async with self.services.callbacks.writer(link, guard) as writer:
                mark = await writer.get(TelegramInboundMarkRow, (link.id, message.message_id, message.chat_id))
                if mark is not None and mark.route_kind is not None:
                    if mark.route_kind == "work_card_hold":
                        return True
                    if mark.route_kind != "work_input" or mark.route_basis.get("work_input", {}).get("version") != 2:
                        return False
                    if mark.route_complete:
                        return True
                    origin = InboxRow.from_row(mark)
                    source_id = mark.route_basis["work_input"]["source_outbox_id"]
                else:
                    origin, source_id = None, None
            prompt_id = None
            if source_id is None:
                if message.reply_to_message is None:
                    return False
                source_id = await self.find(link, message.reply_to_message.message_id, topic=normalize_callback_topic(message.message_thread_id))
                if source_id is None:
                    return False
                async with self.services.callbacks.writer(link, guard) as writer:
                    parent = await writer.get(TelegramOutboxRow, source_id)
                    basis = parent.payload.get("work_request_basis", {}) if parent is not None else {}
                    if basis.get("version") == 3 and basis.get("interaction", {}).get("view") == "prompt":
                        prompts = Prompts(self.services.callbacks)
                        prompt = await prompts.current(link, message.chat_id, normalize_callback_topic(message.message_thread_id), guard=guard, session=writer)
                        if prompt is not None and prompt.id == basis["prompt_id"] and prompt.consumed_at is None and prompts.is_visible(prompt):
                            prompt_id = prompt.id
                origin, _ = await self.cards.inbox.accept(link, update, sanitized_input={"type": "text", "text": message.text})
            await self._apply(channel, link, origin, source_id, guard, prompt_id=prompt_id)
            return True

        return await await_drained(perform())

    async def resume(self, channel, link, origin, *, guard):
        async def perform():
            if origin.link_id != link.id:
                raise CallbackRefused()
            async with self.services.callbacks.writer(link, guard) as writer:
                mark = (await self.cards.inbox._rows(writer, [origin]))[0]
                self.cards.inbox._current(link, mark)
                if mark.route_kind == "work_card_hold":
                    return True
                if mark.route_kind != "work_input" or mark.route_basis.get("work_input", {}).get("version") != 2:
                    return False
                if mark.route_complete:
                    return True
                stored, source_id = InboxRow.from_row(mark), mark.route_basis["work_input"]["source_outbox_id"]
            await self._apply(channel, link, stored, source_id, guard)
            return True

        return await await_drained(perform())

    async def find(self, link, message_id, *, topic):
        """Scan complete scope before choosing, including a multipart first part."""
        after, found = None, []
        while True:
            async with self.cards.plans.outbox.sessions() as reader:
                query = select(TelegramOutboxRow).where(
                    TelegramOutboxRow.link_id == link.id,
                    TelegramOutboxRow.owner_user_id == link.owner_user_id,
                    TelegramOutboxRow.chat_id == link.chat_id,
                    TelegramOutboxRow.binding_generation == link.binding_generation,
                    TelegramOutboxRow.message_thread_id == topic,
                    TelegramOutboxRow.instance_id == link.agent_ref,
                    TelegramOutboxRow.request_id.is_not(None),
                )
                if after is not None:
                    query = query.where(TelegramOutboxRow.id > after)
                page = list(await reader.scalars(query.order_by(TelegramOutboxRow.id).limit(100)))
                for row in page:
                    basis = row.payload.get("work_request_basis", {})
                    if basis.get("version") not in {1, 2, 3, 4, 5, 6}:
                        continue
                    if any(any(type(value) is int and value == message_id for value in part.get("message_ids", [])) for part in row.parts):
                        found.append(row.id)
                if len(page) < 100:
                    break
                after = page[-1].id
        if len(found) > 1:
            raise CallbackRefused()
        return found[0] if found else None

    async def _apply(self, channel, link, origin, source_id, guard, *, prompt_id=None):
        try:
            if prompt_id is not None:
                await self.prompt_input(channel, link, origin, prompt_id, guard)
                return
            await self.cards.capture(link, origin, source_outbox_id=source_id, guard=guard)
            staged = await self.cards.stage(link, origin, guard=guard)
            if staged.attempts == 0:
                expected = {"id": staged.id, "kind": staged.kind, "body": staged.body, "attempts": staged.attempts}
                result = await self.callbacks.acts.check(staged.id, expected=expected, attempt_current=lambda: channel._callback_current(link))
            else:
                result = await self.callbacks.acts.reconcile_receipt(staged.id)
            await self.callbacks._observe(link, result.id, guard=guard)
        except Exception as error:
            # An identified old/spent card remains Work-owned. Closed-input
            # recovery can later offer an explicit new-message choice; this
            # text must never fall through to the ordinary agent queue.
            async with self.services.callbacks.writer(link, guard) as writer:
                stored = (await self.cards.inbox._rows(writer, [origin]))[0]
                if stored.route_kind is None:
                    await self.cards.inbox.route(InboxRow.from_row(stored), kind="work_card_hold", target_id=source_id, basis={"source_outbox_id": source_id}, operation_ids=[], session=writer)
            logger.warning("Work card input observation deferred (%s)", type(error).__name__)
