"""Resume durable Work input routes without inventing an input or operation."""

import logging

from sqlalchemy import select, tuple_

from app.telegram_bots.inbox import InboxRow
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

logger = logging.getLogger(__name__)
_ROUTES = {"work_input", "work_card_hold", "work_file_new", "work_album_hold", "work_file_album", "work_answer", "assignment_hold", "assignment_draft", "assignment_criteria", "management_edit", "command_stop"}


async def recover_inputs(runtime, phase, *, link_id=None, guard_factory):
    host, after = runtime.host, runtime._input_after.get(link_id)
    inputs = getattr(host, "work_text_inputs", None)
    if inputs is None:
        return
    row = TelegramInboundMarkRow
    order = (row.link_id, row.message_id, row.chat_id)
    while runtime.available() and phase():
        async with host.outbox.sessions() as reader:
            query = select(row).where(row.route_kind.in_(_ROUTES), row.route_complete.is_(False))
            if link_id is not None:
                query = query.where(row.link_id == link_id)
            if after is not None:
                query = query.where(tuple_(*order) > after)
            page = tuple(InboxRow.from_row(mark) for mark in await reader.scalars(query.order_by(*order).limit(32)))
        for origin in page:
            if not runtime.available() or not phase():
                return
            after = origin.link_id, origin.message_id, origin.chat_id
            runtime._input_after[link_id] = after
            try:
                link = await host.links.get(origin.link_id)
                channel = host.channel(origin.link_id)
                if link is None or channel is None or link.agent_kind != "instance":
                    continue
                guard = guard_factory(runtime, host.commands._guard(channel, link), phase=phase)
                guard.current()
                chat = getattr(host, "work_chat", None)
                if chat is not None and await chat.recover_input(channel, origin, guard=guard):
                    continue
                await inputs.resume(channel, origin, recovery_guard=guard)
            except Exception:
                logger.warning("Telegram saved Work input remains held: link=%s message=%s", origin.link_id, origin.message_id)
        if len(page) < 32:
            runtime._input_after.pop(link_id, None)
            return
