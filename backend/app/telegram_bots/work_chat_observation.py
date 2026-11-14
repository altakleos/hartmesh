"""Bounded receipt-only observation for chat execution and saved review cards."""

import logging

from sqlalchemy import func, or_, select

from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow

logger = logging.getLogger(__name__)
_PAGE = 32


class WorkChatObservation:
    def __init__(self, chat):
        self.chat, self._cursors = chat, {}

    async def page(self, link, family, table, query):
        key = link.id, family
        saved = self._cursors.get(key)
        after, horizon = saved[1:] if saved is not None and saved[0] == link.binding_generation else (None, None)
        async with self.chat.host.outbox.sessions() as reader:
            if horizon is None:
                horizon = await reader.scalar(query.with_only_columns(func.max(table.id)))
            if horizon is None:
                self._cursors.pop(key, None)
                return []
            query = query.where(table.id <= horizon)
            if after is not None:
                query = query.where(table.id > after)
            rows = list(await reader.scalars(query.order_by(table.id).limit(_PAGE)))
        if len(rows) == _PAGE and rows[-1].id != horizon:
            self._cursors[key] = link.binding_generation, rows[-1].id, horizon
        else:
            self._cursors.pop(key, None)
        return rows

    async def tick(self, link, *, guard):
        await guard.prepare()
        row = TelegramOutboxRow
        cards = await self.page(
            link,
            "cards",
            row,
            select(row).where(
                row.link_id == link.id,
                row.owner_user_id == link.owner_user_id,
                row.binding_generation == link.binding_generation,
                row.chat_id == link.chat_id,
                row.claim_owner.is_(None),
                row.claim_id.is_(None),
                row.claimed_at.is_(None),
                or_(row.payload["assignment_intent"]["draft_id"].as_string().is_not(None), row.payload["work_management_intent"]["work_id"].as_string().is_not(None)),
            ),
        )
        for item in cards:
            guard.current()
            try:
                if "assignment_intent" in item.payload:
                    await self.chat.assignments.observe(link, item.payload["assignment_intent"]["draft_id"], guard=guard)
                else:
                    await self.chat.management.observe(link, item.id, guard=guard)
            except Exception as error:
                logger.warning("Telegram saved card observation deferred (%s)", type(error).__name__)
        row = TelegramActRow
        acts = await self.page(
            link, "execution", row, select(row).where(row.link_id == link.id, row.owner_user_id == link.owner_user_id, row.binding_generation == link.binding_generation, row.chat_id == link.chat_id, row.kind.in_(("activate", "stop")))
        )
        for item in acts:
            guard.current()
            if item.id in self.chat.host.acts._work_active:
                continue
            try:
                await self.chat.executions.observe(link, item.id, guard=guard)
            except Exception as error:
                logger.warning("Telegram execution receipt observation deferred (%s)", type(error).__name__)
