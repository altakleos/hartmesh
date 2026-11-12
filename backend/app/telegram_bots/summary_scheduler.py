"""Dormant periodic summary planner; holds stop admission before storage."""

import asyncio
import logging

from sqlalchemy import select

from app.telegram_bots.links import _snapshot
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)
SUMMARY_TICK_SECONDS = 60


class SummaryScheduler:
    def __init__(self, plans):
        self.plans = plans
        self._lock = asyncio.Lock()

    async def _page(self, after):
        link, settings = TelegramBotLinkRow, TelegramPersonSettingsRow
        async with self.plans.outbox.sessions() as reader:
            rows = await reader.scalars(
                select(link)
                .join(settings, settings.owner_user_id == link.owner_user_id)
                .where(link.id > after, link.agent_kind == "instance", link.state == "connected", link.health == "ok", settings.summary_enabled.is_(True))
                .order_by(link.id)
                .limit(self.plans.outbox.config.producer.batch)
            )
            return [_snapshot(row) for row in rows]

    async def tick(self):
        async def perform():
            if not self.plans.available():
                return 0
            async with self._lock:
                after, completed = "", 0
                while self.plans.available():
                    links = await self._page(after)
                    if not links:
                        break
                    for link in links:
                        if not self.plans.available():
                            return completed
                        after = link.id
                        try:
                            completed += await self.plans.plan(link) is not None
                        except Exception:
                            logger.warning("Telegram summary planning deferred: link=%s", link.id)
                return completed

        return await await_drained(perform())
