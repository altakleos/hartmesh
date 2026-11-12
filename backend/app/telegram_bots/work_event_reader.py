"""Atomic, bounded Work catch-up with a mandatory recipient projection.

The projector borrows this writer for SQL-only event interpretation and outbox
insertion. It must neither commit nor call providers/transport. Installation and
the complete recipient matrix belong to notification integration, not startup
of this primitive. No default projection may silently consume Work history.
"""

import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import and_, case, func, select

from app.telegram_bots.links import _snapshot
from deerflow.persistence.agent_instances.work import AgentWorkEventRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramWorkCursorRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class WorkEventReader:
    def __init__(self, *, links, project, batch=None):
        self.links, self.project = links, project
        self.batch = links.config.producer.batch if batch is None else batch
        if not callable(project) or type(self.batch) is not int or self.batch < 1:
            raise ValueError("work-event-reader-configuration")
        self._lock = asyncio.Lock()
        self._last_link = ""
        self._last_work = {}

    def _due(self):
        link, work, cursor = TelegramBotLinkRow, AgentWorkRow, TelegramWorkCursorRow
        # Round-robin links first, Work within each link second. Rotation moves
        # after failures too: a single unreadable source cannot monopolize batch=1.
        wrap = case(*[(and_(link.id == identity, work.id <= last), 1) for identity, last in self._last_work.items()], else_=0) if self._last_work else 0
        ranked = (
            select(
                link.id.label("link_id"),
                link.binding_generation,
                work.id.label("work_id"),
                func.row_number().over(partition_by=link.id, order_by=(wrap, work.id) if self._last_work else work.id).label("position"),
            )
            .join(work, work.instance_id == link.agent_ref)
            .outerjoin(cursor, and_(cursor.link_id == link.id, cursor.work_id == work.id))
            .where(
                link.agent_kind == "instance",
                link.state == "connected",
                link.work_baseline_generation == link.binding_generation,
                (cursor.link_id.is_(None)) | (cursor.binding_generation == link.binding_generation),
                work.revision > func.coalesce(cursor.revision, 0),
            )
            .subquery()
        )
        return select(ranked.c.link_id, ranked.c.binding_generation, ranked.c.work_id).order_by(ranked.c.position, case((ranked.c.link_id > self._last_link, 0), else_=1), ranked.c.link_id).limit(self.batch)

    async def tick(self):
        return await await_drained(self._tick())

    async def _tick(self):
        async with self._lock:
            async with self.links.session_factory() as reader:
                # Keep only live link bookmarks; persisted cursors own correctness.
                live = set(await reader.scalars(select(TelegramBotLinkRow.id)))
                self._last_work = {key: value for key, value in self._last_work.items() if key in live}
                pairs = list(await reader.execute(self._due()))
            completed = 0
            for identity, generation, work_id in pairs:
                self._last_link, self._last_work[identity] = identity, work_id
                try:
                    completed += await self._consume(identity, generation, work_id)
                except Exception:
                    # Do not log source bodies, provider messages or account data.
                    logger.warning("Telegram Work event projection deferred: link=%s work=%s", identity, work_id)
            return completed

    async def _consume(self, identity, generation, work_id):
        async with self.links._transaction() as writer:
            binding = await writer.get(TelegramBotLinkRow, identity, with_for_update=True, populate_existing=True)
            if binding is None or binding.state != "connected" or binding.agent_kind != "instance" or binding.binding_generation != generation or binding.work_baseline_generation != generation:
                return 0
            link = _snapshot(binding)
            work = await writer.get(AgentWorkRow, work_id)
            if work is None or work.instance_id != link.agent_ref:
                return 0
            cursor = await writer.get(TelegramWorkCursorRow, (identity, work_id))
            if cursor is not None and (cursor.binding_generation != generation or cursor.instance_id != work.instance_id):
                raise ValueError("work-event-cursor-scope")
            after, through = cursor.revision if cursor is not None else 0, work.revision
            if through <= after:
                return 0
            while after < through:
                events = list(
                    await writer.scalars(
                        select(AgentWorkEventRow)
                        .where(AgentWorkEventRow.instance_id == work.instance_id, AgentWorkEventRow.work_id == work_id, AgentWorkEventRow.revision > after, AgentWorkEventRow.revision <= through)
                        .order_by(AgentWorkEventRow.revision)
                        .limit(100)
                    )
                )
                if not events:
                    raise ValueError("work-event-history-incomplete")
                for event in events:
                    if event.revision != after + 1:
                        raise ValueError("work-event-history-incomplete")
                    if await self.project(writer, link, event) is not None:
                        raise ValueError("work-event-projector-result")
                    after = event.revision
            if cursor is None:
                cursor = TelegramWorkCursorRow(link_id=identity, work_id=work_id, instance_id=work.instance_id, binding_generation=generation)
                writer.add(cursor)
            cursor.revision, cursor.updated_at = through, datetime.now(UTC)
            await writer.flush()
            current = await writer.get(TelegramBotLinkRow, identity, populate_existing=True)
            scope = ("binding_generation", "agent_kind", "agent_ref", "owner_user_id", "connection_id", "chat_id", "telegram_user_id")
            if current is None or current.state != "connected" or current.work_baseline_generation != generation or any(getattr(current, key) != getattr(link, key) for key in scope):
                raise ValueError("work-event-binding-changed")
            return 1
