"""Managed-file byte ownership and durable attachment metadata."""

from __future__ import annotations

import asyncio
import errno
import logging
import math
from collections.abc import Sequence

from app.channels.inbound_files import InboundFileLease
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class FileBytesBudget:
    """One fixed process-wide capacity, admitted before any batch is read."""

    def __init__(self, capacity: int) -> None:
        if type(capacity) is not int or capacity <= 0:
            raise ValueError("invalid-file-byte-capacity")
        self._capacity, self._used = capacity, 0
        self._condition = asyncio.Condition()

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def used(self) -> int:
        return self._used

    async def reserve_batch(self, ceilings: Sequence[int], *, timeout: float) -> FileBytesReservation:
        if not isinstance(ceilings, Sequence) or any(type(size) is not int or size < 0 for size in ceilings):
            raise ValueError("invalid-file-byte-batch")
        ceilings = tuple(ceilings)
        total = sum(ceilings)
        if total > self.capacity or type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout < 0:
            raise ValueError("invalid-file-byte-batch")
        async with asyncio.timeout(timeout), self._condition:
            await self._condition.wait_for(lambda: self._used + total <= self._capacity)
            self._used += total
            return FileBytesReservation(self, ceilings)


class FileBytesReservation:
    """Immutable ceilings and shrink-only charges for one admitted batch."""

    def __init__(self, budget: FileBytesBudget, ceilings: tuple[int, ...]) -> None:
        self._budget, self._ceilings = budget, ceilings
        self._charges = list(ceilings)
        self._closing = False
        self._close_task = None

    @property
    def ceilings(self) -> tuple[int, ...]:
        return self._ceilings

    @property
    def charges(self) -> tuple[int, ...]:
        return tuple(self._charges)

    async def shrink(self, index: int, actual_bytes: int) -> None:
        async with self._budget._condition:
            if self._closing or type(index) is not int or not 0 <= index < len(self._charges) or type(actual_bytes) is not int or not 0 <= actual_bytes <= self._charges[index]:
                raise ValueError("invalid-file-byte-shrink")
            self._budget._used -= self._charges[index] - actual_bytes
            self._charges[index] = actual_bytes
            self._budget._condition.notify_all()

    async def _release(self) -> None:
        async with self._budget._condition:
            self._budget._used -= sum(self._charges)
            self._charges = [0] * len(self._charges)
            self._budget._condition.notify_all()

    async def _join_close(self) -> None:
        await self._close_task

    async def aclose(self) -> None:
        if self._close_task is None:
            self._closing = True
            self._close_task = asyncio.create_task(self._release())
        await await_drained(self._join_close())


def _channel(file):
    bot_id = file.get("telegram_bot_id") if type(file) is dict else None
    if type(bot_id) is not int or bot_id <= 0:
        return None
    from app.telegram_bots.supervisor import current

    supervisor = current()
    return supervisor.channel_for_bot(bot_id) if supervisor is not None else None


async def read_inbound(file, client) -> InboundFileLease | None:
    """Borrow the captured adapter; its bounded reader owns download and notices."""
    from app.channels.inbound_files import current_inbound_file_context

    execution_context = current_inbound_file_context()
    try:
        channel = _channel(file)
        if channel is None:
            return None
        result = await channel.read_inbound_file(file, execution_context=execution_context)
        if result is not None and not isinstance(result, InboundFileLease):
            raise ValueError("managed-file-reader-requires-lease")
        return result
    except Exception as exc:
        logger.warning("telegram_bots file_read_failed error=%s", type(exc).__name__)
        return None


async def write_failed(file, exc: Exception) -> None:
    """Translate a manager write failure without retaining its native traceback."""
    from app.channels.inbound_files import current_inbound_file_context

    execution_context = current_inbound_file_context()
    try:
        channel = _channel(file)
        if channel is None:
            return
        if isinstance(exc, OSError) and exc.errno in {errno.ENOSPC, errno.EDQUOT}:
            await channel.file_own_full(file, execution_context=execution_context)
        else:
            await channel.file_not_saved(file, execution_context=execution_context)
    except Exception as failure:
        logger.warning("telegram_bots file_write_notice_failed error=%s", type(failure).__name__)


class PendingFiles:
    """Pending descriptors share the original admitted inbox and guarded writer."""

    def __init__(self, callbacks, inbox, *, guard_factory, config=None, clock=None):
        from datetime import UTC, datetime

        from app.telegram_bots.config import TelegramBotsConfig

        self.callbacks, self.inbox, self.guard_factory = callbacks, inbox, guard_factory
        self.config = config or TelegramBotsConfig()
        self.clock = clock or (lambda: datetime.now(UTC))

    async def add(self, link, row, file, *, agent_file_path=None, guard=None, session=None):
        from copy import deepcopy

        from app.telegram_bots.inbox import InboxRow, chat_input_parts
        from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramPendingFileRow

        if type(file) is not dict or agent_file_path is not None and (type(agent_file_path) is not str or not agent_file_path or len(agent_file_path) > 1024):
            raise ValueError("pending-file-conflict") from None
        file = deepcopy(file)
        guard = guard or self.guard_factory(link)

        async def perform():
            async with self.callbacks.writer(link, guard, session=session) as writer:
                mark = await writer.get(TelegramInboundMarkRow, (row.link_id, row.message_id, row.chat_id), with_for_update=True, populate_existing=True)
                if (
                    mark is None
                    or (mark.link_id, mark.chat_id, mark.message_thread_id, mark.binding_generation, mark.update_id) != (link.id, link.chat_id, row.message_thread_id, link.binding_generation, row.update_id)
                    or row.binding_generation != link.binding_generation
                    or mark.sanitized_input != row.sanitized_input
                    or mark.route_kind not in (None, "turn")
                    or mark.route_kind == "turn"
                    and mark.route_target_id != link.agent_ref
                    or mark.turn_id is not None
                    or mark.thread_id is not None
                    or mark.run_id is not None
                    or {"submission", "human_input_ui", "publications", "prepublication_group", "pending_files"}.intersection(mark.route_basis)
                ):
                    raise ValueError("pending-file-conflict") from None
                actual = InboxRow.from_row(mark)
                text, descriptors = chat_input_parts(actual, bot_id=link.bot_id)
                if text.strip() or len(descriptors) != 1 or descriptors[0] != file or any(type(file[key]) is not type(value) for key, value in descriptors[0].items()) or file["message_id"] != mark.message_id:
                    raise ValueError("pending-file-conflict") from None
                values = {
                    "chat_id": mark.chat_id,
                    "message_thread_id": mark.message_thread_id,
                    "binding_generation": mark.binding_generation,
                    "file_id": file["file_id"],
                    "file_unique_id": file["file_unique_id"],
                    "filename": file["filename"],
                    "mime_type": file["mime_type"],
                    "size": -1 if file["size"] is None else file["size"],
                    "is_image": file["type"] == "image",
                    "agent_file_path": agent_file_path,
                }
                existing = await writer.get(TelegramPendingFileRow, (link.id, mark.message_id), with_for_update=True, populate_existing=True)
                if existing is not None:
                    if any(getattr(existing, key) != value for key, value in values.items()):
                        raise ValueError("pending-file-conflict") from None
                elif not mark.route_complete:
                    writer.add(TelegramPendingFileRow(link_id=link.id, message_id=mark.message_id, created_at=mark.accepted_at, **values))
                # A completed captionless receipt whose pending row was taken or
                # swept cannot recreate it on provider redelivery.
                await writer.flush()

        return await await_drained(perform())

    async def take(self, link, row, *, guard=None, session=None):
        from datetime import timedelta

        from sqlalchemy import select

        from deerflow.persistence.telegram_bots.model import TelegramPendingFileRow

        guard = guard or self.guard_factory(link)

        async def perform():
            async with self.callbacks.writer(link, guard, session=session) as writer:
                actual, taken = await self.inbox.pending_file_take(link, row, session=writer, guard=guard)
                if taken is not None:
                    return taken
                cutoff = self.clock() - timedelta(hours=self.config.files.pending_ttl_hours)
                selected = list(
                    await writer.scalars(
                        select(TelegramPendingFileRow)
                        .where(
                            TelegramPendingFileRow.link_id == link.id,
                            TelegramPendingFileRow.chat_id == actual.chat_id,
                            TelegramPendingFileRow.message_thread_id == actual.message_thread_id,
                            TelegramPendingFileRow.binding_generation == link.binding_generation,
                            TelegramPendingFileRow.created_at > cutoff,
                        )
                        .order_by(TelegramPendingFileRow.created_at, TelegramPendingFileRow.message_id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                )
                await self.inbox.attach_pending_files(link, actual, selected, session=writer, guard=guard)
                _fresh, taken = await self.inbox.pending_file_take(link, actual, session=writer, guard=guard)
                return taken

        return await await_drained(perform())

    async def sweep(self, now):
        from datetime import timedelta

        from sqlalchemy import delete

        from deerflow.persistence.telegram_bots.model import TelegramPendingFileRow

        cutoff = now - timedelta(hours=self.config.files.pending_ttl_hours)

        async def perform():
            async with self.callbacks.sessions() as writer, writer.begin():
                result = await writer.execute(delete(TelegramPendingFileRow).where(TelegramPendingFileRow.created_at <= cutoff))
                return result.rowcount

        return await await_drained(perform())
