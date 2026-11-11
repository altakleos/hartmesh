"""Bounded receipt-only observation; installed separately from action retries."""

import asyncio
import logging

from sqlalchemy import func, select

from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import OUTBOX_SEND_CONCURRENCY
from app.telegram_bots.work_actions import WORK_ACT_KINDS, same_body
from app.telegram_bots.work_outcome_delivery import delivery_receipt, phase
from deerflow.persistence.telegram_bots.model import TelegramActRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)
_PAGE = 100


class _ObservationStopped(Exception):
    pass


class WorkOutcomeRecovery:
    def __init__(self, plans):
        self.plans, self.acts = plans, plans.acts
        self._tasks = {}
        self._slots = asyncio.Semaphore(OUTBOX_SEND_CONCURRENCY)
        self._stopping = False
        self._cursors = {}

    async def stop(self):
        self._stopping = True

        async def drain():
            results = await asyncio.gather(*tuple(self._tasks.values()), return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result

        if self._tasks:
            await await_drained(drain())

    async def tick(self, link, *, guard):
        if self._stopping or link.id in self._tasks:
            return
        task = self._tasks[link.id] = asyncio.create_task(self._run(link, _ObserverGuard(self, guard)))
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            try:
                await await_drained(self.stop())
            except Exception as exc:
                logger.warning("Work outcome shutdown failed (%s)", type(exc).__name__)
            raise
        finally:
            self._tasks.pop(link.id, None)

    async def _run(self, link, guard):
        try:
            async with self._slots:
                guard.current()
                await self._tick(link, guard)
        except _ObservationStopped:
            return

    async def _tick(self, link, guard):
        await guard.prepare()
        cursor = self._cursors.get(link.id)
        cursor, horizon = cursor[1:] if cursor and cursor[0] == link.binding_generation else (None, None)
        async with self.acts.session_factory() as reader:
            act = TelegramActRow
            query = select(act.id).where(
                act.link_id == link.id,
                act.owner_user_id == link.owner_user_id,
                act.binding_generation == link.binding_generation,
                act.kind.in_(WORK_ACT_KINDS),
            )
            if horizon is None:
                horizon = await reader.scalar(query.with_only_columns(func.max(act.id)))
            if horizon is None:
                self._cursors.pop(link.id, None)
                return
            query = query.where(act.id <= horizon)
            if cursor is not None:
                query = query.where(act.id > cursor)
            ids = list(await reader.scalars(query.order_by(act.id).limit(_PAGE)))
        for identity in ids:
            if self._stopping:
                return
            guard.current()
            if identity in self.acts._work_active:
                continue
            try:
                # Inspecting a receipt never calls check(): that method is an
                # explicit retry and may issue the original command again.
                before = await self.acts._work_record(identity)
                receipt = delivery_receipt(before.continuation)
                if receipt is not None and same_body(receipt["phase"], phase({"state": before.state, "reason": before.reason, "attempts": before.attempts})):
                    continue
                if before.state == "pending" and before.reason is None and before.attempts == 0:
                    continue
                await self.acts.reconcile_receipt(identity)
                guard.current()
                await self.plans.plan(link, identity, guard=guard)
            except Exception as exc:
                # A blocked source must not starve independent eligible Work.
                # Logs contain only a typed classification, never action text.
                logger.warning("Work outcome observation deferred (%s)", type(exc).__name__)
        if len(ids) == _PAGE and ids[-1] != horizon:
            self._cursors[link.id] = (link.binding_generation, ids[-1], horizon)
        else:
            self._cursors.pop(link.id, None)


class _ObserverGuard:
    def __init__(self, owner, base):
        self.owner, self.base = owner, base

    def current(self):
        if self.owner._stopping:
            raise _ObservationStopped()
        self.base.current()

    async def prepare(self):
        await prepare_publication_guard(self.base)
        self.current()

    async def check(self, writer):
        self.current()
        result = await self.base.check(writer)
        self.current()
        return result
