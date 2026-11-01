"""Finite recovery of saved publications; never submits a model input."""

from __future__ import annotations

import asyncio
import logging
import math
import re
from collections import deque
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from app.telegram_bots.inbox import PublicationRecord
from app.telegram_bots.publication import MAX_OBSERVATION_PAGES, PublicationObservation
from deerflow.runtime.runs.terminal import failure_chain
from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from app.telegram_bots.inbox import RecoveryCursor
    from app.telegram_bots.recovery_authority import StartupRecoveryEpoch

logger = logging.getLogger(__name__)
RECOVERY_PAGE_SIZE = 32
RECOVERY_MAX_PAGES = 2
RECOVERY_MAX_OPERATIONS = 4
RECOVERY_MAX_ORIGINS = 64
RECOVERY_TIMEOUT_SECONDS = 30.0


class RecoveryRefused(RuntimeError):
    def __init__(self):
        super().__init__("Publication recovery refused")


class _Retired(Exception):
    pass


@dataclass(frozen=True)
class RecoveryPass:
    scanned: int = 0
    started: int = 0
    planned: int = 0
    pending: int = 0
    held: int = 0
    budget_exhausted: bool = False
    still_owned: int = 0
    next_after: RecoveryCursor | None = None


@dataclass(eq=False)
class _Pass:
    service: PublicationRecoveryService
    deadline: float
    periodic: bool
    epoch: StartupRecoveryEpoch | None = None
    scanned: int = 0
    started: int = 0
    planned: int = 0
    pending: int = 0
    held: int = 0
    retired: bool = False
    tasks: set[asyncio.Task] = field(default_factory=set)

    def check(self):
        if self.retired or self.service._closing or asyncio.get_running_loop().time() >= self.deadline or self.periodic and not self.service._allowed():
            raise _Retired from None
        if not self.periodic:
            try:
                if self.epoch is None or self.epoch.check_current() is not None:
                    raise _Retired
            except Exception:
                raise _Retired from None


class PublicationRecoveryService:
    def __init__(self, inbox, outbox, coordinator, links, allowed):
        self.inbox, self.outbox, self.coordinator, self.links = inbox, outbox, coordinator, links
        self._admission = allowed
        self._closing = False
        self._startup_used = False
        self._epoch = None
        self._cursor = None
        self._units: set[asyncio.Task] = set()
        self._active: dict[tuple[str, str, str], asyncio.Task] = {}
        self._metadata: asyncio.Task | None = None
        self._controls: set[_Pass] = set()
        self._drivers: set[asyncio.Task] = set()
        self._driver: asyncio.Task | None = None
        self._stop_task: asyncio.Task | None = None

    def _allowed(self):
        try:
            return self._admission() is True
        except Exception:
            return False

    def _snapshot(self, control=None):
        return RecoveryPass(
            scanned=control.scanned if control else 0,
            started=control.started if control else 0,
            planned=control.planned if control else 0,
            pending=control.pending if control else 0,
            held=control.held if control else 0,
            budget_exhausted=control is not None and asyncio.get_running_loop().time() >= control.deadline,
            still_owned=sum(not task.done() for task in self._units),
            next_after=self._cursor,
        )

    @staticmethod
    def _key(record):
        return record.scope.link_id, record.turn_id, record.publication_id

    async def _leaf(self, control, factory):
        async def admitted():
            # Check in the actual retained child, before calling its backend.
            control.check()
            return await factory()

        return await await_drained(admitted())

    def _retire(self, control):
        control.retired = True
        if not control.periodic and self._epoch is not None:
            self._epoch.retire()
        for task in tuple(control.tasks):
            if not task.done():
                task.cancel()

    def _track(self, task, control):
        self._units.add(task)
        control.tasks.add(task)

        def settled(done):
            self._units.discard(done)
            control.tasks.discard(done)
            if not done.cancelled():
                done.exception()  # Own late failures without logging private data.

        task.add_done_callback(settled)
        return task

    async def _wait(self, control, tasks):
        control.check()
        if tasks:
            done, _ = await asyncio.wait(tasks, timeout=max(0, control.deadline - asyncio.get_running_loop().time()), return_when=asyncio.FIRST_COMPLETED)
            if not done:
                raise _Retired from None
        control.check()

    def _capacity(self, record):
        return len(self._active) < RECOVERY_MAX_OPERATIONS and self._key(record) not in self._active and all(key[0] != record.scope.link_id for key in self._active)

    def _start(self, control, record, epoch):
        control.check()
        key = self._key(record)
        control.started += 1

        async def perform():
            control.check()
            link = await self._leaf(control, lambda: self.links.get(record.scope.link_id))
            control.check()
            if link is None:
                return "held"
            if epoch is None:
                result = await self.coordinator.observe(record, link, max_pages=MAX_OBSERVATION_PAGES, deadline=control.deadline)
                control.check()
                if type(result) is not PublicationObservation:
                    return "held"
                return result.state if result.state in {"planned", "pending"} else "held"
            attempt = await epoch.capture(record, link)
            control.check()
            try:
                identity = await self._leaf(control, lambda: self.outbox.plan_restart_before_dispatch(record, ticket=attempt.ticket, guard=attempt.fixed_guard))
                return "planned" if type(identity) is str and re.fullmatch(r"[0-9a-f]{32}", identity) else "held"
            except asyncio.CancelledError:
                raise
            except Exception:
                # Lost results can only confirm the exact pair, never repeat
                # the writer or renew a startup capability.
                control.check()
                admission = await epoch.prepare_receipt_read(record)
                control.check()
                receipt = await self._leaf(
                    control,
                    lambda: self.inbox.read_restart_receipt(record, captured_link=admission.captured_link, check_live_owner=admission.check_live_owner, check_current=admission.check_current),
                )
                control.check()
                from app.telegram_bots.inbox import RestartReceipt

                return "planned" if type(receipt) is RestartReceipt and receipt.closure.startup_id == attempt.ticket.startup_id else "held"

        task = self._track(asyncio.create_task(perform(), name="telegram-publication-recovery-unit"), control)
        self._active[key] = task

        def finished(done):
            if self._active.get(key) is done:
                self._active.pop(key)
            try:
                outcome = done.result()
            except BaseException:
                outcome = "held"
            if outcome == "planned":
                control.planned += 1
            elif outcome == "pending":
                control.pending += 1
            else:
                control.held += 1

        task.add_done_callback(finished)

    async def _batch(self, control, records, epoch=None):
        waiting = deque(records)
        try:
            while waiting:
                control.check()
                progressed = False
                for _ in range(len(waiting)):
                    record = waiting.popleft()
                    if self._key(record) in self._active:
                        control.held += 1
                    elif self._capacity(record):
                        self._start(control, record, epoch)
                        progressed = True
                    else:
                        waiting.append(record)
                if waiting or not progressed:
                    await self._wait(control, set(self._active.values()))
            while any(not task.done() for task in control.tasks):
                await self._wait(control, set(control.tasks))
        except _Retired:
            control.held += len(waiting)
            raise

    async def _startup(self, control, epoch):
        control.scanned = len(epoch.records)
        await self._batch(control, epoch.records, epoch)

    async def _periodic(self, control):
        from app.telegram_bots.inbox import RecoveryCursor, RecoveryPage

        for _ in range(RECOVERY_MAX_PAGES):
            control.check()
            if len(self._active) >= RECOVERY_MAX_OPERATIONS:
                return
            # A previous timed-out metadata unit still owns its slot/checkout.
            if self._metadata is not None and not self._metadata.done():
                return
            after = self._cursor
            task = self._track(asyncio.create_task(self._leaf(control, lambda: self.inbox.recovery_page(after=after, limit=RECOVERY_PAGE_SIZE, max_origins=RECOVERY_MAX_ORIGINS)), name="telegram-publication-recovery-page"), control)
            self._metadata = task
            await self._wait(control, {task})
            page = task.result()
            control.check()
            if type(page) is not RecoveryPage or len(page.entries) > RECOVERY_PAGE_SIZE or type(page.has_more) is not bool:
                raise RecoveryRefused from None
            if page.has_more and (type(page.next_after) is not RecoveryCursor or page.next_after == after):
                raise RecoveryRefused from None
            self._cursor = page.next_after if page.has_more else None
            control.scanned += len(page.entries)
            records = []
            for entry in page.entries:
                record = entry.record
                if entry.hold_reason is not None or type(record) is not PublicationRecord or record.dispatch_started is not True:
                    control.held += 1
                else:
                    records.append(record)
            await self._batch(control, records)
            if not page.has_more:
                return

    async def _run(self, control, factory):
        async def driver():
            try:
                control.check()
                await factory()
            except asyncio.CancelledError:
                raise
            except _Retired:
                pass
            except Exception as error:
                logger.warning("telegram_bots publication_recovery_failed cause=%s", failure_chain(error))
                control.held += 1
            finally:
                self._retire(control)
            return self._snapshot(control)

        task = asyncio.create_task(driver(), name="telegram-publication-recovery-pass")
        self._driver = task
        self._drivers.add(task)
        self._controls.add(control)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            self._retire(control)
            task.cancel()
            await await_drained(asyncio.gather(task, return_exceptions=True))
            raise
        finally:
            self._drivers.discard(task)
            self._controls.discard(control)

    async def recover_startup(self, epoch):
        from app.telegram_bots.recovery_authority import StartupRecoveryEpoch

        if type(epoch) is not StartupRecoveryEpoch:
            raise RecoveryRefused from None
        if self._closing or self._startup_used or self._driver is not None and not self._driver.done():
            epoch.retire()
            return self._snapshot()
        if type(epoch.records) is not tuple or len(epoch.records) > RECOVERY_PAGE_SIZE or type(epoch.deadline) not in {int, float} or not math.isfinite(epoch.deadline):
            epoch.retire()
            raise RecoveryRefused from None
        self._startup_used, self._epoch = True, epoch
        control = _Pass(self, epoch.deadline, False, epoch=epoch)
        try:
            return await self._run(control, lambda: self._startup(control, epoch))
        finally:
            epoch.retire()

    async def tick(self):
        if self._closing or not self._allowed() or self._driver is not None and not self._driver.done():
            return self._snapshot()
        control = _Pass(self, asyncio.get_running_loop().time() + RECOVERY_TIMEOUT_SECONDS, True)
        return await self._run(control, lambda: self._periodic(control))

    def begin_shutdown(self):
        self._closing = True
        if self._epoch is not None:
            self._epoch.retire()
        for control in tuple(self._controls):
            self._retire(control)
        for task in tuple(self._drivers):
            task.cancel()

    async def _finish_stop(self):
        self.begin_shutdown()
        while pending := {task for task in self._units | self._drivers if not task.done()}:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    async def stop(self):
        if asyncio.current_task() in self._units | self._drivers:
            raise RecoveryRefused from None
        self.begin_shutdown()
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._finish_stop(), name="telegram-publication-recovery-stop")
        await await_drained(asyncio.shield(self._stop_task))
