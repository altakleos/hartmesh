"""Own managed Telegram clients on the Gateway loop and their durable sender.

Construction alone runs off-loop. Telegram I/O and every asynchronous service
collaborator stay on the Gateway loop; no client subscribes to the bus itself.
"""

from __future__ import annotations

import asyncio
import logging
import os
import traceback
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4
from weakref import WeakKeyDictionary

import httpx
from sqlalchemy import delete
from sqlalchemy.exc import SQLAlchemyError
from telegram.error import RetryAfter, TelegramError

from app.telegram_bots.adapter import LongRetryAfter, SendContext, StrangerLedger, TelegramBotChannel
from app.telegram_bots.api_logging import call_failure_fields
from app.telegram_bots.cipher import token_cipher, unwrap_token, wrap_token
from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.deployment import CopyGuard, feature_reason, static_cause
from app.telegram_bots.links import BotLink, BotLinkRepository
from app.telegram_bots.log_filter import install_token_log_filter
from app.telegram_bots.polling import CONFLICT_GRACE_SECONDS, TASK_DEATHS_BEFORE_STOP, TASK_DEATHS_WINDOW_SECONDS, TASK_RESTART_SECONDS, PollRecord, PollTask
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryEligibility, Outbox, OutboxSender
from app.telegram_bots.render import render_plain
from app.telegram_bots.settings import SettingsRepository, silent_now
from app.telegram_bots.voice import resolve_transcriber
from deerflow.config.app_config import get_app_config
from deerflow.config.ui_config import product_name
from deerflow.persistence.engine import get_session_factory
from deerflow.persistence.telegram_bots.model import TelegramBotLinkEventRow
from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from app.channels.message_bus import OutboundMessage

START_CONCURRENCY = 10
HEALTH_WAIT_SECONDS, HEALTH_WAIT_INTERVAL_SECONDS = 60.0, 0.5
STOP_TIMEOUT_SECONDS = 5.0
NOTICE_EVERY_HOURS = 24
RETENTION_SWEEP_SECONDS = 3600
LINK_EVENT_RETENTION_DAYS = 400

logger = logging.getLogger(__name__)
_current: TelegramSupervisor | None = None


def current() -> TelegramSupervisor | None:
    return _current


def _now() -> datetime:
    return datetime.now(UTC)


_LINES = {
    "off": "telegram: off (HARTMESH_TELEGRAM=off)",
    "auth-disabled": "telegram: not started (authentication is disabled)",
    "unbound-allowed": "telegram: not started (channel_connections.enabled and channel_connections.require_bound_identity must both be true)",
    "secret-missing": "telegram: not started (HARTMESH_PROVIDER_KEYS_SECRET is not set)",
    "secret-invalid": "telegram: not started (HARTMESH_PROVIDER_KEYS_SECRET is shorter than 32 characters)",
    "host-changed": "telegram: paused (host-changed); run python -m app.telegram_bots.cli adopt",
    "polled-elsewhere": "telegram: paused (polled-elsewhere); run python -m app.telegram_bots.cli resume",
    "channel-service-failed": "telegram: not started (the channel service failed to start)",
    "gateway-not-ready": "telegram: not started (the Gateway did not answer its own health check in 60 s)",
}


class TelegramSupervisor:
    def __init__(
        self,
        app,
        *,
        config: TelegramBotsConfig,
        channel_service,
        session_factory=None,
        links=None,
        outbox=None,
        settings=None,
        authority=None,
        copy_guard=None,
        thread_client=None,
        app_config=None,
        environ: Mapping[str, str] | None = None,
        request_factory=None,
        health_client_factory=httpx.AsyncClient,
        channel_factory=TelegramBotChannel,
        clock: Callable[[], datetime] = _now,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        global _current
        _current = self
        install_token_log_filter()
        logging.getLogger("telegram").setLevel(logging.INFO)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        self.app, self.config, self.channel_service = app, config, channel_service
        self.session_factory, self.links, self.outbox, self.settings = session_factory, links, outbox, settings
        self.authority, self._app_config = authority, app_config
        self.strangers = StrangerLedger()
        self.copy_guard = copy_guard
        self.thread_client, self.threads = thread_client, None
        self.publication_authority = self.publications = self.model_source = None
        self._publication_reads_open = False
        self._environ = dict(os.environ if environ is None else environ)
        if request_factory is not None:
            self.request_factory = request_factory
        self._health_client_factory, self._channel_factory = health_client_factory, channel_factory
        self._clock, self._monotonic, self._sleep = clock, monotonic, sleep
        self.started_at, self.run_id = clock(), uuid4().hex
        self.tls_context = None
        self.sender: OutboxSender | None = None
        self._cause: str | None = None
        self._checked, self._stopping, self._stopped = False, False, False
        self._start_started = False
        self._check_lock = asyncio.Lock()
        self._health_barrier, self._ready = asyncio.Event(), False
        self._send_event, self._send_ready = asyncio.Event(), False
        self._copy_wake = asyncio.Event()
        self._copy_task: asyncio.Task | None = None
        self._latch_task: asyncio.Task | None = None
        self._latch_retired = asyncio.Event()
        self._latch_persisted = False
        self._unpersisted_logged = False
        self._copy_decision: bool | None = None
        self._cohort_complete = False
        self._initial_links: dict[str, BotLink] = {}
        self._startup_started: set[str] = set()
        self._startup_links: tuple[BotLink, ...] = ()
        self._startup_records: dict[str, tuple[int, PollRecord]] = {}
        self._startup_polls: set[PollTask] = set()
        self._first_poll_at: float | None = None
        self._start_task: asyncio.Task | None = None
        self._stop_task: asyncio.Task | None = None
        self._start_limit = asyncio.Semaphore(START_CONCURRENCY)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._reconcile_lock = asyncio.Lock()
        self._channels: dict[str, TelegramBotChannel] = {}
        self._polls: dict[str, PollTask] = {}
        self._runners: dict[str, asyncio.Task] = {}
        self._rows: dict[str, BotLink] = {}
        self._operations: dict[asyncio.Task, str] = {}
        self._retirements: set[asyncio.Task] = set()
        self._retire_by_link: dict[str, asyncio.Task] = {}
        self._retiring_channels: set[TelegramBotChannel] = set()
        self._background: set[asyncio.Task] = set()
        self._callbacks: set[asyncio.Task] = set()
        self._notices: set[asyncio.Task] = set()
        self._restarts: dict[str, asyncio.Task] = {}
        self._deaths: dict[tuple[str, int], deque[float]] = defaultdict(deque)
        self._death_observed = WeakKeyDictionary()
        self._unsupported: set[str] = set()
        self._subscribed = False
        self._listener = self.dispatch_outbound

    def _time(self) -> float:
        return self._monotonic() if self._monotonic else asyncio.get_running_loop().time()

    def _event(self, event: str, *, link: BotLink | None = None, level=logging.INFO, **facts) -> None:
        if link is not None:
            facts = {"link": link.id, "bot": link.bot_id, **facts}
        logger.log(level, "telegram_bots %s %s", event, " ".join(f"{key}={value}" for key, value in facts.items() if value is not None))

    def _observe(self, saved: BotLink, *, reason: str) -> None:
        before = self._rows.get(saved.id)
        self._rows[saved.id] = saved
        if before is None:
            return
        if before.health != saved.health:
            self._event("health", link=saved, level=logging.INFO if saved.health == "ok" else logging.WARNING, **{"from": before.health, "to": saved.health, "reason": reason})
        if before.state != saved.state:
            self._event("state", link=saved, **{"from": before.state, "to": saved.state, "reason": "held" if saved.state == "held" else "restored" if before.state == "held" else "forbidden" if saved.state == "blocked" else "inbound"})

    def _refuse(self, cause: str, **observations) -> None:
        self._cause, self._ready, self._send_ready = cause, False, False
        line = _LINES[cause]
        if cause == "secret-invalid":
            previous = self._environ.get("HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS", "").strip()
            present = self._environ.get("HARTMESH_PROVIDER_KEYS_SECRET", "").strip()
            if len(present) >= 32 and previous and len(previous) < 32:
                line = line.replace("HARTMESH_PROVIDER_KEYS_SECRET is", "HARTMESH_PROVIDER_KEYS_SECRET_PREVIOUS is")
        logger.info(line)
        if cause == "polled-elsewhere":
            observations = {"polled": 0, "counted": 0, "people": 0, **observations}
        self._event("polling_refused", reason=cause, links=len(self._rows), level=logging.INFO if cause == "off" else logging.ERROR, **observations)
        self._health_barrier.set()
        self._send_event.set()

    async def check(self) -> None:
        async with self._check_lock:
            await self._check_once()

    async def _check_once(self) -> None:
        if self._checked:
            return
        self._checked = True
        try:
            if self._app_config is None:
                self._app_config = await asyncio.to_thread(get_app_config)
            cause = await asyncio.to_thread(static_cause, self.config, self._app_config, self._environ)
            if cause is not None:
                self._refuse(cause)
                return
            manager = self.channel_service.manager if self.channel_service is not None else None
            connections = getattr(manager, "connection_repo", None)
            if self.session_factory is None:
                self.session_factory = get_session_factory()
            if self.session_factory is None:
                self._refuse("channel-service-failed")
                return
            self.links = self.links or BotLinkRepository(self.session_factory, connections, self.config)
            # Freeze membership and original conflict facts before check returns
            # and the Gateway admits routes that can create/start other links.
            initial = await self.links.list_all()
            self._initial_links = {link.id: link for link in initial}
            self._rows = dict(self._initial_links)
            self.copy_guard = self.copy_guard or CopyGuard(self.session_factory, clock=self._clock)
            verdict = await self.copy_guard.at_start()
            if self.copy_guard.this_host_id is None:
                self._event("host_unreadable", level=logging.WARNING)
            if verdict != "running":
                self._refuse(verdict.replace("_", "-"))
                return
            if manager is None or connections is None:
                self._refuse("channel-service-failed")
                return
            if len(self._initial_links) <= 1:
                # This complete original population cannot meet the predicate;
                # its first update need not wait for startup announcements.
                self._copy_decision = False
            self.settings = self.settings or SettingsRepository(self.session_factory, self.config)
            self.outbox = self.outbox or Outbox(self.session_factory, config=self.config, clock=self._clock)
            if self.authority is None:
                from app.telegram_bots.authority import TelegramAuthority

                self.authority = TelegramAuthority(links=self.links, connections=connections)
            self.sender = OutboxSender(self.outbox, run_id=self.run_id, deliver_part=self.deliver_part, eligibility=self.eligibility)
        except Exception as error:
            self._event("supervisor_check_failed", error=type(error).__name__, level=logging.ERROR)
            self._refuse("channel-service-failed")

    async def _wait_health(self) -> bool:
        deadline = self._time() + HEALTH_WAIT_SECONDS
        url = self.channel_service.manager.gateway_url.rstrip("/") + "/health"
        async with self._health_client_factory(verify=self.tls_context, trust_env=False) as client:
            while not self._stopping:
                remaining = deadline - self._time()
                if remaining <= 0:
                    return False
                try:
                    response = await asyncio.wait_for(client.get(url, timeout=min(HEALTH_WAIT_INTERVAL_SECONDS, remaining)), timeout=min(HEALTH_WAIT_INTERVAL_SECONDS, remaining))
                    if response.status_code == 200:
                        return True
                except (httpx.HTTPError, TimeoutError):
                    pass
                remaining = deadline - self._time()
                if remaining <= 0:
                    return False
                await self._sleep(min(HEALTH_WAIT_INTERVAL_SECONDS, remaining))
        return False

    async def start(self) -> None:
        if self._start_started:
            return
        self._start_started = True
        self._start_task = asyncio.current_task()
        self.started_at = self._clock()
        began = self._time()
        try:
            await self.check()
            if self._cause is not None or self._stopping:
                return
            self.channel_service.bus.subscribe_outbound(self._listener)
            self._subscribed = True
            self.tls_context = await await_drained(asyncio.to_thread(httpx.create_ssl_context))
            from app.telegram_bots.gateway_client import GatewayClientOwner
            from app.telegram_bots.model_source import ModelSource
            from app.telegram_bots.publication import PublicationCoordinator
            from app.telegram_bots.publication_authority import PublicationAttemptAuthority
            from app.telegram_bots.threads import ThreadCoordinator

            self.thread_client = self.thread_client or GatewayClientOwner(gateway_url=self.channel_service.manager.gateway_url, langgraph_url=self.channel_service.manager.langgraph_url, tls_context=self.tls_context)
            self.threads = ThreadCoordinator(links=self.links, authority=self.authority, client=self.thread_client, channel=self.channel, send_allowed=self.send_allowed, outbox=self.outbox, product_name=self.product_name)
            self.publication_authority = PublicationAttemptAuthority(links=self.links, authority=self.authority, client=self.thread_client, channel=self.channel, create_allowed=self.send_allowed, read_allowed=self.publication_read_allowed)
            self.publications = PublicationCoordinator(inbox=self.outbox.inbox, outbox=self.outbox, bus=self.channel_service.bus, http_owner=self.thread_client, capture_attempt=self.publication_authority.capture)
            self.model_source = ModelSource(read_output=self.publications.read_output)
            await self.thread_client.start()
            if not await self._wait_health():
                if not self._stopping:
                    self._refuse("gateway-not-ready")
                self._unsubscribe()
                return
            self._ready = True
            self._publication_reads_open = True
            self._health_barrier.set()
            rows = tuple(self._initial_links.values())
            self._event("supervisor_start", links=len(rows), concurrency=START_CONCURRENCY)
            await asyncio.gather(*(self._launch(row, fresh_start=True) for row in rows))
            if self._stopping or self._cause is not None:
                return
            status = self.status()
            try:
                transcriber = await asyncio.to_thread(resolve_transcriber, self.config)
                voice = getattr(transcriber, "provider", "unavailable") if transcriber is not None else "off" if self.config.voice.provider == "off" else "unavailable"
            except Exception:
                voice = "off" if self.config.voice.provider == "off" else "unavailable"
            logger.info(
                "telegram: on, %s %s for %s %s, %s polling, %s failing, %s held; voice %s",
                status["bots"],
                "bot" if status["bots"] == 1 else "bots",
                status["people"],
                "person" if status["people"] == 1 else "people",
                status["polling"],
                status["failing"],
                status["held"],
                voice,
            )
            self._event("supervisor_ready", links=status["bots"], ok=status["polling"], failing=status["failing"], took_ms=round((self._time() - began) * 1000))
            self._startup_links = tuple(link for identity, link in self._initial_links.items() if identity in self._startup_started)
            self._cohort_complete = True
            self._copy_task = asyncio.create_task(self._coordinate_copy_start(), name="telegram-copy-decision")
            await self._copy_task
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._event("supervisor_start_failed", error=type(error).__name__, level=logging.ERROR)
            if self._cause is None:
                self._refuse("channel-service-failed")
            await self.begin_shutdown()
            self._unsubscribe()
        finally:
            if self._start_task is asyncio.current_task():
                self._start_task = None
            if not self._ready:
                self._health_barrier.set()

    def _copy_results(self) -> dict[str, PollRecord]:
        return {identity: record for identity, (_, record) in self._startup_records.items()}

    def _pending_copy_decision(self) -> bool:
        return self._copy_decision is None

    async def _wait_send_release(self) -> bool:
        await self._send_event.wait()
        return self._send_ready and self._cause is None and not self._stopping

    def _poll_dispatched(self) -> None:
        if self._first_poll_at is None:
            self._first_poll_at = self._time()
        self._copy_wake.set()

    def _poll_result(self) -> bool:
        if self._copy_decision is None and self._cohort_complete and not self.copy_guard.can_still_be_broad_conflict(self._startup_links, self._copy_results()):
            self._copy_decision = False
        self._copy_wake.set()
        return self._copy_decision is not False

    async def _wait_copy_change(self, delay: float | None) -> None:
        if delay is None:
            await self._copy_wake.wait()
            return
        changed = asyncio.create_task(self._copy_wake.wait())
        timer = asyncio.create_task(self._sleep(delay))
        try:
            await asyncio.wait((changed, timer), return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (changed, timer):
                if not task.done():
                    task.cancel()
            await asyncio.gather(changed, timer, return_exceptions=True)

    async def _coordinate_copy_start(self) -> None:
        # The complete original population/actual-started cohort is announced
        # before this retained owner can release senders or persist a latch.
        while self._copy_decision is None and not self._stopping:
            self._copy_wake.clear()
            results = self._copy_results()
            if not self.copy_guard.can_still_be_broad_conflict(self._startup_links, results):
                self._copy_decision = False
                break
            remaining = None if self._first_poll_at is None else self._first_poll_at + CONFLICT_GRACE_SECONDS - self._time()
            if remaining is not None and remaining <= 0:
                polled = tuple(link for link in self._startup_links if link.id in results and results[link.id].polls > 0)
                self._copy_decision = self.copy_guard.broad_conflict(polled, results)
                break
            await self._wait_copy_change(remaining)
        if self._stopping:
            return
        if self._copy_decision:
            await self._latch_copy_conflict()
            return
        # Transfer every suffix promptly; failed admission remains its poller's
        # job and never blocks another link or the sender's startup.
        current_polls = set(self._polls.values())
        for retired in self._startup_polls - current_polls:
            retired.discard_held()
        self._startup_polls.clear()
        for poll in tuple(current_polls):
            if poll.pending_drop:
                poll.discard_held()
            await poll.release_held()
        while not self._stopping:
            try:
                await self.sender.recover_prior_claims()
                break
            except SQLAlchemyError as error:
                self._event("claim_recovery_failed", error=type(error).__name__, level=logging.WARNING)
                await self._sleep(1)
        if self._stopping or self._cause is not None:
            return
        self._send_ready = True
        self._send_event.set()
        for poll in tuple(self._polls.values()):
            poll.wake()
        self._periodic(self._tick_sender, 1, "sender")
        self._periodic(self.reconcile, self.config.reconcile_interval_seconds, "reconcile")
        self._periodic(self._refresh_flags, 3600, "bot-flags")
        self._periodic(self.sweep_retention, RETENTION_SWEEP_SECONDS, "retention")

    async def _latch_copy_conflict(self) -> None:
        # Refuse locally before any await, cancellation or SQL persistence.
        self._cause, self._send_ready = "polled-elsewhere", False
        self._send_event.set()
        self._latch_task = asyncio.create_task(self._complete_copy_latch(), name="telegram-copy-latch-persistence")
        await asyncio.shield(self._latch_task)

    async def _complete_copy_latch(self) -> None:
        own = asyncio.current_task()
        tasks = {*self._background, *self._runners.values(), *self._restarts.values(), *self._operations, *self._notices, *self._callbacks}
        tasks.discard(own)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.sender is not None:
            await self.sender.stop()
        polls = set(self._polls.values()) | self._startup_polls
        dropped = sum(poll.discard_held() for poll in polls)
        self._startup_polls.clear()
        self._event("updates_dropped", n=dropped, level=logging.WARNING)
        results = self._copy_results()
        polled = tuple(link for link in self._startup_links if link.id in results and results[link.id].polls > 0)
        observations = self.copy_guard.broad_conflict_counts(polled, results)
        self._refuse("polled-elsewhere", **observations)
        await self._persist_copy_latch()

    def _unpersisted_latch(self) -> None:
        if not self._unpersisted_logged:
            self._unpersisted_logged = True
            logger.error("telegram_bots copy_latch_unpersisted")

    async def _latch_backoff(self) -> None:
        retired = asyncio.create_task(self._latch_retired.wait())
        backoff = asyncio.create_task(self._sleep(1))
        try:
            await asyncio.wait((retired, backoff), return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (retired, backoff):
                if not task.done():
                    task.cancel()
            await asyncio.gather(retired, backoff, return_exceptions=True)

    async def _persist_copy_latch(self) -> None:
        while True:
            if self._latch_retired.is_set():
                self._unpersisted_latch()
                return
            try:
                # Owner retirement never cancels this already-started attempt.
                await self.copy_guard.stop_for_broad_conflict()
            except SQLAlchemyError as error:
                self._event("copy_latch_write_failed", error=type(error).__name__, level=logging.ERROR)
                if self._latch_retired.is_set():
                    self._unpersisted_latch()
                    return
                await self._latch_backoff()
            else:
                self._latch_persisted = True
                return

    def _periodic(self, operation, seconds: float, name: str) -> None:
        async def run():
            while not self._stopping and self._cause is None:
                await self._sleep(seconds)
                if self._stopping or self._cause is not None:
                    return
                try:
                    await operation()
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self._event("maintenance_failed", error=type(error).__name__, level=logging.WARNING)

        task = asyncio.create_task(run(), name=f"telegram-{name}")
        self._background.add(task)
        task.add_done_callback(self._background.discard)

    async def _available(self) -> bool:
        await self._health_barrier.wait()
        return self._ready and self._cause is None and not self._stopping

    async def _open_token(self, link: BotLink) -> tuple[BotLink, str] | None:
        while not self._stopping:
            try:
                stored, revision = await self.links.token(link.id)
            except ValueError:
                if await self.links.get(link.id) is None:
                    return None
                raise
            current_link = await self.links.get(link.id)
            if current_link is None:
                return None
            link = current_link
            if revision != link.token_revision:
                continue
            try:
                cipher = await asyncio.to_thread(token_cipher, self._environ)
                opened = await asyncio.to_thread(unwrap_token, cipher, link.bot_id, stored)
            except Exception:
                saved = await self.links.set_health(link.id, "secret_unreadable", holds=True, expected_token_revision=revision)
                if saved is not None:
                    self._observe(saved, reason="secret")
                return None
            if opened.under_previous:
                wrapped = await asyncio.to_thread(wrap_token, cipher, link.bot_id, opened.key)
                try:
                    link = await self.links.store_token(link.id, wrapped, actor_id="system", action="rewrap", expected_revision=revision, expected_encrypted_token=stored)
                except ValueError:
                    continue
            if await self.links.update_if_current(link.id, token_revision=revision) is None:
                continue
            return link, opened.key
        return None

    async def _launch(self, link: BotLink, token: str | None = None, *, fresh_start=False, drop_pending=False) -> None:
        if not await self._available():
            return
        retirement = self._retire_by_link.get(link.id)
        if retirement is not None:
            await asyncio.shield(retirement)
        for retired in tuple(self._retiring_channels):
            if retired.link.id == link.id:
                await self.sender.retire_link(link.id)
                await self._close_owned(retired)
                self._retiring_channels.discard(retired)
        if self._stopping or self._cause is not None:
            return
        task = asyncio.create_task(self._start_link(link, token, fresh_start=fresh_start, drop_pending=drop_pending), name=f"telegram-start-{link.id}")
        self._operations[task] = link.id
        try:
            await task
        except asyncio.CancelledError:
            # Rotation can retire one scan entry without cancelling the whole
            # startup scan and losing its sender/reconcile initialization.
            if asyncio.current_task().cancelling():
                raise
        finally:
            self._operations.pop(task, None)
        if drop_pending and self._send_ready and not self._stopping:
            channel, poll = self.channel(link.id), self.poll_task(link.id)
            if channel is not None and poll is not None and channel.application.running and channel.transport_revision == link.token_revision and channel.link.binding_generation == link.binding_generation:
                await poll.complete_pending_drop()

    async def _start_link(self, link: BotLink, token: str | None, *, fresh_start: bool, drop_pending: bool) -> None:
        captured_revision = link.token_revision
        async with self._start_limit, self._locks[link.id]:
            retirement = self._retire_by_link.get(link.id)
            retiring_client = any(client.link.id == link.id for client in self._retiring_channels)
            if self._stopping or self._cause is not None or retirement is not None and not retirement.done() or retiring_client:
                return
            saved = await self.links.get(link.id)
            if saved is None:
                return
            link = saved
            self._rows[link.id] = link
            existing = self._channels.get(link.id)
            if existing is not None:
                return
            if fresh_start and link.health == "stopped":
                link = await self.links.set_health(link.id, "stopped", holds=False, expected_token_revision=link.token_revision)
                if link is None:
                    return
                self._observe(link, reason="recovered")
            try:
                target = await self.authority.target(link)
            except Exception:
                target = "agent_unavailable"
            if target == "unsupported":
                self._unsupported.add(link.id)
                return
            self._unsupported.discard(link.id)
            if target in {"agent_unavailable", "policy_off"}:
                link = await self.links.set_health(link.id, target, holds=True, expected_token_revision=link.token_revision)
                if link is None:
                    return
                self._observe(link, reason="policy" if target == "policy_off" else "agent")
            if link.state == "held" or target == "held" or link.health in {"secret_unreadable", "token_rejected", "policy_off", "stopped"}:
                return
            # Supplied tokens are trusted only for the exact link revision the
            # caller captured. Otherwise reopen the current encrypted token.
            if token is None or captured_revision != link.token_revision:
                opened = await self._open_token(link)
                if opened is None:
                    return
                link, token = opened
            construction = asyncio.create_task(asyncio.to_thread(self._channel_factory, link=link, token=token, supervisor=self, monotonic=self._monotonic, sleep=self._sleep, now=self._clock))
            channel = None
            try:
                try:
                    channel = await asyncio.shield(construction)
                except asyncio.CancelledError:
                    # A cancelled awaiter cannot cancel a Python constructor.
                    # Retain both permit and result until loop-owned cleanup.
                    channel = await self._finish_owned(construction)
                    await self._close_owned(channel)
                    raise
                if self._stopping or self._cause is not None or await self.links.update_if_current(link.id, token_revision=link.token_revision) is None:
                    await self._close_owned(channel)
                    return
                self._channels[link.id] = channel
                initial_failure = None
                try:
                    await channel.start()
                    self._rows[link.id] = channel.link
                    self._event("bot_start", link=channel.link, can_manage_bots=channel.link.can_manage_bots, has_topics=channel.link.topics_enabled)
                except TelegramError as error:
                    initial_failure = (error, self._clock())
                    diagnostic = call_failure_fields(error)
                    if isinstance(error, (LongRetryAfter, RetryAfter)):
                        diagnostic.update(http=429, retry_in=error.seconds if isinstance(error, LongRetryAfter) else error._retry_after.total_seconds())
                    self._event("bot_start_failed", link=link, level=logging.WARNING, **diagnostic)
                current_link = await self.links.update_if_current(link.id, token_revision=channel.transport_revision)
                if self._stopping or current_link is None:
                    self._channels.pop(link.id, None)
                    await self._close_owned(channel)
                    return
                self._observe(current_link, reason="invalid-token" if current_link.health == "token_rejected" else "recovered")
                if current_link.state != "held" and current_link.health not in {"secret_unreadable", "token_rejected", "policy_off", "stopped"}:
                    poll = self._make_poll(channel)
                    if initial_failure is not None:
                        poll.defer_start_retry(initial_failure[0], observed_at=initial_failure[1])
                    if drop_pending:
                        poll.require_drop_pending()
                    self._start_poll(channel, poll)
            except BaseException:
                if channel is not None:
                    if self._channels.get(link.id) is channel:
                        self._channels.pop(link.id, None)
                    await self._close_owned(channel)
                raise

    def _make_poll(self, channel: TelegramBotChannel) -> PollTask:
        poll = None

        async def health(cause, holds):
            async with self._locks[channel.link.id]:
                if self._stopping or self._channels.get(channel.link.id) is not channel or self._polls.get(channel.link.id) is not poll:
                    return
                transition = poll.health_transition
                kwargs = {"transition": transition.as_dict()} if transition is not None else {}
                saved = await self.links.set_health(channel.link.id, cause, holds=holds, expected_token_revision=channel.transport_revision, **kwargs)
                if saved is None:
                    return
                reason = (
                    "network"
                    if cause == "unreachable"
                    else "webhook"
                    if cause == "conflict" and holds and transition is not None and transition.webhook_host
                    else "second-poller"
                    if cause == "conflict" and holds
                    else "probe-ok"
                    if cause == "conflict"
                    else "recovered"
                )
                self._observe(saved, reason=reason)
                channel.link = saved
            if cause == "conflict":
                self._schedule_notice(channel, "link.conflict" if holds else "link.conflict_over", throttle=True)
            elif not holds and cause == "unreachable" and poll.back_online_due:
                self._schedule_notice(channel, "link.back_online", throttle=True)

        identity, revision = channel.link.id, channel.transport_revision
        record = PollRecord()
        if identity in self._initial_links:
            previous = self._startup_records.get(identity)
            if previous is not None and previous[0] == revision:
                record = previous[1]
            else:
                self._startup_records[identity] = (revision, record)

        def current_pair():
            return self._cause is None and self._channels.get(identity) is channel and self._polls.get(identity) is poll

        def on_result():
            return self._poll_result() if current_pair() else True

        def on_dispatch():
            if current_pair():
                self._poll_dispatched()

        def control_allowed(mode):
            if not current_pair():
                return False
            return self._copy_decision is False if mode == "confirm" else self._send_ready and not self._stopping

        poll = PollTask(
            channel, on_health=health, record=record, on_result=on_result, on_dispatch=on_dispatch, decision_pending=self._pending_copy_decision, control_allowed=control_allowed, is_current=current_pair, clock=self._clock, sleep=self._sleep
        )

        return poll

    def _start_poll(self, channel: TelegramBotChannel, poll: PollTask) -> None:
        identity = channel.link.id
        self._polls[identity] = poll
        if not self._cohort_complete:
            if identity in self._initial_links:
                self._startup_started.add(identity)
            self._startup_polls.add(poll)
        task = asyncio.create_task(poll.run(), name=f"telegram-poll-{identity}")
        self._runners[identity] = task

        def done(completed):
            callback = asyncio.create_task(self._complete_poll(channel, poll, completed), name=f"telegram-poll-complete-{identity}")
            self._callbacks.add(callback)
            callback.add_done_callback(self._callbacks.discard)

        task.add_done_callback(done)

    async def _complete_poll(self, channel, poll, task) -> None:
        while not self._stopping:
            try:
                await self._poll_done(channel, poll, task)
                return
            except SQLAlchemyError as error:
                self._event("poll_death_write_failed", link=channel.link, error=type(error).__name__, level=logging.WARNING)
                await self._sleep(1)

    async def _poll_done(self, channel: TelegramBotChannel, poll: PollTask, task: asyncio.Task) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is None:
            return
        identity = channel.link.id
        async with self._locks[identity]:
            if self._stopping or self._cause is not None or self._channels.get(identity) is not channel or self._polls.get(identity) is not poll or self._runners.get(identity) is not task:
                return
            saved = await self.links.update_if_current(identity, token_revision=channel.transport_revision)
            if saved is None:
                return
            deaths = self._deaths[(identity, channel.transport_revision)]
            now = self._time()
            while deaths and now - deaths[0] > TASK_DEATHS_WINDOW_SECONDS:
                deaths.popleft()
            first_observation = task not in self._death_observed
            if first_observation:
                deaths.append(now)
                self._death_observed[task] = now
            # Format stack frames without the exception's potentially secret
            # SDK message. The global filter remains a second boundary.
            if first_observation:
                stack = "".join(await asyncio.to_thread(traceback.format_tb, error.__traceback__))
                logging.getLogger("app.telegram_bots.polling").error("telegram_bots poll_task_died link=%s bot=%s error=%s n=%s\n%s", identity, saved.bot_id, type(error).__name__, len(deaths), stack)
            if len(deaths) >= TASK_DEATHS_BEFORE_STOP:
                saved = await self.links.set_health(identity, "stopped", holds=True, expected_token_revision=channel.transport_revision)
                if saved is not None:
                    self._observe(saved, reason="task-died")
                return
            restart = asyncio.create_task(self._restart_poll(channel, poll), name=f"telegram-restart-{identity}")
            self._restarts[identity] = restart

    async def _restart_poll(self, channel: TelegramBotChannel, previous: PollTask) -> None:
        await self._sleep(TASK_RESTART_SECONDS)
        while not self._stopping:
            try:
                async with self._locks[channel.link.id]:
                    if self._cause is not None or self._channels.get(channel.link.id) is not channel or self._polls.get(channel.link.id) is not previous:
                        return
                    saved = await self.links.update_if_current(channel.link.id, token_revision=channel.transport_revision)
                    if saved is None or saved.state == "held" or saved.health in {"secret_unreadable", "token_rejected", "policy_off", "stopped"}:
                        return
                    channel.link = saved
                    self._start_poll(channel, self._make_poll(channel))
                    return
            except SQLAlchemyError as error:
                self._event("poll_restart_write_failed", link=channel.link, error=type(error).__name__, level=logging.WARNING)
                await self._sleep(1)

    def send_allowed(self) -> bool:
        """Final synchronous admission after transport's last awaited guard."""
        return self._send_ready and self._cause is None and not self._stopping

    def publication_read_allowed(self) -> bool:
        """Retain authorized final reads through the manager's phase-one drain."""
        return self._publication_reads_open and self._cause is None and not self._stopped

    async def prepare_live_owner(self, owner_user_id: str):
        return await self.authority.prepare_live_owner(owner_user_id)

    @property
    def product_name(self) -> str:
        return product_name(self._app_config)

    async def prepare_ingress_values(self, link: BotLink) -> dict:
        from app.telegram_bots.pages import page_address
        from deerflow.config.agents_config import load_agent_config

        def prepare():
            product = product_name(self._app_config)
            name = product
            if link.agent_kind == "custom_agent":
                target = load_agent_config(link.agent_ref, user_id=link.owner_user_id)
                name = target.display_name or target.name
            elif link.agent_kind != "lead_agent":
                raise ValueError("unsupported-ingress-target")
            return {"product": product, "name": name, "agent": name, "memory_url": page_address("web.memory", link)}

        return await await_drained(asyncio.to_thread(prepare))

    async def notice_access_off(self, link: BotLink) -> None:
        channel = self.channel(link.id)
        if channel is None or channel.transport_revision != link.token_revision:
            return
        current = await await_drained(self.links.update_if_current(link.id, token_revision=link.token_revision))
        scope = ("owner_user_id", "bot_id", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
        if current is None or current.state != "held" or any(getattr(current, key) != getattr(link, key) for key in scope) or self.channel(link.id) is not channel:
            return
        channel.link = current
        await self._notice(channel, "access.off")

    async def eligibility(self, link, item, part=None) -> DeliveryEligibility:
        if self._stopping or self._cause is not None or not self._send_ready:
            return DeliveryEligibility.wait()
        verdict = await self.authority.delivery(link, item, part)
        if self._stopping or self._cause is not None or not self._send_ready:
            return DeliveryEligibility.wait()
        return verdict

    async def protect_notice(self, context: SendContext) -> bool:
        channel = self.channel(context.link_id)
        if channel is None or channel.transport_revision != context.token_revision or self._cause is not None or not self._send_ready:
            raise DeliveryClaimRetired
        link = await self.links.update_if_current(context.link_id, token_revision=context.token_revision)
        if link is None or link.binding_generation != context.binding_generation or (context.purpose == "bound_message" or context.purpose == "optional" and link.chat_id is not None) and link.chat_id != context.chat_id:
            raise DeliveryClaimRetired
        protected = await self.authority.protect_notice(context)
        link = await self.links.update_if_current(context.link_id, token_revision=context.token_revision)
        if (
            not self._send_ready
            or self._stopping
            or self._cause is not None
            or self.channel(context.link_id) is not channel
            or link is None
            or link.binding_generation != context.binding_generation
            or (context.purpose == "bound_message" or context.purpose == "optional" and link.chat_id is not None)
            and link.chat_id != context.chat_id
        ):
            raise DeliveryClaimRetired
        return protected

    async def deliver_part(self, *, link_id: str, outbox_id: str, part_id: str, claim_owner: str, claim_id: str) -> list[int]:
        if self._stopping or self._cause is not None or not self._send_ready:
            raise DeliveryClaimRetired
        channel = self.channel(link_id)
        if channel is None:
            raise DeliveryClaimRetired
        return await channel.send_part(outbox_id=outbox_id, part_id=part_id, claim_owner=claim_owner, claim_id=claim_id)

    async def dispatch_outbound(self, msg: OutboundMessage) -> None:
        if msg.channel_name != "telegram" or not isinstance(msg.metadata, dict):
            return
        bot_id = msg.metadata.get("telegram_bot_id")
        if type(bot_id) is not int:
            return
        channel = self.channel_for_bot(bot_id)
        if channel is None:
            return
        link = await self.links.update_if_current(channel.link.id, token_revision=channel.transport_revision)
        if link is None or self.channel(link.id) is not channel or msg.connection_id is not None and msg.connection_id != link.connection_id:
            return
        # Current manager frames lack the authoritative final source introduced
        # by PLAN1.14. The adapter rejects raw text, including inherited metadata.
        await channel.send(msg)

    async def _tick_sender(self) -> None:
        if not self._stopping and self._cause is None and self._send_ready:
            await self.sender.tick()

    def channel(self, link_id: str) -> TelegramBotChannel | None:
        return self._channels.get(link_id)

    def poll_task(self, link_id: str) -> PollTask | None:
        return self._polls.get(link_id)

    def channel_for_bot(self, bot_id: int) -> TelegramBotChannel | None:
        if type(bot_id) is not int:
            return None
        return next((channel for channel in self._channels.values() if channel.link.bot_id == bot_id), None)

    def cause(self) -> str | None:
        return self._cause

    def reason(self) -> str | None:
        return feature_reason(self._cause)

    def status(self) -> dict:
        rows = tuple(self._rows.values())
        held = sum(row.state == "held" for row in rows)
        failing = sum(row.state != "held" and (row.health != "ok" or row.id in self._unsupported) for row in rows)
        return {"bots": len(rows), "people": len({row.owner_user_id for row in rows}), "polling": len(rows) - held - failing if self._cause is None else 0, "failing": failing, "held": held}

    async def add(self, link: BotLink, token: str) -> None:
        await self._launch(link, token)

    async def _retire(self, link_id: str, *, reason: str, close=True) -> TelegramBotChannel | None:
        previous = self._retire_by_link.get(link_id)
        if previous is not None:
            await asyncio.shield(previous)
        retiring = asyncio.create_task(self._retire_owned(link_id, reason=reason, close=close), name=f"telegram-retire-{link_id}")
        self._retirements.add(retiring)
        self._retire_by_link[link_id] = retiring
        retiring.add_done_callback(lambda task: self._retirement_done(link_id, task))
        return await asyncio.shield(retiring)

    def _retirement_done(self, link_id, task) -> None:
        self._retirements.discard(task)
        if self._retire_by_link.get(link_id) is task:
            self._retire_by_link.pop(link_id, None)
        if not task.cancelled() and (error := task.exception()) is not None:
            self._event("bot_retirement_failed", error=type(error).__name__, level=logging.WARNING)

    async def _retire_owned(self, link_id: str, *, reason: str, close=True) -> TelegramBotChannel | None:
        own = asyncio.current_task()
        starts = [task for task, identity in self._operations.items() if identity == link_id and task is not own]
        for task in starts:
            task.cancel()
        await asyncio.gather(*starts, return_exceptions=True)
        async with self._locks[link_id]:
            channel = self._channels.get(link_id)
            poll_task = self._runners.pop(link_id, None)
            self._polls.pop(link_id, None)
            restart = self._restarts.pop(link_id, None)
            if close and channel is not None:
                self._retiring_channels.add(channel)
                self._channels.pop(link_id, None)
        tasks = [task for task in (poll_task, restart) if task is not None and task is not own]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.sender is not None:
            await self.sender.retire_link(link_id)
        if close and channel is not None:
            began = self._time()
            await self._close_owned(channel)
            self._retiring_channels.discard(channel)
            self._event("bot_stop", link=channel.link, reason=reason, took_ms=round((self._time() - began) * 1000))
        return channel

    async def replace_token(self, link: BotLink, token: str, *, actor_id: str) -> BotLink:
        # The caller validates bot identity; this owned plaintext comparison is
        # the only basis for clearing a conflict pause on a changed credential.
        stored, revision = await self.links.token(link.id)
        if revision != link.token_revision:
            raise ValueError("token-changed")
        cipher = await asyncio.to_thread(token_cipher, self._environ)
        try:
            old = await asyncio.to_thread(unwrap_token, cipher, link.bot_id, stored)
        except Exception:
            old = None
        changed = old is not None and old.key != token
        action = "retry" if old is not None and not changed else "replace_token"
        encrypted = await asyncio.to_thread(wrap_token, cipher, link.bot_id, token)
        saved = await self.links.store_token(link.id, encrypted, actor_id=actor_id, action=action, expected_revision=revision, expected_encrypted_token=stored, token_changed=changed)
        await self._retire(link.id, reason="token-stored")
        self._rows[link.id] = saved
        if await self._available():
            await self._launch(saved, token)
        return await self.links.get(link.id)

    async def retry(self, link_id: str, *, take_over_webhook: bool = False) -> BotLink:
        link = await self.links.get(link_id)
        if link is None:
            raise ValueError("link-not-found")
        if not await self._available():
            return link
        if link.health == "stopped":
            link = await self.links.set_health(link.id, "stopped", holds=False, expected_token_revision=link.token_revision)
        channel = self.channel(link_id)
        if channel is not None and channel.transport_revision != link.token_revision:
            await self._retire(link_id, reason="token-stored")
            channel = None
        if channel is None:
            await self._launch(link)
            channel = self.channel(link_id)
        if channel is not None:
            if take_over_webhook:
                if not await self._wait_send_release() or self.channel(link_id) is not channel:
                    return await self.links.get(link_id)
                captured = await self.links.update_if_current(link_id, token_revision=channel.transport_revision)
                if captured is None or self.channel(link_id) is not channel or not self.send_allowed():
                    return await self.links.get(link_id)
                await channel.bot.delete_webhook()
                self._event("webhook", link=link, level=logging.WARNING, result="deleted", by="person")
            current_link = await self.links.update_if_current(link_id, token_revision=channel.transport_revision)
            if current_link is None or self.channel(link_id) is not channel:
                return await self.links.get(link_id)
            poll = self.poll_task(link_id)
            runner = self._runners.get(link_id)
            if poll is None or runner is None or runner.done():
                async with self._locks[link_id]:
                    if self.channel(link_id) is channel and not self._stopping:
                        channel.link = current_link
                        poll = self._make_poll(channel)
                        self._start_poll(channel, poll)
            if poll is not None:
                await poll.probe()
        return await self.links.get(link_id)

    async def resume_held(self, link_id: str) -> None:
        link = await self.links.get(link_id)
        if link is None or not await self._available():
            return
        if link.state == "held":
            link = await self.links.release_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
            if link is None:
                return
        await self._retire(link_id, reason="held")
        await self._launch(link, drop_pending=True)

    def _schedule_notice(self, channel, key, *, throttle=False, silent=False):
        task = asyncio.create_task(self._notice(channel, key, throttle=throttle, silent=silent), name=f"telegram-notice-{channel.link.id}")
        self._notices.add(task)
        task.add_done_callback(self._notices.discard)

    async def _notice(self, channel, key, *, throttle=False, silent=False) -> None:
        link = channel.link
        if self._cause is not None or self.channel(link.id) is not channel or link.chat_id is None:
            return
        try:
            if not await self._wait_send_release() or self.channel(link.id) is not channel:
                return
            async with self._locks[link.id]:
                if not self._send_ready or self.channel(link.id) is not channel:
                    return
                claimed = await await_drained(
                    self.links.mark_notice_if_current(
                        link.id,
                        key,
                        token_revision=channel.transport_revision,
                        binding_generation=link.binding_generation,
                        at=self._clock(),
                        minimum_interval_seconds=NOTICE_EVERY_HOURS * 3600 if throttle else 0,
                        only_if_unsent=key == "access.off",
                    )
                )
            if not claimed:
                self._event("notice", link=link, key=key, result="skipped")
                return
            preferences = await await_drained(self.settings.get(link.owner_user_id))
            quiet = await await_drained(asyncio.to_thread(silent_now, link, preferences, self.config, self._clock())) if preferences is not None else True
            context = SendContext(link_id=link.id, chat_id=link.chat_id, binding_generation=link.binding_generation, token_revision=channel.transport_revision, purpose="optional")
            render_key = key
            if key == "link.conflict_over":
                render_key += ".chat" if link.agent_kind == "lead_agent" else ".custom_agent"
            message = await asyncio.wait_for(channel.send_notice(render_key, context=context, silent=silent or quiet, product=product_name(self._app_config)), STOP_TIMEOUT_SECONDS)
            self._event("notice", link=link, key=key, result="sent" if message is not None else "failed")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._event("notice", link=link, key=key, result="failed", error=type(error).__name__, level=logging.WARNING)

    async def remove(self, link_id: str, *, actor_id: str, action="remove", last_text: str | None = "link.removed") -> None:
        link = await self.links.get(link_id)
        if link is None:
            await self._retire(link_id, reason="removed")
            self._rows.pop(link_id, None)
            return
        channel = await self._retire(link_id, reason="removed", close=False)
        try:
            if channel is not None and self._cause is None and self._send_ready and await self._available():
                if last_text is not None:
                    await self._notice(channel, last_text)
                try:
                    settings = await self.settings.get(link.owner_user_id)
                    text = render_plain("profile.removed", settings.locale if settings is not None else "en-US")
                    captured = await self.links.update_if_current(link_id, token_revision=channel.transport_revision)
                    if captured is not None and captured.binding_generation == link.binding_generation and self.channel(link_id) is channel and self.send_allowed():
                        await asyncio.wait_for(channel.bot.set_my_description(description=text), STOP_TIMEOUT_SECONDS)
                except Exception as error:
                    self._event("profile_removed_failed", link=link, error=type(error).__name__, level=logging.WARNING)
        finally:
            try:
                await self.links.remove(link_id, actor_id=actor_id, action=action)
                self._rows.pop(link_id, None)
                self.strangers.forget_bot(link.bot_id)
            finally:
                async with self._locks[link_id]:
                    if self._channels.get(link_id) is channel:
                        self._channels.pop(link_id, None)
                if channel is not None:
                    await self._close_owned(channel)

    async def reconcile(self) -> None:
        async with self._reconcile_lock:
            await self._reconcile_owned()

    @staticmethod
    def _sounding_link(notices) -> str | None:
        candidates = [channel.link for channel, _, blocked in notices if not blocked]
        if not candidates:
            return None

        def rank(link):
            used = link.last_update_at.timestamp() if link.last_update_at is not None else float("-inf")
            return (link.agent_kind != "lead_agent", -used, link.created_at.timestamp(), link.id)

        return min(candidates, key=rank).id

    async def _reconcile_owned(self) -> None:
        if self._stopping or self._cause is not None or not self._send_ready:
            return
        rows = await self.links.list_all()
        live = {row.id: row for row in rows}
        for identity in tuple(self._channels):
            if identity not in live:
                bot_id = self._channels[identity].link.bot_id
                await self._retire(identity, reason="removed")
                self._rows.pop(identity, None)
                self._unsupported.discard(identity)
                self.strangers.forget_bot(bot_id)
        # Failed targeted retirement keeps clients owned until a later drain
        # succeeds. Never close them while their delivery worker remains live.
        for channel in tuple(self._retiring_channels):
            await self.sender.retire_link(channel.link.id)
            await self._close_owned(channel)
            self._retiring_channels.discard(channel)
        for identity in tuple(self._rows):
            if identity not in live:
                self._rows.pop(identity, None)
                self._unsupported.discard(identity)
        transition_notices = []
        close_after_notice = []
        try:
            for link in rows:
                if self._stopping or self._cause is not None:
                    return
                self._observe(link, reason="invalid-token" if link.health == "token_rejected" else "recovered")
                channel = self.channel(link.id)
                if channel is not None and channel.transport_revision != link.token_revision:
                    await self._retire(link.id, reason="token-stored")
                    channel = None
                try:
                    target = await self.authority.target(link)
                except Exception:
                    continue
                if target == "unsupported":
                    self._unsupported.add(link.id)
                    if channel is not None:
                        await self._retire(link.id, reason="deployment")
                    continue
                self._unsupported.discard(link.id)
                if link.state == "held" and target != "held":
                    restored = await self.links.release_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
                    if restored is None:
                        continue
                    link = restored
                    self._observe(link, reason="recovered")
                    await self._retire(link.id, reason="held")
                    await self._launch(link, drop_pending=True)
                    channel = self.channel(link.id)
                    if channel is not None:
                        transition_notices.append((channel, "access.back", False))
                    continue
                if target == "held":
                    was_blocked = link.state == "blocked"
                    held = await self.links.hold_if_current(link.id, token_revision=link.token_revision, binding_generation=link.binding_generation)
                    if held is not None:
                        self._observe(held, reason="recovered")
                        if channel is not None:
                            channel.link = held
                            await self._retire(link.id, reason="held", close=False)
                            transition_notices.append((channel, "access.off", was_blocked))
                            close_after_notice.append(link.id)
                    elif channel is not None:
                        await self._retire(link.id, reason="held")
                    continue
                if target == "policy_off":
                    saved = await self.links.set_health(link.id, "policy_off", holds=True, expected_token_revision=link.token_revision)
                    if saved is not None:
                        self._observe(saved, reason="policy")
                        await self.links.end_pending(link.id, skip_reason="policy-off", consume_callbacks=False)
                    await self._retire(link.id, reason="policy-off")
                    continue
                if link.health == "policy_off":
                    saved = await self.links.set_health(link.id, "policy_off", holds=False, expected_token_revision=link.token_revision)
                    if saved is None:
                        continue
                    link = saved
                if target == "agent_unavailable" or link.health == "agent_unavailable":
                    saved = await self.links.set_health(link.id, "agent_unavailable", holds=target == "agent_unavailable", expected_token_revision=link.token_revision)
                    if saved is None:
                        continue
                    link = saved
                self._observe(link, reason="agent" if target == "agent_unavailable" else "recovered")
                if channel is None:
                    await self._launch(link)
                else:
                    channel.link = link
            owners = {channel.link.owner_user_id for channel, _, _ in transition_notices}
            for owner in owners:
                notices = [entry for entry in transition_notices if entry[0].link.owner_user_id == owner]
                sounding = self._sounding_link(notices)
                for channel, key, blocked in notices:
                    await self._notice(channel, key, silent=blocked or channel.link.id != sounding)
        finally:
            for identity in close_after_notice:
                await self._retire(identity, reason="held")

    async def _refresh_flags(self) -> None:
        for channel in tuple(self._channels.values()):
            if self._stopping or self._cause is not None:
                return
            if not channel.application.running:
                continue
            try:
                verdict = await channel.verify_token()
                if verdict.status == "valid" and self.channel(channel.link.id) is channel:
                    if await channel.record_poll_success():
                        self._rows[channel.link.id] = channel.link
            except (TelegramError, SQLAlchemyError):
                continue

    async def sweep_retention(self, now: datetime | None = None) -> dict[str, int]:
        # This slice has no callback/act/file consumers or full dependency
        # retention implementation. Delete only independent expired link events;
        # conservatively retain every inbox and delivery identity.
        counts = {"inbound": 0, "outbox": 0, "acts": 0, "events": 0}
        if self.session_factory is None or self._cause is not None:
            return counts
        began = self._time()
        async with self.session_factory() as session, session.begin():
            result = await session.execute(delete(TelegramBotLinkEventRow).where(TelegramBotLinkEventRow.created_at < (now or self._clock()) - timedelta(days=LINK_EVENT_RETENTION_DAYS)))
            counts["events"] = result.rowcount
        self._event("retention_sweep", **counts, took_ms=round((self._time() - began) * 1000))
        return counts

    async def begin_shutdown(self) -> None:
        self._stopping, self._ready = True, False
        if self.publication_authority is not None:
            self.publication_authority.close_creation()
        if self.publications is not None:
            self.publications.begin_shutdown()
        if self.thread_client is not None:
            self.thread_client.close_run_admission()
        if self.threads is not None:
            self.threads.begin_shutdown()
        self._health_barrier.set()
        self._send_ready = False
        self._send_event.set()
        own = asyncio.current_task()
        tasks = {*self._background, *self._runners.values(), *self._restarts.values(), *self._operations, *self._notices, *self._callbacks}
        retained_start = getattr(self.app.state, "telegram_bots_start", None)
        if retained_start is not None:
            tasks.add(retained_start)
        if self._start_task is not None:
            tasks.add(self._start_task)
        if self._copy_task is not None:
            tasks.add(self._copy_task)
        tasks.discard(own)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if self.sender is not None:
            await self.sender.stop()
        if self._retirements:
            await asyncio.gather(*tuple(self._retirements), return_exceptions=True)
        if self._callbacks:
            await asyncio.gather(*tuple(self._callbacks), return_exceptions=True)
        if self.threads is not None:
            await self.threads.stop()
        # Listener and clients deliberately survive channel-manager drain.

    def _unsubscribe(self) -> None:
        if self._subscribed:
            self.channel_service.bus.unsubscribe_outbound(self._listener)
            self._subscribed = False

    async def _finish_owned(self, task):
        """Drain an uncancellable worker/result through repeated caller cancels."""
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        return task.result()

    async def _close_owned(self, channel) -> None:
        closing = asyncio.create_task(channel.stop(), name="telegram-retired-client-cleanup")
        await self._finish_owned(closing)

    async def stop(self) -> None:
        if self._stopped:
            return
        if self._stop_task is None or self._stop_task.done() and not self._stopped:
            self._stop_task = asyncio.create_task(self._finish_stop(), name="telegram-supervisor-cleanup")
        # The Gateway's one outer timeout cancels this awaiter; resource cleanup
        # remains owned and loop-affine, and a later stop can await the same task.
        await asyncio.shield(self._stop_task)

    async def drain_after_stop_timeout(self) -> None:
        """Retire future latch attempts only after the Gateway's stop grace.

        An active SQL attempt and the same owned client cleanup still drain
        before the Gateway tears down persistence. A cancelled ordinary stop
        waiter does not invoke this retirement policy.
        """
        self._latch_retired.set()
        try:
            await self.stop()
        except asyncio.CancelledError:
            if self._stop_task is not None:
                await self._finish_owned(self._stop_task)
            raise

    async def _finish_stop(self) -> None:
        global _current
        began = self._time()
        try:
            await self.begin_shutdown()
            if self._latch_task is not None:
                await asyncio.shield(self._latch_task)
            if self._cause is None:
                await asyncio.gather(*(poll.confirm_offset() for poll in tuple(self._polls.values())), return_exceptions=True)
        finally:
            publication_error = None
            self._publication_reads_open = False
            if self.publication_authority is not None:
                self.publication_authority.close_reads()
            if self.publications is not None:
                try:
                    await self.publications.stop()
                except Exception as error:
                    publication_error = error
            thread_error = None
            if self.thread_client is not None and publication_error is None:
                try:
                    if self.threads is not None:
                        await self.threads.stop()
                    await self.thread_client.stop()
                except Exception as error:
                    thread_error = error
            if publication_error is None:
                self._unsubscribe()
            for task in tuple(self._notices):
                task.cancel()
            await asyncio.gather(*tuple(self._notices), return_exceptions=True)
            channels = tuple(set(self._channels.values()) | self._retiring_channels)
            results = await asyncio.gather(*(channel.stop() for channel in channels), return_exceptions=True)
            failed = []
            for channel, result in zip(channels, results, strict=True):
                if isinstance(result, BaseException):
                    self._retiring_channels.add(channel)
                    failed.append(channel)
                    self._event("bot_stop_failed", link=channel.link, error=type(result).__name__, level=logging.WARNING)
                else:
                    self._retiring_channels.discard(channel)
                    if self._channels.get(channel.link.id) is channel:
                        self._channels.pop(channel.link.id, None)
                    self._polls.pop(channel.link.id, None)
                    self._runners.pop(channel.link.id, None)
            self._stopped = not failed and thread_error is None and publication_error is None
            if self._stopped and _current is self:
                _current = None
            if failed:
                raise RuntimeError("telegram-client-cleanup-failed") from None
            if publication_error is not None:
                raise RuntimeError("telegram-publication-cleanup-failed") from None
            if thread_error is not None:
                raise RuntimeError("telegram-thread-client-cleanup-failed") from None
            self._event("supervisor_stop", links=len(channels), took_ms=round((self._time() - began) * 1000))
