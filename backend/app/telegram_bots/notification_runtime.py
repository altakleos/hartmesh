"""Compose notification owners around the supervisor's single durable sender."""

import asyncio
import logging
from datetime import timedelta

from sqlalchemy import select

from app.telegram_bots.links import BotLink
from app.telegram_bots.producer import ProductSourceRetired, _item
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


class NotificationRuntime:
    def __init__(self, host):
        self.host = host
        self._stopping = False
        self._operations = set()
        self._prepare_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._maintenance_lock = asyncio.Lock()
        self._input_after = {}
        self._maintenance_after = ""
        self._batch_after = ""
        self._pin_after = ""
        self._after = ""
        self.events = self.summaries = self.worker = self.observer = None
        self.chat = None
        self._capacity_after = None
        self._capacity_revision, self._capacity_seen = 0, -1
        self.installed = self.initialized = self.ingress_ready = False
        from app.telegram_bots.work_event_cursors import WorkEventCursors

        self.baselines = WorkEventCursors(host.links)

    def available(self):
        return not self._stopping and self.host.send_allowed() is True

    async def initialize(self, phase):
        """Establish the history boundary while startup still owns ingress."""

        async def perform():
            if self._stopping or not phase():
                return False
            self.install()
            if self.events is not None:
                await self.baselines.initialize()
            if self._stopping or not phase():
                return False
            self.initialized = True
            return True

        task = asyncio.create_task(perform())
        self._operations.add(task)
        try:
            return await await_drained(task)
        finally:
            self._operations.discard(task)

    def install(self):
        if self.installed:
            return
        from app.telegram_bots.message_settings_commands import MessageSettings
        from app.telegram_bots.rejected_notices import RejectedNotices
        from app.telegram_bots.settings_prompts import SettingsPrompts

        host = self.host
        host.message_settings = MessageSettings(host)
        host.settings_prompts = SettingsPrompts(host)
        host.rejected_notices = RejectedNotices(host, available=self.available)
        agents = getattr(host.app.state, "agent_instances", None)
        if agents is not None:
            self._install_work(agents)
        from app.telegram_bots.notification_retries import NotificationRetries

        self.host.notification_retries = NotificationRetries(self)
        self.installed = True

    def _install_work(self, agents):
        from app.telegram_bots.acts import ActsWorker
        from app.telegram_bots.digests import DigestReservations
        from app.telegram_bots.result_notice_delivery import ResultNoticePlans, ResultNoticeSources
        from app.telegram_bots.result_notice_sources import ResultNoticeReader
        from app.telegram_bots.summary_delivery import SummaryPlans, SummarySources
        from app.telegram_bots.summary_preferences import SummaryPreferences
        from app.telegram_bots.summary_scheduler import SummaryScheduler
        from app.telegram_bots.summary_sources import SummaryReader
        from app.telegram_bots.summary_views import SummaryViews, SummaryViewSources
        from app.telegram_bots.work_callbacks import WorkCallbacks
        from app.telegram_bots.work_digest_delivery import DigestPlans, DigestSources
        from app.telegram_bots.work_digest_items import DigestItems
        from app.telegram_bots.work_digest_members import DigestMembers
        from app.telegram_bots.work_digest_views import DigestViews, DigestViewSources
        from app.telegram_bots.work_event_delivery import WorkEventDelivery
        from app.telegram_bots.work_event_intents import WorkEventIntents
        from app.telegram_bots.work_event_reader import WorkEventReader
        from app.telegram_bots.work_news_delivery import WorkNewsPlans, WorkNewsSources
        from app.telegram_bots.work_news_sources import WorkNewsReader
        from app.telegram_bots.work_outcome_delivery import WorkOutcomePlans, WorkOutcomeSources
        from app.telegram_bots.work_outcome_recovery import WorkOutcomeRecovery
        from app.telegram_bots.work_question_edits import WorkQuestionEdits
        from app.telegram_bots.work_request_delivery import WorkRequestPlans
        from app.telegram_bots.work_request_source import WorkRequestSources
        from app.telegram_bots.work_requests import WorkRequestReader
        from app.telegram_bots.work_result_delivery import WorkReviewPlans
        from app.telegram_bots.work_result_source import WorkResultSources
        from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery
        from app.telegram_bots.work_routing import WorkRoutingReader
        from app.telegram_bots.work_routing_delivery import WorkRoutingPlans, WorkRoutingSources
        from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery
        from app.telegram_bots.work_status_delivery import WorkStatusPlans
        from app.telegram_bots.work_status_pins import WorkStatusPins
        from app.telegram_bots.work_status_updates import WorkStatusUpdates
        from app.telegram_bots.work_text_inputs import WorkTextInputs
        from deerflow.agent_instances.human_input import HumanInput
        from deerflow.agent_instances.work import AgentWork

        host = self.host
        work = AgentWork(agents)
        shared = dict(work=work, authority=host.authority, links=host.links)
        delivery = dict(outbox=host.outbox, callbacks=host.callbacks)
        self.requests = WorkRequestPlans(reader=WorkRequestReader(**shared), **delivery)
        self.reviews = WorkReviewPlans(reader=self.requests.reader, **delivery)
        self.routing = WorkRoutingPlans(reader=WorkRoutingReader(client=host.thread_client, **shared), **delivery)
        self.news = WorkNewsPlans(reader=WorkNewsReader(clock=host.outbox.clock, **shared), **delivery)
        self.results = ResultNoticePlans(reader=ResultNoticeReader(**shared), **delivery)
        self.status = WorkStatusPlans(reader=self.requests.reader, **delivery)
        self.digests = DigestPlans(members=DigestMembers(requests=self.requests.reader, news=self.news.reader, results=self.results.reader, routing=self.routing.reader), **delivery)
        self.reservations = DigestReservations(host.outbox, eligible=self.digest_eligible, available=self.available)
        self.summary_plans = SummaryPlans(reader=SummaryReader(**shared), available=self.available, **delivery)
        self.summaries = SummaryScheduler(self.summary_plans)
        host.acts.work, host.acts.authority, host.acts.links = work, host.authority, host.links
        self.outcomes = WorkOutcomePlans(self.requests, host.acts, product=host.product_name, routing=self.routing)
        host.work_callbacks = WorkCallbacks(host, self.outcomes)
        host.work_text_inputs = WorkTextInputs(host, host.work_callbacks)
        from app.telegram_bots.work_chat_runtime import WorkChatRuntime

        self.chat = host.work_chat = WorkChatRuntime(self, work)
        from app.telegram_bots.work_chat_observation import WorkChatObservation

        self.chat_observer = WorkChatObservation(self.chat)
        host.work_digest_items = DigestItems(digests=self.digests, requests=self.requests, reviews=self.reviews, routing=self.routing, news=self.news, results=self.results, summaries=self.summary_plans, views=self.chat.detail_plans)
        navigation = host.work_digest_items.navigation
        from app.telegram_bots.work_navigation_recovery import WorkNavigationRecovery, WorkReviewNavigationRecovery, WorkRoutingNavigationRecovery

        host.work_callbacks.recovery = WorkNavigationRecovery(self.requests, navigation=navigation)
        host.work_callbacks.result_recovery = WorkReviewNavigationRecovery(self.reviews, navigation=navigation)
        host.work_callbacks.routing_recovery = WorkRoutingNavigationRecovery(self.routing, navigation=navigation)
        host.work_request_sources = WorkRequestSources(reader=self.requests.reader, recovery=host.work_callbacks.recovery, navigation=navigation, **delivery)
        host.work_result_sources = WorkResultSources(reader=self.requests.reader, recovery=host.work_callbacks.result_recovery, navigation=navigation, **delivery)
        host.work_routing_sources = WorkRoutingSources(reader=self.routing.reader, recovery=host.work_callbacks.routing_recovery, navigation=navigation, **delivery)
        host.work_outcome_sources = WorkOutcomeSources(self.outcomes)
        host.work_news_sources = WorkNewsSources(self.news, navigation=navigation)
        host.result_notice_sources = ResultNoticeSources(self.results, navigation=navigation)
        host.work_status_sources = WorkNewsSources(self.status)
        host.work_status_update_sources = WorkStatusUpdates(self.status)
        host.work_status_pins = WorkStatusPins(host.work_status_update_sources)
        host.work_question_edit_sources = WorkQuestionEdits(self.requests)
        host.work_digest_sources = DigestSources(self.digests)
        host.work_digest_views = DigestViews(self.digests)
        host.work_digest_view_sources = DigestViewSources(host.work_digest_views)
        host.summary_sources = SummarySources(self.summary_plans)
        host.summary_views = SummaryViews(self.summary_plans)
        host.summary_view_sources = SummaryViewSources(host.summary_views)
        host.summary_preferences = SummaryPreferences(self.summary_plans, host)
        self.worker = ActsWorker(host.acts, available=self.available)
        self.observer = WorkOutcomeRecovery(self.outcomes)
        intents = WorkEventIntents(links=host.links, human=HumanInput(work))
        self.events = WorkEventReader(links=host.links, project=intents.project)
        self.materializers = {}
        for owner in (WorkEventDelivery(self.requests), WorkReviewEventDelivery(self.reviews), WorkRoutingEventDelivery(self.routing)):
            self.materializers.update({key: owner for key in owner.keys})

    @staticmethod
    def digest_eligible(row):
        families = {"work_request_basis", "work_news_basis", "result_notice_basis"}.intersection(row.payload)
        return len(families) == 1 and all(getattr(row, name) is None for name in ("claim_owner", "claim_id", "claimed_at"))

    async def prepare_deliveries(self):
        if not self.available() or self.events is None:
            return 0
        task = asyncio.create_task(self._prepare_deliveries())
        self._operations.add(task)
        try:
            return await await_drained(task)
        finally:
            self._operations.discard(task)

    async def _prepare_deliveries(self):
        from app.telegram_bots.work_news import NEWS_KEYS
        from app.telegram_bots.work_question_updates import UPDATE_KEYS
        from app.telegram_bots.work_status_text import STATUS_KEYS

        async with self._prepare_lock:
            if not self.available():
                return 0
            async with self.host.outbox.sessions() as reader:
                rows = (
                    await reader.execute(
                        select(TelegramOutboxRow, TelegramBotLinkRow)
                        .join(TelegramBotLinkRow, TelegramBotLinkRow.id == TelegramOutboxRow.link_id)
                        .where(
                            TelegramOutboxRow.id > self._after,
                            TelegramOutboxRow.state == "pending",
                            TelegramOutboxRow.claim_owner.is_(None),
                            TelegramOutboxRow.claim_id.is_(None),
                            TelegramOutboxRow.claimed_at.is_(None),
                            TelegramBotLinkRow.agent_kind == "instance",
                            TelegramBotLinkRow.state == "connected",
                            TelegramBotLinkRow.health == "ok",
                        )
                        .order_by(TelegramOutboxRow.id)
                        .limit(self.host.config.producer.batch)
                    )
                ).all()
            if not rows:
                self._after = ""
                return 0
            count = 0
            for row, binding in rows:
                if not self.available():
                    break
                self._after = row.id
                if row.parts:
                    continue
                item, link = _item(row), BotLink.from_row(binding)
                key = item.payload.get("work_event_intent", {}).get("key")
                owner = self.digests if item.class_ == "digest" else self.materializers.get(key)
                try:
                    chat_owner = self.chat.materializer(item) if self.chat is not None else None
                except ProductSourceRetired:
                    continue
                if chat_owner is not None:
                    owner = chat_owner
                if key in STATUS_KEYS:
                    owner = self.host.work_status_update_sources if item.dedupe_key.startswith("edit:") else self.status
                elif key in UPDATE_KEYS and item.dedupe_key.startswith("edit:"):
                    owner = self.host.work_question_edit_sources
                elif key in {"result.done", "result.waiting_review"}:
                    owner = self.results
                elif key in NEWS_KEYS:
                    owner = self.news
                if owner is None:
                    continue
                try:
                    if chat_owner is not None:
                        channel = self.host.channel(link.id)
                        if channel is None:
                            continue
                        guard = _RuntimeGuard(self, self.host.commands._guard(channel, link))
                        count += bool(await owner.materialize(link, item.id, guard=guard))
                    else:
                        count += bool(await owner.materialize(link, item.id))
                except ProductSourceRetired:
                    from app.telegram_bots.notification_disposition import retire_obsolete_question

                    if not await retire_obsolete_question(self.host.outbox, link, item, available=self.available):
                        logger.info("Telegram notification source held: delivery=%s", item.id)
                except Exception:
                    logger.warning("Telegram notification preparation deferred: delivery=%s", item.id)
            if len(rows) < self.host.config.producer.batch:
                self._after = ""
            return count

    async def before_send(self):
        """Freeze current event sources and reserve batches before sender selection."""
        if not self.available() or self.events is None:
            return 0
        task = asyncio.create_task(self._before_send())
        self._operations.add(task)
        try:
            return await await_drained(task)
        finally:
            self._operations.discard(task)

    async def _before_send(self):
        async with self._send_lock:
            if not self.available():
                return 0
            await self.host.notification_retries.prepare_notices()
            completed = await self.prepare_deliveries()
            if self.available():
                completed += await self._prepare_pins()
            if not self.available():
                return completed
            async with self.host.links.session_factory() as reader:
                rows = list(
                    await reader.scalars(
                        select(TelegramBotLinkRow)
                        .where(
                            TelegramBotLinkRow.id > self._batch_after,
                            TelegramBotLinkRow.agent_kind == "instance",
                            TelegramBotLinkRow.state == "connected",
                            TelegramBotLinkRow.health == "ok",
                            select(TelegramOutboxRow.id)
                            .where(
                                TelegramOutboxRow.link_id == TelegramBotLinkRow.id,
                                TelegramOutboxRow.state == "pending",
                                TelegramOutboxRow.not_before <= self.host.outbox.clock(),
                            )
                            .exists(),
                        )
                        .order_by(TelegramBotLinkRow.id)
                        .limit(self.host.config.producer.batch)
                    )
                )
            for row in rows:
                if not self.available():
                    break
                self._batch_after = row.id
                link = BotLink.from_row(row)
                for category in ("essential", "discretionary"):
                    if not self.available():
                        break
                    try:
                        identity = await self.reservations.reserve(link, category=category)
                        if identity is not None and self.available():
                            try:
                                completed += bool(await self.digests.materialize(link, identity))
                            except ProductSourceRetired:
                                # A member changed before batch publication. Free each
                                # original delivery for its own current source check.
                                await self.reservations.abandon(link, identity, reason="source-gone")
                    except Exception:
                        logger.warning("Telegram notification batch deferred: link=%s", link.id)
            if len(rows) < self.host.config.producer.batch:
                self._batch_after = ""
            return completed

    async def _prepare_pins(self):
        if not self.available():
            return 0
        async with self.host.outbox.sessions() as reader:
            rows = (
                await reader.execute(
                    select(TelegramOutboxRow.id, TelegramBotLinkRow)
                    .join(TelegramBotLinkRow, TelegramBotLinkRow.id == TelegramOutboxRow.link_id)
                    .where(
                        TelegramOutboxRow.id > self._pin_after,
                        TelegramOutboxRow.dedupe_key.like("progress:%"),
                        TelegramOutboxRow.class_ == "progress",
                        TelegramOutboxRow.state == "sent",
                        TelegramBotLinkRow.agent_kind == "instance",
                        TelegramBotLinkRow.state == "connected",
                        TelegramBotLinkRow.health == "ok",
                    )
                    .order_by(TelegramOutboxRow.id)
                    .limit(self.host.config.producer.batch)
                )
            ).all()
        count = 0
        for identity, row in rows:
            if not self.available():
                break
            self._pin_after = identity
            try:
                count += bool(await self.host.work_status_pins.plan(BotLink.from_row(row), identity))
            except ProductSourceRetired:
                logger.info("Telegram status pin source held: delivery=%s", identity)
            except Exception:
                logger.warning("Telegram status pin preparation deferred: delivery=%s", identity)
        if len(rows) < self.host.config.producer.batch:
            self._pin_after = ""
        return count

    async def tick_acts(self):
        if self.available() and self.worker is not None:
            await self.worker.tick()

    async def tick_events(self):
        # Event metadata still advances under ordinary send refusals, as before.
        if self._stopping or self.events is None:
            return 0
        task = asyncio.create_task(self.events.tick())
        self._operations.add(task)
        try:
            completed = await await_drained(task)
            if completed:
                self._capacity_revision += 1
            return completed
        finally:
            self._operations.discard(task)

    async def tick_maintenance(self):
        if not self.available() or not self.installed:
            return
        task = asyncio.create_task(self._tick_maintenance())
        self._operations.add(task)
        try:
            await await_drained(task)
        finally:
            self._operations.discard(task)

    async def _tick_maintenance(self):
        async with self._maintenance_lock:
            if not self.available():
                return
            async with self.host.links.session_factory() as reader:
                rows = list(
                    await reader.scalars(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id > self._maintenance_after, TelegramBotLinkRow.state == "connected").order_by(TelegramBotLinkRow.id).limit(self.host.config.producer.batch))
                )
            for row in rows:
                if not self.available():
                    break
                self._maintenance_after = row.id
                link = BotLink.from_row(row)
                try:
                    if link.health == "token_rejected":
                        await self.host.rejected_notices.plan(link)
                    elif link.health == "ok" and link.agent_kind == "instance" and self.observer is not None:
                        channel = self.host.channel(link.id)
                        if channel is not None:
                            guard = _RuntimeGuard(self, self.host.commands._guard(channel, link))
                            await self.observer.tick(link, guard=guard)
                            await self.chat_observer.tick(link, guard=guard)
                            deadline = asyncio.get_running_loop().time() + 30
                            await self.recover_inputs(lambda: asyncio.get_running_loop().time() < deadline, link_id=link.id)
                except Exception:
                    logger.warning("Telegram notification recovery deferred: link=%s", link.id)
            if len(rows) < self.host.config.producer.batch:
                self._maintenance_after = ""
            now = self.host.outbox.clock()
            if self.available() and self.chat is not None and (self._capacity_revision != self._capacity_seen or self._capacity_after is None or now >= self._capacity_after):
                revision = self._capacity_revision
                await self.chat.offers.poll()
                self._capacity_seen = revision
                delay = min(60, self.host.config.outbox.place_free_hold_seconds)
                self._capacity_after = now + timedelta(seconds=delay)

    async def recover_inputs(self, phase, *, link_id=None):
        from app.telegram_bots.work_input_recovery import recover_inputs

        if not self.available() or not phase():
            return
        task = asyncio.create_task(recover_inputs(self, phase, link_id=link_id, guard_factory=_RuntimeGuard))
        self._operations.add(task)
        try:
            await await_drained(task)
        finally:
            self._operations.discard(task)

    async def stop(self):
        self._stopping = True

        async def drain():
            results = await asyncio.gather(*(owner.stop() for owner in (self.worker, self.observer, self.chat) if owner is not None), return_exceptions=True)
            operations = tuple(self._operations)
            results.extend(await asyncio.gather(*operations, return_exceptions=True))
            self._operations.difference_update(operations)
            # A failed metadata task must not skip the remaining runtime drain
            # or prevent the supervisor from stopping its sender and clients.
            for result in results:
                if isinstance(result, BaseException):
                    logger.warning("Telegram notification shutdown task failed (%s)", type(result).__name__)

        await await_drained(drain())


class _RuntimeGuard:
    """A retained recovery cannot outlive the supervisor's admission window."""

    def __init__(self, runtime, base, *, phase=lambda: True):
        self.runtime, self.base, self.phase = runtime, base, phase

    @property
    def captured_link(self):
        return self.base.captured_link

    def current(self):
        if not self.runtime.available() or not self.phase():
            raise ProductSourceRetired()
        self.base.current()

    async def prepare(self):
        self.current()
        await self.base.prepare()
        self.current()

    async def check(self, writer):
        self.current()
        result = await self.base.check(writer)
        self.current()
        return result

    async def check_projection(self, writer, projection):
        self.current()
        result = await self.base.check_projection(writer, projection)
        self.current()
        return result
