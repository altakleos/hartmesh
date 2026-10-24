"""One explicitly started SDK Application per managed bot, on the owning loop.

Product plans and worker receipts belong to the outbox. This adapter only sends
the exact claimed part, and never inherits the deployment-wide bot lifecycle.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Protocol

from telegram import LinkPreviewOptions, Update, User
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, InvalidToken, NetworkError, RetryAfter, TelegramError
from telegram.ext import AIORateLimiter, Application, Defaults
from telegram.request import HTTPXRequest

from app.channels.telegram import TelegramChannel
from app.telegram_bots.render import render, render_plain
from app.telegram_bots.settings import silent_now
from app.telegram_bots.token_pattern import TOKEN_BODY as TOKEN_BODY

if TYPE_CHECKING:
    from app.channels.message_bus import OutboundMessage
    from app.telegram_bots.config import TelegramBotsConfig
    from app.telegram_bots.inbox import InboxRow
    from app.telegram_bots.links import BotLink, BotLinkRepository
    from app.telegram_bots.producer import DeliveryEligibility, Outbox, OutboxItem, SendingPart
    from app.telegram_bots.settings import SettingsRepository

LIMITER_HOLD_MAX_SECONDS = 5
SEND_RETRY_AFTER_MAX_SECONDS = 60


class LongRetryAfter(TelegramError):
    def __init__(self, seconds: float) -> None:
        super().__init__("Server retry deferred")
        self.seconds = seconds


class BoundedRateLimiter(AIORateLimiter):
    """Retain the pinned parent's math/retries with an independent short hold."""

    def __init__(self, *args, monotonic: Callable[[], float] | None = None, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep, **kwargs) -> None:
        kwargs.setdefault("max_retries", 2)
        super().__init__(*args, **kwargs)
        self._short_hold_until = 0.0
        self._monotonic, self._sleep = monotonic, sleep

    def _time(self) -> float:
        return self._monotonic() if self._monotonic else asyncio.get_running_loop().time()

    async def process_request(self, callback, args, kwargs, endpoint, data, rate_limit_args):
        async def guarded(*callback_args, **callback_kwargs):
            while (remaining := self._short_hold_until - self._time()) > 0:
                await self._sleep(remaining)
            try:
                return await callback(*callback_args, **callback_kwargs)
            except RetryAfter as error:
                seconds = error._retry_after.total_seconds()
                if seconds > LIMITER_HOLD_MAX_SECONDS:
                    raise LongRetryAfter(seconds) from None
                self._short_hold_until = max(self._short_hold_until, self._time() + seconds + 0.1)
                raise

        return await super().process_request(guarded, args, kwargs, endpoint, data, rate_limit_args)


@dataclass(frozen=True)
class SendContext:
    link_id: str
    chat_id: int | None
    binding_generation: int
    token_revision: int
    purpose: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.link_id, str)
            or not self.link_id
            or len(self.link_id) > 32
            or type(self.binding_generation) is not int
            or self.binding_generation < 0
            or type(self.token_revision) is not int
            or self.token_revision < 0
            or self.chat_id is not None
            and (type(self.chat_id) is not int or self.chat_id == 0)
            or self.purpose not in {"bound_message", "stranger", "profile", "group", "callback", "optional"}
            or self.purpose == "bound_message"
            and (self.chat_id is None or self.chat_id <= 0)
        ):
            raise ValueError("invalid-send-context")


@dataclass(frozen=True)
class TokenVerification:
    status: str  # valid, rejected, unverified, retired
    user: User | None = None
    retry_after_seconds: float | None = None


class TransportServices(Protocol):
    config: TelegramBotsConfig
    links: BotLinkRepository
    outbox: Outbox
    settings: SettingsRepository
    tls_context: Any

    async def protect_notice(self, context: SendContext) -> bool: ...

    async def eligibility(self, link: BotLink, item: OutboxItem, part: dict | None) -> DeliveryEligibility: ...

    def channel(self, link_id: str) -> TelegramBotChannel | None: ...

    def send_allowed(self) -> bool: ...


def _now() -> datetime:
    return datetime.now(UTC)


def _invalid_status(error: InvalidToken) -> int | None:
    """Initialization wraps the description in a token-bearing exception."""
    seen = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, InvalidToken):
            description = str(current).strip().casefold()
            if description == "unauthorized":
                return 401
            if description == "not found":
                return 404
        current = current.__cause__
    return None


class TelegramBotChannel(TelegramChannel):
    def __init__(
        self,
        *,
        link: BotLink,
        token: str,
        supervisor: TransportServices,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        now: Callable[[], datetime] = _now,
    ) -> None:
        # Only the parent's stateless parsing helpers are inherited. Its
        # lifecycle, bus subscription and thread/loop ownership never run.
        self.name, self.link, self._running = "telegram", link, False
        self._transport_revision = link.token_revision
        self._services = supervisor
        self._monotonic, self._sleep, self._now = monotonic, sleep, now
        self._verify_lock, self._lifecycle_lock, self._success_lock = asyncio.Lock(), asyncio.Lock(), asyncio.Lock()
        self._verify_task: asyncio.Task[TokenVerification] | None = None
        self._verify_not_before = 0.0
        self._first_401: float | None = None
        self._close_task: asyncio.Task | None = None
        self._deferred_close: asyncio.Task | None = None
        self._stopping = False
        self._needs_success = True
        self._failure_serial = 0
        factory = getattr(supervisor, "request_factory", HTTPXRequest)
        self._requests = (
            factory(connection_pool_size=8, httpx_kwargs={"verify": supervisor.tls_context}),
            factory(connection_pool_size=1, httpx_kwargs={"verify": supervisor.tls_context}),
        )
        self._application = (
            Application.builder()
            .token(token)
            .request(self._requests[0])
            .get_updates_request(self._requests[1])
            .defaults(Defaults(link_preview_options=LinkPreviewOptions(is_disabled=True), allow_sending_without_reply=True))
            .rate_limiter(BoundedRateLimiter(overall_max_rate=30, overall_time_period=1, max_retries=2, monotonic=monotonic, sleep=sleep))
            .updater(None)
            .job_queue(None)
            .build()
        )

    @property
    def transport_revision(self) -> int:
        return self._transport_revision

    @property
    def services(self) -> TransportServices:
        return self._services

    @property
    def application(self) -> Application:
        return self._application

    @property
    def bot(self):
        return self._application.bot

    @property
    def supports_streaming(self) -> bool:
        return False  # advisory/model output belongs to PLAN 1.14

    def _time(self) -> float:
        return self._monotonic() if self._monotonic else asyncio.get_running_loop().time()

    async def refresh_link(self) -> BotLink | None:
        current = await self._services.links.update_if_current(self.link.id, token_revision=self.transport_revision)
        if current is not None:
            self.link = current
        return current

    def _failure(self) -> None:
        self._needs_success = True
        self._failure_serial += 1

    async def record_poll_success(self, *, recovered: bool = False) -> bool:
        if type(recovered) is not bool:
            raise ValueError("invalid-recovery-fact")
        return await self._success(recovered=recovered)

    async def _success(self, user: User | None = None, *, recovered: bool = False) -> bool:
        async with self._success_lock:
            if recovered:
                self._failure()
            serial = self._failure_serial
            current = await self.refresh_link()  # current-token guard even when throttled
            if current is None:
                return False
            if user is not None and user.id != current.bot_id:
                raise NetworkError("Bot identity mismatch")
            facts = {}
            if user is not None:
                facts.update(can_manage_bots=bool(user.can_manage_bots), topics_enabled=bool(user.has_topics_enabled and not user.allows_users_to_create_topics))
            now = self._now()
            last_ok = current.last_ok_at
            record_success = self._needs_success or last_ok is None or (now - last_ok).total_seconds() >= 60
            if record_success:
                facts.update(last_ok_at=now, last_error=None, last_error_at=None)
            if facts:
                current = await self._services.links.update_if_current(current.id, token_revision=self.transport_revision, **facts)
                if current is None:
                    return False
                self.link = current
            if record_success:
                self._needs_success = self._failure_serial != serial
            if user is not None:
                self._first_401 = None
            return True

    async def _after_send_success(self) -> None:
        from sqlalchemy.exc import SQLAlchemyError

        try:
            await self._success()
        except SQLAlchemyError:
            # The SDK has already returned its acknowledgement. Auxiliary
            # connectivity bookkeeping cannot turn that known send into retry.
            self._failure()

    async def _shutdown_resources(self) -> None:
        try:
            if self.application.running:
                await self.application.stop()
            await self.application.shutdown()
        finally:
            # Application.shutdown is a no-op after failed initialize, although
            # Bot.initialize may already have initialized both request clients.
            try:
                await self.bot.shutdown()
            finally:
                # SDK initialization can fail before its requests-initialized
                # flag is set. Close both exact clients, not only the Bot flag.
                results = await asyncio.gather(*(request.shutdown() for request in self._requests), return_exceptions=True)
                self._running = False
                for result in results:
                    if isinstance(result, BaseException):
                        raise result

    async def _close(self) -> None:
        if self._close_task is None or self._close_task.done():
            self._close_task = asyncio.create_task(self._shutdown_resources())
        await asyncio.shield(self._close_task)

    async def _close_after_verification(self) -> None:
        if self._verify_task is not None:
            await asyncio.gather(self._verify_task, return_exceptions=True)
        async with self._lifecycle_lock:
            if not self.application.running:
                await self._close()

    async def start(self) -> None:
        if self._deferred_close is not None:
            await asyncio.shield(self._deferred_close)
        async with self._lifecycle_lock:
            if self.application.running:
                return
            self._stopping = False
            try:
                verdict = await self.wait_token_verification()
                if verdict is not None and verdict.status in {"retired", "rejected"}:
                    raise NetworkError("Transport unavailable")
                if await self.refresh_link() is None:
                    raise NetworkError("Transport retired")
                await self.application.initialize()
                if not await self._success(self.bot.bot):
                    raise NetworkError("Transport retired")
                await self.application.start()
                self._running = True
            except InvalidToken as error:
                self._failure()
                try:
                    await self.verify_token(error, from_get_me=True)
                except asyncio.CancelledError:
                    self._deferred_close = asyncio.create_task(self._close_after_verification())
                    raise
                finally:
                    if self._verify_task is None or self._verify_task.done():
                        await self._close()
                raise NetworkError("Telegram initialization incomplete") from None
            except BaseException:
                self._failure()
                await self._close()
                raise

    async def stop(self) -> None:
        self._stopping = True
        if self._verify_task is not None:
            if not self._verify_task.done():
                self._verify_task.cancel()
            await asyncio.gather(self._verify_task, return_exceptions=True)
        if self._deferred_close is not None:
            await asyncio.shield(self._deferred_close)
        async with self._lifecycle_lock:
            await self._close()

    async def verify_token(self, error: InvalidToken | None = None, *, from_get_me: bool = False) -> TokenVerification:
        async with self._verify_lock:
            if self._stopping:
                return TokenVerification("retired")
            if error is not None:
                self._failure()
            remaining = self._verify_not_before - self._time()
            if remaining > 0:
                return TokenVerification("unverified", retry_after_seconds=remaining)
            if self._verify_task is None or self._verify_task.done():
                if self._verify_task is not None and not self._verify_task.cancelled():
                    self._verify_task.exception()
                self._verify_task = asyncio.create_task(self._verify_token(error, from_get_me=from_get_me))
            check = self._verify_task
        return await asyncio.shield(check)

    async def wait_token_verification(self) -> TokenVerification | None:
        latest = None
        while True:
            async with self._verify_lock:
                if self._stopping:
                    return TokenVerification("retired")
                check = self._verify_task
                if check is None or check.done():
                    if check is not None and not check.cancelled():
                        check.exception()
                    if self._verify_not_before <= self._time():
                        return latest
                    check = self._verify_task = asyncio.create_task(self._wait_verification_retry())
            latest = await asyncio.shield(check)
            if latest.retry_after_seconds is None or self._verify_not_before <= self._time():
                return latest

    async def _wait_verification_retry(self) -> TokenVerification:
        while (remaining := self._verify_not_before - self._time()) > 0:
            await self._sleep(remaining)
        if self._stopping or await self.refresh_link() is None:
            return TokenVerification("retired")
        # A failed start closed its clients. Full Application.initialize, not a
        # bare getMe on those closed clients, owns the next startup attempt.
        if not self.application.running:
            return TokenVerification("unverified")
        return await self._verify_token(None, from_get_me=False)

    def _verification_retry(self, error: LongRetryAfter | RetryAfter) -> TokenVerification:
        seconds = error.seconds if isinstance(error, LongRetryAfter) else error._retry_after.total_seconds()
        self._failure()
        self._verify_not_before = max(self._verify_not_before, self._time() + seconds)
        self._log_failure("getMe", error)
        return TokenVerification("unverified", retry_after_seconds=max(0, self._verify_not_before - self._time()))

    async def _verify_token(self, error: InvalidToken | None, *, from_get_me: bool) -> TokenVerification:
        from app.telegram_bots.polling import TOKEN_RECHECK_SECONDS

        if await self.refresh_link() is None:
            return TokenVerification("retired")
        if self.link.health == "token_rejected":
            return TokenVerification("rejected")
        status = _invalid_status(error) if from_get_me and error is not None else None
        if not from_get_me:
            try:
                user = await self.bot.get_me()
            except (LongRetryAfter, RetryAfter) as failure:
                verdict = self._verification_retry(failure)  # retain before SQL can fail
                if await self.refresh_link() is None:
                    return TokenVerification("retired")
                return verdict
            except InvalidToken as failure:
                self._failure()
                status = _invalid_status(failure)
            except TelegramError as failure:
                self._failure()
                self._log_failure("getMe", failure)
                return TokenVerification("unverified" if await self.refresh_link() is not None else "retired")
            else:
                return TokenVerification("valid", user) if await self._success(user) else TokenVerification("retired")
        if await self.refresh_link() is None:
            return TokenVerification("retired")
        if status == 404:
            self._first_401 = None
            return TokenVerification("valid")
        if status != 401:
            return TokenVerification("unverified")
        if self._first_401 is None:
            self._first_401 = self._time()
        elif self._time() - self._first_401 >= TOKEN_RECHECK_SECONDS:
            current = await self._services.links.set_health(self.link.id, "token_rejected", holds=True, expected_token_revision=self.transport_revision)
            if current is None:
                return TokenVerification("retired")
            self.link = current
            return TokenVerification("rejected")
        deadline = self._first_401 + TOKEN_RECHECK_SECONDS
        while (remaining := deadline - self._time()) > 0:
            await self._sleep(remaining)
        if self._stopping or await self.refresh_link() is None:
            return TokenVerification("retired")
        return await self._verify_token(None, from_get_me=False)

    async def handle_update(self, update: Update) -> bool:
        # Handler registration and durable gate classification arrive in 1.12.
        # process_update alone can swallow exceptions and cannot acknowledge.
        await self.application.process_update(update)
        return False

    async def _run_on_telegram_loop(self, coroutine):
        return await coroutine

    async def _get_download_bot(self):
        return self.bot

    async def send(self, msg: OutboundMessage, _max_retries: int = 3) -> int | None:
        from app.telegram_bots.producer import PermanentDeliveryError

        # The bus listener/model final planner is not active until 1.14.
        raise PermanentDeliveryError("unsupported-source")

    async def send_text(self, key: str, *, source_id: str, reply_to: int | None = None, buttons=None, silent: bool = False, thread: int | None = None, inbox_rows: Sequence[InboxRow] = (), **values) -> str:
        if buttons is not None:
            raise ValueError("unimplemented-callback-authority")
        part = {"kind": "text", "source_ref": {"key": key, "values": values}, "silent": silent}
        if reply_to is not None:
            part["reply_to_message_id"] = reply_to
        return await self._services.outbox.plan_product(self.link, source_id=source_id, parts=[part], inbox_rows=inbox_rows, message_thread_id=thread)

    async def edit_text(self, message_id: int, key: str, *, source_id: str, buttons=None, inbox_rows: Sequence[InboxRow] = (), **values) -> str:
        if buttons is not None:
            raise ValueError("unimplemented-callback-authority")
        return await self._services.outbox.plan_product(self.link, source_id=source_id, parts=[{"kind": "edit", "source_ref": {"key": key, "values": values}, "target_message_id": message_id}], inbox_rows=inbox_rows)

    async def _render_source(self, link: BotLink, source: dict, *, plain: bool = False) -> str:
        from app.telegram_bots.producer import PermanentDeliveryError

        if set(source) != {"key", "values"} or not isinstance(source["key"], str) or not isinstance(source["values"], dict):
            raise PermanentDeliveryError("unsupported-source")
        settings = await self._services.settings.get(link.owner_user_id)
        if settings is None:
            raise PermanentDeliveryError("missing-settings")
        renderer = render_plain if plain else render
        try:
            return renderer(source["key"], settings.locale, **source["values"])
        except (KeyError, TypeError, ValueError):
            raise PermanentDeliveryError("invalid-product-source") from None

    async def _forbidden(self, context: SendContext) -> None:
        if context.purpose == "bound_message" and context.chat_id is not None:
            await self._services.links.mark_blocked_if_current(link_id=context.link_id, chat_id=context.chat_id, binding_generation=context.binding_generation, token_revision=context.token_revision)

    def _log_failure(self, method: str, error: BaseException) -> None:
        from app.telegram_bots.api_logging import log_call_failure

        log_call_failure(link_id=self.link.id, bot_id=self.link.bot_id, method=method, error=error, retry_after_seconds=error.seconds if isinstance(error, LongRetryAfter) else None)

    @staticmethod
    def _log_part_failure(method: str, error: BaseException, captured: SendingPart, *, failed_reason: str | None = None) -> None:
        from app.telegram_bots.api_logging import log_call_failure

        log_call_failure(
            link_id=captured.link.id,
            bot_id=captured.link.bot_id,
            method=method,
            error=error,
            attempts=captured.item.attempts + 1,
            outbox_id=captured.item.id,
            part_id=captured.part["part_id"],
            class_=captured.item.class_,
            claim_owner=captured.claim_owner,
            claim_id=captured.claim_id,
            failed_reason=failed_reason,
            retry_after_seconds=error.seconds if isinstance(error, LongRetryAfter) else None,
        )

    async def send_notice(self, key: str, *, context: SendContext, reply_to: int | None = None, silent: bool = False, thread: int | None = None, **values) -> int | None:
        if context.chat_id is None:
            raise ValueError("notice-destination-required")
        if not self.application.running:
            return None
        if not await self._notice_context_current(context):
            return None
        text = await self._render_source(self.link, {"key": key, "values": values})
        protect = await self._services.protect_notice(context)
        if type(protect) is not bool:
            raise ValueError("invalid-notice-policy")
        if not await self._notice_context_current(context):
            return None
        try:
            message = await self.bot.send_message(chat_id=context.chat_id, text=text, parse_mode=ParseMode.HTML, reply_to_message_id=reply_to, disable_notification=silent, message_thread_id=thread, protect_content=protect)
        except Forbidden as error:
            self._failure()
            self._log_failure("sendMessage", error)
            await self._forbidden(context)
            return None
        except InvalidToken as error:
            self._failure()
            self._log_failure("sendMessage", error)
            await self.verify_token(error)
            return None
        except TelegramError as error:
            self._failure()
            self._log_failure("sendMessage", error)
            return None
        await self._after_send_success()
        return message.message_id

    def _send_allowed(self) -> bool:
        return self._services.send_allowed() is True

    async def _notice_context_current(self, context: SendContext) -> bool:
        # A disposable transition has separate authorization from ordinary
        # queued delivery (access.off may follow disablement), but never gains
        # the destination or ownership of a later binding/adapter.
        if context.link_id != self.link.id or context.token_revision != self.transport_revision:
            return False
        current = await self.refresh_link()
        return (
            current is not None
            and current.binding_generation == context.binding_generation
            and (context.purpose not in {"bound_message", "optional"} or current.chat_id == context.chat_id or context.purpose == "optional" and current.chat_id is None)
            and self.application.running
            and not self._stopping
            and self._services.channel(context.link_id) is self
            and self._send_allowed()
        )

    async def answer_press(self, query_id: str, key: str | None = None, **values) -> None:
        if not self.application.running or not self._send_allowed():
            return
        logged = False
        try:
            async with asyncio.timeout(2):
                if await self.refresh_link() is None:
                    return
                text = await self._render_source(self.link, {"key": key, "values": values}, plain=True) if key else None
                if not self._send_allowed() or self._services.channel(self.link.id) is not self:
                    return
                try:
                    await self.bot.answer_callback_query(query_id, text=text, show_alert=True if key else None)
                except InvalidToken as error:
                    self._failure()
                    self._log_failure("answerCallbackQuery", error)
                    logged = True
                    # The acknowledgement's own deadline cancels this waiter;
                    # the channel-owned shared checker remains shielded.
                    await self.verify_token(error)
                except BadRequest as error:
                    if "query is too old" in str(error).lower():
                        return
                    raise
        except (TimeoutError, TelegramError) as error:
            if not logged:
                self._log_failure("answerCallbackQuery", error)

    async def _defer_invalid_token(self, captured: SendingPart, verdict: TokenVerification, *, http_status: int | None) -> None:
        from app.telegram_bots.producer import DeliveryClaimRetired, TransientDeliveryError

        if not await self._services.outbox.defer_token_verification(captured, retry_after_seconds=verdict.retry_after_seconds, http_status=http_status):
            raise DeliveryClaimRetired()
        if verdict.retry_after_seconds is None:
            raise TransientDeliveryError("Token verification unavailable")
        raise DeliveryClaimRetired()

    async def _resume_invalid_token(self, captured: SendingPart, pending: dict) -> None:
        from app.telegram_bots.producer import DeliveryClaimRetired, PermanentDeliveryError

        # A completed check after the deferred failure is reusable. A rebuilt
        # adapter has no such cohort and proves the token through a fresh getMe.
        verdict = None
        check = self._verify_task
        if check is not None and check.done() and not check.cancelled():
            try:
                result = check.result()
            except Exception:
                result = None
            if result is not None and result.status == "valid":
                verdict = result
        if verdict is None:
            verdict = await self.verify_token()
        if verdict.status == "valid":
            fault = PermanentDeliveryError("invalid-payload")
            fault._hartmesh_telegram_failure_logged = pending["failure_logged"]
            raise fault
        if verdict.status == "unverified":
            await self._defer_invalid_token(captured, verdict, http_status=pending["http_status"])
        raise DeliveryClaimRetired()

    async def send_part(self, *, outbox_id: str, part_id: str, claim_owner: str, claim_id: str) -> list[int]:
        from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryEligibility, DeliveryRetry, PermanentDeliveryError, SourceUnavailable

        if not self.application.running or self._stopping or self._services.channel(self.link.id) is not self or not self._send_allowed():
            raise DeliveryClaimRetired()
        capture = dict(link_id=self.link.id, outbox_id=outbox_id, part_id=part_id, claim_owner=claim_owner, claim_id=claim_id, token_revision=self.transport_revision)
        saved = await self._services.outbox.capture_sending_part(**capture)
        part = saved.part
        pending = part.get("invalid_token_verification_pending")
        if pending is not None:
            if (
                not isinstance(pending, dict)
                or not {"token_revision", "http_status", "failure_logged"}.issubset(pending)
                or type(pending.get("token_revision")) is not int
                or pending["token_revision"] < 0
                or type(pending.get("failure_logged")) is not bool
                or pending.get("http_status") is not None
                and (type(pending["http_status"]) is not int or pending["http_status"] not in {401, 404})
            ):
                raise PermanentDeliveryError("invalid-verification-continuation")
            if pending["token_revision"] == self.transport_revision:
                await self._resume_invalid_token(saved, pending)
        if part.get("callback_group_id"):
            raise PermanentDeliveryError("unsupported-callback-authority")
        if part["kind"] in {"document", "photo", "album"}:
            name = PurePosixPath(str(part["source_ref"].get("path", "file"))).name or "file"
            raise SourceUnavailable(source_ref={"key": "file.out_failed", "values": {"file": name}})
        if part["kind"] not in {"text", "edit", "card"}:
            raise PermanentDeliveryError("unsupported-source")
        text = await self._render_source(saved.link, part["source_ref"], plain=part.get("plain_fallback", False))
        settings = await self._services.settings.get(saved.link.owner_user_id)
        if settings is None:
            raise PermanentDeliveryError("missing-settings")
        silent = saved.item.silent or bool(part.get("silent", False)) or await asyncio.to_thread(silent_now, saved.link, settings, self._services.config, self._now())
        current = await self._services.outbox.capture_sending_part(**capture)
        if current.part != part or current.item.message_thread_id != saved.item.message_thread_id:
            raise DeliveryClaimRetired()
        decision = await self._services.eligibility(current.link, current.item, current.part)
        if not isinstance(decision, DeliveryEligibility) or decision.decision not in {"allow", "wait", "skip"} or type(decision.protect_content) is not bool:
            raise ValueError("invalid-delivery-eligibility")
        if decision.decision != "allow" or decision.protect_content and not part.get("protect_content", False):
            # A new worker must persist stronger protection before any request;
            # never silently send under a different receipt requirement.
            raise DeliveryClaimRetired()
        latest = await self._services.outbox.capture_sending_part(**capture)
        basis = ("id", "owner_user_id", "agent_kind", "agent_ref", "bot_id", "connection_id", "binding_generation", "telegram_user_id", "chat_id", "token_revision")
        if (
            latest.part != current.part
            or latest.item != current.item
            or any(getattr(latest.link, name) != getattr(current.link, name) for name in basis)
            or not self.application.running
            or self._stopping
            or self._services.channel(self.link.id) is not self
            or not self._send_allowed()
        ):
            raise DeliveryClaimRetired()
        current = latest
        self.link = current.link
        context = SendContext(current.link.id, current.item.chat_id, current.link.binding_generation, self.transport_revision, "bound_message")
        method = "sendMessage" if part["kind"] != "edit" else "editMessageReplyMarkup" if part.get("remove_keyboard") else "editMessageText"
        try:
            if part["kind"] == "edit":
                target = part.get("target_message_id")
                if type(target) is not int or target <= 0:
                    raise PermanentDeliveryError("missing-edit-anchor")
                if part.get("remove_keyboard"):
                    message = await self.bot.edit_message_reply_markup(chat_id=context.chat_id, message_id=target, reply_markup=None)
                else:
                    message = await self.bot.edit_message_text(chat_id=context.chat_id, message_id=target, text=text, parse_mode=None if part.get("plain_fallback") else ParseMode.HTML)
            else:
                message = await self.bot.send_message(
                    chat_id=context.chat_id,
                    text=text,
                    parse_mode=None if part.get("plain_fallback") else ParseMode.HTML,
                    reply_to_message_id=part.get("reply_to_message_id"),
                    disable_notification=silent,
                    message_thread_id=current.item.message_thread_id,
                    protect_content=bool(part.get("protect_content", False)),
                )
        except (LongRetryAfter, RetryAfter) as error:
            self._failure()
            seconds = error.seconds if isinstance(error, LongRetryAfter) else error._retry_after.total_seconds()
            self._log_part_failure(method, error, current)
            raise DeliveryRetry(seconds) from None
        except Forbidden as error:
            self._failure()
            self._log_part_failure(method, error, current)
            await self._forbidden(context)
            raise
        except InvalidToken as error:
            self._failure()
            verdict = await self.verify_token(error)
            if verdict.status == "valid":
                self._log_part_failure(method, error, current, failed_reason="invalid-payload")
                mapped = PermanentDeliveryError("invalid-payload")
                mapped._hartmesh_telegram_failure_logged = True
                raise mapped from None
            self._log_part_failure(method, error, current)
            if verdict.status == "unverified":
                await self._defer_invalid_token(current, verdict, http_status=_invalid_status(error))
            raise DeliveryClaimRetired() from None
        except BadRequest as error:
            description = str(error).lower()
            if "message is not modified" in description and part["kind"] == "edit":
                await self._after_send_success()
                return [part["target_message_id"]]
            self._failure()
            structural = (
                "can't parse entities" in description
                and not part.get("plain_fallback")
                or any(value in description for value in ("message to edit not found", "message can't be edited", "message cannot be edited", "message thread not found"))
            )
            self._log_part_failure(method, error, current, failed_reason=None if structural else "invalid-payload")
            if "can't parse entities" in description and not part.get("plain_fallback"):
                await self._services.outbox.fallback_to_plain(current)
                raise DeliveryClaimRetired() from None
            raise
        except NetworkError as error:
            self._failure()
            self._log_part_failure(method, error, current)
            raise
        await self._after_send_success()
        return [part["target_message_id"] if message is True else message.message_id]
