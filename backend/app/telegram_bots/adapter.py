"""One explicitly started SDK Application per managed bot, on the owning loop.

Product plans and worker receipts belong to the outbox. This adapter only sends
the exact claimed part, and never inherits the deployment-wide bot lifecycle.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from html import escape
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any, Protocol

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, LinkPreviewOptions, Update, User
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, InvalidToken, NetworkError, RetryAfter, TelegramError
from telegram.ext import AIORateLimiter, Application, ApplicationHandlerStop, CallbackQueryHandler, ChatMemberHandler, CommandHandler, Defaults, ExtBot, ManagedBotUpdatedHandler, MessageHandler, TypeHandler, filters
from telegram.request import HTTPXRequest

from app.channels.telegram import TelegramChannel
from app.telegram_bots.render import render, render_plain
from app.telegram_bots.settings import silent_now
from app.telegram_bots.strangers import BAD_CODE_REPLIES_PER_SENDER_PER_HOUR as BAD_CODE_REPLIES_PER_SENDER_PER_HOUR
from app.telegram_bots.strangers import STRANGER_MAP_MAX as STRANGER_MAP_MAX
from app.telegram_bots.strangers import STRANGER_REPLIES_PER_BOT_PER_HOUR as STRANGER_REPLIES_PER_BOT_PER_HOUR
from app.telegram_bots.strangers import STRANGER_REPLY_EVERY_SECONDS as STRANGER_REPLY_EVERY_SECONDS
from app.telegram_bots.strangers import StrangerLedger as StrangerLedger
from app.telegram_bots.token_pattern import TOKEN_BODY as TOKEN_BODY
from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from app.channels.message_bus import OutboundMessage
    from app.telegram_bots.acts import Acts
    from app.telegram_bots.callbacks import CallbackRepository, Prompts
    from app.telegram_bots.commands import CallbackCommands
    from app.telegram_bots.config import TelegramBotsConfig
    from app.telegram_bots.inbox import InboxRow
    from app.telegram_bots.links import BotLink, BotLinkRepository
    from app.telegram_bots.model_source import ModelSource
    from app.telegram_bots.producer import DeliveryEligibility, Outbox, OutboxItem, SendingPart
    from app.telegram_bots.settings import SettingsRepository

LIMITER_HOLD_MAX_SECONDS = 5
SEND_RETRY_AFTER_MAX_SECONDS = 60
EDIT_HASHES_PER_BOT = 256
MEDIA_UNSUPPORTED_EVERY_SECONDS = 60
FIXED_COMMANDS = ("work", "assign", "assistants", "employees", "new", "stop", "settings", "help", "web", "status", "privacy", "agent", "bootstrap", "goal", "models", "memory")
_TOKEN_PASTED = re.compile(r"(?<![0-9])" + TOKEN_BODY)
logger = logging.getLogger(__name__)


@dataclass
class _Admission:
    link: Any
    update_id: int
    task: asyncio.Task
    acknowledged: bool = False
    failed: bool = False


_ADMISSION: ContextVar[_Admission | None] = ContextVar("telegram_ingress_admission", default=None)


class LongRetryAfter(TelegramError):
    def __init__(self, seconds: float) -> None:
        super().__init__("Server retry deferred")
        self.seconds = seconds


@dataclass(frozen=True)
class LiveRequestFence:
    check: Callable[[], Awaitable[None]]


class ManagedExtBot(ExtBot):
    def _insert_defaults(self, data):
        # The pinned SDK treats the empty string as an empty InputPollOption
        # sequence. Telegram's empty initial draft requires a string on wire.
        empty_draft = "draft_id" in data and data.get("text") == ""
        super()._insert_defaults(data)
        if empty_draft:
            data["text"] = ""


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
        fence = rate_limit_args if type(rate_limit_args) is LiveRequestFence else None

        async def guarded(*callback_args, **callback_kwargs):
            while (remaining := self._short_hold_until - self._time()) > 0:
                await self._sleep(remaining)
            if fence is not None:
                await fence.check()
            try:
                return await callback(*callback_args, **callback_kwargs)
            except RetryAfter as error:
                seconds = error._retry_after.total_seconds()
                if seconds > LIMITER_HOLD_MAX_SECONDS:
                    raise LongRetryAfter(seconds) from None
                self._short_hold_until = max(self._short_hold_until, self._time() + seconds + 0.1)
                raise

        return await super().process_request(guarded, args, kwargs, endpoint, data, None if fence is not None else rate_limit_args)


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
    strangers: StrangerLedger
    product_name: str
    model_source: ModelSource | None
    callbacks: CallbackRepository | None
    prompts: Prompts | None
    acts: Acts | None
    commands: CallbackCommands | None

    async def prepare_live_owner(self, owner_user_id: str) -> Any: ...

    async def prepare_ingress_values(self, link: BotLink) -> dict: ...

    async def notice_access_off(self, link: BotLink) -> None: ...

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
        self._callback_locale: tuple[str, int, str] | None = None
        self._verify_lock, self._lifecycle_lock, self._success_lock = asyncio.Lock(), asyncio.Lock(), asyncio.Lock()
        self._verify_task: asyncio.Task[TokenVerification] | None = None
        self._verify_not_before = 0.0
        self._first_401: float | None = None
        self._close_task: asyncio.Task | None = None
        self._deferred_close: asyncio.Task | None = None
        self._stopping = False
        self._ingress_lock = asyncio.Lock()
        self._ingress_tasks: set[asyncio.Task] = set()
        self._edit_hashes: OrderedDict[tuple, bytes] = OrderedDict()
        self._unsupported_at: dict[tuple, float] = {}
        self._needs_success = True
        self._failure_serial = 0
        factory = getattr(supervisor, "request_factory", HTTPXRequest)
        self._requests = (
            factory(connection_pool_size=8, httpx_kwargs={"verify": supervisor.tls_context}),
            factory(connection_pool_size=1, httpx_kwargs={"verify": supervisor.tls_context}),
        )
        self._application = (
            Application.builder()
            .bot(
                ManagedExtBot(
                    token=token,
                    request=self._requests[0],
                    get_updates_request=self._requests[1],
                    defaults=Defaults(link_preview_options=LinkPreviewOptions(is_disabled=True), allow_sending_without_reply=True),
                    rate_limiter=BoundedRateLimiter(overall_max_rate=30, overall_time_period=1, max_retries=2, monotonic=monotonic, sleep=sleep),
                )
            )
            .updater(None)
            .job_queue(None)
            .build()
        )
        self._register_handlers()

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
        # Qualified previews join the exact persisted run independently.
        return False

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

    def _chat_message_sent(self, chat_id, topic):
        turns = getattr(self._services, "turns", None)
        if turns is not None:
            turns.message_sent(self, chat_id=chat_id, topic=topic)

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
        turns = getattr(self._services, "turns", None)
        if turns is not None:
            await turns.retire(self)
        for task in tuple(self._ingress_tasks):
            task.cancel()
        await await_drained(self._drain_ingress())
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
        async with self._ingress_lock:
            if not self.application.running or self._stopping or not self._send_allowed() or self._services.channel(self.link.id) is not self:
                return False
            current = await await_drained(self.refresh_link())
            if current is None:
                return False
            admission = _Admission(current, update.update_id, asyncio.current_task())
            marker = _ADMISSION.set(admission)
            try:
                await self.application.process_update(update)
                return admission.acknowledged and not admission.failed
            finally:
                _ADMISSION.reset(marker)

    def _register_handlers(self) -> None:
        self.application.add_handler(TypeHandler(Update, self._gate), group=-1)
        new = filters.UpdateType.MESSAGE
        handlers = (
            ChatMemberHandler(self._on_my_chat_member, ChatMemberHandler.MY_CHAT_MEMBER),
            ManagedBotUpdatedHandler(self._on_managed_bot),
            CallbackQueryHandler(self._on_callback, pattern=r"^h1:[0-9a-f]{32}$"),
            CallbackQueryHandler(self._on_unknown_callback),
            MessageHandler(filters.UpdateType.EDITED_MESSAGE, self._on_edited),
            MessageHandler(new & filters.StatusUpdate.ALL, self._on_service),
            MessageHandler(new & (filters.Regex(_TOKEN_PASTED) | filters.CaptionRegex(_TOKEN_PASTED)), self._on_token_pasted),
            CommandHandler("start", self._cmd_start, filters=new),
            CommandHandler(FIXED_COMMANDS, self._on_command, filters=new),
            MessageHandler(new & filters.TEXT & filters.COMMAND, self._on_other_slash),
            MessageHandler(new & filters.TEXT & ~filters.COMMAND, self._on_text),
            MessageHandler(new & (filters.PHOTO | filters.Document.ALL), self._on_text),
            MessageHandler(new & filters.VOICE, self._on_voice),
            MessageHandler(new & filters.ALL, self._on_unsupported),
        )
        for handler in handlers:
            self.application.add_handler(handler, group=0)
        self.application.add_error_handler(self._on_error, block=True)

    def _admission(self, update) -> _Admission:
        admission = _ADMISSION.get()
        if admission is None or admission.task is not asyncio.current_task() or admission.update_id != update.update_id:
            raise RuntimeError("unowned-ingress-update")
        return admission

    def _classified(self, update, *, remember=False) -> None:
        admission = self._admission(update)
        admission.acknowledged = True
        message = update.message
        if remember and message is not None and (message.text is not None or message.caption is not None):
            value = message.text if message.text is not None else message.caption
            key = (admission.link.binding_generation, message.chat_id, message.message_id)
            self._edit_hashes[key] = hashlib.sha256(value.encode("utf-8")).digest()
            self._edit_hashes.move_to_end(key)
            while len(self._edit_hashes) > EDIT_HASHES_PER_BOT:
                self._edit_hashes.popitem(last=False)

    @staticmethod
    def _same_scope(left, right) -> bool:
        fields = ("id", "owner_user_id", "agent_kind", "agent_ref", "bot_id", "token_revision", "binding_generation", "connection_id", "telegram_user_id", "chat_id")
        return right is not None and all(getattr(left, field) == getattr(right, field) for field in fields)

    def _command(self, message) -> tuple[str, list[str]]:
        text = message.text or ""
        admission = _ADMISSION.get()
        captured = admission.link if admission is not None and admission.task is asyncio.current_task() else self.link
        normalized = self._strip_bot_username_from_leading_command(text.lstrip(), captured.bot_username)
        words = normalized.split()
        return (words[0][1:].lower(), words[1:]) if words and words[0].startswith("/") else ("", [])

    def _context(self, link, chat_id, purpose) -> SendContext:
        return SendContext(link.id, chat_id, link.binding_generation, self.transport_revision, purpose)

    async def _ingress_current(self, context: SendContext) -> bool:
        if not await self._notice_context_current(context):
            return False
        return self.link.health in {"ok", "agent_unavailable"} and self.link.state != "held" and self._send_allowed()

    async def _input_current(self, captured) -> bool:
        current = await await_drained(self.refresh_link())
        return self._same_scope(captured, current) and self.application.running and not self._stopping and self._services.channel(captured.id) is self and self._send_allowed() and current.health in {"ok", "agent_unavailable"}

    def _callback_current(self, captured) -> bool:
        return bool(self._same_scope(captured, self.link) and self.transport_revision == captured.token_revision and self.application.running and not self._stopping and self._services.channel(captured.id) is self and self._send_allowed())

    def _spawn_ingress(self, coroutine) -> bool:
        if self._stopping or not self.application.running or not self._send_allowed() or self._services.channel(self.link.id) is not self:
            coroutine.close()
            return False
        marker = _ADMISSION.set(None)
        try:
            task = self.application.create_task(coroutine, update=None, name="telegram-disposable-ingress")
        finally:
            _ADMISSION.reset(marker)
        self._ingress_tasks.add(task)
        task.add_done_callback(self._ingress_tasks.discard)
        return True

    async def _drain_ingress(self) -> None:
        await asyncio.gather(*tuple(self._ingress_tasks), return_exceptions=True)

    async def _on_error(self, update, context) -> None:
        logger.warning("telegram_bots handler_failed link=%s bot=%s error=%s", self.link.id, self.link.bot_id, type(context.error).__name__)
        admission = _ADMISSION.get()
        if admission is not None and admission.task is asyncio.current_task() and update is not None and admission.update_id == update.update_id:
            admission.failed = True
            raise ApplicationHandlerStop

    async def _gate(self, update, context) -> None:
        admission = self._admission(update)
        link = admission.link
        if not self._send_allowed() or self._stopping or self._services.channel(link.id) is not self or link.health not in {"ok", "agent_unavailable"}:
            raise ApplicationHandlerStop
        user, chat = update.effective_user, update.effective_chat
        if user is not None and type(user.id) is int and user.id > 0:
            self._services.strangers.touch(link.bot_id, user.id, self._time())
        if update.api_kwargs.get("stopped_message_generation") is not None:
            turns = getattr(self._services, "turns", None)
            if turns is not None and link.state == "connected":
                await turns.native_stop(self, update)
            self._classified(update)
            raise ApplicationHandlerStop
        if update.managed_bot is not None:
            return
        if chat is not None and chat.type != "private":
            member = update.my_chat_member
            if member is not None and member.new_chat_member.user.id == link.bot_id and member.old_chat_member.status in {"left", "kicked"} and member.new_chat_member.status in {"member", "administrator"}:
                self._spawn_ingress(self.leave_group(self._context(link, chat.id, "group")))
            self._classified(update)
            raise ApplicationHandlerStop
        if update.my_chat_member is not None:
            return
        message = update.message
        word, arguments = self._command(message) if message is not None else ("", [])
        if link.state == "waiting_connect" and word == "start" and arguments and link.bind_mode == "code" and user is not None and not user.is_bot:
            return
        if user is None or user.is_bot or user.id != link.telegram_user_id:
            if update.callback_query is not None:
                self._spawn_ingress(self.answer_press(update.callback_query.id, "stale.other_person", context=self._context(link, chat.id if chat else None, "callback")))
            elif message is not None and user is not None and not user.is_bot and user.id > 0 and self._services.strangers.seen(link.bot_id, user.id, self._time()):
                self._spawn_ingress(self._ingress_notice("stranger", self._context(link, message.chat_id, "stranger")))
            self._classified(update)
            raise ApplicationHandlerStop
        if link.state == "held":
            self._spawn_ingress(self._services.notice_access_off(link))
            self._classified(update)
            raise ApplicationHandlerStop
        if chat is None or chat.id != user.id or chat.id != link.chat_id:
            if update.callback_query is not None:
                self._spawn_ingress(self.answer_press(update.callback_query.id, "stale.other_person", context=self._context(link, chat.id if chat else None, "callback")))
            self._classified(update)
            raise ApplicationHandlerStop
        if message is not None and link.state == "blocked":
            checker = await self._services.prepare_live_owner(link.owner_user_id)
            if not await self._input_current(link):
                raise ApplicationHandlerStop
            await await_drained(self._services.links.mark_unblocked_if_current(link, check_live_owner=checker))
            current = await await_drained(self.refresh_link())
            if not self._same_scope(link, current) or current.state != "connected":
                raise ApplicationHandlerStop
            admission.link = current

    async def _on_my_chat_member(self, update, context) -> None:
        link, member = self._admission(update).link, update.my_chat_member
        if member.chat.type == "private" and member.chat.id == link.chat_id and member.new_chat_member.user.id == link.bot_id:
            if member.new_chat_member.status == "kicked":
                await await_drained(self._services.links.mark_blocked_if_current(link_id=link.id, chat_id=member.chat.id, binding_generation=link.binding_generation, token_revision=self.transport_revision))
            elif member.new_chat_member.status == "member" and link.state == "blocked":
                checker = await self._services.prepare_live_owner(link.owner_user_id)
                if not await self._input_current(link):
                    return
                await await_drained(self._services.links.mark_unblocked_if_current(link, check_live_owner=checker))
        self._classified(update)

    async def _on_managed_bot(self, update, context) -> None:
        logger.info("telegram_bots managed_update_ignored link=%s bot=%s", self.link.id, self.link.bot_id)
        self._classified(update)

    async def _on_callback(self, update, context) -> None:
        from app.telegram_bots.callbacks import CallbackHandler

        repository = getattr(self._services, "callbacks", None)
        if repository is None or getattr(self._services, "commands", None) is None:
            await self._on_unknown_callback(update, context)
            return
        captured = self._admission(update).link
        query = update.callback_query

        async def execute(channel, row):
            await channel.execute_callback(row, query_id=query.id, update_id=update.update_id)

        handler = CallbackHandler(repository, execute=execute)
        self._spawn_ingress(handler.press(self, query, captured_link=captured))
        self._classified(update)

    async def execute_callback(self, row, *, query_id=None, update_id=None):
        human_inputs = getattr(self._services, "human_inputs", None)
        if human_inputs is not None and (
            row.kind in {"clarify_option", "clarify_other", "form_option", "form_next", "form_skip", "form_send", "form_change", "form_field", "network", "prompt_reopen", "prompt_send_new", "prompt_discard"}
            or row.kind == "back"
            and "form_id" in row.payload
        ):
            return await human_inputs.execute_callback(self, row, update_id=update_id, query_id=query_id)
        turns = getattr(self._services, "turns", None)
        if turns is not None and row.kind in {"retry_message", "cancel_wait", "stop_answer"}:
            return await turns.execute_callback(self, row, update_id=update_id, query_id=query_id)
        await self._services.commands.execute(self, row, query_id=query_id)

    async def _on_unknown_callback(self, update, context) -> None:
        link, query = self._admission(update).link, update.callback_query
        chat = query.message.chat.id if query.message is not None else None
        self._spawn_ingress(self.answer_press(query.id, "stale.not_waiting" if chat == link.chat_id else "stale.other_person", context=self._context(link, chat, "callback")))
        self._classified(update)

    async def _on_service(self, update, context) -> None:
        self._classified(update)

    async def _on_token_pasted(self, update, context) -> None:
        link = self._admission(update).link
        message = update.message or update.edited_message
        # Only safe numeric identity crosses the task boundary, never Update.
        purpose = "bound_message" if message.chat_id == link.chat_id else "stranger"
        self._spawn_ingress(self._delete_token(self._context(link, message.chat_id, "optional"), self._context(link, message.chat_id, purpose), message.message_id))
        self._classified(update)

    async def leave_group(self, context: SendContext) -> None:
        if context.purpose != "group" or context.chat_id is None or context.chat_id >= 0:
            raise ValueError("invalid-group-context")
        if not await self._ingress_current(context):
            return
        try:
            await self.bot.leave_chat(context.chat_id)
        except InvalidToken as error:
            self._log_failure("leaveChat", error)
            await self.verify_token(error)
        except TelegramError as error:
            self._log_failure("leaveChat", error)

    async def _ingress_notice(self, key, context, **values):
        if await self._ingress_current(context):
            return await self.send_notice(key, context=context, _ingress=True, **values)
        return None

    async def _delete_token(self, deletion: SendContext, notice: SendContext, message_id: int) -> None:
        if (
            deletion.purpose != "optional"
            or deletion.chat_id is None
            or deletion.chat_id <= 0
            or notice.purpose not in {"bound_message", "stranger"}
            or any(getattr(deletion, key) != getattr(notice, key) for key in ("link_id", "chat_id", "binding_generation", "token_revision"))
            or type(message_id) is not int
            or message_id <= 0
        ):
            raise ValueError("invalid-token-refusal-context")
        if not await self._ingress_current(deletion):
            return
        deleted = False
        try:
            await self.bot.delete_message(chat_id=deletion.chat_id, message_id=message_id)
            deleted = True
        except InvalidToken as error:
            self._log_failure("deleteMessage", error)
            await self.verify_token(error)
        except TelegramError as error:
            self._log_failure("deleteMessage", error)
        await self._ingress_notice("token.pasted" if deleted else "token.pasted_kept", notice, product=self._services.product_name)

    async def _on_edited(self, update, context) -> None:
        link, message = self._admission(update).link, update.edited_message
        if any(_TOKEN_PASTED.search(value or "") for value in (message.text, message.caption)):
            await self._on_token_pasted(update, context)
            return
        value = message.text if message.text is not None else message.caption or ""
        key = (link.binding_generation, message.chat_id, message.message_id)
        digest = hashlib.sha256(value.encode("utf-8")).digest()
        if key in self._edit_hashes and self._edit_hashes[key] != digest:
            if not await self._input_current(link):
                return
            await await_drained(self.send_text("edit.ignored", source_id=self._source(update, "edit"), reply_to=message.message_id, _captured_link=link))
            self._edit_hashes[key] = digest
            self._edit_hashes.move_to_end(key)
        self._classified(update)

    def _source(self, update, kind):
        link = self._admission(update).link
        return hashlib.sha256(f"{link.id}:{link.binding_generation}:{link.chat_id}:{update.update_id}:{kind}".encode()).hexdigest()

    async def _cmd_start(self, update, context) -> None:
        word, arguments = self._command(update.message)
        if word != "start":
            await self._on_other_slash(update, context)
            return
        link, message = self._admission(update).link, update.message
        if link.state == "connected":
            await self._fixed_reply(update, "hello.again", "start")
            return
        if link.state != "waiting_connect" or link.bind_mode != "code" or message.from_user is None or message.from_user.is_bot or message.from_user.id != message.chat_id:
            self._classified(update)
            return
        checker = await self._services.prepare_live_owner(link.owner_user_id)
        values = await self._services.prepare_ingress_values(link)
        if not await self._input_current(link):
            return
        hello = "hello.chat" if link.agent_kind == "lead_agent" else "hello.custom_agent"
        try:
            saved = await await_drained(
                self._services.links.bind(
                    link,
                    telegram_user_id=message.from_user.id,
                    chat_id=message.chat_id,
                    first_name=message.from_user.first_name,
                    username=message.from_user.username,
                    start_identity={"update_id": update.update_id, "message_id": message.message_id, "chat_id": message.chat_id, "message_thread_id": message.message_thread_id},
                    hello_parts=[{"source_ref": {"key": hello, "values": {"email": checker.email, "product": values["product"], "agent": values["agent"]}}}, {"source_ref": {"key": "hello.privacy", "values": {}}, "silent": True}],
                    code=arguments[0] if len(arguments) == 1 else None,
                    check_live_owner=checker,
                )
            )
        except ValueError as error:
            key = {"expired": "hello.link_expired", "other-bot": "hello.refused", "account-taken": "hello.account_taken"}.get(str(error))
            if key is None:
                raise
            if self._send_allowed() and self._services.strangers.bad_code(link.bot_id, message.from_user.id, self._time()):
                self._spawn_ingress(self._ingress_notice(key, self._context(link, message.chat_id, "stranger")))
            self._classified(update)
            return
        self.link = saved
        self._admission(update).link = saved
        self._classified(update, remember=True)

    async def _fixed_reply(self, update, key, word, *, prepared=None):
        link, message = self._admission(update).link, update.message
        await self._services.prepare_live_owner(link.owner_user_id)
        if not await self._input_current(link):
            return
        row, created = await await_drained(self._services.outbox.inbox.accept(link, update, sanitized_input={"command": word}))
        if row.binding_generation != link.binding_generation:
            self._classified(update)
            return
        if row.route_complete:
            self._classified(update, remember=True)
            return
        if row.route_kind is None:
            values = prepared or await self._services.prepare_ingress_values(link)
            if not await self._input_current(link):
                return
            source_values = {"command": word} if key == "cmd.unknown" else {"name": values["name"]} if key == "cmd.agent" else {"name": values["name"], "product": values["product"]} if key == "web.memory" else {}
            button = {"page": "web.memory", "url": values["memory_url"]} if key == "web.memory" and values.get("memory_url") else None
            row = await await_drained(self._services.outbox.inbox.route(row, kind="command", target_id=word or "unknown", basis={"key": key, "values": source_values, "url_button": button}, operation_ids=[]))
        basis = row.route_basis
        if not await self._input_current(link):
            return
        prompts = getattr(self._services, "prompts", None)
        if prompts is not None and basis.get("human_input_ui") != {"command_closed": word}:
            if not self._callback_current(link):
                return
            await prompts.close(link, row.chat_id, row.message_thread_id, reason="command")
            if not self._callback_current(link):
                return
        await await_drained(self.send_text(basis["key"], source_id=self._source(update, "command"), reply_to=message.message_id, inbox_rows=[row], finish_route=True, url_button=basis["url_button"], _captured_link=link, **basis["values"]))
        self._classified(update, remember=True)

    async def _on_command(self, update, context) -> None:
        word, arguments = self._command(update.message)
        human_inputs = getattr(self._services, "human_inputs", None)
        if human_inputs is not None and word != "stop":
            await human_inputs.command(self, update, word, captured_link=self._admission(update).link)
        turns = getattr(self._services, "turns", None)
        if word == "stop" and turns is not None:
            await turns.stop_chat(self, update)
            self._classified(update, remember=True)
        elif word not in FIXED_COMMANDS:
            await self._on_other_slash(update, context)
        elif word == "agent":
            await self._fixed_reply(update, "cmd.agent", word)
        elif word == "memory" and self.link.agent_kind == "lead_agent":
            await self._fixed_reply(update, "web.memory", word)
        elif word in {"new", "help", "privacy", "settings", "web"} and getattr(self._services, "commands", None) is not None:
            await self._services.commands.handle(self, update, word)
            self._classified(update, remember=True)
        else:
            await self._fixed_reply(update, "cmd.unknown", word)

    async def _on_other_slash(self, update, context) -> None:
        from deerflow.skills.slash import parse_slash_skill_reference

        word, arguments = self._command(update.message)
        if word == "start":
            await self._cmd_start(update, context)
        elif word in FIXED_COMMANDS:
            await self._on_command(update, context)
        elif parse_slash_skill_reference(update.message.text or "") is None:
            await self._fixed_reply(update, "cmd.unknown", word)
        # A genuine potential skill waits for its real catalog/dispatch owner.

    async def _on_text(self, update, context) -> None:
        if self._command(update.message)[0]:
            await self._on_other_slash(update, context)
            return
        turns = getattr(self._services, "turns", None)
        if turns is not None:
            if type(update.message.text) is not str or not update.message.text.strip():
                await self._on_unsupported(update, context)
                return
            human_inputs = getattr(self._services, "human_inputs", None)
            if human_inputs is not None and await human_inputs.handle_text(self, update, captured_link=self._admission(update).link):
                self._classified(update)
                return
            link = self._admission(update).link
            typing_allowed = getattr(turns, "typing_allowed", None)
            topic = update.message.message_thread_id

            def current():
                return self._callback_current(link) and (typing_allowed is None or typing_allowed(self, topic=topic))

            if current():
                self._spawn_ingress(self.live_request(link, "send_chat_action", is_current=current, action="typing", message_thread_id=topic))
            await turns.accept(self, update)

    async def _on_voice(self, update, context) -> None:
        if getattr(self._services, "turns", None) is not None:
            await self._on_unsupported(update, context)

    async def _on_unsupported(self, update, context) -> None:
        link, message = self._admission(update).link, update.message
        key = (link.binding_generation, message.chat_id)
        now = self._time()
        if now - self._unsupported_at.get(key, float("-inf")) >= MEDIA_UNSUPPORTED_EVERY_SECONDS:
            if not await self._input_current(link):
                return
            await await_drained(self.send_text("media.unsupported", source_id=self._source(update, "unsupported"), reply_to=message.message_id, _captured_link=link))
            self._unsupported_at = {key: now}
        self._classified(update)

    async def _run_on_telegram_loop(self, coroutine):
        return await coroutine

    async def live_request(self, link, method, *, is_current, protected=None, **kwargs):
        """Disposable presentation call with a fence inside limiter retries."""
        from app.telegram_bots.authority import require
        from app.telegram_bots.producer import DeliveryClaimRetired

        context = self._context(link, link.chat_id, "bound_message")

        async def check():
            if is_current() is not True or not await self._input_current(link):
                raise DeliveryClaimRetired()
            await self._services.prepare_live_owner(link.owner_user_id)
            await require(link.owner_user_id, "threads:read", "runs:read")
            policy = await self._services.protect_notice(context)
            if type(policy) is not bool or protected is not None and policy != protected or is_current() is not True or not await self._input_current(link):
                raise DeliveryClaimRetired()
            if is_current() is not True or not self._callback_current(link):
                raise DeliveryClaimRetired()

        try:
            await check()
            result = await await_drained(getattr(self.bot, method)(chat_id=link.chat_id, rate_limit_args=LiveRequestFence(check), **kwargs))
        except DeliveryClaimRetired:
            return None
        except Forbidden as error:
            self._failure()
            self._log_failure(method, error)
            await self._forbidden(context)
            return None
        except InvalidToken as error:
            self._failure()
            self._log_failure(method, error)
            await self.verify_token(error)
            return None
        except TelegramError as error:
            self._failure()
            self._log_failure(method, error)
            return None
        await self._after_send_success()
        return result

    async def react_waiting(self, link, row):
        from telegram import ReactionTypeEmoji

        return await self.live_request(link, "set_message_reaction", is_current=lambda: self._callback_current(link), message_id=row.message_id, reaction=[ReactionTypeEmoji("👀")])

    async def _get_download_bot(self):
        return self.bot

    async def send(self, msg: OutboundMessage, _max_retries: int = 3) -> int | None:
        from app.telegram_bots.producer import PermanentDeliveryError

        turns = getattr(self._services, "turns", None)
        if turns is not None:
            return await turns.outbound(self, msg)
        raise PermanentDeliveryError("unsupported-source")

    async def send_text(
        self, key: str, *, source_id: str, reply_to: int | None = None, buttons=None, url_button=None, finish_route=False, silent: bool = False, thread: int | None = None, inbox_rows: Sequence[InboxRow] = (), _captured_link=None, **values
    ) -> str:
        if buttons is not None:
            raise ValueError("unimplemented-callback-authority")
        part = {"kind": "text", "source_ref": {"key": key, "values": values}, "silent": silent}
        if url_button is not None:
            from app.telegram_bots.producer import validate_url_button

            part["url_button"] = validate_url_button(url_button)
        if reply_to is not None:
            part["reply_to_message_id"] = reply_to
        return await self._services.outbox.plan_product(_captured_link or self.link, source_id=source_id, parts=[part], inbox_rows=inbox_rows, finish_route=finish_route, message_thread_id=thread)

    async def edit_text(self, message_id: int, key: str, *, source_id: str, buttons=None, inbox_rows: Sequence[InboxRow] = (), **values) -> str:
        if buttons is not None:
            raise ValueError("unimplemented-callback-authority")
        return await self._services.outbox.plan_product(self.link, source_id=source_id, parts=[{"kind": "edit", "source_ref": {"key": key, "values": values}, "target_message_id": message_id}], inbox_rows=inbox_rows)

    async def _render_source(self, link: BotLink, source: dict, *, plain: bool = False) -> str:
        from app.telegram_bots.producer import PermanentDeliveryError

        if set(source) != {"key", "values"} or not isinstance(source["key"], str) or not isinstance(source["values"], dict):
            raise PermanentDeliveryError("unsupported-source")
        settings = await await_drained(self._services.settings.get(link.owner_user_id))
        if settings is None:
            raise PermanentDeliveryError("missing-settings")
        self._callback_locale = (link.owner_user_id, link.binding_generation, settings.locale)
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

    async def send_notice(self, key: str, *, context: SendContext, reply_to: int | None = None, silent: bool = False, thread: int | None = None, _ingress=False, **values) -> int | None:
        if context.chat_id is None:
            raise ValueError("notice-destination-required")
        if not self.application.running:
            return None
        if not await self._notice_context_current(context):
            return None
        if _ingress and (self.link.health not in {"ok", "agent_unavailable"} or self.link.state == "held"):
            return None
        text = await self._render_source(self.link, {"key": key, "values": values})
        protect = await self._services.protect_notice(context)
        if type(protect) is not bool:
            raise ValueError("invalid-notice-policy")
        if not await self._notice_context_current(context):
            return None
        if _ingress and (self.link.health not in {"ok", "agent_unavailable"} or self.link.state == "held"):
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
        self._chat_message_sent(context.chat_id, thread)
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
        current = await await_drained(self.refresh_link())
        return (
            current is not None
            and current.binding_generation == context.binding_generation
            and (context.purpose not in {"bound_message", "optional"} or current.chat_id == context.chat_id or context.purpose == "optional" and current.chat_id is None)
            and self.application.running
            and not self._stopping
            and self._services.channel(context.link_id) is self
            and self._send_allowed()
        )

    async def answer_callback_ack(self, query_id: str, key: str | None, *, captured_link: BotLink, deadline: float) -> None:
        """Acknowledge the original press without granting action authority.

        The cached locale is presentation only and cannot cross owner/generation.
        Later actions still need their own current SQL custody/owner guard.
        """
        await self.answer_press(query_id, key, _captured_link=captured_link, _deadline=deadline)

    async def answer_press(self, query_id: str, key: str | None = None, *, context: SendContext | None = None, _captured_link: BotLink | None = None, _deadline: float | None = None, **values) -> None:
        if not self.application.running or not self._send_allowed():
            return
        logged = False
        try:
            deadline = asyncio.get_running_loop().time() + 2
            if _deadline is not None:
                deadline = min(deadline, _deadline)
            async with asyncio.timeout_at(deadline):
                if _captured_link is not None:
                    if not self._callback_current(_captured_link):
                        return
                    remembered = self._callback_locale
                    locale = remembered[2] if remembered is not None and remembered[:2] == (_captured_link.owner_user_id, _captured_link.binding_generation) else "en-US"
                    text = render_plain(key, locale, **values) if key else None
                else:
                    if context is not None and not await self._ingress_current(context) or context is None and await self.refresh_link() is None:
                        return
                    text = await self._render_source(self.link, {"key": key, "values": values}, plain=True) if key else None
                    if context is not None and not await self._ingress_current(context):
                        return
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
        from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryEligibility, DeliveryRetry, PermanentDeliveryError, PreviewExpired, SourceUnavailable, uses_model_source

        if not self.application.running or self._stopping or self._services.channel(self.link.id) is not self or not self._send_allowed():
            raise DeliveryClaimRetired()
        capture = dict(link_id=self.link.id, outbox_id=outbox_id, part_id=part_id, claim_owner=claim_owner, claim_id=claim_id, token_revision=self.transport_revision)
        saved = await self._services.outbox.capture_sending_part(**capture)
        turns = getattr(self._services, "turns", None)
        if saved.item.dedupe_key.startswith("notice:chat-stream:") and (turns is None or turns.preview_allowed(self, saved.item) is not True):
            raise PreviewExpired()
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
        model_part = None
        if uses_model_source(saved.item):
            # Validate model primaries before any legacy callback/media
            # disposition can turn an unknown source into a fixed failure.
            reader = self._services.model_source
            if reader is None:
                raise DeliveryClaimRetired()
            model_part = await reader.read_part(saved)
        stored_keyboard = None
        if part.get("stored_keyboard"):
            if model_part is not None and not model_part.human_input or getattr(self._services, "callbacks", None) is None:
                raise DeliveryClaimRetired()
            stored_keyboard = await self._services.callbacks.read_keyboard(saved)
        elif part.get("callback_group_id"):
            raise PermanentDeliveryError("unsupported-callback-authority")
        if part["kind"] in {"document", "photo", "album"}:
            name = PurePosixPath(str(part["source_ref"].get("path", "file"))).name or "file"
            raise SourceUnavailable(source_ref={"key": "file.out_failed", "values": {"file": name}})
        if part["kind"] not in {"text", "edit", "card"}:
            raise PermanentDeliveryError("unsupported-source")
        button = part.get("url_button")
        if button is not None:
            from app.telegram_bots.producer import url_button_label, validate_url_button

            if part["kind"] != "text":
                raise PermanentDeliveryError("invalid-url-button")
            button = validate_url_button(button)
            button_label = url_button_label(part["source_ref"], button)
        if model_part is None:
            text = await self._render_source(saved.link, part["source_ref"], plain=part.get("plain_fallback", False))
        else:
            text = model_part.plain_text if part.get("plain_fallback", False) else model_part.html
        settings = await self._services.settings.get(saved.link.owner_user_id)
        if settings is None:
            raise PermanentDeliveryError("missing-settings")
        markup = None
        stored_urls = tuple(button.url for line in stored_keyboard or () for button in line if button.url is not None)
        if stored_keyboard is not None:
            try:
                rendered_rows = [
                    [
                        InlineKeyboardButton(render_plain(button.label, settings.locale, **{**part["source_ref"].get("values", {}), **dict(button.label_values or {})}), callback_data=button.callback_data, url=button.url)
                        for button in line
                        if not (part.get("url_button_fallback") and button.url is not None)
                    ]
                    for line in stored_keyboard
                ]
                rows = [line for line in rendered_rows if line]
                # The SDK drops empty sequences from TelegramObject.to_dict().
                # Preserve the Bot API's explicit keyboard-clear representation.
                markup = InlineKeyboardMarkup(rows) if rows else {"inline_keyboard": []}
            except (KeyError, TypeError, ValueError):
                raise PermanentDeliveryError("invalid-product-source") from None
            if part.get("url_button_fallback"):
                text += "".join("\n" + (address if part.get("plain_fallback") else escape(address)) for address in stored_urls)
        if button is not None:
            if part.get("url_button_fallback"):
                text += "\n" + (button["url"] if part.get("plain_fallback") else escape(button["url"]))
            else:
                markup = InlineKeyboardMarkup([[InlineKeyboardButton(render_plain(button_label, settings.locale), url=button["url"])]])
        if part.get("force_reply"):
            from telegram import ForceReply

            if model_part is None or not model_part.human_input or stored_keyboard is not None or button is not None or part["kind"] != "text":
                raise PermanentDeliveryError("invalid-human-input-markup")
            markup = ForceReply(selective=True)
        elif model_part is not None and model_part.human_input and part["kind"] == "edit" and markup is None:
            markup = {"inline_keyboard": []}
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
        if stored_keyboard is not None:
            # Group consumption can change during rendering without changing the
            # outbox part. Never dispatch the earlier buffered controls afterward.
            if await self._services.callbacks.read_keyboard(latest) != stored_keyboard or not self._callback_current(latest.link):
                raise DeliveryClaimRetired()
        current = latest
        if current.item.dedupe_key.startswith("notice:chat-stream:") and (turns is None or turns.preview_allowed(self, current.item) is not True):
            raise PreviewExpired()
        self.link = current.link
        context = SendContext(current.link.id, current.item.chat_id, current.link.binding_generation, self.transport_revision, "bound_message")
        method = "sendMessage" if part["kind"] != "edit" else "editMessageReplyMarkup" if part.get("remove_keyboard") else "editMessageText"
        preview_fence = None
        if current.item.dedupe_key.startswith("notice:chat-stream:"):

            async def check_preview():
                if turns.preview_allowed(self, current.item) is not True:
                    raise PreviewExpired()
                latest_preview = await self._services.outbox.capture_sending_part(**capture)
                decision = await self._services.eligibility(latest_preview.link, latest_preview.item, latest_preview.part)
                if not isinstance(decision, DeliveryEligibility) or decision.decision != "allow" or decision.protect_content and not part.get("protect_content"):
                    raise DeliveryClaimRetired()
                if turns.preview_allowed(self, latest_preview.item) is not True:
                    raise PreviewExpired()
                if not await self._input_current(current.link):
                    raise DeliveryClaimRetired()
                if stored_keyboard is not None and await self._services.callbacks.read_keyboard(latest_preview) != stored_keyboard:
                    raise DeliveryClaimRetired()
                if turns.preview_allowed(self, latest_preview.item) is not True:
                    raise PreviewExpired()
                if not self._callback_current(current.link):
                    raise DeliveryClaimRetired()

            preview_fence = LiveRequestFence(check_preview)
        elif model_part is not None and model_part.human_input:

            async def check_human_input():
                if not self._callback_current(current.link):
                    raise DeliveryClaimRetired()
                latest_input = await self._services.outbox.capture_sending_part(**capture)
                if latest_input.part != part:
                    raise DeliveryClaimRetired()
                verdict = await self._services.eligibility(latest_input.link, latest_input.item, latest_input.part)
                if verdict.decision != "allow" or verdict.protect_content and not part.get("protect_content"):
                    raise DeliveryClaimRetired()
                refreshed = await self._services.model_source.read_part(latest_input)
                if refreshed != model_part or stored_keyboard is not None and await self._services.callbacks.read_keyboard(latest_input) != stored_keyboard:
                    raise DeliveryClaimRetired()
                if not await self._services.human_inputs.step_current(latest_input.item) or not self._callback_current(current.link):
                    raise DeliveryClaimRetired()

            preview_fence = LiveRequestFence(check_human_input)
        try:
            if part["kind"] == "edit":
                target = part.get("target_message_id")
                if type(target) is not int or target <= 0:
                    raise PermanentDeliveryError("missing-edit-anchor")
                if part.get("remove_keyboard"):
                    clear_markup = {"inline_keyboard": []} if model_part is not None and model_part.human_input else None
                    message = await self.bot.edit_message_reply_markup(chat_id=context.chat_id, message_id=target, reply_markup=clear_markup, rate_limit_args=preview_fence)
                else:
                    message = await self.bot.edit_message_text(chat_id=context.chat_id, message_id=target, text=text, parse_mode=None if part.get("plain_fallback") else ParseMode.HTML, reply_markup=markup, rate_limit_args=preview_fence)
            else:
                message = await self.bot.send_message(
                    chat_id=context.chat_id,
                    text=text,
                    parse_mode=None if part.get("plain_fallback") else ParseMode.HTML,
                    reply_to_message_id=part.get("reply_to_message_id"),
                    disable_notification=silent,
                    message_thread_id=current.item.message_thread_id,
                    protect_content=bool(part.get("protect_content", False)),
                    reply_markup=markup,
                    rate_limit_args=preview_fence,
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
                or (button is not None or stored_urls)
                and not part.get("url_button_fallback")
                and "url" in description
                and "invalid" in description
                or any(value in description for value in ("message to edit not found", "message can't be edited", "message cannot be edited", "message thread not found"))
            )
            self._log_part_failure(method, error, current, failed_reason=None if structural else "invalid-payload")
            if "can't parse entities" in description and not part.get("plain_fallback"):
                await self._services.outbox.fallback_to_plain(current)
                raise DeliveryClaimRetired() from None
            if (button is not None or stored_urls) and not part.get("url_button_fallback") and "url" in description and "invalid" in description:
                await self._services.outbox.fallback_url_button(current)
                raise DeliveryClaimRetired() from None
            raise
        except NetworkError as error:
            self._failure()
            self._log_part_failure(method, error, current)
            raise
        if method == "sendMessage":
            self._chat_message_sent(context.chat_id, current.item.message_thread_id)
        await self._after_send_success()
        return [part["target_message_id"] if message is True else message.message_id]
