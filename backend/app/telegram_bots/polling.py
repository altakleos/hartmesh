"""Single-owner polling with durable admission and captured-token fencing.

No task is started by this module. The future supervisor owns lifecycle/death
policy; expected Telegram failures stay local and product exceptions escape.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from sqlalchemy.exc import SQLAlchemyError
from telegram.error import BadRequest, Conflict, Forbidden, InvalidToken, NetworkError, RetryAfter, TimedOut

if TYPE_CHECKING:
    from telegram import Update

    from app.telegram_bots.adapter import TelegramBotChannel
    from app.telegram_bots.links import BotLink

POLL_TIMEOUT_SECONDS = 10
ALLOWED = ["message", "edited_message", "callback_query", "my_chat_member", "managed_bot", "stopped_message_generation"]
BACKOFF_FIRST_SECONDS, BACKOFF_FACTOR, BACKOFF_CAP_SECONDS = 1.0, 1.5, 30
CONFLICT_GRACE_SECONDS = 60
CONFLICT_PROBE_HOURS = 24
UNREACHABLE_AFTER_SECONDS = 300
OFFLINE_NOTICE_AFTER_HOURS = 23
TASK_RESTART_SECONDS, TASK_DEATHS_BEFORE_STOP, TASK_DEATHS_WINDOW_SECONDS = 5, 5, 600
TOKEN_RECHECK_SECONDS = 30
UPDATES_HELD_PER_LINK = 10

logger = logging.getLogger(__name__)


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass
class PollRecord:
    polls: int = 0
    successes: int = 0
    conflicts: int = 0
    webhook: bool = False


@dataclass(frozen=True)
class PollHealthTransition:
    token_revision: int
    conflict_since: datetime
    probe_until: datetime
    webhook_host: str | None

    def as_dict(self) -> dict:
        return {"conflict_since": self.conflict_since, "probe_until": self.probe_until, "webhook_host": self.webhook_host}


class PollTask:
    def __init__(
        self,
        channel: TelegramBotChannel,
        *,
        on_health: Callable[[str, bool], Awaitable[None]],
        record: PollRecord,
        on_result: Callable[[], bool] | None = None,
        clock: Callable[[], datetime] = _now,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.channel, self.on_health, self.record, self.on_result = channel, on_health, record, on_result
        self._transport_revision = channel.transport_revision
        self._link_id, self._bot_id = channel.link.id, channel.link.bot_id
        self._clock, self._sleep = clock, sleep
        self.offset: int | None = None
        self._operation = asyncio.Lock()
        self._wake = asyncio.Event()
        self._pending: list[Update] = []
        self._batch_last: int | None = None
        self._held: list[Update] = []
        self._retired = False
        self._backoff = BACKOFF_FIRST_SECONDS
        self._retry_after: float | None = None
        self._rate_until: datetime | None = None
        self._first_conflict: datetime | None = None
        self._webhook_checked = False
        self._webhook_host: str | None = channel.link.webhook_host
        self._webhook = bool(self._webhook_host)
        self._conflicted = channel.link.health == "conflict"
        self._conflict_startup = self._conflicted
        self._probe_until = channel.link.probe_until
        self._last_probe: datetime | None = None
        self._fence_pending: tuple[datetime, bool, dict] | None = None
        self._fence_committed = False
        self._drop_required = False
        self._network_since: datetime | None = None
        self._had_failure = False
        self._health_transition: PollHealthTransition | None = None
        self._pending_health: tuple[str, bool, PollHealthTransition | None] | None = None
        self._streak = 0
        self._down_since: datetime | None = None
        self._has_success = False
        self._probes = 0
        self._probe_expiry_reported = False
        self.back_online_due = False

    @property
    def transport_revision(self) -> int:
        return self._transport_revision

    @property
    def health_transition(self) -> PollHealthTransition | None:
        """Captured facts for the currently awaited two-argument health sink."""
        return self._health_transition

    @property
    def links(self):
        return self.channel.services.links

    async def _current(self):
        while not self._retired:
            current = await self.channel.refresh_link()
            if current is None or current.token_revision != self.transport_revision or current.health == "token_rejected":
                self._retired = True
                return None
            # A sender may start verification during the awaited SQL guard.
            # With no checker this await returns without yielding, so dispatch
            # follows the last verification gate rather than an earlier CAS.
            verification = await self.channel.wait_token_verification()
            if verification is None:
                return current
            if verification.status in {"rejected", "retired"}:
                self._retired = True
                return None
            # A completed shared checker may have observed a replacement.
            # Recheck identity, then gate a newer in-flight cohort as well.
        return None

    def _event(self, name: str, *, error: Exception | None = None, classification: dict | None = None, level: int = logging.DEBUG, **facts) -> None:
        fields = {"link": self._link_id, "bot": self._bot_id}
        closed_description = None
        if error is not None or classification is not None:
            from app.telegram_bots.api_logging import call_failure_fields

            classified = dict(classification) if classification is not None else call_failure_fields(error)
            closed_description = classified.pop("desc", None)
            fields.update(classified)
        fields.update(facts)
        if closed_description is not None:
            fields["desc"] = closed_description
        logger.log(level, "telegram_bots %s %s", name, " ".join(f"{key}={value}" for key, value in fields.items() if value is not None))

    def _result(self, *, success: bool = False, conflict: bool = False) -> bool:
        self.record.polls += 1
        self.record.successes += int(success)
        self.record.conflicts += int(conflict)
        return bool(self.on_result()) if self.on_result is not None else False

    def _failure(self, error: Exception | None, *, retry_in: float | None = None, http: int | None = None, classification: dict | None = None) -> None:
        first = self._streak == 0
        self._streak += 1
        self._had_failure = True
        self._down_since = self._down_since or self._clock()
        self._event("poll_error", error=error, classification=classification, level=logging.WARNING if first else logging.DEBUG, **({"http": http} if http is not None else {}), streak=self._streak, retry_in=retry_in)

    async def _health(self, cause: str, holds: bool, *, transition: PollHealthTransition | None = None) -> None:
        # Preserve the same captured intent even when its first SQL guard
        # fails, before the sink has had a chance to attempt publication.
        self._pending_health = (cause, holds, transition)
        self._health_transition = transition
        try:
            if await self._current() is None:
                self._pending_health = None
                return
            await self.on_health(cause, holds)
            await self._current()
        except SQLAlchemyError:
            raise
        else:
            self._pending_health = None
        finally:
            self._health_transition = None

    async def _retry_health(self) -> str:
        cause, holds, transition = self._pending_health
        await self._health(cause, holds, transition=transition)
        if self._retired:
            return "invalid-token"
        if cause == "conflict":
            self._conflicted = holds
            self._conflict_startup = False
            self._probe_until = transition.probe_until if holds else None
            if holds:
                self._last_probe = transition.probe_until - timedelta(hours=CONFLICT_PROBE_HOURS)
                self._probe_expiry_reported = False
        return "conflict" if self._conflicted else "ok"

    async def _enter_conflict(self, *, extend: bool = False, current: BotLink | None = None) -> None:
        if current is None:
            current = await self._current()
        if current is None:
            return
        if self._conflicted and not extend:
            return
        now = self._clock()
        since = current.conflict_since or now
        transition = PollHealthTransition(self.transport_revision, since, now + timedelta(hours=CONFLICT_PROBE_HOURS), self._webhook_host)
        await self._health("conflict", True, transition=transition)
        if not self._retired:
            if not self._conflicted:
                self._last_probe = now
            self._conflicted, self._probe_until = True, transition.probe_until
            self._probe_expiry_reported = False

    async def _success(self) -> None:
        current = await self._current()
        if current is None:
            return
        now = self._clock()
        self.back_online_due = bool(self._had_failure and current.last_ok_at is not None and now - current.last_ok_at > timedelta(hours=OFFLINE_NOTICE_AFTER_HOURS))
        if self._conflicted:
            await self._health("conflict", False)
            self._conflicted, self._probe_until = False, None
        if self._had_failure:
            await self._health("unreachable", False)
        if not await self.channel.record_poll_success(recovered=self._had_failure):
            self._retired = True
            return
        if self._streak:
            self._event("poll_recovered", level=logging.INFO, streak=self._streak, down_s=int((now - self._down_since).total_seconds()))
            self._streak, self._down_since = 0, None
        self._first_conflict, self._webhook_checked = None, False
        self._network_since, self._had_failure = None, False
        self._backoff = BACKOFF_FIRST_SECONDS

    async def _network(self, error: Exception) -> None:
        self._had_failure = True
        now = self._clock()
        self._network_since = self._network_since or now
        updated = await self.links.update_if_current(self._link_id, token_revision=self.transport_revision, last_error=type(error).__name__, last_error_at=now)
        if updated is None:
            self._retired = True
            return
        if now - self._network_since >= timedelta(seconds=UNREACHABLE_AFTER_SECONDS):
            await self._health("unreachable", True)
        self._failure(error, retry_in=0 if isinstance(error, TimedOut) else self._backoff)

    async def _verify(self, error: InvalidToken) -> str:
        result = await self.channel.verify_token(error, from_get_me=False)
        if result.status in {"rejected", "retired"}:
            self._retired = True
            return "invalid-token"
        await self._network(error)
        return "network"

    async def _finish_conflict(self) -> str:
        from app.telegram_bots.adapter import LongRetryAfter

        observed, count, classification = self._fence_pending
        if not self._fence_committed:
            try:
                committed = await self.links.record_conflict(self._link_id, token_revision=self.transport_revision)
            except SQLAlchemyError as error:
                self._event("poll_error", error=error, level=logging.WARNING)
                return "network"
            if not committed:
                self._retired = True
                return "invalid-token"
            # Later SQL guards may fail after this committed write. Keep the
            # observation, but never advance its durable epoch a second time.
            self._fence_committed = True
        if await self._current() is None:
            self._retired = True
            return "invalid-token"
        self._had_failure = True
        self._first_conflict = self._first_conflict or observed
        rate_limited = False
        if not self._webhook_checked:
            self._webhook_checked = True
            try:
                info = await self.channel.bot.get_webhook_info()
                self._webhook = bool(info.url)
                try:
                    self._webhook_host = urlsplit(info.url).hostname if info.url else None
                except ValueError:
                    self._webhook_host = None
            except InvalidToken as error:
                return await self._verify(error)
            except (RetryAfter, LongRetryAfter) as error:
                self._defer_rate(error)
                self._event("poll_error", error=error, http=429, retry_in=self._retry_after)
                rate_limited = True
            except (NetworkError, Forbidden) as error:
                self._event("poll_error", error=error, level=logging.WARNING)
        current = await self._current()
        if current is None:
            return "invalid-token"
        self._fence_pending, self._fence_committed = None, False
        self.record.webhook |= self._webhook
        hold = self._result(conflict=True) if count else False
        if self._webhook or not hold and observed - self._first_conflict >= timedelta(seconds=CONFLICT_GRACE_SECONDS):
            # These facts were guarded before publishing the result. Capture
            # its immutable health intent before any further awaited guard.
            await self._enter_conflict(current=current)
        self._failure(None, classification=classification, retry_in=self._backoff)
        return "rate" if rate_limited else "conflict"

    def _defer_rate(self, error: Exception, *, extra: float = 0) -> None:
        seconds = error._retry_after.total_seconds() if isinstance(error, RetryAfter) else error.seconds
        until = self._clock() + timedelta(seconds=seconds + extra)
        self._rate_until = max(self._rate_until, until) if self._rate_until is not None else until
        self._retry_after = (self._rate_until - self._clock()).total_seconds()

    def _rate_pending(self) -> bool:
        if self._rate_until is None:
            return False
        self._retry_after = (self._rate_until - self._clock()).total_seconds()
        if self._retry_after > 0:
            return True
        self._rate_until = None
        return False

    async def _fetch_locked(self, *, mode: str = "poll") -> str:
        if await self._current() is None:
            return "invalid-token"
        if self._fence_pending is not None:
            return await self._finish_conflict()
        if self._pending_health:
            return await self._retry_health()
        if self._rate_pending():
            return "rate"
        parameters = {"timeout": 0 if mode in {"confirm", "drop"} else POLL_TIMEOUT_SECONDS, "allowed_updates": ALLOWED}
        if mode == "drop":
            parameters.update(offset=-1, limit=1)
        elif self.offset is not None:
            parameters["offset"] = self.offset
        count = mode in {"poll", "probe"}
        try:
            updates = await self.channel.bot.get_updates(**parameters)
        except Conflict as error:
            from app.telegram_bots.api_logging import call_failure_fields

            # Keep closed diagnostic facts across SQL retry, not the exception
            # and its traceback/request URL or arbitrary server description.
            self._fence_pending = (self._clock(), count, call_failure_fields(error))
            return await self._finish_conflict()
        except InvalidToken as error:
            return await self._verify(error)
        except RetryAfter as error:
            # A later SQL guard may fail. The known server deadline still
            # belongs to this captured transport and must survive its retry.
            self._defer_rate(error, extra=0.5)
            if await self._current() is None:
                return "invalid-token"
            if count:
                self._result()
            self._failure(error, http=429, retry_in=self._retry_after)
            return "rate"
        except TimedOut as error:
            if await self._current() is None:
                return "invalid-token"
            if count:
                self._result()
            await self._network(error)
            return "timeout"
        except (BadRequest, NetworkError, Forbidden) as error:
            if await self._current() is None:
                return "invalid-token"
            if count:
                self._result()
            await self._network(error)
            return "network"
        if await self._current() is None:
            return "invalid-token"
        if mode == "drop":
            self.offset = updates[-1].update_id + 1 if updates else None
            self._drop_required = False
            self._event("pending_dropped", level=logging.INFO, through=updates[-1].update_id if updates else "none")
            return "ok"
        if mode == "confirm":
            self._event("offset_confirmed", offset=self.offset)
            return "ok"
        await self._success()
        if self._retired or await self._current() is None:
            return "invalid-token"
        hold = self._result(success=True)
        if not self._has_success:
            self._event("poll_first_ok", level=logging.INFO, updates=len(updates))
            self._has_success = True
        if updates:
            if hold:
                self._held = list(updates[:UPDATES_HELD_PER_LINK])
            else:
                self._retain(updates)
        self._event("poll", updates=len(updates), offset=self.offset)
        return "ok"

    def _retain(self, updates: Sequence[Update]) -> None:
        self._pending = list(updates)
        self._batch_last = updates[-1].update_id if updates else None

    async def admit_batch(self, updates: Sequence[Update]) -> int:
        async with self._operation:
            if self._held or self._fence_pending or self._pending_health:
                return 0
            if self._pending and [row.update_id for row in updates] != [row.update_id for row in self._pending]:
                raise ValueError("pending-admission-batch")
            if not self._pending:
                self._retain(updates)
            accepted = 0
            while self._pending:
                if await self._current() is None:
                    return accepted
                try:
                    admitted = await self.channel.handle_update(self._pending[0])
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    self._event("poll_error", error=error, level=logging.WARNING)
                    break
                if admitted is not True:
                    break
                accepted += 1
                self._pending.pop(0)
            if not self._pending and self._batch_last is not None and await self._current() is not None:
                self.offset = self._batch_last + 1
                self._batch_last = None
            return accepted

    async def release_held(self) -> None:
        if self._held:
            self._retain(self._held)
            self._held = []
        self._wake.set()

    def held_count(self) -> int:
        return len(self._held)

    async def _wait(self, seconds: float | None = None) -> None:
        if self._wake.is_set():
            self._wake.clear()
            return
        if seconds is None:
            await self._wake.wait()
            self._wake.clear()
            return
        sleeper = asyncio.create_task(self._sleep(seconds))
        wake = asyncio.create_task(self._wake.wait())
        try:
            await asyncio.wait((sleeper, wake), return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (sleeper, wake):
                if not task.done():
                    task.cancel()
            await asyncio.gather(sleeper, wake, return_exceptions=True)
            self._wake.clear()

    def _expired(self) -> bool:
        expired = self._probe_until is not None and self._clock() >= self._probe_until
        if expired and not self._probe_expiry_reported:
            self._event("conflict_probing_ended", level=logging.WARNING, after_s=int((self._clock() - self._probe_until).total_seconds()) + CONFLICT_PROBE_HOURS * 3600)
            self._probe_expiry_reported = True
        return expired

    async def _probe_locked(self, *, extend: bool) -> str:
        result = await self._probe_attempt(extend=extend)
        if result != "expired":
            self._probes += 1
            self._event("conflict_probe", n=self._probes, result=result)
        return result

    async def _probe_attempt(self, *, extend: bool) -> str:
        from app.telegram_bots.adapter import LongRetryAfter

        if await self._current() is None:
            return "invalid-token"
        if self._fence_pending:
            return await self._finish_conflict()
        if self._pending_health:
            return await self._retry_health()
        if self._rate_pending():
            return "rate"
        if not extend and self._conflicted and self._expired():
            return "expired"
        if extend and self._conflicted:
            await self._enter_conflict(extend=True)
        self._last_probe = self._clock()
        if self._webhook:
            try:
                info = await self.channel.bot.get_webhook_info()
                if await self._current() is None:
                    return "invalid-token"
                if info.url:
                    return "webhook"
                self._webhook, self._webhook_host = False, None
            except InvalidToken as error:
                return await self._verify(error)
            except (RetryAfter, LongRetryAfter) as error:
                self._defer_rate(error)
                self._event("poll_error", error=error, http=429, retry_in=self._retry_after)
                return "rate"
            except (NetworkError, Forbidden) as error:
                self._event("poll_error", error=error)
                return "network"
        if not extend and self._conflicted and self._expired():
            return "expired"
        result = await self._fetch_locked(mode="probe")
        return "network" if result == "timeout" else result

    async def probe(self) -> str:
        async with self._operation:
            if self._pending or self._held or self._drop_required:
                return "network"
            try:
                result = await self._probe_locked(extend=True)
            except SQLAlchemyError as error:
                self._event("poll_error", error=error, level=logging.WARNING)
                result = "network"
        if self._pending:
            await self.admit_batch(tuple(self._pending))
        self._wake.set()
        return "network" if result == "rate" else result

    async def confirm_offset(self) -> None:
        async with self._operation:
            if self._pending or self._held or self._fence_pending or self._pending_health or self._drop_required or self.offset is None:
                return
            await self._fetch_locked(mode="confirm")

    async def drop_pending(self) -> None:
        async with self._operation:
            if self._pending or self._held or self._fence_pending or self._pending_health:
                raise ValueError("pending-admission-batch")
            self._drop_required = True
            await self._fetch_locked(mode="drop")

    async def run(self) -> None:
        # SQL faults can arise in guards, startup renewal, admission, or
        # control recovery as well as ordinary polling. Resume this same
        # owner after every operation scope has unwound, retaining its exact
        # pending intents. Product errors remain the supervisor's concern.
        while not self._retired:
            try:
                await self._run_owned()
                return
            except SQLAlchemyError as error:
                self._event("poll_error", error=error, level=logging.WARNING)
                await self._wait(self._backoff)
                self._backoff = min(BACKOFF_CAP_SECONDS, self._backoff * BACKOFF_FACTOR)

    async def _run_owned(self) -> None:
        from app.telegram_bots.adapter import LongRetryAfter

        while not self._retired:
            current = await self._current()
            if current is None or current.state == "held" or current.health in {"token_rejected", "policy_off", "secret_unreadable", "stopped"}:
                return
            if self._rate_pending():
                await self._wait(self._retry_after)
                continue
            if not self.channel.application.running:
                try:
                    await self.channel.start()
                except (RetryAfter, LongRetryAfter) as error:
                    self._defer_rate(error)
                    if await self._current() is None:
                        return
                    self._failure(error, http=429, retry_in=self._retry_after)
                    continue
                except (NetworkError, InvalidToken, Forbidden) as error:
                    current = await self._current()
                    if current is None or current.health == "token_rejected":
                        return
                    await self._network(error)
                    await self._wait(self._backoff)
                    self._backoff = min(BACKOFF_CAP_SECONDS, self._backoff * BACKOFF_FACTOR)
                    continue
            if self._fence_pending or self._drop_required:
                async with self._operation:
                    # Another owner may have completed the control request
                    # while this run waited for the shared operation lock.
                    if not self._fence_pending and not self._drop_required:
                        continue
                    result = await self._finish_conflict() if self._fence_pending else await self._fetch_locked(mode="drop")
                if self._fence_pending or self._drop_required:
                    if result == "rate":
                        await self._wait(self._retry_after)
                    else:
                        await self._wait(self._backoff)
                        self._backoff = min(BACKOFF_CAP_SECONDS, self._backoff * BACKOFF_FACTOR)
                continue
            if self._pending_health:
                try:
                    async with self._operation:
                        if self._pending_health:
                            await self._retry_health()
                except SQLAlchemyError as error:
                    self._event("poll_error", error=error, level=logging.WARNING)
                if self._pending_health:
                    await self._wait(self._backoff)
                    self._backoff = min(BACKOFF_CAP_SECONDS, self._backoff * BACKOFF_FACTOR)
                continue
            if self._held:
                await self._wait()
                continue
            if self._pending:
                await self.admit_batch(tuple(self._pending))
                if self._pending and not self._retired:
                    await self._wait(self._backoff)
                    self._backoff = min(BACKOFF_CAP_SECONDS, self._backoff * BACKOFF_FACTOR)
                continue
            if self._conflicted:
                if self._conflict_startup:
                    await self._enter_conflict(extend=True)
                    self._conflict_startup = False
                interval = self.channel.services.config.conflict_probe_seconds
                if interval == 0 or self._expired():
                    await self._wait()
                    continue
                if self._last_probe is not None and self._clock() - self._last_probe < timedelta(seconds=interval):
                    await self._wait(interval - (self._clock() - self._last_probe).total_seconds())
                    continue
            try:
                async with self._operation:
                    if self._pending or self._held:
                        continue
                    result = await self._probe_locked(extend=False) if self._conflicted else await self._fetch_locked()
            except SQLAlchemyError as error:
                self._event("poll_error", error=error, level=logging.WARNING)
                result = "network"
            if result in {"network", "conflict"} and not self._conflicted:
                await self._wait(self._backoff)
                self._backoff = min(BACKOFF_CAP_SECONDS, self._backoff * BACKOFF_FACTOR)
            elif result == "rate":
                await self._wait(self._retry_after)
