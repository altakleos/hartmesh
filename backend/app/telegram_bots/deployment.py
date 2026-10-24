"""Shared deployment checks and conditional copy safeguards; no service startup."""

from __future__ import annotations

import asyncio
import os
import selectors
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.gateway.auth_disabled import is_auth_disabled
from app.gateway.provider_keys.cipher import WrappingKeyInvalid
from app.telegram_bots.cipher import token_cipher
from app.telegram_bots.config import TelegramBotsConfig
from deerflow.config.app_config import AppConfig
from deerflow.persistence.telegram_bots.model import TelegramDeploymentRow
from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from app.telegram_bots.links import BotLink
    from app.telegram_bots.polling import PollRecord

_HOST_READ_SECONDS = 2.0
_HOST_ID_LIMIT = 128
_HOST_OUTPUT_LIMIT = _HOST_ID_LIMIT + 2
_LOCAL_DOCKER_HOST = "unix:///var/run/docker.sock"
_PROXY_ENV = frozenset({"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"})
PollingState = Literal["running", "host_changed", "polled_elsewhere"]

_REASONS = {
    "off": "off",
    "auth-disabled": "misconfigured",
    "unbound-allowed": "misconfigured",
    "secret-missing": "secret-missing",
    "secret-invalid": "secret-invalid",
    "host-changed": "host-changed",
    "polled-elsewhere": "polled-elsewhere",
    "channel-service-failed": "misconfigured",
    "gateway-not-ready": "misconfigured",
}


def static_cause(config: TelegramBotsConfig, app_config: AppConfig, environ: Mapping[str, str]) -> str | None:
    if not config.enabled:
        return "off"
    if is_auth_disabled():
        return "auth-disabled"
    connections = app_config.channel_connections
    if not connections.enabled or not connections.require_bound_identity:
        return "unbound-allowed"
    if not environ.get("HARTMESH_PROVIDER_KEYS_SECRET", "").strip():
        return "secret-missing"
    try:
        token_cipher(environ)
    except WrappingKeyInvalid:
        return "secret-invalid"
    return None


def feature_reason(cause: str | None) -> str | None:
    if cause is None:
        return None
    if cause not in _REASONS:
        raise ValueError("invalid-deployment-cause")
    return _REASONS[cause]


def _host_id(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value or len(value) > _HOST_ID_LIMIT or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in value):
        return None
    return value


def host_identity() -> str | None:
    """Read only the local Docker engine ID, with a bounded owned subprocess.

    Call this synchronous discovery off the Gateway loop. The two-second
    observation budget is shared by reading output and waiting for exit; an
    overrun or oversized reply is killed and reaped before releasing ownership.
    """
    if os.name != "posix":
        return None
    deadline = time.monotonic() + _HOST_READ_SECONDS
    process = None
    try:
        environment = {key: value for key, value in os.environ.items() if not key.upper().startswith("DOCKER_") and key.upper() not in _PROXY_ENV}
        process = subprocess.Popen(
            ["docker", "--host", _LOCAL_DOCKER_HOST, "info", "--format", "{{.ID}}"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
            bufsize=0,
        )
        output = bytearray()
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    return None
                chunk = os.read(process.stdout.fileno(), _HOST_OUTPUT_LIMIT + 1 - len(output))
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > _HOST_OUTPUT_LIMIT:
                    return None
        remaining = deadline - time.monotonic()
        if remaining <= 0 or process.wait(timeout=remaining) != 0:
            return None
        return _host_id(output.decode("ascii"))
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
    finally:
        if process is not None:
            try:
                if process.poll() is None:
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                process.wait()
            finally:
                if process.stdout is not None:
                    process.stdout.close()


class GuardRefused(Exception):
    def __init__(self, reason: str) -> None:
        if reason not in {"host-unreadable", "polled-elsewhere", "host-changed"}:
            raise ValueError("invalid-guard-refusal")
        super().__init__(reason)
        self.reason = reason


def _now() -> datetime:
    return datetime.now(UTC)


class CopyGuard:
    """Observe known hosts and a conditional conflict pattern, never elect one.

    A successful poll is no ownership proof. Concurrent copies must be disabled
    before their first boot, including copies retaining the same engine ID.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], *, host_reader: Callable[[], str | None] | None = None, clock: Callable[[], datetime] = _now) -> None:
        self.sessions = session_factory
        self._host_reader, self._clock = host_reader if host_reader is not None else host_identity, clock
        self._this_host_id: str | None = None

    @property
    def this_host_id(self) -> str | None:
        """The host observation made by peek/start/adopt; this starts no work."""
        return self._this_host_id

    async def _observe_host(self) -> str | None:
        # Cancellation cannot abandon the synchronous reader's child process.
        observed = await await_drained(asyncio.to_thread(self._host_reader))
        self._this_host_id = _host_id(observed)
        return self._this_host_id

    @staticmethod
    def _state(row: TelegramDeploymentRow | Row | None, host: str | None) -> PollingState:
        if row is None:
            return "running"
        if row.host_id is not None and host is not None and row.host_id != host:
            return "host_changed"
        return row.polling_state

    def _borrow_read(self, session: AsyncSession) -> None:
        bound = self.sessions.kw.get("bind")
        if not session.in_transaction() or not session.is_active or bound is None or session.get_bind() is not getattr(bound, "sync_engine", bound):
            raise ValueError("borrowed-guard-read-requires-current-transaction")

    async def peek(self, *, session: AsyncSession | None = None) -> PollingState:
        if session is not None:
            self._borrow_read(session)
        host = await self._observe_host()
        if session is not None:
            return await self._peek(session, host)
        async with self.sessions() as reader:
            return await self._peek(reader, host)

    async def _peek(self, session: AsyncSession, host: str | None) -> PollingState:
        # A borrowed read must neither flush the caller's pending mutations nor
        # replace its ORM identity-map values while reading persisted facts.
        with session.no_autoflush:
            row = (await session.execute(select(TelegramDeploymentRow.host_id, TelegramDeploymentRow.polling_state).where(TelegramDeploymentRow.id == 1))).one_or_none()
            return self._state(row, host)

    @asynccontextmanager
    async def _writer(self, *, ensure: bool):
        async with self.sessions() as session, session.begin():
            dialect = session.get_bind().dialect.name
            if dialect == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            if ensure and dialect == "postgresql":
                await session.execute(postgres_insert(TelegramDeploymentRow).values(id=1, host_id=None, polling_state="running", adopted_at=None, updated_at=self._clock()).on_conflict_do_nothing(index_elements=["id"]))
            row = await session.scalar(select(TelegramDeploymentRow).where(TelegramDeploymentRow.id == 1).with_for_update().execution_options(populate_existing=True))
            if row is None and ensure:
                row = TelegramDeploymentRow(id=1, host_id=None, polling_state="running", adopted_at=None, updated_at=self._clock())
                session.add(row)
                await session.flush()
            yield row

    async def at_start(self) -> PollingState:
        host = await self._observe_host()
        async with self._writer(ensure=True) as row:
            state = self._state(row, host)
            if row.host_id is None and host is not None:
                row.host_id, row.updated_at = host, self._clock()
            if state != row.polling_state:
                row.polling_state, row.updated_at = state, self._clock()
            return state

    @staticmethod
    def _links(links: Sequence[BotLink]) -> list[BotLink]:
        # Membership denotes links, not repeated references to the same row.
        return list({link.id: link for link in links}.values())

    def _broad_observations(self, polled: Sequence[BotLink], results: Mapping[str, PollRecord]) -> tuple[list[BotLink], list[BotLink]]:
        observed = [link for link in self._links(polled) if link.id in results and results[link.id].polls > 0]
        candidates = [link for link in observed if link.health != "conflict" and not results[link.id].webhook and results[link.id].successes == 0 and results[link.id].conflicts == results[link.id].polls]
        return observed, candidates

    def broad_conflict(self, polled: Sequence[BotLink], results: Mapping[str, PollRecord]) -> bool:
        observed, candidates = self._broad_observations(polled, results)
        return len(candidates) >= 2 and len(candidates) > len(observed) / 2 and (len({link.owner_user_id for link in candidates}) >= 2 or len({link.owner_user_id for link in observed}) == 1)

    def broad_conflict_counts(self, polled: Sequence[BotLink], results: Mapping[str, PollRecord]) -> dict[str, int]:
        """Safe log facts from the same observation used by the predicate."""
        observed, candidates = self._broad_observations(polled, results)
        return {"polled": len(observed), "counted": len(candidates), "people": len({link.owner_user_id for link in candidates})}

    def can_still_be_broad_conflict(self, started: Sequence[BotLink], results: Mapping[str, PollRecord]) -> bool:
        candidates, floor = 0, 0
        for link in self._links(started):
            record = results.get(link.id)
            candidate = link.health != "conflict" and (record is None or not record.webhook and record.successes == 0)
            if candidate:
                candidates += 1
            elif record is not None and record.polls > 0:
                floor += 1
        return candidates >= 2 and candidates > floor

    async def stop_for_broad_conflict(self) -> None:
        async with self._writer(ensure=True) as row:
            if row.polling_state != "polled_elsewhere":
                row.polling_state, row.updated_at = "polled_elsewhere", self._clock()

    async def adopt(self) -> dict:
        host = await self._observe_host()
        if host is None:
            raise GuardRefused("host-unreadable")
        async with self._writer(ensure=True) as row:
            before = row.host_id
            if before == host and row.polling_state == "polled_elsewhere":
                raise GuardRefused("polled-elsewhere")
            if before == host and row.polling_state == "running":
                return {"from": before, "host_id": host, "verdict": "already_adopted"}
            row.host_id, row.polling_state, row.adopted_at, row.updated_at = host, "running", self._clock(), self._clock()
            return {"from": before, "host_id": host, "verdict": "adopted"}

    async def resume(self) -> dict:
        # This verb is an operator statement about the persisted latch only.
        # peek/at_start still independently refuse a known unadopted host.
        async with self._writer(ensure=False) as row:
            if row is None or row.polling_state == "running":
                return {"verdict": "already_running"}
            if row.polling_state == "host_changed":
                raise GuardRefused("host-changed")
            row.polling_state, row.updated_at = "running", self._clock()
            return {"verdict": "resumed"}
