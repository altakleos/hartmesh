"""Durable bot ownership, capacity and binding transactions.

Capacity changes serialize deployment -> owner -> sorted links. Connection-only
writers start at the owner and never acquire deployment capacity afterward.
Borrowed helpers only use their caller's session and leave its commit to it.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import secrets
import sqlite3
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal, Protocol
from uuid import uuid4

from sqlalchemy import and_, delete, func, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.settings import SettingsRepository
from app.telegram_bots.token_pattern import TOKEN_BODY
from deerflow.persistence.channel_connections.model import ChannelConnectionRow, ChannelConversationRow, ChannelCredentialRow, ChannelOAuthStateRow
from deerflow.persistence.channel_connections.sql import ChannelConnectionRepository
from deerflow.persistence.telegram_bots.model import (
    TelegramAccountRow,
    TelegramAssistantAddRow,
    TelegramBotIntentRow,
    TelegramBotLinkEventRow,
    TelegramBotLinkRow,
    TelegramCallbackRow,
    TelegramDeploymentRow,
    TelegramInboundMarkRow,
    TelegramOutboxRow,
    TelegramPendingFileRow,
    TelegramPersonSettingsRow,
    TelegramWorkTopicRow,
)

if TYPE_CHECKING:
    from app.telegram_bots.authority import LiveOwnerCheck
    from app.telegram_bots.inbox import InboxRow

CONNECT_CODE_SECONDS = 600
CONNECT_CODES_MAX = 5
_EPOCH_MAX = (1 << 63) - 1
_REVISION_MAX = (1 << 31) - 1
_WRITER = "telegram_bot_links_writer"
_TOKEN = re.compile(TOKEN_BODY)
_HEX_ID = re.compile(r"[0-9a-f]{32}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_USERNAME = re.compile(r"[A-Za-z0-9_]{1,64}\Z")
_HEALTH_ORDER = ("secret_unreadable", "token_rejected", "policy_off", "conflict", "stopped", "agent_unavailable", "unreachable")
_OPEN_ADDS = ("draft", "confirmed", "definition_created", "created", "bot_pending")
_NOTICE_KEYS = {
    "link.conflict": ("conflict", None),
    "link.conflict_over": ("conflict_over", None),
    "link.back_online": ("back_online", None),
    "link.token_rejected_other": ("token_rejected_other", None),
    "access.off": ("access_off", None),
    "access.back": ("access_back", None),
    "policy.off": ("policy", "off"),
    "policy.on": ("policy", "allowed"),
    "policy.on_protected": ("policy", "protected"),
    "policy.protected": ("policy", "protected"),
    "policy.unprotected": ("policy", "allowed"),
    "link.removed": ("removed", None),
    "agent.deleted": ("agent_deleted", None),
}
_EVENT_ACTIONS = {
    "add",
    "created_managed",
    "replace_token",
    "replace_bot",
    "managed_token",
    "remove",
    "connect",
    "disconnect",
    "disconnect_all",
    "hold",
    "release",
    "blocked",
    "unblocked",
    "token_rejected",
    "conflict",
    "conflict_ended",
    "conflict_acknowledged",
    "stopped",
    "retry",
    "rewrap",
    "agent_removed",
    "managed_given_away",
    "policy_off",
    "policy_on",
    "token_replace_receipt",
}
_DETAIL_KEYS = {"by", "agent_kind", "agent_ref", "agent_name", "bot_username", "operation_id", "old_link_id", "new_link_id", "token_revision", "managed_replaced", "takeover", "owed", "done", "attempted", "state"}
_IMMUTABLE_LINK_FIELDS = {
    "id",
    "owner_user_id",
    "agent_kind",
    "agent_ref",
    "bot_id",
    "token_revision",
    "connect_conflict_epoch",
    "health",
    "created_at",
    "updated_at",
    "binding_generation",
    "work_baseline_generation",
    "connection_id",
    "telegram_user_id",
    "chat_id",
    "connected_name",
    "connect_expires_at",
    "origin",
    "manager_link_id",
    "creator_telegram_user_id",
    "bind_mode",
}


def _now() -> datetime:
    return datetime.now(UTC)


def _time(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _advance(value: int, *, epoch: bool = False) -> int:
    if value >= (_EPOCH_MAX if epoch else _REVISION_MAX):
        raise RuntimeError("conflict-epoch-exhausted" if epoch else "revision-exhausted")
    return value + 1


def _check_epoch(value: int) -> None:
    if value < 0 or value >= _EPOCH_MAX:
        raise RuntimeError("conflict-epoch-exhausted")


def _insert(session: AsyncSession):
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        return postgres_insert
    if dialect == "sqlite":
        return sqlite_insert
    raise ValueError("unsupported Telegram database")


@dataclass(frozen=True)
class BotLink:
    id: str
    owner_user_id: str
    agent_kind: str
    agent_ref: str
    bot_id: int
    bot_username: str
    bot_name: str | None
    token_revision: int
    connect_conflict_epoch: int
    origin: str
    manager_link_id: str | None
    creator_telegram_user_id: int | None
    can_manage_bots: bool
    bind_mode: str
    connect_expires_at: datetime | None
    connection_id: str | None
    binding_generation: int
    work_baseline_generation: int | None
    telegram_user_id: int | None
    chat_id: int | None
    connected_name: str | None
    state: str
    health: str
    conflict_since: datetime | None
    probe_until: datetime | None
    acts_paused_since: datetime | None
    limited_since: datetime | None
    webhook_host: str | None
    topics_enabled: bool
    profile_pending: bool
    profile_version: int
    profile_synced_at: datetime | None
    notices: dict
    last_error: str | None
    last_error_at: datetime | None
    last_ok_at: datetime | None
    last_update_at: datetime | None
    connected_at: datetime | None
    created_at: datetime
    updated_at: datetime

    @staticmethod
    def from_row(row: TelegramBotLinkRow) -> BotLink:
        """Capture detached public link facts without exposing its credential."""
        return _snapshot(row)


class PublicationGuard(Protocol):
    """Current attempt authority borrowed by one publication writer."""

    @property
    def captured_link(self) -> BotLink: ...

    async def check(self, writer: AsyncSession) -> BotLink: ...


@dataclass(frozen=True)
class _PublicationGuard:
    repository: BotLinkRepository
    _captured: BotLink
    _live_owner: LiveOwnerCheck
    _attempt: Callable[[], bool]

    @property
    def captured_link(self) -> BotLink:
        return deepcopy(self._captured)

    async def check(self, writer: AsyncSession) -> BotLink:
        from deerflow.utils.file_io import await_drained

        return await await_drained(self._check(writer))

    async def _check(self, writer: AsyncSession) -> BotLink:
        captured = self._captured
        bind = self.repository.session_factory.kw.get("bind")
        if bind is None or not writer.in_transaction() or not writer.is_active or writer.get_bind() is not getattr(bind, "sync_engine", bind):
            raise ValueError("publication-writer-required") from None
        if writer.get_bind().dialect.name == "sqlite":
            # A caller's session.begin() need not have started a native writer.
            # Reserve before the first authority read, without another checkout.
            await writer.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == captured.id).values(id=TelegramBotLinkRow.id))
        row = await writer.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == captured.id, TelegramBotLinkRow.owner_user_id == captured.owner_user_id).with_for_update().execution_options(populate_existing=True))
        basis = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
        if (
            row is None
            or captured.state != "connected"
            or row.state != "connected"
            or row.health != "ok"
            or captured.agent_kind not in {"lead_agent", "custom_agent"}
            or captured.agent_kind == "lead_agent"
            and captured.agent_ref != "lead_agent"
            or not isinstance(captured.agent_ref, str)
            or not captured.agent_ref
            or any(type(value) is not int or value < 0 for value in (captured.binding_generation, captured.token_revision))
            or type(captured.telegram_user_id) is not int
            or captured.telegram_user_id <= 0
            or type(captured.chat_id) is not int
            or captured.chat_id != captured.telegram_user_id
            or captured.connection_id is None
            or any(getattr(row, name) != getattr(captured, name) for name in basis)
        ):
            raise ValueError("publication-stale") from None
        account = await writer.get(TelegramAccountRow, captured.telegram_user_id, with_for_update=True, populate_existing=True)
        connection = await self.repository._current_bound_connection(writer, row)
        if account is None or account.owner_user_id != captured.owner_user_id or connection is None or connection.status != "connected":
            raise ValueError("publication-stale") from None
        detached = BotLink.from_row(row)
        await self._live_owner(captured.owner_user_id, writer)
        # No authority/SQL await may follow this original-attempt check.
        try:
            current = self._attempt()
        except Exception:
            raise ValueError("publication-stale") from None
        if current is not True:
            raise ValueError("publication-stale") from None
        return detached


@dataclass(frozen=True)
class Connect:
    url: str
    expires_at: datetime


@dataclass(frozen=True)
class ThreadPreparation:
    """Saved logical intent; a token revision belongs to each live attempt only."""

    origin: InboxRow
    purpose: str
    creation_id: str
    endpoint_id: str
    scope: dict
    expected_mapping_id: str | None
    expected_thread_id: str | None
    selected_thread_id: str | None
    mapping_id: str | None


class _ThreadPreparationStale(ValueError):
    def __init__(self) -> None:
        super().__init__("thread-preparation-stale")


def _thread_scope(link: BotLink | TelegramBotLinkRow) -> dict:
    return {"link_id": link.id, **{key: getattr(link, key) for key in ("owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation")}}


def _thread_payload(preparation: ThreadPreparation) -> dict:
    return {field.name: deepcopy(getattr(preparation, field.name)) for field in fields(ThreadPreparation) if field.name != "origin"}


def _thread_equal(first: dict, second: dict) -> bool:
    try:
        return json.dumps(first, sort_keys=True, allow_nan=False) == json.dumps(second, sort_keys=True, allow_nan=False)
    except (TypeError, ValueError):
        return False


def _valid_thread_id(value) -> bool:
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", value) is not None and _TOKEN.search(value) is None


class BotLinked(Exception):
    def __init__(self, *, own: bool, agent_name: str | None = None) -> None:
        super().__init__("bot-linked")
        self.own, self.agent_name = own, agent_name


class AgentLinked(Exception):
    def __init__(self, *, bot_id: int) -> None:
        super().__init__("agent-linked")
        self.bot_id = bot_id


class LimitPerson(Exception):
    def __init__(self) -> None:
        super().__init__("limit-person")


class LimitDeployment(Exception):
    def __init__(self) -> None:
        super().__init__("limit-deployment")


class TooManyCodes(Exception):
    def __init__(self) -> None:
        super().__init__("too-many-codes")


def _snapshot(row: TelegramBotLinkRow) -> BotLink:
    result = {}
    for field in fields(BotLink):
        value = getattr(row, field.name)
        result[field.name] = _time(value) if isinstance(value, datetime) else deepcopy(value)
    return BotLink(**result)


def _safe_detail(detail: dict | None) -> dict:
    if detail is None:
        return {}
    if not isinstance(detail, dict) or set(detail) - _DETAIL_KEYS:
        raise ValueError("unsafe-event-detail")

    def safe(value, depth=0):
        if depth > 4:
            return False
        if value is None or type(value) in (bool, int):
            return True
        if isinstance(value, str):
            return len(value) <= 1024 and _TOKEN.search(value) is None
        return isinstance(value, dict) and not (set(value) - _DETAIL_KEYS) and all(safe(v, depth + 1) for v in value.values())

    if not all(safe(value) for value in detail.values()):
        raise ValueError("unsafe-event-detail")
    return deepcopy(detail)


def _active_identity_collision(error: IntegrityError) -> bool:
    """Recognize only the competing active channel identity constraint."""
    current, seen = error.orig, set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if getattr(current, "constraint_name", None) == "uq_channel_connection_active_identity":
            return True
        if getattr(getattr(current, "diag", None), "constraint_name", None) == "uq_channel_connection_active_identity":
            return True
        if getattr(current, "sqlite_errorcode", None) == sqlite3.SQLITE_CONSTRAINT_UNIQUE:
            expected = "UNIQUE constraint failed: channel_connections.provider, channel_connections.external_account_id, channel_connections.workspace_id"
            if str(current) == expected:
                return True
        current = current.__cause__
    return False


class BotLinkRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession], connection_repo: ChannelConnectionRepository, config: TelegramBotsConfig) -> None:
        self.session_factory, self.connection_repo, self.config = session_factory, connection_repo, config
        self.settings = SettingsRepository(session_factory, config)
        # Causes are deliberately process-local; startup repopulates them. Only
        # install a changed set after its corresponding SQL commit succeeded.
        self._health_causes: dict[str, set[str]] = {}
        self._health_revisions: dict[str, int] = {}
        self._health_locks: dict[str, asyncio.Lock] = {}

    def publication_guard(self, captured_link: BotLink, *, check_live_owner: LiveOwnerCheck, check_attempt_current: Callable[[], bool]) -> PublicationGuard:
        """Capture the original attempt without acquiring an owner-wide writer."""
        if not isinstance(captured_link, BotLink) or not callable(check_live_owner) or not callable(check_attempt_current):
            raise ValueError("publication-stale") from None
        return _PublicationGuard(self, deepcopy(captured_link), check_live_owner, check_attempt_current)

    def _health_lock(self, link_id: str) -> asyncio.Lock:
        return self._health_locks.setdefault(link_id, asyncio.Lock())

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session, session.begin():
            if session.get_bind().dialect.name == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            yield session

    async def _counts(self, session: AsyncSession, owner: str) -> tuple[int, int]:
        total = await session.scalar(select(func.count()).select_from(TelegramBotLinkRow))
        own = await session.scalar(select(func.count()).select_from(TelegramBotLinkRow).where(TelegramBotLinkRow.owner_user_id == owner))
        return int(total), int(own)

    def _capacity(self, prospective: tuple[int, int], baseline: tuple[int, int]) -> None:
        if prospective[0] > self.config.max_bots and prospective[0] > baseline[0]:
            raise LimitDeployment()
        if prospective[1] > self.config.max_bots_per_person and prospective[1] > baseline[1]:
            raise LimitPerson()

    async def _owner_lock(self, session: AsyncSession, owner: str, *, timezone: str | None = None, locale: str | None = None) -> None:
        existing = await session.scalar(select(TelegramPersonSettingsRow).where(TelegramPersonSettingsRow.owner_user_id == owner).with_for_update().execution_options(populate_existing=True))
        if existing is None:
            count = await session.scalar(select(func.count()).select_from(TelegramBotLinkRow).where(TelegramBotLinkRow.owner_user_id == owner))
            if count:
                raise RuntimeError("Telegram owner settings are missing")
            if timezone is not None or locale is not None:
                if timezone is None:
                    raise ValueError("timezone-required")
                if locale is None:
                    raise ValueError("invalid-locale")
                await self.settings.ensure(owner, timezone=timezone, locale=locale, session=session)
                await session.scalar(select(TelegramPersonSettingsRow).where(TelegramPersonSettingsRow.owner_user_id == owner).with_for_update().execution_options(populate_existing=True))
        await session.execute(select(TelegramBotLinkRow).where(TelegramBotLinkRow.owner_user_id == owner).order_by(TelegramBotLinkRow.id).with_for_update().execution_options(populate_existing=True))

    @asynccontextmanager
    async def capacity_writer(self, owner_user_id: str, *, timezone: str | None = None, locale: str | None = None) -> AsyncIterator[AsyncSession]:
        async with self._transaction() as session:
            if session.get_bind().dialect.name == "postgresql":
                await session.execute(_insert(session)(TelegramDeploymentRow).values(id=1, host_id=None, polling_state="running", adopted_at=None, updated_at=_now()).on_conflict_do_nothing(index_elements=["id"]))
                await session.scalar(select(TelegramDeploymentRow).where(TelegramDeploymentRow.id == 1).with_for_update())
            else:
                await session.execute(_insert(session)(TelegramDeploymentRow).values(id=1, host_id=None, polling_state="running", adopted_at=None, updated_at=_now()).on_conflict_do_nothing(index_elements=["id"]))
            await self._owner_lock(session, owner_user_id, timezone=timezone, locale=locale)
            baseline = await self._counts(session, owner_user_id)
            session.info[_WRITER] = (self, owner_user_id, baseline)
            try:
                yield session
                await session.flush()
                self._capacity(await self._counts(session, owner_user_id), baseline)
            finally:
                session.info.pop(_WRITER, None)

    @asynccontextmanager
    async def _owner_writer(self, owner: str) -> AsyncIterator[AsyncSession]:
        async with self._transaction() as session:
            await self._owner_lock(session, owner)
            session.info[_WRITER] = (self, owner, None)
            try:
                yield session
            finally:
                session.info.pop(_WRITER, None)

    def _borrow(self, session: AsyncSession, owner: str, *, capacity: bool = False) -> tuple[int, int] | None:
        scope = session.info.get(_WRITER)
        if scope is None or scope[0] is not self or scope[1] != owner or not session.in_transaction() or (capacity and scope[2] is None):
            raise ValueError("Telegram helper requires its owning writer session")
        return scope[2]

    async def _locked(self, session: AsyncSession, identity: str, *, captured: BotLink | None = None) -> TelegramBotLinkRow:
        row = await session.get(TelegramBotLinkRow, identity, populate_existing=True)
        if row is None:
            raise ValueError("link-not-found")
        self._borrow(session, row.owner_user_id)
        if captured is not None and (row.owner_user_id != captured.owner_user_id or row.bot_id != captured.bot_id or row.binding_generation != captured.binding_generation):
            raise ValueError("link-changed")
        return row

    async def _owner_of(self, identity: str) -> str:
        async with self.session_factory() as session:
            owner = await session.scalar(select(TelegramBotLinkRow.owner_user_id).where(TelegramBotLinkRow.id == identity))
            if owner is None:
                raise ValueError("link-not-found")
            return owner

    async def get(self, link_id: str) -> BotLink | None:
        async with self.session_factory() as session:
            row = await session.get(TelegramBotLinkRow, link_id)
            return _snapshot(row) if row is not None else None

    async def get_by_bot(self, bot_id: int) -> BotLink | None:
        async with self.session_factory() as session:
            row = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.bot_id == bot_id))
            return _snapshot(row) if row is not None else None

    async def _list(self, *where) -> list[BotLink]:
        async with self.session_factory() as session:
            result = await session.scalars(select(TelegramBotLinkRow).where(*where).order_by(TelegramBotLinkRow.created_at, TelegramBotLinkRow.id))
            return [_snapshot(row) for row in result]

    async def list_for_owner(self, owner_user_id: str) -> list[BotLink]:
        return await self._list(TelegramBotLinkRow.owner_user_id == owner_user_id)

    async def list_for_agent(self, agent_kind: str, agent_ref: str) -> list[BotLink]:
        return await self._list(TelegramBotLinkRow.agent_kind == agent_kind, TelegramBotLinkRow.agent_ref == agent_ref)

    async def list_all(self) -> list[BotLink]:
        return await self._list()

    async def token(self, link_id: str) -> tuple[str, int]:
        async with self.session_factory() as session:
            result = (await session.execute(select(TelegramBotLinkRow.encrypted_token, TelegramBotLinkRow.token_revision).where(TelegramBotLinkRow.id == link_id))).one_or_none()
            if result is None:
                raise ValueError("link-not-found")
            return result[0], result[1]

    async def _event(self, session: AsyncSession, link: BotLink | TelegramBotLinkRow, action: str, *, actor_id: str, detail: dict | None = None, operation_id: str | None = None, request_digest: str | None = None) -> None:
        self._borrow(session, link.owner_user_id)
        if action not in _EVENT_ACTIONS or not isinstance(actor_id, str) or not 1 <= len(actor_id) <= 64 or _TOKEN.search(actor_id):
            raise ValueError("invalid-link-event")
        session.add(
            TelegramBotLinkEventRow(
                id=uuid4().hex,
                link_id=link.id,
                owner_user_id=link.owner_user_id,
                bot_id=link.bot_id,
                actor_id=actor_id,
                action=action,
                detail=_safe_detail(detail),
                operation_id=operation_id,
                request_digest=request_digest,
                created_at=_now(),
            )
        )
        await session.flush()

    async def event(self, link: BotLink, action: str, *, actor_id: str, detail: dict | None = None, session: AsyncSession | None = None) -> None:
        if session is None:
            async with self._owner_writer(link.owner_user_id) as writer:
                await self.event(link, action, actor_id=actor_id, detail=detail, session=writer)
                return
        await self._event(session, link, action, actor_id=actor_id, detail=detail)

    async def add(
        self,
        *,
        owner_user_id: str,
        agent_kind: str,
        agent_ref: str,
        bot_id: int,
        bot_username: str,
        encrypted_token: str,
        origin: str = "pasted",
        manager_link_id: str | None = None,
        creator_telegram_user_id: int | None = None,
        bind_mode: str = "code",
        actor_id: str,
        session: AsyncSession | None = None,
    ) -> BotLink:
        if session is None:
            async with self.capacity_writer(owner_user_id) as writer:
                return await self.add(
                    owner_user_id=owner_user_id,
                    agent_kind=agent_kind,
                    agent_ref=agent_ref,
                    bot_id=bot_id,
                    bot_username=bot_username,
                    encrypted_token=encrypted_token,
                    origin=origin,
                    manager_link_id=manager_link_id,
                    creator_telegram_user_id=creator_telegram_user_id,
                    bind_mode=bind_mode,
                    actor_id=actor_id,
                    session=writer,
                )
        baseline = self._borrow(session, owner_user_id, capacity=True)
        total, own = await self._counts(session, owner_user_id)
        self._capacity((total + 1, own + 1), baseline)
        duplicate = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.bot_id == bot_id))
        if duplicate is not None:
            raise BotLinked(own=duplicate.owner_user_id == owner_user_id)
        duplicate = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.owner_user_id == owner_user_id, TelegramBotLinkRow.agent_kind == agent_kind, TelegramBotLinkRow.agent_ref == agent_ref))
        if duplicate is not None:
            raise AgentLinked(bot_id=duplicate.bot_id)
        if await session.get(TelegramPersonSettingsRow, owner_user_id) is None:
            raise ValueError("timezone-required")
        if (
            agent_kind not in {"lead_agent", "custom_agent", "instance"}
            or not isinstance(agent_ref, str)
            or not 1 <= len(agent_ref) <= 128
            or type(bot_id) is not int
            or not 0 < bot_id <= _EPOCH_MAX
            or not isinstance(bot_username, str)
            or not _USERNAME.fullmatch(bot_username)
            or origin not in {"pasted", "managed"}
            or bind_mode not in {"code", "creator"}
        ):
            raise ValueError("invalid-bot-link")
        if not isinstance(encrypted_token, str) or not encrypted_token or _TOKEN.search(encrypted_token):
            raise ValueError("invalid-encrypted-token")
        if manager_link_id is not None:
            manager = await session.get(TelegramBotLinkRow, manager_link_id, populate_existing=True)
            if manager is None or manager.owner_user_id != owner_user_id:
                raise ValueError("manager-unavailable")
        now, identity = _now(), uuid4().hex
        statement = (
            _insert(session)(TelegramBotLinkRow)
            .values(
                id=identity,
                owner_user_id=owner_user_id,
                agent_kind=agent_kind,
                agent_ref=agent_ref,
                bot_id=bot_id,
                bot_username=bot_username,
                encrypted_token=encrypted_token,
                origin=origin,
                manager_link_id=manager_link_id,
                creator_telegram_user_id=creator_telegram_user_id,
                bind_mode=bind_mode,
                state="waiting_connect",
                health="ok",
                created_at=now,
                updated_at=now,
            )
            .on_conflict_do_nothing()
            .returning(TelegramBotLinkRow.id)
        )
        inserted = await session.scalar(statement)
        if inserted is None:
            duplicate = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.bot_id == bot_id))
            if duplicate is not None:
                raise BotLinked(own=duplicate.owner_user_id == owner_user_id)
            duplicate = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.owner_user_id == owner_user_id, TelegramBotLinkRow.agent_kind == agent_kind, TelegramBotLinkRow.agent_ref == agent_ref))
            if duplicate is not None:
                raise AgentLinked(bot_id=duplicate.bot_id)
            raise RuntimeError("bot link admission did not produce a row")
        row = await self._locked(session, identity)
        await self._event(session, row, "created_managed" if origin == "managed" else "add", actor_id=actor_id)
        return _snapshot(row)

    async def update(self, link_id: str, **columns) -> BotLink:
        allowed = {field.name for field in fields(BotLink)} - _IMMUTABLE_LINK_FIELDS
        if set(columns) - allowed:
            raise ValueError("unknown or immutable link field")
        if (
            "last_error" in columns
            and columns["last_error"] is not None
            and (not isinstance(columns["last_error"], str) or len(columns["last_error"]) > 500 or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*(?: [a-z][a-z0-9_-]*)?", columns["last_error"]))
        ):
            raise ValueError("invalid-error-classification")
        async with self._owner_writer(await self._owner_of(link_id)) as session:
            row = await self._locked(session, link_id)
            for key, value in columns.items():
                if isinstance(value, datetime):
                    if value.tzinfo is None:
                        raise ValueError("a timestamp must include a timezone")
                    value = value.astimezone(UTC)
                setattr(row, key, deepcopy(value))
            row.updated_at = _now()
            await session.flush()
            return _snapshot(row)

    async def update_if_current(self, link_id: str, *, token_revision: int, **facts) -> BotLink | None:
        """Publish only captured transport status, or guard a result without a write."""
        if type(token_revision) is not int or token_revision < 0:
            raise ValueError("invalid-token-revision")
        dates = {"last_ok_at", "last_update_at", "last_error_at"}
        flags = {"can_manage_bots", "topics_enabled"}
        if set(facts) - dates - flags - {"last_error"}:
            raise ValueError("invalid-transport-facts")
        facts = dict(facts)
        for key, value in facts.items():
            if key in flags and type(value) is not bool:
                raise ValueError("invalid-transport-facts")
            if key in dates and value is not None:
                if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
                    raise ValueError("invalid-transport-facts")
                facts[key] = value.astimezone(UTC)
            if key == "last_error" and value is not None and (not isinstance(value, str) or len(value) > 500 or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*(?: [a-z][a-z0-9_-]*)?", value)):
                raise ValueError("invalid-error-classification")
        try:
            owner = await self._owner_of(link_id)
        except ValueError:
            return None
        async with self._owner_writer(owner) as session:
            row = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link_id, TelegramBotLinkRow.owner_user_id == owner).with_for_update())
            if row is None or row.token_revision != token_revision:
                return None
            for key, value in facts.items():
                setattr(row, key, value)
            if facts:
                row.updated_at = _now()
                await session.flush()
            return _snapshot(row)

    async def _end_codes(self, session: AsyncSession, link_id: str, owner: str) -> None:
        self._borrow(session, owner)
        codes = await session.scalars(select(ChannelOAuthStateRow).where(ChannelOAuthStateRow.owner_user_id == owner, ChannelOAuthStateRow.provider == "telegram", ChannelOAuthStateRow.consumed_at.is_(None)))
        for code in codes:
            if code.metadata_json.get("telegram_bot_link_id") == link_id:
                await session.delete(code)
        await session.flush()

    async def codes_waiting(self, owner_user_id: str, *, except_link_id: str | None = None) -> int:
        async with self.session_factory() as session:
            return await self._codes_waiting(session, owner_user_id, except_link_id=except_link_id)

    async def _codes_waiting(self, session: AsyncSession, owner: str, *, except_link_id: str | None = None) -> int:
        codes = await session.scalars(
            select(ChannelOAuthStateRow).where(ChannelOAuthStateRow.owner_user_id == owner, ChannelOAuthStateRow.provider == "telegram", ChannelOAuthStateRow.consumed_at.is_(None), ChannelOAuthStateRow.expires_at >= _now())
        )
        return sum(except_link_id is None or row.metadata_json.get("telegram_bot_link_id") != except_link_id for row in codes)

    async def mint_code(self, link: BotLink, *, session: AsyncSession | None = None) -> Connect:
        if session is None:
            async with self._owner_writer(link.owner_user_id) as writer:
                return await self.mint_code(link, session=writer)
        row = await self._locked(session, link.id, captured=link)
        if row.state != "waiting_connect" or row.bind_mode != "code":
            raise ValueError("already-connected")
        _check_epoch(row.connect_conflict_epoch)
        await self._end_codes(session, row.id, row.owner_user_id)
        await session.execute(delete(ChannelOAuthStateRow).where(ChannelOAuthStateRow.owner_user_id == row.owner_user_id, ChannelOAuthStateRow.provider == "telegram", ChannelOAuthStateRow.expires_at < _now()))
        if await self._codes_waiting(session, row.owner_user_id) >= CONNECT_CODES_MAX:
            raise TooManyCodes()
        code, expires = secrets.token_urlsafe(24), _now() + timedelta(seconds=CONNECT_CODE_SECONDS)
        session.add(
            ChannelOAuthStateRow(
                state_hash=ChannelConnectionRepository.hash_state(code),
                owner_user_id=row.owner_user_id,
                provider="telegram",
                expires_at=expires,
                metadata_json={"telegram_bot_link_id": row.id, "connect_conflict_epoch": row.connect_conflict_epoch, "token_revision": row.token_revision},
            )
        )
        row.connect_expires_at, row.updated_at = expires, _now()
        await session.flush()
        return Connect(url=f"https://t.me/{row.bot_username}?start={code}", expires_at=expires)

    async def consume_code(self, link: BotLink, code: str, *, session: AsyncSession) -> Literal["ok", "expired", "other-bot"]:
        row = await self._locked(session, link.id, captured=link)
        _check_epoch(row.connect_conflict_epoch)
        if not isinstance(code, str):
            return "expired"
        saved = await session.get(ChannelOAuthStateRow, ChannelConnectionRepository.hash_state(code))
        now = _now()
        if saved is None or saved.provider != "telegram" or saved.consumed_at is not None or _time(saved.expires_at) < now:
            return "expired"
        metadata = saved.metadata_json
        if saved.owner_user_id != row.owner_user_id or metadata.get("telegram_bot_link_id") != row.id:
            # Consumption is transactional even for a deliberate wrong-bot press.
            await session.execute(update(ChannelOAuthStateRow).where(ChannelOAuthStateRow.state_hash == saved.state_hash, ChannelOAuthStateRow.consumed_at.is_(None)).values(consumed_at=now))
            return "other-bot"
        if type(metadata.get("connect_conflict_epoch")) is not int or type(metadata.get("token_revision")) is not int or metadata["connect_conflict_epoch"] != row.connect_conflict_epoch or metadata["token_revision"] != row.token_revision:
            return "expired"
        result = await session.execute(
            update(ChannelOAuthStateRow)
            .where(ChannelOAuthStateRow.state_hash == saved.state_hash, ChannelOAuthStateRow.consumed_at.is_(None), ChannelOAuthStateRow.expires_at >= now)
            .values(consumed_at=now)
            .execution_options(synchronize_session=False)
        )
        return "ok" if result.rowcount == 1 else "expired"

    async def record_conflict(self, link_id: str, *, token_revision: int) -> bool:
        async with self._transaction() as session:
            row = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link_id).with_for_update())
            if row is None or row.token_revision != token_revision:
                return False
            row.connect_conflict_epoch = _advance(row.connect_conflict_epoch, epoch=True)
            row.connect_expires_at, row.updated_at = None, _now()
            # This per-link-only writer never enters owner or capacity locks.
            codes = await session.scalars(select(ChannelOAuthStateRow).where(ChannelOAuthStateRow.owner_user_id == row.owner_user_id, ChannelOAuthStateRow.provider == "telegram", ChannelOAuthStateRow.consumed_at.is_(None)))
            for code in codes:
                if code.metadata_json.get("telegram_bot_link_id") == row.id:
                    await session.delete(code)
            return True

    async def mark_blocked_if_current(self, *, link_id: str, chat_id: int, binding_generation: int, token_revision: int) -> bool:
        try:
            owner = await self._owner_of(link_id)
        except ValueError:
            return False
        async with self._owner_writer(owner) as session:
            changed = await session.execute(
                update(TelegramBotLinkRow)
                .where(
                    TelegramBotLinkRow.id == link_id,
                    TelegramBotLinkRow.state == "connected",
                    TelegramBotLinkRow.chat_id == chat_id,
                    TelegramBotLinkRow.binding_generation == binding_generation,
                    TelegramBotLinkRow.token_revision == token_revision,
                )
                .values(state="blocked", updated_at=_now())
            )
            if changed.rowcount != 1:
                return False
            await self._event(session, await self._locked(session, link_id), "blocked", actor_id=owner)
            return True

    async def mark_unblocked_if_current(self, captured_link: BotLink, *, check_live_owner: LiveOwnerCheck) -> BotLink | None:
        """Reconnect only the live owner's captured blocked private binding."""
        self._lifecycle_fences(captured_link.token_revision, captured_link.binding_generation)
        if captured_link.state != "blocked" or type(captured_link.chat_id) is not int or captured_link.chat_id <= 0 or captured_link.chat_id != captured_link.telegram_user_id or captured_link.connection_id is None:
            return None
        basis = ("owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "token_revision", "binding_generation")
        async with self._owner_writer(captured_link.owner_user_id) as session:
            row = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == captured_link.id, TelegramBotLinkRow.owner_user_id == captured_link.owner_user_id).with_for_update().execution_options(populate_existing=True))
            if row is None or row.state != "blocked" or any(getattr(row, key) != getattr(captured_link, key) for key in basis):
                return None
            account = await session.get(TelegramAccountRow, row.telegram_user_id, with_for_update=True, populate_existing=True)
            if account is None or account.owner_user_id != row.owner_user_id:
                return None
            binding = await self._current_bound_connection(session, row)
            if binding is None or binding.status != "connected":
                return None
            await check_live_owner(row.owner_user_id, session)
            changed = await session.execute(
                update(TelegramBotLinkRow)
                .where(TelegramBotLinkRow.id == captured_link.id, TelegramBotLinkRow.state == "blocked", *(getattr(TelegramBotLinkRow, key) == getattr(captured_link, key) for key in basis))
                .values(state="connected", updated_at=_now())
                .execution_options(synchronize_session=False)
            )
            if changed.rowcount != 1:
                return None
            await session.refresh(row)
            await self._event(session, row, "unblocked", actor_id=row.owner_user_id)
            # Pending work keeps server/pacing floors. A blocked 403's producer
            # disposition is already due, held only by structural eligibility.
            return _snapshot(row)

    @staticmethod
    def _lifecycle_fences(token_revision: int, binding_generation: int) -> None:
        if type(token_revision) is not int or token_revision < 0 or type(binding_generation) is not int or binding_generation < 0:
            raise ValueError("invalid-lifecycle-basis")

    async def _current_bound_connection(self, session: AsyncSession, row: TelegramBotLinkRow) -> ChannelConnectionRow | None:
        if row.connection_id is None or row.telegram_user_id is None or row.telegram_user_id <= 0 or row.chat_id != row.telegram_user_id:
            return None
        return await session.scalar(
            select(ChannelConnectionRow)
            .where(
                ChannelConnectionRow.id == row.connection_id,
                ChannelConnectionRow.owner_user_id == row.owner_user_id,
                ChannelConnectionRow.provider == "telegram",
                ChannelConnectionRow.external_account_id == str(row.telegram_user_id),
                ChannelConnectionRow.workspace_id == str(row.bot_id),
                ChannelConnectionRow.bot_user_id == str(row.bot_id),
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )

    async def _thread_context(self, session: AsyncSession, captured: BotLink, origin: InboxRow) -> tuple[TelegramBotLinkRow, TelegramInboundMarkRow]:
        from app.telegram_bots.inbox import SERVER_BOOKKEEPING_KEYS, InboxRow

        if not isinstance(origin, InboxRow) or origin.link_id != captured.id:
            raise _ThreadPreparationStale
        row = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == captured.id, TelegramBotLinkRow.owner_user_id == captured.owner_user_id).with_for_update().execution_options(populate_existing=True))
        basis = ("owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
        if (
            row is None
            or row.state != "connected"
            or captured.state != "connected"
            or row.agent_kind not in {"lead_agent", "custom_agent"}
            or type(captured.token_revision) is not int
            or type(captured.binding_generation) is not int
            or type(row.telegram_user_id) is not int
            or row.telegram_user_id <= 0
            or row.chat_id != row.telegram_user_id
            or row.connection_id is None
            or any(getattr(row, key) != getattr(captured, key) for key in basis)
        ):
            raise _ThreadPreparationStale
        account = await session.get(TelegramAccountRow, row.telegram_user_id, with_for_update=True, populate_existing=True)
        connection = await self._current_bound_connection(session, row)
        if account is None or account.owner_user_id != row.owner_user_id or connection is None or connection.status != "connected":
            raise _ThreadPreparationStale
        mark = await session.scalar(
            select(TelegramInboundMarkRow)
            .where(TelegramInboundMarkRow.link_id == origin.link_id, TelegramInboundMarkRow.message_id == origin.message_id, TelegramInboundMarkRow.chat_id == origin.chat_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if mark is None or mark.route_kind is None or (mark.update_id, mark.binding_generation, mark.message_thread_id, mark.chat_id) != (origin.update_id, row.binding_generation, origin.message_thread_id, row.chat_id):
            raise _ThreadPreparationStale

        def business(value):
            return json.dumps({key: child for key, child in value.items() if key not in SERVER_BOOKKEEPING_KEYS}, sort_keys=True, allow_nan=False)

        if (
            origin.binding_generation != mark.binding_generation
            or (mark.route_kind, mark.route_target_id, mark.operation_ids, mark.sanitized_input) != (origin.route_kind, origin.route_target_id, origin.operation_ids, origin.sanitized_input)
            or business(mark.route_basis) != business(origin.route_basis)
        ):
            raise _ThreadPreparationStale
        return row, mark

    @staticmethod
    async def _thread_mapping(session: AsyncSession, link: TelegramBotLinkRow) -> ChannelConversationRow | None:
        mapping = await session.scalar(
            select(ChannelConversationRow)
            .where(ChannelConversationRow.connection_id == link.connection_id, ChannelConversationRow.external_conversation_id == str(link.chat_id), ChannelConversationRow.external_topic_id == "")
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if mapping is not None and (mapping.owner_user_id != link.owner_user_id or mapping.provider != "telegram"):
            raise _ThreadPreparationStale
        return mapping

    @staticmethod
    def _thread_preparation(mark: TelegramInboundMarkRow, link: TelegramBotLinkRow) -> ThreadPreparation:
        from app.telegram_bots.inbox import InboxRow

        saved = mark.route_basis.get("thread_preparation")
        keys = {field.name for field in fields(ThreadPreparation)} - {"origin"}
        if (
            not isinstance(saved, dict)
            or set(saved) != keys
            or saved["purpose"] not in {"ensure", "new"}
            or not isinstance(saved["creation_id"], str)
            or not _HEX_ID.fullmatch(saved["creation_id"])
            or not isinstance(saved["endpoint_id"], str)
            or not _DIGEST.fullmatch(saved["endpoint_id"])
            or not _thread_equal(saved["scope"], _thread_scope(link))
        ):
            raise _ThreadPreparationStale
        expected_id, expected_thread, selected, mapping_id = (saved[key] for key in ("expected_mapping_id", "expected_thread_id", "selected_thread_id", "mapping_id"))
        if (
            (expected_id is None) != (expected_thread is None)
            or expected_id is not None
            and (not _valid_thread_id(expected_id) or not _valid_thread_id(expected_thread))
            or (selected is None) != (mapping_id is None)
            or selected is not None
            and (not _valid_thread_id(selected) or not _valid_thread_id(mapping_id) or selected not in ({saved["creation_id"]} if saved["purpose"] == "new" else {saved["creation_id"], expected_thread}))
        ):
            raise _ThreadPreparationStale
        return ThreadPreparation(origin=InboxRow.from_row(mark), **deepcopy(saved))

    @staticmethod
    def _thread_attempt(check_attempt_current: Callable[[], bool]) -> None:
        if check_attempt_current() is not True:
            raise _ThreadPreparationStale

    async def reserve_thread_preparation(
        self, captured: BotLink, origin: InboxRow, *, purpose: str, endpoint_id: str, check_live_owner: LiveOwnerCheck, check_attempt_current: Callable[[], bool], require_healthy: bool = False
    ) -> ThreadPreparation:
        """Reserve an origin; normal private dispatch also requires healthy state."""
        from app.telegram_bots.inbox import InboxRow

        if purpose not in {"ensure", "new"} or not isinstance(endpoint_id, str) or not _DIGEST.fullmatch(endpoint_id) or type(require_healthy) is not bool:
            raise _ThreadPreparationStale
        async with self._owner_writer(captured.owner_user_id) as session:
            row, mark = await self._thread_context(session, captured, origin)
            if "thread_preparation" in mark.route_basis:
                preparation = self._thread_preparation(mark, row)
                if preparation.purpose != purpose or preparation.endpoint_id != endpoint_id:
                    raise _ThreadPreparationStale
                if preparation.selected_thread_id is None and (mark.route_complete or mark.state == "interrupted" or mark.thread_id is not None):
                    raise _ThreadPreparationStale
                if preparation.selected_thread_id is None:
                    mapping = await self._thread_mapping(session, row)
                    if (mapping.id if mapping else None, mapping.thread_id if mapping else None) != (preparation.expected_mapping_id, preparation.expected_thread_id):
                        raise _ThreadPreparationStale
                await check_live_owner(row.owner_user_id, session)
                if require_healthy and row.health != "ok":
                    raise _ThreadPreparationStale
                self._thread_attempt(check_attempt_current)
                return preparation
            if mark.route_complete or mark.state == "interrupted" or mark.thread_id is not None:
                raise _ThreadPreparationStale
            mapping = await self._thread_mapping(session, row)
            await check_live_owner(row.owner_user_id, session)
            if require_healthy and row.health != "ok":
                raise _ThreadPreparationStale
            self._thread_attempt(check_attempt_current)
            preparation = ThreadPreparation(
                origin=InboxRow.from_row(mark),
                purpose=purpose,
                creation_id=uuid4().hex,
                endpoint_id=endpoint_id,
                scope=_thread_scope(row),
                expected_mapping_id=mapping.id if mapping else None,
                expected_thread_id=mapping.thread_id if mapping else None,
                selected_thread_id=None,
                mapping_id=None,
            )
            mark.route_basis = {**deepcopy(mark.route_basis), "thread_preparation": _thread_payload(preparation)}
            await session.flush()
            return self._thread_preparation(mark, row)

    async def commit_thread_mapping(self, captured: BotLink, preparation: ThreadPreparation, *, thread_id: str, endpoint_id: str, check_live_owner: LiveOwnerCheck, check_attempt_current: Callable[[], bool]) -> bool:
        """Compare the original mapping and atomically record this exact outcome."""
        if not isinstance(preparation, ThreadPreparation) or not _valid_thread_id(thread_id):
            return False
        try:
            async with self._owner_writer(captured.owner_user_id) as session:
                row, mark = await self._thread_context(session, captured, preparation.origin)
                saved = self._thread_preparation(mark, row)
                supplied, persisted = _thread_payload(preparation), _thread_payload(saved)
                for key in ("selected_thread_id", "mapping_id"):
                    if supplied[key] is None:
                        supplied[key] = persisted[key]
                if not _thread_equal(supplied, persisted) or endpoint_id != saved.endpoint_id or thread_id not in ({saved.creation_id} if saved.purpose == "new" else {saved.creation_id, saved.expected_thread_id}):
                    raise _ThreadPreparationStale
                if saved.selected_thread_id is not None:
                    await check_live_owner(row.owner_user_id, session)
                    if row.health != "ok":
                        raise _ThreadPreparationStale
                    self._thread_attempt(check_attempt_current)
                    return saved.selected_thread_id == thread_id
                if mark.route_complete or mark.state == "interrupted" or mark.thread_id is not None:
                    raise _ThreadPreparationStale
                mapping = await self._thread_mapping(session, row)
                if (mapping.id if mapping else None, mapping.thread_id if mapping else None) != (saved.expected_mapping_id, saved.expected_thread_id):
                    raise _ThreadPreparationStale
                await check_live_owner(row.owner_user_id, session)
                if row.health != "ok":
                    raise _ThreadPreparationStale
                self._thread_attempt(check_attempt_current)
                natural_key = (ChannelConversationRow.connection_id == row.connection_id, ChannelConversationRow.external_conversation_id == str(row.chat_id), ChannelConversationRow.external_topic_id == "")
                if mapping is not None and mapping.thread_id == thread_id:
                    mapping_id = mapping.id
                elif mapping is not None:
                    mapping_id = await session.scalar(
                        update(ChannelConversationRow)
                        .where(
                            *natural_key,
                            ChannelConversationRow.id == saved.expected_mapping_id,
                            ChannelConversationRow.thread_id == saved.expected_thread_id,
                            ChannelConversationRow.owner_user_id == row.owner_user_id,
                            ChannelConversationRow.provider == "telegram",
                        )
                        .values(id=uuid4().hex, thread_id=thread_id, updated_at=_now())
                        .returning(ChannelConversationRow.id)
                        .execution_options(synchronize_session=False)
                    )
                else:
                    mapping_id = await session.scalar(
                        _insert(session)(ChannelConversationRow)
                        .values(id=uuid4().hex, connection_id=row.connection_id, owner_user_id=row.owner_user_id, provider="telegram", external_conversation_id=str(row.chat_id), external_topic_id="", thread_id=thread_id)
                        .on_conflict_do_nothing(index_elements=["connection_id", "external_conversation_id", "external_topic_id"])
                        .returning(ChannelConversationRow.id)
                    )
                if mapping_id is None:
                    raise _ThreadPreparationStale
                self._thread_attempt(check_attempt_current)
                mark.route_basis = {**deepcopy(mark.route_basis), "thread_preparation": {**persisted, "selected_thread_id": thread_id, "mapping_id": mapping_id}}
                await session.flush()
                return True
        except _ThreadPreparationStale:
            return False

    async def hold_if_current(self, link_id: str, *, token_revision: int, binding_generation: int) -> BotLink | None:
        """Hold the freshly refused owner's exact captured binding and retire its work.

        The supervisor reads current owner refusal before calling. This writer
        rechecks connection identity and both fences, and owns no account policy
        or network operation. A repeated or superseded transition does nothing.
        """
        self._lifecycle_fences(token_revision, binding_generation)
        try:
            owner = await self._owner_of(link_id)
        except ValueError:
            return None
        async with self._owner_writer(owner) as session:
            row = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link_id, TelegramBotLinkRow.owner_user_id == owner))
            if row is None or row.token_revision != token_revision or row.binding_generation != binding_generation or row.state not in {"connected", "blocked"}:
                return None
            binding = await self._current_bound_connection(session, row)
            if binding is None or binding.status not in {"connected", "held"}:
                return None
            binding.status = "held"
            row.state, row.binding_generation, row.updated_at = "held", _advance(row.binding_generation), _now()
            if isinstance(row.notices, dict):
                notices = deepcopy(row.notices)
                notices.pop("access_off", None)
                row.notices = notices
            await session.flush()
            await self.end_pending(link_id, skip_reason="disabled", session=session)
            await self._event(session, row, "hold", actor_id=owner)
            return _snapshot(row)

    async def release_if_current(self, link_id: str, *, token_revision: int, binding_generation: int) -> BotLink | None:
        """Join an exact binding already restored by the operator, without restoring it."""
        self._lifecycle_fences(token_revision, binding_generation)
        try:
            owner = await self._owner_of(link_id)
        except ValueError:
            return None
        async with self._owner_writer(owner) as session:
            row = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link_id, TelegramBotLinkRow.owner_user_id == owner))
            if row is None or row.token_revision != token_revision or row.binding_generation != binding_generation or row.state != "held":
                return None
            binding = await self._current_bound_connection(session, row)
            if binding is None or binding.status != "connected":
                return None
            row.state, row.updated_at = "connected", _now()
            await self._event(session, row, "release", actor_id=owner)
            return _snapshot(row)

    async def mark_notice_if_current(self, link_id: str, key: str, *, token_revision: int, binding_generation: int, at: datetime, minimum_interval_seconds: float = 0, only_if_unsent: bool = False) -> bool:
        """Claim one bounded fixed notice before transport; no send runs in SQL.

        A lost/failed disposable send retains this claim. The captured revision
        and binding cannot claim a successor's notice, and stale clock readings
        cannot overwrite a later claim. Existing notice categories keep their
        documented ISO timestamp / policy value-and-time representation.
        """
        self._lifecycle_fences(token_revision, binding_generation)
        if (
            not isinstance(key, str)
            or key not in _NOTICE_KEYS
            or not isinstance(at, datetime)
            or at.tzinfo is None
            or at.utcoffset() is None
            or type(minimum_interval_seconds) not in {int, float}
            or not math.isfinite(minimum_interval_seconds)
            or minimum_interval_seconds < 0
            or type(only_if_unsent) is not bool
            or only_if_unsent
            and key != "access.off"
        ):
            raise ValueError("invalid-notice-claim")
        at = at.astimezone(UTC)
        try:
            owner = await self._owner_of(link_id)
        except ValueError:
            return False
        async with self._owner_writer(owner) as session:
            row = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link_id, TelegramBotLinkRow.owner_user_id == owner))
            if row is None or row.token_revision != token_revision or row.binding_generation != binding_generation or row.state not in {"connected", "blocked", "held"}:
                return False
            binding = await self._current_bound_connection(session, row)
            if binding is None or binding.status not in {"connected", "held"} or not isinstance(row.notices, dict):
                return False
            stored_key, policy_value = _NOTICE_KEYS[key]
            notices = deepcopy(row.notices)
            if only_if_unsent and stored_key in notices:
                return False
            previous = notices.get(stored_key)
            if previous is not None:
                if policy_value is not None:
                    if not isinstance(previous, dict) or set(previous) != {"value", "at"} or not isinstance(previous["value"], str) or previous["value"] not in {"off", "allowed", "protected"}:
                        return False
                    previous = previous["at"]
                try:
                    if not isinstance(previous, str):
                        return False
                    stamp = datetime.fromisoformat(previous)
                    if stamp.tzinfo is None or stamp.utcoffset() is None:
                        return False
                except ValueError:
                    return False
                elapsed = (at - stamp).total_seconds()
                if elapsed <= 0 or elapsed < minimum_interval_seconds:
                    return False
            notices[stored_key] = at.isoformat() if policy_value is None else {"value": policy_value, "at": at.isoformat()}
            row.notices, row.updated_at = notices, _now()
            await session.flush()
            return True

    async def set_health(
        self,
        link_id: str,
        cause: str | None,
        *,
        holds: bool,
        transition: dict | None = None,
        expected_token_revision: int | None = None,
        captured_link: BotLink | None = None,
        check_attempt_current: Callable[[], bool] | None = None,
    ) -> BotLink | None:
        """Apply cause precedence and optional absolute conflict metadata.

        The poller owns its probe-duration constant and supplies all three
        transition fields together on first entry or an explicit new probe.
        Omission preserves an existing window; an explicit null host clears it.
        A captured revision retires late callbacks without changing SQL or the
        cause cache. One live supervisor owns ordinary cause publication.
        """
        if expected_token_revision is not None and (type(expected_token_revision) is not int or expected_token_revision < 0):
            raise ValueError("invalid-token-revision")
        if cause not in _HEALTH_ORDER and cause is not None:
            raise ValueError("invalid-health-cause")
        if transition is not None:
            if cause != "conflict" or not holds or not isinstance(transition, dict) or set(transition) != {"conflict_since", "probe_until", "webhook_host"}:
                raise ValueError("invalid-conflict-transition")
            transition = dict(transition)
            for key in ("conflict_since", "probe_until"):
                value = transition[key]
                if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
                    raise ValueError("invalid-conflict-transition")
                transition[key] = value.astimezone(UTC)
            host = transition["webhook_host"]
            if transition["probe_until"] < transition["conflict_since"] or host is not None and (not isinstance(host, str) or not 1 <= len(host) <= 255 or not re.fullmatch(r"[A-Za-z0-9.:\[\]-]+", host) or _TOKEN.search(host)):
                raise ValueError("invalid-conflict-transition")
        # Keep the process-local publication inside the same per-link scope as
        # the SQL commit, including its asynchronous session cleanup.
        async with self._health_lock(link_id):
            try:
                owner = await self._owner_of(link_id)
            except ValueError:
                if expected_token_revision is not None:
                    return None
                raise
            async with self._owner_writer(owner) as session:
                row = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link_id, TelegramBotLinkRow.owner_user_id == owner).with_for_update())
                if row is None:
                    if expected_token_revision is not None:
                        return None
                    raise ValueError("link-not-found")
                if expected_token_revision is not None and row.token_revision != expected_token_revision:
                    return None
                if captured_link is not None:
                    if (
                        row.id != captured_link.id
                        or row.state != "connected"
                        or captured_link.state != "connected"
                        or not _thread_equal(_thread_scope(row), _thread_scope(captured_link))
                        or row.token_revision != captured_link.token_revision
                    ):
                        return None
                    account = await session.get(TelegramAccountRow, row.telegram_user_id, with_for_update=True, populate_existing=True)
                    binding = await self._current_bound_connection(session, row)
                    if account is None or account.owner_user_id != row.owner_user_id or binding is None or binding.status != "connected":
                        return None
                if check_attempt_current is not None and check_attempt_current() is not True:
                    return None
                cached = self._health_causes.get(link_id) if self._health_revisions.get(link_id) == row.token_revision else None
                causes = set(cached if cached is not None else {row.health} if row.health != "ok" else set())
                if cause is not None:
                    causes.add(cause) if holds else causes.discard(cause)
                selected = next((item for item in _HEALTH_ORDER if item in causes), "ok")
                row.health, row.updated_at = selected, _now()
                if "conflict" in causes and row.acts_paused_since is None:
                    row.acts_paused_since = _now()
                if "conflict" in causes:
                    if transition is not None:
                        if row.conflict_since is not None and _time(row.conflict_since) != transition["conflict_since"]:
                            raise ValueError("conflict-transition-changed")
                        for key, value in transition.items():
                            setattr(row, key, value)
                    elif row.conflict_since is None:
                        row.conflict_since = _now()
                elif cause == "conflict" and not holds:
                    row.conflict_since = row.probe_until = row.webhook_host = None
                await session.flush()
                result = _snapshot(row)
            if check_attempt_current is not None and check_attempt_current() is not True:
                return None
            self._health_causes[link_id] = causes
            self._health_revisions[link_id] = result.token_revision
            return result

    async def _store_token(
        self, session: AsyncSession, row: TelegramBotLinkRow, encrypted_token: str, *, actor_id: str, action: str, expected_revision: int | None = None, expected_encrypted_token: str | None = None, token_changed: bool = False
    ) -> BotLink:
        if not isinstance(encrypted_token, str) or not encrypted_token or _TOKEN.search(encrypted_token):
            raise ValueError("invalid-encrypted-token")
        if expected_revision is not None and row.token_revision != expected_revision or expected_encrypted_token is not None and row.encrypted_token != expected_encrypted_token:
            raise ValueError("token-changed")
        if action == "rewrap":
            if expected_revision is None or expected_encrypted_token is None:
                raise ValueError("rewrap-requires-current-basis")
        else:
            if token_changed and expected_revision is None:
                raise ValueError("token-comparison-requires-current-basis")
            row.token_revision = _advance(row.token_revision)
            row.connect_expires_at = None
            await self._end_codes(session, row.id, row.owner_user_id)
            # A new transport must not inherit an earlier worker's claim. Its
            # saved replies and acknowledged parts still belong to this binding.
            await session.execute(update(TelegramOutboxRow).where(TelegramOutboxRow.link_id == row.id, TelegramOutboxRow.state.in_(("pending", "sending"))).values(state="pending", claim_owner=None, claim_id=None, claimed_at=None))
            row.health, row.profile_pending = "ok", True
            row.conflict_since = row.probe_until = row.webhook_host = None
            if token_changed:
                row.acts_paused_since = None
        row.encrypted_token, row.updated_at = encrypted_token, _now()
        await self._event(session, row, action, actor_id=actor_id)
        await session.flush()
        return _snapshot(row)

    async def store_token(self, link_id: str, encrypted_token: str, *, actor_id: str, action: str = "replace_token", expected_revision: int | None = None, expected_encrypted_token: str | None = None, token_changed: bool = False) -> BotLink:
        """Replace a logical token; rewrapping requires an exact locked CAS basis.

        ``token_changed`` is a caller's plaintext comparison, never a comparison
        of randomized ciphertexts. It requires its captured token revision.
        """
        if action not in {"replace_token", "managed_token", "rewrap", "retry"}:
            raise ValueError("invalid-token-action")
        async with self._health_lock(link_id):
            async with self._owner_writer(await self._owner_of(link_id)) as session:
                row = await self._locked(session, link_id)
                result = await self._store_token(session, row, encrypted_token, actor_id=actor_id, action=action, expected_revision=expected_revision, expected_encrypted_token=expected_encrypted_token, token_changed=token_changed)
            if action != "rewrap":
                self._health_causes.pop(link_id, None)
                self._health_revisions.pop(link_id, None)
            return result

    @staticmethod
    def _start_identity(identity: dict, account: int, chat: int) -> dict:
        allowed = {"update_id", "message_id", "chat_id", "message_thread_id"}
        if type(account) is not int or type(chat) is not int or not 0 < account <= _EPOCH_MAX or account != chat:
            raise ValueError("private-chat-required")
        if not isinstance(identity, dict) or set(identity) - allowed or not {"update_id", "message_id", "chat_id"} <= identity.keys():
            raise ValueError("invalid-start-identity")
        if any(type(identity[key]) is not int or not 0 <= identity[key] <= _EPOCH_MAX for key in ("update_id", "message_id", "chat_id")) or identity["message_id"] == 0 or identity["chat_id"] != chat:
            raise ValueError("invalid-start-identity")
        topic = identity.get("message_thread_id")
        if topic is not None and (type(topic) is not int or not 0 < topic <= _EPOCH_MAX):
            raise ValueError("invalid-start-identity")
        return {**{key: identity[key] for key in ("update_id", "message_id", "chat_id")}, "message_thread_id": None if topic in (None, 1) else topic}

    @staticmethod
    def _hello_parts(parts: list[dict], kind: str) -> list[dict]:
        """Freeze product source references, never caller-rendered text or code.

        Input parts contain ``source_ref={key, values}`` and optional ``silent``.
        The first key names the target's hello; the final key is hello.privacy.
        """
        hellos = {"lead_agent": {"hello.chat"}, "custom_agent": {"hello.custom_agent"}, "instance": {"hello.assistant", "hello.employee"}}[kind]
        if not isinstance(parts, list) or not 2 <= len(parts) <= 4:
            raise ValueError("invalid-welcome-plan")
        result = []
        for index, part in enumerate(parts):
            if not isinstance(part, dict) or set(part) - {"source_ref", "silent"}:
                raise ValueError("invalid-welcome-plan")
            source = part.get("source_ref")
            if not isinstance(source, dict) or set(source) != {"key", "values"} or not isinstance(source["key"], str) or not isinstance(source["values"], dict):
                raise ValueError("invalid-welcome-plan")
            key = source["key"]
            if index == 0 and key not in hellos or index == len(parts) - 1 and key != "hello.privacy" or 0 < index < len(parts) - 1 and (kind != "instance" or key != "privacy.company"):
                raise ValueError("invalid-welcome-plan")
            for name, value in source["values"].items():
                if not isinstance(name, str) or not re.fullmatch(r"[a-z_]{1,40}", name) or any(term in name for term in ("token", "code", "update")):
                    raise ValueError("invalid-welcome-plan")
                if value is not None and type(value) not in (str, bool, int) or isinstance(value, str) and (len(value) > 4096 or _TOKEN.search(value)):
                    raise ValueError("invalid-welcome-plan")
            if "silent" in part and type(part["silent"]) is not bool:
                raise ValueError("invalid-welcome-plan")
            result.append(
                {
                    "part_id": uuid4().hex,
                    "presentation_version": 0,
                    "ordinal": index,
                    "kind": "text",
                    "source_ref": deepcopy(source),
                    "state": "pending",
                    "message_ids": [],
                    "callback_group_id": None,
                    "silent": True if key == "hello.privacy" else part.get("silent", False),
                }
            )
        return result

    async def _binding_row(self, session: AsyncSession, row: TelegramBotLinkRow, account: int, name: str | None) -> ChannelConnectionRow:
        identity = (ChannelConnectionRow.provider == "telegram", ChannelConnectionRow.external_account_id == str(account), ChannelConnectionRow.workspace_id == str(row.bot_id))
        other = await session.scalar(select(ChannelConnectionRow.id).where(*identity, ChannelConnectionRow.owner_user_id != row.owner_user_id, ChannelConnectionRow.status != "revoked").limit(1))
        if other is not None:
            raise ValueError("account-taken")
        owned = (*identity, ChannelConnectionRow.owner_user_id == row.owner_user_id)
        binding = await session.scalar(select(ChannelConnectionRow).where(*owned).with_for_update())
        if binding is None:
            identity_id = uuid4().hex
            inserted = await session.scalar(
                _insert(session)(ChannelConnectionRow)
                .values(id=identity_id, owner_user_id=row.owner_user_id, provider="telegram", external_account_id=str(account), external_account_name=name, workspace_id=str(row.bot_id), bot_user_id=str(row.bot_id), status="connected")
                .on_conflict_do_nothing()
                .returning(ChannelConnectionRow.id)
            )
            if inserted is None:
                binding = await session.scalar(select(ChannelConnectionRow).where(*owned).with_for_update().execution_options(populate_existing=True))
                if binding is None:
                    raise ValueError("account-taken")
            else:
                binding = await session.get(ChannelConnectionRow, inserted)
        binding.status, binding.external_account_name, binding.workspace_name, binding.bot_user_id = "connected", name, None, str(row.bot_id)
        binding.scopes_json, binding.capabilities_json, binding.metadata_json = [], {}, {}
        await session.flush()
        return binding

    async def bind(
        self, link: BotLink, *, telegram_user_id: int, chat_id: int, first_name: str | None, username: str | None, start_identity: dict, hello_parts: list[dict], code: str | None = None, check_live_owner: LiveOwnerCheck | None = None
    ) -> BotLink:
        try:
            result = await self._bind(link, telegram_user_id=telegram_user_id, chat_id=chat_id, first_name=first_name, username=username, start_identity=start_identity, hello_parts=hello_parts, code=code, check_live_owner=check_live_owner)
        except IntegrityError as error:
            # _bind's owning transaction and session have fully exited here.
            # A legacy writer does not take our owner/account locks and can win
            # an identity just before a revoked row is reactivated. Report the
            # defined custody refusal, without masking other schema failures.
            if _active_identity_collision(error):
                raise ValueError("account-taken") from None
            raise
        if result == "other-bot":
            # Only this deliberate refusal commits its one-use consumption.
            # Every exception during a successful-code bind still rolls back.
            raise ValueError("other-bot")
        return result

    async def _bind(
        self, link: BotLink, *, telegram_user_id: int, chat_id: int, first_name: str | None, username: str | None, start_identity: dict, hello_parts: list[dict], code: str | None = None, check_live_owner: LiveOwnerCheck | None = None
    ) -> BotLink | Literal["other-bot"]:
        identity = self._start_identity(start_identity, telegram_user_id, chat_id)
        for value in (first_name, username):
            if value is not None and (not isinstance(value, str) or len(value) > 64):
                raise ValueError("invalid-account-name")
        # Pure preparation occurs before entering the database writer. Part ids
        # from a losing replay are discarded, never used to replace a saved plan.
        parts = self._hello_parts(hello_parts, link.agent_kind)
        async with self._owner_writer(link.owner_user_id) as session:
            current = await self._locked(session, link.id)
            previous = await session.scalar(
                select(TelegramInboundMarkRow).where(
                    TelegramInboundMarkRow.link_id == link.id,
                    or_(and_(TelegramInboundMarkRow.chat_id == chat_id, TelegramInboundMarkRow.message_id == identity["message_id"]), TelegramInboundMarkRow.update_id == identity["update_id"]),
                )
            )
            if previous is not None:
                if previous.update_id != identity["update_id"] or previous.message_id != identity["message_id"] or previous.chat_id != chat_id or previous.message_thread_id != identity["message_thread_id"]:
                    raise ValueError("invalid-start-identity")
                if (
                    current.binding_generation != previous.binding_generation
                    or current.telegram_user_id != telegram_user_id
                    or current.chat_id != chat_id
                    or current.state not in {"connected", "blocked"}
                    or previous.route_kind != "command"
                    or previous.route_target_id != "start"
                    or not previous.route_complete
                ):
                    raise ValueError("link-changed")
                return _snapshot(current)
            current = await self._locked(session, link.id, captured=link)
            _check_epoch(current.connect_conflict_epoch)
            if current.state != "waiting_connect":
                raise ValueError("already-connected")
            if current.token_revision != link.token_revision:
                raise ValueError("link-changed")
            if check_live_owner is not None:
                await check_live_owner(current.owner_user_id, session)
            if current.bind_mode == "code":
                answer = await self.consume_code(_snapshot(current), code, session=session)
                if answer == "other-bot":
                    return "other-bot"
                if answer != "ok":
                    raise ValueError(answer)
            else:
                manager = await session.get(TelegramBotLinkRow, current.manager_link_id, populate_existing=True) if current.manager_link_id is not None else None
                if current.creator_telegram_user_id != telegram_user_id or manager is None or manager.owner_user_id != current.owner_user_id or manager.state != "connected" or manager.telegram_user_id != telegram_user_id:
                    raise ValueError("creator-not-authorized")
                binding = await session.get(ChannelConnectionRow, manager.connection_id) if manager.connection_id is not None else None
                if binding is None or binding.status != "connected" or binding.owner_user_id != current.owner_user_id:
                    raise ValueError("creator-not-authorized")
            now = _now()
            await session.execute(
                _insert(session)(TelegramAccountRow)
                .values(telegram_user_id=telegram_user_id, owner_user_id=current.owner_user_id, first_name=first_name, username=username, created_at=now)
                .on_conflict_do_nothing(index_elements=["telegram_user_id"])
            )
            account = await session.scalar(select(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == telegram_user_id).with_for_update())
            if account.owner_user_id != current.owner_user_id:
                raise ValueError("account-taken")
            binding = await self._binding_row(session, current, telegram_user_id, first_name)
            current.connection_id, current.telegram_user_id, current.chat_id = binding.id, telegram_user_id, chat_id
            current.connected_name = first_name or username
            current.connected_at, current.updated_at = now, now
            current.binding_generation = _advance(current.binding_generation)
            current.state, current.connect_expires_at = "connected", None
            await self._end_codes(session, current.id, current.owner_user_id)
            await self._event(session, current, "connect", actor_id=current.owner_user_id)
            plan_id = uuid4().hex
            basis = ["start", current.id, current.binding_generation, identity["update_id"], identity["message_id"], chat_id, identity["message_thread_id"]]
            delivery_key = "notice:" + hashlib.sha256(json.dumps(basis, separators=(",", ":")).encode("utf-8")).hexdigest()
            session.add(
                TelegramOutboxRow(
                    id=plan_id,
                    link_id=current.id,
                    owner_user_id=current.owner_user_id,
                    dedupe_key=delivery_key,
                    class_="reply",
                    binding_generation=current.binding_generation,
                    payload={"kind": "welcome", "presentation_version": 0},
                    parts=parts,
                    state="pending",
                    not_before=now,
                    chat_id=chat_id,
                    message_thread_id=identity["message_thread_id"],
                    created_at=now,
                )
            )
            session.add(
                TelegramInboundMarkRow(
                    link_id=current.id,
                    message_id=identity["message_id"],
                    update_id=identity["update_id"],
                    chat_id=chat_id,
                    message_thread_id=identity["message_thread_id"],
                    binding_generation=current.binding_generation,
                    accepted_at=now,
                    route_kind="command",
                    route_target_id="start",
                    sanitized_input={"command": "start"},
                    outbox_id=plan_id,
                    delivery_ids=[plan_id],
                    route_complete=True,
                    state="reply_pending",
                )
            )
            await session.flush()
            # A Work baseline joins this same session in stage three. Until then
            # its generation marker stays null and claims no seeded history.
            return _snapshot(current)

    @staticmethod
    def _operation(operation_id: str, request_digest: str) -> None:
        if not isinstance(operation_id, str) or not _HEX_ID.fullmatch(operation_id) or not isinstance(request_digest, str) or not _DIGEST.fullmatch(request_digest):
            raise ValueError("invalid-operation")

    async def _receipt(self, session: AsyncSession, owner: str, operation_id: str, request_digest: str) -> dict | None:
        saved = await session.scalar(select(TelegramBotLinkEventRow).where(TelegramBotLinkEventRow.owner_user_id == owner, TelegramBotLinkEventRow.operation_id == operation_id))
        if saved is None:
            return None
        if saved.action != "token_replace_receipt" or saved.request_digest != request_digest:
            raise ValueError("operation-mismatch")
        return deepcopy(saved.detail)

    async def replacement_receipt(self, owner_user_id: str, operation_id: str, request_digest: str) -> dict | None:
        """Read an exact receipt before a caller looks up an old link or token."""
        self._operation(operation_id, request_digest)
        async with self.session_factory() as session:
            return await self._receipt(session, owner_user_id, operation_id, request_digest)

    async def replace_from_token(self, *, owner_user_id: str, old_link_id: str, operation_id: str, request_digest: str, validated_bot: dict, encrypted_token: str) -> dict:
        """Commit one idempotent token repair or net-zero bot replacement.

        ``validated_bot`` carries getMe's id/username. A ``token_changed=True``
        plaintext comparison additionally carries ``expected_token_revision``.
        Optional ``take_over_webhook`` records an owed postcommit action only;
        this repository performs no network call and returns no connect code.
        """
        self._operation(operation_id, request_digest)
        async with self._health_lock(old_link_id):
            changed_link = None
            async with self.capacity_writer(owner_user_id) as session:
                previous = await self._receipt(session, owner_user_id, operation_id, request_digest)
                if previous is not None:
                    return previous
                row = await self._locked(session, old_link_id)
                if row.owner_user_id != owner_user_id:
                    raise ValueError("link-not-found")
                if not isinstance(validated_bot, dict):
                    raise ValueError("invalid-bot")
                bot_id, username = validated_bot.get("id"), validated_bot.get("username")
                if type(bot_id) is not int or not 0 < bot_id <= _EPOCH_MAX or not isinstance(username, str) or not _USERNAME.fullmatch(username):
                    raise ValueError("invalid-bot")
                proof = validated_bot.get("token_changed", False)
                expected = validated_bot.get("expected_token_revision")
                takeover = validated_bot.get("take_over_webhook", False)
                if type(proof) is not bool or type(takeover) is not bool or expected is not None and (type(expected) is not int or expected < 1):
                    raise ValueError("invalid-token-comparison")
                if proof and expected is None:
                    raise ValueError("token-comparison-requires-current-basis")
                if expected is not None and expected != row.token_revision:
                    raise ValueError("token-changed")
                if bot_id == row.bot_id:
                    result = await self._store_token(session, row, encrypted_token, actor_id=owner_user_id, action="replace_token", expected_revision=expected, token_changed=proof)
                    row.bot_username = username
                    row.updated_at = _now()
                    changed_link = row.id
                else:
                    duplicate = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.bot_id == bot_id))
                    if duplicate is not None:
                        raise BotLinked(own=duplicate.owner_user_id == owner_user_id)
                    if await self._codes_waiting(session, owner_user_id, except_link_id=old_link_id) >= CONNECT_CODES_MAX:
                        raise TooManyCodes()
                    target_kind, target_ref = row.agent_kind, row.agent_ref
                    await self.remove(old_link_id, actor_id=owner_user_id, action="replace_bot", session=session)
                    result = await self.add(owner_user_id=owner_user_id, agent_kind=target_kind, agent_ref=target_ref, bot_id=bot_id, bot_username=username, encrypted_token=encrypted_token, actor_id=owner_user_id, session=session)
                    await self.mint_code(result, session=session)
                receipt = {"operation_id": operation_id, "old_link_id": old_link_id, "new_link_id": result.id, "token_revision": result.token_revision, "managed_replaced": 0}
                if takeover:
                    receipt["takeover"] = {"owed": True, "done": False}
                await self._event(session, result, "token_replace_receipt", actor_id=owner_user_id, detail=receipt, operation_id=operation_id, request_digest=request_digest)
            self._health_causes.pop(changed_link or old_link_id, None)
            self._health_revisions.pop(changed_link or old_link_id, None)
            return receipt

    async def _close_deliveries(self, session: AsyncSession, link_id: str) -> None:
        inputs = await session.scalars(select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.link_id == link_id, TelegramInboundMarkRow.route_complete.is_(True), TelegramInboundMarkRow.state == "reply_pending"))
        for mark in inputs:
            identities = list(mark.delivery_ids)
            if not identities:
                continue
            states = list(await session.scalars(select(TelegramOutboxRow.state).where(TelegramOutboxRow.link_id == link_id, TelegramOutboxRow.id.in_(identities))))
            if len(states) == len(identities) and all(state in {"sent", "skipped"} for state in states):
                mark.state, mark.closed_at = "done", _now()

    async def end_pending(self, link_id: str, *, skip_reason: str, consume_callbacks: bool = True, session: AsyncSession | None = None) -> None:
        if not isinstance(skip_reason, str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,31}", skip_reason):
            raise ValueError("invalid-skip-reason")
        if session is None:
            async with self._owner_writer(await self._owner_of(link_id)) as writer:
                await self.end_pending(link_id, skip_reason=skip_reason, consume_callbacks=consume_callbacks, session=writer)
                return
        row = await self._locked(session, link_id)
        await session.execute(
            update(TelegramOutboxRow)
            .where(TelegramOutboxRow.link_id == link_id, TelegramOutboxRow.state.in_(("pending", "sending")))
            .values(state="skipped", skip_reason=skip_reason, claim_owner=None, claim_id=None, claimed_at=None, closed_at=_now())
        )
        if consume_callbacks:
            await session.execute(update(TelegramCallbackRow).where(TelegramCallbackRow.link_id == link_id, TelegramCallbackRow.consumed_at.is_(None)).values(consumed_at=_now()))
        for cls in (TelegramPendingFileRow, TelegramWorkTopicRow):
            await session.execute(delete(cls).where(cls.link_id == link_id, cls.binding_generation != row.binding_generation))
        await self._close_deliveries(session, link_id)

    async def _unbind(self, session: AsyncSession, row: TelegramBotLinkRow) -> BotLink:
        owner, account = row.owner_user_id, row.telegram_user_id
        if row.connection_id is not None:
            binding = await session.scalar(select(ChannelConnectionRow).where(ChannelConnectionRow.id == row.connection_id, ChannelConnectionRow.owner_user_id == owner).with_for_update())
            if binding is not None:
                binding.status = "revoked"
                await session.execute(delete(ChannelCredentialRow).where(ChannelCredentialRow.connection_id == binding.id))
        row.binding_generation = _advance(row.binding_generation)
        row.connection_id = row.telegram_user_id = row.chat_id = row.connected_name = None
        row.state, row.bind_mode, row.connect_expires_at, row.updated_at = "waiting_connect", "code", None, _now()
        await session.flush()
        await self._end_codes(session, row.id, owner)
        await self.end_pending(row.id, skip_reason="link-not-connected", session=session)
        if account is not None:
            other = await session.scalar(select(TelegramBotLinkRow.id).where(TelegramBotLinkRow.owner_user_id == owner, TelegramBotLinkRow.telegram_user_id == account, TelegramBotLinkRow.connection_id.is_not(None)).limit(1))
            if other is None:
                await session.execute(delete(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == account, TelegramAccountRow.owner_user_id == owner))
        return _snapshot(row)

    async def unbind(self, link: BotLink, *, actor_id: str, action: str = "disconnect", session: AsyncSession | None = None) -> BotLink:
        if session is None:
            async with self._owner_writer(link.owner_user_id) as writer:
                return await self.unbind(link, actor_id=actor_id, action=action, session=writer)
        row = await self._locked(session, link.id, captured=link)
        result = await self._unbind(session, row)
        await self._event(session, row, action, actor_id=actor_id)
        return result

    async def disconnect_all(self, owner_user_id: str, *, actor_id: str) -> int:
        async with self._owner_writer(owner_user_id) as session:
            links = list(await session.scalars(select(TelegramBotLinkRow).where(TelegramBotLinkRow.owner_user_id == owner_user_id).order_by(TelegramBotLinkRow.id)))
            codes = list(await session.scalars(select(ChannelOAuthStateRow).where(ChannelOAuthStateRow.owner_user_id == owner_user_id, ChannelOAuthStateRow.provider == "telegram", ChannelOAuthStateRow.consumed_at.is_(None))))
            waiting = {code.metadata_json.get("telegram_bot_link_id") for code in codes}
            changed = 0
            for row in links:
                if row.state == "waiting_connect" and row.connection_id is None and row.telegram_user_id is None and row.bind_mode == "code" and row.id not in waiting and row.connect_expires_at is None:
                    continue
                await self._unbind(session, row)
                await self._event(session, row, "disconnect_all", actor_id=actor_id)
                changed += 1
            await session.execute(update(TelegramBotIntentRow).where(TelegramBotIntentRow.owner_user_id == owner_user_id, TelegramBotIntentRow.consumed_at.is_(None), TelegramBotIntentRow.cancelled_at.is_(None)).values(cancelled_at=_now()))
            await session.execute(update(TelegramAssistantAddRow).where(TelegramAssistantAddRow.owner_user_id == owner_user_id, TelegramAssistantAddRow.state.in_(_OPEN_ADDS)).values(state="abandoned", updated_at=_now()))
            return changed

    async def remove(self, link_id: str, *, actor_id: str, action: str = "remove", detail: dict | None = None, session: AsyncSession | None = None) -> None:
        if session is None:
            try:
                owner = await self._owner_of(link_id)
            except ValueError:
                return
            async with self.capacity_writer(owner) as writer:
                await self.remove(link_id, actor_id=actor_id, action=action, detail=detail, session=writer)
                return
        row = await session.get(TelegramBotLinkRow, link_id, populate_existing=True)
        if row is None:
            return
        self._borrow(session, row.owner_user_id, capacity=True)
        await self._unbind(session, row)
        # Unbind clears the current connection pointer but retains revoked
        # identities. Remove all this bot's owned mappings, including earlier
        # accounts, so a later link cannot inherit one of its old threads.
        identities = select(ChannelConnectionRow.id).where(ChannelConnectionRow.owner_user_id == row.owner_user_id, ChannelConnectionRow.provider == "telegram", ChannelConnectionRow.workspace_id == str(row.bot_id))
        await session.execute(delete(ChannelConversationRow).where(ChannelConversationRow.owner_user_id == row.owner_user_id, ChannelConversationRow.connection_id.in_(identities)).execution_options(synchronize_session=False))
        await self._event(session, row, action, actor_id=actor_id, detail=detail)
        await session.delete(row)
        await session.flush()

    async def remove_all_for_owner(self, owner_user_id: str, *, actor_id: str, detail: dict | None = None, session: AsyncSession | None = None) -> list[BotLink]:
        if session is None:
            async with self.capacity_writer(owner_user_id) as writer:
                return await self.remove_all_for_owner(owner_user_id, actor_id=actor_id, detail=detail, session=writer)
        self._borrow(session, owner_user_id, capacity=True)
        links = [_snapshot(row) for row in await session.scalars(select(TelegramBotLinkRow).where(TelegramBotLinkRow.owner_user_id == owner_user_id).order_by(TelegramBotLinkRow.id))]
        for link in links:
            await self.remove(link.id, actor_id=actor_id, detail=detail, session=session)
        await session.execute(update(TelegramBotIntentRow).where(TelegramBotIntentRow.owner_user_id == owner_user_id, TelegramBotIntentRow.consumed_at.is_(None), TelegramBotIntentRow.cancelled_at.is_(None)).values(cancelled_at=_now()))
        await session.execute(update(TelegramAssistantAddRow).where(TelegramAssistantAddRow.owner_user_id == owner_user_id, TelegramAssistantAddRow.state.in_(_OPEN_ADDS)).values(state="abandoned", updated_at=_now()))
        await session.execute(delete(TelegramAccountRow).where(TelegramAccountRow.owner_user_id == owner_user_id))
        return links

    async def removed_for_owner(self, owner_user_id: str) -> list[dict]:
        async with self.session_factory() as session:
            events = await session.scalars(
                select(TelegramBotLinkEventRow)
                .where(TelegramBotLinkEventRow.owner_user_id == owner_user_id, TelegramBotLinkEventRow.action.in_(("managed_given_away", "agent_removed")), TelegramBotLinkEventRow.seen_at.is_(None))
                .order_by(TelegramBotLinkEventRow.created_at.desc(), TelegramBotLinkEventRow.id.desc())
                .limit(10)
            )
            return [
                {
                    "id": row.id,
                    "reason": "given-away" if row.action == "managed_given_away" else "agent-deleted",
                    **{key: row.detail.get(key) for key in ("agent_kind", "agent_ref", "agent_name", "bot_username")},
                    "at": _time(row.created_at),
                }
                for row in events
            ]

    async def mark_seen(self, owner_user_id: str, event_id: str) -> bool:
        async with self._owner_writer(owner_user_id) as session:
            result = await session.execute(
                update(TelegramBotLinkEventRow)
                .where(TelegramBotLinkEventRow.id == event_id, TelegramBotLinkEventRow.owner_user_id == owner_user_id, TelegramBotLinkEventRow.action.in_(("managed_given_away", "agent_removed")), TelegramBotLinkEventRow.seen_at.is_(None))
                .values(seen_at=_now())
            )
            return result.rowcount == 1
