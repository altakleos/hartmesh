"""Durable content identities, immutable routes and publication fences.

Owned writers reserve SQLite before reading and lock the link before inbox rows
on PostgreSQL. Delivery helpers borrow the caller's existing writer transaction;
they neither commit it nor create an independent session. No network work belongs
inside these transactions.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal
from uuid import uuid4

from sqlalchemy import and_, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.telegram_bots.adapter import TOKEN_BODY
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkRow, TelegramInboundMarkRow, TelegramOutboxRow

if TYPE_CHECKING:
    from telegram import Update

    from app.telegram_bots.authority import LiveOwnerCheck
    from app.telegram_bots.links import BotLink, PublicationGuard

_TOKEN = re.compile(TOKEN_BODY)
SERVER_BOOKKEEPING_KEYS = frozenset({"publications", "current_publication_id", "thread_preparation", "publication_recovery", "callback_choice"})
_INPUT_KEYS = {"text", "caption", "files", "voice", "command", "type", "reply_to_message_id", "prompt_id", "media_group_id"}
_UNSAFE_KEYS = {"raw_update", "update", "token", "bot_token", "access_token", "download_url", "file_path", "audio_bytes", "bytes", "_content", "content_bytes"}
_FILE_KEYS = {"type", "file_id", "file_unique_id", "filename", "mime_type", "size", "message_id", "telegram_bot_id", "duration", "width", "height"}
_PUBLICATION_HEX = re.compile(r"[0-9a-f]{32}\Z")
_PUBLICATION_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


def _now() -> datetime:
    return datetime.now(UTC)


def _time(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _topic(value: int | None) -> int | None:
    if value is not None and (type(value) is not int or value <= 0):
        raise ValueError("invalid-inbox-update")
    return None if value in (None, 1) else value


def _identifier(value: str, limit: int, error: str) -> None:
    if not isinstance(value, str) or not value or len(value) > limit or _TOKEN.search(value):
        raise ValueError(error)


def _json_copy(value: dict, error: str) -> dict:
    def safe(item, depth=0):
        if depth > 12:
            return False
        if item is None or type(item) in (bool, int):
            return True
        if type(item) is float:
            return math.isfinite(item)
        if isinstance(item, str):
            return not _TOKEN.search(item)
        if isinstance(item, list):
            return all(safe(child, depth + 1) for child in item)
        return isinstance(item, dict) and all(isinstance(key, str) and key not in _UNSAFE_KEYS and not _TOKEN.search(key) and safe(child, depth + 1) for key, child in item.items())

    if not isinstance(value, dict) or not safe(value):
        raise ValueError(error)
    # Reject non-JSON subclasses/keys rather than letting the SQL serializer
    # decide after other changes have been staged in a borrowed transaction.
    try:
        return json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError, OverflowError):
        raise ValueError(error) from None


def _input(value: dict) -> dict:
    result = _json_copy(value, "unsafe-sanitized-input")
    if set(result) - _INPUT_KEYS:
        raise ValueError("unsafe-sanitized-input")
    if any(not isinstance(result[key], str) for key in ("text", "caption", "command", "type", "prompt_id", "media_group_id") if key in result):
        raise ValueError("unsafe-sanitized-input")
    if "reply_to_message_id" in result and (type(result["reply_to_message_id"]) is not int or result["reply_to_message_id"] <= 0):
        raise ValueError("unsafe-sanitized-input")
    files = result.get("files", [])
    if not isinstance(files, list) or any(not isinstance(item, dict) or set(item) - _FILE_KEYS for item in files):
        raise ValueError("unsafe-sanitized-input")
    if "voice" in result and (not isinstance(result["voice"], dict) or set(result["voice"]) - _FILE_KEYS):
        raise ValueError("unsafe-sanitized-input")

    def descriptors(item):
        if isinstance(item, str):
            return not item.lower().startswith(("http://", "https://", "data:"))
        if isinstance(item, list):
            return all(descriptors(child) for child in item)
        return not isinstance(item, dict) or all(key not in {"url", "audio"} and descriptors(child) for key, child in item.items())

    if any(not descriptors(result[key]) for key in ("files", "voice") if key in result):
        raise ValueError("unsafe-sanitized-input")
    return result


@dataclass(frozen=True)
class InboxRow:
    link_id: str
    message_id: int
    update_id: int
    chat_id: int
    message_thread_id: int | None
    binding_generation: int
    accepted_at: datetime
    route_kind: str | None
    route_target_id: str | None
    route_basis: dict
    sanitized_input: dict
    turn_id: str | None
    grouped_message_ids: list[int]
    thread_id: str | None
    run_id: str | None
    assistant_message_id: str | None
    operation_ids: list[str]
    outbox_id: str | None
    delivery_ids: list[str]
    route_complete: bool
    state: str
    closed_at: datetime | None

    @staticmethod
    def from_row(row: TelegramInboundMarkRow) -> InboxRow:
        """Capture an admitted origin without retaining its writer or ORM row."""
        return _snapshot(row)


def _snapshot(row: TelegramInboundMarkRow) -> InboxRow:
    return InboxRow(**{field.name: _time(value) if isinstance(value := getattr(row, field.name), datetime) else deepcopy(value) for field in fields(InboxRow)})


def _identity(row: InboxRow | TelegramInboundMarkRow) -> tuple[str, int, int]:
    return row.link_id, row.message_id, row.chat_id


def _basis(row: InboxRow | TelegramInboundMarkRow) -> dict:
    return {key: value for key, value in row.route_basis.items() if key not in SERVER_BOOKKEEPING_KEYS}


def _canonical(value: dict) -> str:
    # Python equality equates True and 1, but displayed routing revisions and
    # choices retain their JSON types. Key insertion order carries no meaning.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _publication_basis(row: InboxRow | TelegramInboundMarkRow) -> dict:
    # Each joined input retains its own preparation receipt. Publications are
    # shared by the group and must still compare their complete identities.
    return {key: value for key, value in row.route_basis.items() if key != "thread_preparation"}


def _scope(row: TelegramInboundMarkRow) -> tuple:
    return (
        row.link_id,
        row.chat_id,
        row.message_thread_id,
        row.binding_generation,
        row.route_kind,
        row.route_target_id,
        _canonical(_basis(row)),
        *(row.sanitized_input.get(key) for key in ("reply_to_message_id", "prompt_id", "media_group_id")),
    )


def _publication_text(value: str, limit: int) -> None:
    if type(value) is not str:
        raise ValueError("publication-conflict") from None
    _identifier(value, limit, "publication-conflict")


@dataclass(frozen=True)
class PublicationOrigin:
    link_id: str
    message_id: int
    chat_id: int
    update_id: int
    message_thread_id: int | None
    binding_generation: int

    def __post_init__(self) -> None:
        _publication_text(self.link_id, 32)
        if any(type(value) is not int or value <= 0 for value in (self.message_id, self.chat_id)) or any(type(value) is not int or value < 0 for value in (self.update_id, self.binding_generation)):
            raise ValueError("publication-conflict") from None
        object.__setattr__(self, "message_thread_id", _topic(self.message_thread_id))


@dataclass(frozen=True)
class PublicationScope:
    link_id: str
    owner_user_id: str
    bot_id: int
    agent_kind: Literal["lead_agent", "custom_agent"]
    agent_ref: str
    assistant_id: Literal["lead_agent"]
    agent_name: str | None
    connection_id: str
    telegram_user_id: int
    chat_id: int
    message_thread_id: int | None
    binding_generation: int
    thread_id: str
    endpoint_id: str

    def __post_init__(self) -> None:
        for name, limit in (("link_id", 32), ("owner_user_id", 64), ("agent_ref", 128), ("connection_id", 64), ("thread_id", 64)):
            _publication_text(getattr(self, name), limit)
        if (
            any(type(value) is not int or value <= 0 for value in (self.bot_id, self.telegram_user_id, self.chat_id))
            or self.chat_id != self.telegram_user_id
            or type(self.binding_generation) is not int
            or self.binding_generation < 0
            or self.agent_kind not in {"lead_agent", "custom_agent"}
            or self.assistant_id != "lead_agent"
            or self.agent_kind == "lead_agent"
            and (self.agent_ref != "lead_agent" or self.agent_name is not None)
            or self.agent_kind == "custom_agent"
            and (type(self.agent_name) is not str or self.agent_name != self.agent_ref)
            or type(self.endpoint_id) is not str
            or not _PUBLICATION_DIGEST.fullmatch(self.endpoint_id)
        ):
            raise ValueError("publication-conflict") from None
        object.__setattr__(self, "message_thread_id", _topic(self.message_thread_id))


@dataclass(frozen=True)
class PublicationRecord:
    scope: PublicationScope
    turn_id: str
    publication_id: str
    event_id: str
    origins: tuple[PublicationOrigin, ...]
    input_version: Literal[1]
    input_sha256: str
    dispatch_started: bool
    run_id: str | None

    def __post_init__(self) -> None:
        if type(self.scope) is not PublicationScope:
            raise ValueError("publication-conflict") from None
        _publication_text(self.turn_id, 32)
        _publication_text(self.event_id, 96)
        if (
            type(self.publication_id) is not str
            or not _PUBLICATION_HEX.fullmatch(self.publication_id)
            or self.publication_id != hashlib.sha256((self.turn_id + ":" + self.event_id).encode()).hexdigest()[:32]
            or type(self.origins) is not tuple
            or not self.origins
            or any(type(origin) is not PublicationOrigin for origin in self.origins)
            or self.origins != tuple(sorted(set(self.origins), key=lambda origin: (origin.link_id, origin.message_id, origin.chat_id)))
            or any((origin.link_id, origin.chat_id, origin.message_thread_id, origin.binding_generation) != (self.scope.link_id, self.scope.chat_id, self.scope.message_thread_id, self.scope.binding_generation) for origin in self.origins)
            or type(self.input_version) is not int
            or self.input_version != 1
            or type(self.input_sha256) is not str
            or not _PUBLICATION_DIGEST.fullmatch(self.input_sha256)
            or type(self.dispatch_started) is not bool
        ):
            raise ValueError("publication-conflict") from None
        if self.run_id is not None:
            _publication_text(self.run_id, 64)
            if not self.dispatch_started:
                raise ValueError("publication-conflict") from None


def _publication_identity(record: PublicationRecord) -> dict:
    if type(record) is not PublicationRecord:
        raise ValueError("publication-conflict") from None
    value = asdict(record)
    value["origins"] = list(value["origins"])
    value.pop("dispatch_started")
    value.pop("run_id")
    return value


def publication_identity(record: PublicationRecord) -> dict:
    """Detached immutable identity; marker/run remain stored observations."""
    return _publication_identity(record)


@dataclass(frozen=True)
class RestartClosure:
    version: Literal[1]
    kind: Literal["restart_before_dispatch"]
    startup_id: str
    publication_id: str
    input_version: Literal[1]
    input_sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.version) is not int
            or self.version != 1
            or self.kind != "restart_before_dispatch"
            or type(self.input_version) is not int
            or self.input_version != 1
            or any(type(value) is not str or not _PUBLICATION_HEX.fullmatch(value) for value in (self.startup_id, self.publication_id))
            or type(self.input_sha256) is not str
            or not _PUBLICATION_DIGEST.fullmatch(self.input_sha256)
        ):
            raise ValueError("publication-recovery-conflict") from None

    @classmethod
    def from_json(cls, value) -> RestartClosure:
        try:
            if type(value) is not dict or set(value) != {field.name for field in fields(cls)}:
                raise ValueError
            return cls(**value)
        except (TypeError, ValueError):
            raise ValueError("publication-recovery-conflict") from None


@dataclass(frozen=True)
class RestartReceipt:
    record: PublicationRecord
    closure: RestartClosure
    outbox_id: str


def _restart_closure(record: PublicationRecord, rows: Sequence[InboxRow | TelegramInboundMarkRow]) -> RestartClosure | None:
    present = ["publication_recovery" in row.route_basis for row in rows]
    if not any(present):
        return None
    fact = RestartClosure.from_json(rows[0].route_basis.get("publication_recovery"))
    if (
        not all(present)
        or any(_canonical(row.route_basis["publication_recovery"]) != _canonical(asdict(fact)) for row in rows)
        or (fact.publication_id, fact.input_version, fact.input_sha256) != (record.publication_id, record.input_version, record.input_sha256)
        or record.dispatch_started is not False
        or record.run_id is not None
        or not rows[0].outbox_id
        or any(
            row.outbox_id != rows[0].outbox_id or not row.route_complete or row.run_id is not None or row.assistant_message_id is not None or row.state not in {"reply_pending", "done"} or rows[0].outbox_id not in row.delivery_ids
            for row in rows
        )
    ):
        raise ValueError("publication-recovery-conflict") from None
    return fact


RecoveryHold = Literal["legacy", "invalid_group", "incomplete_group", "group_too_large", "unsupported_input", "stale_scope"]


@dataclass(frozen=True)
class RecoveryCursor:
    link_id: str
    turn_id: str

    def __post_init__(self) -> None:
        # These are stored SQL bookmarks, not publication authority. Corrupt
        # empty/control turn IDs still need to advance to a later valid head.
        if any(type(value) is not str for value in (self.link_id, self.turn_id)):
            raise ValueError("invalid-recovery-cursor") from None


@dataclass(frozen=True)
class RecoveryEntry:
    key: RecoveryCursor
    record: PublicationRecord | None
    origins: tuple[InboxRow, ...]
    hold_reason: RecoveryHold | None


@dataclass(frozen=True)
class RecoveryPage:
    entries: tuple[RecoveryEntry, ...]
    next_after: RecoveryCursor | None
    has_more: bool


def _recovery_plain_input(rows: Sequence[InboxRow | TelegramInboundMarkRow]) -> bool:
    for row in rows:
        profile = row.route_basis.get("input_profile")
        value = row.sanitized_input
        if (
            type(profile) is not dict
            or set(profile) != {"version", "kind", "forwarded"}
            or type(profile["version"]) is not int
            or profile["version"] != 1
            or profile["kind"] != "plain_text"
            or profile["forwarded"] is not False
            or type(value) is not dict
            or set(value) != {"type", "text"}
            or value["type"] != "text"
            or type(value["text"]) is not str
            or not value["text"].strip()
        ):
            return False
    return True


def _publication_origin(row: InboxRow | TelegramInboundMarkRow) -> PublicationOrigin:
    return PublicationOrigin(**{field.name: getattr(row, field.name) for field in fields(PublicationOrigin)})


def _publication_input(rows: Sequence[InboxRow | TelegramInboundMarkRow]) -> str:
    assembly = []
    for row in sorted(rows, key=lambda mark: (_time(mark.accepted_at), mark.update_id, mark.message_id)):
        stamp = _time(row.accepted_at)
        if not isinstance(stamp, datetime):
            raise ValueError("publication-conflict") from None
        assembly.append(
            {
                "origin": asdict(_publication_origin(row)),
                "accepted_at": stamp.isoformat(timespec="microseconds").replace("+00:00", "Z"),
                "sanitized_input": _input(row.sanitized_input),
                "route_kind": row.route_kind,
                "route_target_id": row.route_target_id,
                "operation_ids": row.operation_ids,
                "route_basis": _basis(row),
            }
        )
    try:
        content = json.dumps({"version": 1, "assembly": assembly}, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise ValueError("publication-conflict") from None
    return hashlib.sha256(content).hexdigest()


class InboxRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self.session_factory = session_factory

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        async with self.session_factory() as session, session.begin():
            if session.get_bind().dialect.name == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            yield session

    @staticmethod
    def _borrow(session: AsyncSession) -> None:
        if not session.in_transaction():
            raise ValueError("Telegram inbox helper requires an existing writer transaction")

    @staticmethod
    async def _link(session: AsyncSession, link_id: str) -> TelegramBotLinkRow:
        link = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link_id).with_for_update().execution_options(populate_existing=True))
        if link is None:
            raise ValueError("link-changed")
        return link

    @staticmethod
    def _current(link: TelegramBotLinkRow, row: InboxRow | TelegramInboundMarkRow) -> None:
        if link.state != "connected" or link.binding_generation != row.binding_generation or link.chat_id != row.chat_id or link.telegram_user_id != row.chat_id or link.connection_id is None:
            raise ValueError("link-changed")

    @staticmethod
    def _requested(rows: Sequence[InboxRow]) -> str:
        if not rows or any(not isinstance(row, InboxRow) for row in rows) or len({_identity(row) for row in rows}) != len(rows) or len({row.link_id for row in rows}) != 1:
            raise ValueError("inbox-group-conflict")
        return rows[0].link_id

    @staticmethod
    async def _rows(session: AsyncSession, rows: Sequence[InboxRow]) -> list[TelegramInboundMarkRow]:
        saved = list(
            await session.scalars(
                select(TelegramInboundMarkRow)
                .where(or_(*(and_(TelegramInboundMarkRow.link_id == row.link_id, TelegramInboundMarkRow.message_id == row.message_id, TelegramInboundMarkRow.chat_id == row.chat_id) for row in rows)))
                .order_by(TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        captured = {_identity(row): row for row in rows}
        if len(saved) != len(captured) or any(
            (row.update_id, row.binding_generation, row.message_thread_id) != (captured[_identity(row)].update_id, captured[_identity(row)].binding_generation, captured[_identity(row)].message_thread_id) for row in saved
        ):
            raise ValueError("inbox-identity-conflict")
        return saved

    @staticmethod
    def _group(rows: Sequence[TelegramInboundMarkRow]) -> None:
        if any(row.route_kind is None or _scope(row) != _scope(rows[0]) for row in rows):
            raise ValueError("inbox-group-conflict")

    @staticmethod
    async def _turn(session: AsyncSession, turn_id: str, *, limit: int | None = None) -> list[TelegramInboundMarkRow]:
        return list(
            await session.scalars(
                select(TelegramInboundMarkRow)
                .where(TelegramInboundMarkRow.turn_id == turn_id)
                .limit(limit)
                .order_by(TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )

    def _publication_borrow(self, session: AsyncSession) -> None:
        bind = self.session_factory.kw.get("bind")
        if bind is None or not session.in_transaction() or not session.is_active or session.get_bind() is not getattr(bind, "sync_engine", bind):
            raise ValueError("publication-writer-required") from None

    @staticmethod
    def _publication_scope(link: BotLink | TelegramBotLinkRow, *, topic: int | None, thread_id: str, endpoint_id: str) -> PublicationScope:
        return PublicationScope(
            link_id=link.id,
            owner_user_id=link.owner_user_id,
            bot_id=link.bot_id,
            agent_kind=link.agent_kind,
            agent_ref=link.agent_ref,
            assistant_id="lead_agent",
            agent_name=link.agent_ref if link.agent_kind == "custom_agent" else None,
            connection_id=link.connection_id,
            telegram_user_id=link.telegram_user_id,
            chat_id=link.chat_id,
            message_thread_id=topic,
            binding_generation=link.binding_generation,
            thread_id=thread_id,
            endpoint_id=endpoint_id,
        )

    @staticmethod
    def _publication_guard_matches(guard: PublicationGuard, *, link: BotLink | None = None, scope: PublicationScope | None = None) -> None:
        try:
            captured = guard.captured_link
            if link is not None:
                names = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
                if any(getattr(captured, name) != getattr(link, name) for name in names):
                    raise ValueError
            if scope is not None and InboxRepository._publication_scope(captured, topic=scope.message_thread_id, thread_id=scope.thread_id, endpoint_id=scope.endpoint_id) != scope:
                raise ValueError
        except (AttributeError, TypeError, ValueError):
            raise ValueError("publication-stale") from None

    @staticmethod
    def _publication_snapshot_matches(saved: TelegramInboundMarkRow, captured: InboxRow) -> None:
        try:
            same = (
                _publication_origin(saved) == _publication_origin(captured)
                and _time(saved.accepted_at) == _time(captured.accepted_at)
                and (saved.route_kind, saved.route_target_id) == (captured.route_kind, captured.route_target_id)
                and _canonical(_basis(saved)) == _canonical(_basis(captured))
                and _canonical({"input": _input(saved.sanitized_input), "operations": saved.operation_ids}) == _canonical({"input": _input(captured.sanitized_input), "operations": captured.operation_ids})
            )
        except (AttributeError, TypeError, ValueError):
            raise ValueError("publication-conflict") from None
        if not same:
            raise ValueError("publication-conflict") from None

    @staticmethod
    def _publication_record(marks: Sequence[TelegramInboundMarkRow]) -> PublicationRecord:
        """Parse a complete locked group and recompute its saved input proof."""
        try:
            if not marks or marks[0].turn_id is None or marks[0].thread_id is None:
                raise ValueError
            InboxRepository._group(marks)
            first = marks[0]
            if first.route_kind != "turn" or any((mark.turn_id, mark.thread_id) != (first.turn_id, first.thread_id) for mark in marks):
                raise ValueError
            if any(_canonical(_publication_basis(mark)) != _canonical(_publication_basis(first)) for mark in marks):
                raise ValueError
            ordered = sorted(marks, key=lambda mark: (_time(mark.accepted_at), mark.update_id, mark.message_id))
            if any(_canonical({"group": mark.grouped_message_ids}) != _canonical({"group": [other.message_id for other in ordered if other is not mark]}) for mark in marks):
                raise ValueError
            publications = first.route_basis["publications"]
            current = first.route_basis["current_publication_id"]
            if type(publications) is not list or not publications or current != publications[-1]["publication_id"]:
                raise ValueError
            seen_publications, seen_events = set(), set()
            for item in publications:
                if type(item) is not dict or set(item) not in ({"publication_id", "event_id", "run_id"}, {"publication_id", "event_id", "run_id", "publication_execution"}):
                    raise ValueError
                _publication_text(item["event_id"], 96)
                if type(item["publication_id"]) is not str or item["publication_id"] != InboxRepository._publication(first.turn_id, item["event_id"]) or item["publication_id"] in seen_publications or item["event_id"] in seen_events:
                    raise ValueError
                seen_publications.add(item["publication_id"])
                seen_events.add(item["event_id"])
                if item["run_id"] is not None:
                    _publication_text(item["run_id"], 64)
            entry = publications[-1]
            execution = entry["publication_execution"]
            if type(execution) is not dict or set(execution) != {"version", "scope", "origins", "input_version", "input_sha256", "dispatch_started"} or type(execution["version"]) is not int or execution["version"] != 1:
                raise ValueError
            if type(execution["scope"]) is not dict or set(execution["scope"]) != {field.name for field in fields(PublicationScope)} or type(execution["origins"]) is not list:
                raise ValueError
            scope = PublicationScope(**execution["scope"])
            origins = []
            for origin in execution["origins"]:
                if type(origin) is not dict or set(origin) != {field.name for field in fields(PublicationOrigin)}:
                    raise ValueError
                origins.append(PublicationOrigin(**origin))
            record = PublicationRecord(scope, first.turn_id, entry["publication_id"], entry["event_id"], tuple(origins), execution["input_version"], execution["input_sha256"], execution["dispatch_started"], entry["run_id"])
            if (
                tuple(_publication_origin(mark) for mark in marks) != record.origins
                or first.thread_id != scope.thread_id
                or first.route_target_id != scope.agent_ref
                or any(mark.run_id != record.run_id for mark in marks)
                or _publication_input(marks) != record.input_sha256
            ):
                raise ValueError
            _restart_closure(record, marks)
            return record
        except (AttributeError, KeyError, TypeError, ValueError, UnicodeError):
            # Missing execution receipts are uncertainty, never a false marker
            # from which a legacy publication can be dispatched again.
            raise ValueError("publication-conflict") from None

    async def _locked_publication(self, record: PublicationRecord, session: AsyncSession, *, max_origins: int | None = None) -> tuple[PublicationRecord, list[TelegramInboundMarkRow]]:
        self._publication_borrow(session)
        identity = _publication_identity(record)
        link = await self._link(session, record.scope.link_id)
        actual_scope = self._publication_scope(link, topic=record.scope.message_thread_id, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id)
        if actual_scope != record.scope:
            raise ValueError("publication-stale") from None
        marks = await self._turn(session, record.turn_id, limit=None if max_origins is None else max_origins + 1)
        if max_origins is not None and len(marks) > max_origins:
            raise ValueError("publication-conflict") from None
        fresh = self._publication_record(marks)
        if _canonical(_publication_identity(fresh)) != _canonical(identity):
            raise ValueError("publication-conflict") from None
        return fresh, marks

    async def reserve_publication(self, captured_link: BotLink, rows: Sequence[InboxRow], *, thread_id: str, endpoint_id: str, guard: PublicationGuard) -> PublicationRecord:
        """Atomically fix the first managed identity; never adopt legacy intent."""
        from deerflow.utils.file_io import await_drained

        captured_link, rows = deepcopy(captured_link), tuple(deepcopy(rows))
        link_id = self._requested(rows)
        self._publication_guard_matches(guard, link=captured_link)
        if captured_link.id != link_id:
            raise ValueError("publication-conflict") from None
        expected = self._publication_scope(captured_link, topic=rows[0].message_thread_id, thread_id=thread_id, endpoint_id=endpoint_id)

        async def perform() -> PublicationRecord:
            from app.telegram_bots.links import prepare_publication_guard

            await prepare_publication_guard(guard)
            async with self._transaction() as session:
                checked = await guard.check(session)
                if self._publication_scope(checked, topic=expected.message_thread_id, thread_id=thread_id, endpoint_id=endpoint_id) != expected:
                    raise ValueError("publication-stale") from None
                marks = await self._rows(session, rows)
                self._group(marks)
                for mark, captured in zip(marks, sorted(rows, key=_identity), strict=True):
                    self._publication_snapshot_matches(mark, captured)
                    if "publication_recovery" in mark.route_basis or mark.route_kind != "turn" or mark.route_target_id != captured_link.agent_ref:
                        raise ValueError("publication-conflict") from None
                    self._current(checked, mark)
                if any(mark.turn_id is not None for mark in marks):
                    turns = {mark.turn_id for mark in marks}
                    if len(turns) != 1 or None in turns:
                        raise ValueError("publication-conflict") from None
                    complete = await self._turn(session, marks[0].turn_id)
                    if {_identity(mark) for mark in complete} != {_identity(mark) for mark in marks}:
                        raise ValueError("publication-conflict") from None
                    record = self._publication_record(complete)
                    if record.scope != expected:
                        raise ValueError("publication-conflict") from None
                else:
                    if any(
                        mark.thread_id is not None or mark.run_id is not None or mark.assistant_message_id is not None or mark.route_complete or mark.state not in {"accepted", "reply_pending"} or mark.grouped_message_ids for mark in marks
                    ):
                        raise ValueError("publication-conflict") from None
                    if any({"publications", "current_publication_id"}.intersection(mark.route_basis) for mark in marks):
                        raise ValueError("publication-conflict") from None
                    turn_id = uuid4().hex
                    if session.get_bind().dialect.name == "postgresql":
                        key = int.from_bytes(hashlib.sha256(("telegram-inbox-turn:" + turn_id).encode()).digest()[:8], signed=True)
                        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
                    if await self._turn(session, turn_id):
                        raise ValueError("publication-conflict") from None
                    ordered = sorted(marks, key=lambda mark: (_time(mark.accepted_at), mark.update_id, mark.message_id))
                    event_id = f"m{ordered[0].message_id}"
                    record = PublicationRecord(expected, turn_id, self._publication(turn_id, event_id), event_id, tuple(_publication_origin(mark) for mark in marks), 1, _publication_input(marks), False, None)
                    execution = {"version": 1, "scope": asdict(record.scope), "origins": [asdict(origin) for origin in record.origins], "input_version": 1, "input_sha256": record.input_sha256, "dispatch_started": False}
                    history = [{"publication_id": record.publication_id, "event_id": event_id, "run_id": None, "publication_execution": execution}]
                    for mark in marks:
                        mark.turn_id, mark.thread_id, mark.state = turn_id, thread_id, "dispatched"
                        mark.grouped_message_ids = [other.message_id for other in ordered if other is not mark]
                        mark.route_basis = {**deepcopy(mark.route_basis), "publications": deepcopy(history), "current_publication_id": record.publication_id}
                await session.flush()
                await guard.check(session)
                return record

        return await await_drained(perform())

    async def mark_dispatch_started(self, record: PublicationRecord, *, guard: PublicationGuard) -> bool:
        """One irreversible winner; an ambiguous committed result is never reset."""
        from deerflow.utils.file_io import await_drained

        self._publication_guard_matches(guard, scope=record.scope)

        async def perform() -> bool:
            from app.telegram_bots.links import prepare_publication_guard

            await prepare_publication_guard(guard)
            async with self._transaction() as session:
                await guard.check(session)
                fresh, marks = await self._locked_publication(record, session)
                if any("publication_recovery" in mark.route_basis for mark in marks):
                    raise ValueError("publication-conflict") from None
                changed = not fresh.dispatch_started
                if changed:
                    if any(mark.route_complete or mark.state == "interrupted" for mark in marks):
                        raise ValueError("publication-conflict") from None
                    for mark in marks:
                        basis = deepcopy(mark.route_basis)
                        basis["publications"][-1]["publication_execution"]["dispatch_started"] = True
                        mark.route_basis = basis
                await session.flush()
                await guard.check(session)
                return changed

        return await await_drained(perform())

    async def bind_publication_run(self, record: PublicationRecord, *, run_id: str, guard: PublicationGuard) -> PublicationRecord:
        """Save only this marked publication's first exact run; HTTP proof is external."""
        from deerflow.utils.file_io import await_drained

        _publication_text(run_id, 64)
        self._publication_guard_matches(guard, scope=record.scope)

        async def perform() -> PublicationRecord:
            from app.telegram_bots.links import prepare_publication_guard

            await prepare_publication_guard(guard)
            async with self._transaction() as session:
                await guard.check(session)
                fresh, marks = await self._locked_publication(record, session)
                if any("publication_recovery" in mark.route_basis for mark in marks):
                    raise ValueError("publication-conflict") from None
                if not fresh.dispatch_started or fresh.run_id not in (None, run_id) or fresh.run_id is None and any(mark.route_complete or mark.state == "interrupted" for mark in marks):
                    raise ValueError("publication-conflict") from None
                for mark in marks:
                    basis = deepcopy(mark.route_basis)
                    basis["publications"][-1]["run_id"] = run_id
                    mark.run_id, mark.route_basis = run_id, basis
                await session.flush()
                result = self._publication_record(marks)
                await guard.check(session)
                return result

        return await await_drained(perform())

    async def lock_restart_publication(self, record: PublicationRecord, *, session: AsyncSession) -> tuple[PublicationRecord, tuple[InboxRow, ...]]:
        """Borrow link/outbox ordering and decode at most the complete-group sentinel."""
        from deerflow.utils.file_io import await_drained

        async def perform():
            fresh, marks = await self._locked_publication(record, session, max_origins=64)
            return fresh, tuple(_snapshot(mark) for mark in marks)

        return await await_drained(perform())

    async def seal_restart_before_dispatch(self, record: PublicationRecord, *, outbox_id: str, ticket, session: AsyncSession) -> RestartClosure:
        """Atomically attach one fixed primary and its actual startup closure."""
        from app.telegram_bots.recovery_authority import check_restart_ticket
        from deerflow.utils.file_io import await_drained

        async def perform():
            check_restart_ticket(ticket, record)
            fresh, marks = await self._locked_publication(record, session, max_origins=64)
            if fresh.dispatch_started or fresh.run_id is not None or not _recovery_plain_input(marks):
                raise ValueError("publication-recovery-conflict") from None
            fact = RestartClosure(1, "restart_before_dispatch", ticket.startup_id, fresh.publication_id, fresh.input_version, fresh.input_sha256)
            delivery = await session.get(TelegramOutboxRow, outbox_id, populate_existing=True)
            if delivery is None:
                raise ValueError("publication-recovery-conflict") from None

            def validate():
                from app.telegram_bots.producer import _item, validate_restart_reply_basis

                return validate_restart_reply_basis(_item(delivery))

            proof = await await_drained(asyncio.to_thread(validate))
            if publication_identity(proof.record) != publication_identity(fresh) or proof.closure != fact:
                raise ValueError("publication-recovery-conflict") from None
            saved = _restart_closure(fresh, marks)
            if saved is not None:
                if saved != fact or any(mark.outbox_id != outbox_id for mark in marks):
                    raise ValueError("publication-recovery-conflict") from None
            else:
                if any(mark.route_complete or mark.outbox_id is not None or mark.assistant_message_id is not None or mark.state != "dispatched" for mark in marks):
                    raise ValueError("publication-recovery-conflict") from None
                await self.register_delivery(tuple(_snapshot(mark) for mark in marks), outbox_id=outbox_id, primary=True, finish=True, session=session)
                for mark in marks:
                    mark.route_basis = {**deepcopy(mark.route_basis), "publication_recovery": asdict(fact)}
            await session.flush()
            check_restart_ticket(ticket, fresh)
            return fact

        return await await_drained(perform())

    async def read_restart_receipt(self, record: PublicationRecord, *, captured_link: BotLink, check_live_owner: LiveOwnerCheck, check_current: Callable[[], bool]) -> RestartReceipt | None:
        """Fresh same-engine metadata custody; this read grants no write or send."""
        from app.telegram_bots.links import BotLink
        from deerflow.utils.file_io import await_drained

        def current():
            try:
                if check_current() is not True:
                    raise ValueError
            except Exception:
                raise ValueError("publication-stale") from None

        captured_link = deepcopy(captured_link)
        if type(captured_link) is not BotLink or not callable(check_live_owner) or not callable(check_current):
            raise ValueError("publication-stale") from None
        current()

        async def custody(reader):
            link = await reader.get(TelegramBotLinkRow, captured_link.id, populate_existing=True)
            names = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
            if (
                link is None
                or link.state != "connected"
                or link.health != "ok"
                or captured_link.state != "connected"
                or any(getattr(link, name) != getattr(captured_link, name) for name in names)
                or self._publication_scope(link, topic=record.scope.message_thread_id, thread_id=record.scope.thread_id, endpoint_id=record.scope.endpoint_id) != record.scope
            ):
                raise ValueError("publication-stale") from None
            account = await reader.get(TelegramAccountRow, captured_link.telegram_user_id, populate_existing=True)
            connection = await reader.get(ChannelConnectionRow, captured_link.connection_id, populate_existing=True)
            if (
                account is None
                or account.owner_user_id != captured_link.owner_user_id
                or connection is None
                or connection.status != "connected"
                or connection.owner_user_id != captured_link.owner_user_id
                or connection.provider != "telegram"
                or connection.external_account_id != str(captured_link.telegram_user_id)
                or connection.workspace_id != str(captured_link.bot_id)
                or connection.bot_user_id != str(captured_link.bot_id)
            ):
                raise ValueError("publication-stale") from None

        async def perform():
            current()
            async with self.session_factory() as reader, reader.begin():
                await custody(reader)
                rows = list(
                    await reader.scalars(
                        select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == record.turn_id).order_by(TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id).limit(65)
                    )
                )
                if len(rows) > 64:
                    raise ValueError("publication-recovery-conflict") from None
                detached = tuple(_snapshot(row) for row in rows)
                delivery = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == record.scope.link_id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id))
                from app.telegram_bots.producer import _item

                item = None if delivery is None else _item(delivery)

                def decode():
                    from app.telegram_bots.producer import validate_restart_reply_basis

                    fresh = self._publication_record(detached)
                    if _canonical(publication_identity(fresh)) != _canonical(publication_identity(record)):
                        raise ValueError("publication-recovery-conflict") from None
                    fact = _restart_closure(fresh, detached)
                    source = None if item is None else item.payload.get("source")
                    basis = None if item is None else item.payload.get("reply_basis")
                    declared = (type(source) is dict and source.get("kind") == "restart_before_dispatch") or (type(basis) is dict and type(basis.get("version")) is int and basis["version"] == 3)
                    if fact is None:
                        if declared:
                            raise ValueError("publication-recovery-conflict") from None
                        return None
                    if item is None:
                        raise ValueError("publication-recovery-conflict") from None
                    proof = validate_restart_reply_basis(item)
                    if proof.closure != fact or _canonical(publication_identity(proof.record)) != _canonical(publication_identity(fresh)) or any(row.outbox_id != item.id for row in detached):
                        raise ValueError("publication-recovery-conflict") from None
                    return RestartReceipt(fresh, fact, item.id)

                result = await await_drained(asyncio.to_thread(decode))
                current()
                await custody(reader)
                await check_live_owner(captured_link.owner_user_id, reader)
                current()
            current()
            return result

        result = await await_drained(perform())
        current()
        return result

    async def lock_publication(self, record: PublicationRecord, *, session: AsyncSession) -> tuple[PublicationRecord, tuple[InboxRow, ...]]:
        """Borrow the planner's already ordered writer and return no ORM rows."""
        from deerflow.utils.file_io import await_drained

        async def perform():
            fresh, marks = await self._locked_publication(record, session)
            return fresh, tuple(_snapshot(mark) for mark in marks)

        return await await_drained(perform())

    async def seal_publication(self, record: PublicationRecord, *, outbox_id: str, assistant_message_id: str | None, session: AsyncSession) -> None:
        """Attach the exact shared primary, retaining the caller's transaction."""
        from deerflow.utils.file_io import await_drained

        _publication_text(outbox_id, 32)
        if assistant_message_id is not None and (type(assistant_message_id) is not str or not assistant_message_id):
            raise ValueError("publication-conflict") from None

        async def perform():
            fresh, marks = await self._locked_publication(record, session)
            delivery = await session.get(TelegramOutboxRow, outbox_id, populate_existing=True)
            if delivery is None or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.dedupe_key, delivery.class_) != (
                fresh.scope.link_id,
                fresh.scope.owner_user_id,
                fresh.scope.binding_generation,
                fresh.scope.chat_id,
                f"reply:{fresh.turn_id}",
                "reply",
            ):
                raise ValueError("publication-conflict") from None
            try:
                basis = delivery.payload["reply_basis"]
                if type(basis) is not dict or type(basis["version"]) is not int or basis["version"] not in {1, 2} or basis["finish_route"] is not True or _canonical(basis["publication"]) != _canonical(_publication_identity(fresh)):
                    raise ValueError
                outcome = basis["outcome"]
                kind = outcome["kind"]
                if basis["version"] == 2:

                    def validate_verified():
                        from app.telegram_bots.model_source import validate_model_reply_basis
                        from app.telegram_bots.producer import _item

                        return validate_model_reply_basis(_item(delivery)).output.output.model_dump(mode="json")

                    projection = await await_drained(asyncio.to_thread(validate_verified))
                    if (
                        kind != "model_text"
                        or not fresh.dispatch_started
                        or fresh.run_id is None
                        or projection["run_id"] != fresh.run_id
                        or projection["output_evidence"]["assistant_message_id"] != assistant_message_id
                        or assistant_message_id is None
                    ):
                        raise ValueError
                elif kind == "model_text":
                    projection = outcome["projection"]
                    if not fresh.dispatch_started or fresh.run_id is None or projection["run_id"] != fresh.run_id or projection["output_evidence"]["assistant_message_id"] != assistant_message_id or assistant_message_id is None:
                        raise ValueError
                elif kind in {"sealed_failed", "sealed_cancelled"}:
                    if assistant_message_id is not None or not fresh.dispatch_started or fresh.run_id is None or outcome["projection"]["run_id"] != fresh.run_id:
                        raise ValueError
                else:
                    raise ValueError
            except (AttributeError, KeyError, TypeError, ValueError):
                raise ValueError("publication-conflict") from None
            for mark in marks:
                if mark.state == "interrupted" or mark.assistant_message_id not in (None, assistant_message_id):
                    raise ValueError("publication-conflict") from None
            await self.register_delivery(tuple(_snapshot(mark) for mark in marks), outbox_id=outbox_id, primary=True, finish=True, session=session)
            for mark in marks:
                mark.assistant_message_id = assistant_message_id
            await session.flush()

        await await_drained(perform())

    async def accept(self, link: BotLink, update: Update, *, sanitized_input: dict) -> tuple[InboxRow, bool]:
        message = update.message
        if message is None or update.edited_message is not None or message.chat.type != "private" or message.from_user is None or message.from_user.is_bot:
            raise ValueError("invalid-inbox-update")
        if any(type(value) is not int or value <= 0 for value in (message.message_id, message.chat_id, message.from_user.id)) or type(update.update_id) is not int or update.update_id < 0:
            raise ValueError("invalid-inbox-update")
        if message.from_user.id != message.chat_id:
            raise ValueError("invalid-inbox-update")
        topic = _topic(message.message_thread_id)
        payload = _input(sanitized_input)
        # Keep safe grouping identities even when the caller only needs text
        # for an explicit retry. Never retain the Update or a nested Message.
        for key, value in (("reply_to_message_id", message.reply_to_message.message_id if message.reply_to_message else None), ("media_group_id", message.media_group_id)):
            if key in payload and payload[key] != value:
                raise ValueError("unsafe-sanitized-input")
            if value is not None:
                payload[key] = value
        async with self._transaction() as session:
            current = await self._link(session, link.id)
            saved = list(
                await session.scalars(
                    select(TelegramInboundMarkRow)
                    .where(
                        TelegramInboundMarkRow.link_id == link.id, or_(and_(TelegramInboundMarkRow.message_id == message.message_id, TelegramInboundMarkRow.chat_id == message.chat_id), TelegramInboundMarkRow.update_id == update.update_id)
                    )
                    .order_by(TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id)
                    .with_for_update()
                )
            )
            if saved:
                if len(saved) != 1 or (saved[0].message_id, saved[0].update_id, saved[0].chat_id, saved[0].message_thread_id) != (message.message_id, update.update_id, message.chat_id, topic):
                    raise ValueError("inbox-identity-conflict")
                return _snapshot(saved[0]), False
            if any(getattr(current, name) != getattr(link, name) for name in ("owner_user_id", "bot_id", "binding_generation", "token_revision", "connection_id", "telegram_user_id", "chat_id")):
                raise ValueError("link-changed")
            if current.state != "connected" or current.telegram_user_id != message.from_user.id or current.chat_id != message.chat_id or message.from_user.id != message.chat_id:
                raise ValueError("link-changed")
            account = await session.scalar(select(TelegramAccountRow).where(TelegramAccountRow.telegram_user_id == message.from_user.id).with_for_update())
            binding = await session.scalar(select(ChannelConnectionRow).where(ChannelConnectionRow.id == current.connection_id).with_for_update()) if current.connection_id else None
            if (
                account is None
                or account.owner_user_id != current.owner_user_id
                or binding is None
                or binding.status != "connected"
                or binding.owner_user_id != current.owner_user_id
                or binding.provider != "telegram"
                or binding.external_account_id != str(message.from_user.id)
                or binding.workspace_id != str(current.bot_id)
                or binding.bot_user_id != str(current.bot_id)
            ):
                raise ValueError("link-changed")
            mark = TelegramInboundMarkRow(
                link_id=current.id,
                message_id=message.message_id,
                update_id=update.update_id,
                chat_id=message.chat_id,
                message_thread_id=topic,
                binding_generation=current.binding_generation,
                accepted_at=_now(),
                sanitized_input=payload,
                state="accepted",
            )
            session.add(mark)
            await session.flush()
            return _snapshot(mark), True

    async def claim_callback_choice(
        self, link: BotLink, origin: InboxRow, *, callback_id: str, chat_id: int, message_id: int, message_thread_id: int | None, callbacks, guard: PublicationGuard, session: AsyncSession | None = None
    ) -> InboxRow:
        """Fix one /new choice before HTTP; completion remains its caller's job."""
        from app.telegram_bots.callbacks import CALLBACK_METADATA_KEYS
        from app.telegram_bots.links import prepare_publication_guard
        from deerflow.utils.file_io import await_drained

        if not isinstance(origin, InboxRow) or origin.link_id != link.id or origin.message_thread_id is not None or _topic(message_thread_id) is not None:
            raise ValueError("callback-choice-conflict") from None
        self._publication_guard_matches(guard, link=link)
        if session is not None:
            self._publication_borrow(session)

        async def claim(writer):
            group = await callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=guard, session=writer)
            parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id)
            mark = (await self._rows(writer, [origin]))[0]
            self._publication_snapshot_matches(mark, origin)
            self._current(link, mark)
            basis = _basis(mark)
            prep = mark.route_basis.get("thread_preparation")
            scope = {"link_id": link.id, **{key: getattr(link, key) for key in ("owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation")}}
            if (
                mark.route_kind != "command"
                or mark.route_target_id != "new"
                or mark.route_complete
                or mark.state not in {"accepted", "reply_pending"}
                or mark.turn_id is not None
                or mark.thread_id is not None
                or mark.run_id is not None
                or mark.assistant_message_id is not None
                or mark.grouped_message_ids
                or basis.get("key") != "new.confirm_plain"
                or basis.get("keyboard_template") != "new.confirm_plain"
                or type(basis.get("values")) is not dict
                or type(prep) is not dict
                or set(prep) != {"purpose", "creation_id", "endpoint_id", "scope", "expected_mapping_id", "expected_thread_id", "selected_thread_id", "mapping_id"}
                or prep["purpose"] != "new"
                or type(prep["creation_id"]) is not str
                or not _PUBLICATION_HEX.fullmatch(prep["creation_id"])
                or type(prep["endpoint_id"]) is not str
                or not _PUBLICATION_DIGEST.fullmatch(prep["endpoint_id"])
                or _canonical(prep["scope"]) != _canonical(scope)
                or (prep["expected_mapping_id"] is None) != (prep["expected_thread_id"] is None)
                or any(type(prep[key]) is not str or not prep[key] for key in ("expected_mapping_id", "expected_thread_id") if prep[key] is not None)
                or (prep["selected_thread_id"] is None) != (prep["mapping_id"] is None)
                or prep["selected_thread_id"] is not None
                and (prep["selected_thread_id"] != prep["creation_id"] or type(prep["mapping_id"]) is not str or not prep["mapping_id"])
            ):
                raise ValueError("callback-choice-conflict") from None
            expected = {
                "origins": [[mark.link_id, mark.message_id, mark.chat_id]],
                "chat_id": link.chat_id,
                "message_thread_id": None,
                "finish_route": False,
                "parts": [{"kind": "text", "source_ref": {"key": "new.confirm_plain", "values": basis["values"]}}],
            }
            bookmark = prep["expected_thread_id"] or prep["creation_id"]
            if (
                parent is None
                or _canonical(parent.payload.get("product_basis")) != _canonical(expected)
                or parent.id not in mark.delivery_ids
                or len(group.rows) != 2
                or {row.kind for row in group.rows} != {"new_confirm", "new_keep"}
                or any(
                    row.target_kind != "thread" or row.target_id != bookmark or _canonical({key: value for key, value in row.payload.items() if key not in CALLBACK_METADATA_KEYS}) != _canonical({"thread_id": bookmark}) for row in group.rows
                )
            ):
                raise ValueError("callback-choice-conflict") from None
            choice = {"version": 1, "callback_id": group.selected.id, "kind": group.selected.kind, "group_id": group.selected.group_id, "anchor_outbox_id": group.anchor_outbox_id}
            if "callback_choice" in mark.route_basis and _canonical(mark.route_basis["callback_choice"]) != _canonical(choice):
                raise ValueError("callback-choice-conflict") from None
            mark.route_basis = {**deepcopy(mark.route_basis), "callback_choice": choice}
            await writer.flush()
            result = _snapshot(mark)
            await guard.check(writer)
            return result

        async def perform():
            if session is not None:
                return await claim(session)
            await prepare_publication_guard(guard)
            async with self._transaction() as writer:
                return await claim(writer)

        return await await_drained(perform())

    async def route(self, row: InboxRow, *, kind: str, target_id: str | None, basis: dict, operation_ids: list[str]) -> InboxRow:
        _identifier(kind, 24, "invalid-inbox-route")
        if target_id is not None:
            _identifier(target_id, 64, "invalid-inbox-route")
        basis = _json_copy(basis, "invalid-inbox-route")
        if SERVER_BOOKKEEPING_KEYS.intersection(basis) or not isinstance(operation_ids, list) or any(not isinstance(item, str) for item in operation_ids) or len(set(operation_ids)) != len(operation_ids):
            raise ValueError("invalid-inbox-route")
        for operation in operation_ids:
            _identifier(operation, 64, "invalid-inbox-route")
        async with self._transaction() as session:
            link = await self._link(session, row.link_id)
            mark = (await self._rows(session, [row]))[0]
            if mark.route_kind is not None:
                if (mark.route_kind, mark.route_target_id, _canonical(_basis(mark)), mark.operation_ids) != (kind, target_id, _canonical(basis), operation_ids):
                    raise ValueError("inbox-route-conflict")
                return _snapshot(mark)
            self._current(link, mark)
            if mark.route_complete:
                raise ValueError("inbox-route-complete")
            mark.route_kind, mark.route_target_id, mark.route_basis, mark.operation_ids = kind, target_id, basis, deepcopy(operation_ids)
            await session.flush()
            return _snapshot(mark)

    async def bind_turn(self, rows: Sequence[InboxRow], *, turn_id: str, thread_id: str) -> None:
        link_id = self._requested(rows)
        _identifier(turn_id, 32, "invalid-inbox-turn")
        _identifier(thread_id, 64, "invalid-inbox-turn")
        async with self._transaction() as session:
            link = await self._link(session, link_id)
            if session.get_bind().dialect.name == "postgresql":
                # The schema intentionally has no single-row turn parent. Fence
                # concurrent attempts to reuse a turn id on different links.
                key = int.from_bytes(hashlib.sha256(("telegram-inbox-turn:" + turn_id).encode()).digest()[:8], signed=True)
                await session.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": key})
            marks = await self._rows(session, rows)
            self._group(marks)
            if any("publication_recovery" in mark.route_basis for mark in marks):
                raise ValueError("inbox-publication-conflict") from None
            for mark in marks:
                self._current(link, mark)
            if any(mark.route_complete or mark.state == "interrupted" for mark in marks):
                raise ValueError("inbox-route-complete")
            existing_links = list(await session.scalars(select(TelegramInboundMarkRow.link_id).where(TelegramInboundMarkRow.turn_id == turn_id).distinct()))
            if any(existing_link != link_id for existing_link in existing_links):
                raise ValueError("inbox-turn-conflict")
            existing = await self._turn(session, turn_id)
            if existing or any(mark.turn_id is not None for mark in marks):
                if {_identity(mark) for mark in existing} != {_identity(mark) for mark in marks} or any(mark.turn_id != turn_id or mark.thread_id != thread_id for mark in marks):
                    raise ValueError("inbox-turn-conflict")
                return
            ordered = sorted(marks, key=lambda mark: (_time(mark.accepted_at), mark.update_id, mark.message_id))
            event_id = f"m{ordered[0].message_id}"
            publication_id = self._publication(turn_id, event_id)
            publications = [{"publication_id": publication_id, "event_id": event_id, "run_id": None}]
            for mark in marks:
                mark.turn_id, mark.thread_id, mark.state = turn_id, thread_id, "dispatched"
                mark.grouped_message_ids = [other.message_id for other in ordered if other is not mark]
                mark.route_basis = {**deepcopy(mark.route_basis), "publications": deepcopy(publications), "current_publication_id": publication_id}

    @staticmethod
    def _publication(turn_id: str, event_id: str) -> str:
        return hashlib.sha256((turn_id + ":" + event_id).encode()).hexdigest()[:32]

    async def publish(self, rows: Sequence[InboxRow], *, event_id: str, expected_publication_id: str) -> str:
        """Persist an explicit next retry, or replay that same current intent.

        Telegram redelivery uses accept/bind_turn; it cannot allocate a retry.
        The caller supplies the stored current publication as a comparison fence.
        """
        link_id = self._requested(rows)
        _identifier(event_id, 96, "invalid-inbox-publication")
        async with self._transaction() as session:
            link = await self._link(session, link_id)
            marks = await self._rows(session, rows)
            self._group(marks)
            if any("publication_recovery" in mark.route_basis for mark in marks):
                raise ValueError("inbox-publication-conflict") from None
            if marks[0].turn_id is None:
                raise ValueError("inbox-publication-conflict")
            group = await self._turn(session, marks[0].turn_id)
            if {_identity(mark) for mark in group} != {_identity(mark) for mark in marks}:
                raise ValueError("inbox-group-conflict")
            for mark in marks:
                self._current(link, mark)
            if any(mark.route_complete or mark.state == "interrupted" for mark in marks):
                raise ValueError("inbox-route-complete")
            basis = marks[0].route_basis
            if any(_canonical(_publication_basis(mark)) != _canonical(_publication_basis(marks[0])) for mark in marks):
                raise ValueError("inbox-publication-conflict")
            publications = deepcopy(basis.get("publications", []))
            current = basis.get("current_publication_id")
            for saved in publications:
                if saved["event_id"] == event_id:
                    if saved["publication_id"] != current:
                        raise ValueError("inbox-publication-conflict")
                    return current
            if not publications or current != expected_publication_id or event_id != f"{publications[0]['event_id']}:r{len(publications)}":
                raise ValueError("inbox-publication-conflict")
            publication_id = self._publication(marks[0].turn_id, event_id)
            publications.append({"publication_id": publication_id, "event_id": event_id, "run_id": None})
            for mark in marks:
                mark.route_basis = {**deepcopy(mark.route_basis), "publications": deepcopy(publications), "current_publication_id": publication_id}
                mark.run_id, mark.assistant_message_id = None, None
                mark.state = "dispatched"
            return publication_id

    async def bind_run(self, *, turn_id: str, publication_id: str, run_id: str) -> None:
        _identifier(run_id, 64, "invalid-inbox-run")
        async with self._transaction() as session:
            # Discovery is read-only. Authority is checked again after taking
            # the one parent lock and then the sorted group rows.
            links = list(await session.scalars(select(TelegramInboundMarkRow.link_id).where(TelegramInboundMarkRow.turn_id == turn_id).distinct()))
            if len(links) != 1:
                raise ValueError("inbox-publication-conflict")
            link = await self._link(session, links[0])
            marks = await self._turn(session, turn_id)
            if not marks:
                raise ValueError("inbox-publication-conflict")
            self._group(marks)
            if any("publication_recovery" in mark.route_basis for mark in marks):
                raise ValueError("inbox-publication-conflict") from None
            if any(_canonical(_publication_basis(mark)) != _canonical(_publication_basis(marks[0])) for mark in marks):
                raise ValueError("inbox-publication-conflict")
            for mark in marks:
                self._current(link, mark)
                if mark.route_basis.get("current_publication_id") != publication_id:
                    raise ValueError("inbox-publication-conflict")
                if mark.run_id not in (None, run_id):
                    raise ValueError("inbox-run-conflict")
                if mark.route_complete and mark.run_id != run_id:
                    raise ValueError("inbox-route-complete")
            publications = deepcopy(marks[0].route_basis.get("publications", []))
            entry = next((item for item in publications if item["publication_id"] == publication_id), None)
            if entry is None:
                raise ValueError("inbox-publication-conflict")
            if entry["run_id"] not in (None, run_id):
                raise ValueError("inbox-run-conflict")
            entry["run_id"] = run_id
            for mark in marks:
                mark.run_id = run_id
                mark.route_basis = {**deepcopy(mark.route_basis), "publications": deepcopy(publications)}

    async def register_delivery(self, rows: Sequence[InboxRow], *, outbox_id: str, primary: bool = False, finish: bool = False, session: AsyncSession) -> None:
        """Attach a plan in its caller's transaction, then recompute completion."""
        self._borrow(session)
        link_id = self._requested(rows)
        await session.flush()
        link = await self._link(session, link_id)
        marks = await self._rows(session, rows)
        delivery = await session.get(TelegramOutboxRow, outbox_id)
        if delivery is None or delivery.link_id != link_id or delivery.owner_user_id != link.owner_user_id:
            raise ValueError("inbox-delivery-conflict")
        if primary:
            turns = {mark.turn_id for mark in marks}
            if len(turns) != 1:
                raise ValueError("inbox-group-conflict")
            if marks[0].turn_id is not None:
                group = await self._turn(session, marks[0].turn_id)
                if {_identity(mark) for mark in group} != {_identity(mark) for mark in marks}:
                    raise ValueError("inbox-group-conflict")
        for mark in marks:
            if (delivery.binding_generation, delivery.chat_id) != (mark.binding_generation, mark.chat_id):
                raise ValueError("inbox-delivery-conflict")
            # The first attachment must use the captured source topic. A saved
            # delivery can later relocate its presentation (for example, to
            # General after a topic disappears); replay retains that same id.
            if outbox_id not in mark.delivery_ids and _topic(delivery.message_thread_id) != mark.message_thread_id:
                raise ValueError("inbox-delivery-conflict")
            if primary and mark.outbox_id not in (None, outbox_id):
                raise ValueError("inbox-delivery-conflict")
            if mark.route_complete and outbox_id not in mark.delivery_ids:
                raise ValueError("inbox-route-complete")
            if not mark.route_complete:
                self._current(link, mark)
            if finish and mark.turn_id is not None and not primary and mark.outbox_id is None:
                raise ValueError("inbox-route-unfinished")
        for mark in marks:
            if outbox_id not in mark.delivery_ids:
                mark.delivery_ids = [*mark.delivery_ids, outbox_id]
            if primary:
                mark.outbox_id = outbox_id
            if finish:
                mark.route_complete = True
            if mark.state != "done":
                mark.state = "reply_pending"
        await self.settle_deliveries(link_id=link_id, session=session)

    async def finish_route(self, row: InboxRow, *, session: AsyncSession | None = None) -> None:
        if session is None:
            async with self._transaction() as writer:
                await self.finish_route(row, session=writer)
                return
        self._borrow(session)
        await session.flush()
        await self._link(session, row.link_id)
        mark = (await self._rows(session, [row]))[0]
        if mark.route_kind is None or mark.turn_id is not None and mark.outbox_id is None:
            raise ValueError("inbox-route-unfinished")
        mark.route_complete = True
        await self.settle_deliveries(link_id=row.link_id, session=session)

    async def settle_deliveries(self, *, link_id: str, session: AsyncSession) -> None:
        """Settle only sealed inputs whose complete delivery set is terminal.

        The caller already owns the link writer lock. This also understands
        welcome records written by links.bind before this repository existed.
        A failed or missing delivery remains unresolved for repair.
        """
        self._borrow(session)
        # Caller sessions may disable autoflush. Persist their plan/receipt and
        # our sealing flags before the completion SELECT in this same unit.
        await session.flush()
        marks = list(
            await session.scalars(
                select(TelegramInboundMarkRow)
                .where(TelegramInboundMarkRow.link_id == link_id, TelegramInboundMarkRow.route_complete.is_(True), TelegramInboundMarkRow.state != "done")
                .order_by(TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        identities = {identity for mark in marks for identity in [*mark.delivery_ids, *([mark.outbox_id] if mark.outbox_id else [])]}
        deliveries = {row.id: row for row in await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link_id, TelegramOutboxRow.id.in_(identities)))} if identities else {}
        for mark in marks:
            required = set(mark.delivery_ids) | ({mark.outbox_id} if mark.outbox_id else set())
            if all(
                identity in deliveries and deliveries[identity].binding_generation == mark.binding_generation and deliveries[identity].chat_id == mark.chat_id and deliveries[identity].state in {"sent", "skipped"} for identity in required
            ):
                mark.state, mark.closed_at = "done", _now()
            elif required:
                mark.state, mark.closed_at = "reply_pending", None

    async def leftovers(self, *, before: datetime) -> list[InboxRow]:
        if not isinstance(before, datetime):
            raise ValueError("invalid-inbox-cutoff")
        async with self.session_factory() as session:
            marks = await session.scalars(
                select(TelegramInboundMarkRow)
                .where(TelegramInboundMarkRow.accepted_at < _time(before), TelegramInboundMarkRow.state != "done", TelegramInboundMarkRow.outbox_id.is_(None))
                .order_by(TelegramInboundMarkRow.accepted_at, TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.update_id)
            )
            return [_snapshot(mark) for mark in marks]

    async def recovery_page(self, *, after: RecoveryCursor | None, limit: int = 32, max_origins: int = 64) -> RecoveryPage:
        """Bound returned groups/members, without claiming bounded scan bytes/time."""
        from deerflow.utils.file_io import await_drained

        if type(limit) is not int or not 1 <= limit <= 32 or type(max_origins) is not int or not 1 <= max_origins <= 64 or after is not None and type(after) is not RecoveryCursor:
            raise ValueError("invalid-recovery-page") from None

        async def perform() -> RecoveryPage:
            groups = []
            async with self.session_factory() as session:
                statement = select(TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.turn_id).where(
                    TelegramInboundMarkRow.route_kind == "turn", TelegramInboundMarkRow.turn_id.is_not(None), TelegramInboundMarkRow.state != "done", TelegramInboundMarkRow.outbox_id.is_(None)
                )
                if after is not None:
                    statement = statement.where(or_(TelegramInboundMarkRow.link_id > after.link_id, and_(TelegramInboundMarkRow.link_id == after.link_id, TelegramInboundMarkRow.turn_id > after.turn_id)))
                heads = list((await session.execute(statement.distinct().order_by(TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.turn_id).limit(limit + 1))).all())
                for link_id, turn_id in heads[:limit]:
                    # Head eligibility must never manufacture a complete prefix.
                    # Preserve global _turn collision semantics, including rows
                    # that are done, interrupted or already have a primary.
                    members = list(
                        await session.scalars(
                            select(TelegramInboundMarkRow).where(TelegramInboundMarkRow.turn_id == turn_id).order_by(TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id).limit(max_origins + 1)
                        )
                    )
                    groups.append((RecoveryCursor(link_id, turn_id), len(members) > max_origins, tuple(_snapshot(row) for row in members) if len(members) <= max_origins else ()))

            def decode() -> tuple[RecoveryEntry, ...]:
                def legacy(rows) -> bool:
                    try:
                        for row in rows:
                            if type(row.route_basis) is not dict:
                                return False
                            history = row.route_basis.get("publications")
                            if type(history) is not list or not history:
                                return False
                            for item in history:
                                if type(item) is not dict or set(item) != {"publication_id", "event_id", "run_id"}:
                                    return False
                                _publication_text(item["event_id"], 96)
                                if item["publication_id"] != self._publication(row.turn_id, item["event_id"]):
                                    return False
                                if item["run_id"] is not None:
                                    _publication_text(item["run_id"], 64)
                            if row.route_basis.get("current_publication_id") != history[-1]["publication_id"]:
                                return False
                        return True
                    except (AttributeError, KeyError, TypeError, ValueError):
                        return False

                entries = []
                for key, overflow, rows in groups:
                    if overflow:
                        entries.append(RecoveryEntry(key, None, (), "group_too_large"))
                        continue
                    if not rows:
                        entries.append(RecoveryEntry(key, None, (), "incomplete_group"))
                        continue
                    try:
                        record = self._publication_record(rows)
                    except ValueError:
                        entries.append(RecoveryEntry(key, None, rows, "legacy" if legacy(rows) else "invalid_group"))
                        continue
                    if (
                        any(row.route_complete or row.outbox_id is not None or row.assistant_message_id is not None or row.state not in {"dispatched", "reply_pending"} for row in rows)
                        or len({row.state for row in rows}) != 1
                        or any("publication_recovery" in row.route_basis for row in rows)
                    ):
                        entries.append(RecoveryEntry(key, record, rows, "incomplete_group"))
                    elif not _recovery_plain_input(rows):
                        entries.append(RecoveryEntry(key, record, rows, "unsupported_input"))
                    else:
                        entries.append(RecoveryEntry(key, record, rows, None))
                return tuple(entries)

            entries = await await_drained(asyncio.to_thread(decode))
            more = len(heads) > limit
            return RecoveryPage(entries, entries[-1].key if more else None, more)

        return await await_drained(perform())
