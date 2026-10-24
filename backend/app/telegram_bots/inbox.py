"""Durable content identities, immutable routes and publication fences.

Owned writers reserve SQLite before reading and lock the link before inbox rows
on PostgreSQL. Delivery helpers borrow the caller's existing writer transaction;
they neither commit it nor create an independent session. No network work belongs
inside these transactions.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from sqlalchemy import and_, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.telegram_bots.adapter import TOKEN_BODY
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramBotLinkRow, TelegramInboundMarkRow, TelegramOutboxRow

if TYPE_CHECKING:
    from telegram import Update

    from app.telegram_bots.links import BotLink

_TOKEN = re.compile(TOKEN_BODY)
_PUBLICATION_KEYS = {"publications", "current_publication_id"}
_INPUT_KEYS = {"text", "caption", "files", "voice", "command", "type", "reply_to_message_id", "prompt_id", "media_group_id"}
_UNSAFE_KEYS = {"raw_update", "update", "token", "bot_token", "access_token", "download_url", "file_path", "audio_bytes", "bytes", "_content", "content_bytes"}
_FILE_KEYS = {"type", "file_id", "file_unique_id", "filename", "mime_type", "size", "message_id", "telegram_bot_id", "duration", "width", "height"}


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


def _snapshot(row: TelegramInboundMarkRow) -> InboxRow:
    return InboxRow(**{field.name: _time(value) if isinstance(value := getattr(row, field.name), datetime) else deepcopy(value) for field in fields(InboxRow)})


def _identity(row: InboxRow | TelegramInboundMarkRow) -> tuple[str, int, int]:
    return row.link_id, row.message_id, row.chat_id


def _basis(row: InboxRow | TelegramInboundMarkRow) -> dict:
    return {key: value for key, value in row.route_basis.items() if key not in _PUBLICATION_KEYS}


def _canonical(value: dict) -> str:
    # Python equality equates True and 1, but displayed routing revisions and
    # choices retain their JSON types. Key insertion order carries no meaning.
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


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
    async def _turn(session: AsyncSession, turn_id: str) -> list[TelegramInboundMarkRow]:
        return list(
            await session.scalars(
                select(TelegramInboundMarkRow)
                .where(TelegramInboundMarkRow.turn_id == turn_id)
                .order_by(TelegramInboundMarkRow.link_id, TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )

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

    async def route(self, row: InboxRow, *, kind: str, target_id: str | None, basis: dict, operation_ids: list[str]) -> InboxRow:
        _identifier(kind, 24, "invalid-inbox-route")
        if target_id is not None:
            _identifier(target_id, 64, "invalid-inbox-route")
        basis = _json_copy(basis, "invalid-inbox-route")
        if _PUBLICATION_KEYS.intersection(basis) or not isinstance(operation_ids, list) or any(not isinstance(item, str) for item in operation_ids) or len(set(operation_ids)) != len(operation_ids):
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
            if any(mark.route_basis != basis for mark in marks):
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
            if any(mark.route_basis != marks[0].route_basis for mark in marks):
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
