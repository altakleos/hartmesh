"""Settings actions share one custody, preference and presentation transaction.

Later service actions and their retry workers are deliberately not installed.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType

from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.telegram_bots.callbacks import CALLBACK_METADATA_KEYS, CallbackRefused
from app.telegram_bots.links import BotLink, prepare_publication_guard
from app.telegram_bots.texts import KEYBOARDS
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.runtime.keyed_lock import AsyncKeyedLockTable
from deerflow.utils.file_io import await_drained

_LOCALES = {"en-US", "zh-CN"}
_SCOPE = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")


def _utc(value):
    return value.replace(tzinfo=UTC) if value is not None and value.tzinfo is None else value


def _frozen(value):
    if isinstance(value, Mapping):
        return MappingProxyType({key: _frozen(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_frozen(item) for item in value)
    return value


@dataclass(frozen=True)
class ActRow:
    id: str
    link_id: str | None
    bot_id: int
    owner_user_id: str
    binding_generation: int
    telegram_user_id: int
    chat_id: int | None
    message_id: int | None
    kind: str
    instance_id: str | None
    work_id: str | None
    request_id: str | None
    operation_ids: tuple[str, ...]
    continuation: Mapping | None
    stop_dispatch: Mapping | None
    cancel_requested_at: datetime | None
    body: Mapping
    state: str
    reason: str | None
    result: Mapping | None
    attempts: int
    first_attempt_at: datetime | None
    created_at: datetime
    updated_at: datetime


def _snapshot(row):
    values = {name: getattr(row, name) for name in ActRow.__dataclass_fields__}
    for name in ("body", "result", "continuation", "stop_dispatch"):
        values[name] = _frozen(values[name])
    values["operation_ids"] = tuple(values["operation_ids"])
    for name in ("cancel_requested_at", "first_attempt_at", "created_at", "updated_at"):
        values[name] = _utc(values[name])
    return ActRow(**values)


def _body(value):
    if not isinstance(value, Mapping) or set(value) != {"field", "old", "new"} or value["field"] != "locale" or any(type(value[key]) is not str or value[key] not in _LOCALES for key in ("old", "new")):
        raise ValueError("invalid-settings-act") from None
    return dict(value)


def _network_body(value):
    keys = {"thread_id", "request_id", "option_id", "host", "port"}
    if not isinstance(value, Mapping) or set(value) != keys or value["option_id"] not in {"allow_temporary", "deny"}:
        raise ValueError("invalid-network-act") from None
    for key in ("thread_id", "request_id", "host"):
        if type(value[key]) is not str or not value[key].strip() or not value[key].isprintable():
            raise ValueError("invalid-network-act") from None
    if len(value["thread_id"]) > 64 or type(value["port"]) is not int or not 1 <= value["port"] <= 65535:
        raise ValueError("invalid-network-act") from None
    return dict(value)


@dataclass
class _AttemptGuard:
    actual: object
    retired: bool = False

    @property
    def captured_link(self):
        return self.actual.captured_link

    def current(self):
        if self.retired:
            raise CallbackRefused() from None

    async def prepare(self):
        self.current()
        await prepare_publication_guard(self.actual)
        self.current()

    async def check(self, session):
        self.current()
        link = await self.actual.check(session)
        self.current()
        return link


class Acts:
    def __init__(self, session_factory, *, settings, callbacks, outbox):
        self.session_factory = session_factory
        self.settings, self.callbacks, self.outbox = settings, callbacks, outbox
        self._locks = AsyncKeyedLockTable()
        self._operations: set[asyncio.Task] = set()

    def _writer_required(self, session):
        bind = self.session_factory.kw.get("bind")
        if not isinstance(session, AsyncSession) or bind is None or not session.in_transaction() or not session.is_active or session.get_bind() is not getattr(bind, "sync_engine", bind):
            raise ValueError("act-writer-required") from None

    async def write_network(self, link, *, callback_id, body, guard, session):
        """Journal the actual card choice in its publication owner's writer."""
        self._writer_required(session)
        body = _network_body(body)
        async with self.callbacks.writer(link, guard, session=session) as writer:
            selected = await writer.get(TelegramCallbackRow, callback_id, with_for_update=True, populate_existing=True)
            if (
                selected is None
                or selected.kind != "network"
                or (selected.link_id, selected.chat_id, selected.binding_generation, selected.target_kind, selected.target_id) != (link.id, link.chat_id, link.binding_generation, "thread", body["thread_id"])
            ):
                raise CallbackRefused() from None
            saved_body = {key: child for key, child in selected.payload.items() if key not in CALLBACK_METADATA_KEYS}
            if _network_body(saved_body) != body:
                raise CallbackRefused() from None
            row = await writer.get(TelegramActRow, callback_id, with_for_update=True, populate_existing=True)
            scope = dict(
                link_id=link.id, bot_id=link.bot_id, owner_user_id=link.owner_user_id, binding_generation=link.binding_generation, telegram_user_id=link.telegram_user_id, chat_id=link.chat_id, message_id=selected.message_id, kind="network"
            )
            if row is not None:
                if any(getattr(row, key) != value for key, value in scope.items()) or _network_body(row.body) != body:
                    raise CallbackRefused() from None
                return _snapshot(row)
            await self.callbacks.lock_current(link, callback_id, chat_id=link.chat_id, message_id=selected.message_id, message_thread_id=selected.message_thread_id, guard=guard, session=writer)
            now = datetime.now(UTC)
            row = TelegramActRow(id=callback_id, **scope, body=body, state="pending", operation_ids=[], attempts=0, first_attempt_at=None, created_at=now, updated_at=now)
            writer.add(row)
            await writer.flush()
            return _snapshot(row)

    async def confirm_network_publication(self, record, *, session, guard=None):
        """Record enqueue/bind evidence; this never applies a network grant."""
        from app.telegram_bots.inbox import InboxRepository

        self._writer_required(session)
        if record.owner_callback_id is None:
            return None
        row = await session.get(TelegramActRow, record.owner_callback_id, with_for_update=True, populate_existing=True)
        if row is None or row.kind != "network":
            return None
        fresh, _ = await InboxRepository(self.session_factory).lock_publication(record, session=session)
        selected = await session.get(TelegramCallbackRow, record.owner_callback_id, populate_existing=True)
        actual_link = await session.get(TelegramBotLinkRow, record.scope.link_id, populate_existing=True)
        if selected is None or actual_link is None or selected.kind != "network":
            raise CallbackRefused() from None
        link = BotLink.from_row(actual_link)
        body = _network_body({key: child for key, child in selected.payload.items() if key not in CALLBACK_METADATA_KEYS})
        scope = dict(link_id=link.id, bot_id=link.bot_id, owner_user_id=link.owner_user_id, binding_generation=link.binding_generation, telegram_user_id=link.telegram_user_id, chat_id=link.chat_id, message_id=selected.message_id)
        owner = selected.payload.get("publication_owner")
        if any(getattr(row, key) != value for key, value in scope.items()) or _network_body(row.body) != body or body["thread_id"] != fresh.scope.thread_id or selected.message_thread_id != fresh.scope.message_thread_id:
            raise CallbackRefused() from None
        if not isinstance(owner, dict) or owner.get("confirmed_publication_id") != fresh.publication_id or not fresh.dispatch_started:
            raise CallbackRefused() from None
        if guard is not None:
            await guard.check(session)
        if row.state == "done":
            if row.result.get("turn_id") != fresh.turn_id or row.result.get("publication_id") not in {entry["publication_id"] for entry in owner["publications"]}:
                raise CallbackRefused() from None
            return _snapshot(row)
        if row.state != "pending":
            raise CallbackRefused() from None
        now = datetime.now(UTC)
        row.state, row.attempts, row.first_attempt_at, row.updated_at = "done", 1, now, now
        row.result = {"turn_id": fresh.turn_id, "publication_id": fresh.publication_id}
        await session.flush()
        if guard is not None:
            await guard.check(session)
        return _snapshot(row)

    async def voice_seconds_since(self, owner_user_id: str, since: datetime) -> int:
        """Seconds of the person's finished transcriptions, on any of their bots."""

        async def perform():
            async with self.session_factory() as session:
                rows = await session.scalars(select(TelegramActRow.body).where(TelegramActRow.owner_user_id == owner_user_id, TelegramActRow.kind == "voice_note", TelegramActRow.state == "done", TelegramActRow.created_at >= since))
                return sum(body["duration"] for body in rows)

        return await await_drained(perform())

    async def write_voice_note(self, link: BotLink, *, message_id: int, duration: int, provider: str, used_as: str, session: AsyncSession) -> ActRow:
        """A finished transcription, recorded once in the writer that saves its text.

        The audit is terminal: no worker repeats it, and it holds no audio,
        transcript or file id.
        """
        self._writer_required(session)
        if provider not in {"openai", "gemini"} or used_as not in {"message", "answer"} or type(duration) is not int or duration < 0:
            raise ValueError("invalid-voice-act") from None
        now = datetime.now(UTC)
        row = TelegramActRow(
            id=hashlib.sha256(f"telegram:{link.id}:{link.chat_id}:{message_id}:voice_note".encode()).hexdigest()[:32],
            link_id=link.id,
            bot_id=link.bot_id,
            owner_user_id=link.owner_user_id,
            binding_generation=link.binding_generation,
            telegram_user_id=link.telegram_user_id,
            chat_id=link.chat_id,
            message_id=message_id,
            kind="voice_note",
            body={"duration": duration, "provider": provider, "used_as": used_as},
            state="done",
            operation_ids=[],
            attempts=1,
            first_attempt_at=now,
            created_at=now,
            updated_at=now,
        )
        session.add(row)
        await session.flush()
        return _snapshot(row)

    async def write(self, link: BotLink, *, act_id: str, kind: str, body: dict, state: str = "pending", session: AsyncSession, **targets) -> ActRow:
        """Borrowed stage-one metadata only; the effect owner supplies custody."""
        self._writer_required(session)
        if type(act_id) is not str or re.fullmatch(r"[0-9a-f]{32}", act_id) is None or kind != "settings" or state != "pending" or set(targets) - {"message_id"}:
            raise ValueError("invalid-settings-act") from None
        canonical = _body(body)
        message = targets.get("message_id")
        if (
            not isinstance(link, BotLink)
            or link.state != "connected"
            or type(link.telegram_user_id) is not int
            or link.telegram_user_id <= 0
            or link.chat_id != link.telegram_user_id
            or message is not None
            and (type(message) is not int or message <= 0)
        ):
            raise ValueError("invalid-settings-act") from None
        return await await_drained(self._write(link, act_id, canonical, message, session))

    async def _write(self, link, identity, body, message, session):
        row = await session.scalar(select(TelegramActRow).where(TelegramActRow.id == identity).with_for_update().execution_options(populate_existing=True))
        basis = dict(link_id=link.id, bot_id=link.bot_id, owner_user_id=link.owner_user_id, binding_generation=link.binding_generation, telegram_user_id=link.telegram_user_id, chat_id=link.chat_id, message_id=message, kind="settings")
        if row is not None:
            if any(getattr(row, key) != value for key, value in basis.items()) or _body(row.body) != body:
                raise ValueError("act-source-conflict") from None
            return _snapshot(row)
        now = datetime.now(UTC)
        row = TelegramActRow(id=identity, **basis, body=body, state="pending", operation_ids=[], attempts=0, first_attempt_at=None, created_at=now, updated_at=now)
        session.add(row)
        await session.flush()
        return _snapshot(row)

    async def apply_settings(self, link: BotLink, callback_id: str, *, chat_id: int, message_id: int, message_thread_id: int | None, guard) -> ActRow:
        """Apply only the actual saved language choice under one original attempt."""
        if type(callback_id) is not str or re.fullmatch(r"[0-9a-f]{32}", callback_id) is None or type(chat_id) is not int or chat_id != link.chat_id or type(message_id) is not int or message_id <= 0:
            raise CallbackRefused() from None
        if message_thread_id is not None and (type(message_thread_id) is not int or message_thread_id <= 0):
            raise CallbackRefused() from None
        topic = None if message_thread_id == 1 else message_thread_id
        attempt = _AttemptGuard(guard)

        async def run():
            async with self._locks.hold(callback_id):
                attempt.current()
                return await await_drained(self._apply_settings(link, callback_id, chat_id, message_id, topic, attempt))

        task = asyncio.create_task(run(), name="telegram-settings-act")
        self._operations.add(task)

        def settled(done):
            self._operations.discard(done)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(settled)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            attempt.retired = True
            task.cancel()
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not task.cancelled():
                task.exception()
            raise

    @staticmethod
    def _act_scope(row, link, message):
        if (
            (row.link_id, row.bot_id, row.owner_user_id, row.binding_generation, row.telegram_user_id, row.chat_id, row.message_id, row.kind)
            != (link.id, link.bot_id, link.owner_user_id, link.binding_generation, link.telegram_user_id, link.chat_id, message, "settings")
            or row.operation_ids != []
            or any(getattr(row, key) is not None for key in ("instance_id", "work_id", "request_id", "continuation", "stop_dispatch", "cancel_requested_at", "reason"))
        ):
            raise CallbackRefused() from None
        return _body(row.body)

    async def _completed(self, session, row, link, message, topic):
        body = self._act_scope(row, link, message)
        result = row.result
        if row.state != "done" or row.attempts != 1 or row.first_attempt_at is None or type(result) is not dict or set(result) != {"outbox_id", "locale"} or result["locale"] != body["new"] or type(result["outbox_id"]) is not str:
            raise CallbackRefused() from None
        outbox = await session.get(TelegramOutboxRow, result["outbox_id"], with_for_update=True, populate_existing=True)
        if outbox is None or (outbox.link_id, outbox.owner_user_id, outbox.binding_generation, outbox.chat_id, outbox.message_thread_id, outbox.dedupe_key) != (
            link.id,
            link.owner_user_id,
            link.binding_generation,
            link.chat_id,
            topic,
            "notice:settings:" + row.id,
        ):
            raise CallbackRefused() from None
        return _snapshot(row)

    async def _apply_settings(self, link, identity, chat, message, topic, guard):
        # Preparation can touch config/provider state; no writer is open yet.
        await prepare_publication_guard(guard)
        async with self.session_factory() as session, session.begin():
            if session.get_bind().dialect.name == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            preferences = await session.scalar(select(TelegramPersonSettingsRow).where(TelegramPersonSettingsRow.owner_user_id == link.owner_user_id).with_for_update().execution_options(populate_existing=True))
            if preferences is None:
                raise CallbackRefused() from None
            # Settings, then links in ID order: this writer marks them all owed.
            await session.execute(select(TelegramBotLinkRow.id).where(TelegramBotLinkRow.owner_user_id == link.owner_user_id).order_by(TelegramBotLinkRow.id).with_for_update())
            # The owner lock serializes its Acts. Read without taking an Act
            # lock before link/anchor/group; an existing receipt causes no effect.
            existing = await session.get(TelegramActRow, identity, populate_existing=True)
            if existing is not None and existing.state == "done":
                current = await guard.check(session)
                if any(getattr(current, name) != getattr(link, name) for name in _SCOPE):
                    raise CallbackRefused() from None
                result = await self._completed(session, existing, link, message, topic)
            else:
                group = await self.callbacks.lock_current(link, identity, chat_id=chat, message_id=message, message_thread_id=topic, guard=guard, session=session)
                selected = group.selected
                payload = {key: value for key, value in selected.payload.items() if key not in CALLBACK_METADATA_KEYS}
                if (
                    selected.kind != "settings_set"
                    or (selected.target_kind, selected.target_id) != ("link", link.id)
                    or set(payload) != {"field", "value"}
                    or payload["field"] != "locale"
                    or type(payload["value"]) is not str
                    or payload["value"] not in _LOCALES
                ):
                    raise CallbackRefused() from None
                body = {"field": "locale", "old": preferences.locale, "new": payload["value"]}
                if existing is not None:
                    saved = self._act_scope(existing, link, message)
                    if existing.state != "pending" or existing.attempts != 0 or existing.first_attempt_at is not None or saved != body:
                        raise CallbackRefused() from None
                guard.current()
                await self.write(link, act_id=identity, kind="settings", body=body, message_id=message, session=session)
                row = await session.get(TelegramActRow, identity, with_for_update=True)
                row.attempts, row.first_attempt_at = 1, datetime.now(UTC)
                guard.current()
                await self.settings.update(link.owner_user_id, locale=body["new"], session=session)
                # Every public text of the person's bots is written in this language.
                await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.owner_user_id == link.owner_user_id).values(profile_pending=True))
                guard.current()
                outbox_id = await self.outbox.plan_product_keyboard(
                    link,
                    source_id="settings:" + identity,
                    parts=[{"kind": "edit", "target_message_id": message, "source_ref": {"key": "settings.state_basic", "values": {"language": "English" if body["new"] == "en-US" else "中文"}}}],
                    keyboard=KEYBOARDS["settings.state_basic"](link=link),
                    callbacks=self.callbacks,
                    guard=guard,
                    message_thread_id=topic,
                    session=session,
                )
                guard.current()
                await self.callbacks.consume_group(group, reason="done", session=session)
                row.state, row.result, row.updated_at = "done", {"outbox_id": outbox_id, "locale": body["new"]}, datetime.now(UTC)
                result = _snapshot(row)
            await session.flush()
            # Only commit/exit may follow this original live-human/attempt fence.
            await guard.check(session)
        return result
