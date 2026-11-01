"""Durable callback identities and prompt routing; messages supply no action authority."""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from uuid import uuid4

from sqlalchemy import select, text

from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.inbox import callback_publication_pending
from app.telegram_bots.links import BotLink
from app.telegram_bots.token_pattern import TOKEN_IN_TEXT
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.telegram_bots.model import TelegramAccountRow, TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

CALLBACK_PREFIX = "h1:"
PRESS_TIMEOUT_SECONDS = 2.0
_ID = re.compile(r"^[0-9a-f]{32}$")
_TOKEN = re.compile(TOKEN_IN_TEXT)
_SCOPE = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
_RESERVED = frozenset(
    {"label", "label_values", "keyboard_row", "keyboard_column", "anchor_outbox_id", "presentation_version", "presentation_pending", "presentation_history", "message_adopted", "prompt_successor", "publication_owner", "prompt_visibility"}
)
CALLBACK_METADATA_KEYS = _RESERVED
_SECRET = frozenset({"token", "bot_token", "access_token", "raw_update", "update", "audio", "audio_bytes", "download_url", "temporary_url", "bytes"})
_KINDS = frozenset(
    (
        "clarify_option clarify_other form_option form_next form_skip form_send form_change form_field network retry_message cancel_wait stop_answer "
        "stop_which new_confirm new_keep file_retry voice_ok voice_no web_list dismiss back act_check delivery_retry prompt_reopen prompt_send_new "
        "prompt_discard work_list_page work_list_open reveal digest_item hi_choice hi_answer hi_cant_open hi_cannot hi_wrong answer_yes answer_no "
        "voice_save_answer voice_save_resume voice_discard route_to decide_choice decide_own decide_note decide_confirm decide_confirm_resume accept_open "
        "accept_confirm changes_open changes_confirm assign_start assign_save assign_edit assign_discard assign_answer assign_from_suggestion work_start "
        "stop_attempt work_details work_change work_change_save work_withdraw work_withdraw_confirm settings_open settings_set settings_prompt summary_off "
        "add_open add_role add_rename add_confirm add_custom add_cancel bot_create"
    ).split()
)
_PAUSED = frozenset({"decide_confirm", "decide_confirm_resume", "accept_confirm", "work_change_save", "work_withdraw_confirm", "add_open", "add_confirm"})
_COMPLETES = _KINDS - frozenset(
    (
        "clarify_other form_change form_field web_list back act_check work_list_page work_list_open digest_item hi_answer hi_cant_open hi_cannot hi_wrong "
        "decide_choice decide_own decide_note accept_open changes_open assign_edit assign_from_suggestion work_details work_change work_withdraw "
        "settings_open settings_prompt add_open add_role add_rename bot_create"
    ).split()
)
CALLBACK_POLICIES = MappingProxyType(
    {
        kind: MappingProxyType(
            {
                "completes": {"form_option": "except_multi_select", "delivery_retry": "delivery_sent"}.get(kind, kind in _COMPLETES),
                "paused": {"network": "allow_temporary", "bot_create": "manager", "act_check": "original_act", "prompt_reopen": "original_prompt"}.get(kind, kind in _PAUSED),
            }
        )
        for kind in _KINDS
    }
)


class CallbackRefused(ValueError):
    """`key` is what the person is told. None tells them nothing: that press lost to one on its way in."""

    def __init__(self, key="stale.not_waiting"):
        self.key = key
        super().__init__(key)


def _now():
    return datetime.now(UTC)


def form_submission_value(fields, values):
    from app.telegram_bots.forms import form_submission_value as build

    return build(fields, values)


def _aware(value):
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _topic(value):
    if value is None or type(value) is int and value == 1:
        return None
    if type(value) is not int or value <= 0:
        raise CallbackRefused() from None
    return value


def _plain(value, depth=0):
    if depth > 16:
        raise CallbackRefused() from None
    if isinstance(value, Mapping):
        if any(type(key) is not str or key in _SECRET or _TOKEN.search(key) for key in value):
            raise CallbackRefused() from None
        return {key: _plain(child, depth + 1) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(child, depth + 1) for child in value]
    if value is None or type(value) in {bool, int} or type(value) is str and not _TOKEN.search(value):
        return value
    raise CallbackRefused() from None


def _freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(child) for key, child in value.items()})
    return tuple(_freeze(child) for child in value) if isinstance(value, list) else value


def safe_callback_value(value):
    return _plain(value)


def freeze_callback_value(value):
    return _freeze(value)


def normalize_callback_topic(value):
    return _topic(value)


@dataclass(frozen=True)
class CallbackSpec:
    label: str
    kind: str | None = None
    target_kind: str | None = None
    target_id: str | None = None
    payload: Mapping = field(default_factory=dict)
    expires_at: datetime | None = None
    url: str | None = None
    label_values: Mapping | None = None


type Keyboard = Sequence[Sequence[CallbackSpec | Mapping]]


def canonical_keyboard(keyboard: Keyboard) -> list[list[dict]]:
    if not isinstance(keyboard, (list, tuple)):
        raise CallbackRefused() from None
    result = []
    for line in keyboard:
        if not isinstance(line, (list, tuple)) or not line:
            raise CallbackRefused() from None
        current = []
        for supplied in line:
            if isinstance(supplied, CallbackSpec):
                label_values = supplied.label_values
                supplied = (
                    {"label": supplied.label, "url": supplied.url}
                    if supplied.url is not None
                    else {"label": supplied.label, "kind": supplied.kind, "target_kind": supplied.target_kind, "target_id": supplied.target_id, "payload": supplied.payload, "expires_at": supplied.expires_at}
                )
                if label_values is not None:
                    supplied["label_values"] = label_values
            if not isinstance(supplied, Mapping) or type(supplied.get("label")) is not str or not supplied["label"].startswith("label."):
                raise CallbackRefused() from None
            if "url" in supplied:
                from app.telegram_bots.producer import validate_url_button

                if set(supplied) - {"label", "url", "label_values"}:
                    raise CallbackRefused() from None
                if "label_values" in supplied and not isinstance(supplied["label_values"], Mapping):
                    raise CallbackRefused() from None
                validate_url_button({"page": "web.telegram", "url": supplied["url"]})
                current.append(_plain(dict(supplied)))
                continue
            if set(supplied) - {"label", "kind", "target_kind", "target_id", "payload", "expires_at", "label_values"} or supplied.get("kind") not in _KINDS:
                raise CallbackRefused() from None
            payload = _plain(supplied.get("payload", {}))
            if not isinstance(payload, dict) or _RESERVED.intersection(payload):
                raise CallbackRefused() from None
            expiry = supplied.get("expires_at")
            if isinstance(expiry, str):
                try:
                    expiry = datetime.fromisoformat(expiry)
                except ValueError:
                    raise CallbackRefused() from None
            if expiry is not None and (not isinstance(expiry, datetime) or expiry.tzinfo is None):
                raise CallbackRefused() from None
            target_kind, target = supplied.get("target_kind"), supplied.get("target_id")
            if target_kind not in {None, "request", "work", "attempt", "thread", "draft", "add_assistant", "link", "outbox", "act"} or target is not None and (type(target) is not str or not 1 <= len(target) <= 64):
                raise CallbackRefused() from None
            descriptor = {"label": supplied["label"], "kind": supplied["kind"], "target_kind": target_kind, "target_id": target, "payload": payload, "expires_at": _aware(expiry).isoformat() if expiry else None}
            if "label_values" in supplied:
                if not isinstance(supplied["label_values"], Mapping):
                    raise CallbackRefused() from None
                descriptor["label_values"] = _plain(supplied["label_values"])
            current.append(descriptor)
        result.append(current)
    return result


@dataclass(frozen=True)
class CallbackRow:
    id: str
    group_id: str
    link_id: str
    chat_id: int
    binding_generation: int
    message_id: int | None
    message_thread_id: int | None
    kind: str
    target_kind: str | None
    target_id: str | None
    payload: Mapping
    expires_at: datetime | None
    consumed_at: datetime | None
    closed_reason: str | None
    created_at: datetime


def _row(value):
    values = {name: getattr(value, name) for name in CallbackRow.__dataclass_fields__}
    values["payload"] = _freeze(_plain(values["payload"]))
    for name in ("expires_at", "consumed_at", "created_at"):
        if values[name] is not None:
            values[name] = _aware(values[name])
    return CallbackRow(**values)


def _validate_rows(parent, rows, *, part=None):
    """A stored row cannot change what its frozen keyboard offered."""
    version = parent.payload.get("presentation_version", 0)
    if type(version) is not int or version < 0:
        raise CallbackRefused() from None
    if parent.payload.get("reply_basis", {}).get("version") in {6, 7}:
        from app.telegram_bots.file_source import file_warning_keyboard, validate_media_reply_basis
        from app.telegram_bots.producer import _item

        try:
            validate_media_reply_basis(_item(parent))
            groups = {row.group_id for row in rows}
            part = part if part is not None else next(value for value in parent.parts if value.get("callback_group_id") in groups)
            if part.get("source_ref", {}).get("key") == "human_input.presentation" and parent.payload["reply_basis"]["version"] == 7:
                descriptors = canonical_keyboard(parent.payload.get("product_keyboard_basis"))
            else:
                descriptors = file_warning_keyboard(part)
        except (StopIteration, ValueError):
            raise CallbackRefused() from None
    else:
        descriptors = canonical_keyboard(parent.payload.get("product_keyboard_basis"))
    expected = {(line, column): spec for line, buttons in enumerate(descriptors) for column, spec in enumerate(buttons) if "url" not in spec}
    positions = {}
    for row in rows:
        line, column = row.payload.get("keyboard_row"), row.payload.get("keyboard_column")
        if type(line) is not int or type(column) is not int or (line, column) in positions:
            raise CallbackRefused() from None
        positions[line, column] = row
        spec = expected.get((line, column))
        payload = {key: value for key, value in row.payload.items() if key not in _RESERVED}
        if (
            spec is None
            or type(row.payload.get("presentation_version")) is not int
            or row.payload.get("presentation_version") != version
            or row.payload.get("presentation_pending") is not False
            or (row.link_id, row.chat_id, row.binding_generation, row.message_thread_id, row.payload.get("anchor_outbox_id")) != (parent.link_id, parent.chat_id, parent.binding_generation, parent.message_thread_id, parent.id)
            or (row.kind, row.target_kind, row.target_id, row.payload.get("label")) != (spec["kind"], spec["target_kind"], spec["target_id"], spec["label"])
            or json.dumps(payload, sort_keys=True) != json.dumps(spec["payload"], sort_keys=True)
            or json.dumps(row.payload.get("label_values"), sort_keys=True) != json.dumps(spec.get("label_values"), sort_keys=True)
            or (_aware(row.expires_at).isoformat() if row.expires_at else None) != spec["expires_at"]
        ):
            raise CallbackRefused() from None
    if positions.keys() != expected.keys():
        raise CallbackRefused() from None
    return descriptors, positions


@dataclass(frozen=True)
class WrittenCallbackGroup:
    group_id: str
    callback_ids: tuple[str, ...]


@dataclass(frozen=True)
class CallbackGroup:
    selected: CallbackRow
    rows: tuple[CallbackRow, ...]
    anchor_outbox_id: str
    presentation_version: int


@dataclass(frozen=True)
class CallbackMessageRoute:
    rows: tuple[CallbackRow, ...]
    retired_presentation: bool


@dataclass(frozen=True)
class StoredButton:
    label: str
    callback_data: str | None = None
    url: str | None = None
    label_values: Mapping | None = None


type StoredKeyboard = tuple[tuple[StoredButton, ...], ...]


def pause_covers(kind, payload, link, *, original_act_kind=None, original_act_body=None, original_prompt_kind=None):
    if kind in _PAUSED:
        return True
    if kind == "network":
        return payload.get("option_id") == "allow_temporary"
    if kind == "bot_create":
        return getattr(link, "can_manage_bots", False) is True
    if kind == "act_check":
        if original_act_kind == "network":
            return isinstance(original_act_body, Mapping) and original_act_body.get("option_id") == "allow_temporary"
        if original_act_kind == "managed_create":
            return getattr(link, "can_manage_bots", False) is True
        return original_act_kind in {"decide", "accept", "edit", "cancel", "add_assistant"}
    if kind == "prompt_reopen":
        return original_prompt_kind in {"decide_own", "decide_note"}
    return False


class CallbackRepository:
    def __init__(self, session_factory, *, config=None, clock=_now):
        self.sessions = session_factory
        self.config = config or TelegramBotsConfig()
        self.clock = clock

    def _borrow(self, session):
        bind = self.sessions.kw.get("bind")
        if not session.in_transaction() or not session.is_active or session.get_bind() is not getattr(bind, "sync_engine", bind):
            raise CallbackRefused() from None

    @asynccontextmanager
    async def _writer(self):
        async with self.sessions() as session, session.begin():
            if session.get_bind().dialect.name == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            yield session

    @asynccontextmanager
    async def writer(self, link, guard, *, session=None):
        """Share one guarded UI/input transaction; borrowed writers never commit."""
        from app.telegram_bots.links import prepare_publication_guard

        async def check(writer):
            current = await guard.check(writer)
            if type(current) is not BotLink or any(getattr(current, key) != getattr(link, key) for key in _SCOPE):
                raise CallbackRefused() from None

        if session is not None:
            self._borrow(session)
            await check(session)
            yield session
            await session.flush()
            await check(session)
        else:
            await prepare_publication_guard(guard)
            async with self._writer() as writer:
                await check(writer)
                yield writer
                await writer.flush()
                await check(writer)

    async def rows_in_scope(self, link, *, kinds, message_thread_id=None, include_consumed=False, guard, session=None):
        """Return detached locators only; a lookup does not authorize an action."""
        topic = _topic(message_thread_id)
        if not isinstance(kinds, (tuple, list, set, frozenset)) or not kinds or any(type(kind) is not str for kind in kinds) or type(include_consumed) is not bool:
            raise CallbackRefused() from None
        async with self.writer(link, guard, session=session) as writer:
            statement = select(TelegramCallbackRow).where(
                TelegramCallbackRow.link_id == link.id,
                TelegramCallbackRow.chat_id == link.chat_id,
                TelegramCallbackRow.binding_generation == link.binding_generation,
                TelegramCallbackRow.message_thread_id == topic,
                TelegramCallbackRow.kind.in_(kinds),
            )
            if not include_consumed:
                statement = statement.where(TelegramCallbackRow.consumed_at.is_(None))
            rows = list(await writer.scalars(statement.order_by(TelegramCallbackRow.id).with_for_update().execution_options(populate_existing=True)))
            return tuple(_row(row) for row in rows)

    async def consume_scope(self, link, *, kinds, reason, message_thread_id=None, guard, session):
        """Close only captured scope metadata in the caller's guarded writer."""
        if type(reason) is not str or not 1 <= len(reason) <= 16:
            raise CallbackRefused() from None
        rows = await self.rows_in_scope(link, kinds=kinds, message_thread_id=message_thread_id, guard=guard, session=session)
        for captured in rows:
            row = await session.get(TelegramCallbackRow, captured.id)
            row.consumed_at, row.closed_reason = self.clock(), reason
        await session.flush()
        return rows

    async def _binding(self, session, captured, *, healthy=True):
        self._borrow(session)
        link = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == captured.id).with_for_update().execution_options(populate_existing=True))
        if link is None or any(getattr(link, key) != getattr(captured, key) for key in _SCOPE) or link.state != "connected" or healthy and link.health != "ok":
            raise CallbackRefused() from None
        account = await session.get(TelegramAccountRow, link.telegram_user_id, with_for_update=True, populate_existing=True)
        connection = await session.get(ChannelConnectionRow, link.connection_id, with_for_update=True, populate_existing=True) if link.connection_id else None
        if (
            account is None
            or account.owner_user_id != link.owner_user_id
            or connection is None
            or (connection.owner_user_id, connection.provider, connection.status, connection.external_account_id, connection.workspace_id, connection.bot_user_id)
            != (link.owner_user_id, "telegram", "connected", str(link.telegram_user_id), str(link.bot_id), str(link.bot_id))
        ):
            raise CallbackRefused() from None
        return link

    async def current_link(self, captured):
        async def perform():
            async with self.sessions() as session:
                link = await session.get(TelegramBotLinkRow, captured.id)
                return BotLink.from_row(link) if link is not None and all(getattr(link, key) == getattr(captured, key) for key in _SCOPE) else None

        return await await_drained(perform())

    async def get(self, callback_id):
        if type(callback_id) is not str or not _ID.fullmatch(callback_id):
            return None

        async def perform():
            async with self.sessions() as session:
                saved = await session.get(TelegramCallbackRow, callback_id)
                return _row(saved) if saved else None

        return await await_drained(perform())

    async def write_group(self, link, keyboard, *, anchor_outbox_id, presentation_version, chat_id, message_id, message_thread_id, session):
        return await self._write_group(link, keyboard, anchor_outbox_id=anchor_outbox_id, presentation_version=presentation_version, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, session=session, healthy=True)

    async def write_repair_group(self, link, *, failed_outbox_id, anchor_outbox_id, session):
        """Store only the fixed repair of an actual failure, even during a hold.

        This records a delivery fact, never current permission to press or send.
        Ordinary group creation, presses and raw delivery keep their existing
        health and current-authority gates.
        """
        self._borrow(session)
        await self._binding(session, link, healthy=False)
        target = await session.get(TelegramOutboxRow, failed_outbox_id, with_for_update=True)
        parent = await session.get(TelegramOutboxRow, anchor_outbox_id, with_for_update=True)
        source = {"kind": "text", "source_ref": {"key": "delivery.failed", "values": {}}}
        if (
            target is None
            or parent is None
            or any((row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id) for row in (target, parent))
            or target.state != "failed"
            or target.payload.get("kind") == "delivery_failure_notice"
            or parent.class_ != "reply"
            or parent.state != "pending"
            or parent.message_thread_id != target.message_thread_id
            or type(target.payload.get("failure_version")) is not int
            or target.payload["failure_version"] <= 0
            or parent.dedupe_key != f"delivery-failed:{target.id}:{target.payload['failure_version']}"
            or json.dumps(parent.payload, sort_keys=True)
            != json.dumps(
                {
                    "kind": "delivery_failure_notice",
                    "repair_outbox_id": target.id,
                    "presentation_version": 0,
                    "product_basis": {"origins": [], "chat_id": link.chat_id, "message_thread_id": target.message_thread_id, "finish_route": False, "parts": [source]},
                },
                sort_keys=True,
            )
            or len(parent.parts) != 1
            or parent.parts[0].get("kind") != "text"
            or parent.parts[0].get("source_ref") != source["source_ref"]
            or parent.parts[0].get("stored_keyboard") is not True
            or parent.parts[0].get("callback_group_id") is not None
        ):
            raise CallbackRefused() from None
        keyboard = [[{"label": "label.retry_delivery", "kind": "delivery_retry", "target_kind": "outbox", "target_id": target.id, "payload": {"outbox_id": target.id}}]]
        parent.payload = {**parent.payload, "product_keyboard_basis": canonical_keyboard(keyboard)}
        return await self._write_group(link, keyboard, anchor_outbox_id=parent.id, presentation_version=0, chat_id=link.chat_id, message_id=None, message_thread_id=parent.message_thread_id, session=session, healthy=False)

    async def _write_group(self, link, keyboard, *, anchor_outbox_id, presentation_version, chat_id, message_id, message_thread_id, session, healthy):
        self._borrow(session)
        descriptors = canonical_keyboard(keyboard)
        if type(presentation_version) is not int or presentation_version < 0 or chat_id != link.chat_id or type(chat_id) is not int or message_id is not None and (type(message_id) is not int or message_id <= 0):
            raise CallbackRefused() from None
        await self._binding(session, link, healthy=healthy)
        parent = await session.get(TelegramOutboxRow, anchor_outbox_id, with_for_update=True, populate_existing=True)
        topic = _topic(message_thread_id)
        file_keyboard = False
        if parent is not None and parent.payload.get("reply_basis", {}).get("version") in {6, 7}:
            from app.telegram_bots.file_source import file_warning_keyboard, validate_media_reply_basis
            from app.telegram_bots.producer import _item

            try:
                validate_media_reply_basis(_item(parent))
                file_keyboard = any(value.get("stored_keyboard") and value.get("source_ref", {}).get("key") in {"file.out_failed", "file.too_large_out"} and file_warning_keyboard(value) == descriptors for value in parent.parts)
            except ValueError:
                raise CallbackRefused() from None
        if (
            parent is None
            or (parent.link_id, parent.owner_user_id, parent.chat_id, parent.binding_generation, parent.message_thread_id) != (link.id, link.owner_user_id, chat_id, link.binding_generation, topic)
            or parent.payload.get("presentation_version", 0) != presentation_version
            or not file_keyboard
            and parent.payload.get("product_keyboard_basis") != descriptors
        ):
            raise CallbackRefused() from None
        group, ids = uuid4().hex, []
        for line, buttons in enumerate(descriptors):
            for column, spec in enumerate(buttons):
                if "url" in spec:
                    continue
                identity = uuid4().hex
                ids.append(identity)
                session.add(
                    TelegramCallbackRow(
                        id=identity,
                        group_id=group,
                        link_id=link.id,
                        chat_id=chat_id,
                        binding_generation=link.binding_generation,
                        message_id=message_id,
                        message_thread_id=topic,
                        kind=spec["kind"],
                        target_kind=spec["target_kind"],
                        target_id=spec["target_id"],
                        payload={
                            **spec["payload"],
                            "label": spec["label"],
                            **({"label_values": spec["label_values"]} if "label_values" in spec else {}),
                            "keyboard_row": line,
                            "keyboard_column": column,
                            "anchor_outbox_id": anchor_outbox_id,
                            "presentation_version": presentation_version,
                            "presentation_pending": False,
                        },
                        expires_at=datetime.fromisoformat(spec["expires_at"]) if spec["expires_at"] else None,
                        created_at=self.clock(),
                    )
                )
        await session.flush()
        return WrittenCallbackGroup(group, tuple(ids))

    async def _locked(self, session, link, callback_id):
        await self._binding(session, link)
        selected = await session.get(TelegramCallbackRow, callback_id)
        if selected is None or selected.link_id != link.id:
            raise CallbackRefused() from None
        anchor = selected.payload.get("anchor_outbox_id")
        parent = await session.get(TelegramOutboxRow, anchor, with_for_update=True, populate_existing=True) if type(anchor) is str else None
        rows = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == selected.group_id).order_by(TelegramCallbackRow.id).with_for_update().execution_options(populate_existing=True)))
        version = parent.payload.get("presentation_version", 0) if parent else None
        if (
            parent is None
            or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id)
            or type(version) is not int
            or not rows
            or any(
                (row.link_id, row.chat_id, row.binding_generation, row.message_thread_id, row.payload.get("anchor_outbox_id"), row.payload.get("presentation_version"))
                != (link.id, link.chat_id, link.binding_generation, parent.message_thread_id, parent.id, version)
                or row.payload.get("presentation_pending") is not False
                for row in rows
            )
        ):
            raise CallbackRefused() from None
        selected = next((row for row in rows if row.id == callback_id), None)
        if selected is None:
            raise CallbackRefused() from None
        _validate_rows(parent, rows)
        return parent, rows, selected

    def _usable(self, row):
        now = self.clock()
        created = _aware(row.created_at)
        too_old = now >= created + timedelta(days=self.config.callbacks.max_age_days) or row.target_kind == "draft" and now >= created + timedelta(hours=self.config.callbacks.draft_ttl_hours)
        if row.consumed_at is not None or too_old or row.expires_at is not None and now >= _aware(row.expires_at):
            raise CallbackRefused() from None

    async def adopt(self, link, group_id, message_id, *, chat_id, message_thread_id, outbox_id, presentation_version, check_current):
        if type(message_id) is not int or message_id <= 0 or type(chat_id) is not int or chat_id != link.chat_id or type(presentation_version) is not int:
            return False

        async def perform():
            try:
                if check_current() is not True:
                    return False
                async with self._writer() as session:
                    # Discover one pointer, then always lock anchor before group.
                    identity = await session.scalar(select(TelegramCallbackRow.id).where(TelegramCallbackRow.group_id == group_id, TelegramCallbackRow.link_id == link.id).order_by(TelegramCallbackRow.id).limit(1))
                    parent, rows, _ = await self._locked(session, link, identity)
                    if (
                        parent.id != outbox_id
                        or parent.payload.get("presentation_version", 0) != presentation_version
                        or _topic(message_thread_id) != parent.message_thread_id
                        or any(row.message_id not in {None, message_id} for row in rows)
                    ):
                        raise CallbackRefused()
                    for row in rows:
                        self._usable(row)
                        if row.message_id is None:
                            row.payload = {**row.payload, "message_adopted": True}
                        row.message_id = message_id
                    await session.flush()
                    if check_current() is not True:
                        raise CallbackRefused()
                return True
            except CallbackRefused:
                return False

        return await await_drained(perform())

    async def lock_current(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard, session):
        current = await guard.check(session)
        if not isinstance(current, BotLink) or any(getattr(current, name) != getattr(link, name) for name in _SCOPE):
            raise CallbackRefused() from None
        parent, rows, selected = await self._locked(session, link, callback_id)
        if type(message_id) is not int or message_id <= 0 or type(chat_id) is not int or (selected.chat_id, selected.message_id, selected.message_thread_id) != (chat_id, message_id, _topic(message_thread_id)):
            raise CallbackRefused() from None
        self._usable(selected)
        current = await session.get(TelegramBotLinkRow, link.id, populate_existing=True)
        if current.acts_paused_since is not None and await self._paused(session, selected, current):
            raise CallbackRefused("link.limited") from None
        return CallbackGroup(_row(selected), tuple(_row(row) for row in rows), parent.id, parent.payload.get("presentation_version", 0))

    async def _paused(self, session, row, link):
        original_act = await session.get(TelegramActRow, row.payload.get("act_id")) if row.kind == "act_check" and type(row.payload.get("act_id")) is str else None
        original_prompt = await session.get(TelegramCallbackRow, row.payload.get("prompt_id")) if row.kind == "prompt_reopen" and type(row.payload.get("prompt_id")) is str else None
        if row.kind == "act_check" and (original_act is None or (original_act.link_id, original_act.owner_user_id, original_act.binding_generation) != (row.link_id, link.owner_user_id, row.binding_generation)):
            raise CallbackRefused() from None
        if row.kind == "prompt_reopen" and (
            original_prompt is None or (original_prompt.link_id, original_prompt.chat_id, original_prompt.message_thread_id, original_prompt.binding_generation) != (row.link_id, row.chat_id, row.message_thread_id, row.binding_generation)
        ):
            raise CallbackRefused() from None
        return pause_covers(
            row.kind,
            row.payload,
            link,
            original_act_kind=original_act.kind if original_act else None,
            original_act_body=original_act.body if original_act else None,
            original_prompt_kind=original_prompt.payload.get("purpose") if original_prompt else None,
        )

    async def consume_group(self, group, *, reason="done", session=None, link=None, guard=None):
        """Close one exact current group inside an optional UI transaction."""
        if type(group) is CallbackGroup and session is not None and link is None and guard is None:
            return await self._consume_group(group, reason=reason, session=session)
        if type(group) is not str or _ID.fullmatch(group) is None or link is None or guard is None:
            raise CallbackRefused() from None

        async def perform():
            async with self.writer(link, guard, session=session) as writer:
                selected = await writer.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group).order_by(TelegramCallbackRow.id).limit(1))
                if selected is None or (selected.link_id, selected.chat_id, selected.binding_generation) != (link.id, link.chat_id, link.binding_generation):
                    raise CallbackRefused() from None
                parent, rows, selected = await self._locked(writer, link, selected.id)
                captured = CallbackGroup(_row(selected), tuple(_row(row) for row in rows), parent.id, parent.payload.get("presentation_version", 0))
                await self._consume_group(captured, reason=reason, session=writer)

        return await await_drained(perform())

    async def _consume_group(self, group, *, reason, session):
        self._borrow(session)
        if type(group) is not CallbackGroup or type(reason) is not str or not 1 <= len(reason) <= 16:
            raise CallbackRefused() from None
        rows = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.selected.group_id).order_by(TelegramCallbackRow.id).with_for_update()))
        if tuple(row.id for row in rows) != tuple(row.id for row in group.rows) or any(
            (row.link_id, row.chat_id, row.binding_generation, row.message_id, row.message_thread_id, row.payload.get("anchor_outbox_id"), row.payload.get("presentation_version"), row.payload.get("presentation_pending"))
            != (old.link_id, old.chat_id, old.binding_generation, old.message_id, old.message_thread_id, group.anchor_outbox_id, group.presentation_version, False)
            for row, old in zip(rows, group.rows)
        ):
            raise CallbackRefused() from None
        for row in rows:
            if row.consumed_at is None:
                row.consumed_at, row.closed_reason = self.clock(), reason
        await session.flush()

    async def for_message(self, link_id, message_id, *, chat_id, message_thread_id, binding_generation):
        topic = _topic(message_thread_id)

        async def perform():
            async with self.sessions() as session:
                link = await session.get(TelegramBotLinkRow, link_id)
                if link is None or (link.chat_id, link.binding_generation) != (chat_id, binding_generation):
                    return None
                rows = list(
                    await session.scalars(
                        select(TelegramCallbackRow)
                        .where(TelegramCallbackRow.link_id == link_id, TelegramCallbackRow.chat_id == chat_id, TelegramCallbackRow.binding_generation == binding_generation)
                        .order_by(TelegramCallbackRow.created_at, TelegramCallbackRow.id)
                    )
                )
                found, current_rows, checked_groups = [], [], {}
                for row in rows:
                    if row.message_thread_id != topic:
                        continue
                    if row.kind == "prompt" and "prompt_visibility" in row.payload and not Prompts.is_visible(_row(row)):
                        continue
                    current = row.message_id == message_id
                    history = row.payload.get("presentation_history", [])
                    alias = any(type(item) is dict and item.get("message_id") == message_id and item.get("message_thread_id") == topic for item in history) if isinstance(history, list) else False
                    unbound_prompt = row.kind == "prompt" and row.message_id is None and type(row.payload.get("anchor_outbox_id")) is str
                    if not current and not alias and not unbound_prompt:
                        continue
                    anchor = await session.get(TelegramOutboxRow, row.payload.get("anchor_outbox_id")) if type(row.payload.get("anchor_outbox_id")) is str else None
                    parent_scope = anchor is not None and (anchor.link_id, anchor.owner_user_id, anchor.chat_id, anchor.binding_generation, anchor.message_thread_id) == (
                        row.link_id,
                        link.owner_user_id,
                        row.chat_id,
                        row.binding_generation,
                        row.message_thread_id,
                    )
                    acknowledged = parent_scope and (
                        row.payload.get("message_adopted") is True
                        and current
                        or any(part.get("state") == "sent" and message_id in part.get("message_ids", []) and part.get("presentation_version") == row.payload.get("presentation_version") for part in anchor.parts)
                    )
                    metadata_current = (
                        parent_scope and type(row.payload.get("presentation_version")) is int and row.payload.get("presentation_version") == anchor.payload.get("presentation_version", 0) and row.payload.get("presentation_pending") is False
                    )
                    if unbound_prompt:
                        if not acknowledged:
                            continue
                        current = True
                    if row.group_id not in checked_groups:
                        complete = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == row.group_id).order_by(TelegramCallbackRow.id).execution_options(populate_existing=True)))
                        try:
                            if row.kind != "prompt":
                                if not parent_scope:
                                    raise CallbackRefused()
                                _validate_rows(anchor, complete)
                            for member in complete:
                                self._usable(member)
                                if member.message_id != message_id and not (unbound_prompt and member.kind == "prompt" and member.message_id is None):
                                    raise CallbackRefused()
                            checked_groups[row.group_id] = True
                        except CallbackRefused:
                            checked_groups[row.group_id] = False
                    snapshot = _row(row)
                    try:
                        self._usable(row)
                        legacy_prompt = row.kind == "prompt" and "anchor_outbox_id" not in row.payload
                        if current and checked_groups[row.group_id] and (legacy_prompt or acknowledged and metadata_current):
                            current_rows.append(snapshot)
                    except CallbackRefused:
                        pass
                    found.append(snapshot)
                # A list's ticks each draw a keyboard, and they are still one card.
                if current_rows and len({row.payload.get("form_id") or row.group_id for row in current_rows}) == 1:
                    return CallbackMessageRoute(tuple(current_rows), False)
                prompts = [row for row in current_rows if row.kind == "prompt"]
                controls = [row for row in current_rows if row.kind != "prompt"]
                # A typed field can share its acknowledged presentation with
                # a separately owned control group, such as its Skip button.
                if len(prompts) == 1 and controls and len({row.group_id for row in controls}) == 1 and prompts[0].group_id != controls[0].group_id:
                    presentation = (prompts[0].payload.get("anchor_outbox_id"), prompts[0].payload.get("presentation_version"))
                    if type(presentation[0]) is str and all((row.payload.get("anchor_outbox_id"), row.payload.get("presentation_version")) == presentation for row in controls):
                        return CallbackMessageRoute(tuple(current_rows), False)
                return CallbackMessageRoute(tuple(found), True) if found else None

        return await await_drained(perform())

    async def read_keyboard(self, captured):
        from app.telegram_bots.producer import DeliveryClaimRetired

        async def perform():
            async with self.sessions() as session:
                link = await session.get(TelegramBotLinkRow, captured.link.id)
                parent = await session.get(TelegramOutboxRow, captured.item.id)
                if (
                    link is None
                    or parent is None
                    or any(getattr(link, key) != getattr(captured.link, key) for key in _SCOPE)
                    or link.token_revision != captured.token_revision
                    or parent.state != "sending"
                    or (parent.claim_owner, parent.claim_id, parent.binding_generation) != (captured.claim_owner, captured.claim_id, captured.link.binding_generation)
                ):
                    raise DeliveryClaimRetired() from None
                version = parent.payload.get("presentation_version", 0)
                part = next((part for part in parent.parts if part.get("part_id") == captured.part.get("part_id") and part.get("presentation_version") == version and part.get("state") == "sending"), None)
                if part is None or part != captured.part:
                    raise DeliveryClaimRetired() from None
                group = part.get("callback_group_id")
                rows = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group).order_by(TelegramCallbackRow.id)))
                try:
                    descriptors, positions = _validate_rows(parent, rows, part=part)
                except CallbackRefused:
                    raise DeliveryClaimRetired() from None
                result = []
                expected = 0
                for line, buttons in enumerate(descriptors):
                    rendered = []
                    for column, spec in enumerate(buttons):
                        if "url" in spec:
                            rendered.append(StoredButton(spec["label"], url=spec["url"], label_values=_freeze(spec.get("label_values"))))
                            continue
                        expected += 1
                        row = positions.get((line, column))
                        if row is None or (
                            row.link_id,
                            row.chat_id,
                            row.binding_generation,
                            row.message_thread_id,
                            row.payload.get("anchor_outbox_id"),
                            row.payload.get("presentation_version"),
                            row.payload.get("presentation_pending"),
                            row.payload.get("label"),
                        ) != (link.id, parent.chat_id, link.binding_generation, parent.message_thread_id, parent.id, version, False, spec["label"]):
                            raise DeliveryClaimRetired() from None
                        try:
                            self._usable(row)
                        except CallbackRefused:
                            return ()
                        rendered.append(StoredButton(spec["label"], callback_data=CALLBACK_PREFIX + row.id, label_values=_freeze(spec.get("label_values"))))
                    result.append(tuple(rendered))
                if len(rows) != expected:
                    raise DeliveryClaimRetired() from None
                return tuple(result)

        return await await_drained(perform())

    async def sweep(self, now):
        async def perform():
            async with self._writer() as session:
                rows = list(await session.scalars(select(TelegramCallbackRow).order_by(TelegramCallbackRow.id).with_for_update()))
                pending_groups = {row.group_id for row in rows if callback_publication_pending(row.payload)}
                removed = 0
                for row in rows:
                    if row.group_id in pending_groups:
                        continue
                    age = _aware(now) - _aware(row.created_at)
                    if row.kind == "prompt" and row.consumed_at is None and "prompt_visibility" in row.payload and _visibility(row.payload["prompt_visibility"])["state"] == "pending":
                        continue
                    if row.kind == "prompt" and row.consumed_at is None and row.expires_at is not None and _aware(now) >= _aware(row.expires_at):
                        row.consumed_at, row.closed_reason = now, "expired"
                    anchor = await session.get(TelegramOutboxRow, row.payload.get("anchor_outbox_id")) if type(row.payload.get("anchor_outbox_id")) is str else None
                    if anchor is not None and anchor.state in {"pending", "sending", "failed"}:
                        continue
                    closed = row.consumed_at or row.expires_at
                    if age >= timedelta(days=self.config.callbacks.max_age_days) or row.kind != "prompt" and closed is not None and _aware(now) - _aware(closed) >= timedelta(days=self.config.callbacks.sweep_after_days):
                        await session.delete(row)
                        removed += 1
                return removed

        return await await_drained(perform())


def _visibility(value):
    value = _plain(value)
    if (
        type(value) is not dict
        or set(value) != {"state", "part_id", "ttl_seconds"}
        or value["state"] not in {"pending", "visible"}
        or type(value["ttl_seconds"]) is not int
        or value["ttl_seconds"] <= 0
        or value["part_id"] is not None
        and (type(value["part_id"]) is not str or _ID.fullmatch(value["part_id"]) is None)
    ):
        raise CallbackRefused() from None
    return value


def _arm_visible_prompt(row, part):
    visibility = _visibility(row.payload["prompt_visibility"])
    if row.consumed_at is not None or "prompt_successor" in row.payload or visibility["state"] != "pending" or visibility["part_id"] != part["part_id"] or part["state"] != "sent" or not part["message_ids"]:
        return
    try:
        acknowledged_at = _aware(datetime.fromisoformat(part["sent_at"]))
    except (KeyError, TypeError, ValueError):
        raise CallbackRefused() from None
    row.payload = {**row.payload, "prompt_visibility": {**visibility, "state": "visible"}}
    row.message_id = part["message_ids"][-1]
    row.created_at = acknowledged_at
    row.expires_at = acknowledged_at + timedelta(seconds=visibility["ttl_seconds"])


class Prompts:
    def __init__(self, repository):
        self.repository = repository

    @asynccontextmanager
    async def _unit(self, link, guard, session):
        if guard is not None:
            async with self.repository.writer(link, guard, session=session) as writer:
                yield writer
        elif session is not None:
            self.repository._borrow(session)
            await self.repository._binding(session, link)
            yield session
        else:
            async with self.repository._writer() as writer:
                await self.repository._binding(writer, link)
                yield writer

    async def _rows(self, writer, link, chat_id, topic):
        return list(
            await writer.scalars(
                select(TelegramCallbackRow)
                .where(
                    TelegramCallbackRow.link_id == link.id,
                    TelegramCallbackRow.chat_id == chat_id,
                    TelegramCallbackRow.binding_generation == link.binding_generation,
                    TelegramCallbackRow.kind == "prompt",
                    TelegramCallbackRow.message_thread_id == topic,
                )
                .order_by(TelegramCallbackRow.created_at.desc(), TelegramCallbackRow.id.desc())
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )

    async def open(self, link, purpose, payload, *, chat_id, message_thread_id, message_id, prompt_id=None, guard=None, session=None, defer_visibility=False):
        topic, supplied = _topic(message_thread_id), _plain(payload)
        if type(defer_visibility) is not bool:
            raise CallbackRefused() from None
        if type(purpose) is not str or not purpose or chat_id != link.chat_id or "purpose" in supplied or _RESERVED.intersection(supplied) or prompt_id is not None and (type(prompt_id) is not str or _ID.fullmatch(prompt_id) is None):
            raise CallbackRefused() from None
        if message_id is not None and (type(message_id) is not int or message_id <= 0):
            raise CallbackRefused() from None

        async def perform():
            async with self._unit(link, guard, session) as writer:
                rows = await self._rows(writer, link, chat_id, topic)
                identity = prompt_id or uuid4().hex
                existing = await writer.get(TelegramCallbackRow, identity, with_for_update=True)
                if existing is not None:
                    if (
                        existing not in rows
                        or (existing.message_id != message_id and not (defer_visibility and message_id is None and self.is_visible(_row(existing))))
                        or ("prompt_visibility" in existing.payload) is not defer_visibility
                        or {key: child for key, child in existing.payload.items() if key not in _RESERVED} != {"purpose": purpose, **supplied}
                    ):
                        raise CallbackRefused() from None
                    return identity
                for row in rows:
                    if row.consumed_at is None:
                        row.consumed_at, row.closed_reason = self.repository.clock(), "replaced"
                    row.payload = {**row.payload, "prompt_successor": identity}
                writer.add(
                    TelegramCallbackRow(
                        id=identity,
                        group_id=uuid4().hex,
                        link_id=link.id,
                        chat_id=chat_id,
                        binding_generation=link.binding_generation,
                        message_id=message_id,
                        message_thread_id=topic,
                        kind="prompt",
                        target_kind="thread" if "thread_id" in supplied else None,
                        target_id=supplied.get("thread_id"),
                        payload={"purpose": purpose, **supplied, **({"prompt_visibility": {"state": "pending", "part_id": None, "ttl_seconds": self.repository.config.prompts.ttl_seconds}} if defer_visibility else {})},
                        expires_at=None if defer_visibility else self.repository.clock() + timedelta(seconds=self.repository.config.prompts.ttl_seconds),
                        created_at=self.repository.clock(),
                    )
                )
                await writer.flush()
                return identity

        return await await_drained(perform())

    async def get(self, link, prompt_id, *, guard, session=None):
        async with self._unit(link, guard, session) as writer:
            row = await writer.get(TelegramCallbackRow, prompt_id, with_for_update=True, populate_existing=True)
            if row is None or row.kind != "prompt" or (row.link_id, row.chat_id, row.binding_generation) != (link.id, link.chat_id, link.binding_generation):
                raise CallbackRefused() from None
            return _row(row)

    async def attach(self, link, prompt_id, *, outbox_id, guard, session):
        async with self._unit(link, guard, session) as writer:
            row = await writer.get(TelegramCallbackRow, prompt_id, with_for_update=True, populate_existing=True)
            parent = await writer.get(TelegramOutboxRow, outbox_id, with_for_update=True, populate_existing=True)
            if (
                row is None
                or row.kind != "prompt"
                or parent is None
                or (row.link_id, row.chat_id, row.binding_generation, row.message_thread_id) != (link.id, link.chat_id, link.binding_generation, parent.message_thread_id)
                or (parent.link_id, parent.owner_user_id, parent.chat_id, parent.binding_generation) != (link.id, link.owner_user_id, link.chat_id, link.binding_generation)
            ):
                raise CallbackRefused() from None
            version = parent.payload.get("presentation_version", 0)
            if type(version) is not int or version < 0 or row.payload.get("anchor_outbox_id", outbox_id) != outbox_id or row.payload.get("presentation_version", version) != version:
                raise CallbackRefused() from None
            row.payload = {**row.payload, "anchor_outbox_id": outbox_id, "presentation_version": version, "presentation_pending": False}
            if "prompt_visibility" in row.payload:
                from app.telegram_bots.mixed_source import human_input_parts
                from app.telegram_bots.producer import _item

                basis = parent.payload.get("reply_basis", {})
                if basis.get("version") != 7 and not (basis.get("version") == 5 and basis.get("step_id") is not None and basis.get("outcome", {}).get("request_basis", {}).get("version") == 2):
                    raise CallbackRefused() from None
                owned = await await_drained(asyncio.to_thread(human_input_parts, _item(parent)))
                if not owned:
                    raise CallbackRefused() from None
                final = owned[-1]
                visibility = _visibility(row.payload["prompt_visibility"])
                if visibility["state"] == "pending":
                    row.payload = {**row.payload, "prompt_visibility": {**visibility, "part_id": final["part_id"]}}
                    _arm_visible_prompt(row, final)
                elif visibility["part_id"] != final["part_id"]:
                    raise CallbackRefused() from None
            await writer.flush()
            return _row(row)

    @staticmethod
    def is_visible(prompt):
        if "prompt_visibility" not in prompt.payload:
            return True
        try:
            visibility = _visibility(prompt.payload["prompt_visibility"])
            return visibility["state"] == "visible" and type(prompt.message_id) is int and prompt.message_id > 0 and prompt.expires_at is not None
        except CallbackRefused:
            return False

    async def acknowledge_part(self, parent, part, *, session):
        """Arm only the originally attached deferred prompt in the ACK writer."""
        from app.telegram_bots.mixed_source import human_input_parts
        from app.telegram_bots.producer import _item

        self.repository._borrow(session)
        basis = parent.payload.get("reply_basis", {})
        if basis.get("version") not in {5, 7}:
            return
        pending = list(
            await session.scalars(
                select(TelegramCallbackRow)
                .where(
                    TelegramCallbackRow.link_id == parent.link_id,
                    TelegramCallbackRow.binding_generation == parent.binding_generation,
                    TelegramCallbackRow.chat_id == parent.chat_id,
                    TelegramCallbackRow.kind == "prompt",
                    TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == parent.id,
                )
                .with_for_update()
            )
        )
        pending = [
            row
            for row in pending
            if "prompt_visibility" in row.payload
            and row.consumed_at is None
            and "prompt_successor" not in row.payload
            and _visibility(row.payload["prompt_visibility"])["state"] == "pending"
            and _visibility(row.payload["prompt_visibility"])["part_id"] == part["part_id"]
        ]
        if not pending:
            return
        owned = await await_drained(asyncio.to_thread(human_input_parts, _item(parent)))
        if not owned or owned[-1]["part_id"] != part["part_id"]:
            raise CallbackRefused() from None
        for row in pending:
            _arm_visible_prompt(row, part)

    async def current(self, link, chat_id, message_thread_id, *, guard=None, session=None):
        topic = _topic(message_thread_id)
        if chat_id != link.chat_id:
            raise CallbackRefused() from None

        async def perform():
            async with self._unit(link, guard, session) as writer:
                rows = await self._rows(writer, link, chat_id, topic)
                tips = [row for row in rows if "prompt_successor" not in row.payload]
                for row in tips or rows:
                    if row.consumed_at is None and "prompt_visibility" in row.payload and _visibility(row.payload["prompt_visibility"])["state"] == "pending":
                        return None
                    if row.consumed_at is None:
                        if row.expires_at is not None and self.repository.clock() >= _aware(row.expires_at):
                            row.consumed_at, row.closed_reason = self.repository.clock(), "expired"
                        return _row(row)
                    # A reply to a spent card holds that one text: no prompt ever waited there.
                    if row.closed_reason not in {"command", "navigation", "discarded", "answered"} and row.payload.get("purpose") != "retired_presentation":
                        return _row(row)
                return None

        return await await_drained(perform())

    async def close(self, link, chat_id, message_thread_id, *, reason, guard=None, session=None, shown_only=False):
        """`shown_only` spares a prompt still on its way: closing it would stop its question from being sent."""
        topic = _topic(message_thread_id)
        if chat_id != link.chat_id or reason not in {"command", "navigation", "discarded", "answered", "cancelled", "expired", "replaced"}:
            raise CallbackRefused() from None

        async def perform():
            async with self._unit(link, guard, session) as writer:
                for row in await self._rows(writer, link, chat_id, topic):
                    anchored = shown_only and type(row.payload.get("anchor_outbox_id")) is str
                    anchor = await writer.get(TelegramOutboxRow, row.payload["anchor_outbox_id"]) if anchored else None
                    if anchor is not None and anchor.state != "sent":
                        continue
                    if row.consumed_at is None or reason in {"command", "navigation", "discarded", "answered"}:
                        row.consumed_at, row.closed_reason = self.repository.clock(), reason

        await await_drained(perform())


class CallbackHandler:
    def __init__(self, repository, *, execute):
        self.repository, self.execute = repository, execute

    async def press(self, channel, query, *, captured_link=None):
        loop = asyncio.get_running_loop()
        deadline, active = loop.time() + PRESS_TIMEOUT_SECONDS, [True]
        captured = captured_link or channel.link

        def current():
            return active[0] and loop.time() < deadline and all(getattr(channel.link, key) == getattr(captured, key) for key in _SCOPE)

        async def checks():
            if not current():
                raise CallbackRefused() from None
            sender = getattr(getattr(query, "from_user", None), "id", None)
            message = getattr(query, "message", None)
            if sender != captured.telegram_user_id or type(sender) is not int:
                raise CallbackRefused("stale.other_person")
            if message is None or getattr(getattr(message, "chat", None), "id", None) != captured.chat_id:
                raise CallbackRefused("stale.other_person")
            link = await self.repository.current_link(captured)
            if not current():
                raise CallbackRefused() from None
            data = getattr(query, "data", None)
            row = await self.repository.get(data[len(CALLBACK_PREFIX) :]) if type(data) is str and data.startswith(CALLBACK_PREFIX) and _ID.fullmatch(data[len(CALLBACK_PREFIX) :]) else None
            if not current():
                raise CallbackRefused() from None
            if row is None or row.link_id != captured.id or row.binding_generation != captured.binding_generation or row.chat_id != captured.chat_id or row.message_thread_id != _topic(getattr(message, "message_thread_id", None)):
                raise CallbackRefused()
            if row.message_id is None or row.message_id == message.message_id:
                if not current() or not await self.repository.adopt(
                    captured,
                    row.group_id,
                    message.message_id,
                    chat_id=captured.chat_id,
                    message_thread_id=row.message_thread_id,
                    outbox_id=row.payload.get("anchor_outbox_id"),
                    presentation_version=row.payload.get("presentation_version"),
                    check_current=current,
                ):
                    raise CallbackRefused()
                if not current():
                    raise CallbackRefused() from None
                row = await self.repository.get(row.id)
            if row.message_id != message.message_id:
                raise CallbackRefused()
            self.repository._usable(row)
            if link is None or link.state != "connected" or link.health != "ok":
                raise CallbackRefused()
            if not current():
                raise CallbackRefused() from None
            async with self.repository.sessions() as session:
                if link.acts_paused_since is not None and await self.repository._paused(session, row, link):
                    raise CallbackRefused("link.limited")
            if not current():
                raise CallbackRefused() from None
            return row

        task = asyncio.create_task(checks(), name="telegram-callback-precheck")
        key, row = None, None
        try:
            # Leave part of the same logical deadline for the one raw ack.
            # The adapter's host-only path performs no additional SQL lookup.
            done, _pending = await asyncio.wait({task}, timeout=max(0, deadline - loop.time() - PRESS_TIMEOUT_SECONDS / 4))
            if not done:
                active[0] = False
                key = "tap.failed"  # Nothing is known about the button yet, so nothing can be said against it.
            else:
                try:
                    row = task.result()
                except CallbackRefused as error:
                    key = error.key
                except Exception:
                    key = "tap.failed"
            # One acknowledgement attempt; later results/refusals are ordinary
            # replies supplied by the host execute collaborator.
            try:
                async with asyncio.timeout_at(deadline):
                    await channel.answer_callback_ack(query.id, key, captured_link=captured, deadline=deadline)
            except TimeoutError:
                active[0] = False
            if row is not None and current():
                await self.execute(channel, row)
        finally:
            active[0] = False
            # Started SQL is not abandoned when the logical press expires.
            await await_drained(asyncio.gather(task, return_exceptions=True))
