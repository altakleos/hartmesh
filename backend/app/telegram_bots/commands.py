"""Current fixed commands and stored callback leaves; no model-input activation."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass

from sqlalchemy import text

from app.telegram_bots.callbacks import CallbackRefused, canonical_keyboard
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.texts import KEYBOARDS
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


@dataclass
class _FixedGuard:
    services: object
    channel: object
    captured_link: object
    actual: object = None

    def current(self):
        if self.channel._callback_current(self.captured_link) is not True:
            raise CallbackRefused() from None

    async def prepare(self):
        self.current()
        # Target/config work stays outside the writer. It never replaces the
        # original link, actor, saved presentation or operation identity.
        await self.services.prepare_ingress_values(self.captured_link)
        self.current()
        checker = await self.services.prepare_live_owner(self.captured_link.owner_user_id)
        self.current()
        self.actual = self.services.links.publication_guard(self.captured_link, check_live_owner=checker, check_attempt_current=lambda: self.channel._callback_current(self.captured_link))

    async def check(self, writer):
        self.current()
        if self.actual is None:
            raise CallbackRefused() from None
        return await self.actual.check(writer)


class CallbackCommands:
    def __init__(self, services):
        self.services = services

    def _guard(self, channel, link):
        return _FixedGuard(self.services, channel, link)

    async def _web_keyboard(self, link):
        from app.telegram_bots.authority import is_administrator
        from app.telegram_bots.pages import page_address

        administrator = await is_administrator(link.owner_user_id) if link.agent_kind == "lead_agent" else False
        entries = [("web.conversations", "label.open_conversations"), ("web.telegram", "label.open_telegram"), ("web.scheduled", "label.open_scheduled"), ("web.account", "label.open_product")]
        if link.agent_kind == "lead_agent":
            entries = [
                ("web.memory", "label.open_memory"),
                ("web.conversations", "label.open_conversations"),
                ("web.addAssistant", "label.add_assistant"),
                *(([("web.addEmployee", "label.add_employee")]) if administrator else []),
                *entries[1:],
            ]

        def prepare():
            return [[{"label": label, "url": address}] for key, label in entries if (address := page_address(key, link, reader_is_administrator=administrator)) is not None]

        return await await_drained(asyncio.to_thread(prepare))

    async def _presentation(self, word, link):
        values = await self.services.prepare_ingress_values(link)
        if link.agent_kind not in {"lead_agent", "custom_agent"}:
            raise CallbackRefused() from None
        if word == "new":
            return {"key": "new.confirm_plain", "values": {}, "keyboard_template": "new.confirm_plain"}
        if word == "settings":
            settings = await self.services.settings.get(link.owner_user_id)
            if settings is None:
                raise CallbackRefused() from None
            return {"key": "settings.state_basic", "values": {"language": "中文" if settings.locale == "zh-CN" else "English"}, "keyboard": canonical_keyboard(KEYBOARDS["settings.state_basic"](link=link))}
        if word == "help":
            key = "help.chat" if link.agent_kind == "lead_agent" else "help.custom_agent"
            # No voice consumer is exposed in this stage.
            return {"key": key, "values": {"product": values["product"], "agent": values["agent"], "voice_enabled": False}, "keyboard": canonical_keyboard(KEYBOARDS[key](link=link))}
        if word == "web":
            return {"key": "web.list", "values": {"product": values["product"]}, "keyboard": canonical_keyboard(await self._web_keyboard(link))}
        if word == "privacy":
            from app.telegram_bots.pages import page_address

            address = await await_drained(asyncio.to_thread(page_address, "web.telegram", link))
            keyboard = [[{"label": "label.open_telegram", "url": address}]] if address else []
            return {"key": "privacy.full", "values": {"product": values["product"]}, "keyboard": canonical_keyboard(keyboard)}
        raise CallbackRefused() from None

    async def handle(self, channel, update, word):
        link = channel._admission(update).link
        guard = self._guard(channel, link)
        await guard.prepare()
        inbox = self.services.outbox.inbox
        origin, _created = await await_drained(inbox.accept(link, update, sanitized_input={"command": word}))
        guard.current()
        if origin.binding_generation != link.binding_generation:
            raise CallbackRefused() from None
        if origin.route_complete:
            return
        if origin.route_kind is None:
            basis = await self._presentation(word, link)
            if word == "new" and origin.message_thread_id is not None:
                basis = {"key": "tap.failed", "values": {}, "keyboard": []}
            guard.current()
            origin = await await_drained(inbox.route(origin, kind="command", target_id=word, basis=basis, operation_ids=[]))
        if origin.route_kind != "command" or origin.route_target_id not in {"new", "help", "privacy", "settings", "web"}:
            raise CallbackRefused() from None
        guard.current()
        basis = origin.route_basis
        keyboard = basis.get("keyboard", [])
        if basis.get("keyboard_template") == "new.confirm_plain":
            preparation = await self.services.threads.reserve_new_conversation(link, origin=origin)
            guard.current()
            origin = preparation.origin
            keyboard = KEYBOARDS["new.confirm_plain"](thread_id=preparation.expected_thread_id or preparation.creation_id)
        if origin.route_basis.get("human_input_ui") != {"command_closed": word}:
            await self.services.prompts.close(link, origin.chat_id, origin.message_thread_id, reason="command")
        guard.current()
        part = {"kind": "text", "source_ref": {"key": basis["key"], "values": basis["values"]}}
        if basis.get("keyboard_template") != "new.confirm_plain":
            part["reply_to_message_id"] = origin.message_id
        await self.services.outbox.plan_product_keyboard(
            link,
            source_id=channel._source(update, "command"),
            parts=[part],
            keyboard=keyboard,
            callbacks=self.services.callbacks,
            guard=guard,
            inbox_rows=[origin],
            finish_route=basis.get("keyboard_template") != "new.confirm_plain",
            message_thread_id=origin.message_thread_id,
        )

    @staticmethod
    def _source_id(row, purpose, query_id=None):
        if query_id is None:
            raise CallbackRefused() from None
        if type(query_id) is not str or not query_id:
            raise CallbackRefused() from None
        digest = hashlib.sha256((row.id + ":" + query_id + ":" + purpose).encode("utf-8")).hexdigest()
        return "callback:" + digest

    async def _origin(self, link, row, guard):
        """Resolve the actual parent receipt; the bot message is never an input."""
        await prepare_publication_guard(guard)
        return await await_drained(self._read_origin(link, row, guard))

    async def _read_origin(self, link, row, guard):
        async with self.services.outbox.sessions() as writer, writer.begin():
            if writer.get_bind().dialect.name == "sqlite":
                await writer.execute(text("BEGIN IMMEDIATE"))
            group = await self.services.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id)
            origins = parent.payload.get("product_basis", {}).get("origins") if parent else None
            if not isinstance(origins, list) or len(origins) != 1 or type(origins[0]) is not list or len(origins[0]) != 3 or (origins[0][0], origins[0][2]) != (link.id, link.chat_id):
                raise CallbackRefused() from None
            actual = await writer.get(TelegramInboundMarkRow, tuple(origins[0]))
            if actual is None or actual.route_kind != "command" or actual.route_target_id != "new":
                raise CallbackRefused() from None
            result = InboxRow.from_row(actual)
            await guard.check(writer)
        return result

    async def _new(self, channel, link, row, guard):
        origin = await self._origin(link, row, guard)
        claimed = await self.services.outbox.inbox.claim_callback_choice(
            link, origin, callback_id=row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, callbacks=self.services.callbacks, guard=guard
        )
        choice = claimed.route_basis["callback_choice"]
        if choice["kind"] == "new_confirm":
            await self.services.threads.new_conversation(link, origin=claimed)
            key = "new.done"
        elif choice["kind"] == "new_keep":
            key = "new.kept"
        else:
            raise CallbackRefused() from None
        await prepare_publication_guard(guard)
        await await_drained(self._finish_new(link, row, guard, claimed, key))

    async def _finish_new(self, link, row, guard, claimed, key):
        async with self.services.outbox.sessions() as writer, writer.begin():
            if writer.get_bind().dialect.name == "sqlite":
                await writer.execute(text("BEGIN IMMEDIATE"))
            group = await self.services.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            await self.services.outbox.plan_product_keyboard(
                link,
                source_id="choice:" + row.id,
                parts=[{"kind": "edit", "target_message_id": row.message_id, "source_ref": {"key": key, "values": {}}}],
                keyboard=[],
                callbacks=self.services.callbacks,
                guard=guard,
                inbox_rows=[claimed],
                finish_route=True,
                message_thread_id=row.message_thread_id,
                session=writer,
            )
            await self.services.callbacks.consume_group(group, reason="done", session=writer)
            await writer.flush()
            await guard.check(writer)

    async def execute(self, channel, row, *, query_id=None):
        link = channel.link
        if row.link_id != link.id or row.binding_generation != link.binding_generation or row.chat_id != link.chat_id or row.message_id is None:
            raise CallbackRefused() from None
        guard = self._guard(channel, link)
        guard.current()
        if row.kind == "settings_set":
            return await self.services.acts.apply_settings(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard)
        if row.kind in {"new_confirm", "new_keep"}:
            return await self._new(channel, link, row, guard)
        keyboard = await self._web_keyboard(link) if row.kind == "web_list" else []
        await prepare_publication_guard(guard)
        result = await await_drained(self._execute_local(link, row, guard, keyboard, query_id))
        if row.kind == "web_list":
            guard.current()
            await self.services.prompts.close(link, row.chat_id, row.message_thread_id, reason="navigation")
            guard.current()
        return result

    async def _execute_local(self, link, row, guard, keyboard, query_id):
        async with self.services.outbox.sessions() as writer, writer.begin():
            if writer.get_bind().dialect.name == "sqlite":
                await writer.execute(text("BEGIN IMMEDIATE"))
            group = await self.services.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            selected = group.selected
            if selected.kind != row.kind:
                raise CallbackRefused() from None
            if selected.kind == "delivery_retry":
                identity = selected.target_id
                if selected.target_kind != "outbox" or identity is None or selected.payload.get("outbox_id", identity) != identity:
                    raise CallbackRefused() from None
                target = await writer.get(TelegramOutboxRow, identity)
                if target is None or (target.link_id, target.owner_user_id, target.binding_generation, target.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
                    raise CallbackRefused() from None
                await self.services.outbox.retry(link, identity, session=writer)
            else:
                if selected.kind == "web_list":
                    key, values = "web.list", {"product": self.services.product_name}
                    part = {"kind": "text", "source_ref": {"key": key, "values": values}, "reply_to_message_id": row.message_id}
                elif selected.kind == "dismiss":
                    parent = await writer.get(TelegramOutboxRow, group.anchor_outbox_id)
                    source = parent.payload["product_basis"]["parts"][-1]["source_ref"]
                    part = {"kind": "edit", "target_message_id": row.message_id, "source_ref": source, "remove_keyboard": True}
                else:
                    part = {"kind": "text", "reply_to_message_id": row.message_id, "source_ref": {"key": "tap.failed", "values": {}}}
                    keyboard = []
                source_id = "callback:" + row.id if selected.kind == "dismiss" else self._source_id(selected, selected.kind, query_id)
                await self.services.outbox.plan_product_keyboard(link, source_id=source_id, parts=[part], keyboard=keyboard, callbacks=self.services.callbacks, guard=guard, message_thread_id=row.message_thread_id, session=writer)
                if selected.kind == "dismiss":
                    await self.services.callbacks.consume_group(group, reason="done", session=writer)
            await writer.flush()
            await guard.check(writer)
