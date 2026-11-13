"""Optional chat preference pages, using the existing atomic settings action."""

import asyncio
from dataclasses import dataclass

from sqlalchemy import select

from app.telegram_bots.callbacks import CALLBACK_METADATA_KEYS, CallbackRefused
from app.telegram_bots.message_settings import present
from app.telegram_bots.message_settings_custody import SettingsGroupGuard, require_settings_group
from app.telegram_bots.pages import page_address
from app.telegram_bots.settings import _snapshot
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class SettingsPresentation:
    product: str
    settings_url: str | None

    def show(self, settings, *, link, page="state", notice=None):
        return present(settings, link=link, product=self.product, settings_url=self.settings_url, page=page, notice=notice)


class MessageSettings:
    def __init__(self, services):
        self.services = services

    async def context(self, link):
        values = await self.services.prepare_ingress_values(link)
        address = await await_drained(asyncio.to_thread(page_address, "web.timezone", link))
        return SettingsPresentation(values["product"], address)

    async def presentation(self, link):
        context = await self.context(link)
        settings = await self.services.settings.get(link.owner_user_id)
        if settings is None:
            raise CallbackRefused()
        return context.show(settings, link=link)

    async def execute_callback(self, channel, row, *, query_id=None):
        if row.kind not in {"settings_open", "settings_set", "settings_prompt"} and not (row.kind == "back" and row.payload.get("settings_page") == "state"):
            return False
        link = channel.link
        guard = self.services.commands._guard(channel, link)
        context = await self.context(link)
        if row.kind == "settings_set":
            await self.services.acts.apply_settings(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, presentation=context)
            profiles = getattr(self.services, "profile_sync", None)
            if profiles is not None and row.payload.get("field") == "locale":
                channel._spawn_ingress(profiles.push_owed(owner_user_id=link.owner_user_id))
            return True
        if row.kind == "settings_prompt":
            raise CallbackRefused()
        if type(query_id) is not str or not 1 <= len(query_id) <= 256:
            raise CallbackRefused()

        guard = SettingsGroupGuard(guard, self.services.callbacks)

        async def perform():
            async with self.services.callbacks.writer(link, guard) as writer:
                group = await self.services.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                await require_settings_group(writer, group)
                guard.group = group
                selected = group.selected
                payload = {key: value for key, value in selected.payload.items() if key not in CALLBACK_METADATA_KEYS}
                if (selected.target_kind, selected.target_id) != ("link", link.id):
                    raise CallbackRefused()
                if selected.kind == "back" and payload == {"settings_page": "state"}:
                    page = "state"
                elif selected.kind == "settings_open" and set(payload) == {"field"} and payload["field"] in {"notify", "quiet", "summary", "locale"}:
                    page = payload["field"]
                else:
                    raise CallbackRefused()
                preferences = await writer.scalar(select(TelegramPersonSettingsRow).where(TelegramPersonSettingsRow.owner_user_id == link.owner_user_id).execution_options(populate_existing=True))
                if preferences is None:
                    raise CallbackRefused()
                source = context.show(_snapshot(preferences), link=link, page=page)
                await self.services.outbox.plan_product_keyboard(
                    link,
                    source_id="settings-page:" + digest([row.id, query_id]),
                    parts=[{"kind": "edit", "target_message_id": row.message_id, "source_ref": {"key": source["key"], "values": source["values"]}}],
                    keyboard=source["keyboard"],
                    callbacks=self.services.callbacks,
                    guard=guard,
                    message_thread_id=row.message_thread_id,
                    session=writer,
                )
                await self.services.callbacks.consume_group(group, reason="superseded", session=writer)
                await guard.check(writer)

        await await_drained(perform())
        return True
