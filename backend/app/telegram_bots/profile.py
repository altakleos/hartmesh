"""A bot's public profile: its name, descriptions, command menu and picture."""

from __future__ import annotations

import asyncio
import logging
import secrets
from datetime import UTC, datetime, timedelta

from telegram import BotCommand, BotCommandScopeAllPrivateChats, InputProfilePhotoStatic, MenuButtonCommands

from app.telegram_bots.avatar import CHAT_COLOR, color_of, initials_of, render_avatar
from app.telegram_bots.render import render_plain
from deerflow.runtime.keyed_lock import AsyncKeyedLockTable
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)
PROFILE_VERSION = 1
ALL_PARTS = frozenset({"name", "descriptions", "commands", "picture"})
_MENUS = {"lead_agent": ("new", "stop", "settings", "help"), "custom_agent": ("new", "stop", "help")}
_TEXTS = {"lead_agent": "chat", "custom_agent": "custom_agent"}
_USERNAME_RANDOM = "abcdefghjkmnpqrstuvwxyz23456789"
_NAME_MAX = 64


def display_name_of(name: str) -> str:
    """A target's name as a bot's display name, cut at a word when it must be."""
    if len(name) <= _NAME_MAX:
        return name
    head = name[:62]
    return (head[: head.rindex(" ")] if " " in head else head) + "…"


def suggested_username(name: str) -> str:
    """``<slug>_<random>_bot``, from letters and digits of the target's name."""
    slug = "".join(character for character in name.lower() if character.isascii() and character.isalnum())[:20]
    if not slug or not slug[0].isalpha():
        slug = ("ai_" + slug).rstrip("_")
    return f"{slug}_{''.join(secrets.choice(_USERNAME_RANDOM) for _ in range(4))}_bot"


class _Retired(Exception):
    """The bot's client changed during a push; its replacement owes the push."""


class ProfileSync:
    """Sets a bot's public profile on its running client. It owns no worker."""

    def __init__(self, services):
        self.services = services
        self._locks = AsyncKeyedLockTable()
        self._failing_since: dict[str, datetime] = {}

    def _owed(self, link) -> bool:
        return link.agent_kind in _MENUS and (link.profile_pending or link.profile_version < PROFILE_VERSION)

    async def push_owed(self, *, owner_user_id: str | None = None) -> None:
        """Pushes owed links; naming an owner is a person's act, so it reopens their retries."""
        limit = timedelta(hours=self.services.config.profile.retry_hours)
        now = datetime.now(UTC)
        for channel in self.services.channels():
            if owner_user_id is not None and channel.link.owner_user_id != owner_user_id:
                continue
            async with self._locks.hold(channel.link.id):
                link = await await_drained(channel.refresh_link())
                if link is not None and self._owed(link) and (owner_user_id is not None or now - self._failing_since.get(link.id, now) < limit):
                    await self._attempt(link, ALL_PARTS)

    async def push(self, link, *, parts: frozenset[str] = ALL_PARTS) -> bool:
        async with self._locks.hold(link.id):
            return await self._attempt(link, parts)

    async def _attempt(self, link, parts) -> bool:
        try:
            await self._push(link, parts)
        except asyncio.CancelledError:
            raise
        except _Retired:
            return False
        except Exception as error:
            self._failing_since.setdefault(link.id, datetime.now(UTC))
            logger.warning("telegram_bots profile_push_failed link=%s error=%s", link.id, type(error).__name__)
            return False
        self._failing_since.pop(link.id, None)
        return True

    async def _push(self, link, parts) -> None:
        services = self.services
        channel = services.channel(link.id)
        if link.agent_kind not in _MENUS or channel is None:
            raise _Retired()

        def check():
            if services.channel(link.id) is not channel or channel.transport_revision != link.token_revision or not channel.application.running or channel._stopping or not services.send_allowed():
                raise _Retired()

        check()
        await channel.read_bot_flags()
        settings = await services.settings.get(link.owner_user_id)
        values = await services.prepare_ingress_values(link)
        locale, product, kind = settings.locale, values["product"], _TEXTS[link.agent_kind]
        lead = link.agent_kind == "lead_agent"
        name = render_plain("profile.name.chat", locale, product=product) if lead else display_name_of(values["name"])
        bot = channel.bot
        check()
        if "descriptions" in parts:
            await bot.set_my_short_description(short_description=render_plain(f"profile.short.{kind}", locale, product=product))
            check()
            await bot.set_my_description(description=render_plain(f"profile.description.{kind}", locale, product=product))
            check()
        if "commands" in parts:
            commands = [BotCommand(word, render_plain(f"menu.{word}", locale)) for word in _MENUS[link.agent_kind]]
            await bot.set_my_commands(commands, scope=BotCommandScopeAllPrivateChats())
            check()
            await bot.set_chat_menu_button(menu_button=MenuButtonCommands())
            check()
        if "picture" in parts:
            initials, color = ("", CHAT_COLOR) if lead else (initials_of(values["name"]), color_of(link.agent_ref))
            picture = await await_drained(asyncio.to_thread(render_avatar, initials, color, link.agent_kind))
            check()
            await bot.set_my_profile_photo(photo=InputProfilePhotoStatic(picture))
            check()
        if "name" in parts:
            # Last: Telegram rations this call, and its long wait must not strand the rest.
            await bot.set_my_name(name=name)
            check()
        saved = await services.links.finish_profile(link.id, token_revision=link.token_revision, version=PROFILE_VERSION, bot_name=name, locale=locale, at=datetime.now(UTC))
        if saved is None:
            raise _Retired()
