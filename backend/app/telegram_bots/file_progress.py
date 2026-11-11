"""Disposable save hints, owned by one direct file operation."""

import secrets

from telegram import ReplyParameters
from telegram.constants import ParseMode

from app.telegram_bots.split import clip_draft


class FileProgress:
    def __init__(self, channel, link, origin, file, current):
        self.channel, self.link, self.origin, self.file, self.current = channel, link, origin, file, current
        self.draft_id = secrets.randbelow((1 << 63) - 1) + 1
        self.message_id = None
        self.drafted = False

    async def show(self, key):
        if key not in {"file.saving", "file.waiting"}:
            raise ValueError("invalid-file-progress")
        if not self.current():
            return
        view = await self.channel._services.authority.instance_view(self.link)
        text = clip_draft(await self.channel._render_source(self.link, {"key": key, "values": {"file": self.file["filename"], "name": view.name}}), 512)
        protected = await self.channel._services.protect_notice(self.channel._context(self.link, self.link.chat_id, "bound_message"))
        common = dict(is_current=self.current, protected=protected, text=text, parse_mode=ParseMode.HTML)
        if protected is False and self.message_id is None:
            result = await self.channel.live_request(self.link, "send_message_draft", draft_id=self.draft_id, message_thread_id=self.origin.message_thread_id, **common)
            self.drafted = self.drafted or result is not None
        elif protected is True or self.message_id is not None:
            # A protected message stays protected when the policy is relaxed.
            if self.message_id is None:
                result = await self.channel.live_request(
                    self.link,
                    "send_message",
                    protect_content=True,
                    disable_notification=True,
                    message_thread_id=self.origin.message_thread_id,
                    reply_parameters=ReplyParameters(message_id=self.file["message_id"], allow_sending_without_reply=True),
                    **common,
                )
                if result is not None:
                    self.message_id = result.message_id
            else:
                await self.channel.live_request(self.link, "edit_message_text", message_id=self.message_id, **common)

    async def close(self):
        if self.drafted:
            await self.channel.live_request(self.link, "send_message_draft", is_current=self.current, protected=False, draft_id=self.draft_id, text="", message_thread_id=self.origin.message_thread_id)
        if self.message_id is not None:
            await self.channel.live_request(self.link, "delete_message", is_current=self.current, message_id=self.message_id)
