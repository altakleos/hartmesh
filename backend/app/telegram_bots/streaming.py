"""Ephemeral previews; only acknowledged qualified source spans reach a final."""

from __future__ import annotations

import asyncio
from uuid import uuid4

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode

from app.telegram_bots.markdown import to_html
from app.telegram_bots.split import clip_draft, utf16_units, visible_text
from app.telegram_bots.stopped_preview import preview_source_slices

DRAFT_INTERVAL_SECONDS = EDIT_INTERVAL_SECONDS = 1.0
DRAFT_KEEPALIVE_SECONDS = 20.0


class LivePresentation:
    def __init__(self, *, channel, link, draft_id, topic, is_current, observer, origins=(), reply_to_message_id=None):
        self.channel, self.link, self.draft_id, self.topic = channel, link, draft_id, topic
        self.is_current, self.observer, self.origins = is_current, observer, tuple(origins)
        actual_reply = self.origins[0].message_id if self.origins else draft_id
        self.reply_to_message_id = actual_reply if reply_to_message_id is None else reply_to_message_id
        if type(self.reply_to_message_id) is not int or self.reply_to_message_id <= 0 or self.origins and self.reply_to_message_id != actual_reply:
            raise ValueError("invalid-preview-reply")
        self.mode = channel._services.config.streaming
        self.protected = False
        self.closed = False
        self.paused = False
        self.started = False
        self._last_send = float("-inf")
        self._dirty, self._redisplay = False, False
        self._preview, self._activity = None, ""
        self._last_html = None
        self._parts = []
        self._anchor_outbox = None
        self._anchor = None
        self._keyboard = None
        self._lock = asyncio.Lock()
        self._identity = uuid4().hex
        self._source_start = 0
        self._model_span = None
        self._shown_anchor = None
        self._revision = 0

    def _current(self, *, include_paused=False):
        return not self.closed and (include_paused or not self.paused) and self.is_current() is True and self.channel._callback_current(self.link)

    async def _call(self, method, *, _cleanup=False, **kwargs):
        def current():
            return self._current(include_paused=_cleanup)

        if not current():
            return None
        return await self.channel.live_request(self.link, method, is_current=current, protected=self.protected, **kwargs)

    async def start(self):
        async with self._lock:
            await self._start()

    async def _start(self):
        if self.started or not self._current():
            return
        self.protected = await self.channel._services.protect_notice(self.channel._context(self.link, self.link.chat_id, "bound_message"))
        if type(self.protected) is not bool or not self._current():
            return
        if self.protected:
            self.mode = "edit"
        self.started = True
        if self.mode == "draft":
            result = await self._call("send_message_draft", draft_id=self.draft_id, text="", message_thread_id=self.topic, api_kwargs={"can_stop": True, "keep_on_stop": True})
            self._last_send = self.channel._time()
            if result:
                self._last_html = ""
        else:
            await self._plan_anchor()

    async def activity(self, key, values):
        if not self._current():
            return
        # The observer admits catalog keys and sanitized basenames/hosts.
        self._activity = "" if key is None else clip_draft(await self.channel._render_source(self.link, {"key": key, "values": values}), 512)
        self._revision += 1
        self._dirty = True

    async def preview(self, value):
        from app.telegram_bots.progress import ObservedPreview

        if type(value) is not ObservedPreview or not self._current():
            return
        self._preview, self._dirty = value, True
        self._revision += 1

    def message_sent(self, *, chat_id, topic):
        if self.mode == "draft" and self.started and self._current() and chat_id == self.link.chat_id and (None if topic == 1 else topic) == self.topic:
            self._redisplay = True
            self._revision += 1

    def matches_stop(self, *, chat_id, draft_id):
        return self.mode == "draft" and self.started and self._current() and type(chat_id) is int and chat_id == self.link.chat_id and type(draft_id) is int and draft_id == self.draft_id

    def _render(self, raw):
        html = to_html(raw, partial=True).html if raw else ""
        if self._activity:
            line = "<i>" + self._activity + "</i>"
            budget = 4096 - utf16_units(visible_text(self._activity)) - (1 if html else 0)
            return (clip_draft(html, max(1, budget)) + "\n" if html else "") + line
        return clip_draft(html, 4096) if html else ""

    def _fit(self, start, end, raw):
        limit = 4096 - utf16_units(visible_text(self._activity)) - (1 if self._activity else 0)
        units = utf16_units(to_html(raw, partial=True).plain)
        while units > limit and raw:
            # In proportion: a labelled table renders longer than its source, so a fixed step shows rows and takes them back.
            raw = raw[: min(len(raw) - 1, len(raw) * limit // units)]
            end = start + len(raw)
            units = utf16_units(to_html(raw, partial=True).plain)
        return start, end, raw

    def _shown(self, preview, *, start, end, part_id, index, message_id=None):
        if preview is not None and end > start:
            self.observer.record_shown(
                preview,
                mode=self.mode,
                part_id=part_id,
                message_id=message_id,
                draft_id=self.draft_id if self.mode == "draft" else None,
                part_index=index,
                source_start=start,
                source_end=end,
                text_prefix=preview.text,
            )
            self._shown_anchor = message_id

    async def flush(self):
        async with self._lock:
            if not self.started or not self._current():
                return
            now = self.channel._time()
            if now - self._last_send < DRAFT_INTERVAL_SECONDS:
                return
            if not self._dirty and not self._redisplay and (self.mode != "draft" or now - self._last_send < DRAFT_KEEPALIVE_SECONDS):
                return
            preview, revision = self._preview, self._revision
            if self.mode == "edit":
                await self._flush_edit(preview, revision)
                return
            raw = preview.text if preview is not None else ""
            slices = preview_source_slices(raw)
            start, end, raw = slices[0] if slices else (0, 0, "")
            start, end, raw = self._fit(start, end, raw)
            html = self._render(raw)
            result = await self._call("send_message_draft", draft_id=self.draft_id, text=html, parse_mode=ParseMode.HTML, message_thread_id=self.topic, api_kwargs={"can_stop": True, "keep_on_stop": True})
            self._last_send = self.channel._time()
            self._dirty = self._revision != revision
            self._redisplay = self._redisplay and self._revision != revision
            if result and self._current():
                self._last_html = html
                self._shown(preview, start=start, end=end, part_id=self._identity, index=0)

    async def _plan_anchor(self):
        from app.telegram_bots.commands import _FixedGuard

        services = self.channel._services
        guard = _FixedGuard(services, self.channel, self.link)
        self._anchor_outbox = await services.outbox.plan_product_keyboard(
            self.link,
            source_id=f"chat-stream:{self._identity}:{len(self._parts)}",
            parts=[{"kind": "text", "source_ref": {"key": "answer.thinking", "values": {}}, "reply_to_message_id": self.reply_to_message_id, "protect_content": self.protected}],
            keyboard=[[{"label": "label.stop", "kind": "stop_answer", "target_kind": "draft", "target_id": str(self.draft_id), "payload": {}}]],
            callbacks=services.callbacks,
            guard=guard,
            inbox_rows=self.origins,
            message_thread_id=self.topic,
        )
        self._anchor, self._keyboard = None, None
        self._dirty = True

    async def _read_anchor(self, *, allow_consumed=False):
        from sqlalchemy import select

        from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow

        services = self.channel._services
        async with services.outbox.sessions() as session:
            parent = await session.get(TelegramOutboxRow, self._anchor_outbox)
            if (
                parent is None
                or (parent.link_id, parent.owner_user_id, parent.chat_id, parent.message_thread_id, parent.binding_generation) != (self.link.id, self.link.owner_user_id, self.link.chat_id, self.topic, self.link.binding_generation)
                or len(parent.parts) != 1
                or parent.parts[0].get("state") != "sent"
                or len(parent.parts[0].get("message_ids", [])) != 1
            ):
                return False
            group = parent.parts[0].get("callback_group_id")
            rows = list(await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group)))
            if (
                len(rows) != 1
                or rows[0].kind != "stop_answer"
                or (rows[0].link_id, rows[0].chat_id, rows[0].message_thread_id, rows[0].binding_generation) != (self.link.id, self.link.chat_id, self.topic, self.link.binding_generation)
                or rows[0].message_id != parent.parts[0]["message_ids"][0]
                or not allow_consumed
                and rows[0].consumed_at is not None
            ):
                return False
            message_id, callback_id = rows[0].message_id, rows[0].id
        if not self._current(include_paused=True):
            return False
        label = await self.channel._render_source(self.link, {"key": "label.stop", "values": {}}, plain=True)
        self._anchor = message_id
        self._keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data="h1:" + callback_id)]])
        return True

    async def _flush_edit(self, preview, revision):
        span = None if preview is None else (preview.span.model_call_id, preview.span.message_id)
        if span is not None and self._model_span is not None and span != self._model_span:
            if not await self._settle_anchor():
                return
            if self._anchor is not None:
                self._parts.append((self._source_start, self._source_start, self._anchor))
            self._source_start, self._last_html, self._shown_anchor = 0, None, None
            await self._plan_anchor()
        if span is not None:
            self._model_span = span
        if self._anchor is None and not await self._read_anchor():
            return
        raw = preview.text if preview is not None else ""
        start = self._source_start
        slices = preview_source_slices(raw, start=start)
        begin, end, value = slices[0] if slices else (start, start, "")
        begin, end, value = self._fit(begin, end, value)
        final_part = len(slices) > 1
        html = self._render(value) if not final_part else to_html(value, partial=True).html
        if not html:
            return
        if html == self._last_html and not final_part:
            self._dirty = self._revision != revision
            return
        result = await self._call("edit_message_text", message_id=self._anchor, text=html, parse_mode=ParseMode.HTML, reply_markup={"inline_keyboard": []} if final_part else self._keyboard)
        self._last_send = self.channel._time()
        if result is None or not self._current():
            return
        self._last_html = html
        self._shown(preview, start=begin, end=end, part_id=f"{self._identity}:{len(self._parts)}", index=len(self._parts), message_id=self._anchor)
        if final_part:
            self._parts.append((begin, end, self._anchor))
            self._source_start = end
            self._last_html = None
            await self._plan_anchor()
        else:
            self._dirty = self._revision != revision

    async def close(self):
        self.closed = True

    async def pause(self):
        # Acknowledgements from the admitted display finish before Stop freezes
        # them. No new draft/edit may start after this barrier.
        async with self._lock:
            self.paused = True

    async def resume(self):
        """Resume the same live view after a definitively refused Stop."""
        async with self._lock:
            if not self._current(include_paused=True):
                return False
            self.paused = False
            self._dirty = True
            self._redisplay = self.mode == "draft"
            self._revision += 1
            return True

    def allows_placeholder(self, item):
        return (
            self._current()
            and item.id == self._anchor_outbox
            and item.dedupe_key == f"notice:chat-stream:{self._identity}:{len(self._parts)}"
            and (item.link_id, item.owner_user_id, item.chat_id, item.message_thread_id, item.binding_generation) == (self.link.id, self.link.owner_user_id, self.link.chat_id, self.topic, self.link.binding_generation)
        )

    async def matches_callback(self, row):
        if self.mode != "edit" or not self._current() or row.payload.get("anchor_outbox_id") != self._anchor_outbox:
            return False
        if self._anchor is None and not await self._read_anchor():
            return False
        return (row.link_id, row.chat_id, row.message_thread_id, row.binding_generation, row.message_id) == (self.link.id, self.link.chat_id, self.topic, self.link.binding_generation, self._anchor)

    async def prepare_final(self):
        if self.closed:
            return True
        if not self._current(include_paused=True):
            # Final-source authority has its own retained read lifetime. The
            # absence of a disposable UI cannot withdraw that source authority.
            await self.close()
            return True
        await self.pause()
        if not await self._settle_anchor():
            return False
        await self.close()
        return True

    async def _settle_anchor(self):
        if self.mode == "edit" and self._anchor_outbox is not None:
            from app.telegram_bots.commands import _FixedGuard

            await self._read_anchor()
            state = await self.channel._services.outbox.retire_stream_placeholder(self.link, outbox_id=self._anchor_outbox, guard=_FixedGuard(self.channel._services, self.channel, self.link))
            if state == "sending":
                return False
            if state == "sent" and self._anchor is None:
                if not await self._read_anchor(allow_consumed=True):
                    return False
            if state == "sent" and self._anchor != self._shown_anchor:
                await self._call("delete_message", _cleanup=True, message_id=self._anchor)
            elif state == "sent":
                await self._call("edit_message_reply_markup", _cleanup=True, message_id=self._anchor, reply_markup={"inline_keyboard": []})
        return True

    async def abandon(self):
        """Close an error preview once; durable error delivery stays in outbox."""
        if self.mode == "edit" and self._anchor is not None:
            if self._preview is None:
                await self._call("delete_message", _cleanup=True, message_id=self._anchor)
            else:
                await self._call("edit_message_reply_markup", _cleanup=True, message_id=self._anchor, reply_markup={"inline_keyboard": []})
        await self.close()
