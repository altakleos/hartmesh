"""Explicit Show for acknowledged colleague-news teasers."""

from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace

from sqlalchemy import select

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired, _item, _safe_json, keyboard_cleanup_row
from app.telegram_bots.work_news_sources import WorkNewsProjection
from app.telegram_bots.work_result_navigation import NavigationOrigin
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkNewsRecovery:
    def __init__(self, plans):
        self.plans, self.reader, self.outbox, self.callbacks = plans, plans.reader, plans.outbox, plans.callbacks

    async def reveal(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            from app.telegram_bots.work_news_delivery import frozen_news, recipes, require_teaser

            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None:
                    raise CallbackRefused()
                row = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id"))
                if row is None or row.state != "sent" or "work_news_basis" not in row.payload:
                    raise CallbackRefused()
                item = _item(row)
                basis, previous, _part = frozen_news(SimpleNamespace(link=link, item=item, part=item.parts[-1]), acknowledged=True)
                if basis["version"] != 1 or not basis["concealed"]:
                    raise CallbackRefused()
            source = await self.reader.read(link, item)
            if source.event_signature != previous.event_signature:
                raise ProductSourceRetired()
            settings = await self.plans.settings.get(link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            origin = NavigationOrigin()
            admission = await prepare_attention(self.reader, link)
            composed = _RevealGuard(self, guard, admission, link, item, source)
            async with self.callbacks.writer(link, composed) as writer:
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
                if group.anchor_outbox_id != item.id or group.selected.kind != "reveal" or group.selected.payload.get("outbox_id") != item.id or row is None or _item(row) != item:
                    raise CallbackRefused()
                try:
                    require_teaser(group.selected, row)
                except ProductSourceRetired:
                    raise CallbackRefused() from None
                await self.callbacks.consume_group(group, reason="revealed", session=writer)
                fresh_basis = {"version": 2, "source": asdict(source), "locale": settings.locale, "concealed": False, "public_base_url": origin.base, "reveal": callback_id, "cleanup_id": None, "origin": deepcopy(basis)}
                parts, keyboard = recipes(source, fresh_basis, row.id, link)
                fresh = self.outbox._parts(parts, 1)
                if len(fresh) == 1:
                    fresh[0].update(kind="edit", target_message_id=message_id, target_protect_content=item.parts[-1]["protect_content"])
                else:
                    cleanup = keyboard_cleanup_row(group.selected, owner_user_id=link.owner_user_id, now=self.outbox.clock())
                    cleanup.payload = {**cleanup.payload, "work_request_parent_id": row.id}
                    writer.add(cleanup)
                    fresh_basis["cleanup_id"] = cleanup.id
                row.parts = [*row.parts, *fresh]
                row.payload = _safe_json({**row.payload, "presentation_version": 1, "work_news_basis": {**fresh_basis, "parts": parts, "keyboard": keyboard}, "product_keyboard_basis": keyboard})
                row.class_, row.state, row.not_before = "reply", "pending", self.outbox.clock()
                row.sent_at = row.closed_at = row.failed_reason = row.skip_reason = None
                row.claim_id = row.claim_owner = row.claimed_at = None
                if keyboard:
                    current = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=1, chat_id=link.chat_id, message_id=None, message_thread_id=row.message_thread_id, session=writer)
                    row.parts = [*row.parts[:-1], {**row.parts[-1], "callback_group_id": current.group_id}]
                    for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == current.group_id)):
                        control.payload = {**control.payload, "presentation_pending": True}
                await writer.flush()
                composed.item = _item(row)
                origin.check(fresh_basis)
                return row.id

        return await await_drained(perform())


class _RevealGuard:
    def __init__(self, host, base, admission, link, item, source):
        if not callable(getattr(base, "current", None)):
            raise CallbackRefused()
        self.host, self.base, self.admission, self.link, self.item, self.source = host, base, admission, link, item, source

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        await self.admission(session=writer, projection=WorkNewsProjection(self.host.reader, self.link, self.item, self.source))
        self.base.current()
        return current
