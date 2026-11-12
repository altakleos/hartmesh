"""Explicit protected result reveal, preserving its actual teaser receipt."""

from dataclasses import asdict
from types import SimpleNamespace

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import ProductSourceRetired, _item, _safe_json, keyboard_cleanup_row
from app.telegram_bots.work_result_ledger import initial_ledger
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class ResultNoticeRecovery:
    def __init__(self, plans):
        self.plans, self.reader, self.outbox, self.callbacks = plans, plans.reader, plans.outbox, plans.callbacks

    async def reveal(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            from app.telegram_bots.result_notice_delivery import frozen_notice, recipes

            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None:
                    raise CallbackRefused()
                row = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id"))
                if row is None or row.state != "sent" or "result_notice_basis" not in row.payload:
                    raise CallbackRefused()
                item = _item(row)
                basis, _old_source, _part = frozen_notice(SimpleNamespace(link=link, item=item, part=item.parts[-1]), acknowledged=True)
                if not basis["concealed"]:
                    raise CallbackRefused()
            source = await self.reader.read(link, item)
            if source.identity != item.payload["work_result_identity"]:
                raise ProductSourceRetired()
            settings = await self.plans.settings.get(link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            admission = await prepare_attention(self.reader, link)
            composed = _RevealGuard(self, guard, admission, link, item, source)
            async with self.callbacks.writer(link, composed) as writer:
                group = await self.callbacks.lock_current(link, callback_id, chat_id=chat_id, message_id=message_id, message_thread_id=message_thread_id, guard=composed, session=writer)
                row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
                if group.anchor_outbox_id != item.id or group.selected.kind != "reveal" or group.selected.payload.get("outbox_id") != item.id or row is None or _item(row) != item:
                    raise CallbackRefused()
                await self.callbacks.consume_group(group, reason="revealed", session=writer)
                cleanup = keyboard_cleanup_row(group.selected, owner_user_id=link.owner_user_id, now=self.outbox.clock())
                cleanup.payload = {**cleanup.payload, "work_request_parent_id": row.id}
                writer.add(cleanup)
                version = item.payload["presentation_version"] + 1
                fresh_basis = {"version": 2, "source": asdict(source), "locale": settings.locale, "concealed": False, "result_files": basis["result_files"], "reveal": callback_id, "cleanup_id": cleanup.id}
                parts, controls = recipes(source, fresh_basis, row.id)
                fresh = self.outbox._parts(parts, version)
                row.parts = [*row.parts, *fresh]
                row.payload = _safe_json(
                    {
                        **row.payload,
                        "presentation_version": version,
                        "result_notice_basis": {**fresh_basis, "parts": parts, "keyboard": controls},
                        "product_keyboard_basis": controls,
                        "work_result_ledger": initial_ledger(fresh),
                        "work_result_navigation": source.navigation,
                    }
                )
                row.class_, row.state, row.silent, row.not_before = "reply", "pending", bool(item.silent), self.outbox.clock()
                row.sent_at = row.closed_at = row.failed_reason = row.skip_reason = None
                row.claim_id = row.claim_owner = row.claimed_at = None
                await writer.flush()
                composed.item = _item(row)
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
        from app.telegram_bots.result_notice_delivery import _MaterializationProjection

        current = await self.base.check(writer)
        await self.admission(session=writer, projection=_MaterializationProjection(self.host.reader, self.link, self.item, self.source))
        self.base.current()
        return current
