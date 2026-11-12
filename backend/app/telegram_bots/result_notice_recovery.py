"""Source-aware result recovery preserves actual media and teaser receipts."""

from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace

from sqlalchemy import select

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import DeliveryClaimRetired, PermanentDeliveryError, ProductSourceRetired, _item, _safe_json, keyboard_cleanup_row
from app.telegram_bots.result_notice_sources import ResultNoticeProjection
from app.telegram_bots.work_request_source import require_show
from app.telegram_bots.work_result_history import archive_media, qualified_history, read_media_history
from app.telegram_bots.work_result_ledger import carry_media, initial_ledger
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class ResultNoticeRecovery:
    def __init__(self, plans):
        self.plans, self.reader, self.outbox, self.callbacks = plans, plans.reader, plans.outbox, plans.callbacks

    async def refresh(self, captured):
        async def perform():
            source = await self.reader.read(captured.link, captured.item)
            settings = await self.plans.settings.get(captured.link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            admission = await prepare_attention(self.reader, captured.link)
            async with self.outbox._writer(captured.link.id) as (writer, binding):
                row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True, populate_existing=True)
                current = self.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
                if current != captured:
                    raise DeliveryClaimRetired()
                await admission(session=writer, projection=ResultNoticeProjection(self.reader, captured.link, captured.item, source))
                await self._replace(writer, captured.link, row, source, settings)
                from app.telegram_bots.result_notice_delivery import _MaterializationProjection

                await admission(session=writer, projection=_MaterializationProjection(self.reader, captured.link, _item(row), source))

        return await await_drained(perform())

    @staticmethod
    def _repairable(row):
        return row is not None and row.state == "failed" and row.failed_reason == "work-result-refresh-required" and all(getattr(row, field) is None for field in ("claim_owner", "claim_id", "claimed_at", "message_thread_id"))

    async def repair(self, link, outbox_id, *, guard):
        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, outbox_id)
                if row is None or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
                    raise CallbackRefused()
                if not self._repairable(row):
                    return None
                item = _item(row)
            source = await self.reader.read(link, item)
            settings = await self.plans.settings.get(link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            admission = await prepare_attention(self.reader, link)
            composed = _RevealGuard(self, guard, admission, link, item, source)
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
                if not self._repairable(row) or _item(row) != item:
                    return None
                await self._replace(writer, link, row, source, settings, repair=True)
                composed.item = _item(row)
                return row.id

        return await await_drained(perform())

    async def reveal(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            from app.telegram_bots.result_notice_delivery import frozen_notice

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
                await self._replace(writer, link, row, source, settings, reveal=callback_id)
                composed.item = _item(row)
                return row.id

        return await await_drained(perform())

    async def _replace(self, writer, link, row, source, settings, *, reveal=None, repair=False):
        from app.telegram_bots.result_notice_delivery import frozen_notice, recipes

        item = _item(row)
        parts = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        if repair:
            if not self._repairable(row) or reveal is not None:
                raise ProductSourceRetired()
            selected = next((part for part in parts if part["state"] == "pending"), None)
        else:
            selected = next((part for part in parts if part["state"] == "sending"), parts[-1] if row.state == "sent" and parts else None)
        if selected is None:
            raise ProductSourceRetired()
        try:
            basis, previous, _part = frozen_notice(SimpleNamespace(link=link, item=item, part=selected), acknowledged=selected["state"] == "sent", pending=repair)
            if source.identity != previous.identity or source.event_signature != previous.event_signature or reveal is not None and not basis["concealed"]:
                raise ProductSourceRetired()
            earlier, hashes = qualified_history(item, previous)
            history = deepcopy(item.payload.get("work_result_history", []))
            if not basis["concealed"]:
                proof = archive_media(item, previous, locale=basis["locale"], options=basis["result_files"], earlier=earlier)
                known, remembered = read_media_history(item, proof, source, earlier=earlier)
                if any(earlier.get(index) != receipt for index, receipt in known.items()) or any(hashes.get(index) != digest for index, digest in remembered.items()):
                    history.append(proof)
        except (PermanentDeliveryError, ValueError):
            raise ProductSourceRetired() from None
        origin = reveal or basis.get("reveal")
        if origin is not None:
            require_show(await writer.get(TelegramCallbackRow, origin, with_for_update=True, populate_existing=True), row)
        old = deepcopy(row.parts)
        groups = {part.get("callback_group_id") for part in parts} - {None}
        controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups)).order_by(TelegramCallbackRow.id).with_for_update()))
        try:
            _validate_rows(row, controls, allow_pending=True)
        except CallbackRefused:
            raise ProductSourceRetired() from None
        for control in controls:
            own = next((part for part in parts if part.get("callback_group_id") == control.group_id), None)
            if own is None or control.message_id is not None and (own["state"] != "sent" or own["message_ids"] != [control.message_id]):
                raise ProductSourceRetired()
            if control.consumed_at is None:
                control.consumed_at, control.closed_reason = self.outbox.clock(), "superseded"
        version = item.payload["presentation_version"] + 1
        cleanup_id = basis.get("cleanup_id")
        cleaned = set()
        for control in controls:
            if control.message_id is not None and control.group_id not in cleaned:
                cleaned.add(control.group_id)
                cleanup = keyboard_cleanup_row(control, owner_user_id=link.owner_user_id, now=self.outbox.clock(), dedupe_key=f"notice:result-preview-cleanup:{row.id}:{version}:{control.group_id}")
                cleanup.payload = {**cleanup.payload, "work_request_parent_id": row.id}
                writer.add(cleanup)
                if control.id == reveal:
                    cleanup_id = cleanup.id
        for part in old:
            if part["state"] not in {"sent", "retired", "unavailable"}:
                part["state"] = "retired"
        fresh_basis = {"version": 2, "source": asdict(source), "locale": settings.locale, "concealed": source.protected and origin is None, "result_files": basis["result_files"], "reveal": origin, "cleanup_id": cleanup_id}
        templates, keyboard = recipes(source, fresh_basis, row.id)
        fresh = self.outbox._parts(templates, version)
        row.parts = old + fresh
        row.payload = _safe_json(
            {
                **row.payload,
                "presentation_version": version,
                "result_notice_basis": {**fresh_basis, "parts": templates, "keyboard": keyboard},
                "product_keyboard_basis": keyboard,
                "work_result_ledger": initial_ledger(fresh),
                "work_result_history": history,
                "work_result_navigation": source.navigation,
            }
        )
        carried, hashes = qualified_history(_item(row), source)
        documents = set()
        for proof in history:
            for event in proof["ledger"]["events"]:
                if event["reason"] == "photo-rejected":
                    parent = next(part for part in proof["parts"] if part["part_id"] == event["old"])
                    documents.update(file["source_index"] for file in parent["files"])
        carry_media(row, carried, hashes, documents=documents)
        row.class_, row.state, row.silent, row.not_before = "reply" if origin else "result", "pending", bool(item.silent), self.outbox.clock()
        row.sent_at = row.closed_at = row.failed_reason = row.skip_reason = row.degraded_at = None
        row.claim_id = row.claim_owner = row.claimed_at = None
        if keyboard:
            group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=version, chat_id=link.chat_id, message_id=None, message_thread_id=row.message_thread_id, session=writer)
            row.parts = [*row.parts[:-1], {**row.parts[-1], "callback_group_id": group.group_id}]
            for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                control.payload = {**control.payload, "presentation_pending": True}
                if controls:
                    control.created_at = min(old_control.created_at for old_control in controls)
        await writer.flush()


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
