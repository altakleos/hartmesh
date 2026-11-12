"""Explicit Show for acknowledged colleague-news teasers."""

from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256
from types import SimpleNamespace

from sqlalchemy import select

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired, _item, _safe_json, keyboard_cleanup_row
from app.telegram_bots.work_news_sources import WorkNewsProjection
from app.telegram_bots.work_result_navigation import NavigationOrigin
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class WorkNewsRecovery:
    def __init__(self, plans):
        self.plans, self.reader, self.outbox, self.callbacks = plans, plans.reader, plans.outbox, plans.callbacks

    async def refresh(self, captured):
        async def perform():
            source = await self.reader.read(captured.link, captured.item)
            settings = await self.plans.settings.get(captured.link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            origin = NavigationOrigin()
            admission = await prepare_attention(self.reader, captured.link)
            async with self.outbox._writer(captured.link.id) as (writer, binding):
                row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True, populate_existing=True)
                current = self.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
                if current != captured:
                    raise DeliveryClaimRetired()
                await admission(session=writer, projection=WorkNewsProjection(self.reader, captured.link, captured.item, source))
                await self._replace(writer, captured.link, row, source, settings, origin)
                await admission(session=writer, projection=WorkNewsProjection(self.reader, captured.link, _item(row), source))
                origin.check(row.payload["work_news_basis"])

        return await await_drained(perform())

    @staticmethod
    def _repairable(row):
        return row is not None and row.state == "failed" and row.failed_reason == "work-news-refresh-required" and all(getattr(row, field) is None for field in ("claim_owner", "claim_id", "claimed_at", "message_thread_id"))

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
            origin = NavigationOrigin()
            admission = await prepare_attention(self.reader, link)
            composed = _RevealGuard(self, guard, admission, link, item, source)
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
                if not self._repairable(row) or _item(row) != item:
                    return None
                await self._replace(writer, link, row, source, settings, origin, repair=True)
                composed.item = _item(row)
                origin.check(row.payload["work_news_basis"])
                return row.id

        return await await_drained(perform())

    async def reveal(self, link, callback_id, *, chat_id, message_id, message_thread_id, guard):
        async def perform():
            from app.telegram_bots.work_news_delivery import frozen_news, require_teaser

            async with self.outbox.sessions() as reader:
                callback = await reader.get(TelegramCallbackRow, callback_id)
                if callback is None:
                    raise CallbackRefused()
                row = await reader.get(TelegramOutboxRow, callback.payload.get("anchor_outbox_id"))
                if row is None or row.state != "sent" or "work_news_basis" not in row.payload:
                    raise CallbackRefused()
                item = _item(row)
                basis, previous, _part = frozen_news(SimpleNamespace(link=link, item=item, part=item.parts[-1]), acknowledged=True)
                if not basis["concealed"]:
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
                await self._replace(writer, link, row, source, settings, origin, reveal=group.selected)
                composed.item = _item(row)
                origin.check(row.payload["work_news_basis"])
                return row.id

        return await await_drained(perform())

    async def _replace(self, writer, link, row, source, settings, origin, *, reveal=None, repair=False):
        from app.telegram_bots.work_news_delivery import frozen_news, recipes, require_teaser
        from app.telegram_bots.work_request_source import require_show

        item = _item(row)
        version = item.payload["presentation_version"]
        current = [part for part in row.parts if part["presentation_version"] == version]
        selected = next((part for part in current if part["state"] == ("pending" if repair else "sending")), current[-1] if row.state == "sent" else None)
        if selected is None or repair and not self._repairable(row):
            raise ProductSourceRetired()
        basis, previous, _part = frozen_news(SimpleNamespace(link=link, item=item, part=selected), acknowledged=selected["state"] == "sent", pending=repair)
        if source.event_signature != previous.event_signature or source.work_id != previous.work_id:
            raise ProductSourceRetired()
        reveal_id = reveal.id if reveal else basis.get("reveal")
        proof = await writer.get(TelegramCallbackRow, reveal_id, with_for_update=True, populate_existing=True) if reveal_id else None
        if reveal_id:
            require_show(proof, row)
            require_teaser(proof, row)
        groups = {part.get("callback_group_id") for part in current} - {None}
        controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups)).order_by(TelegramCallbackRow.id).with_for_update()))
        try:
            _validate_rows(row, controls, allow_pending=True)
        except CallbackRefused:
            raise ProductSourceRetired() from None
        for control in controls:
            own = next(part for part in current if part.get("callback_group_id") == control.group_id)
            if control.message_id is not None and (own["state"] != "sent" or own["message_ids"] != [control.message_id]):
                raise ProductSourceRetired()
            if control.consumed_at is None:
                control.consumed_at, control.closed_reason = self.outbox.clock(), "superseded"
        history = deepcopy(basis.get("history", []))
        if basis["version"] == 2:
            history = [{"basis": deepcopy(basis["origin"]), "parts": deepcopy([part for part in row.parts if part["presentation_version"] == 0]), "class": "info", "presentation_version": 0}]
        archived = deepcopy(basis)
        if archived["version"] == 3:
            archived["history"] = []
        history.append({"basis": archived, "parts": deepcopy(current), "class": item.class_, "presentation_version": version})
        fresh_basis = {
            "version": 3,
            "source": asdict(source),
            "locale": settings.locale,
            "concealed": source.protected and reveal_id is None,
            "public_base_url": origin.base,
            "reveal": reveal_id,
            "teaser_version": version if reveal else basis.get("teaser_version", 0) if reveal_id else None,
            "cleanup_id": basis.get("cleanup_id"),
            "edit_target": None,
            "history": history,
        }
        parts, keyboard = recipes(source, fresh_basis, row.id, link)
        fresh = self.outbox._parts(parts, version + 1)
        if reveal and len(fresh) == 1:
            fresh[0].update(kind="edit", target_message_id=reveal.message_id, target_protect_content=current[-1]["protect_content"])
            fresh_basis["edit_target"], fresh_basis["cleanup_id"] = current[-1]["part_id"], None
        else:
            candidates = [control for control in controls if control.message_id is not None]
            if proof is not None and fresh_basis["cleanup_id"] is None:
                candidates.append(proof)
            cleaned = set()
            for control in candidates:
                if control.group_id in cleaned:
                    continue
                cleaned.add(control.group_id)
                cleanup = keyboard_cleanup_row(control, owner_user_id=link.owner_user_id, now=self.outbox.clock(), dedupe_key="news-cleanup:" + sha256(f"{row.id}:{version + 1}:{control.group_id}".encode("ascii")).hexdigest())
                cleanup.payload = {**cleanup.payload, "work_request_parent_id": row.id}
                writer.add(cleanup)
                if proof is None or control.id == proof.id:
                    fresh_basis["cleanup_id"] = cleanup.id
        old = deepcopy(row.parts)
        for part in old:
            if part["state"] in {"pending", "sending"}:
                part["state"] = "retired"
        row.parts = [*old, *fresh]
        row.payload = _safe_json({**row.payload, "presentation_version": version + 1, "work_news_basis": {**fresh_basis, "parts": parts, "keyboard": keyboard}, "product_keyboard_basis": keyboard})
        row.class_, row.state, row.not_before = "reply" if reveal_id else "info", "pending", self.outbox.clock()
        row.sent_at = row.closed_at = row.failed_reason = row.skip_reason = row.degraded_at = None
        row.claim_id = row.claim_owner = row.claimed_at = None
        if keyboard:
            group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=version + 1, chat_id=link.chat_id, message_id=None, message_thread_id=row.message_thread_id, session=writer)
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
        current = await self.base.check(writer)
        await self.admission(session=writer, projection=WorkNewsProjection(self.host.reader, self.link, self.item, self.source))
        self.base.current()
        return current
