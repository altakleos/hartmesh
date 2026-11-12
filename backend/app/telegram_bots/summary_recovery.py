"""Refresh unacknowledged summaries while retaining their original daily intent."""

import asyncio
from copy import deepcopy
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired, _item, _safe_json
from app.telegram_bots.summary_delivery import _SummaryWriteProjection, frozen_summary, recipes
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_navigation import _same_item
from app.telegram_bots.work_result_navigation import NavigationOrigin
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


async def current_summary(plans, link, basis):
    settings = await plans.settings.get(link.owner_user_id)
    if settings is None or not settings.summary_enabled or not plans.available():
        raise ProductSourceRetired()
    zone = await asyncio.to_thread(ZoneInfo, settings.timezone)
    source = await plans.reader.read(link, as_of=datetime.fromisoformat(basis["as_of"]), since=datetime.fromisoformat(basis["since"]))
    fresh = {
        **basis,
        "locale": settings.locale,
        "timezone": settings.timezone,
        "summary_minute": settings.summary_minute,
        "sections": source.sections,
        "protected": source.protected,
        "concealed": source.protected,
        "public_base_url": NavigationOrigin().base,
    }
    return source, fresh, zone


def unchanged_summary(original, fresh):
    fields = ("locale", "timezone", "summary_minute", "sections", "protected", "concealed", "public_base_url")
    return same_body({key: original[key] for key in fields}, {key: fresh[key] for key in fields})


class SummaryRecovery:
    def __init__(self, plans):
        self.plans, self.outbox, self.callbacks = plans, plans.outbox, plans.callbacks

    @staticmethod
    def repairable(row):
        return (
            row is not None
            and "summary_basis" in row.payload
            and "summary_dispatch_conflict" not in row.payload
            and claim_of(row) == (None, None, None)
            and (row.state == "failed" and row.failed_reason == "summary-refresh-required" or row.state == "skipped" and row.skip_reason == "superseded")
        )

    async def repair(self, link, identity, *, guard):
        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, identity)
                if not self.repairable(row):
                    return None
                item, disposition = _item(row), _disposition(row)
            basis, _ = frozen_summary(SimpleNamespace(link=link, item=item, part=None), pending=item.parts[-1]["state"] == "pending")
            source, fresh, zone = await current_summary(self.plans, link, basis)
            composed = _RepairGuard(guard, self.plans, source, fresh, zone, item, disposition)
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
                if not self.repairable(row) or not _same_item(row, item):
                    return None
                await self.replace(writer, link, row, fresh)
                composed.item, composed.disposition = _item(row), _disposition(row)
                return identity

        return await await_drained(perform())

    async def refresh(self, captured):
        async def perform():
            basis, _ = frozen_summary(captured)
            source, fresh, zone = await current_summary(self.plans, captured.link, basis)
            async with self.outbox._writer(captured.link.id) as (writer, binding):
                row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True, populate_existing=True)
                current = self.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
                if current != captured:
                    raise DeliveryClaimRetired()
                await source.authorize(session=writer, projection=_SummaryWriteProjection(self.plans, source, fresh, zone, captured=captured, now=self.outbox.clock(), require_due=False))
                await self.replace(writer, captured.link, row, fresh)
                projection = _SummaryWriteProjection(self.plans, source, fresh, zone, require_due=False)
                projection.item = _item(row)
                await source.authorize(session=writer, projection=projection)

        return await await_drained(perform())

    async def replace(self, writer, link, row, fresh_basis):
        item = _item(row)
        basis, _ = frozen_summary(SimpleNamespace(link=link, item=item, part=None), pending=item.parts[-1]["state"] == "pending")
        history = deepcopy(basis.get("history", []))
        previous = deepcopy(basis)
        if previous["version"] == 2:
            previous["history"] = []
        history.append({"basis": previous, "part": deepcopy(item.parts[-1])})
        fresh_basis = {**fresh_basis, "version": 2, "history": history}
        parts, keyboard = recipes(fresh_basis, link, row.id)
        fresh_basis.update(parts=parts, keyboard=keyboard)
        version = item.payload["presentation_version"] + 1
        old = deepcopy(item.parts)
        old[-1]["state"] = "retired"
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == item.parts[-1]["callback_group_id"])):
            control.consumed_at, control.closed_reason = self.outbox.clock(), "superseded"
        row.payload = _safe_json({**row.payload, "presentation_version": version, "summary_basis": fresh_basis, "product_keyboard_basis": keyboard})
        new = self.outbox._parts(parts, version) if parts else []
        row.parts = old + new
        row.state, row.not_before = "pending", self.outbox.clock()
        row.claim_owner = row.claim_id = row.claimed_at = None
        row.sent_at = row.closed_at = row.failed_reason = row.skip_reason = row.degraded_at = None
        if parts:
            group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=version, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
            row.parts = [*old, {**new[0], "callback_group_id": group.group_id}]
            for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
                control.created_at = aware(row.created_at)
                control.payload = {**control.payload, "presentation_pending": True}
        else:
            row.state, row.skip_reason, row.closed_at = "skipped", "nothing-to-say", self.outbox.clock()
        await writer.flush()
        frozen_summary(SimpleNamespace(link=link, item=_item(row), part=None), pending=True)


class _RepairGuard:
    def __init__(self, base, plans, source, basis, zone, item, disposition):
        self.base, self.plans, self.source, self.basis, self.zone, self.item = base, plans, source, basis, zone, item
        self.disposition = disposition

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        projection = _HeldProjection(self.plans, self.source, self.basis, self.zone, disposition=self.disposition)
        projection.item = self.item
        await self.source.authorize(session=writer, projection=projection)
        self.base.current()
        return current


def _disposition(row):
    return row.state, row.failed_reason, row.skip_reason, aware(row.closed_at) if row.closed_at else None


class _HeldProjection(_SummaryWriteProjection):
    def __init__(self, plans, source, basis, zone, *, disposition):
        super().__init__(plans, source, basis, zone, require_due=False)
        self.disposition = disposition

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        if _disposition(rows[0][self.delivery_column + 1]) != self.disposition:
            raise ProductSourceRetired()
