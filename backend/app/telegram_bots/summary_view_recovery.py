"""Refresh an explicit summary Show without changing its original gesture."""

from copy import deepcopy
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired, _item, _safe_json
from app.telegram_bots.summary_navigation import SummaryNavigationProjection
from app.telegram_bots.summary_recovery import _disposition
from app.telegram_bots.summary_views import frozen_view, recipes, summary_basis
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_navigation import _same_item
from app.telegram_bots.work_digest_views import parent_proof
from app.telegram_bots.work_result_navigation import NavigationOrigin
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class SummaryViewRecovery:
    def __init__(self, views):
        self.views, self.outbox, self.callbacks = views, views.outbox, views.callbacks

    @staticmethod
    def repairable(row):
        return (
            row is not None
            and "summary_view_basis" in row.payload
            and "summary_dispatch_conflict" not in row.payload
            and claim_of(row) == (None, None, None)
            and (row.state == "failed" and row.failed_reason == "summary-view-refresh-required" or row.state == "skipped" and row.skip_reason == "superseded")
        )

    async def source(self, link, item):
        basis, _ = frozen_view(SimpleNamespace(link=link, item=item, part=None), pending=item.parts[-1]["state"] == "pending")
        snapshot = await self.views.navigation.read(link, basis["parent_id"], callback_id=basis["callback_id"], closed_reason="revealed")
        if basis["parent_proof"] != parent_proof(snapshot.item) or basis["target"] != snapshot.item.parts[-1]["message_ids"][0]:
            raise ProductSourceRetired()
        return snapshot

    async def repair(self, link, identity, *, guard):
        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, identity)
                if not self.repairable(row):
                    return None
                item, disposition = _item(row), _disposition(row)
                missing = row.failed_reason == "summary-view-refresh-required"
            snapshot = await self.source(link, item)
            composed = _RepairGuard(guard, snapshot, item, disposition)
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
                if not self.repairable(row) or not _same_item(row, item) or _disposition(row) != disposition:
                    return None
                await self.replace(writer, link, row, snapshot, missing=missing)
                composed.item, composed.disposition = _item(row), _disposition(row)
                return identity

        return await await_drained(perform())

    async def refresh(self, captured):
        async def perform():
            snapshot = await self.source(captured.link, captured.item)
            async with self.outbox._writer(captured.link.id) as (writer, binding):
                row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True, populate_existing=True)
                current = self.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
                if current != captured:
                    raise DeliveryClaimRetired()
                await snapshot.source.authorize(session=writer, projection=_RepairProjection(snapshot, _item(row), claim_of(row), _disposition(row)))
                await self.replace(writer, captured.link, row, snapshot)
                await snapshot.source.authorize(session=writer, projection=_RepairProjection(snapshot, _item(row), claim_of(row), _disposition(row)))

        return await await_drained(perform())

    async def replace(self, writer, link, row, snapshot, *, missing=False):
        item = _item(row)
        basis, _ = frozen_view(SimpleNamespace(link=link, item=item, part=None), pending=item.parts[-1]["state"] == "pending")
        history = deepcopy(basis.get("history", []))
        previous = deepcopy(basis)
        if previous["version"] == 2:
            previous["history"] = []
        history.append({"basis": previous, "part": deepcopy(item.parts[-1])})
        if missing and item.parts[-1]["kind"] != "edit":
            raise ProductSourceRetired()
        replacement = {"reason": "missing-target", "part_id": item.parts[-1]["part_id"]} if missing else basis.get("replacement")
        fresh_basis = {**basis, "version": 2, "history": history, "replacement": replacement, "summary": summary_basis(snapshot)}
        part, keyboard = recipes(fresh_basis, link, row.id)
        fresh_basis.update(parts=[part], keyboard=keyboard)
        version = item.payload["presentation_version"] + 1
        old = deepcopy(item.parts)
        old[-1]["state"] = "retired"
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == item.parts[-1]["callback_group_id"])):
            control.consumed_at, control.closed_reason = self.outbox.clock(), "superseded"
        row.payload = _safe_json({**row.payload, "presentation_version": version, "summary_view_basis": fresh_basis, "product_keyboard_basis": keyboard})
        new = self.outbox._parts([part], version)
        row.parts = old + new
        row.state, row.not_before = "pending", self.outbox.clock()
        row.claim_owner = row.claim_id = row.claimed_at = None
        row.sent_at = row.closed_at = row.failed_reason = row.skip_reason = row.degraded_at = None
        group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=version, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
        row.parts = [*old, {**new[0], "callback_group_id": group.group_id}]
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
            control.created_at = aware(snapshot.item.created_at)
            control.payload = {**control.payload, "presentation_pending": True}
        await writer.flush()
        frozen_view(SimpleNamespace(link=link, item=_item(row), part=None), pending=True)


class _RepairProjection(SummaryNavigationProjection):
    def __init__(self, snapshot, item, claim, disposition):
        super().__init__(snapshot)
        self.item, self.claim, self.disposition = item, claim, disposition
        self.origin = NavigationOrigin()

    def extend(self, query):
        query = super().extend(query)
        self.repair_column = len(query.column_descriptions)
        row, control = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        return query.add_columns(row, control).join(row, row.id == self.item.id).outerjoin(control, control.group_id == self.item.parts[-1]["callback_group_id"])

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        self.origin.check(summary_basis(self.snapshot))
        row = rows[0][self.repair_column]
        if claim_of(row) != self.claim:
            raise DeliveryClaimRetired()
        if not _same_item(row, self.item) or _disposition(row) != self.disposition:
            raise ProductSourceRetired()
        controls = list({control.id: control for record in rows if (control := record[self.repair_column + 1]) is not None}.values())
        try:
            _validate_rows(row, controls, allow_pending=True)
            for control in controls:
                self.snapshot.owner.plans.callbacks._usable(control)
                if control.message_id is not None or control.payload.get("presentation_pending") is not True or aware(control.created_at) != aware(self.snapshot.item.created_at):
                    raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None


class _RepairGuard:
    def __init__(self, base, snapshot, item, disposition):
        self.base, self.snapshot, self.item, self.disposition = base, snapshot, item, disposition

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        await self.snapshot.source.authorize(session=writer, projection=_RepairProjection(self.snapshot, self.item, (None, None, None), self.disposition))
        self.base.current()
        return current
