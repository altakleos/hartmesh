"""Source-aware recovery of an unacknowledged protected digest reveal."""

from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired, _item, _safe_json
from app.telegram_bots.work_digest_delivery import member_proof
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_navigation import _NavigationProjection
from app.telegram_bots.work_digest_views import frozen_view, parent_proof, recipes
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class DigestViewRecovery:
    def __init__(self, views):
        self.views, self.outbox, self.callbacks = views, views.outbox, views.callbacks

    @staticmethod
    def repairable(row):
        return (
            row is not None
            and "work_digest_view_basis" in row.payload
            and claim_of(row) == (None, None, None)
            and row.message_thread_id is None
            and (row.state == "failed" and row.failed_reason == "digest-view-refresh-required" or row.state == "skipped" and row.skip_reason == "superseded")
        )

    async def source(self, link, item, *, pending):
        basis, _ = frozen_view(SimpleNamespace(link=link, item=item, part=None), pending=pending)
        snapshot = await self.views.navigation.read(link, basis["parent_id"], callback_id=basis["callback_id"], closed_reason="revealed")
        if basis["parent_proof"] != parent_proof(snapshot.item) or basis["target"] != snapshot.item.parts[0]["message_ids"][0] or snapshot.item.parts[0]["protect_content"] is not True:
            raise ProductSourceRetired()
        return snapshot

    async def repair(self, link, identity, *, guard):
        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, identity)
                if row is None or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
                    raise CallbackRefused()
                if not self.repairable(row):
                    return None
                item, missing = _item(row), row.failed_reason == "digest-view-refresh-required"
            snapshot = await self.source(link, item, pending=item.parts[-1]["state"] == "pending")
            admission = await prepare_attention(self.views.plans.reader, link)
            composed = _RepairGuard(guard, admission, snapshot, item, (None, None, None))
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, identity, with_for_update=True, populate_existing=True)
                if not self.repairable(row) or _item(row) != item:
                    return None
                await self.replace(writer, link, row, snapshot, missing=missing)
                composed.item, composed.claim = _item(row), claim_of(row)
                return identity

        return await await_drained(perform())

    async def refresh(self, captured):
        async def perform():
            snapshot = await self.source(captured.link, captured.item, pending=False)
            admission = await prepare_attention(self.views.plans.reader, captured.link)
            async with self.outbox._writer(captured.link.id) as (writer, binding):
                row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True, populate_existing=True)
                current = self.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
                if current != captured:
                    raise DeliveryClaimRetired()
                await admission(session=writer, projection=_RepairProjection(snapshot, _item(row), claim_of(row)))
                await self.replace(writer, captured.link, row, snapshot)
                await admission(session=writer, projection=_RepairProjection(snapshot, _item(row), claim_of(row)))

        return await await_drained(perform())

    async def replace(self, writer, link, row, snapshot, *, missing=False):
        item = _item(row)
        basis, _ = frozen_view(SimpleNamespace(link=link, item=item, part=None), pending=item.parts[-1]["state"] == "pending")
        controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == item.parts[-1]["callback_group_id"]).order_by(TelegramCallbackRow.id).with_for_update()))
        try:
            _validate_rows(row, controls, allow_pending=True)
            for control in controls:
                self.callbacks._usable(control)
                if control.message_id is not None:
                    raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None
        history = deepcopy(basis.get("history", []))
        previous = deepcopy(basis)
        if previous["version"] == 2:
            previous["history"] = []
        history.append({"basis": previous, "part": deepcopy(item.parts[-1])})
        replacement = {"reason": "missing-target", "part_id": item.parts[-1]["part_id"]} if missing else basis.get("replacement")
        if missing and item.parts[-1]["kind"] != "edit":
            raise ProductSourceRetired()
        fresh_basis = {
            **basis,
            "version": 2,
            "history": history,
            "replacement": replacement,
            "locale": snapshot.locale,
            "entries": [asdict(member.entry) for member in snapshot.members],
            "members": [member_proof(member) for member in snapshot.members],
        }
        part, keyboard = recipes(fresh_basis, row.id)
        fresh_basis.update(parts=[part], keyboard=keyboard)
        version = item.payload["presentation_version"] + 1
        old = deepcopy(item.parts)
        old[-1]["state"] = "retired"
        fresh = self.outbox._parts([part], version)
        for control in controls:
            control.consumed_at, control.closed_reason = self.outbox.clock(), "superseded"
        row.payload = _safe_json({**row.payload, "presentation_version": version, "work_digest_view_basis": fresh_basis, "product_keyboard_basis": keyboard})
        row.parts = old + fresh
        row.state, row.not_before = "pending", self.outbox.clock()
        row.claim_owner = row.claim_id = row.claimed_at = None
        row.sent_at = row.closed_at = row.failed_reason = row.skip_reason = row.degraded_at = None
        group = await self.callbacks.write_group(link, keyboard, anchor_outbox_id=row.id, presentation_version=version, chat_id=link.chat_id, message_id=None, message_thread_id=None, session=writer)
        row.parts = [*old, {**fresh[0], "callback_group_id": group.group_id}]
        for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == group.group_id)):
            control.created_at = min(value.created_at for value in controls)
            control.payload = {**control.payload, "presentation_pending": True}
        await writer.flush()
        frozen_view(SimpleNamespace(link=link, item=_item(row), part=None), pending=True)


class _RepairProjection(_NavigationProjection):
    def __init__(self, snapshot, item, claim):
        super().__init__(snapshot)
        self.item, self.claim = item, claim

    def extend(self, query):
        query = super().extend(query)
        self.repair_column = len(query.column_descriptions)
        row, control = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        return query.add_columns(row, control).join(row, row.id == self.item.id).outerjoin(control, control.group_id == self.item.parts[-1]["callback_group_id"])

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        row = rows[0][self.repair_column]
        if _item(row) != self.item or claim_of(row) != self.claim:
            raise DeliveryClaimRetired()
        controls = list({control.id: control for *_, control in rows if control is not None}.values())
        try:
            _validate_rows(row, controls, allow_pending=True)
            for control in controls:
                self.snapshot.owner.plans.callbacks._usable(control)
                if control.message_id is not None:
                    raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None


class _RepairGuard:
    def __init__(self, base, admission, snapshot, item, claim):
        self.base, self.admission, self.snapshot, self.item, self.claim = base, admission, snapshot, item, claim

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        await self.admission(session=writer, projection=_RepairProjection(self.snapshot, self.item, self.claim))
        self.base.current()
        return current
