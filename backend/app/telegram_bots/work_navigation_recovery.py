"""Refresh full request previews while retaining their exact navigation origin."""

from types import SimpleNamespace

from sqlalchemy import select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired, _item
from app.telegram_bots.work_digest_members import claim_of, require_request_origin
from app.telegram_bots.work_digest_navigation import _same_item
from app.telegram_bots.work_digest_projection import DigestProjection
from app.telegram_bots.work_navigation_origin import NavigationOriginProjection
from app.telegram_bots.work_request_recovery import WorkRequestRecovery, _RecoveryProjection
from app.telegram_bots.work_request_source import frozen
from app.telegram_bots.work_result_recovery import WorkResultRecovery, _ResultRecoveryProjection
from app.telegram_bots.work_routing_recovery import WorkRoutingRecovery, _RoutingRecoveryProjection
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained


async def inherit_navigation_age(writer, row, origin):
    """Even a previously buttonless preview keeps the original gesture age."""
    original = await writer.get(TelegramCallbackRow, origin.value["callback_id"], with_for_update=True, populate_existing=True)
    if original is None:
        raise ProductSourceRetired()
    groups = {part["callback_group_id"] for part in row.parts if part["presentation_version"] == row.payload["presentation_version"] and "callback_group_id" in part}
    for control in await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups))):
        control.created_at = original.created_at
    await writer.flush()


class WorkNavigationRecovery(WorkRequestRecovery):
    def __init__(self, plans, *, navigation):
        super().__init__(plans)
        self.navigation = navigation

    async def refresh_navigation(self, captured):
        return await await_drained(self._refresh(captured))

    async def _refresh(self, captured):
        basis, _previous, _part = frozen(captured, replay="navigation_origin" in captured.item.payload.get("work_request_basis", {}))
        if "navigation_origin" not in basis:
            return await super()._refresh(captured)
        origin = await self.navigation.restore(captured.link, basis["navigation_origin"])
        source, settings = await self._source(captured.link, captured.item.request_id, intent=captured.item.payload.get("work_event_intent"))
        require_request_origin(origin.member.source, source)
        admission = await prepare_attention(self.reader, captured.link)
        async with self.outbox._writer(captured.link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
            current = self.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
            if current != captured or not _same_item(row, captured.item):
                raise DeliveryClaimRetired()
            await self._qualify(admission, writer, origin, source, row)
            await self._replace(writer, captured.link, row, source, settings, navigation_origin=origin.value)
            await inherit_navigation_age(writer, row, origin)
            self._validate_fresh(captured.link, row)
            await self._qualify(admission, writer, origin, source, row)

    @staticmethod
    def _validate_fresh(link, row):
        parts = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        selected = next((part for part in parts if part["state"] == "pending"), parts[-1])
        frozen(SimpleNamespace(link=link, item=_item(row), part=selected), pending=selected["state"] == "pending", acknowledged=selected["state"] == "sent", replay=True)

    def _source_projection(self, source):
        return _RecoveryProjection(source)

    async def _qualify(self, admission, writer, origin, source, row):
        projection = DigestProjection(origin.snapshot.link, [self._source_projection(source), _NavigationRepairProjection(origin, _item(row), claim_of(row))])
        return await admission(session=writer, projection=projection)

    held_reason = None

    def _held(self, row):
        return self.held_reason is not None and row is not None and row.state == "failed" and row.failed_reason == self.held_reason and claim_of(row) == (None, None, None) and row.message_thread_id is None

    async def _replace_held(self, writer, link, row, source, settings, origin):
        await self._replace(writer, link, row, source, settings, force_new=True, navigation_origin=origin.value)

    async def repair(self, link, outbox_id, *, guard):
        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, outbox_id)
                if row is None or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
                    raise CallbackRefused()
                if "navigation_origin" not in row.payload.get("work_request_basis", {}):
                    fallback = getattr(super(WorkNavigationRecovery, self), "repair", None)
                    return await fallback(link, outbox_id, guard=guard) if fallback else None
                if not self._held(row):
                    return None
                item = _item(row)
                parts = [part for part in item.parts if part["presentation_version"] == item.payload["presentation_version"]]
                selected = next((part for part in parts if part["state"] == "pending"), None)
                if selected is None:
                    raise ProductSourceRetired()
                basis, _previous, _part = frozen(SimpleNamespace(link=link, item=item, part=selected), pending=True, replay=True)
            origin = await self.navigation.restore(link, basis["navigation_origin"])
            source, settings = await self._source(link, item.request_id, intent=item.payload.get("work_event_intent"))
            require_request_origin(origin.member.source, source)
            admission = await prepare_attention(self.reader, link)
            composed = _RefreshGuard(self, guard, origin, source, item, admission)
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, outbox_id, with_for_update=True, populate_existing=True)
                if not self._held(row) or not _same_item(row, item):
                    raise ProductSourceRetired()
                await self._replace_held(writer, link, row, source, settings, origin)
                await inherit_navigation_age(writer, row, origin)
                self._validate_fresh(link, row)
                composed.item = _item(row)
                return outbox_id

        return await await_drained(perform())

    async def refresh_anchor(self, link, outbox_id, *, expected_version, guard):
        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramOutboxRow, outbox_id)
                if row is None or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id):
                    raise CallbackRefused()
                if "navigation_origin" not in row.payload.get("work_request_basis", {}):
                    return await super(WorkNavigationRecovery, self).refresh_anchor(link, outbox_id, expected_version=expected_version, guard=guard)
                if row.state != "sent" or row.payload["presentation_version"] != expected_version:
                    return None
                item = _item(row)
                basis, _source, _part = frozen(SimpleNamespace(link=link, item=item, part=item.parts[-1]), acknowledged=True, replay=True)
            origin = await self.navigation.restore(link, basis["navigation_origin"])
            source, settings = await self._source(link, item.request_id, intent=item.payload.get("work_event_intent"))
            require_request_origin(origin.member.source, source)
            admission = await prepare_attention(self.reader, link)
            composed = _RefreshGuard(self, guard, origin, source, item, admission)
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, outbox_id, with_for_update=True, populate_existing=True)
                if not _same_item(row, item) or claim_of(row) != (None, None, None):
                    raise ProductSourceRetired()
                await self._replace(writer, link, row, source, settings, navigation_origin=origin.value)
                await inherit_navigation_age(writer, row, origin)
                self._validate_fresh(link, row)
                composed.item = _item(row)
                return outbox_id

        return await await_drained(perform())


class _RefreshGuard:
    def __init__(self, owner, base, origin, source, item, admission):
        self.owner, self.base, self.origin, self.source, self.item, self.admission = owner, base, origin, source, item, admission

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        row = await writer.get(TelegramOutboxRow, self.item.id, populate_existing=True)
        if row is None or not _same_item(row, self.item) or claim_of(row) != (None, None, None):
            raise ProductSourceRetired()
        await self.owner._qualify(self.admission, writer, self.origin, self.source, row)
        self.base.current()
        return current


class _NavigationRepairProjection(NavigationOriginProjection):
    def __init__(self, origin, item, claim):
        super().__init__(origin)
        self.item, self.claim = item, claim

    def extend(self, query):
        query = super().extend(query)
        self.repair_column = len(query.column_descriptions)
        row, callback = aliased(TelegramOutboxRow), aliased(TelegramCallbackRow)
        groups = {part["callback_group_id"] for part in self.item.parts if part["presentation_version"] == self.item.payload["presentation_version"] and "callback_group_id" in part}
        return query.add_columns(row, callback).join(row, row.id == self.item.id).outerjoin(callback, callback.group_id.in_(groups))

    def check_with_permissions(self, rows, held):
        super().check_with_permissions([row[: self.repair_column] for row in rows], held)
        row = rows[0][self.repair_column]
        if not _same_item(row, self.item) or claim_of(row) != self.claim:
            raise ProductSourceRetired()
        controls = {value.id: value for result in rows if (value := result[self.repair_column + 1]) is not None}
        origin_column = self.column + 2 if self.origin_column is None else self.origin_column + 1
        original = next(result[origin_column] for result in rows if result[origin_column] is not None and result[origin_column].id == self.navigation.value["callback_id"])
        try:
            _validate_rows(row, list(controls.values()), allow_pending=True)
            for control in controls.values():
                self.navigation.owner.plans.callbacks._usable(control)
                if aware(control.created_at) != aware(original.created_at):
                    raise CallbackRefused()
                part = next(part for part in row.parts if part.get("callback_group_id") == control.group_id and part["presentation_version"] == row.payload["presentation_version"])
                acknowledged = part["state"] == "sent"
                if control.message_id != (part["message_ids"][-1] if acknowledged else None) or control.payload.get("presentation_pending") is not (not acknowledged):
                    raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None


# The common navigation transition delegates family source reads and media or
# routing reconstruction to the existing recovery owners.


class WorkReviewNavigationRecovery(WorkNavigationRecovery, WorkResultRecovery):
    held_reason = "work-result-refresh-required"

    async def _replace_held(self, writer, link, row, source, settings, origin):
        await self._replace(writer, link, row, source, settings, repair=True, navigation_origin=origin.value)

    def _source_projection(self, source):
        return _ResultRecoveryProjection(source)


class WorkRoutingNavigationRecovery(WorkNavigationRecovery, WorkRoutingRecovery):
    held_reason = "work-routing-refresh-required"

    def _source_projection(self, source):
        return _RoutingRecoveryProjection(self.outbox.sessions, source)
