"""Refresh source-owned notice previews without losing their navigation proof."""

from types import SimpleNamespace

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired, _item
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_navigation import _same_item
from app.telegram_bots.work_digest_preview_plans import frozen_preview
from app.telegram_bots.work_digest_projection import DigestProjection
from app.telegram_bots.work_navigation_recovery import _NavigationRepairProjection, inherit_navigation_age
from app.telegram_bots.work_navigation_sources import qualify_navigation
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow
from deerflow.utils.file_io import await_drained


class NoticeNavigationRecovery:
    def __init__(self, owner, navigation, family):
        self.owner, self.navigation, self.family = owner, navigation, family
        self.outbox, self.callbacks = owner.outbox, owner.callbacks

    async def _current(self, link, item, basis):
        if self.navigation is None:
            raise ProductSourceRetired()
        origin = await self.navigation.restore(link, basis["navigation_origin"])
        source = await self.owner.reader.read(link, item)
        settings = await self.owner.plans.settings.get(link.owner_user_id)
        if settings is None:
            raise ProductSourceRetired()
        clock = await self.owner.reader.prepare_clock(link, item) if self.family == "work_news_basis" else None
        # Qualify the same family/event before mutation as well as at dispatch.
        await qualify_navigation(self.navigation, SimpleNamespace(link=link, item=item), basis, source, self._source_projection(link, item, source, clock), family=self.family)
        return origin, source, settings, clock

    def _source_projection(self, link, item, source, clock):
        if self.family == "work_news_basis":
            return self.owner.plans.projection(self.owner.reader, link, item, source, clock=clock)
        from app.telegram_bots.result_notice_sources import ResultNoticeProjection

        return ResultNoticeProjection(self.owner.reader, link, item, source)

    async def _qualify(self, admission, writer, origin, source, row, clock):
        item, link = _item(row), origin.snapshot.link
        projection = DigestProjection(link, [self._source_projection(link, item, source, clock), _NavigationRepairProjection(origin, item, claim_of(row))])
        return await admission(session=writer, projection=projection)

    async def _replace(self, writer, link, row, source, settings, origin, *, repair=False):
        options = dict(repair=repair, navigation_origin=origin.value)
        if self.family == "work_news_basis":
            from app.telegram_bots.work_result_navigation import NavigationOrigin

            await self.owner._replace(writer, link, row, source, settings, NavigationOrigin(), **options)
        else:
            await self.owner._replace(writer, link, row, source, settings, **options)
        await inherit_navigation_age(writer, row, origin)
        fresh = [part for part in row.parts if part["presentation_version"] == row.payload["presentation_version"]]
        selected = next((part for part in fresh if part["state"] == "pending"), fresh[-1])
        frozen_preview(SimpleNamespace(link=link, item=_item(row), part=selected), pending=selected["state"] == "pending", acknowledged=selected["state"] == "sent", replay=True)

    async def refresh(self, captured):
        async def perform():
            basis, _previous, _part = frozen_preview(captured, replay=True)
            origin, source, settings, clock = await self._current(captured.link, captured.item, basis)
            admission = await prepare_attention(self.owner.reader, captured.link)
            async with self.outbox._writer(captured.link.id) as (writer, binding):
                row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True, populate_existing=True)
                current = self.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
                if current != captured or not _same_item(row, captured.item):
                    raise DeliveryClaimRetired()
                await self._qualify(admission, writer, origin, source, row, clock)
                await self._replace(writer, captured.link, row, source, settings, origin)
                await self._qualify(admission, writer, origin, source, row, clock)

        return await await_drained(perform())

    async def repair(self, link, item, *, guard):
        async def perform():
            parts = [part for part in item.parts if part["presentation_version"] == item.payload["presentation_version"]]
            selected = next((part for part in parts if part["state"] == "pending"), None)
            if selected is None:
                raise ProductSourceRetired()
            basis, _previous, _part = frozen_preview(SimpleNamespace(link=link, item=item, part=selected), pending=True, replay=True)
            origin, source, settings, clock = await self._current(link, item, basis)
            admission = await prepare_attention(self.owner.reader, link)
            composed = _RepairGuard(self, guard, admission, origin, source, item, clock)
            async with self.callbacks.writer(link, composed) as writer:
                row = await writer.get(TelegramOutboxRow, item.id, with_for_update=True, populate_existing=True)
                if not self.owner._repairable(row) or not _same_item(row, item) or claim_of(row) != (None, None, None):
                    raise ProductSourceRetired()
                await self._replace(writer, link, row, source, settings, origin, repair=True)
                composed.item = _item(row)
                return row.id

        return await await_drained(perform())


class _RepairGuard:
    def __init__(self, owner, base, admission, origin, source, item, clock):
        self.owner, self.base, self.admission, self.origin, self.source, self.item, self.clock = owner, base, admission, origin, source, item, clock

    async def prepare(self):
        await prepare_publication_guard(self.base)

    async def check(self, writer):
        current = await self.base.check(writer)
        row = await writer.get(TelegramOutboxRow, self.item.id, populate_existing=True)
        if row is None or not _same_item(row, self.item) or claim_of(row) != (None, None, None):
            raise ProductSourceRetired()
        await self.owner._qualify(self.admission, writer, self.origin, self.source, row, self.clock)
        self.base.current()
        return current
