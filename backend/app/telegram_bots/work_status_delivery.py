"""Status roots share the source-owned Work notice delivery protocol."""

from dataclasses import asdict

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.authority import AccessOff, CeilingRefused
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.settings import SettingsRepository
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_details import WorkDetailProjection, WorkDetailsReader
from app.telegram_bots.work_news_delivery import WorkNewsPlans
from app.telegram_bots.work_news_sources import WorkNewsRead
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_status import StatusProjection, WorkStatusReader
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.utils.file_io import await_drained


def notice(source, item, *, detail=None, dates=None):
    data = {"id": item.work_id, "attempt_id": source.attempt_id, "terminal": source.terminal, "event_proofs": source.event_proofs, **({"activation_act_id": source.activation_act_id} if source.activation_act_id is not None else {})}
    if detail is not None:
        data.update(detail=thaw(asdict(detail)), detail_dates=dates)
    return WorkNewsRead(
        work_id=item.work_id,
        key=source.key,
        values=source.values,
        protected=source.protected,
        work=data,
        event_signature=source.origin_signature,
        signature=digest([source.signature, data]) if detail is not None else source.signature,
    )


class StatusNoticeProjection:
    def __init__(self, reader, link, item, source=None, *, clock, detail=None):
        from app.telegram_bots.work_view_sources import detail_read

        self.item, self.source, self.clock = item, source, clock
        self.status = StatusProjection(link, item, clock)
        detail = detail_read(source.work["detail"]) if source is not None and "detail" in source.work else detail
        self.detail = WorkDetailProjection(reader, link, detail.record, detail.request, reusable=detail.can_reuse_conversation, expected=detail) if detail is not None else None

    def extend(self, query):
        result = self.status.extend(query)
        self.delivery_column = self.status.column + 4
        return self.detail.extend(result) if self.detail is not None else result

    def check_with_permissions(self, rows, held):
        if self.detail is not None:
            self.detail.check_with_permissions(rows, held)
        self._check(rows)

    def check(self, rows):
        if self.detail is not None:
            raise ProductSourceRetired()
        self._check(rows)

    def _check(self, rows):
        self.status.check(rows)
        status = self.status.source
        if self.item.dedupe_key != "progress:" + status.attempt_id:
            raise ProductSourceRetired()
        from app.telegram_bots.work_news_carriers import detail_dates

        detail = self.detail.source if self.detail is not None else None
        fresh = notice(status, self.item, detail=detail, dates=detail_dates(detail, self.clock) if detail is not None else None)
        if self.source is not None and not same_body(asdict(self.source), asdict(fresh)):
            raise ProductSourceRetired()
        self.source = fresh


class StatusNoticeReader(WorkStatusReader):
    def __init__(self, reader, *, settings, clock):
        super().__init__(reader, settings=settings, clock=clock)
        self.work, self.links, self.authority = reader.work, reader.links, reader.authority
        self.details = WorkDetailsReader(work=self.work, authority=self.authority, links=self.links, clock=clock)

    async def prepare_clock(self, link, item):
        return await self.prepare(link)

    async def read(self, link, item):
        async def perform():
            projection = StatusNoticeProjection(self, link, item, clock=await self.prepare(link), detail=await self.details.detail(link, item.work_id))
            admission = await prepare_attention(self, link)
            await admission(projection=projection)
            return projection.source

        try:
            return await await_drained(perform())
        except (AccessOff, CeilingRefused, AgentConflict, AgentDenied):
            raise ProductSourceRetired() from None


class WorkStatusPlans(WorkNewsPlans):
    def __init__(self, *, reader, outbox, callbacks):
        status = StatusNoticeReader(reader, settings=SettingsRepository(outbox.sessions, outbox.config), clock=outbox.clock)
        super().__init__(reader=status, outbox=outbox, callbacks=callbacks, projection=StatusNoticeProjection)
