"""Complete summary counts and titles stay bound to the final authority read."""

from datetime import timedelta

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages


@pytest.mark.asyncio
async def test_summary_source_reads_existing_requests_without_creating_deliveries(work_delivery):
    from sqlalchemy import func, select

    from app.telegram_bots.summary_sources import SummaryReader
    from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

    e = work_delivery
    now = e.sender.outbox.clock()
    async with e.db.sessions() as reader:
        count = await reader.scalar(select(func.count()).select_from(TelegramOutboxRow))
    source = await SummaryReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read(e.link, as_of=now, since=now - timedelta(days=1))
    assert any(item["request_id"] == e.request["id"] for item in source.sections["waiting"])
    await source.authorize()
    async with e.db.sessions() as reader:
        assert await reader.scalar(select(func.count()).select_from(TelegramOutboxRow)) == count


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["title", "request", "grant", "new_work"])
async def test_summary_final_provider_read_rejects_changed_complete_selection(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.summary_sources import SummaryReader
    from deerflow.agent_instances.contract import AgentDenied
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    now = e.sender.outbox.clock()
    source = await SummaryReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read(e.link, as_of=now, since=now - timedelta(days=1))
    resolve, changed = authz.resolve_route_permissions, []

    async def provider(*args, **kwargs):
        held = await resolve(*args, **kwargs)
        if not changed:
            async with e.db.sessions.begin() as writer:
                if change == "title":
                    (await writer.get(AgentWorkRow, e.work["id"])).objective = "Changed after provider"
                elif change == "request":
                    row = await writer.get(HumanInputRequestRow, e.request["id"])
                    row.state, row.closed_reason = "closed", "withdrawn"
                elif change == "grant":
                    (await writer.get(AgentInstanceGrantRow, (e.link.agent_ref, e.link.owner_user_id))).permissions = 1
                else:
                    from copy import deepcopy
                    from uuid import uuid4

                    old = await writer.get(AgentWorkRow, e.work["id"])
                    fields = {column.name: deepcopy(getattr(old, column.name)) for column in old.__table__.columns}
                    fields.update(id=uuid4().hex, status="open", blocker=None, created_at=now - timedelta(hours=1))
                    writer.add(AgentWorkRow(**fields))
            changed.append(True)
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", provider)
    with pytest.raises((ProductSourceRetired, AgentDenied)):
        await source.authorize()
    assert changed


@pytest.mark.asyncio
async def test_summary_source_reads_all_request_pages_before_display_limits(work_delivery):
    from copy import deepcopy
    from uuid import uuid4

    from app.telegram_bots.summary_sources import SummaryReader
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    now = e.sender.outbox.clock()
    async with e.db.sessions.begin() as writer:
        original = await writer.get(AgentWorkRow, e.work["id"])
        question = await writer.get(HumanInputRequestRow, e.request["id"])
        for index in range(106):
            identity, request_id, basis_id = uuid4().hex, uuid4().hex, uuid4().hex
            fields = {column.name: deepcopy(getattr(original, column.name)) for column in original.__table__.columns}
            fields.update(id=identity, created_at=now - timedelta(hours=index + 1), blocker={**fields["blocker"], "id": basis_id})
            writer.add(AgentWorkRow(**fields))
            await writer.flush()
            fields = {column.name: deepcopy(getattr(question, column.name)) for column in question.__table__.columns}
            fields.update(id=request_id, work_id=identity, basis_id=basis_id, created_at=now - timedelta(hours=index + 1))
            writer.add(HumanInputRequestRow(**fields))
    source = await SummaryReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read(e.link, as_of=now, since=now - timedelta(days=1))
    assert len(source.sections["waiting"]) == 107
    assert source.sections["waiting"][-1]["request_id"] == e.request["id"]
    await source.authorize()


@pytest.mark.asyncio
async def test_summary_reader_qualifies_a_colleagues_current_saved_answer(work_delivery):
    from test_telegram_work_answer_news import answered

    from app.telegram_bots.summary_sources import SummaryReader

    e = work_delivery
    item = await answered(e)
    now = e.sender.outbox.clock()
    source = await SummaryReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read(e.link, as_of=now, since=now - timedelta(days=1))
    assert any(row["work_id"] == item.work_id and row["kind"] == "resume" for row in source.sections["waiting"])
    await source.authorize()


class AdditionalProjection:
    def extend(self, query):
        return query

    def check(self, rows):
        pass


@pytest.mark.asyncio
async def test_additional_projection_cannot_remove_summary_source_fence(work_delivery):
    from app.telegram_bots.producer import ProductSourceRetired
    from app.telegram_bots.summary_sources import SummaryReader
    from deerflow.persistence.agent_instances.work import AgentWorkRow

    e = work_delivery
    now = e.sender.outbox.clock()
    source = await SummaryReader(work=e.human.work, authority=e.live.authority, links=e.db.links).read(e.link, as_of=now, since=now - timedelta(days=1))
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkRow, e.work["id"])).objective = "Changed after source read"
    with pytest.raises(ProductSourceRetired):
        await source.authorize(projection=AdditionalProjection())
