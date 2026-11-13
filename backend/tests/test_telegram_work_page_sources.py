"""Work list source pages requalify their entire displayed row set together."""

import pytest
from _telegram_repository_test_support import db as db
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.authority import AuthorityUnavailable
from deerflow.persistence.agent_instances.work import AgentWorkRow


@pytest.mark.asyncio
async def test_page_sources_keep_snapshot_order_and_complete_current_details(work_pages):
    from app.telegram_bots.work_view_reads import WorkViewsReader

    e = work_pages
    reader = WorkViewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    token = await reader.open_list(e.link)
    source = await reader.read_page(e.link, token)
    assert source.frame["work"][0]["id"] == e.work["id"]
    assert source.details[0].record["objective"] == e.work["objective"]
    assert source.details[0].request["id"] == e.request["id"]
    assert source.frame["pending"][0]["id"] == e.request["id"]
    check = await reader.prepare_page(e.link, source)
    async with e.db.sessions.begin() as writer:
        await check(session=writer)
    async with e.db.sessions.begin() as writer:
        (await writer.get(AgentWorkRow, e.work["id"])).progress = "New progress since page preparation"
    with pytest.raises(AuthorityUnavailable):
        async with e.db.sessions.begin() as writer:
            await check(session=writer)


@pytest.mark.asyncio
async def test_ten_row_source_uses_one_shared_join_and_empty_list_stays_authorized(work_pages):
    from test_work_attention_pages import seed_records

    from app.telegram_bots.work_view_reads import WorkViewsReader

    e = work_pages
    await seed_records(e, 11)
    reader = WorkViewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    first = await reader.read_page(e.link, await reader.open_list(e.link))
    assert len(first.frame["work"]) == len(first.details) == 10
    assert first.frame["pages"] == 2
    check = await reader.prepare_page(e.link, first)
    async with e.db.sessions.begin() as writer:
        await check(session=writer)
    from sqlalchemy import delete, select

    from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputReadRow, HumanInputRequestRow, HumanInputResponseRow
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow

    async with e.db.sessions.begin() as writer:
        requests = select(HumanInputRequestRow.id).where(HumanInputRequestRow.instance_id == e.instance.id)
        for model in (HumanInputReadRow, HumanInputResponseRow, HumanInputEventRow):
            await writer.execute(delete(model).where(model.request_id.in_(requests)))
        await writer.execute(delete(HumanInputRequestRow).where(HumanInputRequestRow.instance_id == e.instance.id))
        await writer.execute(delete(AgentWorkEventRow).where(AgentWorkEventRow.instance_id == e.instance.id))
        await writer.execute(delete(AgentWorkAttemptRow).where(AgentWorkAttemptRow.work_id.in_(select(AgentWorkRow.id).where(AgentWorkRow.instance_id == e.instance.id))))
        await writer.execute(delete(AgentWorkRow).where(AgentWorkRow.instance_id == e.instance.id))
    empty = await reader.read_page(e.link, await reader.open_list(e.link))
    assert not empty.frame["work"] and not empty.details and empty.name
    check = await reader.prepare_page(e.link, empty)
    async with e.db.sessions.begin() as writer:
        await check(session=writer)


@pytest.mark.asyncio
async def test_saved_pages_keep_original_slots_and_empty_pages_keep_navigation(work_pages):
    from sqlalchemy import delete
    from test_work_attention_pages import seed_records

    from app.telegram_bots.work_view_reads import WorkViewsReader
    from app.telegram_bots.work_views import present_work_page

    e = work_pages
    await seed_records(e, 11)
    reader = WorkViewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    token = await reader.open_list(e.link)
    first = await reader.read_page(e.link, token)
    second = await reader.read_page(e.link, first.frame["next"])
    original = [record["id"] for record in second.frame["work"]]
    removed = next(record["id"] for record in first.frame["work"] if record["id"] != e.work["id"])
    async with e.db.sessions.begin() as writer:
        await writer.execute(delete(AgentWorkRow).where(AgentWorkRow.id == removed))
    following = await reader.read_page(e.link, first.frame["next"])
    assert [record["id"] for record in following.frame["work"]] == original
    async with e.db.sessions.begin() as writer:
        await writer.execute(delete(AgentWorkRow).where(AgentWorkRow.id.in_(original)))
    empty = await reader.read_page(e.link, first.frame["next"])
    assert empty.frame["pages"] == 2 and empty.frame["previous"] == token and not empty.frame["work"]
    view = present_work_page(empty.frame, link=e.link, owner_id=e.link.owner_user_id, name=empty.name, locale="en-US")
    assert any(button.get("kind") == "work_list_page" for line in view.keyboard for button in line)
