"""Several digest sources share the same final authority SQL statement."""

import pytest
from _telegram_repository_test_support import db as db
from sqlalchemy import select
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import seed_requests
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
from app.telegram_bots.work_request_recovery import _RecoveryProjection
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_digest_composes_distinct_member_queries_with_final_provider_check(work_delivery, monkeypatch, changed):
    from app.gateway import authz
    from app.telegram_bots.work_digest_projection import DigestProjection

    e = work_delivery
    await seed_requests(e, hidden=0, pending=1)
    async with e.db.sessions() as reader:
        identities = list(await reader.scalars(select(HumanInputRequestRow.id).where(HumanInputRequestRow.instance_id == e.instance.id)))
    assert len(identities) == 2
    sources = [await e.reader.read_request(e.link, identity) for identity in identities]
    original = authz.resolve_route_permissions

    async def late(*args, **kwargs):
        result = await original(*args, **kwargs)
        if changed:
            async with e.db.sessions.begin() as writer:
                (await writer.get(HumanInputRequestRow, identities[-1])).revision += 1
        return result

    monkeypatch.setattr(authz, "resolve_route_permissions", late)
    projection = DigestProjection(e.link, [_RecoveryProjection(source) for source in sources])
    admission = await prepare_attention(e.reader, e.link)
    if changed:
        with pytest.raises(DeliveryRetry):
            await admission(projection=projection)
    else:
        await admission(projection=projection)
        assert projection.widths == [3, 3]


@pytest.mark.asyncio
async def test_digest_missing_member_cannot_hide_behind_another_valid_member(work_delivery):
    from dataclasses import replace

    from app.telegram_bots.work_digest_projection import DigestProjection

    e = work_delivery
    source = await e.reader.read_request(e.link, e.request["id"])
    missing = replace(source, request={**source.request, "id": "f" * 32})
    admission = await prepare_attention(e.reader, e.link)
    with pytest.raises(ProductSourceRetired):
        await admission(projection=DigestProjection(e.link, [_RecoveryProjection(source), _RecoveryProjection(missing)]))


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_digest_preserves_aliased_news_actor_and_request_models(work_delivery, monkeypatch, changed):
    from test_telegram_work_news_sources import news

    from app.gateway import authz
    from app.telegram_bots.work_digest_projection import DigestProjection
    from app.telegram_bots.work_news_sources import WorkNewsProjection, WorkNewsReader
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    request = await e.reader.read_request(e.link, e.request["id"])
    item, _result, _peer = await news(e)
    reader = WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    source = await reader.read(e.link, item)
    original = authz.resolve_route_permissions

    async def late(*args, **kwargs):
        result = await original(*args, **kwargs)
        if changed:
            async with e.db.sessions.begin() as writer:
                (await writer.get(AgentWorkEventRow, item.event_id)).actor_id = e.link.owner_user_id
        return result

    monkeypatch.setattr(authz, "resolve_route_permissions", late)
    projection = DigestProjection(e.link, [WorkNewsProjection(reader, e.link, item, source), _RecoveryProjection(request)])
    admission = await prepare_attention(e.reader, e.link)
    if changed:
        with pytest.raises(ProductSourceRetired):
            await admission(projection=projection)
    else:
        await admission(projection=projection)
        assert projection.widths == [6, 3]


@pytest.mark.asyncio
async def test_ten_complex_sources_fit_postgres_result_column_limit(work_delivery):
    from sqlalchemy.dialects.postgresql import asyncpg
    from sqlalchemy.dialects.sqlite import dialect
    from test_telegram_attempt_notices import ended

    from app.telegram_bots.work_digest_projection import DigestProjection, _member_base
    from app.telegram_bots.work_news_sources import WorkNewsProjection, WorkNewsReader

    e = work_delivery
    item = await ended(e, "attempt.no_result")
    reader = WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    source = await reader.read(e.link, item)
    projection = DigestProjection(e.link, [WorkNewsProjection(reader, e.link, item, source) for _ in range(10)])
    query = projection.extend(_member_base(e.link))
    for target in (dialect(), asyncpg.dialect()):
        compiled = query.compile(dialect=target)
        assert len(compiled._result_columns) <= 1664
    admission = await prepare_attention(e.reader, e.link)
    await admission(projection=projection)


@pytest.mark.asyncio
@pytest.mark.parametrize("permitted", [True, False])
async def test_compact_source_preserves_verified_output_and_final_route_permissions(work_delivery, monkeypatch, permitted):
    from test_telegram_attempt_output import captured_output

    from app.gateway import authz
    from app.telegram_bots.work_digest_projection import DigestProjection
    from app.telegram_bots.work_news_sources import WorkNewsProjection, WorkNewsReader

    e = work_delivery
    item, text = await captured_output(e)
    reader = WorkNewsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)
    direct = await reader.read(e.link, item)
    assert direct.values["excerpt"] == text
    original = authz.resolve_route_permissions

    async def permissions(*args, **kwargs):
        held = await original(*args, **kwargs)
        return held if permitted else frozenset(held) - {"runs:read", "threads:read"}

    monkeypatch.setattr(authz, "resolve_route_permissions", permissions)
    source = WorkNewsProjection(reader, e.link, item, work=direct.work, protected=direct.protected)
    admission = await prepare_attention(e.reader, e.link)
    await admission(projection=DigestProjection(e.link, [source]))
    assert source.source.values["excerpt"] == (text if permitted else None)
    assert bool(source.source.values["thread_id"]) is permitted
