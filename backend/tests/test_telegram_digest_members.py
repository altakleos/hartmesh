"""Digest titles retain canonical member source and final access qualification."""

from types import SimpleNamespace

import pytest
from _telegram_repository_test_support import db as db
from test_telegram_work_event_delivery import queued
from test_telegram_work_file_exports import work_exports as work_exports
from test_telegram_work_source_delivery import work_delivery as work_delivery
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.producer import ProductSourceRetired, _item
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
async def test_pending_question_can_be_validated_without_fabricating_dispatch(work_delivery):
    from app.telegram_bots.work_event_delivery import WorkEventDelivery
    from app.telegram_bots.work_request_source import frozen

    e = work_delivery
    identity, _ = await queued(e)
    await WorkEventDelivery(e.plans).materialize(e.link, identity)
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, identity))
    basis, source, recipe = frozen(SimpleNamespace(link=e.link, item=item, part=item.parts[0]), pending=True)
    assert source.request["question"] == "Which source?"
    assert recipe == basis["parts"][0]
    assert item.parts[0]["state"] == "pending"


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "event", "claim", "request"])
async def test_question_digest_source_is_rechecked_after_provider(work_delivery, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots.work_digest_members import DigestMembers
    from app.telegram_bots.work_digest_projection import DigestProjection
    from app.telegram_bots.work_event_delivery import WorkEventDelivery
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.agent_instances.work import AgentWorkEventRow

    e = work_delivery
    identity, req = await queued(e)
    await WorkEventDelivery(e.plans).materialize(e.link, identity)
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, identity))
    members = DigestMembers(requests=e.reader)
    member = await members.read(e.link, item, locale="en")
    assert member.entry.kind == "question" and member.entry.objective
    assert not member.protected
    original = authz.resolve_route_permissions

    async def late(*args, **kwargs):
        held = await original(*args, **kwargs)
        if change:
            async with e.db.sessions.begin() as writer:
                if change == "event":
                    (await writer.get(AgentWorkEventRow, item.event_id)).actor_id = "changed-source"
                elif change == "request":
                    row = await writer.get(HumanInputRequestRow, req["id"])
                    row.state, row.closed_reason = "closed", "resolved"
                else:
                    (await writer.get(TelegramOutboxRow, item.id)).claim_id = "another-owner"
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", late)
    admission = await prepare_attention(e.reader, e.link)
    if change:
        with pytest.raises(ProductSourceRetired):
            await admission(projection=DigestProjection(e.link, [member.projection]))
    else:
        await admission(projection=DigestProjection(e.link, [member.projection]))


@pytest.mark.asyncio
async def test_digest_news_title_comes_from_current_canonical_notice(work_delivery):
    from test_telegram_work_news_delivery import prepare

    from app.telegram_bots.work_digest_members import DigestMembers
    from app.telegram_bots.work_digest_projection import DigestProjection

    e = work_delivery
    plans, identity = await prepare(e)
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, identity))
    member = await DigestMembers(requests=e.reader, news=plans.reader).read(e.link, item, locale="en")
    assert member.entry.kind == "news" and member.entry.first_line == "event-peer@example.com changed an assignment"
    admission = await prepare_attention(e.reader, e.link)
    await admission(projection=DigestProjection(e.link, [member.projection]))


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["review_event"], indirect=True)
async def test_digest_review_retains_result_identity_without_reading_files(work_exports):
    from test_telegram_work_review_event_delivery import queued

    from app.telegram_bots.work_digest_members import DigestMembers
    from app.telegram_bots.work_digest_projection import DigestProjection
    from app.telegram_bots.work_review_event_delivery import WorkReviewEventDelivery

    e = work_exports
    plans, identity = await queued(e)
    await WorkReviewEventDelivery(plans).materialize(e.link, identity)
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, identity))
    member = await DigestMembers(requests=e.reader).read(e.link, item, locale="en")
    assert member.entry.kind == "review" and not e.native_reads
    admission = await prepare_attention(e.reader, e.link)
    await admission(projection=DigestProjection(e.link, [member.projection]))


@pytest.mark.asyncio
async def test_digest_manager_question_qualifies_routing_roster(work_delivery, monkeypatch):
    from test_telegram_work_routing_event_delivery import routing_event

    from app.telegram_bots.work_digest_members import DigestMembers
    from app.telegram_bots.work_digest_projection import DigestProjection
    from app.telegram_bots.work_routing_event_delivery import WorkRoutingEventDelivery

    e = work_delivery
    plans, identity, _, _ = await routing_event(e, monkeypatch)
    await WorkRoutingEventDelivery(plans).materialize(e.link, identity)
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, identity))
    member = await DigestMembers(requests=e.reader, routing=plans.reader).read(e.link, item, locale="en")
    assert member.entry.kind == "news" and member.entry.first_line
    admission = await prepare_attention(e.reader, e.link)
    await admission(projection=DigestProjection(e.link, [member.projection]))


@pytest.mark.asyncio
@pytest.mark.parametrize("work_exports", ["done_suggestion_event"], indirect=True)
async def test_digest_result_navigation_requires_current_exact_result(work_exports):
    from test_telegram_result_notice_delivery import prepare

    from app.telegram_bots.work_digest_members import DigestMembers
    from app.telegram_bots.work_digest_projection import DigestProjection

    e = work_exports
    plans, identity = await prepare(e)
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, identity))
    member = await DigestMembers(requests=e.reader, results=plans.reader).read(e.link, item, locale="en")
    assert member.entry.kind == "news" and member.entry.first_line.startswith("Done:")
    admission = await prepare_attention(e.reader, e.link)
    await admission(projection=DigestProjection(e.link, [member.projection]))


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["permissions", "scope", "parts"])
async def test_digest_questions_refuse_unqualified_navigation_sources(work_delivery, monkeypatch, change):
    from dataclasses import replace

    from app.gateway import authz
    from app.telegram_bots.work_digest_members import DigestMembers
    from app.telegram_bots.work_digest_projection import DigestProjection
    from app.telegram_bots.work_event_delivery import WorkEventDelivery

    e = work_delivery
    identity, _ = await queued(e)
    await WorkEventDelivery(e.plans).materialize(e.link, identity)
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, identity))
    members = DigestMembers(requests=e.reader)
    if change == "parts":
        item = replace(item, parts=[{**part, "message_ids": [999]} for part in item.parts])
        with pytest.raises(ProductSourceRetired):
            await members.read(e.link, item, locale="en")
        return
    member = await members.read(e.link, item, locale="en")
    original = authz.resolve_route_permissions

    async def late(*args, **kwargs):
        held = await original(*args, **kwargs)
        if change == "scope":
            async with e.db.sessions.begin() as writer:
                (await writer.get(TelegramOutboxRow, item.id)).chat_id = 999
        return frozenset(held) - {"agents:write"} if change == "permissions" else held

    monkeypatch.setattr(authz, "resolve_route_permissions", late)
    admission = await prepare_attention(e.reader, e.link)
    with pytest.raises(ProductSourceRetired):
        await admission(projection=DigestProjection(e.link, [member.projection]))


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("purpose", "decision"), ("question", "Substituted question"), ("reason", "Substituted reason")])
async def test_digest_cannot_adopt_unversioned_request_semantics(work_delivery, field, value):
    from app.telegram_bots.work_digest_members import DigestMembers
    from app.telegram_bots.work_digest_projection import DigestProjection
    from app.telegram_bots.work_event_delivery import WorkEventDelivery
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

    e = work_delivery
    identity, req = await queued(e)
    await WorkEventDelivery(e.plans).materialize(e.link, identity)
    async with e.db.sessions.begin() as writer:
        setattr(await writer.get(HumanInputRequestRow, req["id"]), field, value)
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, identity))
    with pytest.raises(ProductSourceRetired):
        member = await DigestMembers(requests=e.reader).read(e.link, item, locale="en")
        admission = await prepare_attention(e.reader, e.link)
        await admission(projection=DigestProjection(e.link, [member.projection]))


@pytest.mark.asyncio
@pytest.mark.parametrize("version", [[], {}], ids=["list", "object"])
async def test_digest_malformed_request_version_is_retired(work_delivery, version):
    from dataclasses import replace

    from app.telegram_bots.work_digest_members import DigestMembers

    e = work_delivery
    async with e.db.sessions() as reader:
        item = _item(await reader.get(TelegramOutboxRow, e.identity))
    item = replace(item, payload={**item.payload, "work_request_basis": {**item.payload["work_request_basis"], "version": version}})
    with pytest.raises(ProductSourceRetired):
        await DigestMembers(requests=e.reader).read(e.link, item, locale="en")
