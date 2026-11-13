"""Complete detail facts retain current owner, request and conversation authority."""

from datetime import UTC, datetime

import pytest
from _telegram_repository_test_support import db as db
from test_work_attention_pages import work_pages as work_pages

from app.telegram_bots.authority import AuthorityUnavailable
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.persistence.agent_instances.work import AgentWorkRow


def reader(e):
    from app.telegram_bots.work_details import WorkDetailsReader

    return WorkDetailsReader(work=e.human.work, authority=e.live.authority, links=e.db.links)


@pytest.mark.asyncio
async def test_detail_contains_complete_current_work_question_and_capabilities(work_pages):
    e = work_pages
    source = await reader(e).detail(e.link, e.work["id"])
    assert source.record["objective"] == e.work["objective"]
    assert source.record["success_criteria"] == e.work["success_criteria"]
    assert source.request["id"] == e.request["id"] and source.request["can_respond"]
    assert source.permissions & 3 == 3 and "agents:read" in source.held
    assert not source.can_reuse_conversation and not source.lifecycle_pending
    assert len(source.signature) == 64


@pytest.mark.asyncio
@pytest.mark.parametrize("withdraw", ["inspect", "assignment", "request", "definition"])
async def test_final_detail_facts_reject_mutation_at_actual_provider_wait(work_pages, monkeypatch, withdraw):
    from app.gateway import authz
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow

    e = work_pages
    host = reader(e)
    real = authz.resolve_route_permissions
    changed, calls = [], []

    async def revoke(*args, **kwargs):
        held = await real(*args, **kwargs)
        calls.append(True)
        if "prepared_provider" in kwargs and len(calls) == 2 and not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                if withdraw == "inspect":
                    (await writer.get(AgentInstanceGrantRow, (e.instance.id, e.actor.subject_id))).permissions = 1
                elif withdraw == "assignment":
                    (await writer.get(AgentWorkRow, e.work["id"])).objective = "Changed while provider awaited"
                elif withdraw == "definition":
                    from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow

                    row = await writer.get(AgentDefinitionRevisionRow, e.instance.definition_revision)
                    row.config = {**row.config, "work_policy": {"enabled": False}}
                else:
                    (await writer.get(HumanInputRequestRow, e.request["id"])).question = "Changed while provider awaited"
        return held

    monkeypatch.setattr(authz, "resolve_route_permissions", revoke)
    with pytest.raises((AuthorityUnavailable, AgentDenied)):
        await host.detail(e.link, e.work["id"])
    assert changed


@pytest.mark.asyncio
async def test_detail_reuses_only_a_prior_bound_conversation_with_live_metadata(work_pages):
    from test_telegram_activation import seeded_attempt

    from deerflow.agent_instances.conversations import AgentConversations
    from deerflow.persistence.thread_meta.model import ThreadMetaRow

    e = work_pages
    _attempt, captured, _manager = await seeded_attempt(e)
    conversations = AgentConversations(e.human.work.agents)
    await conversations.create(actor=e.actor, instance_id=e.instance.id, thread_id=captured.thread_id)
    source = await reader(e).detail(e.link, e.work["id"])
    assert source.can_reuse_conversation is True
    async with e.db.sessions.begin() as writer:
        await writer.delete(await writer.get(ThreadMetaRow, captured.thread_id))
    assert (await reader(e).detail(e.link, e.work["id"])).can_reuse_conversation is False


@pytest.mark.asyncio
async def test_personal_conversation_reuse_requires_its_live_custodian(work_pages):
    from test_telegram_activation import seeded_attempt

    from app.gateway.auth.models import User
    from deerflow.agent_instances.conversations import AgentConversations
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = work_pages
    custodian = await e.live.users.create_user(User(email="detail-custodian@example.com"))
    async with e.db.sessions.begin() as writer:
        instance = await writer.get(AgentInstanceRow, e.instance.id)
        instance.custody, instance.owner_id = "personal", str(custodian.id)
    _attempt, captured, _manager = await seeded_attempt(e)
    await AgentConversations(e.human.work.agents).create(actor=e.actor, instance_id=e.instance.id, thread_id=captured.thread_id)
    assert (await reader(e).detail(e.link, e.work["id"])).can_reuse_conversation is True
    async with e.db.sessions.begin() as writer:
        writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=str(custodian.id), disabled_at=datetime.now(UTC)))
    assert (await reader(e).detail(e.link, e.work["id"])).can_reuse_conversation is False


@pytest.mark.asyncio
async def test_detail_question_recipient_disable_is_current_routing_fact(work_pages):
    from app.gateway.auth.models import User
    from deerflow.config.auth_config import LOCAL_ACCOUNT_ISSUER
    from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
    from deerflow.persistence.user.access import DisabledIdentityRow

    e = work_pages
    person = await e.live.users.create_user(User(email="detail-recipient@example.com"))
    async with e.db.sessions.begin() as writer:
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=str(person.id), permissions=3))
        (await writer.get(HumanInputRequestRow, e.request["id"])).recipient_id = str(person.id)
    before = await reader(e).detail(e.link, e.work["id"])
    assert not before.request["needs_routing"]
    async with e.db.sessions.begin() as writer:
        writer.add(DisabledIdentityRow(issuer=LOCAL_ACCOUNT_ISSUER, subject=str(person.id), disabled_at=datetime.now(UTC)))
    after = await reader(e).detail(e.link, e.work["id"])
    assert after.request["needs_routing"] and after.signature != before.signature


@pytest.mark.asyncio
async def test_prepared_detail_borrows_the_writer_and_refuses_source_drift(work_pages):
    from sqlalchemy import event

    e = work_pages
    host = reader(e)
    source = await host.detail(e.link, e.work["id"])
    check = await host.prepare_detail(e.link, source)
    active, maximum = [0], [0]

    def checkout(*args):
        active[0] += 1
        maximum[0] = max(maximum[0], active[0])

    def checkin(*args):
        active[0] -= 1

    engine = e.db.engine.sync_engine
    event.listen(engine, "checkout", checkout)
    event.listen(engine, "checkin", checkin)
    try:
        async with e.db.sessions.begin() as writer:
            await check(session=writer)
            (await writer.get(AgentWorkRow, e.work["id"])).objective = "A later assignment"
            await writer.flush()
            with pytest.raises(AuthorityUnavailable):
                await check(session=writer)
        assert maximum[0] == 1 and active[0] == 0
    finally:
        event.remove(engine, "checkout", checkout)
        event.remove(engine, "checkin", checkin)
