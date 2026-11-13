"""Owner HTTP capacity reads retain a final proof of displayed foreign facts."""

from types import SimpleNamespace
from uuid import uuid4

import pytest
from _storage_spaces_test_support import operation
from _telegram_repository_test_support import db as db
from test_telegram_work_actions import actions as actions
from test_telegram_work_start import startable as startable

from app.telegram_bots.authority import AuthorityUnavailable
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkRow
from deerflow.persistence.channel_connections.model import ChannelConnectionRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow
from deerflow.persistence.user.model import UserRow


async def foreign_capacity(e):
    from app.gateway.routers.agent_work import router

    e.http_app.include_router(router)
    instance_id, work_id, home_id = operation(), operation(), operation()
    async with e.db.sessions.begin() as writer:
        parent = await writer.get(AgentInstanceRow, e.instance.id)
        home = await writer.get(SpaceRow, e.instance.home_id)
        work = await writer.get(AgentWorkRow, e.work["id"])
        writer.add(SpaceRow(**{**{column.name: getattr(home, column.name) for column in SpaceRow.__table__.columns}, "id": home_id, "backing_handle": operation()}))
        await writer.flush()
        writer.add(
            AgentInstanceRow(
                **{
                    **{column.name: getattr(parent, column.name) for column in AgentInstanceRow.__table__.columns},
                    "id": instance_id,
                    "home_id": home_id,
                    "principal_id": "agent:" + instance_id,
                    "creation_id": operation(),
                    "name": "Other reviewer",
                }
            )
        )
        await writer.flush()
        writer.add(AgentWorkRow(**{**{column.name: getattr(work, column.name) for column in AgentWorkRow.__table__.columns}, "id": work_id, "instance_id": instance_id, "objective": "Review the other guide"}))
        writer.add(AgentInstanceGrantRow(instance_id=instance_id, user_id=e.actor.subject_id, permissions=2))
    e.human.agents.residents = SimpleNamespace(work_max=lambda: 1, attempts_in_flight=lambda: [dict(instance_id=instance_id, work_id=work_id, run_id="visible-capacity-run", requester_id=e.actor.subject_id, started_at=None)])
    from app.telegram_bots.work_capacity_reads import WorkCapacityReader

    return WorkCapacityReader(work=e.human.work, authority=e.live.authority, links=e.db.links, gateway=e.http_client), instance_id, work_id


@pytest.mark.asyncio
async def test_capacity_reads_the_real_owner_route_and_keeps_partial_task_facts(startable):
    e = startable
    reader, instance_id, work_id = await foreign_capacity(e)
    source = await reader.read(e.link)
    assert source.capacity.maximum == source.capacity.in_use == 1
    assert source.capacity.running[0].work_id == work_id and source.capacity.running[0].instance_id == instance_id
    assert source.capacity.running[0].started_at is None and source.person
    assert e.observed[-1][:3] == ("GET", f"/api/agent-instances/{e.instance.id}/work", e.link.owner_user_id)
    prepared = await reader.prepare(e.link, source)
    async with e.acts.outbox._writer(e.link.id) as (writer, _binding):
        await prepared(session=writer)
    assert not e.launches


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["grant", "objective", "name"])
async def test_capacity_foreign_facts_are_rechecked_after_actual_provider_wait(startable, monkeypatch, change):
    from app.gateway import authz
    from app.telegram_bots.authority import AuthorityUnavailable
    from app.telegram_bots.producer import ProductSourceRetired

    e = startable
    reader, instance_id, work_id = await foreign_capacity(e)
    real_http, real_permissions = e.http_client.get_work_capacity, authz.resolve_route_permissions
    read, changed = [], []

    async def http(*args, **kwargs):
        result = await real_http(*args, **kwargs)
        read.append(True)
        return result

    async def permissions(*args, **kwargs):
        result = await real_permissions(*args, **kwargs)
        if read and not changed:
            changed.append(True)
            async with e.db.sessions.begin() as writer:
                if change == "grant":
                    (await writer.get(AgentInstanceGrantRow, (instance_id, e.actor.subject_id))).permissions = 1
                elif change == "objective":
                    (await writer.get(AgentWorkRow, work_id)).objective = "Changed after HTTP"
                else:
                    (await writer.get(AgentInstanceRow, instance_id)).name = "Changed after HTTP"
        return result

    monkeypatch.setattr(e.http_client, "get_work_capacity", http)
    monkeypatch.setattr(authz, "resolve_route_permissions", permissions)
    with pytest.raises((AuthorityUnavailable, ProductSourceRetired)):
        await reader.read(e.link)
    assert changed and not e.launches


@pytest.mark.asyncio
async def test_prepared_capacity_rejects_changed_current_person_in_borrowed_writer(startable):
    e = startable
    reader, _, _ = await foreign_capacity(e)
    source = await reader.read(e.link)
    prepared = await reader.prepare(e.link, source)
    async with e.db.sessions.begin() as writer:
        person = await writer.get(UserRow, e.link.owner_user_id)
        person.email = "changed-person@example.com"
        with pytest.raises(AuthorityUnavailable):
            await prepared(session=writer)


@pytest.mark.asyncio
async def test_owner_filtered_snapshot_cannot_be_reused_as_another_owner(startable):
    e = startable
    reader, shared_instance, shared_work = await foreign_capacity(e)
    _, private_instance, private_work = await foreign_capacity(e)
    e.human.agents.residents = SimpleNamespace(
        work_max=lambda: 1,
        attempts_in_flight=lambda: [
            dict(instance_id=shared_instance, work_id=shared_work, run_id="shared-run", requester_id=e.actor.subject_id, started_at=None),
            dict(instance_id=private_instance, work_id=private_work, run_id="private-run", requester_id=e.actor.subject_id, started_at=None),
        ],
    )
    source = await reader.read(e.link)
    assert len(source.capacity.running) == 2
    other = str(uuid4())
    async with e.db.sessions.begin() as writer:
        writer.add(UserRow(id=other, email="other-capacity-owner@example.com", password_hash="unusable-fixture", system_role="user"))
        await writer.flush()
        writer.add(AgentInstanceGrantRow(instance_id=e.instance.id, user_id=other, permissions=3))
        writer.add(AgentInstanceGrantRow(instance_id=shared_instance, user_id=other, permissions=2))
        (await writer.get(TelegramBotLinkRow, e.link.id)).owner_user_id = other
        (await writer.get(ChannelConnectionRow, e.link.connection_id)).owner_user_id = other
    current = await e.db.links.get(e.link.id)
    actual = await reader.read(current)
    assert len(actual.capacity.running) == 1 and actual.capacity.running[0].work_id == shared_work
    assert source.capacity.running[1].work_id == private_work
    with pytest.raises(AuthorityUnavailable):
        await reader.prepare(current, source)


@pytest.mark.asyncio
async def test_capacity_prepared_snapshot_rejects_current_conflict_epoch(startable):
    e = startable
    reader, _, _ = await foreign_capacity(e)
    source = await reader.read(e.link)
    check = await reader.prepare(e.link, source)
    async with e.db.sessions.begin() as writer:
        (await writer.get(TelegramBotLinkRow, e.link.id)).connect_conflict_epoch += 1
        with pytest.raises(AuthorityUnavailable):
            await check(session=writer)
