"""Deletion commits agent and Home tombstones together before reclamation."""

import httpx
import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from sqlalchemy import select
from test_agent_conversation_http import app_for
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_agent_lifecycle import lifecycle_fixture

from deerflow.agent_instances.contract import AgentConflict
from deerflow.persistence.agent_instances.model import AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository
from deerflow.spaces.recovery import SpaceRecovery


@pytest.mark.asyncio
@pytest.mark.parametrize("custody", ["personal", "company"])
async def test_finalize_marks_the_agent_and_its_space_deleted_in_one_transaction(instances, custody):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents, custody=custody, supervisor=BOB if custody == "company" else ALICE)
    home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
    key = operation()
    answer = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="delete")
    assert answer["complete"] and answer["instance"].status == "deleted"
    async with sf() as session:
        instance = await session.get(AgentInstanceRow, agent.id)
        space = await session.get(SpaceRow, home.id)
        intent = await session.get(AgentLifecycleRow, (agent.id, key))
        events = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == home.id, SpaceEventRow.action == "delete"))).all()
        assert instance.status == space.status == "deleted"
        assert space.generation > home.generation and intent.home_generation == space.generation
        assert len(events) == 1 and events[0].generation == space.generation
        assert events[0].details["retained_backups"] is False and events[0].details["cause"] == "agent-deleted"
        assert await session.get(SpaceBackingRow, home.backing_handle) is not None
    replay = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="delete")
    assert replay["complete"] and replay["instance"].status == "deleted"


@pytest.mark.asyncio
async def test_a_failure_in_finalize_leaves_neither_agent_nor_space_deleted(instances, monkeypatch):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    key = operation()
    advance = SpaceRecovery._advance

    def fail_after_space_change(session, row, actor, action, details):
        advance(session, row, actor, action, details)
        assert row.status == "deleted"
        raise RuntimeError("failure after the Home changed")

    monkeypatch.setattr(SpaceRecovery, "_advance", staticmethod(fail_after_space_change))
    with pytest.raises(RuntimeError, match="after the Home"):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="delete")
    async with sf() as session:
        assert (await session.get(AgentInstanceRow, agent.id)).status == "suspended"
        assert (await session.get(SpaceRow, agent.home_id)).status == "active"
        assert (await session.get(AgentLifecycleRow, (agent.id, key))).phase == "pending"
        assert await session.scalar(select(SpaceEventRow.generation).where(SpaceEventRow.space_id == agent.home_id, SpaceEventRow.action == "delete")) is None
    monkeypatch.setattr(SpaceRecovery, "_advance", staticmethod(advance))
    answer = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="delete")
    assert answer["complete"] and answer["instance"].status == "deleted"


@pytest.mark.asyncio
async def test_delete_refuses_exhausted_space_generation_before_finalize_mutation(instances):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    async with sf() as session, session.begin():
        (await session.get(SpaceRow, agent.home_id)).generation = 2**31 - 1
    with pytest.raises(AgentConflict, match="generation"):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="delete")
    async with sf() as session:
        assert (await session.get(AgentInstanceRow, agent.id)).status != "deleted"
        home = await session.get(SpaceRow, agent.home_id)
        assert home.status == "active" and home.generation == 2**31 - 1


@pytest.mark.asyncio
async def test_restore_of_a_deleted_agent_is_hidden_and_writes_no_new_intent(instances, monkeypatch):
    agents, authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    removed = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="delete")
    app = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=ALICE)
    key = operation()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.post(f"/api/agent-instances/{agent.id}/lifecycle", json={"generation": removed["instance"].generation, "operation_id": key, "action": "restore"})
        assert response.status_code == 404 and response.json() == {"detail": "Resource or file is unavailable"}, response.text
    async with sf() as session:
        assert await session.get(AgentLifecycleRow, (agent.id, key)) is None
        assert (await session.get(AgentInstanceRow, agent.id)).status == (await session.get(SpaceRow, agent.home_id)).status == "deleted"


@pytest.mark.asyncio
async def test_historical_completed_delete_never_retroactively_deletes_an_active_home(instances):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
    key = operation()
    async with sf() as session, session.begin():
        row = await session.get(AgentInstanceRow, agent.id)
        row.status, row.generation = "deleted", row.generation + 1
        session.add(
            AgentLifecycleRow(
                instance_id=agent.id,
                operation_id=key,
                actor_id=ALICE.subject_id,
                generation=agent.generation,
                home_id=home.id,
                home_generation=home.generation,
                request={"action": "delete"},
                attachment_ids=[],
                phase="complete",
                prior_status="active",
                resolved_by=ALICE.subject_id,
            )
        )
    answer = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="delete")
    assert answer["complete"] and answer["instance"].status == "deleted"
    async with sf() as session:
        row = await session.get(SpaceRow, home.id)
        assert row.status == "active" and row.generation == home.generation
        assert await session.scalar(select(SpaceEventRow.generation).where(SpaceEventRow.space_id == home.id, SpaceEventRow.action == "delete")) is None
