"""Lifecycle membership maps file audience without widening chat-only access."""

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_instances import create, definition
from test_agent_instances import instances as instances
from test_agent_lifecycle import lifecycle_fixture
from test_compose_owned_space_routes import metadata

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, Refusal
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.contract import Permission, SpaceConflict, SpaceDenied


@pytest.mark.asyncio
@pytest.mark.parametrize("permissions", [4, 5])
async def test_self_demotion_cannot_remove_the_file_authority_needed_to_finish(instances, permissions):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    agent = (await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="grant", member=BOB, permissions=7))["instance"]
    home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
    before = await metadata(sf, home)
    key = operation()
    with pytest.raises((AgentDenied, SpaceDenied)) as caught:
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="grant", member=ALICE, permissions=permissions)
    assert await metadata(sf, home) == before
    assert isinstance(caught.value, AgentDenied)
    async with sf() as session:
        assert await session.get(AgentLifecycleRow, (agent.id, key)) is None
        assert (await session.get(AgentInstanceRow, agent.id)).status == "active"


@pytest.mark.asyncio
async def test_equal_mapped_file_access_does_not_write_another_grant(instances, monkeypatch):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    agent = (await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="grant", member=BOB, permissions=3))["instance"]
    home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
    before = await metadata(sf, home)

    async def forbidden(**_kwargs):
        raise AssertionError("The mapped file access already matches")

    monkeypatch.setattr(agents.files.registry, "set_grant", forbidden)
    assert (await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="grant", member=BOB, permissions=2))["complete"]
    assert await metadata(sf, home) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("permissions", range(8))
async def test_each_membership_maps_the_space_grant_before_finalizing(instances, permissions, monkeypatch):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    expected = [0, 0, 17, 17, 0, 0, 27, 27][permissions]
    original = lifecycle._grant_ready
    seen = []

    async def check(session, *args):
        row = await session.get(SpaceGrantRow, (agent.home_id, "human", "bob"))
        seen.append(row.permissions if row else 0)
        assert await session.get(AgentInstanceGrantRow, (agent.id, "bob")) is None
        await original(session, *args)

    monkeypatch.setattr(lifecycle, "_grant_ready", check)
    result = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="grant", member=BOB, permissions=permissions)
    assert result["complete"] and len(seen) >= 2 and seen[-1] == expected
    async with sf() as session:
        grant = await session.get(SpaceGrantRow, (agent.home_id, "human", "bob"))
        assert (grant.permissions if grant else 0) == expected
        resident = await session.get(SpaceGrantRow, (agent.home_id, agent.principal.kind, agent.principal.subject_id))
        assert resident.permissions == 19


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["grant", "supervise", "adopt"])
async def test_only_if_idle_checks_real_run_ids_before_withdrawing_any_state(instances, action):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    key = operation()
    fields = {"grant": {"member": BOB, "permissions": 1}, "supervise": {"supervisor": BOB}, "adopt": {"definition": definition(soul="Revised")}}[action]
    with pytest.raises(Refusal) as caught:
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action=action, only_if_idle=True, runs_in_flight=lambda: ["actual-run"], **fields)
    assert caught.value.reason == "work-in-flight" and caught.value.extra["run_ids"] == ["actual-run"]
    async with sf() as session:
        assert await session.get(AgentLifecycleRow, (agent.id, key)) is None
        assert (await session.get(AgentInstanceRow, agent.id)).status == "active"
    result = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action=action, only_if_idle=True, runs_in_flight=lambda: [], **fields)
    assert result["complete"]
    async with sf() as session:
        request = (await session.get(AgentLifecycleRow, (agent.id, key))).request
        assert "only_if_idle" not in request and "runs_in_flight" not in request
    assert (await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action=action, **fields))["complete"]


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["normal", "stale", "reset-failure", "drift", "retired"])
async def test_abandon_compensates_a_space_grant_written_before_finalize(instances, monkeypatch, outcome):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    if outcome == "retired":
        agent = (await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="grant", member=BOB, permissions=2))["instance"]
    key = operation()
    original = lifecycle._grant_ready

    async def stop(session, *args):
        grant = await session.get(SpaceGrantRow, (agent.home_id, "human", "bob"))
        if grant is not None and grant.permissions == (27 if outcome == "retired" else 17):
            raise AgentConflict("injected finalize refusal")
        await original(session, *args)

    monkeypatch.setattr(lifecycle, "_grant_ready", stop)
    with pytest.raises(AgentConflict, match="injected"):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="grant", member=BOB, permissions=6 if outcome == "retired" else 2)
    async with sf() as session:
        assert (await session.get(AgentLifecycleRow, (agent.id, key))).phase == "pending"
        assert (await session.get(SpaceGrantRow, (agent.home_id, "human", "bob"))).permissions == (27 if outcome == "retired" else 17)
    home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
    before = await metadata(sf, home)
    if outcome == "stale":
        with pytest.raises(AgentConflict):
            await lifecycle.abandon(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key)
        assert await metadata(sf, home) == before
        return
    if outcome in {"reset-failure", "drift"}:
        set_grant = agents.files.registry.set_grant

        async def reset(**kwargs):
            if outcome == "reset-failure":
                raise SpaceConflict("reset failed")
            value = await set_grant(**kwargs)
            await set_grant(actor=ALICE, space_id=home.id, expected_generation=value.generation, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
            return value

        monkeypatch.setattr(agents.files.registry, "set_grant", reset)
        with pytest.raises(SpaceConflict if outcome == "reset-failure" else AgentConflict) as caught:
            await lifecycle.abandon(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation + 1, operation_id=key)
        if outcome == "drift":
            assert str(caught.value) == "The member's grant on the files changed; abandon the operation again"
        async with sf() as session:
            assert (await session.get(AgentLifecycleRow, (agent.id, key))).phase == "pending"
        return
    if outcome == "retired":
        lookup = agents.directory.human

        async def departed(reference):
            return None if reference == BOB else await lookup(reference)

        monkeypatch.setattr(agents.directory, "human", departed)
        monkeypatch.setattr(agents.files.registry._resolver, "_human", departed)
    result = await lifecycle.abandon(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation + 1, operation_id=key)
    assert result["abandoned"] and result["instance"].status == "suspended"
    async with sf() as session:
        assert await session.get(SpaceGrantRow, (agent.home_id, "human", "bob")) is None
        member = await session.get(AgentInstanceGrantRow, (agent.id, "bob"))
        assert (member.permissions if member else 0) == (2 if outcome == "retired" else 0)
