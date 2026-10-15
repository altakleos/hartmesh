"""Lifecycle borrows identity reads while holding a one-connection SQL pool."""

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from sqlalchemy import select
from test_agent_instances import create, definition
from test_agent_instances import instances as instances
from test_agent_lifecycle import lifecycle_fixture

from deerflow.agent_instances.contract import AgentConflict
from deerflow.persistence.spaces.model import SpaceGrantRow


@pytest.fixture(autouse=True)
def one_connection(monkeypatch):
    import _storage_spaces_test_support as support

    original = support.create_async_engine

    def limited(*args, **kwargs):
        return original(*args, **kwargs, pool_size=1, max_overflow=0, pool_timeout=0.2)

    monkeypatch.setattr(support, "create_async_engine", limited)


def install_human_reads(agents, sf, monkeypatch):
    original = agents.directory.human
    borrowed = []

    async def human(reference):
        async with sf() as session:
            await session.execute(select(1))
            return await original(reference)

    async def in_session(reference, session):
        await session.execute(select(1))
        borrowed.append(reference)
        return await original(reference)

    human.in_session = in_session
    monkeypatch.setattr(agents.directory, "human", human)
    monkeypatch.setattr(agents.files.registry._resolver, "_human", human)
    return borrowed


@pytest.mark.asyncio
async def test_lifecycle_sequence_never_checks_out_a_second_connection_for_human_reads(instances, monkeypatch):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    borrowed = install_human_reads(agents, sf, monkeypatch)
    actions = [("suspend", {}), ("restore", {}), ("grant", {"member": BOB, "permissions": 2}), ("grant", {"member": BOB, "permissions": 0}), ("supervise", {"supervisor": BOB}), ("adopt", {"definition": definition(soul="Revised")})]
    for action, fields in actions:
        answer = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action=action, **fields)
        assert answer["complete"]
        agent = answer["instance"]
    assert ALICE in borrowed and BOB in borrowed


@pytest.mark.asyncio
async def test_pending_grant_abandon_and_terminal_replay_borrow_one_connection(instances, monkeypatch):
    agents, _authority, sf, lifecycle, _attachments, _provider, _memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    install_human_reads(agents, sf, monkeypatch)
    key = operation()
    original = lifecycle._grant_ready

    async def stop(session, *args):
        grant = await session.get(SpaceGrantRow, (agent.home_id, "human", "bob"))
        if grant is not None and grant.permissions == 17:
            raise AgentConflict("injected finalize refusal")
        await original(session, *args)

    monkeypatch.setattr(lifecycle, "_grant_ready", stop)
    with pytest.raises(AgentConflict, match="injected"):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="grant", member=BOB, permissions=2)
    for _retry in range(2):
        assert (await lifecycle.abandon(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation + 1, operation_id=key))["abandoned"]
