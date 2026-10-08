"""Native writable destinations must be compatible with protected context."""

import asyncio
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_instance_memory import backend, setup_memory
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.agent_instances.contract import AgentDenied, AgentPermission
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.agent_instances.memory import execution_memory_enabled
from deerflow.agent_instances.middleware import InstanceAuthorityMiddleware
from deerflow.config.memory_config import MemoryConfig
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.spaces.contract import Permission


async def grant_home_read(agents, instance, subject):
    home = await agents.files.registry.get(actor=ALICE, space_id=instance.home_id)
    return await agents.files.registry.set_grant(actor=ALICE, space_id=home.id, expected_generation=home.generation, subject=subject, permissions=Permission.READ, acknowledge_existing_data=True)


@pytest.mark.asyncio
async def test_generic_home_grant_does_not_authorize_protected_conversation_context(instances):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    instance = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=instance.id, creation_id=operation())
    await grant_home_read(agents, instance, BOB)
    with pytest.raises(AgentDenied, match="audience"):
        await authority.execution(actor=ALICE, thread_id=chat["thread_id"])


@pytest.mark.asyncio
async def test_fresh_original_use_requester_has_no_instance_memory(instances):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    instance = await create(agents)
    async with sf() as session, session.begin():
        session.add(AgentInstanceGrantRow(instance_id=instance.id, user_id="bob", permissions=1))
    await grant_home_read(agents, instance, BOB)
    chat = await authority.create(actor=BOB, instance_id=instance.id, creation_id=operation())
    execution = await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    assert execution.memory_audience_allowed is False
    assert not execution_memory_enabled(execution, MemoryConfig())


@pytest.mark.asyncio
async def test_memory_bearing_context_cannot_be_downgraded_on_checkpoint_reuse(instances, tmp_path):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    instance = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB, instance_id=instance.id, creation_id=operation())
    execution = await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    manager = backend(tmp_path).with_storage(await memory.bind(actor=BOB, instance_id=instance.id, execution=execution))
    await asyncio.to_thread(manager.get_context, "bob", agent_name="analyst")
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 1
    with pytest.raises(AgentDenied):
        await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    assert await threads.get(chat["thread_id"], user_id="bob") is None
    assert await threads.search(user_id="bob") == []
    assert not await authority.allowed(actor=BOB, thread_id=chat["thread_id"], permission=AgentPermission.INSPECT)
    with pytest.raises(AgentDenied):
        await authority.validate(execution)
    runtime = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: execution})
    with pytest.raises(AgentDenied):
        await InstanceAuthorityMiddleware().abefore_model({}, runtime)
    fresh = await authority.create(actor=BOB, instance_id=instance.id, creation_id=operation())
    assert (await authority.execution(actor=BOB, thread_id=fresh["thread_id"])).memory_audience_allowed is False


@pytest.mark.asyncio
async def test_inspector_running_use_only_requesters_chat_cannot_enable_memory(instances):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    instance = await create(agents)
    async with sf() as session, session.begin():
        session.add(AgentInstanceGrantRow(instance_id=instance.id, user_id="bob", permissions=1))
    chat = await authority.create(actor=BOB, instance_id=instance.id, creation_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    assert execution.memory_audience_allowed is False
    assert not execution_memory_enabled(execution, MemoryConfig())
