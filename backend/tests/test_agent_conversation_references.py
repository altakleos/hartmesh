"""Instance reference grants stay within the current instance audience."""

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_conversation_access import _put, _setup

from app.gateway.conversation_access import prepare_conversation_reader
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow


@pytest.mark.asyncio
async def test_instance_references_include_other_requesters_but_exclude_private_and_other_instances(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    destination = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    source = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    other = await create(agents)
    foreign = await authority.create(actor=ALICE, instance_id=other.id, creation_id=operation())
    private = await threads.create(operation(), user_id="alice")
    _, events, _, manager, request = _setup()
    execution = await authority.execution(actor=ALICE, thread_id=destination["thread_id"])
    context = SimpleNamespace(event_store=events, thread_store=threads, agent_execution=execution)
    from deerflow.config.app_config import AppConfig

    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "tools": [{"name": "read_conversation", "group": "conversation", "use": "deerflow.tools.conversation:read_conversation"}]})
    ids = [row["thread_id"] for row in (source, foreign, private)]
    for row in (source, foreign, private):
        await _put(events, "retained source", thread=row["thread_id"])
    reader, _ = prepare_conversation_reader(ids, request=request, user_id="alice", run_context=context, run_manager=manager, app_config=config)
    assert json.loads(await reader(thread_id=ids[0], cursor=None, limit=10))["messages"][0]["text"] == "retained source"
    for thread_id in ids[1:]:
        assert json.loads(await reader(thread_id=thread_id, cursor=None, limit=10))["status"] == "unavailable"
    async with sf() as session, session.begin():
        await session.delete(await session.get(AgentInstanceGrantRow, (agent.id, "alice")))
    assert json.loads(await reader(thread_id=ids[0], cursor=None, limit=10))["status"] == "unavailable"


@pytest.mark.asyncio
async def test_instance_source_deletion_during_read_cannot_disclose_cached_rows(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    destination = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    source = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    _, events, _, manager, request = _setup()
    execution = await authority.execution(actor=ALICE, thread_id=destination["thread_id"])
    from deerflow.config.app_config import AppConfig

    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "tools": [{"name": "read_conversation", "group": "conversation", "use": "deerflow.tools.conversation:read_conversation"}]})
    await _put(events, "must not survive deletion", thread=source["thread_id"])
    original = manager.list_successful_regenerate_sources

    async def delete_while_reading(*args, **kwargs):
        await threads.delete(source["thread_id"], user_id="alice")
        return await original(*args, **kwargs)

    manager.list_successful_regenerate_sources = AsyncMock(side_effect=delete_while_reading)
    context = SimpleNamespace(event_store=events, thread_store=threads, agent_execution=execution)
    reader, _ = prepare_conversation_reader([source["thread_id"]], request=request, user_id="alice", run_context=context, run_manager=manager, app_config=config)
    assert json.loads(await reader(thread_id=source["thread_id"], cursor=None, limit=10))["status"] == "unavailable"
