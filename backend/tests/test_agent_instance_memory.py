"""Instance facts and summaries use actual SQL authority, never fake users."""

import asyncio
import json
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_conversations import conversations
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.agent_instances.contract import AgentDenied, AgentPermission
from deerflow.agent_instances.memory import InstanceMemory
from deerflow.agents.memory.backends.deermem.deer_mem import DeerMem
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentInstanceRow, AgentMemoryRow


@pytest.mark.asyncio
async def test_actual_memory_tools_ignore_forged_requester_scope_and_use_only_is_disabled(instances, tmp_path, monkeypatch):
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
    from deerflow.agent_instances.memory import execution_memory_enabled
    from deerflow.agents.memory import manager as manager_module
    from deerflow.agents.memory.tools import memory_add_tool, memory_search_tool
    from deerflow.config.memory_config import MemoryConfig

    agents, authority, threads, sf, memory = await setup_memory(instances)
    agents.memory_service = memory
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    execution = await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    base = backend(tmp_path)
    monkeypatch.setattr(manager_module, "_get_host_memory_manager", lambda: base)
    legacy_before = {str(path): path.read_bytes() for path in (tmp_path / "private").rglob("*") if path.is_file()}
    runtime = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: execution, "user_id": "mallory", "agent_name": "private"})
    result = json.loads(await asyncio.to_thread(memory_add_tool.func, runtime, "Shared fact"))
    assert result["status"] == "added", result
    assert json.loads(await asyncio.to_thread(memory_search_tool.func, runtime, "Shared"))["count"] == 1
    assert {str(path): path.read_bytes() for path in (tmp_path / "private").rglob("*") if path.is_file()} == legacy_before
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = 1
    current = replace(execution, instance=await agents.get(actor=BOB, instance_id=agent.id, permission=AgentPermission.USE))
    assert not execution_memory_enabled(current, MemoryConfig())
    denied = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: current})
    assert "error" in json.loads(await asyncio.to_thread(memory_add_tool.func, denied, "Forbidden"))
    assert not execution_memory_enabled(execution, MemoryConfig(enabled=False))
    assert not execution_memory_enabled(execution, MemoryConfig(manager_class="noop"))


@pytest.mark.asyncio
async def test_in_flight_extraction_holds_no_sql_lock_and_cannot_resurrect_cleared_memory(instances, tmp_path):
    from langchain_core.messages import AIMessage, HumanMessage

    agents, authority, threads, sf, memory = await setup_memory(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    scoped = backend(tmp_path).with_storage(await memory.bind(actor=ALICE, instance_id=agent.id, execution=execution))
    entered, release = threading.Event(), threading.Event()

    class Model:
        def invoke(self, *args, **kwargs):
            entered.set()
            assert release.wait(10)
            return SimpleNamespace(
                content=json.dumps(
                    {"user": {}, "history": {}, "factsToRemove": [], "newFacts": [{"content": "Old queued fact", "confidence": 0.9, "scope": "user", "durability": "durable", "authority": "descriptive", "category": "context"}]}
                )
            )

    scoped._updater._llm = Model()
    work = asyncio.create_task(
        asyncio.to_thread(
            scoped._updater.update_memory,
            [HumanMessage(content="Remember that our company uses blue labels."), AIMessage(content="I will remember the blue label convention.")],
            thread_id=chat["thread_id"],
            agent_name="analyst",
            user_id="alice",
        )
    )
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        clearing = backend(tmp_path).with_storage(await memory.bind(actor=ALICE, instance_id=agent.id, management_write=True))
        await asyncio.wait_for(asyncio.to_thread(clearing.clear_memory, agent_name="analyst", user_id="alice"), 5)
    finally:
        release.set()
        assert await work is False
    fresh = backend(tmp_path).with_storage(await memory.bind(actor=ALICE, instance_id=agent.id))
    assert (await asyncio.to_thread(fresh.get_memory, agent_name="analyst", user_id="alice"))["facts"] == []


@pytest.mark.asyncio
async def test_http_memory_management_and_provider_ceiling(instances, tmp_path, monkeypatch):
    import httpx
    from test_agent_conversation_http import app_for

    from app.gateway.routers import agent_instances as routes

    agents, authority, threads, sf, memory = await setup_memory(instances)
    agents.memory_service = memory
    agent = await create(agents, custody="company", supervisor=BOB)
    monkeypatch.setattr(routes, "_get_host_memory_manager", lambda: backend(tmp_path), raising=False)
    app = app_for(agents, authority, threads, monkeypatch, permissions=["agents:read", "agents:write", "memory:read", "memory:write"])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        endpoint = f"/api/agent-instances/{agent.id}/memory"
        first = await client.get(endpoint)
        assert first.status_code == 200 and first.json()["facts"] == [], first.text
        response = await client.post(endpoint + "/facts", json={"content": "HTTP instance fact", "confidence": 0.8})
        assert response.status_code == 201, response.text
        exported = await client.get(endpoint + "/export")
        assert exported.status_code == 200 and exported.json()["facts"][0]["content"] == "HTTP instance fact"
        assert (await client.delete(endpoint)).status_code == 200
    limited = app_for(agents, authority, threads, monkeypatch, permissions=["agents:read"])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=limited), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        assert (await client.get(endpoint)).status_code == 403


@pytest.mark.asyncio
async def test_instance_prompt_and_model_dispatch_recheck_whole_memory_audience(instances, tmp_path, monkeypatch):
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
    from deerflow.agent_instances.middleware import InstanceAuthorityMiddleware
    from deerflow.agents.lead_agent.prompt import _get_memory_context
    from deerflow.agents.memory import manager as manager_module
    from deerflow.agents.memory.manager import memory_execution_scope
    from deerflow.config.app_config import AppConfig

    agents, authority, threads, sf, memory = await setup_memory(instances)
    agents.memory_service = memory
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    execution = await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    base = backend(tmp_path)
    manual = base.with_storage(await memory.bind(actor=BOB, instance_id=agent.id, management_write=True))
    await asyncio.to_thread(manual.create_fact, "Instance-only injection", agent_name="analyst", user_id="bob")
    monkeypatch.setattr(manager_module, "_get_host_memory_manager", lambda: base)
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}})
    with memory_execution_scope(execution):
        content = await asyncio.to_thread(_get_memory_context, "forged-private-name", app_config=config, user_id="forged-human")
        assert "Instance-only injection" in content
    middleware = InstanceAuthorityMiddleware(require_memory_audience=True)
    runtime = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: execution})
    await middleware.abefore_model({}, runtime)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = 1
    with pytest.raises(AgentDenied):
        await middleware.abefore_model({}, runtime)


async def setup_memory(instances):
    agents, authority, threads, sf = await conversations(instances)
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(lambda c: AgentMemoryRow.__table__.create(c))
    return agents, authority, threads, sf, InstanceMemory(agents)


def backend(tmp_path):
    return DeerMem(backend_config={"storage_path": str(tmp_path / "private"), "token_counting": "char", "debounce_seconds": 300})


@pytest.mark.asyncio
async def test_distinct_instances_share_neither_facts_nor_summaries_and_new_chats_retain_them(instances, tmp_path):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    one = await create(agents, custody="company", supervisor=BOB)
    two = await create(agents, custody="company", supervisor=BOB)
    base = backend(tmp_path)
    private = base._storage.load(user_id="alice")
    private["user"]["workContext"]["summary"] = "Creator's personal secret"
    base._storage.save(private, user_id="alice")
    scoped = base.with_storage(await memory.bind(actor=ALICE, instance_id=one.id, management_write=True))
    assert "personal secret" not in await asyncio.to_thread(scoped.get_context, "alice", agent_name="analyst")
    document, fact = await asyncio.to_thread(scoped.create_fact, "Company preference", agent_name="analyst", user_id="alice")
    document["user"]["workContext"]["summary"] = "Company summary"
    await asyncio.to_thread(scoped.import_memory, document, agent_name="analyst", user_id="alice")
    for actor in (ALICE, BOB):
        chat = await authority.create(actor=actor, instance_id=one.id, creation_id=operation())
        execution = await authority.execution(actor=actor, thread_id=chat["thread_id"])
        retained = base.with_storage(await memory.bind(actor=actor, instance_id=one.id, execution=execution))
        content = await asyncio.to_thread(retained.get_context, actor.subject_id, agent_name="analyst")
        assert "Company summary" in content and "Company preference" in content and "personal secret" not in content
    empty = base.with_storage(await memory.bind(actor=BOB, instance_id=two.id))
    assert (await asyncio.to_thread(empty.get_memory, agent_name="analyst", user_id="bob"))["facts"] == []
    assert "Company summary" not in await asyncio.to_thread(empty.get_context, "bob", agent_name="analyst")
    assert "personal secret" in base._storage.load(user_id="alice")["user"]["workContext"]["summary"]


@pytest.mark.asyncio
async def test_clear_and_import_fence_queued_epoch_without_refresh_on_reload(instances, tmp_path):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    storage = await memory.bind(actor=ALICE, instance_id=agent.id, execution=execution)
    base = backend(tmp_path)
    delayed = base.with_storage(storage)
    manager = base.with_storage(await memory.bind(actor=ALICE, instance_id=agent.id, management_write=True))
    await asyncio.to_thread(manager.clear_memory, agent_name="analyst", user_id="alice")
    await asyncio.to_thread(storage.reload, "analyst", user_id="alice")
    with pytest.raises(AgentDenied):
        await asyncio.to_thread(delayed.create_fact, "Late extraction", agent_name="analyst", user_id="alice")
    fresh = base.with_storage(await memory.bind(actor=ALICE, instance_id=agent.id, execution=execution))
    assert (await asyncio.to_thread(fresh.get_memory, agent_name="analyst", user_id="alice"))["facts"] == []
    imported = base.with_storage(await memory.bind(actor=ALICE, instance_id=agent.id, management_write=True))
    await asyncio.to_thread(imported.import_memory, {"facts": [], "history": {"recentMonths": {"summary": "Replacement"}}}, agent_name="analyst", user_id="alice")
    with pytest.raises(AgentDenied):
        await asyncio.to_thread(fresh.create_fact, "Another late update", agent_name="analyst", user_id="alice")


@pytest.mark.asyncio
async def test_current_grants_generation_and_deleted_conversation_fence_memory(instances, tmp_path):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    execution = await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    scoped = backend(tmp_path).with_storage(await memory.bind(actor=BOB, instance_id=agent.id, execution=execution))
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = int(AgentPermission.USE)
    with pytest.raises(AgentDenied):
        await asyncio.to_thread(scoped.get_memory, agent_name="analyst", user_id="bob")
    with pytest.raises(AgentDenied):
        await memory.bind(actor=BOB, instance_id=agent.id, execution=execution)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = 7
        row = await session.get(AgentInstanceRow, agent.id)
        row.status = "deleted"
        row.generation += 1
    with pytest.raises(AgentDenied):
        await asyncio.to_thread(scoped.create_fact, "Deleted", agent_name="analyst", user_id="bob")
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, agent.id)).status = "active"
    with pytest.raises(AgentDenied):
        await asyncio.to_thread(scoped.create_fact, "Restored but stale", agent_name="analyst", user_id="bob")
    refreshed = replace(execution, instance=await agents.get(actor=BOB, instance_id=agent.id))
    current = backend(tmp_path).with_storage(await memory.bind(actor=BOB, instance_id=agent.id, execution=refreshed))
    await threads.delete(chat["thread_id"], user_id="bob")
    with pytest.raises(AgentDenied):
        await asyncio.to_thread(current.create_fact, "Chat deleted", agent_name="analyst", user_id="bob")


@pytest.mark.asyncio
async def test_passive_capture_and_compaction_flush_use_instance_storage(instances, tmp_path, monkeypatch):
    from langchain_core.messages import AIMessage, HumanMessage

    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
    from deerflow.agents.memory import manager as manager_module
    from deerflow.agents.memory.summarization_hook import memory_flush_hook
    from deerflow.agents.middlewares.memory_middleware import MemoryMiddleware
    from deerflow.agents.middlewares.summarization_middleware import SummarizationEvent

    agents, authority, threads, sf, memory = await setup_memory(instances)
    agents.memory_service = memory
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    base = backend(tmp_path)
    base._llm = SimpleNamespace(
        invoke=lambda *a, **kw: SimpleNamespace(
            content=json.dumps(
                {"user": {}, "history": {}, "factsToRemove": [], "newFacts": [{"content": "Passive shared fact", "category": "context", "confidence": 0.9, "scope": "user", "durability": "durable", "authority": "descriptive"}]}
            )
        )
    )
    monkeypatch.setattr(manager_module, "_get_host_memory_manager", lambda: base)
    runtime = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: execution, "thread_id": chat["thread_id"], "user_id": "alice"})
    messages = [HumanMessage(content="Remember our team's blue labels."), AIMessage(content="I will retain the blue label convention.")]
    middleware = MemoryMiddleware(agent_name="analyst")
    await middleware.aafter_agent({"messages": messages}, runtime)
    scoped = await asyncio.to_thread(memory.manager, execution, base)
    assert scoped._queue.pending_count == 1
    assert await asyncio.to_thread(scoped.shutdown_flush, 5)
    document = await asyncio.to_thread(scoped.get_memory, agent_name="analyst", user_id="alice")
    assert [f["content"] for f in document["facts"]] == ["Passive shared fact"]
    event = SummarizationEvent(thread_id=chat["thread_id"], agent_name="analyst", runtime=runtime, messages_to_summarize=tuple(messages), preserved_messages=())
    await asyncio.to_thread(memory_flush_hook, event)
    assert await asyncio.to_thread(scoped.shutdown_flush, 5)
    assert base._storage.load("analyst", user_id="alice")["facts"] == []


@pytest.mark.asyncio
async def test_concurrent_duplicate_fact_and_company_creator_departure(instances, tmp_path, monkeypatch):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    base = backend(tmp_path)
    one = base.with_storage(await memory.bind(actor=BOB, instance_id=agent.id, management_write=True))
    two = base.with_storage(await memory.bind(actor=ALICE, instance_id=agent.id, management_write=True))
    results = await asyncio.gather(asyncio.to_thread(one.create_fact, "same fact", agent_name="analyst", user_id="bob"), asyncio.to_thread(two.create_fact, "same fact", agent_name="analyst", user_id="alice"), return_exceptions=True)
    assert sum(isinstance(r, tuple) for r in results) == 1
    assert any(isinstance(r, ValueError) and "Duplicate" in str(r) for r in results)
    human = agents.directory.human

    async def remaining(actor):
        return None if actor == ALICE else await human(actor)

    agents.directory.human = remaining
    agents.files.registry._resolver._human = remaining
    retained = base.with_storage(await memory.bind(actor=BOB, instance_id=agent.id))
    assert len((await asyncio.to_thread(retained.get_memory, agent_name="analyst", user_id="bob"))["facts"]) == 1


@pytest.mark.asyncio
async def test_memory_document_bounds_and_sync_host_loop_refuse_without_writes(instances, tmp_path):

    agents, authority, threads, sf, memory = await setup_memory(instances)
    agent = await create(agents)
    storage = await memory.bind(actor=ALICE, instance_id=agent.id, management_write=True)
    manager = backend(tmp_path).with_storage(storage)
    with pytest.raises(AgentDenied, match="worker thread"):
        manager.get_memory(agent_name="analyst", user_id="alice")
    with pytest.raises(ValueError, match="oversized"):
        await asyncio.to_thread(manager.create_fact, "x" * 65537, agent_name="analyst", user_id="alice")
    assert (await asyncio.to_thread(manager.get_memory, agent_name="analyst", user_id="alice"))["facts"] == []


@pytest.mark.asyncio
async def test_manager_construction_does_not_hold_mutex_while_waiting_for_host_loop(tmp_path, monkeypatch):
    from deerflow.agent_instances.memory import SqlMemoryStorage

    memory = InstanceMemory(None)
    loop = asyncio.get_running_loop()
    execution = SimpleNamespace(owner_loop=loop, requester=ALICE, instance=SimpleNamespace(id="a" * 32))
    entered, release = asyncio.Event(), asyncio.Event()

    async def blocked_bind(**kwargs):
        entered.set()
        await release.wait()
        return SqlMemoryStorage(memory, None)

    monkeypatch.setattr(memory, "bind", blocked_bind)
    construction = asyncio.create_task(asyncio.to_thread(memory.manager, execution, backend(tmp_path)))
    finishing = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        finishing = asyncio.create_task(asyncio.to_thread(memory.finish, execution))
        # This executes off-loop so the old deadlock fails without hanging pytest.
        # Production finish runs on the host loop; acquiring the same mutex there
        # would prevent the pending bridge from ever receiving its bind result.
        await asyncio.wait_for(asyncio.shield(finishing), 3)
        memory.finish(execution)
        await asyncio.sleep(0)
    finally:
        release.set()
        await construction
        if finishing is not None:
            await finishing


@pytest.mark.parametrize("capability", ["enabled", "use-only", "disabled", "unsupported"])
def test_real_lead_tool_mode_assembly_exposes_only_qualified_instance_memory(capability, monkeypatch):
    from test_agent_assembly_descriptor import TestLeadAgentAssembly
    from test_agent_execution_runtime import execution

    from deerflow.agent_instances.contract import AgentInstance
    from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
    from deerflow.agents.lead_agent import agent as lead
    from deerflow.config.memory_config import MemoryConfig

    config = TestLeadAgentAssembly._isolate_from_the_ambient_config(monkeypatch)
    config.memory = MemoryConfig(enabled=capability != "disabled", mode="tool", injection_enabled=False, manager_class="noop" if capability == "unsupported" else "deermem")
    bound = execution()
    identity = AgentInstance(**vars(bound.instance), permissions=AgentPermission.USE if capability == "use-only" else AgentPermission(7))
    bound = replace(bound, instance=identity)
    assembly = lead.assemble_lead_agent({"context": {AGENT_EXECUTION_CONTEXT_KEY: bound}, "configurable": {"thread_id": "chat"}}, app_config=config)
    names = {tool.name for tool in assembly.graph["tools"]}
    memory_tools = {"memory_search", "memory_add", "memory_update", "memory_delete"}
    assert (names & memory_tools) == (memory_tools if capability == "enabled" else set())


@pytest.mark.asyncio
async def test_shutdown_waits_for_construction_without_blocking_host_finish(tmp_path, monkeypatch):
    from deerflow.agent_instances.memory import SqlMemoryStorage

    memory = InstanceMemory(None)
    execution = SimpleNamespace(owner_loop=asyncio.get_running_loop(), requester=ALICE, instance=SimpleNamespace(id="a" * 32))
    entered, release = asyncio.Event(), asyncio.Event()

    async def bind(**kwargs):
        entered.set()
        await release.wait()
        return SqlMemoryStorage(memory, None)

    monkeypatch.setattr(memory, "bind", bind)
    base = backend(tmp_path)
    construction = asyncio.create_task(asyncio.to_thread(memory.manager, execution, base))
    await asyncio.wait_for(entered.wait(), 2)
    shutdown = asyncio.create_task(asyncio.to_thread(memory.shutdown_flush, 5))

    async def closing():
        while not memory._closing:
            await asyncio.sleep(0)

    try:
        await asyncio.wait_for(closing(), 2)
        memory.finish(execution)
        await asyncio.sleep(0)
    finally:
        release.set()
        await construction
    assert await shutdown is True
    with pytest.raises(AgentDenied, match="closing"):
        await asyncio.to_thread(memory.manager, execution, base)
