"""Instance conversations use durable bindings and current grants, not metadata."""

import asyncio

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, AgentPermission
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentInstanceGrantRow, AgentInstanceRow, AgentProtectedContextRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.thread_meta.sql import ThreadMetaRepository


async def conversations(instances):
    agents, files, sf, catalog = instances
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(lambda c: ThreadMetaRow.__table__.create(c))
        await connection.run_sync(lambda c: AgentConversationRow.__table__.create(c))
        await connection.run_sync(lambda c: AgentProtectedContextRow.__table__.create(c))
    authority = AgentConversations(agents)
    threads = ThreadMetaRepository(sf, instance_authority=authority)
    return agents, authority, threads, sf


@pytest.mark.asyncio
async def test_two_chats_bind_one_home_and_keep_human_requester(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    one = await authority.create(actor=ALICE, instance_id=agent.id, thread_id=operation())
    two = await authority.create(actor=BOB, instance_id=agent.id, thread_id=operation())
    assert one["user_id"] == "alice" and two["user_id"] == "bob"
    for record in (one, two):
        assert await threads.check_access(record["thread_id"], "bob", require_existing=True)
        execution = await authority.execution(actor=BOB, thread_id=record["thread_id"])
        assert execution.instance.principal == agent.principal
        assert execution.instance.home_id == agent.home_id
        assert execution.requester == BOB
        assert execution.definition.revision == agent.definition_revision


@pytest.mark.asyncio
async def test_revocation_overrides_creator_owner_and_filter_precedes_pagination(instances):
    agents, authority, threads, sf = await conversations(instances)
    hidden = await create(agents, custody="company", supervisor=BOB)
    visible = await create(agents)
    hidden_chat = await authority.create(actor=ALICE, instance_id=hidden.id, thread_id=operation())
    visible_chat = await authority.create(actor=ALICE, instance_id=visible.id, thread_id=operation())
    async with sf() as session, session.begin():
        await session.delete(await session.get(AgentInstanceGrantRow, (hidden.id, "alice")))
    assert not await threads.check_access(hidden_chat["thread_id"], "alice")
    assert await threads.get(hidden_chat["thread_id"], user_id="alice") is None
    assert [r["thread_id"] for r in await threads.search(user_id="alice", limit=1)] == [visible_chat["thread_id"]]
    assert await threads.get(hidden_chat["thread_id"], user_id="bob") is not None
    with pytest.raises(AgentDenied):
        await authority.execution(actor=ALICE, thread_id=hidden_chat["thread_id"])


@pytest.mark.asyncio
async def test_use_inspect_manage_are_checked_for_conversation_operations(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, thread_id=operation())
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = int(AgentPermission.INSPECT)
    assert await threads.get(chat["thread_id"], user_id="bob") is not None
    assert await authority.allowed(actor=BOB, thread_id=chat["thread_id"], permission=AgentPermission.INSPECT)
    assert not await authority.allowed(actor=BOB, thread_id=chat["thread_id"], permission=AgentPermission.MANAGE)
    with pytest.raises(AgentDenied):
        await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    await threads.update_metadata(chat["thread_id"], {"changed": True}, user_id="bob")
    await threads.update_display_name(chat["thread_id"], "unauthorized", user_id="bob")
    await threads.delete(chat["thread_id"], user_id="bob")
    retained = await threads.get(chat["thread_id"], user_id="bob")
    assert retained is not None and "changed" not in retained["metadata"] and retained["display_name"] is None


@pytest.mark.asyncio
async def test_deleting_chat_retains_home_and_binding_tombstone(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, thread_id=operation())
    captured = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    await threads.delete(chat["thread_id"], user_id="alice")
    assert (await agents.get(actor=ALICE, instance_id=agent.id)).home_id == agent.home_id
    assert not await threads.check_access(chat["thread_id"], "alice")
    async with sf() as session:
        assert await session.get(AgentConversationRow, chat["thread_id"]) is not None
    with pytest.raises(AgentDenied):
        await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    with pytest.raises(AgentDenied):
        await authority.validate(captured)
    with pytest.raises(AgentDenied):
        await threads.create(chat["thread_id"], user_id="alice", metadata={})


@pytest.mark.asyncio
async def test_metadata_cannot_bind_or_rebind_legacy_chat(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    legacy = await threads.create(operation(), user_id="alice", metadata={"agent_instance_id": agent.id})
    assert await authority.binding(legacy["thread_id"]) is None
    assert not await threads.check_access(legacy["thread_id"], "bob")
    with pytest.raises(AgentDenied):
        await authority.execution(actor=ALICE, thread_id=legacy["thread_id"])


@pytest.mark.asyncio
async def test_execution_fails_closed_if_resource_or_instance_changes(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, thread_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    assert execution.thread_paths["workspace_path"] == "/mnt/spaces/home"
    assert "files_path" not in execution.thread_paths and "shared_path" not in execution.thread_paths
    await agents.rename(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, name="Renamed")
    with pytest.raises(AgentDenied):
        await authority.validate(execution)


@pytest.mark.asyncio
async def test_unavailable_adapter_does_not_restore_creator_owner_access(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, thread_id=operation())
    disabled = ThreadMetaRepository(sf, instance_authority=AgentConversations(None, session_factory=sf))
    assert await disabled.get(chat["thread_id"], user_id="alice") is None
    assert not await disabled.check_access(chat["thread_id"], "alice")
    assert await disabled.search(user_id="alice") == []


@pytest.mark.asyncio
async def test_malformed_persisted_identity_is_not_conversation_authority(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, thread_id=operation())
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, agent.id)).supervisor_id = "invalid id"
    assert await threads.get(chat["thread_id"], user_id="bob") is None
    assert await threads.search(user_id="bob") == []


@pytest.mark.asyncio
async def test_personal_custodian_departure_blocks_read_but_company_survives(instances):
    agents, authority, threads, sf = await conversations(instances)
    personal = await create(agents, supervisor=BOB)
    company = await create(agents, custody="company", supervisor=BOB)
    private = await authority.create(actor=ALICE, instance_id=personal.id, thread_id=operation())
    shared = await authority.create(actor=ALICE, instance_id=company.id, thread_id=operation())
    original = agents.directory.human

    async def remaining(reference):
        return None if reference == ALICE else await original(reference)

    agents.directory.human = remaining
    assert await threads.get(private["thread_id"], user_id="bob") is None
    assert await threads.get(shared["thread_id"], user_id="bob") is not None


@pytest.mark.asyncio
async def test_server_mints_conversation_identity_and_retry_cannot_adopt_legacy_data(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    key = operation()
    first = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=key)
    retry = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=key)
    assert first["thread_id"] == retry["thread_id"] and first["thread_id"] != key
    second = await create(agents)
    with pytest.raises(AgentConflict):
        await authority.create(actor=ALICE, instance_id=second.id, creation_id=key)
    await threads.delete(first["thread_id"], user_id="alice")
    with pytest.raises(AgentDenied):
        await authority.create(actor=ALICE, instance_id=agent.id, creation_id=key)


@pytest.mark.asyncio
async def test_use_only_staff_can_converse_in_own_chat_but_not_inspect_other_chats(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    other = await authority.create(actor=ALICE, instance_id=agent.id, thread_id=operation())
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = int(AgentPermission.USE)
    own = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    assert await threads.get(own["thread_id"], user_id="bob") is not None
    assert await threads.get(other["thread_id"], user_id="bob") is None
    assert (await authority.execution(actor=BOB, thread_id=own["thread_id"])).requester == BOB
    with pytest.raises(AgentDenied):
        await authority.execution(actor=BOB, thread_id=other["thread_id"])


@pytest.mark.asyncio
async def test_inspection_projects_the_adopted_definition_without_execution_authority(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = int(AgentPermission.INSPECT)
    projection = await authority.inspection(actor=BOB, thread_id=chat["thread_id"])
    assert projection.definition.revision == agent.definition_revision and not projection.execution_allowed
    with pytest.raises(AgentDenied, match="cannot execute"):
        await projection.validate()


@pytest.mark.asyncio
async def test_worker_bookkeeping_and_manager_edit_use_one_sql_lock_order(instances, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession

    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB, instance_id=agent.id, creation_id=operation())
    execution = await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    if sf.kw["bind"].dialect.name == "sqlite":
        await authority.update_run_metadata(execution, status="running")
        await threads.update_metadata(chat["thread_id"], {"managed": True}, user_id="alice")
    else:
        first_lock, release, manager_started = asyncio.Event(), asyncio.Event(), asyncio.Event()
        order = []
        original = AsyncSession.execute

        async def observe(session, statement, *args, **kwargs):
            task = asyncio.current_task().get_name()
            locked = getattr(statement, "_for_update_arg", None) is not None
            if task == "agent-manager-edit" and locked:
                manager_started.set()
            result = await original(session, statement, *args, **kwargs)
            if task == "agent-worker-bookkeeping" and locked and "FROM storage_spaces" not in str(statement):
                order.append(str(statement))
                if len(order) == 1:
                    first_lock.set()
                    await release.wait()
            return result

        monkeypatch.setattr(AsyncSession, "execute", observe)
        worker = asyncio.create_task(authority.update_run_metadata(execution, status="running"), name="agent-worker-bookkeeping")
        manager = None
        try:
            await asyncio.wait_for(first_lock.wait(), 3)
            assert "FROM threads_meta" in order[0], "Worker must lock the same first row as repository mutations"
            manager = asyncio.create_task(threads.update_metadata(chat["thread_id"], {"managed": True}, user_id="alice"), name="agent-manager-edit")
            await asyncio.wait_for(manager_started.wait(), 3)
            release.set()
            await asyncio.wait_for(asyncio.gather(worker, manager), 3)
        finally:
            release.set()
            await asyncio.gather(worker, *([manager] if manager is not None else []), return_exceptions=True)
    retained = await threads.get(chat["thread_id"], user_id="bob")
    assert retained["status"] == "running" and retained["metadata"]["managed"] is True
    await agents.rename(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, name="New name")
    with pytest.raises(AgentDenied):
        await authority.update_run_metadata(execution, status="idle")


@pytest.mark.asyncio
async def test_child_loop_and_sync_worker_return_to_the_host_authority_loop(instances):
    agents, authority, threads, sf = await conversations(instances)
    agent = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=agent.id, creation_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    await asyncio.wait_for(asyncio.to_thread(lambda: asyncio.run(execution.validate())), 3)
    await asyncio.wait_for(asyncio.to_thread(execution.validate_sync), 3)
    with pytest.raises(AgentDenied, match="cannot block"):
        execution.validate_sync()
