"""Instance lifecycle consumes qualified containment and retains Home custody."""

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from test_agent_instance_memory import setup_memory
from test_agent_instances import create, definition
from test_agent_instances import instances as instances
from test_storage_spaces_attachments import ContainedProvider

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, AgentPermission
from deerflow.agent_instances.lifecycle import InstanceLifecycle
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow, AgentLifecycleRow
from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
from deerflow.spaces.contract import Permission


async def lifecycle_fixture(instances):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(lambda c: AgentLifecycleRow.__table__.create(c, checkfirst=True))
    provider = ContainedProvider()
    attachments = SpaceAttachments(agents.files, provider)
    return agents, authority, sf, InstanceLifecycle(agents), attachments, provider, memory


@pytest.mark.asyncio
async def test_suspend_contains_exact_writer_and_restore_preserves_home_and_current_grants(instances):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB, instance_id=agent.id)
    execution = await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    writer = await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    stopped = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="suspend")
    assert stopped["complete"] is True and stopped["instance"].status == "suspended"
    assert writer.container_id not in provider.containers
    with pytest.raises(AgentDenied):
        await execution.validate()
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = int(AgentPermission.INSPECT)
    restored = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=stopped["instance"].generation, operation_id=operation(), action="restore")
    assert restored["complete"] is True and restored["instance"].status == "active"
    assert restored["instance"].home_id == agent.home_id and restored["instance"].principal == agent.principal
    assert (await agents.get(actor=BOB, instance_id=agent.id)).permissions == AgentPermission.INSPECT
    with pytest.raises(AgentDenied):
        await execution.validate()


@pytest.mark.asyncio
async def test_unknown_containment_stays_pending_and_exact_retry_survives_restart(instances):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    writer = await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    provider.uncertain = True
    key = operation()
    pending = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="archive")
    assert pending["complete"] is False and pending["instance"].status == "suspended"
    assert writer.container_id in provider.containers
    with pytest.raises(AgentConflict):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="delete")
    with pytest.raises(AgentConflict):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=pending["instance"].generation, operation_id=operation(), action="restore")
    provider.uncertain = False
    complete = await InstanceLifecycle(agents).change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="archive")
    assert complete["complete"] is True and complete["instance"].status == "archived"
    assert not provider.containers


@pytest.mark.asyncio
async def test_completed_retry_does_not_stop_replacement_writer(instances):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    key = operation()
    stopped = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend")
    restored = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=stopped["instance"].generation, operation_id=operation(), action="restore")
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    replacement = await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    retry = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend")
    assert retry["complete"] is True and retry["instance"].status == "active"
    assert replacement.container_id in provider.containers and restored["instance"].status == "active"


@pytest.mark.asyncio
async def test_manage_does_not_grant_storage_administration(instances):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    async with sf() as session, session.begin():
        session.add(AgentInstanceGrantRow(instance_id=agent.id, user_id="bob", permissions=7))
    home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
    await agents.files.registry.set_grant(actor=ALICE, space_id=home.id, expected_generation=home.generation, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    from deerflow.spaces.contract import SpaceDenied

    with pytest.raises(SpaceDenied):
        await lifecycle.change(actor=BOB, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="suspend")
    assert (await agents.get(actor=ALICE, instance_id=agent.id)).status == "active"


@pytest.mark.asyncio
async def test_adoption_and_supervision_preserve_identity_and_do_not_reactivate_archived_agent(instances):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    archived = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="archive")
    changed = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=archived["instance"].generation, operation_id=operation(), action="adopt", definition=definition(soul="Revised business instructions."))
    assert changed["instance"].status == "archived" and changed["instance"].definition_revision != agent.definition_revision
    supervised = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=changed["instance"].generation, operation_id=operation(), action="supervise", supervisor=ALICE)
    assert supervised["instance"].supervisor == ALICE and supervised["instance"].status == "archived"
    assert (supervised["instance"].principal, supervised["instance"].home_id) == (agent.principal, agent.home_id)


@pytest.mark.asyncio
async def test_delete_retains_home_and_explicit_restore_does_not_replay_old_grants(instances):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
    await agents.files.write(actor=ALICE, space_id=home.id, expected_generation=home.generation, operation_id=operation(), path="wiki.txt", content=b"Retained knowledge", create=True)
    removed = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=operation(), action="delete")
    with pytest.raises(AgentDenied):
        await agents.get(actor=ALICE, instance_id=agent.id)
    assert (await agents.get(actor=ALICE, instance_id=agent.id, include_deleted=True)).status == "deleted"
    assert await agents.files.read(actor=ALICE, space_id=home.id, path="wiki.txt", max_bytes=100) == b"Retained knowledge"
    restored = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=removed["instance"].generation, operation_id=operation(), action="restore")
    assert restored["instance"].status == "active" and restored["instance"].home_id == home.id


@pytest.mark.asyncio
async def test_http_lifecycle_reports_pending_and_provider_ceilings(instances, monkeypatch):
    import httpx
    from test_agent_conversation_http import app_for

    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    provider.uncertain = True
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

    app = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=ALICE)
    endpoint = f"/api/agent-instances/{agent.id}/lifecycle"
    body = {"generation": agent.generation, "operation_id": operation(), "action": "suspend"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        result = await client.post(endpoint, json=body)
        assert result.status_code == 202 and result.json()["complete"] is False, result.text
        status = await client.get(endpoint)
        assert status.status_code == 200 and status.json()["operations"][0]["complete"] is False
        provider.uncertain = False
        complete = await client.post(endpoint, json=body)
        assert complete.status_code == 200 and complete.json()["complete"] is True
        assert (await client.post(endpoint, json={**body, "actor_id": "bob"})).status_code == 422
    limited = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=ALICE, permissions=["agents:read"])
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=limited), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        assert (await client.post(endpoint, json=body)).status_code == 403


@pytest.mark.asyncio
async def test_completed_fence_then_home_grant_change_completes_without_more_fencing(instances, monkeypatch):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    original = lifecycle._parent
    calls = 0

    async def lose_finalization(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise AgentConflict("Interrupted SQL finalization")
        return await original(*args)

    monkeypatch.setattr(lifecycle, "_parent", lose_finalization)
    key = operation()
    with pytest.raises(AgentConflict):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend")
    assert not provider.containers
    await agents.files.registry.set_grant(actor=ALICE, space_id=home.id, expected_generation=home.generation, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    fence_calls = []
    monkeypatch.setattr(provider, "fence", lambda *args: fence_calls.append(args))
    done = await InstanceLifecycle(agents).change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend")
    assert done["complete"] is True and done["instance"].status == "suspended"
    assert not fence_calls
    assert (await agents.files.registry.get(actor=BOB, space_id=home.id)).permissions == Permission.READ


@pytest.mark.asyncio
async def test_pending_http_adoption_uses_committed_snapshot_after_source_removal(instances, monkeypatch):
    from unittest.mock import AsyncMock

    import httpx
    from test_agent_conversation_http import app_for

    from app.gateway.routers import agent_instances as routes
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    provider.uncertain = True
    snapshot = definition(soul="Captured independent revision")
    loader = AsyncMock(return_value=snapshot)
    monkeypatch.setattr(routes, "_load_definition", loader)
    app = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=ALICE)
    body = {"generation": agent.generation, "operation_id": operation(), "action": "adopt", "definition_name": "analyst"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        endpoint = f"/api/agent-instances/{agent.id}/lifecycle"
        assert (await client.post(endpoint, json=body)).status_code == 202
        loader.side_effect = FileNotFoundError("Source removed")
        provider.uncertain = False
        result = await client.post(endpoint, json=body)
        assert result.status_code == 200 and result.json()["instance"]["definition_revision"] == snapshot.revision, result.text
        assert loader.await_count == 1


@pytest.mark.asyncio
async def test_manage_policy_and_canonical_conversation_projection_use_current_grants(instances, monkeypatch):
    import httpx
    from test_agent_conversation_http import app_for

    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB, instance_id=agent.id)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = 1
    app = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=BOB)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        projection = await client.get(f"/api/agent-instances/conversations/{chat['thread_id']}/instance")
        assert projection.status_code == 200 and projection.json()["instance"]["id"] == agent.id, projection.text
        endpoint = f"/api/agent-instances/{agent.id}"
        assert (await client.get(endpoint + "/definition")).status_code == 404
        assert (await client.get(endpoint + "/grants")).status_code == 404
        async with sf() as session, session.begin():
            (await session.get(AgentInstanceGrantRow, (agent.id, "bob"))).permissions = 4
        grants = await client.get(endpoint + "/grants")
        assert grants.status_code == 200 and {v["user_id"] for v in grants.json()["grants"]} == {"alice", "bob"}, grants.text
        assert (await client.get(endpoint)).status_code == 200


@pytest.mark.asyncio
async def test_pending_empty_scope_never_retires_a_new_human_attachment(instances, monkeypatch):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    original = lifecycle._parent
    calls = 0

    async def interrupt(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise AgentConflict("Interrupted SQL completion")
        return await original(*args)

    monkeypatch.setattr(lifecycle, "_parent", interrupt)
    key = operation()
    with pytest.raises(AgentConflict):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend")
    home = await agents.files.registry.get(actor=ALICE, space_id=agent.home_id)
    replacement = await attachments.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "human", writable=True)])
    pending = await InstanceLifecycle(agents).change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend")
    assert pending["complete"] is False and replacement.container_id in provider.containers


@pytest.mark.asyncio
async def test_company_manager_resolves_captured_adoption_after_creator_departure(instances, monkeypatch):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    snapshot = definition(soul="Company business instructions captured before departure")
    key = operation()
    provider.uncertain = True
    assert not (await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="adopt", definition=snapshot))["complete"]
    human = agents.directory.human

    async def after_departure(reference):
        return None if reference == ALICE else await human(reference)

    monkeypatch.setattr(agents.directory, "human", after_departure)
    monkeypatch.setattr(agents.files.registry._resolver, "_human", after_departure)
    provider.uncertain = False
    done = await InstanceLifecycle(agents).change(actor=BOB, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="adopt", definition=snapshot)
    assert done["complete"] and done["instance"].definition_revision == snapshot.revision
    async with sf() as session:
        intent = await session.get(AgentLifecycleRow, (agent.id, key))
        assert intent.actor_id == "alice" and intent.resolved_by == "bob"
    monkeypatch.setattr(provider, "fence", lambda *_: pytest.fail("Completed recovery cannot fence a replacement"))
    replay = await lifecycle.change(actor=BOB, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="adopt", definition=snapshot)
    assert replay["complete"] and replay["instance"].custody == "company"


@pytest.mark.asyncio
async def test_pending_containment_cannot_be_stranded_by_a_concurrent_rename(instances):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    provider.uncertain = True
    key = operation()
    pending = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend")
    with pytest.raises(AgentConflict, match="pending"):
        await agents.rename(actor=ALICE, instance_id=agent.id, expected_generation=pending["instance"].generation, name="Concurrent name")
    assert (await agents.get(actor=ALICE, instance_id=agent.id)).generation == pending["instance"].generation
    provider.uncertain = False
    assert (await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend"))["complete"]


@pytest.mark.asyncio
async def test_explicit_company_copy_creates_distinct_empty_home_and_memory(instances, monkeypatch):
    import httpx
    from test_agent_conversation_http import app_for

    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    personal = await create(agents)
    home = await agents.files.registry.get(actor=ALICE, space_id=personal.home_id)
    await agents.files.write(actor=ALICE, space_id=home.id, expected_generation=home.generation, operation_id=operation(), path="private-wiki.txt", content=b"Explicit copy required", create=True)
    app = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=ALICE)
    body = {"creation_id": operation(), "generation": personal.generation, "name": "Company analyst"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        endpoint = f"/api/agent-instances/{personal.id}/company-copy"
        result = await client.post(endpoint, json=body)
        assert result.status_code == 201, result.text
        company = result.json()
        assert company["custody"] == "company" and company["owner_id"] is None
        assert company["id"] != personal.id and company["home_id"] != personal.home_id
        assert (await client.post(endpoint, json=body)).json()["id"] == company["id"]
        files, truncated = await agents.files.list_directory(actor=ALICE, space_id=company["home_id"])
        assert files == [] and truncated is False
        assert (await agents.get(actor=ALICE, instance_id=personal.id)).custody == "personal"
        port = await memory.bind(actor=ALICE, instance_id=company["id"])
        import asyncio

        assert (await asyncio.to_thread(port.load, agent_name=port.scope.agent_name, user_id="alice"))["facts"] == []


@pytest.mark.asyncio
async def test_cancellation_owns_containment_and_sql_completion(instances, monkeypatch):
    import asyncio
    import threading

    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    entered, release = threading.Event(), threading.Event()
    original = provider.fence

    def paused(*args):
        entered.set()
        assert release.wait(10)
        return original(*args)

    monkeypatch.setattr(provider, "fence", paused)
    key = operation()
    task = asyncio.create_task(lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="delete"))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0.02)
        task.cancel()
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        async with sf() as session:
            intent = await session.get(AgentLifecycleRow, (agent.id, key))
            assert intent.phase == "complete" and intent.resolved_by == "alice"
        assert not provider.containers
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["generation", "inspect"])
async def test_company_copy_rechecks_source_when_first_target_enrolls(instances, monkeypatch, change):
    import httpx
    from sqlalchemy import func, select
    from test_agent_conversation_http import app_for

    from deerflow.persistence.agent_instances.model import AgentInstanceRow
    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    personal = await create(agents)
    original = agents.create

    async def change_after_preflight(**kwargs):
        async with sf() as session, session.begin():
            if change == "generation":
                (await session.get(AgentInstanceRow, personal.id)).generation += 1
            else:
                (await session.get(AgentInstanceGrantRow, (personal.id, "alice"))).permissions = 5
        return await original(**kwargs)

    monkeypatch.setattr(agents, "create", change_after_preflight)
    app = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=ALICE)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        result = await client.post(f"/api/agent-instances/{personal.id}/company-copy", json={"creation_id": operation(), "generation": personal.generation, "name": "Company"})
        assert result.status_code in {404, 409}, result.text
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentInstanceRow)) == 1


@pytest.mark.asyncio
async def test_company_pending_revocation_cannot_remove_last_current_resolver(instances, monkeypatch):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    provider.uncertain = True
    key = operation()
    assert not (await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="grant", member=BOB, permissions=0))["complete"]
    original = agents.directory.human

    async def humans(reference):
        return None if reference == ALICE else await original(reference)

    monkeypatch.setattr(agents.directory, "human", humans)
    monkeypatch.setattr(agents.files.registry._resolver, "_human", humans)
    provider.uncertain = False
    with pytest.raises(AgentDenied):
        await InstanceLifecycle(agents).change(actor=BOB, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="grant", member=BOB, permissions=0)
    assert (await agents.get(actor=BOB, instance_id=agent.id)).permissions & AgentPermission.MANAGE
    async with sf() as session:
        assert (await session.get(AgentLifecycleRow, (agent.id, key))).phase == "pending"


@pytest.mark.asyncio
async def test_stale_home_preflight_does_not_report_withdrawal_that_never_committed(instances, monkeypatch):
    from deerflow.spaces.contract import SpaceConflict

    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    original = attachments.retire

    async def drift(**kwargs):
        await agents.files.registry.set_grant(actor=ALICE, space_id=agent.home_id, expected_generation=kwargs["expected_generation"], subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
        return await original(**kwargs)

    monkeypatch.setattr(attachments, "retire", drift)
    key = operation()
    with pytest.raises(SpaceConflict):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend")
    assert (await agents.get(actor=ALICE, instance_id=agent.id)).status == "active"
    async with sf() as session:
        assert await session.get(AgentLifecycleRow, (agent.id, key)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["supervise", "grant"])
async def test_retired_supervision_target_can_be_abandoned_only_after_exact_containment(instances, monkeypatch, action):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents, custody="company", supervisor=BOB)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    writer = await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    key = operation()
    provider.uncertain = True
    fields = {"supervisor": ALICE} if action == "supervise" else {"member": ALICE, "permissions": 7}
    pending = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action=action, **fields)
    original = agents.directory.human

    async def humans(reference):
        return None if reference == ALICE else await original(reference)

    monkeypatch.setattr(agents.directory, "human", humans)
    monkeypatch.setattr(agents.files.registry._resolver, "_human", humans)
    from deerflow.spaces.contract import SpaceConflict

    with pytest.raises(SpaceConflict):
        await lifecycle.abandon(actor=BOB, instance_id=agent.id, expected_generation=pending["instance"].generation, operation_id=key)
    assert writer.container_id in provider.containers
    provider.uncertain = False
    await attachments.retire(actor=BOB, space_id=home.id, expected_generation=home.generation, attachment_ids=[writer.id])
    done = await lifecycle.abandon(actor=BOB, instance_id=agent.id, expected_generation=pending["instance"].generation, operation_id=key)
    assert done["complete"] and done["instance"].status == "suspended" and done["instance"].supervisor == BOB
    async with sf() as session:
        intent = await session.get(AgentLifecycleRow, (agent.id, key))
        assert intent.phase == "abandoned" and intent.actor_id == "alice" and intent.resolved_by == "bob"
    restored = await lifecycle.change(actor=BOB, instance_id=agent.id, expected_generation=done["instance"].generation, operation_id=operation(), action="restore")
    assert restored["instance"].status == "active"

    replay = await lifecycle.change(actor=BOB, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action=action, **fields)
    assert replay["complete"] and replay["abandoned"] and replay["instance"].status == "active"


@pytest.mark.asyncio
async def test_http_abandon_resolves_exact_intent_without_reactivating(instances, monkeypatch):
    import httpx
    from test_agent_conversation_http import app_for

    from deerflow.persistence.thread_meta.sql import ThreadMetaRepository

    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    original = lifecycle._parent
    calls = 0

    async def interrupt(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise AgentConflict("Interrupted finalization")
        return await original(*args)

    monkeypatch.setattr(lifecycle, "_parent", interrupt)
    key = operation()
    with pytest.raises(AgentConflict):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend")
    current = await agents.get(actor=ALICE, instance_id=agent.id)
    app = app_for(agents, authority, ThreadMetaRepository(sf, instance_authority=authority), monkeypatch, actor=ALICE)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}) as client:
        response = await client.post(f"/api/agent-instances/{agent.id}/lifecycle/abandon", json={"generation": current.generation, "operation_id": key})
        assert response.status_code == 200 and response.json()["abandoned"] and response.json()["instance"]["status"] == "suspended", response.text
        states = (await client.get(f"/api/agent-instances/{agent.id}/lifecycle")).json()["operations"]
        assert states[0]["complete"] and states[0]["abandoned"]


@pytest.mark.asyncio
async def test_uncommitted_cross_host_refusal_is_not_reported_as_withdrawn(instances, monkeypatch):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    writer = await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    monkeypatch.setattr(provider, "host_id", "unqualified-other-host")
    key = operation()
    with pytest.raises(AgentConflict, match="not committed"):
        await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="suspend")
    assert writer.container_id in provider.containers
    assert (await agents.get(actor=ALICE, instance_id=agent.id)).status == "active"
    async with sf() as session:
        assert await session.get(AgentLifecycleRow, (agent.id, key)) is None


@pytest.mark.asyncio
async def test_abandon_wins_before_original_retry_admission_and_is_reported_truthfully(instances, monkeypatch):
    agents, authority, sf, lifecycle, attachments, provider, memory = await lifecycle_fixture(instances)
    agent = await create(agents)
    home = await agents.files.registry.get(actor=agent.principal, space_id=agent.home_id)
    await attachments.attach(actor=agent.principal, incarnation=operation(), resources=[ResourceMount(home.id, home.generation, "home", writable=True)])
    provider.uncertain = True
    key = operation()
    pending = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="archive")
    provider.uncertain = False
    original = attachments.retire

    async def abandon_between_preflight_and_admission(**kwargs):
        external = {k: v for k, v in kwargs.items() if k != "admission"}
        await original(**external)
        await lifecycle.abandon(actor=ALICE, instance_id=agent.id, expected_generation=pending["instance"].generation, operation_id=key)
        return await original(**kwargs)

    monkeypatch.setattr(attachments, "retire", abandon_between_preflight_and_admission)
    result = await lifecycle.change(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, operation_id=key, action="archive")
    assert result["complete"] and result["abandoned"] and result["instance"].status == "suspended"
