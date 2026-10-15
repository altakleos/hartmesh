"""Creation limits borrow real SQL; fixture backings never qualify native quotas."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from sqlalchemy import func, select
from test_agent_instances import create, definition
from test_agent_instances import instances as instances

from deerflow.agent_instances.contract import AgentDenied, Refusal
from deerflow.config import agents_api_config
from deerflow.config.agents_api_config import AgentsApiConfig
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.spaces.files import SpaceBackingRow


@pytest.fixture(autouse=True)
def configuration(monkeypatch, request):
    monkeypatch.setattr(agents_api_config, "_agents_api_config", AgentsApiConfig(enabled=True, ai_assistants_per_user=10, ai_employees=10))
    if "single_connection" in request.node.name:
        import _storage_spaces_test_support as support

        factory = support.create_async_engine
        monkeypatch.setattr(support, "create_async_engine", lambda *args, **kwargs: factory(*args, **kwargs, pool_size=1, max_overflow=0, pool_timeout=0.5))


def policy(service, *, prepared=4, allowed_key=4):
    async def allowance(kind, *, session):
        assert session.in_transaction() and session.is_active
        assert session.bind is service._sf.kw["bind"]
        count = await session.scalar(
            select(func.count()).select_from(AgentInstanceRow).where(AgentInstanceRow.custody == ("personal" if kind == "assistant" else "company"), AgentInstanceRow.status.in_(("provisioning", "active", "suspended", "archived")))
        )
        allowed = min(prepared, max(allowed_key, count))
        return SimpleNamespace(allowed=allowed, allowed_key=allowed_key, prepared=prepared, in_use=count, free=allowed - count)

    service.allowance_of = allowance


async def row_count(sf):
    async with sf() as session:
        return await session.scalar(select(func.count()).select_from(AgentInstanceRow))


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, 1])
async def test_assistant_per_user_limit_precedes_new_enrollment_and_applies_to_admin(instances, limit):
    service, _files, sf, _catalog = instances
    agents_api_config.set_agents_api_config(AgentsApiConfig(enabled=True, ai_assistants_per_user=limit, ai_employees=4))
    policy(service)
    if limit:
        await create(service)
    with pytest.raises(Refusal) as error:
        await create(service)
    assert error.value.reason == "ai-assistant-limit" and error.value.status == 409
    assert error.value.extra == {"per_user": limit, "mine": limit, "allowed": 4, "in_use": limit}
    assert await row_count(sf) == limit


@pytest.mark.asyncio
@pytest.mark.parametrize("prepared", [0, 1])
async def test_assistant_zero_or_negative_free_capacity_refuses_without_a_new_row(instances, prepared):
    service, _files, sf, _catalog = instances
    policy(service)
    if prepared:
        await create(service)
        await create(service)
    policy(service, prepared=prepared)
    before = await row_count(sf)
    with pytest.raises(Refusal) as error:
        await create(service)
    assert error.value.reason == "ai-assistant-none-free"
    assert error.value.extra == {"per_user": 10, "mine": before, "allowed": prepared, "in_use": before}
    assert await row_count(sf) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("allowed_key", [0, 1])
async def test_employee_key_or_count_refuses_with_truthful_in_use(instances, allowed_key):
    service, _files, sf, _catalog = instances
    policy(service)
    await create(service, custody="company")
    policy(service, allowed_key=allowed_key)
    with pytest.raises(Refusal) as error:
        await create(service, custody="company")
    assert error.value.reason == "ai-employee-limit"
    assert error.value.extra == {"allowed": allowed_key, "in_use": 1}
    assert await row_count(sf) == 1
    with pytest.raises(AgentDenied):
        await create(service, actor=BOB, custody="company", snapshot=definition("bob"))


@pytest.mark.asyncio
async def test_exact_creation_retry_bypasses_counts_and_allocates_no_second_home(instances):
    service, _files, sf, _catalog = instances
    agents_api_config.set_agents_api_config(AgentsApiConfig(enabled=True, ai_assistants_per_user=1, ai_employees=1))
    policy(service, prepared=1, allowed_key=1)
    creation_id = operation()
    original = await create(service, creation_id=creation_id)
    assert await create(service, creation_id=creation_id) == original
    with pytest.raises(Refusal):
        await create(service)
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(SpaceBackingRow)) == 1


@pytest.mark.asyncio
async def test_simultaneous_creations_at_the_last_place_commit_only_one_intent(instances):
    service, _files, sf, _catalog = instances
    policy(service, prepared=1, allowed_key=1)
    outcomes = await asyncio.gather(create(service), create(service), return_exceptions=True)
    accepted = [value for value in outcomes if not isinstance(value, BaseException)]
    refused = [value for value in outcomes if isinstance(value, Refusal)]
    assert len(accepted) == len(refused) == 1 and refused[0].reason == "ai-assistant-none-free"
    assert await row_count(sf) == 1


@pytest.mark.asyncio
async def test_custody_assigns_its_own_typed_slot_and_does_not_consume_another_kind(instances):
    service, _files, sf, catalog = instances
    specs = list(catalog.volumes.values())
    for spec, kind in zip(specs, ("employee", "assistant", "user", "company"), strict=True):
        spec.kind = kind
    policy(service, prepared=1, allowed_key=1)
    assistant = await create(service)
    employee = await create(service, custody="company")
    async with sf() as session:
        rows = (await session.scalars(select(SpaceBackingRow).where(SpaceBackingRow.space_id.in_((assistant.home_id, employee.home_id))))).all()
    by_home = {row.space_id: catalog.volumes[row.slot_id].kind for row in rows}
    assert by_home == {assistant.home_id: "assistant", employee.home_id: "employee"}


@pytest.mark.asyncio
async def test_typed_physical_exhaustion_retains_unfinished_identity_and_retry_never_takes_another_kind(instances):
    service, _files, sf, catalog = instances
    specs = list(catalog.volumes.values())
    for spec, kind in zip(specs, ("assistant", "employee", "user", "company"), strict=True):
        spec.kind = kind
    policy(service)
    await create(service)
    creation_id = operation()
    with pytest.raises(Refusal) as error:
        await create(service, creation_id=creation_id)
    assert error.value.reason == "ai-assistant-none-free"
    async with sf() as session:
        pending = await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.creation_id == creation_id))
        assert pending.status == "provisioning" and pending.home_id is None
        pending_id = pending.id
        assert await session.scalar(select(func.count()).select_from(SpaceBackingRow)) == 1
    specs[2].kind = "assistant"
    resumed = await create(service, creation_id=creation_id)
    assert resumed.id == pending_id and resumed.status == "active"
    async with sf() as session:
        binding = await session.scalar(select(SpaceBackingRow).where(SpaceBackingRow.space_id == resumed.home_id))
    assert catalog.volumes[binding.slot_id].kind == "assistant"


@pytest.mark.asyncio
@pytest.mark.parametrize("request_kind", ["personal", "company", "company-copy"])
async def test_http_late_kind_capacity_refusal_retains_intent_and_exact_retry_resumes_it(instances, monkeypatch, request_kind):
    import httpx

    from app.gateway.routers import agent_instances as routes

    service, _files, sf, catalog = instances
    specs = list(catalog.volumes.values())
    for spec, kind in zip(specs, ("assistant", "employee", "user", "company"), strict=True):
        spec.kind = kind
    policy(service)
    custody = "personal" if request_kind == "personal" else "company"
    await create(service, custody=custody)
    body = {"creation_id": operation(), "name": "New AI assistant" if custody == "personal" else "New AI employee"}
    if request_kind == "company-copy":
        source = await create(service)
        endpoint = f"/api/agent-instances/{source.id}/company-copy"
        body["generation"] = source.generation
    else:
        endpoint = "/api/agent-instances"
        body.update(custody=custody, definition_name="analyst")
        monkeypatch.setattr(routes, "_load_definition", AsyncMock(return_value=definition()))
    async with sf() as session:
        original_bindings = await session.scalar(select(func.count()).select_from(SpaceBackingRow))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=http_app(service)), base_url="http://test") as client:
        response = await client.post(endpoint, json=body)
        assert response.status_code == 409, response.text
        assert response.json()["reason"] == ("ai-assistant-none-free" if custody == "personal" else "ai-employee-limit")
        assert response.json()["allowed"] == 4 and response.json()["in_use"] == 2
        async with sf() as session:
            pending = await session.scalar(select(AgentInstanceRow).where(AgentInstanceRow.creation_id == body["creation_id"]))
            assert pending is not None and pending.status == "provisioning" and pending.home_id is None
            pending_id = pending.id
            assert await session.scalar(select(func.count()).select_from(SpaceBackingRow)) == original_bindings
        specs[2].kind = "assistant" if custody == "personal" else "employee"
        response = await client.post(endpoint, json=body)
        assert response.status_code == 201, response.text
        assert response.json()["id"] == pending_id and response.json()["status"] == "active"
        async with sf() as session:
            binding = await session.scalar(select(SpaceBackingRow).where(SpaceBackingRow.space_id == response.json()["home_id"]))
            assert binding.slot_id == specs[2].slot_id


async def sql_human(service, files, sf):
    from deerflow.spaces.principals import HostPrincipalResolver

    human = service.directory.human
    borrowed = []

    async def current(reference, *, session=None):
        if session is None:
            async with sf() as reader:
                await reader.scalar(select(func.count()).select_from(AgentInstanceRow))
        else:
            assert session.in_transaction()
            await session.scalar(select(func.count()).select_from(AgentInstanceRow))
            borrowed.append(reference)
        return await human(reference)

    async def in_session(reference, session):
        return await current(reference, session=session)

    current.in_session = in_session
    service.directory.human = current
    files.registry._resolver = HostPrincipalResolver(human=current, nonhuman=service.directory.lookup, nonhuman_in_session=service.directory.lookup_in_session)
    return borrowed


@pytest.mark.asyncio
async def test_actual_single_connection_creation_and_exact_retry_borrow_all_writer_lookups(instances):
    service, files, sf, _catalog = instances
    borrowed = await sql_human(service, files, sf)
    policy(service)
    creation_id = operation()
    async with asyncio.timeout(5):
        instance = await create(service, creation_id=creation_id)
        assert await create(service, creation_id=creation_id) == instance
    assert len(borrowed) >= 4


@pytest.mark.asyncio
async def test_actual_single_connection_http_company_copy_borrows_source_snapshot_writer(instances):
    import httpx

    service, files, sf, _catalog = instances
    await sql_human(service, files, sf)
    policy(service)
    source = await create(service)
    async with asyncio.timeout(5), httpx.AsyncClient(transport=httpx.ASGITransport(app=http_app(service)), base_url="http://test") as client:
        response = await client.post(f"/api/agent-instances/{source.id}/company-copy", json={"creation_id": operation(), "generation": source.generation, "name": "Company copy"})
    assert response.status_code == 201, response.text
    assert response.json()["custody"] == "company"


@pytest.mark.asyncio
async def test_actual_single_connection_http_conversation_creation_borrows_writer(instances):
    import httpx
    from test_agent_conversations import conversations

    service, authority, _threads, sf = await conversations(instances)
    await sql_human(service, instances[1], sf)
    source = await create(service)
    service.execution_probe = AsyncMock(return_value=SimpleNamespace(execution="ready", reason=None))
    app = http_app(service)
    app.state.agent_conversations = authority
    async with asyncio.timeout(5), httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(f"/api/agent-instances/{source.id}/conversations", json={"creation_id": operation()})
    assert response.status_code == 201, response.text


@pytest.mark.asyncio
async def test_company_copy_non_admin_is_forbidden_only_after_visible_source_admission(instances):
    import httpx

    service, _files, _sf, _catalog = instances
    source = await create(service, actor=BOB, snapshot=definition("bob"))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=http_app(service, actor=BOB)), base_url="http://test") as client:
        body = {"creation_id": operation(), "generation": source.generation, "name": "Company copy"}
        response = await client.post(f"/api/agent-instances/{source.id}/company-copy", json=body)
        hidden = await client.post(f"/api/agent-instances/{operation()}/company-copy", json={**body, "creation_id": operation()})
    assert response.status_code == 403
    assert hidden.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["provisioning", "active", "suspended", "archived", "deleted"])
async def test_personal_count_includes_unfinished_paused_and_retired_but_excludes_deleted(instances, status):
    service, _files, sf, _catalog = instances
    policy(service)
    first = await create(service)
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, first.id)).status = status
    agents_api_config.set_agents_api_config(AgentsApiConfig(enabled=True, ai_assistants_per_user=1, ai_employees=4))
    if status == "deleted":
        second = await create(service)
        assert second.id != first.id
    else:
        with pytest.raises(Refusal) as error:
            await create(service)
        assert error.value.reason == "ai-assistant-limit" and error.value.extra["mine"] == 1


@pytest.mark.asyncio
async def test_company_retry_rechecks_current_provisioning_authority(instances):
    from deerflow.spaces.contract import ResolvedPrincipal

    service, _files, _sf, _catalog = instances
    policy(service)
    creation_id = operation()
    await create(service, creation_id=creation_id, custody="company")
    human = service.directory.human

    async def demoted(reference):
        return ResolvedPrincipal(ALICE, False) if reference == ALICE else await human(reference)

    service.directory.human = demoted
    with pytest.raises(AgentDenied, match="provisioning authority"):
        await create(service, creation_id=creation_id, custody="company")


def http_app(service, *, actor=ALICE):
    from test_agent_instances import request_app

    from app.gateway.refusals import refusal_handler

    app = request_app(service, actor=actor, permissions=("agents:read", "agents:write", "threads:write"))
    app.add_exception_handler(Refusal, refusal_handler)
    return app


@pytest.mark.asyncio
async def test_list_reports_callers_current_ai_assistant_counts(instances):
    import httpx

    service, _files, _sf, _catalog = instances
    policy(service)
    await create(service)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=http_app(service)), base_url="http://test") as client:
        response = await client.get("/api/agent-instances")
    assert response.status_code == 200
    assert response.json()["ai_assistants"] == {"per_user": 10, "allowed": 4, "in_use": 1, "mine": 1}


@pytest.mark.asyncio
async def test_unavailable_creation_refuses_before_enrollment(instances, monkeypatch):
    import httpx

    from app.gateway.routers import agent_instances as routes

    service, _files, sf, _catalog = instances
    service.execution_probe = AsyncMock(return_value=SimpleNamespace(execution="unavailable", reason="operator-hold"))
    monkeypatch.setattr(routes, "_load_definition", AsyncMock(return_value=definition()))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=http_app(service)), base_url="http://test") as client:
        response = await client.post("/api/agent-instances", json={"creation_id": operation(), "name": "Analyst", "definition_name": "analyst"})
    assert response.status_code == 409 and response.json()["reason"] == "execution-unavailable"
    assert response.json()["execution_reason"] == "operator-hold" and await row_count(sf) == 0


@pytest.mark.asyncio
async def test_conversation_creation_checks_grants_then_execution_before_creating_a_thread(instances):
    import httpx

    service, _files, _sf, _catalog = instances
    instance = await create(service)
    service.execution_probe = AsyncMock(return_value=SimpleNamespace(execution="unavailable", reason="operator-hold"))
    app = http_app(service)
    authority = SimpleNamespace(create=AsyncMock(return_value={"thread_id": "thread"}))
    app.state.agent_conversations = authority
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(f"/api/agent-instances/{instance.id}/conversations", json={"creation_id": operation()})
    assert response.status_code == 409 and response.json()["execution_reason"] == "operator-hold"
    authority.create.assert_not_awaited()
    service.execution_probe.reset_mock()
    hidden_app = http_app(service, actor=BOB)
    hidden_app.state.agent_conversations = authority
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=hidden_app), base_url="http://test") as client:
        denied = await client.post(f"/api/agent-instances/{instance.id}/conversations", json={"creation_id": operation()})
    assert denied.status_code == 404
    service.execution_probe.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["binding", "publication"])
async def test_revoked_company_authority_cannot_bind_or_publish_and_retry_preserves_intent(instances, monkeypatch, phase):
    from deerflow.spaces.contract import ResolvedPrincipal

    service, _files, sf, _catalog = instances
    policy(service)
    human = service.directory.human
    revoked = [False]

    async def current(reference):
        return ResolvedPrincipal(ALICE, False) if reference == ALICE and revoked[0] else await human(reference)

    service.directory.human = current
    method = "_bind_home" if phase == "binding" else "_finish"
    original = getattr(service, method)

    async def demote(*args):
        revoked[0] = True
        return await original(*args)

    monkeypatch.setattr(service, method, demote)
    creation_id = operation()
    with pytest.raises(AgentDenied, match="provisioning authority"):
        await create(service, creation_id=creation_id, custody="company")
    async with sf() as session:
        row = (await session.scalars(select(AgentInstanceRow))).one()
        assert row.status == "provisioning"
        assert (row.home_id is None) == (phase == "binding")
        retained_id, retained_home = row.id, row.home_id
    revoked[0] = False
    monkeypatch.setattr(service, method, original)
    ready = await create(service, creation_id=creation_id, custody="company")
    assert ready.id == retained_id and ready.status == "active"
    if retained_home is not None:
        assert ready.home_id == retained_home


@pytest.mark.asyncio
@pytest.mark.parametrize("reason", ["operator-hold", "storage-unavailable", "agents-none", "space-unverified"])
@pytest.mark.parametrize("action", ["suspend", "delete", "abandon"])
async def test_lifecycle_checks_manage_then_execution_and_allows_unverified_delete(instances, monkeypatch, reason, action):
    import httpx

    from deerflow.agent_instances.lifecycle import InstanceLifecycle

    service, _files, _sf, _catalog = instances
    instance = await create(service)
    service.execution_probe = AsyncMock(return_value=SimpleNamespace(execution="unavailable", reason=reason))
    result = {"complete": True, "instance": instance}
    effect = AsyncMock(return_value=result)
    monkeypatch.setattr(InstanceLifecycle, "abandon" if action == "abandon" else "change", effect)
    suffix = "lifecycle/abandon" if action == "abandon" else "lifecycle"
    body = {"operation_id": operation(), "generation": instance.generation}
    if action != "abandon":
        body["action"] = action
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=http_app(service)), base_url="http://test") as client:
        response = await client.post(f"/api/agent-instances/{instance.id}/{suffix}", json=body)
    if reason == "space-unverified" and action in {"delete", "abandon"}:
        assert response.status_code == 200, response.text
        effect.assert_awaited_once()
    else:
        assert response.status_code == 409, response.text
        assert response.json()["execution_reason"] == reason
        effect.assert_not_awaited()
    service.execution_probe.reset_mock()
    effect.reset_mock()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=http_app(service, actor=BOB)), base_url="http://test") as client:
        hidden = await client.post(f"/api/agent-instances/{instance.id}/{suffix}", json=body)
    assert hidden.status_code == 404
    service.execution_probe.assert_not_awaited()
    effect.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["archive", "delete", "backup", "restore", "recovery"])
async def test_space_routes_preserve_agent_ownership_and_allow_recovery(instances, monkeypatch, action):
    import httpx
    from test_storage_spaces_routes import request_app

    from app.gateway.refusals import refusal_handler
    from deerflow.features.resources import StorageFeatureLinkRow
    from deerflow.spaces.recovery import SpaceRecovery

    service, files, sf, _catalog = instances
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(StorageFeatureLinkRow.__table__.create)
    instance = await create(service)
    home = await files.registry.get(actor=ALICE, space_id=instance.home_id)
    effect = AsyncMock(return_value=home)
    monkeypatch.setattr(SpaceRecovery, "accept_current_state" if action == "recovery" else action, effect)
    app = request_app(files)
    app.state.agent_instances = service
    app.add_exception_handler(Refusal, refusal_handler)
    body = {"operation_id": operation(), "generation": home.generation}
    if action == "recovery":
        body["acknowledge_uncertain_outcome"] = True
    else:
        body["action"] = action
    if action == "restore":
        body["backup_id"] = operation()
    suffix = "recovery" if action == "recovery" else "lifecycle"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(f"/api/spaces/{home.id}/{suffix}", json=body)
        if action in {"archive", "delete"}:
            assert response.status_code == 409, response.text
            assert response.json()["reason"] == "owned-space"
            effect.assert_not_awaited()
            async with sf() as session, session.begin():
                (await session.get(AgentInstanceRow, instance.id)).status = "deleted"
            response = await client.post(f"/api/spaces/{home.id}/{suffix}", json=body)
        assert response.status_code == 200, response.text
        effect.assert_awaited_once()
