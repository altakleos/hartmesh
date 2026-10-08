"""Persistent agent binding on real SQL/storage APIs; no native quota claims."""

import asyncio
import uuid

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, make_storage_fixture, operation
from sqlalchemy import func, select

from deerflow.agent_instances.contract import AgentConflict, AgentDenied, AgentPermission, DefinitionSnapshot
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.agent_instances.service import AgentInstances
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow
from deerflow.spaces.contract import InvalidPrincipal, Permission, PrincipalRef, SpaceDenied
from deerflow.spaces.principals import HostPrincipalResolver


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def instances(tmp_path, request):
    async for files, sf, catalog in make_storage_fixture(tmp_path, request):
        async with sf.kw["bind"].begin() as connection:
            await connection.run_sync(lambda c: AgentDefinitionRevisionRow.__table__.create(c))
            await connection.run_sync(lambda c: AgentInstanceRow.__table__.create(c))
            await connection.run_sync(lambda c: AgentInstanceGrantRow.__table__.create(c))
        human = files.registry._resolver._human
        directory = InstanceDirectory(sf, human=human)
        files.registry._resolver = HostPrincipalResolver(human=human, nonhuman=directory.lookup)
        yield AgentInstances(files, directory), files, sf, catalog


def definition(owner="alice", name="analyst", soul="Help with analysis."):
    return DefinitionSnapshot.capture(owner_id=owner, config={"name": name, "skills": [], "mcp_plugins": []}, soul=soul)


async def create(service, *, actor=ALICE, creation_id=None, name="Analyst", custody="personal", supervisor=None, snapshot=None):
    return await service.create(actor=actor, creation_id=creation_id or operation(), name=name, custody=custody, supervisor=supervisor or actor, definition=snapshot or definition())


@pytest.mark.asyncio
async def test_two_instances_from_one_definition_have_independent_persistent_principals_and_homes(instances):
    service, files, sf, _ = instances
    first = await create(service)
    second = await create(service)
    assert first.id != second.id and first.principal != second.principal and first.home_id != second.home_id
    assert first.definition_revision == second.definition_revision
    assert first.principal == PrincipalRef("nonhuman", "agent:" + first.id)
    assert first.status == second.status == "active"
    assert first.custody == "personal" and first.owner_id == "alice"
    first_home = await files.registry.get(actor=first.principal, space_id=first.home_id)
    assert first_home.custody.principal == ALICE
    assert first_home.permissions == Permission.READ | Permission.WRITE | Permission.EXPORT
    await files.write(actor=first.principal, space_id=first.home_id, expected_generation=first_home.generation, operation_id=operation(), path=".notes", content=b"first", create=True)
    with pytest.raises(SpaceDenied):
        await files.read(actor=second.principal, space_id=first.home_id, path=".notes", max_bytes=20)
    # Restarting the consumer keeps all identities without a chat or sandbox.
    restarted = AgentInstances(files, service.directory)
    retained = await restarted.get(actor=ALICE, instance_id=first.id)
    assert retained.principal == first.principal and retained.home_id == first.home_id
    assert await files.read(actor=retained.principal, space_id=retained.home_id, path=".notes", max_bytes=20) == b"first"
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentDefinitionRevisionRow)) == 1


@pytest.mark.asyncio
async def test_rename_is_generation_fenced_and_preserves_adopted_definition_and_home(instances):
    service, _, _, _ = instances
    agent = await create(service)
    renamed = await service.rename(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, name="研究助手")
    assert renamed.name == "研究助手" and renamed.generation == agent.generation + 1
    assert (renamed.id, renamed.principal, renamed.home_id, renamed.definition_revision) == (agent.id, agent.principal, agent.home_id, agent.definition_revision)
    with pytest.raises(AgentConflict):
        await service.rename(actor=ALICE, instance_id=agent.id, expected_generation=agent.generation, name="stale")
    with pytest.raises(AgentDenied):
        await service.rename(actor=BOB, instance_id=agent.id, expected_generation=renamed.generation, name="other")


@pytest.mark.asyncio
async def test_company_custody_and_supervision_are_explicit_without_creator_impersonation(instances):
    service, files, sf, _ = instances
    agent = await create(service, custody="company", supervisor=BOB)
    assert agent.owner_id is None and agent.creator_id == "alice" and agent.supervisor == BOB
    assert (await service.get(actor=BOB, instance_id=agent.id)).permissions == AgentPermission.USE | AgentPermission.INSPECT | AgentPermission.MANAGE
    home = await files.registry.get(actor=BOB, space_id=agent.home_id)
    assert home.custody.kind == "company" and home.custody.principal is None
    changed = await service.rename(actor=BOB, instance_id=agent.id, expected_generation=agent.generation, name="Company analyst")
    assert changed.home_id == agent.home_id
    with pytest.raises(AgentDenied):
        await create(service, actor=BOB, custody="company", snapshot=definition("bob"))
    async with sf() as session:
        row = await session.get(AgentInstanceRow, agent.id)
        assert row.owner_id is None and row.principal_id != row.creator_id


@pytest.mark.asyncio
async def test_nonhuman_directory_rechecks_instance_without_synthesizing_a_user(instances):
    service, files, sf, _ = instances
    agent = await create(service)
    resolved = await service.directory.lookup(agent.principal)
    assert resolved.reference == agent.principal and not resolved.can_provision_company
    assert await service.directory.lookup(PrincipalRef("human", agent.principal.subject_id)) is None
    assert await service.directory.lookup(PrincipalRef("nonhuman", "agent:" + uuid.uuid4().hex)) is None
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, agent.id)).status = "suspended"
    assert await service.directory.lookup(agent.principal) is None
    with pytest.raises(InvalidPrincipal):
        await files.registry.get(actor=agent.principal, space_id=agent.home_id)


@pytest.mark.asyncio
async def test_same_creation_id_recovers_the_same_home_after_a_grant_failure(instances, monkeypatch):
    service, files, sf, _ = instances
    key = operation()
    original = files.registry.set_grant
    calls = 0

    async def lost_ack(**kwargs):
        nonlocal calls
        value = await original(**kwargs)
        calls += 1
        if calls == 1:
            raise OSError("acknowledgement lost after grant commit")
        return value

    monkeypatch.setattr(files.registry, "set_grant", lost_ack)
    with pytest.raises(OSError):
        await create(service, creation_id=key)
    pending = (await service.list(actor=ALICE))[0]
    assert pending.status == "provisioning" and pending.home_id is not None
    assert await service.directory.lookup(pending.principal) is None
    ready = await create(service, creation_id=key)
    assert ready.status == "active" and ready.id == pending.id and ready.home_id == pending.home_id
    again = await create(service, creation_id=key)
    assert again.id == ready.id and again.generation == ready.generation
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentInstanceRow)) == 1
    with pytest.raises(AgentConflict):
        await create(service, creation_id=key, name="different")


@pytest.mark.asyncio
async def test_concurrent_identical_creation_does_not_allocate_two_homes(instances):
    service, _, sf, _ = instances
    key = operation()
    outcomes = await asyncio.gather(create(service, creation_id=key), create(service, creation_id=key))
    assert outcomes[0].id == outcomes[1].id and outcomes[0].home_id == outcomes[1].home_id
    assert all(value.status == "active" for value in outcomes)
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentInstanceRow)) == 1


@pytest.mark.asyncio
async def test_definition_is_an_adopted_snapshot_not_mutable_requester_configuration(instances):
    service, _, _, _ = instances
    snapshot = definition(soul="Original")
    agent = await create(service, snapshot=snapshot)
    newer = definition(soul="Changed later")
    assert newer.revision != agent.definition_revision
    saved = await service.definition(actor=ALICE, instance_id=agent.id)
    assert saved.soul == "Original" and saved.revision == snapshot.revision
    with pytest.raises(AgentDenied):
        await service.definition(actor=BOB, instance_id=agent.id)
    with pytest.raises(AgentDenied):
        await create(service, snapshot=definition(owner="bob"))


@pytest.mark.asyncio
async def test_pending_capacity_never_becomes_an_active_unbacked_agent(instances):
    service, files, _, catalog = instances
    catalog.volumes.clear()
    from deerflow.spaces.contract import SpaceConflict

    key = operation()
    with pytest.raises(SpaceConflict):
        await create(service, creation_id=key)
    pending = (await service.list(actor=ALICE))[0]
    assert pending.status == "provisioning" and pending.home_id is None
    assert await service.directory.lookup(pending.principal) is None
    assert await files.registry.list(actor=ALICE) == []


@pytest.mark.parametrize("name", ["", " ", "x" * 129, "a\nb"])
@pytest.mark.asyncio
async def test_invalid_creation_inputs_do_not_persist_identity(instances, name):
    service, _, sf, _ = instances
    with pytest.raises(ValueError):
        await create(service, name=name)
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentInstanceRow)) == 0


def request_app(service, *, actor=ALICE, permissions=("agents:read", "agents:write"), source="session"):
    from types import SimpleNamespace

    from fastapi import FastAPI

    from app.gateway.authz import AuthContext
    from app.gateway.routers.agent_instances import router

    app = FastAPI()
    app.state.agent_instances = service
    app.state.storage_spaces = service.files

    @app.middleware("http")
    async def bind(request, call_next):
        user = SimpleNamespace(id=actor.subject_id, system_role="user") if actor else None
        request.state.user = user
        request.state.auth = AuthContext(user, list(permissions))
        request.state.auth_source = source
        return await call_next(request)

    app.include_router(router)
    return app


@pytest.mark.asyncio
async def test_http_adopts_only_host_loaded_definitions_and_reuses_exact_creation_identity(instances, monkeypatch):
    from types import SimpleNamespace

    import httpx

    from app.gateway.routers import agent_instances as routes

    service, _, _, _ = instances
    loaded = []

    async def approved(request, actor, name):
        loaded.append((actor, name))
        return definition(actor.subject_id, name)

    monkeypatch.setattr(routes, "_load_definition", approved)
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    app = request_app(service)
    body = {"creation_id": operation(), "name": "Analyst", "definition_name": "analyst"}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        forged = await client.post("/api/agent-instances", json={**body, "owner_id": "bob", "principal": {"kind": "human", "subject_id": "alice"}})
        assert forged.status_code == 422 and loaded == []
        first = await client.post("/api/agent-instances", json=body)
        assert first.status_code == 201, first.text
        assert first.json()["principal"]["kind"] == "nonhuman" and first.json()["home_id"]

        # A committed retry does not depend on the source definition still existing.
        async def removed(*args):
            raise FileNotFoundError("definition removed")

        monkeypatch.setattr(routes, "_load_definition", removed)
        again = await client.post("/api/agent-instances", json=body)
        assert again.status_code == 201 and again.json() == first.json()
        changed = await client.post("/api/agent-instances", json={**body, "definition_name": "another"})
        assert changed.status_code == 409
        listed = await client.get("/api/agent-instances")
        assert listed.json()["instances"][0]["id"] == first.json()["id"]


@pytest.mark.asyncio
async def test_http_provider_ceiling_and_credential_kind_precede_creation(instances, monkeypatch):
    from types import SimpleNamespace

    import httpx

    from app.gateway.routers import agent_instances as routes

    service, _, _, _ = instances
    monkeypatch.setattr(routes, "get_agents_api_config", lambda: SimpleNamespace(enabled=True))
    body = {"creation_id": operation(), "name": "Analyst", "definition_name": "analyst"}
    for app, status in [(request_app(service, permissions=("agents:read",)), 403), (request_app(service, permissions=("agents:write",)), 403), (request_app(service, actor=None), 401), (request_app(service, source="pat"), 403)]:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.post("/api/agent-instances", json=body)).status_code == status
    assert await service.list(actor=ALICE) == []


@pytest.mark.asyncio
async def test_creator_departure_retires_personal_actor_but_preserves_company_custody(instances):
    service, files, _, _ = instances
    personal = await create(service)
    company = await create(service, custody="company", supervisor=BOB)
    human = service.directory.human

    async def current(reference):
        return None if reference == ALICE else await human(reference)

    service.directory.human = current
    assert await service.directory.lookup(personal.principal) is None
    assert (await service.directory.lookup(company.principal)).reference == company.principal
    retained = await service.get(actor=BOB, instance_id=company.id)
    home = await files.registry.get(actor=company.principal, space_id=retained.home_id)
    assert home.custody.kind == "company"


@pytest.mark.asyncio
async def test_startup_connects_real_instance_directory_to_resource_provider(instances, monkeypatch):
    from types import SimpleNamespace

    from app.gateway import storage_spaces
    from deerflow.config.storage_spaces_config import StorageSpacesConfig

    service, _, sf, catalog = instances
    agent = await create(service)
    monkeypatch.setattr(storage_spaces, "human_lookup", lambda repository: service.directory.human)
    monkeypatch.setattr(storage_spaces.PreparedVolumeCatalog, "from_manifest", lambda path: catalog)
    app = SimpleNamespace(state=SimpleNamespace())
    await storage_spaces.initialize_storage_spaces(app, StorageSpacesConfig(enabled=True, inventory_path="/fixture/inventory.json"), session_factory=sf)
    assert isinstance(app.state.agent_instances, AgentInstances)
    assert (await app.state.storage_spaces.registry._resolver.resolve(agent.principal)).reference == agent.principal
    assert (await app.state.agent_instances.get(actor=ALICE, instance_id=agent.id)).home_id == agent.home_id


@pytest.mark.asyncio
async def test_creation_retry_rejects_corrupted_adopted_definition(instances):
    service, _, sf, _ = instances
    key = operation()
    agent = await create(service, creation_id=key)
    async with sf() as session, session.begin():
        (await session.get(AgentDefinitionRevisionRow, agent.definition_revision)).soul = "unrecorded revision"
    with pytest.raises(AgentConflict):
        await service.creation(actor=ALICE, creation_id=key)


@pytest.mark.asyncio
async def test_ready_creation_retry_does_not_restore_revoked_home_authority(instances):
    service, files, _, _ = instances
    key = operation()
    agent = await create(service, creation_id=key)
    home = await files.registry.get(actor=ALICE, space_id=agent.home_id)
    await files.registry.set_grant(actor=ALICE, space_id=home.id, expected_generation=home.generation, subject=agent.principal, permissions=Permission.READ)
    assert (await create(service, creation_id=key)).home_id == home.id
    assert (await files.registry.get(actor=agent.principal, space_id=home.id)).permissions == Permission.READ


@pytest.mark.asyncio
async def test_cancelled_creation_drains_owned_home_and_grant_writes(instances, monkeypatch):
    service, files, _, _ = instances
    entered, resume = asyncio.Event(), asyncio.Event()
    original = files.registry.set_grant

    async def paused(**kwargs):
        entered.set()
        await resume.wait()
        return await original(**kwargs)

    monkeypatch.setattr(files.registry, "set_grant", paused)
    task = asyncio.create_task(create(service))
    try:
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        resume.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        agent = (await service.list(actor=ALICE))[0]
        assert agent.status == "active"
        assert (await files.registry.get(actor=agent.principal, space_id=agent.home_id)).permissions == Permission.READ | Permission.WRITE | Permission.EXPORT
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_frozen_migration_rebuilds_the_same_tables_on_both_backends(instances):
    from alembic.operations import Operations
    from alembic.runtime.migration import MigrationContext
    from alembic.script import ScriptDirectory
    from sqlalchemy import inspect

    from deerflow.persistence import bootstrap

    service, _, sf, _ = instances
    module = ScriptDirectory(str(bootstrap._MIGRATIONS_DIR)).get_revision("0034_agent_instances").module
    tables = [model.__table__ for model in (AgentDefinitionRevisionRow, AgentInstanceRow, AgentInstanceGrantRow)]

    def columns(connection):
        inspector = inspect(connection)
        return {table.name: [(c["name"], str(c["type"]), c["nullable"], c["default"]) for c in inspector.get_columns(table.name)] for table in tables}

    def rebuild(connection):
        before = columns(connection)
        with Operations.context(MigrationContext.configure(connection)):
            module.upgrade()  # Canonical check/FK validation on ORM-created tables.
            module.downgrade()
            module.upgrade()  # Actual frozen DDL creation, including PostgreSQL.
        assert columns(connection) == before

    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(rebuild)
    assert (await create(service)).status == "active"


@pytest.mark.asyncio
async def test_new_instance_rejects_corrupted_deduplicated_revision_before_enrollment(instances):
    service, files, sf, _ = instances
    snapshot = definition()
    first = await create(service, snapshot=snapshot)
    async with sf() as session, session.begin():
        (await session.get(AgentDefinitionRevisionRow, first.definition_revision)).soul = "unrecorded bytes"
    with pytest.raises(AgentConflict):
        await create(service, snapshot=snapshot)
    async with sf() as session:
        assert await session.scalar(select(func.count()).select_from(AgentInstanceRow)) == 1
    assert len(await files.registry.list(actor=ALICE)) == 1


@pytest.mark.parametrize("malformed", ["id", "revision", "creator", "supervisor", "name"])
@pytest.mark.asyncio
async def test_directory_rejects_sql_valid_but_malformed_persisted_identity(instances, malformed):
    from deerflow.spaces.contract import Custody, MutationMode

    service, files, sf, _ = instances
    first = await create(service)
    home = await files.create(actor=ALICE, name="Malformed binding fixture", custody=Custody.company(), mode=MutationMode.NATIVE)
    key = "short" if malformed == "id" else operation()
    revision = "g" * 64 if malformed == "revision" else first.definition_revision
    async with sf() as session, session.begin():
        if malformed == "revision":
            session.add(AgentDefinitionRevisionRow(revision=revision, owner_id="alice", config={"name": "analyst"}, soul="fixture"))
            await session.flush()
        session.add(
            AgentInstanceRow(
                id=key,
                principal_id="agent:" + key,
                name="bad\nlabel" if malformed == "name" else "fixture",
                custody="company",
                owner_id=None,
                creator_id="bad id" if malformed == "creator" else "alice",
                supervisor_id="bad id" if malformed == "supervisor" else "bob",
                definition_revision=revision,
                home_id=home.id,
                status="active",
                generation=1,
                creation_id=operation(),
                creation_request={},
            )
        )
    principal = PrincipalRef("nonhuman", "agent:" + key)
    assert await service.directory.lookup(principal) is None
    with pytest.raises(InvalidPrincipal):
        await files.registry.set_grant(actor=ALICE, space_id=home.id, expected_generation=home.generation, subject=principal, permissions=Permission.READ, acknowledge_existing_data=True)


@pytest.mark.asyncio
async def test_identical_concurrent_creation_reuses_home_after_last_slot_is_consumed(instances, monkeypatch):
    from contextlib import asynccontextmanager

    service, files, _, catalog = instances
    slot = sorted(catalog.volumes)[0]
    catalog.volumes = {slot: catalog.volumes[slot]}
    follower_arrived, leader_done = asyncio.Event(), asyncio.Event()
    original = files.provisioning
    calls = 0

    @asynccontextmanager
    async def paused(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            await follower_arrived.wait()
        else:
            follower_arrived.set()
            await leader_done.wait()
        async with original(**kwargs) as result:
            yield result

    monkeypatch.setattr(files, "provisioning", paused)
    key = operation()
    leader = asyncio.create_task(create(service, creation_id=key))
    follower = asyncio.create_task(create(service, creation_id=key))
    try:
        ready = await leader
        leader_done.set()
        recovered = await follower
        assert recovered.status == "active" and recovered.id == ready.id and recovered.home_id == ready.home_id
    finally:
        follower_arrived.set()
        leader_done.set()
        await asyncio.gather(leader, follower, return_exceptions=True)


@pytest.mark.parametrize("boundary", ["home-binding", "grant-admission"])
@pytest.mark.asyncio
async def test_stale_creation_follower_never_restores_a_ready_agents_revoked_grant(instances, monkeypatch, boundary):
    from contextlib import asynccontextmanager
    from contextvars import ContextVar

    service, files, _, _ = instances
    role = ContextVar("fixture_creation_role", default=None)
    follower_paused, leader_done = asyncio.Event(), asyncio.Event()
    original_provision = files.provisioning
    original_grant = files.registry.set_grant

    @asynccontextmanager
    async def paused_home(**kwargs):
        if role.get() == "leader":
            await follower_paused.wait()
        else:
            follower_paused.set()
            await leader_done.wait()
        async with original_provision(**kwargs) as result:
            yield result

    async def paused_grant(**kwargs):
        if role.get() == "leader":
            await follower_paused.wait()
        elif role.get() == "follower":
            follower_paused.set()
            await leader_done.wait()
        return await original_grant(**kwargs)

    if boundary == "home-binding":
        monkeypatch.setattr(files, "provisioning", paused_home)
    else:
        monkeypatch.setattr(files.registry, "set_grant", paused_grant)

    key = operation()

    async def named(name):
        token = role.set(name)
        try:
            return await create(service, creation_id=key)
        finally:
            role.reset(token)

    leader, follower = asyncio.create_task(named("leader")), asyncio.create_task(named("follower"))
    try:
        ready = await leader
        home = await files.registry.get(actor=ALICE, space_id=ready.home_id)
        await original_grant(actor=ALICE, space_id=home.id, expected_generation=home.generation, subject=ready.principal, permissions=Permission.READ)
        leader_done.set()
        retained = await follower
        assert retained.id == ready.id and retained.status == "active"
        assert (await files.registry.get(actor=ready.principal, space_id=ready.home_id)).permissions == Permission.READ
    finally:
        follower_paused.set()
        leader_done.set()
        await asyncio.gather(leader, follower, return_exceptions=True)
