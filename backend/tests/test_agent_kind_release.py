"""Account release owns real AI assistant metadata; native cleanup remains separate."""

import asyncio
from contextvars import ContextVar

import pytest
from _storage_spaces_test_support import operation
from sqlalchemy import select
from test_compose_user_assignment import account_storage as account_storage
from test_compose_user_assignment import seated_accounts as seated_accounts

from app.gateway import storage_spaces as ss
from app.gateway.auth.accounts import AccountsCommand
from app.gateway.auth.models import User
from deerflow.agent_instances.contract import AgentDenied, DefinitionSnapshot
from deerflow.agent_instances.directory import InstanceDirectory
from deerflow.agent_instances.service import AgentInstances
from deerflow.config import agents_api_config
from deerflow.config.agents_api_config import AgentsApiConfig
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import InvalidPrincipal, PrincipalRef, SpaceConflict
from deerflow.spaces.principals import HostPrincipalResolver


async def agents(account_storage, monkeypatch):
    app, repo, sf, files, catalog, _company = account_storage
    monkeypatch.setattr(agents_api_config, "_agents_api_config", AgentsApiConfig(enabled=True, ai_assistants_per_user=10, ai_employees=10))
    async with sf.kw["bind"].begin() as connection:
        for model in (AgentDefinitionRevisionRow, AgentInstanceRow, AgentInstanceGrantRow, AgentLifecycleRow):
            await connection.run_sync(model.__table__.create, checkfirst=True)
    for spec, kind in zip(catalog.volumes.values(), ("assistant", "assistant", "employee", "company"), strict=True):
        spec.kind = kind
    human = ss.human_lookup(repo)
    directory = InstanceDirectory(sf, human=human)
    files.registry._resolver = HostPrincipalResolver(human=human, nonhuman=directory.lookup, nonhuman_in_session=directory.lookup_in_session)
    service = AgentInstances(files, directory)
    app.state.agent_instances = service
    person = await repo.create_user(User(email="owner@example.com", password_hash="fixture", system_role="admin"))
    actor = PrincipalRef("human", str(person.id))

    async def create(custody="personal", *, creation_id=None):
        snapshot = DefinitionSnapshot.capture(owner_id=actor.subject_id, config={"name": "analyst", "skills": [], "mcp_plugins": []}, soul="Analyze.")
        return await service.create(actor=actor, creation_id=creation_id or operation(), name="Analyst", custody=custody, supervisor=actor, definition=snapshot)

    return person, create


@pytest.mark.asyncio
async def test_release_marks_all_personal_ai_assistants_and_their_spaces_but_preserves_company(account_storage, monkeypatch):
    _app, repo, sf, _files, _catalog, _company = account_storage
    person, create = await agents(account_storage, monkeypatch)
    first, second, employee = await create(), await create(), await create("company")
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", account_id=str(person.id))
    result = await command.run("release-seat", account_id=str(person.id))
    assert result["assistants"] == 2
    async with sf() as session:
        for assistant in (first, second):
            assert (await session.get(AgentInstanceRow, assistant.id)).status == "deleted"
            assert (await session.get(SpaceRow, assistant.home_id)).status == "deleted"
            events = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == assistant.home_id, SpaceEventRow.action == "delete"))).all()
            assert len(events) == 1 and events[0].details["cause"] == "seat-released"
            assert events[0].details["retained_backups"] is False
        assert (await session.get(AgentInstanceRow, employee.id)).status == "active"
        assert (await session.get(SpaceRow, employee.home_id)).status == "active"
    again = await command.run("release-seat", account_id=str(person.id))
    assert again["assistants"] == 2
    async with sf() as session:
        assert len((await session.scalars(select(SpaceEventRow).where(SpaceEventRow.action == "delete"))).all()) == 2


@pytest.mark.asyncio
async def test_release_includes_unfinished_personal_enrollment_without_a_home(account_storage, monkeypatch):
    app, repo, sf, _files, _catalog, _company = account_storage
    person, create = await agents(account_storage, monkeypatch)

    async def unavailable(*_args):
        raise OSError("fixture binding interrupted")

    monkeypatch.setattr(app.state.agent_instances, "_bind_home", unavailable)
    with pytest.raises(OSError):
        await create()
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", account_id=str(person.id))
    result = await command.run("release-seat", account_id=str(person.id))
    assert result["assistants"] == 1
    async with sf() as session:
        assert (await session.scalars(select(AgentInstanceRow))).all() == []
        assert (await session.scalars(select(AgentInstanceGrantRow))).all() == []
    assert (await command.run("release-seat", account_id=str(person.id)))["assistants"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["enrollment", "binding"])
async def test_release_fences_an_enrollment_or_binding_that_observed_an_enabled_account(account_storage, monkeypatch, phase):
    app, repo, sf, _files, _catalog, _company = account_storage
    person, create = await agents(account_storage, monkeypatch)
    service = app.state.agent_instances
    entered, proceed = asyncio.Event(), asyncio.Event()
    method = "_enroll" if phase == "enrollment" else "_bind_home"
    original = getattr(service, method)

    async def paused(*args):
        entered.set()
        await proceed.wait()
        return await original(*args)

    monkeypatch.setattr(service, method, paused)
    creation = asyncio.create_task(create())
    try:
        await asyncio.wait_for(entered.wait(), 5)
        command = AccountsCommand(repo, tokens=None, schedules=None)
        await command.run("disable", account_id=str(person.id))
        released = await command.run("release-seat", account_id=str(person.id))
        assert released["assistants"] == (0 if phase == "enrollment" else 1)
        proceed.set()
        with pytest.raises((AgentDenied, InvalidPrincipal)):
            await asyncio.wait_for(creation, 5)
        async with sf() as session:
            rows = (await session.scalars(select(AgentInstanceRow))).all()
            assert rows == []
    finally:
        proceed.set()
        await asyncio.gather(creation, return_exceptions=True)


@pytest.mark.asyncio
async def test_generation_failure_rolls_back_account_and_all_assistant_deletions(account_storage, monkeypatch):
    _app, repo, sf, _files, _catalog, _company = account_storage
    person, create = await agents(account_storage, monkeypatch)
    first, second = await create(), await create()
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceRow, second.id)).generation = 2**31 - 1
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", account_id=str(person.id))
    with pytest.raises(SpaceConflict, match="generation"):
        await command.run("release-seat", account_id=str(person.id))
    async with sf() as session:
        assert (await session.get(UserRow, str(person.id))).released_at is None
        for instance in (first, second):
            assert (await session.get(AgentInstanceRow, instance.id)).status == "active"
            assert (await session.get(SpaceRow, instance.home_id)).status == "active"
        assert (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.action == "delete"))).all() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["enrollment", "binding"])
async def test_postgres_release_waits_for_allocation_before_snapshot_and_parent_locks(account_storage, monkeypatch, phase):
    from deerflow.spaces.service import SpaceFiles

    app, repo, sf, _files, _catalog, _company = account_storage
    if sf.kw["bind"].dialect.name != "postgresql":
        pytest.skip("PostgreSQL allocation advisory-lock ordering")
    person, create = await agents(account_storage, monkeypatch)
    service = app.state.agent_instances
    selected = ContextVar("release_test_phase", default=False)
    release_selected = ContextVar("release_test_operation", default=False)
    entered, proceed, release_waiting = asyncio.Event(), asyncio.Event(), asyncio.Event()
    authority = service._creation_authority
    method = "_enroll" if phase == "enrollment" else "_bind_home"
    original = getattr(service, method)
    paused = [False]

    async def within_phase(*args):
        token = selected.set(True)
        try:
            return await original(*args)
        finally:
            selected.reset(token)

    async def after_authority(*args):
        await authority(*args)
        if selected.get() and not paused[0]:
            paused[0] = True
            entered.set()
            await proceed.wait()

    allocation = SpaceFiles._serialize_allocation

    async def observed_allocation(session):
        if release_selected.get():
            release_waiting.set()
        await allocation(session)

    monkeypatch.setattr(service, method, within_phase)
    monkeypatch.setattr(service, "_creation_authority", after_authority)
    monkeypatch.setattr(SpaceFiles, "_serialize_allocation", staticmethod(observed_allocation))
    command = AccountsCommand(repo, tokens=None, schedules=None)
    creation = asyncio.create_task(create())
    releasing = None

    async def release():
        token = release_selected.set(True)
        try:
            return await command.run("release-seat", account_id=str(person.id))
        finally:
            release_selected.reset(token)

    try:
        await asyncio.wait_for(entered.wait(), 10)
        await asyncio.wait_for(command.run("disable", account_id=str(person.id)), 10)
        releasing = asyncio.create_task(release())
        await asyncio.wait_for(release_waiting.wait(), 10)
        assert not releasing.done()
        proceed.set()
        outcomes = await asyncio.wait_for(asyncio.gather(creation, releasing, return_exceptions=True), 10)
        assert isinstance(outcomes[0], (AgentDenied, InvalidPrincipal))
        assert not isinstance(outcomes[1], BaseException), outcomes[1]
        async with sf() as session:
            rows = (await session.scalars(select(AgentInstanceRow))).all()
            assert all(row.status == "deleted" for row in rows)
            assert (await session.get(UserRow, str(person.id))).released_at is not None
    finally:
        proceed.set()
        await asyncio.gather(creation, *([releasing] if releasing is not None else []), return_exceptions=True)


@pytest.mark.asyncio
async def test_released_assistant_slot_stays_charged_until_offline_containment_proves_absence(account_storage, monkeypatch):
    from test_compose_user_reclamation import ContainmentFixture

    app, repo, sf, files, catalog, _company = account_storage
    person, create = await agents(account_storage, monkeypatch)
    assistant = await create()
    async with sf() as session:
        home = await session.get(SpaceRow, assistant.home_id)
        handle = home.backing_handle
        slot = (await session.get(SpaceBackingRow, handle)).slot_id
    volume = catalog.verified[slot]
    (volume.data_path / "retained.txt").write_bytes(b"old assistant data")
    provider = app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    monkeypatch.setattr("app.gateway.storage_reclamation.RETIREMENT_WAIT_SECONDS", 0.01)
    reference = provider.gate.enter("space:" + assistant.home_id)
    provider.gate.register_slots(reference, (slot,))
    command = AccountsCommand(repo, tokens=None, schedules=None)
    await command.run("disable", account_id=str(person.id))
    assert (await command.run("release-seat", account_id=str(person.id)))["assistants"] == 1
    try:
        assert await ss.finish_releases(app) == 0
        assert (volume.data_path / "retained.txt").read_bytes() == b"old assistant data"
    finally:
        provider.gate.end(reference)
    async with sf() as session:
        assert await session.get(SpaceBackingRow, handle) is not None
    provider.proof_error = OSError("fixture final absence publication is unconfirmed")
    assert await ss.finish_releases(app) == 0
    async with sf() as session:
        assert await session.get(SpaceBackingRow, handle) is not None
    provider.proof_error = None
    assert await ss.finish_releases(app) == 1
    assert not any(volume.data_path.iterdir())
    async with sf() as session:
        assert await session.get(SpaceBackingRow, handle) is None
        events = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == assistant.home_id, SpaceEventRow.action == "delete"))).all()
        assert len(events) == 1 and events[0].details["retained_backups"] is False
    successor = await repo.create_user(User(email="successor@example.com", password_hash="fixture", system_role="admin"))
    actor = PrincipalRef("human", str(successor.id))
    snapshot = DefinitionSnapshot.capture(owner_id=actor.subject_id, config={"name": "analyst"}, soul="Analyze.")
    next_instance = await app.state.agent_instances.create(actor=actor, creation_id=operation(), name="Successor", custody="personal", supervisor=actor, definition=snapshot)
    async with sf() as session:
        next_home = await session.get(SpaceRow, next_instance.home_id)
        next_slot = (await session.get(SpaceBackingRow, next_home.backing_handle)).slot_id
    assert next_slot == slot and catalog.volumes[next_slot].kind == "assistant"
