"""Periodic assignment repairs current accounts without changing seat admission."""

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
import pytest_asyncio
from sqlalchemy import select
from test_agent_instances import instances as instances
from test_compose_account_seats import seated_accounts as seated_accounts
from test_storage_spaces_startup import startup as startup

from app.gateway import storage_spaces as ss
from app.gateway.auth.models import User
from deerflow.config.storage_spaces_config import StoragePlan, StorageSpacesConfig
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.principals import HostPrincipalResolver


@pytest_asyncio.fixture
async def maintenance(seated_accounts):
    repo, sf, files, catalog = seated_accounts
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(StorageFeatureLinkRow.__table__.create)
    for spec, kind in zip(catalog.volumes.values(), ["user", "assistant", "employee", "company"], strict=True):
        spec.kind = kind
    files.registry._resolver = HostPrincipalResolver(human=ss.human_lookup(repo))
    config = StorageSpacesConfig(enabled=True, inventory_path="/fixture/inventory", plan=StoragePlan(users=1, user_gib=1, ai_assistants_per_user=1, ai_assistant_gib=1, ai_employees=1, ai_employee_gib=1, company_gib=1))
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, storage_spaces_config=config, storage_spaces_enabled=True, agent_execution=ss.ExecutionStatus(host_id="fixture-host")))
    try:
        yield app, repo, sf
    finally:
        await ss.close_storage_spaces(app)


@pytest.mark.asyncio
async def test_oldest_account_gets_user_slot_and_company_grants_repair_independently(maintenance):
    app, repo, sf = maintenance
    younger = await repo.create_user(User(email="younger@example.com"))
    older = await repo.create_user(User(email="older@example.com", created_at=datetime.now(UTC) - timedelta(days=1)))
    assert await ss.assign_spaces(app) == (0, 0)  # Company must exist first.
    await ss.ensure_company_space(app)
    assert await ss.assign_spaces(app) == (1, 2)
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(older.id))) is not None
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(younger.id))) is None
        company = await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))
        assert {row.subject_id for row in await session.scalars(select(SpaceGrantRow).where(SpaceGrantRow.space_id == company.space_id))} == {str(older.id), str(younger.id)}
    assert await ss.assign_spaces(app) == (0, 0)


@pytest.mark.asyncio
async def test_released_and_refused_accounts_never_receive_storage(maintenance):
    app, repo, sf = maintenance
    released = await repo.create_user(User(email="released@example.com"))
    await repo.create_user(User(email="disabled@example.com", oauth_provider="sso", oauth_id="disabled", oauth_issuer="https://identity.example.com"))
    await repo.disable_identity("https://identity.example.com", "disabled")
    async with sf() as session, session.begin():
        (await session.get(UserRow, str(released.id))).released_at = datetime.now(UTC)
    await ss.ensure_company_space(app)
    assert await ss.assign_spaces(app) == (0, 0)
    async with sf() as session:
        assert not (await session.scalars(select(SpaceGrantRow))).all()


@pytest.mark.asyncio
async def test_transient_assignment_failure_retries_but_does_not_skip_company_grant(maintenance, monkeypatch):
    app, repo, _sf = maintenance
    await repo.create_user(User(email="current@example.com"))
    await ss.ensure_company_space(app)
    original = app.state.storage_spaces.assign

    async def fail(**kwargs):
        raise OSError("fixture transient allocation failure")

    monkeypatch.setattr(app.state.storage_spaces, "assign", fail)
    assert await ss.assign_spaces(app) == (0, 1)
    monkeypatch.setattr(app.state.storage_spaces, "assign", original)
    assert await ss.assign_spaces(app) == (1, 0)


@pytest.mark.asyncio
async def test_maintenance_timeout_reuses_owned_task_and_shutdown_drains(maintenance, monkeypatch):
    app, _repo, _sf = maintenance
    entered, resume = asyncio.Event(), asyncio.Event()
    calls = []

    async def blocked(_app):
        calls.append(True)
        entered.set()
        await resume.wait()
        return 0, 0

    monkeypatch.setattr(ss, "assign_spaces", blocked)
    assert not await ss._maintenance_step(app, timeout=0.01)
    await entered.wait()
    owned = app.state.agent_execution.maintenance_task
    assert not await ss._maintenance_step(app, timeout=0)
    assert app.state.agent_execution.maintenance_task is owned and calls == [True]
    stop = asyncio.create_task(ss.close_storage_spaces(app))
    try:
        await asyncio.sleep(0.02)
        assert not stop.done() and not owned.cancelled()
    finally:
        resume.set()
        await stop
    assert owned.done()


@pytest.mark.asyncio
async def test_pass_continues_settlement_after_assignment_failure(maintenance, monkeypatch):
    app, _repo, _sf = maintenance
    calls = []

    async def fail(_app):
        raise RuntimeError("fixture assignment step failure")

    async def settle():
        calls.append(True)
        return 3

    monkeypatch.setattr(ss, "assign_spaces", fail)
    monkeypatch.setattr(app.state.storage_spaces, "settle_interrupted", settle)
    assert await ss._maintenance_step(app, timeout=5)
    assert calls == [True] and app.state.agent_execution.file_operations_settled == 3


@pytest.mark.asyncio
async def test_completed_background_work_does_not_skip_the_next_periodic_pass(maintenance, monkeypatch):
    app, _repo, _sf = maintenance
    resume = asyncio.Event()
    calls = []

    async def work(_app):
        calls.append(True)
        await resume.wait()
        return 0, 0

    monkeypatch.setattr(ss, "assign_spaces", work)
    await ss._maintenance_step(app, timeout=0)
    first = app.state.agent_execution.maintenance_task
    assert first is not None
    resume.set()
    await first
    await ss._maintenance_step(app, timeout=5)
    assert calls == [True, True]


@pytest.mark.asyncio
async def test_feature_callbacks_bind_when_services_start_before_storage(startup):
    from deerflow_extension_api import ExtensionRuntimeDeps

    from deerflow.extensions.loader import load_extensions
    from deerflow.features.plugins import with_default_features
    from deerflow.spaces.facade import HostStorageProvider

    storage, app, config, sf, _catalog, _adapter, _agents = startup
    loaded, diagnostics = load_extensions(with_default_features([]))
    assert not diagnostics
    app.state.extensions = loaded
    provider = HostStorageProvider(lambda: getattr(app.state, "storage_spaces", None))
    for _, service in loaded.services:
        await service.start(ExtensionRuntimeDeps(app_store=loaded.app_store, session_factory=sf, storage=provider))
    try:
        await storage.initialize_storage_spaces(app, config, session_factory=sf)
        assert set(app.state.storage_spaces.workflow_settlers) == {"hm.my-files", "hm.projects", "hm.shared"}
    finally:
        for _, service in loaded.services:
            await service.stop()
    assert not app.state.storage_spaces.workflow_settlers


@pytest.mark.asyncio
async def test_equal_creation_times_use_stable_account_id_order(maintenance):
    app, repo, sf = maintenance
    created = datetime.now(UTC)
    higher = await repo.create_user(User(id=UUID("ffffffff-ffff-4fff-8fff-ffffffffffff"), email="higher@example.com", created_at=created))
    lower = await repo.create_user(User(id=UUID("00000000-0000-4000-8000-000000000001"), email="lower@example.com", created_at=created))
    await ss.ensure_company_space(app)
    assert await ss.assign_spaces(app) == (1, 2)
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(lower.id))) is not None
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(higher.id))) is None


@pytest.mark.asyncio
async def test_release_after_account_snapshot_refuses_both_storage_changes(maintenance, monkeypatch):
    app, repo, sf = maintenance
    account = await repo.create_user(User(email="released-race@example.com"))
    await ss.ensure_company_space(app)
    original = app.state.storage_spaces.assign

    async def release_then_assign(**kwargs):
        async with sf() as session, session.begin():
            (await session.get(UserRow, str(account.id))).released_at = datetime.now(UTC)
        return await original(**kwargs)

    monkeypatch.setattr(app.state.storage_spaces, "assign", release_then_assign)
    assert await ss.assign_spaces(app) == (0, 0)
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(account.id))) is None
        assert not (await session.scalars(select(SpaceGrantRow))).all()


@pytest.mark.asyncio
async def test_startup_records_committed_file_settlement_count(startup, caplog):
    import logging

    from _storage_spaces_test_support import ALICE, operation

    from deerflow.persistence.spaces.files import SpaceFileOperationRow
    from deerflow.spaces.contract import Custody, MutationMode

    storage, app, config, sf, _catalog, _adapter, agents = startup
    identifier = await agents.files.assign(kind="user", actor=ALICE, name="Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    space = await agents.files.registry.get(actor=ALICE, space_id=identifier)
    async with sf() as session, session.begin():
        session.add(SpaceFileOperationRow(space_id=space.id, operation_id=operation(), actor_kind="human", actor_id="alice", generation=space.generation, phase="pending", request={"action": "remove", "path": "absent"}))
    caplog.set_level(logging.INFO, logger=storage.__name__)
    await storage.initialize_storage_spaces(app, config, session_factory=sf)
    assert app.state.agent_execution.file_operations_settled == 1
    assert any(record.message == "storage: file operations settled 1" for record in caplog.records)
