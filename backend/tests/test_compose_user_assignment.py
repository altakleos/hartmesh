"""Post-commit account storage is idempotent and failures do not erase accounts."""

import asyncio
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import select
from test_compose_account_seats import seated_accounts as seated_accounts

from app.gateway import storage_spaces as ss
from app.gateway.auth.models import User
from deerflow.config.storage_spaces_config import StoragePlan, StorageSpacesConfig
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import Custody, MutationMode, Permission
from deerflow.spaces.principals import HostPrincipalResolver


@pytest_asyncio.fixture
async def account_storage(seated_accounts, monkeypatch, tmp_path):
    repo, sf, files, catalog = seated_accounts
    monkeypatch.setattr("app.gateway.auth.mode.sign_on_only", lambda: False)
    files.registry._resolver = HostPrincipalResolver(human=ss.human_lookup(repo))
    async with sf.kw["bind"].begin() as connection:
        await connection.run_sync(StorageFeatureLinkRow.__table__.create)
    for spec, kind in zip(catalog.volumes.values(), ("user", "user", "user", "company"), strict=True):
        spec.kind = kind
    company = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
    config = StorageSpacesConfig(
        enabled=True, inventory_path=str(tmp_path / "inventory.v2.json"), user_spaces=False, plan=StoragePlan(users=3, user_gib=1, ai_assistants_per_user=0, ai_assistant_gib=1, ai_employees=0, ai_employee_gib=1, company_gib=1)
    )
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, storage_spaces_enabled=True, storage_spaces_config=config, agent_execution=ss.ExecutionStatus(host_id="qualified-test-host")))
    yield app, repo, sf, files, catalog, company


@pytest.mark.asyncio
async def test_setup_pending_account_gets_one_space_and_read_export_company_grant(account_storage, caplog):
    app, repo, sf, files, catalog, company = account_storage
    account = await repo.create_user(User(email="new@example.com", needs_setup=True))
    caplog.set_level("INFO", logger=ss.__name__)
    await asyncio.gather(*(ss.assign_user_space(app, account) for _ in range(4)))
    async with sf() as session:
        link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(account.id)))
        assert link is not None
        row = await session.get(SpaceRow, link.space_id)
        assert (row.custody_kind, row.custodian_id, row.mode, row.status) == ("personal", str(account.id), "native", "active")
        backing = await session.get(SpaceBackingRow, row.backing_handle)
        volume = catalog.verify(backing.slot_id)
        assert sorted(p.name for p in volume.data_path.iterdir()) == ["conversations", "files", "projects"]
        assert len((await session.execute(select(SpaceRow))).scalars().all()) == 2
        grant = await session.get(SpaceGrantRow, (company, "human", str(account.id)))
        assert grant.permissions == int(Permission.READ | Permission.EXPORT)
        assert len((await session.execute(select(SpaceEventRow).where(SpaceEventRow.space_id == company))).scalars().all()) == 1
    assert await repo.get_user_by_id(str(account.id)) is not None
    grants = [record.getMessage() for record in caplog.records if "company grant written" in record.getMessage()]
    assert grants == ["storage: company grant written for " + str(account.id)]


@pytest.mark.asyncio
async def test_personal_assignment_failure_still_attempts_company_grant(account_storage, monkeypatch, caplog):
    app, repo, sf, files, _catalog, company = account_storage
    account = await repo.create_user(User(email="new@example.com"))

    async def fail(**_values):
        raise OSError("fixture personal assignment unavailable")

    monkeypatch.setattr(files, "assign", fail)
    await ss.assign_user_space(app, account)
    assert await repo.get_user_by_id(str(account.id)) is not None
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(account.id))) is None
        assert (await session.get(SpaceGrantRow, (company, "human", str(account.id)))).permissions == 17
    assert "assignment" in caplog.text


@pytest.mark.asyncio
async def test_company_grant_failure_preserves_personal_assignment(account_storage, monkeypatch):
    app, repo, sf, _files, _catalog, _company = account_storage
    account = await repo.create_user(User(email="new@example.com"))

    async def fail(*_args):
        raise OSError("fixture company grant unavailable")

    monkeypatch.setattr(ss, "grant_company_space", fail)
    await ss.assign_user_space(app, account)
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(account.id))) is not None
    assert await repo.get_user_by_id(str(account.id)) is not None


@pytest.mark.asyncio
async def test_released_account_never_gets_a_new_assignment_or_company_grant(account_storage):
    from datetime import UTC, datetime

    app, repo, sf, _files, _catalog, company = account_storage
    account = await repo.create_user(User(email="new@example.com"))
    async with sf() as session, session.begin():
        (await session.get(UserRow, str(account.id))).released_at = datetime.now(UTC)
    await ss.assign_user_space(app, account)
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(account.id))) is None
        assert await session.get(SpaceGrantRow, (company, "human", str(account.id))) is None
        assert len((await session.execute(select(SpaceRow))).scalars().all()) == 1
