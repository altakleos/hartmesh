"""Bundle seat callbacks belong to the creation repository and its transaction."""

import pytest
from test_agent_instances import instances as instances
from test_compose_account_seats import seated_accounts as seated_accounts
from test_storage_spaces_startup import startup as startup

from app.gateway.auth.repositories.sqlite import SQLiteUserRepository


@pytest.mark.asyncio
async def test_storage_installs_and_clears_only_its_creation_repository_seat_callback(startup, monkeypatch):

    ss, app, config, sf, _catalog, _adapter, _agents = startup
    repo = SQLiteUserRepository(sf)
    await ss.initialize_storage_spaces(app, config, session_factory=sf, creation_repository=repo)
    assert repo.seats_allowed is not None
    async with sf() as session, session.begin():
        assert await repo.seats_allowed(session) == 1
    await ss.close_storage_spaces(app)
    assert repo.seats_allowed is None


@pytest.mark.asyncio
async def test_missing_pool_keeps_bundle_seat_admission_installed_at_zero(startup, monkeypatch):

    ss, app, config, sf, _catalog, _adapter, _agents = startup
    repo = SQLiteUserRepository(sf)

    def missing(_path):
        raise FileNotFoundError("fixture pool missing")

    monkeypatch.setattr(ss.PreparedVolumeCatalog, "from_manifest", missing)
    await ss.initialize_storage_spaces(app, config, session_factory=sf, creation_repository=repo)
    assert (await ss.storage_state(app)).state == "unavailable"
    assert repo.seats_allowed is not None
    async with sf() as session, session.begin():
        assert await repo.seats_allowed(session) == 0


@pytest.mark.asyncio
async def test_legacy_unplanned_storage_has_no_bundle_seat_callback(startup, monkeypatch):

    ss, app, config, sf, _catalog, _adapter, _agents = startup
    repo = SQLiteUserRepository(sf)
    await ss.initialize_storage_spaces(app, config.model_copy(update={"plan": None}), session_factory=sf, creation_repository=repo)
    assert repo.seats_allowed is None


@pytest.mark.asyncio
async def test_storage_teardown_does_not_clear_another_owners_replacement_callback(startup, monkeypatch):

    ss, app, config, sf, _catalog, _adapter, _agents = startup
    repo = SQLiteUserRepository(sf)
    await ss.initialize_storage_spaces(app, config, session_factory=sf, creation_repository=repo)

    async def replacement(_session):
        return 99

    repo.seats_allowed = replacement
    await ss.close_storage_spaces(app)
    assert repo.seats_allowed is replacement


@pytest.mark.asyncio
async def test_generic_assignment_rejection_precedes_any_layout(startup, monkeypatch):
    from _storage_spaces_test_support import ALICE
    from sqlalchemy import select

    from deerflow.persistence.spaces.files import SpaceBackingRow
    from deerflow.persistence.spaces.model import SpaceRow
    from deerflow.spaces.contract import Custody, MutationMode, SpaceDenied

    _ss, _app, _config, sf, _catalog, _adapter, agents = startup
    files = agents.files
    reached = []

    async def denied(session):
        assert session.in_transaction()
        reached.append(True)
        raise SpaceDenied("fixture admission denied before layout")

    def layout(*args):
        raise AssertionError("A denied assignment must make no filesystem effect")

    monkeypatch.setattr(files, "_layout", layout)
    with pytest.raises(SpaceDenied, match="before layout"):
        await files.assign(kind="user", actor=ALICE, name="Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE, admission=denied)
    assert reached == [True]
    async with sf() as session:
        assert not (await session.execute(select(SpaceRow))).first()
        assert not (await session.execute(select(SpaceBackingRow))).first()


@pytest.mark.asyncio
async def test_creation_repository_with_another_sessionmaker_on_same_engine_is_enforced(startup):
    from sqlalchemy.ext.asyncio import async_sessionmaker

    ss, app, config, sf, *_ = startup
    repo = SQLiteUserRepository(async_sessionmaker(sf.kw["bind"], expire_on_commit=False))
    await ss.initialize_storage_spaces(app, config, session_factory=sf, creation_repository=repo)
    assert repo.seats_allowed is not None


@pytest.mark.asyncio
async def test_runtime_rebinds_an_auth_repository_from_an_earlier_engine(seated_accounts, monkeypatch):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.gateway import deps
    from app.gateway.auth.local_provider import LocalAuthProvider

    repo, _sf, *_ = seated_accounts
    monkeypatch.setattr(deps, "_cached_repo", repo)
    monkeypatch.setattr(deps, "_cached_local_provider", LocalAuthProvider(repo))
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        sf = async_sessionmaker(engine)
        bound = deps.bind_user_repo(sf)
        assert bound is not repo and bound._sf is sf
        assert deps._cached_local_provider is None
    finally:
        await engine.dispose()
