"""Released-account policy borrows SQL and retains generic native containment."""

import asyncio
from contextlib import asynccontextmanager
from threading import Event, Lock
from types import SimpleNamespace

import pytest
from sqlalchemy import select, text
from test_compose_user_assignment import account_storage as account_storage
from test_compose_user_assignment import seated_accounts as seated_accounts

from app.gateway import storage_spaces as ss
from app.gateway.auth.accounts import AccountsCommand
from app.gateway.auth.models import User
from deerflow.community.aio_sandbox.native_retirement import NativeRetirementScope
from deerflow.community.aio_sandbox.owner_admission import OwnerAdmissions
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceRow
from deerflow.persistence.user.model import UserRow


class ContainmentFixture:
    """Real scope/admission contract; physical proof remains an offline double."""

    def __init__(self, sf):
        self.sf = sf
        self.calls = []
        self.held = False
        self.proof_count = 0
        self.on_enter = None
        self.proof_error = None
        self.on_proof = None
        self._native_creation = object()
        self.gate = OwnerAdmissions(lock=Lock())

    def _pool_runtime_current(self):
        return self.held

    def _prove_native_retirement_absent(self, scope):
        scope.assert_current()
        self.proof_count += 1
        if self.on_proof is not None:
            asyncio.run_coroutine_threadsafe(self.on_proof(), self.loop).result(timeout=30)
        if self.proof_error is not None:
            raise self.proof_error

    @asynccontextmanager
    async def _native_retirement_scope(self, owner, slot):
        # Native admission must begin after the metadata preflight releases
        # SQL. An independent writer must be able to acquire its transaction.
        async with self.sf() as session:
            if session.get_bind().dialect.name == "sqlite":
                await session.execute(text("BEGIN IMMEDIATE"))
            else:
                await session.begin()
            await session.rollback()
        self.calls.append((owner, slot))
        reservation = self.gate.reserve(owner, slot)
        self.loop = asyncio.get_running_loop()
        self.held = True
        try:
            if self.on_enter is not None:
                await self.on_enter()
            yield NativeRetirementScope(self, self.gate, reservation, self._native_creation)
        finally:
            self.held = False
            self.gate.end_retirement(reservation)


async def assigned(sf, account):
    async with sf() as session:
        link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(account.id)))
        if link is None:
            return None
        row = await session.get(SpaceRow, link.space_id)
        binding = await session.get(SpaceBackingRow, row.backing_handle)
        return row.id, row.backing_handle, binding.slot_id


async def release(repo, account):
    command = AccountsCommand(repo, tokens=None, schedules=None)
    assert (await command.run("disable", account_id=str(account.id)))["returncode"] == 0
    assert (await command.run("release-seat", account_id=str(account.id)))["returncode"] == 0


@pytest.mark.asyncio
async def test_release_reclaims_one_slot_for_next_account_and_preserves_other_users(account_storage):
    app, repo, sf, files, catalog, _company = account_storage
    provider = app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    accounts = [await repo.create_user(User(email=f"person-{i}@example.com")) for i in range(3)]
    for person in accounts:
        await ss.assign_user_space(app, person)
    old = await assigned(sf, accounts[0])
    others = [await assigned(sf, person) for person in accounts[1:]]
    (catalog.verified[old[2]].data_path / "private-document").write_bytes(b"old account")
    await release(repo, accounts[0])
    new = await repo.create_user(User(email="next@example.com"))
    await ss.assign_user_space(app, new)
    assert await assigned(sf, new) is None

    assert await ss.reclaim_released_user_spaces(app) == 1
    assert provider.calls == [(str(accounts[0].id), old[2])]
    assert provider.proof_count == 1 and not provider.held
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(accounts[0].id))) is None
        assert await session.get(SpaceBackingRow, old[1]) is None
        assert (await session.get(SpaceRow, old[0])).status == "deleted"
    assert not any(catalog.verified[old[2]].data_path.iterdir())
    assert [await assigned(sf, person) for person in accounts[1:]] == others
    await ss.assign_user_space(app, new)
    assert (await assigned(sf, new))[2] == old[2]
    assert await ss.reclaim_released_user_spaces(app) == 0
    assert await repo.count_seated_users() == 3


@pytest.mark.asyncio
async def test_missing_qualified_provider_keeps_released_binding_and_role(account_storage):
    app, repo, sf, _files, _catalog, _company = account_storage
    person = await repo.create_user(User(email="person@example.com"))
    await ss.assign_user_space(app, person)
    original = await assigned(sf, person)
    await release(repo, person)
    assert await ss.reclaim_released_user_spaces(app) == 0
    assert await assigned(sf, person) == original


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["preflight", "contained"])
@pytest.mark.parametrize("change", ["account", "role", "custody", "status", "feature", "retention", "kind"])
async def test_changed_eligibility_keeps_binding_role_and_data(account_storage, phase, change):
    app, repo, sf, _files, catalog, _company = account_storage
    provider = app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    person = await repo.create_user(User(email="person@example.com"))
    await ss.assign_user_space(app, person)
    original = await assigned(sf, person)
    volume = catalog.verified[original[2]]
    (volume.data_path / "retained").write_bytes(b"unchanged")
    await release(repo, person)

    async def change_facts():
        async with sf() as session, session.begin():
            account = await session.get(UserRow, str(person.id))
            row = await session.get(SpaceRow, original[0])
            link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id)))
            if change == "account":
                account.released_at = None
            elif change == "role":
                link.namespace = "fixture.other-role"
            elif change == "custody":
                row.custodian_id = "another-account"
            elif change == "status":
                row.status = "archived"
            elif change == "feature":
                row.mode, row.feature_namespace, row.feature_controller, row.feature_metadata_version = "mediated", "fixture", "controller", 1
            elif change == "retention":
                event = (await session.scalars(select(SpaceEventRow).where(SpaceEventRow.space_id == row.id, SpaceEventRow.action == "delete"))).one()
                event.details = {**event.details, "retained_backups": True}
            else:
                volume.spec.kind = "employee"

    if phase == "preflight":
        await change_facts()
    else:
        provider.on_enter = change_facts
    assert await ss.reclaim_released_user_spaces(app) == 0
    assert not provider.held
    if phase == "preflight":
        assert not provider.calls
    assert (volume.data_path / "retained").read_bytes() == b"unchanged"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, original[1]) is not None
        assert len((await session.scalars(select(StorageFeatureLinkRow).where(StorageFeatureLinkRow.space_id == original[0]))).all()) == 1
        assert not (await session.scalars(select(SpaceFileOperationRow))).all()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["binding", "generation"])
async def test_preflight_snapshot_cannot_authorize_replacement_binding(account_storage, change):
    app, repo, sf, _files, catalog, _company = account_storage
    provider = app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    person = await repo.create_user(User(email="person@example.com"))
    await ss.assign_user_space(app, person)
    original = await assigned(sf, person)
    old_volume = catalog.verified[original[2]]
    (old_volume.data_path / "retained").write_bytes(b"old backing")
    replacement = next(v for slot, v in catalog.verified.items() if slot != original[2] and v.spec.kind == "user")
    await release(repo, person)

    async def replace_binding():
        async with sf() as session, session.begin():
            row = await session.get(SpaceRow, original[0])
            if change == "generation":
                row.generation += 1
            else:
                binding = await session.get(SpaceBackingRow, original[1])
                binding.slot_id = replacement.spec.slot_id
                binding.filesystem_uuid = replacement.spec.filesystem_uuid
                binding.root_inode, binding.control_inode = replacement.root_inode, replacement.control_inode

    provider.on_enter = replace_binding
    assert await ss.reclaim_released_user_spaces(app) == 0
    assert provider.calls == [(str(person.id), original[2])]
    assert provider.proof_count == 0 and not provider.held
    assert (old_volume.data_path / "retained").read_bytes() == b"old backing"
    assert not any(replacement.data_path.iterdir())
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id))) is not None
        assert await session.get(SpaceBackingRow, original[1]) is not None
        assert not (await session.scalars(select(SpaceFileOperationRow))).all()


@pytest.mark.asyncio
async def test_failed_absence_proof_retains_completed_checkpoint_and_retries_metadata_only(account_storage, monkeypatch):
    app, repo, sf, files, _catalog, _company = account_storage
    provider = app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    provider.proof_error = RuntimeError("The terminal native inventory is unconfirmed")
    person = await repo.create_user(User(email="person@example.com"))
    await ss.assign_user_space(app, person)
    original = await assigned(sf, person)
    await release(repo, person)

    async def no_ordinary_volume(*_args, **_kwargs):
        pytest.fail("Root-rotation retries must not require ordinary bound admission")

    monkeypatch.setattr(files, "_volume", no_ordinary_volume)
    assert await ss.reclaim_released_user_spaces(app) == 0
    assert not provider.held
    async with sf() as session:
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id))) is not None
        assert await session.get(SpaceBackingRow, original[1]) is not None
        pending = (await session.scalars(select(SpaceFileOperationRow))).one()
        assert pending.phase == "pending" and pending.result["stage"] == "filesystem-complete"
    provider.proof_error = None
    assert await ss.reclaim_released_user_spaces(app) == 1
    assert provider.calls == [(str(person.id), original[2])] * 2
    assert provider.proof_count == 2 and not provider.held
    assert await ss.reclaim_released_user_spaces(app) == 0


@pytest.mark.asyncio
async def test_bad_candidate_does_not_starve_later_released_user(account_storage):
    app, repo, sf, _files, catalog, _company = account_storage
    provider = app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    people = [await repo.create_user(User(email=f"person-{i}@example.com")) for i in range(2)]
    originals = {}
    for person in people:
        await ss.assign_user_space(app, person)
        originals[str(person.id)] = await assigned(sf, person)
        await release(repo, person)
    bad = min(originals, key=lambda account_id: originals[account_id][0])
    good = next(account_id for account_id in originals if account_id != bad)
    (catalog.verified[originals[bad][2]].data_path / "retained").write_bytes(b"unconfirmed")

    async def refuse_first():
        if provider.calls[-1][0] == bad:
            raise RuntimeError("This candidate's native containment is unconfirmed")

    provider.on_enter = refuse_first
    assert await ss.reclaim_released_user_spaces(app) == 1
    assert [owner for owner, _slot in provider.calls] == [bad, good]
    assert (catalog.verified[originals[bad][2]].data_path / "retained").read_bytes() == b"unconfirmed"
    async with sf() as session:
        assert await session.get(SpaceBackingRow, originals[bad][1]) is not None
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + bad)) is not None
        assert await session.get(SpaceBackingRow, originals[good][1]) is None
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + good)) is None


@pytest.mark.asyncio
async def test_unverified_binding_does_not_remove_user_role_or_count_capacity(account_storage, monkeypatch):
    from deerflow.spaces.backings import BackingUnavailable

    app, repo, sf, _files, catalog, _company = account_storage
    provider = app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    person = await repo.create_user(User(email="person@example.com"))
    await ss.assign_user_space(app, person)
    original = await assigned(sf, person)
    await release(repo, person)

    def unavailable(*_args, **_kwargs):
        raise BackingUnavailable("The physical roots are unverified")

    monkeypatch.setattr(catalog, "verify", unavailable)
    assert await ss.reclaim_released_user_spaces(app) == 0
    assert provider.proof_count == 0 and not provider.held
    async with sf() as session:
        assert await session.get(SpaceBackingRow, original[1]) is not None
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id))) is not None
        assert not (await session.scalars(select(SpaceFileOperationRow))).all()


@pytest.mark.asyncio
async def test_adapter_keeps_exact_successfully_qualified_provider(account_storage, monkeypatch):
    app, _repo, _sf, files, _catalog, _company = account_storage
    baseline = object()

    async def qualified(actual_files):
        assert actual_files is files
        return baseline, SimpleNamespace(provider=SimpleNamespace(host_id="fixture-qualified-host"))

    monkeypatch.setattr(ss, "qualified_attachments", qualified)
    assert await ss._adapter_step(app, timeout=5)
    assert app.state.agent_execution.qualified_provider is baseline
    assert app.state.agent_execution.host_id == "fixture-qualified-host"


@pytest.mark.asyncio
async def test_reclamation_borrows_one_connection_and_orders_seat_role_locks(account_storage, monkeypatch):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository

    app, repo, sf, files, _catalog, _company = account_storage
    person = await repo.create_user(User(email="person@example.com"))
    await ss.assign_user_space(app, person)
    await release(repo, person)
    original_engine = sf.kw["bind"]
    dialect = original_engine.dialect.name
    connect_args = {}
    if dialect == "postgresql":
        async with sf() as session:
            schema = await session.scalar(text("select current_schema()"))
        connect_args = {"server_settings": {"search_path": schema}}
    engine = create_async_engine(original_engine.url, pool_size=1, max_overflow=0, pool_timeout=1, connect_args=connect_args)
    single = async_sessionmaker(engine, expire_on_commit=False)
    files.registry._sf = single
    app.state.agent_execution.qualified_provider = ContainmentFixture(single)
    calls = []
    original_seats = SQLiteUserRepository.serialize_seats
    original_role = ss.serialize_role_assignment

    async def seats(session):
        assert dialect == "postgresql", "SQLite's kernel already owns BEGIN IMMEDIATE"
        calls.append("seats")
        await original_seats(session)

    async def role(session):
        if dialect == "postgresql":
            assert calls[-1] == "seats"
        calls.append("role")
        await original_role(session)

    monkeypatch.setattr(SQLiteUserRepository, "serialize_seats", staticmethod(seats))
    monkeypatch.setattr(ss, "serialize_role_assignment", role)
    try:
        import asyncio

        assert await asyncio.wait_for(ss.reclaim_released_user_spaces(app), 20) == 1
        assert calls
        if dialect == "postgresql":
            assert calls == ["seats", "role"] * (len(calls) // 2)
        else:
            assert set(calls) == {"role"}
    finally:
        files.registry._sf = sf
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["commit", "close"])
async def test_cancellation_retains_containment_through_commit_and_worker_close(account_storage, monkeypatch, boundary):
    import asyncio

    from deerflow.spaces.release_filesystem import ReleaseFilesystem

    app, repo, sf, files, _catalog, _company = account_storage
    provider = app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    person = await repo.create_user(User(email="person@example.com"))
    await ss.assign_user_space(app, person)
    original = await assigned(sf, person)
    await release(repo, person)
    entered, resume, finished = Event(), Event(), Event()
    original_close = ReleaseFilesystem.close
    original_lock = files.registry.host_locked

    def held_close(owner):
        entered.set()
        try:
            assert resume.wait(15)
            return original_close(owner)
        finally:
            finished.set()

    @asynccontextmanager
    async def held_commit(identifiers, **kwargs):
        async with original_lock(identifiers, **kwargs) as (session, rows):
            yield session, rows
            await session.flush()
            if await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id))) is None:
                entered.set()
                assert await asyncio.to_thread(resume.wait, 15)
                finished.set()

    if boundary == "close":
        monkeypatch.setattr(ReleaseFilesystem, "close", held_close)
    else:
        monkeypatch.setattr(files.registry, "host_locked", held_commit)
    task = asyncio.create_task(ss.reclaim_released_user_spaces(app))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert provider.held and not task.done()
        resume.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert finished.is_set() and not provider.held
        async with sf() as session:
            assert await session.get(SpaceBackingRow, original[1]) is None
            assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id))) is None
    finally:
        resume.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_final_commit_failure_keeps_role_binding_and_checkpoint_for_retry(account_storage, monkeypatch):
    app, repo, sf, files, _catalog, _company = account_storage
    provider = app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    person = await repo.create_user(User(email="person@example.com"))
    await ss.assign_user_space(app, person)
    original = await assigned(sf, person)
    await release(repo, person)
    original_lock = files.registry.host_locked
    failed = False

    @asynccontextmanager
    async def fail_publication_commit(identifiers, **kwargs):
        nonlocal failed
        async with original_lock(identifiers, **kwargs) as (session, rows):
            yield session, rows
            await session.flush()
            if not failed and await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id))) is None:
                assert provider.held
                failed = True
                raise OSError("The final transaction could not commit")

    monkeypatch.setattr(files.registry, "host_locked", fail_publication_commit)
    assert await ss.reclaim_released_user_spaces(app) == 0
    assert failed and not provider.held
    async with sf() as session:
        assert await session.get(SpaceBackingRow, original[1]) is not None
        assert await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id))) is not None
        operation = (await session.scalars(select(SpaceFileOperationRow))).one()
        assert operation.phase == "pending" and operation.result["stage"] == "filesystem-complete"
    assert await ss.reclaim_released_user_spaces(app) == 1
    assert provider.proof_count == 2 and not provider.held


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["account", "role", "binding", "provider"])
async def test_final_borrowed_admission_refuses_changed_facts_after_proof(account_storage, change):
    app, repo, sf, _files, _catalog, _company = account_storage
    provider = app.state.agent_execution.qualified_provider = ContainmentFixture(sf)
    person = await repo.create_user(User(email="person@example.com"))
    await ss.assign_user_space(app, person)
    original = await assigned(sf, person)
    await release(repo, person)

    async def mutate():
        async with sf() as session, session.begin():
            if change == "account":
                (await session.get(UserRow, str(person.id))).released_at = None
            elif change == "role":
                link = await session.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(person.id)))
                link.namespace = "fixture.other-role"
            elif change == "binding":
                (await session.get(SpaceBackingRow, original[1])).root_inode += 1
            else:
                app.state.agent_execution.qualified_provider = object()

    provider.on_proof = mutate
    assert await ss.reclaim_released_user_spaces(app) == 0
    assert provider.proof_count == 1 and not provider.held
    async with sf() as session:
        assert await session.get(SpaceBackingRow, original[1]) is not None
        assert len((await session.scalars(select(StorageFeatureLinkRow).where(StorageFeatureLinkRow.space_id == original[0]))).all()) == 1
        operation = (await session.scalars(select(SpaceFileOperationRow))).one()
        assert operation.phase == "pending" and operation.result["stage"] == "filesystem-complete"
