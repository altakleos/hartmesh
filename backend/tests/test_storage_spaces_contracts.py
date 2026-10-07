"""Resource identity and authority are independent of chats and feature plugins."""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import uuid
from dataclasses import replace

import pytest
import pytest_asyncio
from sqlalchemy import event, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.schema import CreateSchema, DropSchema

from deerflow.persistence.base import Base
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.spaces.contract import (
    Custody,
    FeatureBinding,
    InvalidPrincipal,
    MutationMode,
    Permission,
    PrincipalRef,
    ResolvedPrincipal,
    SpaceConflict,
    SpaceDenied,
    SpaceNotFound,
)
from deerflow.spaces.principals import HostPrincipalResolver, current_human_principal
from deerflow.spaces.registry import SpaceRegistry

ALICE = PrincipalRef("human", "alice")
BOB = PrincipalRef("human", "bob")
WORKER = PrincipalRef("nonhuman", "alice")  # Same ID must not alias the human.
OWNER = Permission.READ | Permission.EXPORT | Permission.WRITE | Permission.ADMIN


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def registry(tmp_path, request):
    admin = None
    schema = None
    if request.param == "postgres":
        uri = os.environ.get("TEST_POSTGRES_URI")
        if not uri:
            pytest.skip("TEST_POSTGRES_URI is not configured")
        pytest.importorskip("asyncpg")
        url = make_url(uri).set(drivername="postgresql+asyncpg")
        ssl = {"ssl": False} if url.query.get("sslmode") == "disable" else {}
        url = url.difference_update_query(["sslmode"])
        admin = create_async_engine(url, connect_args=ssl)
        schema = "storage_contract_" + uuid.uuid4().hex
        async with admin.begin() as connection:
            await connection.execute(CreateSchema(schema))
        engine = create_async_engine(url, connect_args={**ssl, "server_settings": {"search_path": schema}})
    else:
        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'spaces.db'}")

    @event.listens_for(engine.sync_engine, "connect")
    def pragmas(connection, _):
        if engine.dialect.name == "sqlite":
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=30000")

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all, tables=[SpaceRow.__table__, SpaceGrantRow.__table__, SpaceEventRow.__table__])
    sf = async_sessionmaker(engine, expire_on_commit=False)
    subjects = {ALICE: ResolvedPrincipal(ALICE, can_provision_company=True), BOB: ResolvedPrincipal(BOB), WORKER: ResolvedPrincipal(WORKER)}

    async def human(reference):
        return subjects.get(reference)

    async def nonhuman(reference):
        return subjects.get(reference)

    resolver = HostPrincipalResolver(human=human, nonhuman=nonhuman)
    try:
        yield SpaceRegistry(sf, resolver), subjects, sf
    finally:
        await engine.dispose()
        if admin is not None:
            try:
                async with admin.begin() as connection:
                    await connection.execute(DropSchema(schema, cascade=True))
            finally:
                await admin.dispose()


@pytest.mark.parametrize("kind,subject", [("agent", "alice"), ("human", ""), ("human", "../alice"), ("nonhuman", "a/b"), ("human", "a" * 129)])
def test_principal_references_are_typed_and_bounded(kind, subject):
    with pytest.raises(ValueError):
        PrincipalRef(kind, subject)


def test_missing_user_never_means_default_or_an_unscoped_actor():
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    token = set_current_user(None)
    try:
        with pytest.raises(InvalidPrincipal):
            current_human_principal()
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_human_adapter_uses_authenticated_host_context():
    from types import SimpleNamespace

    from deerflow.runtime.user_context import reset_current_user, set_current_user

    token = set_current_user(SimpleNamespace(id="alice"))
    try:
        assert current_human_principal() == ALICE
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_unknown_and_misresolved_principals_fail_closed(registry):
    spaces, subjects, _ = registry
    for actor in (None, PrincipalRef("human", "unknown"), PrincipalRef("nonhuman", "unknown")):
        with pytest.raises(InvalidPrincipal):
            await spaces.list(actor=actor)
        with pytest.raises(InvalidPrincipal):
            await spaces.create(actor=actor, name="unknown", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    subjects[BOB] = replace(subjects[BOB], reference=ALICE)
    with pytest.raises(InvalidPrincipal):
        await spaces.list(actor=BOB)


@pytest.mark.asyncio
async def test_rename_does_not_change_identity_root_custody_or_members(registry):
    spaces, _, _ = registry
    original = await spaces.create(actor=ALICE, name="first", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    assert original.permissions == OWNER
    assert original.id != original.backing_handle and "/" not in original.backing_handle
    renamed = await spaces.rename(actor=ALICE, space_id=original.id, name="renamed", expected_generation=original.generation)
    assert (renamed.id, renamed.backing_handle, renamed.custody, renamed.permissions) == (original.id, original.backing_handle, original.custody, OWNER)
    assert renamed.generation == original.generation + 1
    assert (await spaces.list(actor=ALICE))[0].name == "renamed"
    with pytest.raises(SpaceNotFound):
        await spaces.get(actor=BOB, space_id=original.id)
    assert await spaces.list(actor=BOB) == []


@pytest.mark.asyncio
async def test_company_custody_requires_host_authority_and_never_means_ambient_read(registry):
    spaces, subjects, sf = registry
    with pytest.raises(SpaceDenied):
        await spaces.create(actor=BOB, name="company", custody=Custody.company(), mode=MutationMode.NATIVE)
    space = await spaces.create(actor=ALICE, name="company", custody=Custody.company(), mode=MutationMode.NATIVE)
    assert space.custody == Custody.company()
    assert await spaces.list(actor=BOB) == []
    subjects.pop(ALICE)
    with pytest.raises(InvalidPrincipal):
        await spaces.get(actor=ALICE, space_id=space.id)
    async with sf() as session:
        row = await session.get(SpaceRow, space.id)
        assert row is not None and row.custody_kind == "company" and row.custodian_id is None


@pytest.mark.asyncio
async def test_personal_custody_cannot_impersonate_an_owner(registry):
    spaces, _, _ = registry
    with pytest.raises(SpaceDenied):
        await spaces.create(actor=ALICE, name="claimed", custody=Custody.personal(BOB), mode=MutationMode.NATIVE)


@pytest.mark.asyncio
async def test_nonhuman_membership_is_explicit_and_independent_of_human_alias(registry):
    spaces, subjects, _ = registry
    space = await spaces.create(actor=ALICE, name="restricted", custody=Custody.company(), mode=MutationMode.NATIVE)
    assert await spaces.list(actor=WORKER) == []
    with pytest.raises(SpaceDenied, match="existing data"):
        await spaces.set_grant(actor=ALICE, space_id=space.id, subject=WORKER, permissions=Permission.READ, expected_generation=space.generation)
    updated = await spaces.set_grant(actor=ALICE, space_id=space.id, subject=WORKER, permissions=Permission.READ | Permission.WRITE, expected_generation=space.generation, acknowledge_existing_data=True)
    member = await spaces.get(actor=WORKER, space_id=space.id)
    assert member.permissions == Permission.READ | Permission.WRITE
    assert member.generation == updated.generation
    with pytest.raises(SpaceDenied):
        await spaces.rename(actor=WORKER, space_id=space.id, name="owned?", expected_generation=updated.generation)
    subjects.pop(WORKER)
    with pytest.raises(InvalidPrincipal):
        await spaces.get(actor=WORKER, space_id=space.id)
    assert (await spaces.get(actor=ALICE, space_id=space.id)).custody == Custody.company()


@pytest.mark.asyncio
async def test_grants_validate_the_target_and_cannot_escalate_or_lock_out_last_admin(registry):
    spaces, _, _ = registry
    space = await spaces.create(actor=ALICE, name="home", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    with pytest.raises(InvalidPrincipal):
        await spaces.set_grant(actor=ALICE, space_id=space.id, subject=PrincipalRef("nonhuman", "forged"), permissions=Permission.READ, expected_generation=space.generation, acknowledge_existing_data=True)
    with pytest.raises(SpaceDenied):
        await spaces.set_grant(actor=BOB, space_id=space.id, subject=BOB, permissions=OWNER, expected_generation=space.generation, acknowledge_existing_data=True)
    with pytest.raises(SpaceConflict, match="administrator"):
        await spaces.set_grant(actor=ALICE, space_id=space.id, subject=ALICE, permissions=Permission.READ, expected_generation=space.generation)


@pytest.mark.asyncio
async def test_mediated_resources_never_accept_generic_write_authority(registry):
    spaces, _, _ = registry
    binding = FeatureBinding("publication", "controller", 1)
    space = await spaces.create(actor=ALICE, name="mediated", custody=Custody.company(), mode=MutationMode.MEDIATED, feature=binding)
    assert space.feature == binding
    assert space.permissions == Permission.READ | Permission.EXPORT | Permission.OPERATE | Permission.ADMIN
    with pytest.raises(SpaceDenied):
        await spaces.get(actor=ALICE, space_id=space.id, permission=Permission.WRITE)
    with pytest.raises(ValueError):
        await spaces.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission.WRITE, expected_generation=space.generation)
    with pytest.raises(ValueError):
        await spaces.create(actor=ALICE, name="unbound", custody=Custody.company(), mode=MutationMode.MEDIATED)


@pytest.mark.asyncio
async def test_generation_check_and_grant_revocation_are_serialized(registry):
    spaces, _, sf = registry
    space = await spaces.create(actor=ALICE, name="serial", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    outcomes = await asyncio.gather(
        spaces.rename(actor=ALICE, space_id=space.id, name="one", expected_generation=space.generation),
        spaces.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission.READ, expected_generation=space.generation, acknowledge_existing_data=True),
        return_exceptions=True,
    )
    assert sum(isinstance(o, SpaceConflict) for o in outcomes) == 1
    current = await spaces.get(actor=ALICE, space_id=space.id)
    assert current.generation == space.generation + 1
    current = await spaces.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission.READ, expected_generation=current.generation, acknowledge_existing_data=True)
    assert len(await spaces.list(actor=BOB)) == 1
    current = await spaces.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission(0), expected_generation=current.generation)
    assert await spaces.list(actor=BOB) == []
    with pytest.raises(SpaceNotFound):
        await spaces.get(actor=BOB, space_id=space.id)
    async with sf() as session:
        rows = (await session.execute(select(SpaceGrantRow).where(SpaceGrantRow.space_id == space.id))).scalars().all()
        assert len(rows) == 1


@pytest.mark.asyncio
async def test_deleted_resources_and_old_generations_never_authorize(registry):
    spaces, _, sf = registry
    space = await spaces.create(actor=ALICE, name="retained", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    async with sf() as session:
        row = await session.get(SpaceRow, space.id)
        row.status = "deleted"
        row.generation += 1
        await session.commit()
    assert await spaces.list(actor=ALICE) == []
    with pytest.raises(SpaceNotFound):
        await spaces.get(actor=ALICE, space_id=space.id)
    with pytest.raises(SpaceNotFound):
        await spaces.rename(actor=ALICE, space_id=space.id, name="resurrect", expected_generation=space.generation)


@pytest.mark.asyncio
async def test_native_and_mediated_permissions_are_not_interchangeable(registry):
    spaces, _, _ = registry
    space = await spaces.create(actor=ALICE, name="native", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    for permissions in (Permission.OPERATE, Permission(64), Permission(-1)):
        with pytest.raises(ValueError):
            await spaces.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=permissions, expected_generation=space.generation)


@pytest.mark.parametrize("custody", [("company", "alice"), ("personal", None), ("other", None)])
def test_custody_does_not_accept_untyped_or_extra_owner_metadata(custody):
    with pytest.raises(ValueError):
        Custody(*custody)


@pytest.mark.asyncio
async def test_nonhuman_principals_require_an_explicit_host_adapter():
    async def human(reference):
        return ResolvedPrincipal(reference)

    with pytest.raises(InvalidPrincipal):
        await HostPrincipalResolver(human=human).resolve(WORKER)


@pytest.mark.asyncio
async def test_stale_and_invalid_generations_fail_before_mutation(registry):
    spaces, _, _ = registry
    space = await spaces.create(actor=ALICE, name="versioned", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    await spaces.rename(actor=ALICE, space_id=space.id, name="current", expected_generation=space.generation)
    with pytest.raises(SpaceConflict):
        await spaces.get(actor=ALICE, space_id=space.id, expected_generation=space.generation)
    for generation in (0, True, "2"):
        with pytest.raises(ValueError):
            await spaces.get(actor=ALICE, space_id=space.id, expected_generation=generation)


@pytest.mark.asyncio
async def test_archive_retains_reads_and_reports_no_effective_native_write(registry):
    spaces, _, sf = registry
    space = await spaces.create(actor=ALICE, name="archive", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    async with sf() as session:
        row = await session.get(SpaceRow, space.id)
        row.status = "archived"
        row.generation += 1
        await session.commit()
    archived = await spaces.get(actor=ALICE, space_id=space.id)
    assert archived.status == "archived" and not archived.permissions & Permission.WRITE
    with pytest.raises(SpaceDenied):
        await spaces.get(actor=ALICE, space_id=space.id, permission=Permission.WRITE)


@pytest.mark.asyncio
async def test_retired_members_can_be_removed_without_deleting_company_custody(registry):
    spaces, subjects, _ = registry
    space = await spaces.create(actor=ALICE, name="company", custody=Custody.company(), mode=MutationMode.NATIVE)
    space = await spaces.set_grant(actor=ALICE, space_id=space.id, subject=WORKER, permissions=Permission.READ, expected_generation=space.generation, acknowledge_existing_data=True)
    subjects.pop(WORKER)
    space = await spaces.set_grant(actor=ALICE, space_id=space.id, subject=WORKER, permissions=Permission(0), expected_generation=space.generation)
    assert space.custody == Custody.company()
    with pytest.raises(InvalidPrincipal):
        await spaces.set_grant(actor=ALICE, space_id=space.id, subject=WORKER, permissions=Permission.READ, expected_generation=space.generation, acknowledge_existing_data=True)


@pytest.mark.asyncio
async def test_resource_events_bind_the_actual_typed_actor_and_generation(registry):
    spaces, _, sf = registry
    space = await spaces.create(actor=WORKER, name="home", custody=Custody.personal(WORKER), mode=MutationMode.NATIVE)
    updated = await spaces.rename(actor=WORKER, space_id=space.id, name="renamed", expected_generation=space.generation)
    async with sf() as session:
        events = (await session.execute(select(SpaceEventRow).where(SpaceEventRow.space_id == space.id).order_by(SpaceEventRow.generation))).scalars().all()
        assert [(e.generation, e.action, e.actor_kind, e.actor_id) for e in events] == [(1, "create", "nonhuman", "alice"), (2, "rename", "nonhuman", "alice")]
    assert updated.generation == 2
    with pytest.raises(SpaceConflict):
        await spaces.rename(actor=WORKER, space_id=space.id, name="stale", expected_generation=1)
    async with sf() as session:
        assert len((await session.execute(select(SpaceEventRow).where(SpaceEventRow.space_id == space.id))).scalars().all()) == 2


@pytest.mark.asyncio
async def test_malformed_persisted_grants_cannot_authorize_a_partial_mutation(registry):
    spaces, _, sf = registry
    space = await spaces.create(actor=ALICE, name="original", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    async with sf() as session:
        grant = await session.get(SpaceGrantRow, (space.id, ALICE.kind, ALICE.subject_id))
        grant.permissions |= int(Permission.OPERATE)  # Legal SQL bits, invalid native capability.
        await session.commit()
    with pytest.raises(ValueError):
        await spaces.rename(actor=ALICE, space_id=space.id, name="must-not-commit", expected_generation=space.generation)
    async with sf() as session:
        row = await session.get(SpaceRow, space.id)
        assert row.name == "original" and row.generation == 1
        assert len((await session.execute(select(SpaceEventRow).where(SpaceEventRow.space_id == space.id))).scalars().all()) == 1


@pytest.mark.asyncio
async def test_grant_targets_are_validated_inside_the_mutation_boundary(registry):
    spaces, subjects, sf = registry
    space = await spaces.create(actor=ALICE, name="valid", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    original = spaces._resolver.resolve
    target_calls = 0

    async def retire_between_checks(reference):
        nonlocal target_calls
        result = await original(reference)
        if reference == BOB:
            target_calls += 1
            if target_calls == 1:
                subjects.pop(BOB)
        return result

    spaces._resolver.resolve = retire_between_checks
    with pytest.raises(InvalidPrincipal):
        await spaces.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission.READ, expected_generation=space.generation, acknowledge_existing_data=True)
    async with sf() as session:
        assert (await session.get(SpaceRow, space.id)).generation == 1
        assert await session.get(SpaceGrantRow, (space.id, BOB.kind, BOB.subject_id)) is None


@pytest.mark.asyncio
async def test_actor_is_revalidated_after_waiting_for_the_parent_lock(registry):
    from sqlalchemy import text

    spaces, subjects, sf = registry
    space = await spaces.create(actor=ALICE, name="locked", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    entered = asyncio.Event()
    original = spaces._resolver.resolve

    async def signal_resolution(reference):
        resolved = await original(reference)
        entered.set()
        return resolved

    spaces._resolver.resolve = signal_resolution
    async with sf() as holding:
        if holding.bind.dialect.name == "sqlite":
            await holding.execute(text("BEGIN IMMEDIATE"))
        else:
            await holding.execute(select(SpaceRow).where(SpaceRow.id == space.id).with_for_update())
        mutation = asyncio.create_task(spaces.rename(actor=ALICE, space_id=space.id, name="retired", expected_generation=1))
        await asyncio.wait_for(entered.wait(), 5)
        subjects.pop(ALICE)
        await holding.commit()
        with pytest.raises(InvalidPrincipal):
            await asyncio.wait_for(mutation, 5)
    async with sf() as session:
        assert (await session.get(SpaceRow, space.id)).name == "locked"


@pytest.mark.asyncio
async def test_native_write_implies_existing_data_access_and_requires_read(registry):
    spaces, _, _ = registry
    space = await spaces.create(actor=ALICE, name="native", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    with pytest.raises(ValueError, match="read"):
        await spaces.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission.WRITE, expected_generation=1)


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("custodian_id", "extra"), ("feature_controller", "partial"), ("backing_handle", "../outside"), ("name", ""), ("generation", 2**31 - 1)])
async def test_malformed_resource_definitions_or_exhausted_generations_never_commit(registry, field, value):
    from sqlalchemy import text

    spaces, _, sf = registry
    sqlite = sf.kw["bind"].dialect.name == "sqlite"
    if not sqlite and field in ("custodian_id", "feature_controller"):
        pytest.skip("PostgreSQL check constraints reject these corrupted definitions before registry access")
    space = await spaces.create(actor=ALICE, name="original", custody=Custody.company(), mode=MutationMode.NATIVE)
    async with sf() as session:
        if sqlite:
            await session.execute(text("PRAGMA ignore_check_constraints=ON"))
        row = await session.get(SpaceRow, space.id)
        setattr(row, field, value)
        await session.commit()
        if sqlite:
            await session.execute(text("PRAGMA ignore_check_constraints=OFF"))
    with pytest.raises((ValueError, SpaceConflict)):
        await spaces.rename(actor=ALICE, space_id=space.id, name="must-not-commit", expected_generation=value if field == "generation" else 1)
    async with sf() as session:
        row = await session.get(SpaceRow, space.id)
        assert row.name == (value if field == "name" else "original")
        assert row.generation == (value if field == "generation" else 1)


@pytest.mark.asyncio
async def test_retired_admins_do_not_allow_the_last_active_admin_to_leave(registry):
    spaces, subjects, _ = registry
    space = await spaces.create(actor=ALICE, name="admin", custody=Custody.company(), mode=MutationMode.NATIVE)
    space = await spaces.set_grant(actor=ALICE, space_id=space.id, subject=BOB, permissions=Permission.ADMIN, expected_generation=1)
    subjects.pop(BOB)
    with pytest.raises(SpaceConflict, match="administrator"):
        await spaces.set_grant(actor=ALICE, space_id=space.id, subject=ALICE, permissions=Permission.READ, expected_generation=space.generation)


@pytest.mark.asyncio
@pytest.mark.parametrize("permissions", [-24, -15])
async def test_negative_persisted_permission_bits_never_normalize_into_authority(registry, permissions):
    from sqlalchemy import text

    spaces, _, sf = registry
    if sf.kw["bind"].dialect.name != "sqlite":
        pytest.skip("PostgreSQL rejects negative permissions at the database boundary")
    space = await spaces.create(actor=ALICE, name="original", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    async with sf() as session:
        await session.execute(text("PRAGMA ignore_check_constraints=ON"))
        grant = await session.get(SpaceGrantRow, (space.id, ALICE.kind, ALICE.subject_id))
        grant.permissions = permissions
        await session.commit()
        await session.execute(text("PRAGMA ignore_check_constraints=OFF"))
    # IntFlag(-24) becomes ADMIN and IntFlag(-15) becomes READ|EXPORT.
    with pytest.raises(ValueError):
        if permissions == -24:
            await spaces.rename(actor=ALICE, space_id=space.id, name="must-not-commit", expected_generation=1)
        else:
            await spaces.get(actor=ALICE, space_id=space.id)
    async with sf() as session:
        row = await session.get(SpaceRow, space.id)
        assert row.name == "original" and row.generation == 1


@pytest.mark.asyncio
async def test_fresh_process_registers_resource_tables_for_startup_bootstrap():
    # This process's test imports register models as a side effect, which can
    # hide missing host registration. Use the actual cold startup entrypoint.
    source = "import deerflow.persistence.models; from deerflow.persistence.base import Base; assert {'storage_spaces', 'storage_space_grants', 'storage_space_events'} <= set(Base.metadata.tables)"
    result = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", source], capture_output=True, text=True, env={**os.environ, "PYTHON_DOTENV_DISABLED": "1"}, timeout=30)
    assert result.returncode == 0, result.stderr
