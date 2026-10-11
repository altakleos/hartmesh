"""Host-only allocation and grants retain atomicity and principal boundaries."""

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, home, make_storage_fixture, operation
from sqlalchemy import select
from test_storage_spaces_attachments import ContainedProvider

from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceEventRow, SpaceGrantRow, SpaceRow
from deerflow.spaces.attachments import ResourceMount, SpaceAttachments
from deerflow.spaces.contract import Custody, InvalidPrincipal, MutationMode, Permission, PrincipalRef, SpaceConflict


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def storage(tmp_path, request):
    async for files, sf, catalog in make_storage_fixture(tmp_path, request):
        async with sf().bind.begin() as connection:
            await connection.run_sync(StorageFeatureLinkRow.__table__.create)
        yield files, sf, catalog


@pytest.mark.asyncio
async def test_host_create_has_no_actor_grant_or_event_and_links_its_role(storage):
    files, sf, catalog = storage
    for spec in catalog.volumes.values():
        spec.kind = "company"
    identifier = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
    async with sf() as session:
        row = await session.get(SpaceRow, identifier)
        assert (row.custody_kind, row.mode, row.generation, row.status) == ("company", "native", 1, "active")
        assert not (await session.execute(select(SpaceGrantRow))).first()
        assert not (await session.execute(select(SpaceEventRow))).first()
        link = (await session.execute(select(StorageFeatureLinkRow))).scalar_one()
        assert (link.namespace, link.key, link.space_id) == ("hm.company-space", "company", identifier)
        binding = (await session.execute(select(SpaceBackingRow))).scalar_one()
        volume = catalog.verify(binding.slot_id)
        assert (volume.data_path / "shared").is_dir()
    with pytest.raises(InvalidPrincipal):
        await files.registry.create(actor=None, name="Anonymous", custody=Custody.company(), mode=MutationMode.NATIVE)


@pytest.mark.asyncio
async def test_host_grant_is_atomic_noop_and_validates_callback_before_any_write(storage):
    files, sf, _ = storage
    identifier = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
    calls = []

    async def admission(session):
        assert session.in_transaction()
        assert await session.get(SpaceGrantRow, (identifier, "human", "alice")) is None
        calls.append(True)

    await files.registry.host_grant(identifier, ALICE, Permission.READ | Permission.EXPORT, host_id="host-test", admission=admission)
    assert calls == [True]
    async with sf() as session:
        row = await session.get(SpaceRow, identifier)
        assert row.generation == 2
        event = (await session.execute(select(SpaceEventRow))).scalar_one()
        assert (event.actor_kind, event.actor_id, event.action, event.generation) == ("human", "alice", "grant", 2)
        assert event.details == {"subject_kind": "human", "subject_id": "alice", "permissions": 17, "by": "host", "operator_host_id": "host-test"}

    async def reject(session):
        assert session.in_transaction()
        raise RuntimeError("domain refuses")

    # Domain/fencing/current principal checks precede no-op equality.
    with pytest.raises(RuntimeError, match="domain refuses"):
        await files.registry.host_grant(identifier, ALICE, Permission.READ | Permission.EXPORT, admission=reject)
    await files.registry.host_grant(identifier, ALICE, Permission.READ | Permission.EXPORT)
    with pytest.raises(RuntimeError):
        await files.registry.host_grant(identifier, BOB, Permission.READ, admission=reject)
    async with sf() as session:
        assert (await session.get(SpaceRow, identifier)).generation == 2
        assert await session.get(SpaceGrantRow, (identifier, "human", "bob")) is None
        assert len((await session.execute(select(SpaceEventRow))).scalars().all()) == 1


@pytest.mark.asyncio
async def test_host_grant_keeps_fencing_identity_and_permission_floor(storage):
    files, sf, _ = storage
    identifier = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
    for subject in (PrincipalRef("nonhuman", "worker"), PrincipalRef("human", "missing")):
        with pytest.raises(InvalidPrincipal):
            await files.registry.host_grant(identifier, subject, Permission.READ)
    for permissions in (Permission(0), Permission.OPERATE, Permission.WRITE, 9):
        with pytest.raises(ValueError):
            await files.registry.host_grant(identifier, ALICE, permissions)
    await files.registry.host_grant(identifier, ALICE, Permission.READ | Permission.EXPORT)
    mounts = SpaceAttachments(files, ContainedProvider())
    await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(identifier, 2, "shared")])
    with pytest.raises(SpaceConflict, match="fenced"):
        await files.registry.host_grant(identifier, ALICE, Permission.READ | Permission.EXPORT)
    with pytest.raises(SpaceConflict, match="fenced"):
        await files.registry.host_grant(identifier, BOB, Permission.READ)
    async with sf() as session:
        assert (await session.get(SpaceRow, identifier)).generation == 2
        assert await session.get(SpaceGrantRow, (identifier, "human", "bob")) is None


@pytest.mark.asyncio
async def test_host_locked_uses_explicit_scope_without_grant_or_operation_admission(storage):
    files, sf, _ = storage
    first, second = await home(files), await home(files)
    async with sf() as session:
        row = await session.get(SpaceRow, second.id)
        row.status = "deleted"
        await session.commit()

    async def forbidden(*args):
        pytest.fail("Host scope invoked ordinary operation admission")

    files.registry.operation_guard = forbidden
    with pytest.raises(ValueError, match="explicit scope"):
        async with files.registry.host_locked([]):
            pass
    async with files.registry.host_locked([second.id, "f" * 32, first.id]) as (session, rows):
        assert session.in_transaction()
        assert list(rows) == sorted([first.id, second.id])
        assert rows[second.id].status == "deleted"
        rows[first.id].name = "Host update"
    async with sf() as session:
        assert (await session.get(SpaceRow, first.id)).name == "Host update"


@pytest.mark.asyncio
async def test_host_workflow_records_host_authority_and_retains_persons_read_grant(storage):
    from deerflow.persistence.spaces.files import SpaceFileOperationRow
    from deerflow.spaces.contract import SpaceDenied
    from deerflow.spaces.workflows import mark_effect
    from deerflow.utils.file_io import run_file_io

    files, sf, _ = storage
    identifier = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
    await files.registry.host_grant(identifier, ALICE, Permission.READ | Permission.EXPORT)
    requests = {identifier: (Permission.READ | Permission.EXPORT, 2)}
    op = operation()

    async def publish(volumes):
        def write():
            mark_effect()
            with volumes[identifier].filesystem() as filesystem:
                filesystem.write_atomic("shared/file", b"host", expected_sha256=None, create=True)

        await run_file_io(write)
        return "published"

    with pytest.raises(SpaceDenied):
        await files.run_workflow(actor=ALICE, requests=requests, destination_id=identifier, operation_id=op, request={"action": "feature-workflow"}, call=publish)
    result = await files.run_workflow(actor=ALICE, requests=requests, destination_id=identifier, operation_id=op, request={"action": "feature-workflow"}, call=publish, host_write=True)
    assert result == "published"
    async with sf() as session:
        intent = await session.get(SpaceFileOperationRow, (identifier, op))
        assert intent.request["by"] == "host" and intent.phase == "complete"
        assert intent.actor_kind == "human" and intent.actor_id == "alice"
        assert (await session.get(SpaceGrantRow, (identifier, "human", "alice"))).permissions == 17
    assert await files.read(actor=ALICE, space_id=identifier, path="shared/file", max_bytes=10) == b"host"


@pytest.mark.asyncio
async def test_host_grant_exhausted_generation_and_callback_rollback_preserve_rows(storage):
    files, sf, _ = storage
    identifier = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")

    async def reject_after_sql(session):
        row = await session.get(SpaceRow, identifier)
        row.name = "Must roll back"
        raise RuntimeError("callback refusal")

    with pytest.raises(RuntimeError):
        await files.registry.host_grant(identifier, ALICE, Permission.READ, admission=reject_after_sql)
    async with sf() as session:
        row = await session.get(SpaceRow, identifier)
        assert (row.name, row.generation) == ("Company", 1)
        assert not (await session.execute(select(SpaceGrantRow))).first()
        assert not (await session.execute(select(SpaceEventRow))).first()
        row.generation = 2**31 - 1
        await session.commit()
    with pytest.raises(SpaceConflict, match="exhausted"):
        await files.registry.host_grant(identifier, ALICE, Permission.READ)
    async with sf() as session:
        assert (await session.get(SpaceRow, identifier)).generation == 2**31 - 1
        assert not (await session.execute(select(SpaceGrantRow))).first()
        assert not (await session.execute(select(SpaceEventRow))).first()


@pytest.mark.asyncio
async def test_host_workflow_cannot_write_an_archived_space(storage):
    from deerflow.spaces.contract import SpaceDenied

    files, sf, _ = storage
    identifier = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
    await files.registry.host_grant(identifier, ALICE, Permission.READ | Permission.EXPORT)
    async with sf() as session:
        row = await session.get(SpaceRow, identifier)
        row.status = "archived"
        await session.commit()

    async def forbidden(volumes):
        pytest.fail("Archived destination reached a writing callback")

    with pytest.raises(SpaceDenied, match="active native"):
        await files.run_workflow(actor=ALICE, requests={identifier: (Permission.READ | Permission.EXPORT, 2)}, destination_id=identifier, operation_id=operation(), request={"action": "feature-workflow"}, call=forbidden, host_write=True)
    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    async with sf() as session:
        assert not (await session.execute(select(SpaceFileOperationRow))).first()


@pytest.mark.asyncio
async def test_host_write_refuses_an_unresolved_destination_without_blocking_ordinary_reads(storage):
    from deerflow.persistence.spaces.files import SpaceFileOperationRow
    from deerflow.spaces.service import SpaceOperationPending

    files, sf, _ = storage
    identifier = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
    await files.registry.host_grant(identifier, ALICE, Permission.READ | Permission.EXPORT)
    previous = operation()
    async with sf() as session:
        session.add(SpaceFileOperationRow(space_id=identifier, operation_id=previous, actor_kind="human", actor_id="alice", generation=2, phase="pending", request={"action": "feature-workflow"}))
        await session.commit()

    async def forbidden(volumes):
        pytest.fail("Unresolved destination reached a mutating callback")

    with pytest.raises(SpaceOperationPending):
        await files.run_workflow(actor=ALICE, requests={identifier: (Permission.READ | Permission.EXPORT, 2)}, destination_id=identifier, operation_id=operation(), request={"action": "feature-workflow"}, call=forbidden, host_write=True)
    entries, _ = await files.list_directory(actor=ALICE, space_id=identifier)
    assert [entry.name for entry in entries] == ["projects", "shared"]
    async with sf() as session:
        rows = (await session.execute(select(SpaceFileOperationRow))).scalars().all()
        assert len(rows) == 1 and rows[0].operation_id == previous and rows[0].phase == "pending"


@pytest.mark.asyncio
async def test_host_write_allows_a_pending_source_read_and_only_its_exact_reconciliation(storage):
    from deerflow.persistence.spaces.files import SpaceFileOperationRow
    from deerflow.spaces.service import SpaceOperationPending
    from deerflow.utils.file_io import run_file_io

    files, sf, _ = storage
    source = await home(files)
    await files.write(actor=ALICE, space_id=source.id, expected_generation=1, operation_id=operation(), path="source", content=b"retained", create=True)
    identifier = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
    await files.registry.host_grant(identifier, ALICE, Permission.READ | Permission.EXPORT)
    async with sf() as session:
        session.add(SpaceFileOperationRow(space_id=source.id, operation_id=operation(), actor_kind="human", actor_id="alice", generation=1, phase="pending", request={"action": "write"}))
        await session.commit()
    op = operation()
    requests = {source.id: (Permission.READ | Permission.EXPORT, 1), identifier: (Permission.READ | Permission.EXPORT, 2)}

    async def interrupted(volumes):
        raise OSError("acknowledgement lost before publication")

    with pytest.raises(SpaceOperationPending):
        await files.run_workflow(actor=ALICE, requests=requests, destination_id=identifier, operation_id=op, request={"action": "feature-workflow"}, call=interrupted, host_write=True)

    async def copy(volumes):
        def publish():
            with volumes[source.id].filesystem() as source_fs:
                content = source_fs.read_bytes("source", max_bytes=100)
            from deerflow.spaces.workflows import mark_effect

            mark_effect()
            with volumes[identifier].filesystem() as destination:
                return destination.write_atomic("shared/copied", content, expected_sha256=None, create=True)

        return await run_file_io(publish)

    await files.run_workflow(actor=ALICE, requests=requests, destination_id=identifier, operation_id=op, request={"action": "feature-workflow"}, call=copy, host_write=True, reconcile=True)
    assert await files.read(actor=ALICE, space_id=identifier, path="shared/copied", max_bytes=100) == b"retained"
    async with sf() as session:
        assert (await session.get(SpaceFileOperationRow, (identifier, op))).phase == "complete"
        source_rows = (await session.execute(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == source.id, SpaceFileOperationRow.phase == "pending"))).scalars().all()
        assert len(source_rows) == 1
