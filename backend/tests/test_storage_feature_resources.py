"""Fresh first-party links use real SQL; directory fixtures do not qualify quotas."""

import asyncio

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, make_storage_fixture
from sqlalchemy import select

from deerflow.features.resources import FeatureResources, StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.spaces.contract import Custody, MutationMode, SpaceNotFound
from deerflow.spaces.registry import SpaceRegistry
from deerflow.spaces.service import SpaceFiles


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def resources(tmp_path, request):
    async for files, sf, catalog in make_storage_fixture(tmp_path, request):
        async with sf().bind.begin() as connection:
            await connection.run_sync(StorageFeatureLinkRow.__table__.create)
        yield FeatureResources(files, "hm.my-files"), files, sf, catalog


@pytest.mark.asyncio
async def test_concurrent_first_use_has_one_resource_and_no_orphan_backing(resources):
    feature, files, sf, catalog = resources
    clients = [FeatureResources(SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog), feature.namespace) for _ in range(4)]
    homes = await asyncio.gather(*(client.ensure(actor=ALICE, key="human:alice", name="My Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE) for client in clients))
    assert len({home.id for home in homes}) == 1
    async with sf() as session:
        assert len((await session.execute(select(SpaceBackingRow))).scalars().all()) == 1
        assert len((await session.execute(select(StorageFeatureLinkRow))).scalars().all()) == 1
    markers = [volume.control_path / ".space-binding" for volume in catalog.verified.values() if (volume.control_path / ".space-binding").exists()]
    assert len(markers) == 1 and markers[0].read_bytes() == homes[0].backing_handle.encode("ascii")
    for volume in catalog.verified.values():
        assert not list(volume.data_path.iterdir())
        if not (volume.control_path / ".space-binding").exists():
            assert not list(volume.control_path.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("revoked", ["grant", "identity"])
async def test_waiting_first_use_rechecks_link_and_current_grant(resources, monkeypatch, revoked):
    from deerflow.persistence.spaces.model import SpaceGrantRow
    from deerflow.spaces.contract import InvalidPrincipal, ResolvedPrincipal
    from deerflow.spaces.principals import HostPrincipalResolver

    feature, files, sf, catalog = resources
    active = True
    validated = asyncio.Event()

    async def human(reference):
        validated.set()
        return ResolvedPrincipal(reference) if active else None

    second = FeatureResources(SpaceFiles(SpaceRegistry(sf, HostPrincipalResolver(human=human)), catalog), feature.namespace)
    attempted = asyncio.Event()
    original_find = second.find

    async def observed_find(key):
        value = await original_find(key)
        attempted.set()
        return value

    monkeypatch.setattr(second, "find", observed_find)
    task = None
    try:
        async with files.provisioning(actor=ALICE, name="Original", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE) as (session, original):
            session.add(StorageFeatureLinkRow(namespace=feature.namespace, key="human:alice", space_id=original.id))
            await session.flush()
            task = asyncio.create_task(second.ensure(actor=ALICE, key="human:alice", name="My Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE))
            async with asyncio.timeout(5):
                await attempted.wait()
                await validated.wait()
            await asyncio.sleep(0.03)
            assert not task.done()
            if revoked == "grant":
                grant = await session.get(SpaceGrantRow, (original.id, "human", "alice"))
                await session.delete(grant)
            else:
                active = False
        with pytest.raises(SpaceNotFound if revoked == "grant" else InvalidPrincipal):
            await task
        async with sf() as session:
            assert len((await session.scalars(select(SpaceBackingRow))).all()) == 1
            assert len((await session.scalars(select(StorageFeatureLinkRow))).all()) == 1
        assert sum((volume.control_path / ".space-binding").exists() for volume in catalog.verified.values()) == 1
    finally:
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_link_is_stable_after_rename_restart_and_current_grant_revocation(resources):
    feature, files, sf, _ = resources
    home = await feature.ensure(actor=ALICE, key="human:alice", name="My Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    await files.registry.rename(actor=ALICE, space_id=home.id, expected_generation=1, name="Renamed")
    restarted = FeatureResources(files, "hm.my-files")
    assert (await restarted.get(actor=ALICE, key="human:alice")).id == home.id
    with pytest.raises(SpaceNotFound):
        await restarted.get(actor=BOB, key="human:alice")
    async with sf() as session:
        from deerflow.persistence.spaces.model import SpaceGrantRow

        grant = await session.get(SpaceGrantRow, (home.id, "human", "alice"))
        await session.delete(grant)
        await session.commit()
    with pytest.raises(SpaceNotFound):
        await restarted.ensure(actor=ALICE, key="human:alice", name="My Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    async with sf() as session:
        assert len((await session.execute(select(SpaceBackingRow))).scalars().all()) == 1


@pytest.mark.asyncio
async def test_link_namespace_is_metadata_not_a_grant(resources):
    feature, files, _, _ = resources
    home = await feature.ensure(actor=ALICE, key="human:alice", name="My Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    assert await FeatureResources(files, "hm.projects").find("human:alice") is None
    assert await feature.find("human:alice") == home.id
