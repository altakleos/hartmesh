"""Fresh first-party links use real SQL; directory fixtures do not qualify quotas."""

import asyncio

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, make_storage_fixture
from sqlalchemy import select

from deerflow.features.resources import FeatureResources, StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.spaces.contract import Custody, MutationMode, SpaceNotFound


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def resources(tmp_path, request):
    async for files, sf, catalog in make_storage_fixture(tmp_path, request):
        async with sf().bind.begin() as connection:
            await connection.run_sync(StorageFeatureLinkRow.__table__.create)
        yield FeatureResources(files, "hm.my-files"), files, sf, catalog


@pytest.mark.asyncio
async def test_concurrent_first_use_has_one_resource_and_no_orphan_backing(resources):
    feature, _, sf, _ = resources
    homes = await asyncio.gather(*(feature.ensure(actor=ALICE, key="human:alice", name="My Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE) for _ in range(4)))
    assert len({home.id for home in homes}) == 1
    async with sf() as session:
        assert len((await session.execute(select(SpaceBackingRow))).scalars().all()) == 1
        assert len((await session.execute(select(StorageFeatureLinkRow))).scalars().all()) == 1


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
