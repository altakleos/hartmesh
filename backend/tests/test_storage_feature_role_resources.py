"""Bundled namespaces look up assigned role links and never allocate on reads."""

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, make_storage_fixture
from sqlalchemy import select

from deerflow.features.plugins import FeatureService
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.spaces.files import SpaceBackingRow
from deerflow.persistence.spaces.model import SpaceGrantRow
from deerflow.spaces.contract import Custody, MutationMode, PrincipalRef, SpaceNotFound
from deerflow.spaces.facade import HostStorageProvider, storage_actor_scope


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def storage(tmp_path, request):
    async for files, sf, catalog in make_storage_fixture(tmp_path, request):
        async with sf().bind.begin() as connection:
            await connection.run_sync(StorageFeatureLinkRow.__table__.create)
        yield files, sf, catalog


def service(files, namespace):
    item = FeatureService(namespace, "Files", None, mediated=namespace == "hm.shared")
    item.healthy = True
    item.provider = HostStorageProvider(lambda: files)
    return item


@pytest.mark.asyncio
async def test_two_personal_features_share_the_assigned_role_without_allocating(storage, monkeypatch):
    from deerflow.features.resources import FeatureResources

    files, sf, _ = storage
    identifier = await files.assign(kind="user", actor=ALICE, name="Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)

    async def forbidden(*args, **kwargs):
        pytest.fail("Plugin allocated instead of looking up an assignment")

    monkeypatch.setattr(FeatureResources, "ensure", forbidden)
    with storage_actor_scope(ALICE):
        assert (await service(files, "hm.my-files").resource()).id == identifier
        assert (await service(files, "hm.projects").resource()).id == identifier
    async with sf() as session:
        assert len((await session.execute(select(SpaceBackingRow))).scalars().all()) == 1
        links = (await session.execute(select(StorageFeatureLinkRow))).scalars().all()
        assert len(links) == 1 and links[0].namespace == "hm.user-space"
    with storage_actor_scope(BOB), pytest.raises(SpaceNotFound):
        await service(files, "hm.my-files").resource()


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace", ["hm.my-files", "hm.projects", "hm.shared"])
async def test_missing_role_is_distinct_and_nonhuman_owners_remain_refused(storage, namespace):
    from deerflow_extension_api.storage import StorageUnsupported

    from deerflow.features.resources import MissingFeatureResource

    files, sf, _ = storage
    with storage_actor_scope(ALICE), pytest.raises(MissingFeatureResource):
        await service(files, namespace).resource()
    with storage_actor_scope(PrincipalRef("nonhuman", "worker")), pytest.raises(StorageUnsupported):
        await service(files, namespace).resource()
    async with sf() as session:
        assert not (await session.execute(select(SpaceBackingRow))).first()
        assert not (await session.execute(select(StorageFeatureLinkRow))).first()


@pytest.mark.asyncio
async def test_revoked_assigned_space_is_not_reported_as_unassigned(storage):
    from deerflow.features.resources import MissingFeatureResource

    files, sf, _ = storage
    identifier = await files.assign(kind="user", actor=ALICE, name="Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
    async with sf() as session:
        await session.delete(await session.get(SpaceGrantRow, (identifier, "human", "alice")))
        await session.commit()
    with storage_actor_scope(ALICE), pytest.raises(SpaceNotFound) as caught:
        await service(files, "hm.projects").resource()
    assert not isinstance(caught.value, MissingFeatureResource)
