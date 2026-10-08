"""Real SQL admission; injected containment is not native qualification."""

import pytest
import pytest_asyncio
from _storage_spaces_test_support import ALICE, BOB, home, make_storage_fixture, operation
from sqlalchemy import select

from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.spaces.attachments import AttachmentPending, ResourceMount, SpaceAttachments
from deerflow.spaces.contract import Permission, SpaceConflict, SpaceDenied


class ContainedProvider:
    host_id = "qualified-test-host"

    def __init__(self):
        self.containers = {}
        self.uncertain = False
        self.started = []

    def prepare(self, plan):
        container = operation() * 2
        self.containers[container] = plan
        return container

    def start(self, container, plan):
        self.started.append(container)

    def fence(self, container, attachment_id):
        if self.uncertain:
            raise RuntimeError("Daemon unavailable")
        self.containers.pop(container, None)

    def discover(self, attachment_id):
        if self.uncertain:
            raise RuntimeError("Daemon unavailable")
        return [key for key, value in self.containers.items() if value.id == attachment_id]


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def storage(tmp_path, request):
    async for files, sf, catalog in make_storage_fixture(tmp_path, request):
        provider = ContainedProvider()
        yield files, sf, catalog, SpaceAttachments(files, provider), provider


@pytest.mark.asyncio
async def test_attachment_survives_calls_and_blocks_host_editor(storage):
    files, sf, _, mounts, provider = storage
    space = await home(files)
    attachment = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    assert attachment.host_id == provider.host_id and attachment.container_id in provider.started
    with pytest.raises(SpaceConflict, match="attachment"):
        await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"edit", create=True)
    restarted = SpaceAttachments(files, provider)
    with pytest.raises(SpaceConflict):
        await restarted.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "new", writable=True)])
    await restarted.retire(actor=ALICE, space_id=space.id, expected_generation=1)
    assert not provider.containers
    async with sf() as session:
        assert (await session.execute(select(SpaceAttachmentRow))).scalar_one().phase == "fenced"
    await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"persisted", create=True)


@pytest.mark.asyncio
async def test_unknown_writer_keeps_retirement_pending_without_expiry(storage):
    files, sf, _, mounts, provider = storage
    space = await home(files)
    await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    provider.uncertain = True
    with pytest.raises(AttachmentPending):
        await mounts.retire(actor=ALICE, space_id=space.id, expected_generation=1)
    async with sf() as session:
        assert (await session.execute(select(SpaceAttachmentRow))).scalar_one().phase == "fence_pending"
    with pytest.raises(SpaceConflict):
        await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=1, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    with pytest.raises(SpaceConflict):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "takeover", writable=True)])
    provider.uncertain = False
    await mounts.retire(actor=ALICE, space_id=space.id, expected_generation=1)
    assert not provider.containers


@pytest.mark.asyncio
async def test_readonly_attachments_do_not_authorize_mutation_and_modes_are_enforced(storage):
    files, _, _, mounts, _ = storage
    space = await home(files)
    await files.registry.set_grant(actor=ALICE, space_id=space.id, expected_generation=1, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    with pytest.raises(SpaceDenied):
        await mounts.attach(actor=BOB, incarnation=operation(), resources=[ResourceMount(space.id, 2, "home", writable=True)])
    await mounts.attach(actor=BOB, incarnation=operation(), resources=[ResourceMount(space.id, 2, "home")])
    with pytest.raises(SpaceDenied):
        await mounts.retire(actor=BOB, space_id=space.id, expected_generation=2)


@pytest.mark.asyncio
async def test_joint_views_require_disclosure_authority(storage):
    files, _, _, mounts, _ = storage
    first, second = await home(files), await home(files)
    await files.registry.set_grant(actor=ALICE, space_id=second.id, expected_generation=1, subject=BOB, permissions=Permission.READ, acknowledge_existing_data=True)
    views = [ResourceMount(first.id, 1, "source"), ResourceMount(second.id, 2, "destination", writable=True)]
    with pytest.raises(SpaceDenied, match="audience"):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=views)
    await mounts.attach(actor=ALICE, incarnation=operation(), resources=views, acknowledge_disclosure=True)


@pytest.mark.asyncio
async def test_aliases_duplicates_and_stale_generations_are_rejected(storage):
    files, _, _, mounts, _ = storage
    space = await home(files)
    with pytest.raises(ValueError):
        ResourceMount(space.id, 1, "../escape")
    with pytest.raises(ValueError):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "a"), ResourceMount(space.id, 1, "b")])
    with pytest.raises(SpaceConflict):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 2, "a")])


@pytest.mark.asyncio
async def test_failed_start_persists_intent_and_requires_real_containment(storage):
    files, sf, _, mounts, provider = storage
    space = await home(files)

    def uncertain_start(container, plan):
        raise RuntimeError("Start response lost")

    provider.start = uncertain_start
    with pytest.raises(AttachmentPending):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    async with sf() as session:
        row = (await session.execute(select(SpaceAttachmentRow))).scalar_one()
        assert row.phase == "pending" and row.container_id in provider.containers
    await mounts.retire(actor=ALICE, space_id=space.id, expected_generation=1)
    assert not provider.containers


@pytest.mark.asyncio
async def test_unknown_prepare_without_discoverable_identity_stays_pending(storage):
    files, _, _, mounts, provider = storage
    space = await home(files)

    def unknown_prepare(plan):
        raise RuntimeError("Prepare result lost before exact identity")

    provider.prepare = unknown_prepare
    with pytest.raises(AttachmentPending):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    with pytest.raises(AttachmentPending, match="identity is unknown"):
        await mounts.retire(actor=ALICE, space_id=space.id, expected_generation=1)
    with pytest.raises(SpaceConflict):
        await files.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"unsafe", create=True)


@pytest.mark.asyncio
async def test_invalid_prepared_ownership_retains_known_identity_and_denies_takeover(storage):
    from test_storage_spaces_docker import ID, TOKEN, Docker, inspect_entry

    from deerflow.spaces.docker import DockerStorageAdapter

    files, sf, _, _, _ = storage
    space = await home(files)
    docker = Docker()
    docker.entry = inspect_entry()
    docker.entry["State"] = {"Status": "running", "Running": True}
    docker.entry["Config"]["Labels"]["hartmesh.storage.attachment"] = TOKEN
    provider = DockerStorageAdapter(prepare=lambda plan: ID, runner=docker, host_verifier=lambda: None)
    mounts = SpaceAttachments(files, provider)
    with pytest.raises(AttachmentPending):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    async with sf() as session:
        assert (await session.execute(select(SpaceAttachmentRow))).scalar_one().container_id == ID
    with pytest.raises(AttachmentPending):
        await mounts.retire(actor=ALICE, space_id=space.id, expected_generation=1)
    with pytest.raises(SpaceConflict):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "takeover", writable=True)])
    assert docker.entry is not None and not any(command[1] == "rm" for command in docker.calls)


@pytest.mark.asyncio
async def test_explicit_retirement_retry_does_not_stop_replacement(storage):
    files, _, _, mounts, provider = storage
    space = await home(files)
    first = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    await mounts.retire(actor=ALICE, space_id=space.id, expected_generation=1, attachment_ids=[first.id])
    replacement = await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    await mounts.retire(actor=ALICE, space_id=space.id, expected_generation=1, attachment_ids=[first.id])
    assert replacement.container_id in provider.containers


@pytest.mark.asyncio
async def test_adapter_loss_never_grants_a_host_edit_window(storage):
    from deerflow.spaces.registry import SpaceRegistry
    from deerflow.spaces.service import SpaceFiles

    files, sf, catalog, mounts, _ = storage
    space = await home(files)
    await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(space.id, 1, "home", writable=True)])
    restarted = SpaceFiles(SpaceRegistry(sf, files.registry._resolver), catalog)
    with pytest.raises(SpaceConflict, match="provider"):
        await restarted.write(actor=ALICE, space_id=space.id, expected_generation=1, operation_id=operation(), path="page", content=b"racy", create=True)


@pytest.mark.asyncio
async def test_joint_view_requires_export_for_copyable_source(storage):
    files, _, _, mounts, _ = storage
    source, destination = await home(files), await home(files)
    await files.registry.set_grant(actor=ALICE, space_id=source.id, subject=BOB, expected_generation=1, permissions=Permission.READ | Permission.ADMIN | Permission.EXPORT, acknowledge_existing_data=True)
    await files.registry.set_grant(actor=ALICE, space_id=source.id, subject=ALICE, expected_generation=2, permissions=Permission.READ)
    with pytest.raises(SpaceDenied, match="EXPORT"):
        await mounts.attach(actor=ALICE, incarnation=operation(), resources=[ResourceMount(source.id, 3, "source"), ResourceMount(destination.id, 1, "destination", writable=True)])
