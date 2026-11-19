"""Permission repair is an explicit operation on a bound Home directory."""

import os
import stat
from types import SimpleNamespace

import pytest
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.spaces.backings import BackingUnavailable, PreparedVolumeCatalog
from deerflow.spaces.incarnations import BINDING_MARKER, publish_marker, repair_home_mode


@pytest.mark.parametrize("mode", [0, 0o300, 0o400, 0o644, 0o500])
def test_bound_home_mode_repairs_only_the_pinned_root(tmp_path, mode):
    assert os.geteuid() != 0, "Exercise Home directory access as its unprivileged owner"
    data, control = tmp_path / "data", tmp_path / "control"
    data.mkdir()
    control.mkdir(mode=0o700)
    child = data / "note"
    child.touch(mode=0o600)
    root = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY)
    directory = os.open(data, os.O_PATH | os.O_NOFOLLOW | os.O_DIRECTORY)
    private = os.open(control, os.O_RDONLY | os.O_NOFOLLOW | os.O_DIRECTORY)
    try:
        handle = "1" * 32
        publish_marker(SimpleNamespace(_control_fd=private), handle)
        inodes = os.fstat(directory).st_ino, os.fstat(private).st_ino
        volume = SimpleNamespace(data_path=data, device=data.stat().st_dev)
        data.chmod(mode)
        with pytest.raises(BackingUnavailable):
            repair_home_mode(volume, root, directory, private, "2" * 32, inodes)
        assert stat.S_IMODE(data.stat().st_mode) == mode
        repair_home_mode(volume, root, directory, private, handle, inodes)
        assert stat.S_IMODE(data.stat().st_mode) == 0o770
        assert stat.S_IMODE(child.stat().st_mode) == 0o600
        assert (control / BINDING_MARKER).read_text() == handle
    finally:
        os.close(private)
        os.close(directory)
        os.close(root)


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["current", "changed-marker", "other-inode", "lifecycle", "reassigned", "journal", "held-lock", "wrong-kind", "wrong-custodian"])
async def test_maintenance_reloads_the_bound_home_under_lifecycle_exclusion(instances, case):
    from contextlib import asynccontextmanager
    from unittest.mock import Mock

    from sqlalchemy import select

    from app.gateway.agent_home_maintenance import repair_home_directories
    from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
    from deerflow.persistence.spaces.model import SpaceRow
    from deerflow.spaces.release_filesystem import PREFIX
    from deerflow.spaces.workflows import acquire, close

    assert os.geteuid() != 0
    agents, files, sf, fixture = instances
    instance = await create(agents)
    async with sf() as session:
        binding = await session.scalar(select(SpaceBackingRow).where(SpaceBackingRow.space_id == instance.home_id))
        generation = (await session.get(SpaceRow, instance.home_id)).generation
    volume = fixture.verified[binding.slot_id]
    for v in fixture.verified.values():
        v.spec.kind = None
        v.spec.mount_path = v.data_path.parent
        v.data_path.chmod(0o500)
    catalog = Mock(spec=PreparedVolumeCatalog)
    catalog.volumes = fixture.volumes
    files.catalog = catalog

    def physical_confirmation(slot, *, previous, backing_handle, inodes):
        assert slot == binding.slot_id
        root = os.open(volume.data_path.parent, os.O_RDONLY | os.O_DIRECTORY)
        data = os.open(volume.data_path, os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW)
        private = os.open(volume.control_path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            repair_home_mode(volume, root, data, private, backing_handle, inodes)
        finally:
            os.close(private)
            os.close(data)
            os.close(root)
        return volume

    catalog.repair_bound_home.side_effect = physical_confirmation
    held = []
    if case == "journal":
        (volume.data_path.parent / (PREFIX + "0" * 32)).mkdir()
    if case == "held-lock":
        held = acquire({instance.home_id: volume})
    if case == "wrong-kind":
        volume.spec.kind = "employee" if instance.custody == "personal" else "assistant"
    if case == "wrong-custodian":
        async with sf.begin() as session:
            row = await session.get(SpaceRow, instance.home_id)
            row.custody_kind, row.custodian_kind, row.custodian_id = "personal", "human", "someone-else"
    if case == "changed-marker":
        (volume.control_path / BINDING_MARKER).write_text("0" * 32, encoding="ascii")
    if case == "other-inode":
        volume.data_path.rename(volume.data_path.with_name("previous"))
        volume.data_path.mkdir(mode=0o500)
    if case == "lifecycle":
        async with sf.begin() as session:
            session.add(SpaceFileOperationRow(space_id=instance.home_id, generation=generation, operation_id="e" * 32, actor_kind="human", actor_id="alice", request={"action": "release"}, phase="pending"))
    if case == "reassigned":
        locked = files.registry.host_locked

        @asynccontextmanager
        async def reassigned(ids, **options):
            async with sf.begin() as session:
                (await session.get(SpaceRow, instance.home_id)).generation += 1
            async with locked(ids, **options) as result:
                yield result

        files.registry.host_locked = reassigned
    status = SimpleNamespace(unverified={binding.slot_id: "previous observation"})
    app = SimpleNamespace(state=SimpleNamespace(storage_spaces=files, agent_execution=status))
    try:
        await repair_home_directories(app)
        assert stat.S_IMODE(volume.data_path.stat().st_mode) == (0o770 if case == "current" else 0o500)
        assert (binding.slot_id not in status.unverified) == (case == "current")
        assert all(stat.S_IMODE(v.data_path.stat().st_mode) == 0o500 for v in fixture.verified.values() if v is not volume)
    finally:
        close(held)
        for v in fixture.verified.values():
            v.data_path.chmod(0o700)
