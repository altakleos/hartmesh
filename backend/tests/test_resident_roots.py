"""Offline qualified-directory projections, with no Docker or native fixtures."""

from dataclasses import replace

import pytest
from test_agent_instances import instances as instances
from test_storage_spaces_docker import ID, PLAN, Docker, adapter

from deerflow.spaces.filesystem import FilesystemUnavailable, UnsafeSpacePath
from deerflow.spaces.readonly_roots import ReadOnlyRoot


@pytest.mark.asyncio
async def test_readonly_root_rejects_replaced_directory_at_the_same_path(instances):
    _agents, _files, _sf, catalog = instances
    volume = catalog.verify(next(iter(catalog.volumes)))
    shared = volume.data_path / "shared"
    shared.mkdir()
    root = ReadOnlyRoot.capture(volume, "shared", "/mnt/user-data/shared")
    assert root.slot_id == volume.spec.slot_id
    assert root.source == str(shared)
    root.validate()
    shared.rename(shared.with_name("old-shared"))
    shared.mkdir()
    with pytest.raises(FilesystemUnavailable):
        root.validate()
    current = ReadOnlyRoot.capture(volume, "shared", "/mnt/user-data/shared")
    assert current.revision != root.revision


@pytest.mark.asyncio
async def test_readonly_root_rejects_symlink_and_nested_symlink(instances):
    _agents, _files, _sf, catalog = instances
    volume = catalog.verify(next(iter(catalog.volumes)))
    directory = volume.data_path / "real"
    directory.mkdir()
    (directory / "leaf").mkdir()
    (volume.data_path / "alias").symlink_to(directory)
    with pytest.raises((OSError, UnsafeSpacePath)):
        ReadOnlyRoot.capture(volume, "alias/leaf", "/mnt/user-data/shared")


def test_adapter_accepts_only_exact_readonly_configured_mounts_and_network():
    docker = Docker()
    provider = adapter(docker)
    provider._configured_mounts = (("/operator/config", "/opt/config", True),)
    provider._network_of = lambda _plan: "resident-internal"
    docker.entry["HostConfig"]["NetworkMode"] = "resident-internal"
    mount = {"Type": "bind", "Source": "/operator/config", "Destination": "/opt/config", "RW": False, "Propagation": "rprivate"}
    docker.entry["Mounts"].append(mount)
    provider.start(ID, PLAN)
    docker.entry["State"] = {"Status": "created", "Running": False}
    mount["RW"] = True
    with pytest.raises(FilesystemUnavailable):
        provider.start(ID, PLAN)
    mount["RW"] = False
    docker.entry["HostConfig"]["NetworkMode"] = "bridge"
    with pytest.raises(FilesystemUnavailable):
        provider.start(ID, PLAN)


@pytest.mark.asyncio
async def test_adapter_uses_each_plans_readonly_roots_and_checks_revision(instances):
    _agents, _files, _sf, catalog = instances
    volume = catalog.verify(next(iter(catalog.volumes)))
    for leaf in ("first", "second"):
        (volume.data_path / leaf).mkdir()
    roots = [ReadOnlyRoot.capture(volume, leaf, "/mnt/user-data/shared") for leaf in ("first", "second")]
    first, second = [replace(PLAN, readonly_roots=(root,)) for root in roots]
    docker = Docker()
    provider = adapter(docker)
    for plan in (second, first):
        root = plan.readonly_roots[0]
        docker.entry["Mounts"] = [docker.entry["Mounts"][0], {"Type": "bind", "Source": root.source, "Destination": root.destination, "RW": False, "Propagation": "rprivate"}]
        docker.entry["Config"]["Labels"][root.label] = root.revision
        provider._validate(ID, plan)
        docker.entry["Mounts"][1]["RW"] = True
        with pytest.raises(FilesystemUnavailable):
            provider._validate(ID, plan)
        docker.entry["Mounts"][1]["RW"] = False
    docker.entry["Config"]["Labels"][roots[0].label] = roots[1].revision
    with pytest.raises(FilesystemUnavailable):
        provider._validate(ID, first)


@pytest.mark.parametrize("runtime,writable", [("container", False), ("docker", True)])
def test_from_local_backend_refuses_unsupported_runtime_or_writable_configuration(runtime, writable):
    from pathlib import Path
    from types import SimpleNamespace

    from test_resident_topology import backend

    from deerflow.spaces.docker import DockerStorageAdapter, StorageHostUnqualified

    local, calls = backend()
    local._runtime = runtime
    local._config_mounts = [SimpleNamespace(host_path=Path("/operator/config"), container_path="/opt/config", read_only=not writable)]
    with pytest.raises(StorageHostUnqualified):
        DockerStorageAdapter.from_local_backend(local)
    assert calls == []
