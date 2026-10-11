"""Actual Docker inspection, rather than configured binds, gates chat reuse."""

import json
from types import SimpleNamespace

import pytest
from test_compose_chat_storage import pinned_chat_volume

from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
from deerflow.community.aio_sandbox.sandbox_info import SandboxInfo
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.chat_paths import ThreadMounts


@pytest.mark.parametrize("extra", ["control", "unused-slot", "source-alias", "overlay"])
def test_planned_mount_exposure_is_refused_before_any_container_start(tmp_path, extra):
    backend = LocalContainerBackend.__new__(LocalContainerBackend)
    backend._runtime = "docker"
    volume = pinned_chat_volume(tmp_path / "unused")
    source = volume.control_path if extra == "control" else volume.data_path
    target = "/policy"
    if extra == "source-alias":
        alias = tmp_path / "alias"
        alias.symlink_to(source, target_is_directory=True)
        source = alias
    elif extra == "overlay":
        source, target = tmp_path / "ordinary", "/mnt/user-data/outputs"
    backend._config_mounts = [SimpleNamespace(host_path=str(source), container_path=target, read_only=True)]
    with pytest.raises(BackingUnavailable):
        backend.validate_planned_thread_mounts([], mounts=ThreadMounts("alice", "chat-a"), protected_roots=(volume.spec.mount_path,))


def inspection_fixture(tmp_path, *, personal=True, shared=True):
    user = pinned_chat_volume(tmp_path) if personal else None
    company = pinned_chat_volume(tmp_path / "company") if personal and shared else None
    mounts = ThreadMounts("alice", "chat-a", user, company)
    backend = LocalContainerBackend.__new__(LocalContainerBackend)
    backend._runtime = "docker"
    identifier = "a" * 64
    info = SandboxInfo("scope", "http://localhost:8080", container_name="sandbox-scope", container_id=identifier)
    entry = {
        "Name": "/sandbox-scope",
        "Id": identifier,
        "Config": {"Labels": {**backend._owner_labels("alice", "chat-a"), **mounts.labels}},
        "Mounts": [{"Type": "bind", "Source": source, "Destination": target, "RW": not read_only, "Propagation": "rprivate"} for source, target, read_only in mounts.binds],
    }
    protected = tuple(volume.spec.mount_path for volume in (user, company) if volume is not None)
    return backend, info, mounts, entry, protected


def run_validation(monkeypatch, fixture):
    backend, info, mounts, entry, protected = fixture
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.subprocess.run", lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=json.dumps([entry]), stderr=""))
    return backend.validate_thread_mounts(info, mounts=mounts, protected_roots=protected)


@pytest.mark.parametrize("personal,shared", [(False, False), (True, False), (True, True)])
def test_exact_mounts_and_slot_labels_validate_from_docker(monkeypatch, tmp_path, personal, shared):
    fixture = inspection_fixture(tmp_path, personal=personal, shared=shared)
    assert run_validation(monkeypatch, fixture) == fixture[1].container_id


@pytest.mark.parametrize(
    "fault", ["missing", "wrong-source", "wrong-destination", "readonly-user", "writable-shared", "shared-propagation", "volume-type", "unknown-mounts", "missing-slot-label", "extra-slot-label", "foreign-owner", "replaced-container"]
)
def test_bad_mount_facts_or_identity_are_never_adopted(monkeypatch, tmp_path, fault):
    fixture = inspection_fixture(tmp_path)
    _backend, _info, _mounts, entry, _protected = fixture
    if fault == "missing":
        entry["Mounts"].pop(0)
    elif fault == "wrong-source":
        entry["Mounts"][0]["Source"] += "-wrong"
    elif fault == "wrong-destination":
        entry["Mounts"][0]["Destination"] += "-wrong"
    elif fault == "readonly-user":
        entry["Mounts"][0]["RW"] = False
    elif fault == "writable-shared":
        next(m for m in entry["Mounts"] if m["Destination"].endswith("/shared"))["RW"] = True
    elif fault == "shared-propagation":
        entry["Mounts"][0]["Propagation"] = "rshared"
    elif fault == "volume-type":
        entry["Mounts"][0]["Type"] = "volume"
    elif fault == "unknown-mounts":
        del entry["Mounts"]
    elif fault == "missing-slot-label":
        entry["Config"]["Labels"].pop(next(k for k in entry["Config"]["Labels"] if k.startswith("hartmesh.storage.space.")))
    elif fault == "extra-slot-label":
        entry["Config"]["Labels"]["hartmesh.storage.space." + "f" * 32] = "1"
    elif fault == "foreign-owner":
        entry["Config"]["Labels"].update(_backend._owner_labels("bob", "chat-a"))
    elif fault == "replaced-container":
        entry["Id"] = "b" * 64
    with pytest.raises(BackingUnavailable):
        run_validation(monkeypatch, fixture)


@pytest.mark.parametrize("source_kind,target", [("whole-data", "/extra"), ("control", "/extra"), ("projects", "/extra"), ("slot-parent", "/extra"), ("nested", "/mnt/user-data/outputs/.extra"), ("userdata-root", "/mnt/user-data")])
def test_extra_space_exposure_and_nested_overlays_are_refused(monkeypatch, tmp_path, source_kind, target):
    fixture = inspection_fixture(tmp_path)
    _backend, _info, mounts, entry, _protected = fixture
    root = mounts.user.data_path
    source = {"whole-data": root, "control": root.parent / "control", "projects": root / "projects", "slot-parent": root.parent, "nested": tmp_path / "unrelated", "userdata-root": tmp_path / "unrelated"}[source_kind]
    entry["Mounts"].append({"Type": "bind", "Source": str(source), "Destination": target, "RW": False, "Propagation": "rprivate"})
    with pytest.raises(BackingUnavailable):
        run_validation(monkeypatch, fixture)


def test_mountless_candidate_cannot_reuse_browser_created_data_binds(monkeypatch, tmp_path):
    backend, info, _mounts, entry, protected = inspection_fixture(tmp_path)
    mounts = ThreadMounts("alice", "chat-a")
    with pytest.raises(BackingUnavailable):
        run_validation(monkeypatch, (backend, info, mounts, entry, protected))


def test_unknown_container_incarnation_cannot_be_adopted_after_discovery(monkeypatch, tmp_path):
    fixture = inspection_fixture(tmp_path)
    _backend, info, _mounts, entry, _protected = fixture
    info.container_id = None
    entry["Id"] = "unknown"
    with pytest.raises(BackingUnavailable):
        run_validation(monkeypatch, fixture)


def test_mount_type_cannot_hide_a_protected_source(monkeypatch, tmp_path):
    fixture = inspection_fixture(tmp_path)
    _backend, _info, mounts, entry, _protected = fixture
    entry["Mounts"].append({"Type": "volume", "Source": str(mounts.user.control_path), "Destination": "/extra", "RW": False, "Propagation": ""})
    with pytest.raises(BackingUnavailable):
        run_validation(monkeypatch, fixture)


@pytest.mark.parametrize("mode", ["open", "allowlist"])
def test_creation_attributes_every_bound_slot_in_both_network_modes(monkeypatch, mode):
    from test_aio_sandbox_local_backend import _restricted_backend

    backend = _restricted_backend()
    backend._network_mode = mode
    starts = []

    def start(*args, **kwargs):
        starts.append(kwargs["labels"])
        return "a" * 64

    monkeypatch.setattr(backend, "_start_container", start)
    monkeypatch.setattr(backend, "_restricted_resources_status", lambda *args, **kwargs: "missing")
    monkeypatch.setattr(backend, "_create_internal_network", lambda *args: None)
    monkeypatch.setattr(backend, "_create_egress_network", lambda *args: None)
    monkeypatch.setattr(backend, "_start_network_proxy", lambda *args: "10.0.0.2")
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 18080)
    labels = {"hartmesh.storage.space." + "b" * 32: "1", "hartmesh.storage.space." + "c" * 32: "1"}
    info = backend.create("chat-a", "scope", user_id="alice", storage_labels=labels)
    assert len(starts) == 1
    assert all(starts[0][key] == value for key, value in labels.items())
    assert starts[0]["hartmesh.chat.storage"] == "on"
    assert info.chat_storage_enabled is True
    assert "chat_storage_enabled" not in info.to_dict()


@pytest.mark.parametrize("wanted", [True, False])
def test_discovery_requests_fenced_replacement_across_storage_mode_change(monkeypatch, tmp_path, wanted):
    from test_aio_sandbox_local_backend import _backend_for_inspect_tests

    from deerflow.community.aio_sandbox.local_backend import _ContainerInspection
    from deerflow.config.paths import Paths, paths_scope
    from deerflow.spaces.chat_paths import ChatStoragePaths

    backend = _backend_for_inspect_tests()
    labels = {"deerflow.sandbox_id": "scope", "deerflow.role": "sandbox", "deerflow.network_mode": "open"}
    if not wanted:
        labels.update({"hartmesh.chat.storage": "on", "hartmesh.storage.space." + "b" * 32: "1"})
    observed = _ContainerInspection(1.0, 18080, labels, "sandbox:latest", frozenset({"bridge"}), container_id="a" * 64)
    monkeypatch.setattr(backend, "_is_container_running", lambda _name: True)
    monkeypatch.setattr(backend, "_batch_inspect", lambda *args, **kwargs: {"sandbox-scope": observed})
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.wait_for_sandbox_ready", lambda *args, **kwargs: True)
    base = Paths(tmp_path / "legacy")
    paths = ChatStoragePaths(base, user_id="alice") if wanted else base
    with paths_scope(paths):
        info = backend.discover("scope")
    assert info is not None and info.requires_replacement
    assert info.chat_storage_enabled is (not wanted)
