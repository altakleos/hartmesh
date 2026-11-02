"""A failed restricted start on a real daemon leaves nothing to refuse the owner with.

The proxy here is a real ``--rm`` container on the real networks of the set, so what
is removed and what reads as gone are the daemon's own answers.

Needs Docker and an image holding ``sleep``, named by HARTMESH_STORAGE_WRITER_IMAGE
and never pulled here. The managed storage job in CI sets it and refuses a skip.

Run with: HARTMESH_STORAGE_WRITER_IMAGE=<image> PYTHONPATH=. uv run --no-sync pytest tests/test_native_create_withdrawal_docker.py -v
"""

import os
import subprocess
from uuid import uuid4

import pytest
from test_native_create_bridge import SLOT
from test_native_create_bridge import fixture as fixture
from test_native_create_local_backend import backend

from deerflow.community.aio_sandbox.native_resource_set import NativeResourceSet, PairedResource
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_teardown_evidence import NativeTeardownResource

IMAGE = os.environ.get("HARTMESH_STORAGE_WRITER_IMAGE")
LABELS = {"hartmesh.storage.space." + SLOT: "1"}

pytestmark = pytest.mark.skipif(not IMAGE, reason="Requires Docker and HARTMESH_STORAGE_WRITER_IMAGE")


def _docker(*arguments):
    return subprocess.run(["docker", *arguments], capture_output=True, text=True, timeout=60)


def _exists(kind, name):
    return _docker(kind, "inspect", name).returncode == 0


@pytest.fixture
def local(fixture, monkeypatch):
    journal, state, bridge, _owner = fixture
    result = backend(bridge)
    result._network_mode, result._network_config = "isolated", {"mode": "isolated"}
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.get_free_port", lambda **kwargs: 19080)
    result.journal, result.state = journal, state
    return result


@pytest.fixture
def sandbox_id(local):
    identifier = "withdraw-" + uuid4().hex[:12]
    yield identifier
    # Only what this test named, so a failing run leaves nothing behind.
    proxy_name, network_name = local._resource_names(identifier)
    _docker("rm", "-f", proxy_name)
    for name in (network_name, local._egress_network_name(identifier)):
        _docker("network", "rm", name)


def _start_proxy(name, network):
    started = _docker("run", "-d", "--rm", "--pull=never", "--name", name, "--network", network, IMAGE, "sleep", "300")
    assert started.returncode == 0, started.stderr


def test_a_real_partial_set_goes_and_the_owner_can_start_again(local, sandbox_id):
    proxy_name, network_name = local._resource_names(sandbox_id)
    egress_name = local._egress_network_name(sandbox_id)
    pending_at_the_failure = []

    def start_a_proxy_then_fail(*args, **kwargs):
        pending_at_the_failure.extend(local.journal.pending())
        _start_proxy(proxy_name, network_name)
        connected = _docker("network", "connect", egress_name, proxy_name)
        assert connected.returncode == 0, connected.stderr
        raise RuntimeError("Proxy reported no address")

    local._start_network_proxy = start_a_proxy_then_fail
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", sandbox_id, user_id="alice", storage_labels=LABELS)

    assert len(pending_at_the_failure) == 1
    assert not _exists("container", proxy_name)
    assert not _exists("network", network_name) and not _exists("network", egress_name)
    assert local.journal.pending() == () and not local.state.pending_for(owner_id="alice")
    assert local._restricted_resources_status(sandbox_id) == "missing"
    local._native_creation.require_admissible(name=f"native-{sandbox_id}", owner_id="alice")


def _census_items(reader, local, sandbox_id):
    """Facts built from the daemon's own answers, for this test's set alone."""
    proxy_name, network_name = local._resource_names(sandbox_id)
    digest = local._network_policy_digest()
    items = []
    for kind, role, name in (("container", "network-proxy", proxy_name), ("network", "network", network_name)):
        entry = reader.inspect(kind, name)
        items.append(PairedResource(NativeTeardownResource(kind, role, entry["Id"], name, entry["Created"]), sandbox_id, local._network_mode, digest))
    return items


def _start_labelled_proxy(local, sandbox_id, *extra):
    proxy_name, network_name = local._resource_names(sandbox_id)
    local._create_internal_network(network_name, sandbox_id)
    labels = [item for key, value in local._restricted_labels(sandbox_id, "network-proxy").items() for item in ("--label", f"{key}={value}")]
    started = _docker("run", "-d", "--rm", "--pull=never", "--name", proxy_name, "--network", network_name, *labels, *extra, IMAGE, "sleep", "300")
    assert started.returncode == 0, started.stderr


def test_a_real_leftover_is_proved_to_hold_no_writer(local, sandbox_id):
    local._network_mode, local._network_config = "blocked", {"mode": "blocked"}
    _start_labelled_proxy(local, sandbox_id)
    reader = NativeResourceSet(local)
    reader.require_no_writer(_census_items(reader, local, sandbox_id))


@pytest.mark.parametrize("fault", ["host-mount", "stranger-on-the-network"])
def test_a_real_leftover_that_could_hold_a_writer_is_refused(local, sandbox_id, tmp_path, fault):
    local._network_mode, local._network_config = "blocked", {"mode": "blocked"}
    _start_labelled_proxy(local, sandbox_id, *(("-v", f"{tmp_path}:/data") if fault == "host-mount" else ()))
    stranger = f"withdraw-stranger-{sandbox_id}"
    if fault == "stranger-on-the-network":
        started = _docker("run", "-d", "--rm", "--pull=never", "--name", stranger, "--network", local._resource_names(sandbox_id)[1], IMAGE, "sleep", "300")
        assert started.returncode == 0, started.stderr
    try:
        reader = NativeResourceSet(local)
        with pytest.raises(StorageHostUnqualified):
            reader.require_no_writer(_census_items(reader, local, sandbox_id))
    finally:
        _docker("rm", "-f", stranger)


def test_a_set_it_cannot_prove_gone_keeps_the_owner_refused(local, sandbox_id, monkeypatch):
    monkeypatch.setattr("deerflow.community.aio_sandbox.local_backend.time.sleep", lambda seconds: None)
    proxy_name, network_name = local._resource_names(sandbox_id)

    def start_a_proxy_then_fail(*args, **kwargs):
        _start_proxy(proxy_name, network_name)
        raise RuntimeError("Proxy reported no address")

    local._start_network_proxy = start_a_proxy_then_fail
    local._cleanup_restricted_resources = lambda *args, **kwargs: None
    with pytest.raises(StorageHostUnqualified):
        local.create("chat", sandbox_id, user_id="alice", storage_labels=LABELS)

    assert _exists("container", proxy_name)
    assert len(local.journal.pending()) == 1 and local.state.pending_for(owner_id="alice")
