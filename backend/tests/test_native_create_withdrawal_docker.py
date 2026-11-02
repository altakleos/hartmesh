"""A failed restricted start on a real daemon leaves nothing to refuse the owner with.

The proxy here is a real ``--rm`` container on the real networks of the set, so what
is removed and what reads as gone are the daemon's own answers, on the Engine CI runs
and on the README's minimum when run locally.

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

from deerflow.spaces.docker import StorageHostUnqualified

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
