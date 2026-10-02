"""Shared doubles for sandbox provider tests: a counted container backend and a provider built on it.

Every backend here is a fake. Tests built on these establish ordering,
identity and lifecycle invariants; they say nothing about a real container
runtime's performance.
"""

from __future__ import annotations

import copy
import importlib
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

from deerflow.community.aio_sandbox.ownership.memory import MemoryOwnershipStore
from deerflow.config.paths import Paths
from deerflow.config.sandbox_config import SandboxOwnershipConfig
from deerflow.sandbox.acquire_serialization import AcquireSerializer


def _aio_mod():
    return importlib.import_module("deerflow.community.aio_sandbox.aio_sandbox_provider")


class _FakeBackend:
    """A counted container backend: no Docker, no sleeps, exact call records.

    It answers ``create`` the way ``LocalContainerBackend`` does: a fresh start
    is reported as ``created``, and (with ``adopt_on_conflict``) a running
    container under the deterministic name is returned as ``rediscovered``.
    The provenance is set as an attribute rather than a constructor argument
    so this file still collects against a ``SandboxInfo`` that predates it, and
    the lifecycle tests below then fail on behaviour rather than on import.
    """

    def __init__(self, *, adopt_on_conflict: bool = False, discoverable: bool = False) -> None:
        self.created: list[str] = []
        self.destroyed: list[str] = []
        self.adopted: list[str] = []
        self.alive: dict[str, bool] = {}
        self.unverifiable: set[str] = set()
        self.infos: dict[str, object] = {}
        # ``LocalContainerBackend.create`` answers a Docker name conflict by
        # discovering and returning the running container; this mirrors it.
        self.adopt_on_conflict = adopt_on_conflict
        # Whether ``discover`` answers for running containers, as the local
        # backend's does; off by default so the older tests keep their shape.
        self.discoverable = discoverable

    def create(self, thread_id, sandbox_id, **_kwargs):
        if self.adopt_on_conflict and self.alive.get(sandbox_id):
            self.adopted.append(sandbox_id)
            found = copy.copy(self.infos[sandbox_id])
            found.provenance = "rediscovered"
            return found
        self.created.append(sandbox_id)
        self.alive[sandbox_id] = True
        self.infos[sandbox_id] = self._unused_info(sandbox_id)
        started = copy.copy(self.infos[sandbox_id])
        started.provenance = "created"
        return started

    def _unused_info(self, sandbox_id):
        aio = _aio_mod()
        return aio.SandboxInfo(
            sandbox_id=sandbox_id,
            sandbox_url=f"http://sandbox/{sandbox_id}",
            container_name=f"deer-flow-sandbox-{sandbox_id}",
        )

    def destroy(self, info) -> None:
        self.destroyed.append(info.sandbox_id)
        self.alive[info.sandbox_id] = False

    def is_alive(self, info) -> bool:
        if info.sandbox_id in self.unverifiable:
            raise RuntimeError("daemon did not answer")
        return self.alive.get(info.sandbox_id, True)

    def discover(self, sandbox_id):
        if self.discoverable and self.alive.get(sandbox_id):
            return copy.copy(self.infos[sandbox_id])
        return None

    def list_running(self):
        return []


def _make_provider(tmp_path, monkeypatch, *, replicas: int = 2, adopt_on_conflict: bool = False, discoverable: bool = False) -> tuple[object, _FakeBackend]:
    """A provider wired to a fake backend, with no threads and no real config.

    ``replicas=2`` matches the consumed profile's two sandbox slots, which is
    what makes the eviction in these tests the same eviction the deployment
    saw.
    """
    aio = _aio_mod()
    provider = aio.AioSandboxProvider.__new__(aio.AioSandboxProvider)
    provider._config = {"idle_timeout": 600, "replicas": replicas}
    provider._sandboxes = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._warm_pool = {}
    provider._active_sandbox_identity = {}
    provider._warm_pool_identity = {}
    provider._unowned_since = {}
    provider._last_activity = {}
    provider._local_teardown = set()
    provider._starting = set()
    provider._cleanup_pending = {}
    provider._acquire_epoch = {}
    provider._acquire_epoch_counter = 0
    provider._acquire_inflight = {}
    provider._acquire_serializer = AcquireSerializer(thread_name_prefix="sandbox-provider-test")
    provider._acquire_worker_executor = _aio_mod().ThreadPoolExecutor(thread_name_prefix="sandbox-provider-test-worker")
    provider._lock = threading.Lock()
    provider._idle_checker_stop = MagicMock()
    provider._idle_checker_thread = None
    provider._renewal_stop = MagicMock()
    provider._renewal_thread = None
    provider._shutdown_called = False
    provider._owner_id = "warm-reuse-worker"
    provider._ownership_config = SandboxOwnershipConfig()
    provider._ownership = MemoryOwnershipStore(owner_id="warm-reuse-worker", ttl_seconds=600)

    backend = _FakeBackend(adopt_on_conflict=adopt_on_conflict, discoverable=discoverable)
    provider._backend = backend

    paths = Paths(base_dir=tmp_path / "state")
    monkeypatch.setattr(aio, "get_paths", lambda: paths)
    monkeypatch.setattr(aio, "get_effective_user_id", lambda: None)
    monkeypatch.setattr(aio, "wait_for_sandbox_ready", lambda _url, timeout=60, **_kw: True)

    async def _ready_async(_url, timeout=60, **_kw):
        return True

    monkeypatch.setattr(aio, "wait_for_sandbox_ready_async", _ready_async)
    monkeypatch.setattr(
        aio,
        "get_app_config",
        lambda: SimpleNamespace(skills=SimpleNamespace(container_path="/mnt/skills")),
    )
    monkeypatch.setattr(aio.AioSandboxProvider, "_get_extra_mounts", lambda *_a, **_k: [])
    monkeypatch.setattr(aio.AioSandboxProvider, "_lark_integration_active", lambda *_a, **_k: False)
    monkeypatch.setattr(aio.AioSandboxProvider, "_lark_broker_active", lambda *_a, **_k: False)
    monkeypatch.setattr(aio.AioSandboxProvider, "_local_config_mount_exclusion_root", lambda *_a, **_k: None)
    monkeypatch.setattr(aio.AioSandboxProvider, "_ensure_skills_projection", staticmethod(lambda _user_id: None))
    return provider, backend
