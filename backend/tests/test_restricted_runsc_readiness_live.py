"""Live regression: the released restricted topology under runsc, end to end.

The existing live smoke tests start the image directly (``backend.create`` on
Docker's default runtime) and drive the relay with a throwaway image. Neither
exercises what the released Compose profile actually does on a tenant VM: the
allowlist topology (per-sandbox internal and egress networks, the authenticated
relay sidecar), the released resource limits and hardening (``--cpus 1``, 1 GiB
memory, 384 pids, uid 1000, ``--cap-drop=ALL``, ``no-new-privileges``, Docker's
built-in seccomp), gVisor's ``runsc``, and the provider's own readiness budget
with its ownership-fenced teardown. This module does, through both acquisition
paths of the real provider, with a never-ready control.

Everything here consumes the *released* values: the sandbox section of
``deploy/compose/config.yaml`` (image digest, proxy image digest, allowlist,
``ready_timeout``) and the Gateway's ``DEER_FLOW_SANDBOX_*`` environment from
``deploy/compose/compose.yaml``. The budget the tests wait for is the one the
Gateway would enforce, resolved by the same function; no test extends it.

Opt in with ``pytest -m live tests/test_restricted_runsc_readiness_live.py -s``
on a host whose Docker daemon (28+) registers a ``runsc`` runtime; anywhere else
the module skips. Each cold start costs about a CPU-minute and a gibibyte, and
the never-ready controls run the whole budget on purpose. On failure the tests
print the inner listener state and the python-server/nginx program logs, with
the relay token redacted; they never print ``docker inspect`` output for the
sidecar, whose environment carries that token.

Records are printed as one JSON line per cold start (``create_seconds`` is
``docker run`` and the sidecar, measured separately from ``readiness_seconds``,
the poll after that; ``polls`` is the sample count; ``margin_seconds`` is the
budget minus readiness). One passing run on one host is a data point, not a
fleet-wide bound; the profile README says which hosts the estate must measure.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import threading
import time
from pathlib import Path

import pytest
import requests
import yaml

from deerflow.community.aio_sandbox.local_backend import RELAY_AUTH_HEADER, LocalContainerBackend

pytestmark = pytest.mark.live

REPO = Path(__file__).resolve().parents[2]
PROFILE = REPO / "deploy" / "compose"
TEMPLATE = PROFILE / "config.yaml"
COMPOSE = PROFILE / "compose.yaml"
CONTAINER_PREFIX = "hartmesh-pz-live"
# The documented allowance for the ownership-fenced teardown after the budget
# runs out: two container stops (Docker's 10 s SIGKILL escalation each), the
# sidecar removal and two network removals, with room for a slow daemon.
CLEANUP_ALLOWANCE_SECONDS = 60.0
# How long a never-ready control runs before its diagnostics are taken: long
# enough for the image's services to have started failing.
SETTLE_SECONDS = 25.0
REDACTED = "<relay-token redacted>"
# How many cold starts each healthy test performs (serially, and as pairs for
# the concurrent test). One is a regression run; the acceptance gate wants
# repeated starts on each representative host.
SAMPLES = max(1, int(os.environ.get("HARTMESH_READINESS_SAMPLES", "1")))


# ── Prerequisites and released values ───────────────────────────────────────


def _docker_info() -> dict | None:
    try:
        result = subprocess.run(["docker", "info", "--format", "{{json .}}"], capture_output=True, text=True, timeout=30, check=True)
        return json.loads(result.stdout)
    except Exception:
        return None


def _require_runsc_daemon() -> dict:
    info = _docker_info()
    if info is None:
        pytest.skip("requires a running Docker daemon")
    if "runsc" not in (info.get("Runtimes") or {}):
        pytest.skip("requires a Docker daemon with a registered runsc runtime")
    major = int(str(info.get("ServerVersion", "0")).split(".")[0] or 0)
    if major < 28:
        pytest.skip("restricted sandbox networks require Docker Engine 28+")
    return info


def _released_sandbox_section() -> dict:
    return yaml.safe_load(TEMPLATE.read_text(encoding="utf-8"))["sandbox"]


def _released_gateway_environment() -> dict[str, str]:
    """The Gateway's DEER_FLOW_SANDBOX_* limits and hardening, as compose.yaml ships them."""
    environment = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))["services"]["gateway"]["environment"]
    released = {key: str(value) for key, value in environment.items() if key.startswith("DEER_FLOW_SANDBOX_") and "${" not in str(value)}
    # Addressing is per deployment; limits and hardening are what is under test.
    for key in ("DEER_FLOW_SANDBOX_HOST", "DEER_FLOW_SANDBOX_NETWORK"):
        released.pop(key, None)
    released["DEER_FLOW_SANDBOX_RUNTIME"] = "runsc"  # ${SANDBOX_RUNTIME} on the tenant VM
    return released


def _production_budget() -> float:
    """The budget the released Gateway enforces, resolved the way it resolves it."""
    from deerflow.community.aio_sandbox.aio_sandbox_provider import resolve_ready_timeout
    from deerflow.config.sandbox_config import SandboxConfig

    return resolve_ready_timeout(SandboxConfig(**_released_sandbox_section()).ready_timeout)


def _apply_released_environment(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    for key in [key for key in os.environ if key.startswith("DEER_FLOW_SANDBOX_")]:
        monkeypatch.delenv(key)
    released = _released_gateway_environment()
    for key, value in released.items():
        monkeypatch.setenv(key, value)
    return released


def _released_backend(section: dict, *, extra_environment: dict[str, str] | None = None, base_port: int = 18400) -> LocalContainerBackend:
    return LocalContainerBackend(
        image=section["image"],
        base_port=base_port,
        container_prefix=CONTAINER_PREFIX,
        config_mounts=[],
        environment=dict(extra_environment or {}),
        network_config=dict(section["network"]),
    )


def _live_provider(backend: LocalContainerBackend, budget: float, monkeypatch: pytest.MonkeyPatch):
    """A real AioSandboxProvider over *backend*, built without an AppConfig."""
    from deerflow.community.aio_sandbox import aio_sandbox_provider as aio_mod
    from deerflow.community.aio_sandbox.ownership.memory import MemoryOwnershipStore
    from deerflow.config.sandbox_config import SandboxOwnershipConfig
    from deerflow.sandbox.acquire_serialization import AcquireSerializer

    provider = aio_mod.AioSandboxProvider.__new__(aio_mod.AioSandboxProvider)
    provider._lock = threading.Lock()
    provider._sandboxes = {}
    provider._sandbox_infos = {}
    provider._thread_sandboxes = {}
    provider._acquire_serializer = AcquireSerializer(thread_name_prefix="aio-sandbox-lock-wait")
    provider._last_activity = {}
    provider._warm_pool = {}
    provider._active_sandbox_identity = {}
    provider._warm_pool_identity = {}
    provider._unowned_since = {}
    provider._local_teardown = set()
    provider._starting = set()
    provider._acquire_epoch = {}
    provider._acquire_epoch_counter = 0
    provider._acquire_inflight = {}
    provider._shutdown_called = False
    provider._idle_checker_stop = threading.Event()
    provider._idle_checker_thread = None
    provider._renewal_stop = threading.Event()
    provider._renewal_thread = None
    provider._config = {"idle_timeout": 0, "replicas": 2, "ready_timeout": budget}
    provider._backend = backend
    provider._owner_id = "pz-live"
    provider._ownership_config = SandboxOwnershipConfig()
    provider._ownership = MemoryOwnershipStore(owner_id="pz-live", ttl_seconds=600)
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_get_extra_mounts", lambda self, thread_id, *, user_id=None, accepted_skills_only=False: [])
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_lark_integration_active", staticmethod(lambda user_id=None: False))
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_lark_broker_active", staticmethod(lambda user_id=None: False))
    monkeypatch.setattr(aio_mod.AioSandboxProvider, "_local_config_mount_exclusion_root", lambda self, thread_id, *, user_id=None: None)
    return provider


# ── Observation helpers ─────────────────────────────────────────────────────


class _CreateTimer:
    """Wraps ``backend.create`` to time it and keep its result for the observer."""

    def __init__(self, backend: LocalContainerBackend) -> None:
        self.backend = backend
        self.real_create = backend.create
        self.started: dict[str, float] = {}
        self.seconds: dict[str, float] = {}
        self.infos: dict[str, object] = {}
        self.ready = threading.Event()
        backend.create = self  # type: ignore[method-assign]

    def __call__(self, thread_id, sandbox_id, **kwargs):
        self.started[sandbox_id] = time.monotonic()
        info = self.real_create(thread_id, sandbox_id, **kwargs)
        self.seconds[sandbox_id] = time.monotonic() - self.started[sandbox_id]
        self.infos[sandbox_id] = info
        self.ready.set()
        return info

    def tokens(self) -> list[str]:
        return [info.request_headers[RELAY_AUTH_HEADER] for info in self.infos.values() if info.request_headers.get(RELAY_AUTH_HEADER)]


class _ProbeCounter:
    """Counts the production poller's probes without changing its behaviour."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import httpx

        from deerflow.community.aio_sandbox import backend as readiness

        counter = self
        self.count = 0
        real_session = requests.Session
        real_client = httpx.AsyncClient

        class CountingSession(real_session):  # type: ignore[misc,valid-type]
            def get(self, url, **kwargs):
                counter.count += 1
                return super().get(url, **kwargs)

        class CountingClient(real_client):  # type: ignore[misc,valid-type]
            async def get(self, url, **kwargs):
                counter.count += 1
                return await super().get(url, **kwargs)

        monkeypatch.setattr(readiness.requests, "Session", CountingSession)
        monkeypatch.setattr(readiness.httpx, "AsyncClient", CountingClient)


def _run(cmd: list[str], timeout: float = 30) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def _inspect(name: str) -> dict | None:
    result = _run(["docker", "inspect", name])
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)[0]


def _network_exists(name: str) -> bool:
    return _run(["docker", "network", "inspect", name]).returncode == 0


def _names(backend: LocalContainerBackend, sandbox_id: str) -> tuple[str, str, str, str]:
    proxy, network = backend._resource_names(sandbox_id)
    return f"{CONTAINER_PREFIX}-{sandbox_id}", proxy, network, backend._egress_network_name(sandbox_id)


def _resources_present(backend: LocalContainerBackend, sandbox_id: str) -> dict[str, bool]:
    container, proxy, network, egress = _names(backend, sandbox_id)
    return {
        "sandbox": _inspect(container) is not None,
        "sidecar": _inspect(proxy) is not None,
        "internal network": _network_exists(network),
        "egress network": _network_exists(egress),
    }


def _diagnostics(backend: LocalContainerBackend, sandbox_id: str, *, secrets: list[str]) -> str:
    """Inner listener state plus the python-server and nginx program logs.

    Deliberately never ``docker inspect``: the sidecar's environment carries
    the relay token. Every known token is scrubbed from what is collected.
    """
    container, proxy, _network, _egress = _names(backend, sandbox_id)
    parts: list[str] = []

    def collect(label: str, cmd: list[str]) -> None:
        try:
            result = _run(cmd, timeout=30)
            text = result.stdout + result.stderr
        except Exception as exc:  # noqa: BLE001 -- a diagnostics dump must not itself fail the test
            text = f"<{type(exc).__name__}: {exc}>"
        parts.append(f"--- {label} ---\n" + "\n".join(text.splitlines()[-60:]))

    collect("sandbox container log (tail)", ["docker", "logs", "--tail", "60", container])
    collect("sandbox listeners", ["docker", "exec", container, "sh", "-c", "ss -ltnp 2>/dev/null || netstat -ltn 2>/dev/null || cat /proc/net/tcp"])
    collect(
        "sandbox program logs",
        ["docker", "exec", container, "sh", "-c", 'for f in /var/log/gem/python-server.log /var/log/gem/nginx-error.log /var/log/gem/nginx-access.log; do echo "== $f =="; tail -n 25 "$f" 2>/dev/null; done'],
    )
    collect("relay sidecar log (tail)", ["docker", "logs", "--tail", "30", proxy])
    text = "\n".join(parts)
    for secret in secrets:
        if secret:
            text = text.replace(secret, REDACTED)
    return text


def _probe(url: str, headers: dict[str, str]) -> int | None:
    try:
        with requests.Session() as session:
            session.trust_env = False
            return session.get(f"{url}/v1/sandbox", headers=headers, timeout=5).status_code
    except requests.RequestException:
        return None


def _assert_released_hardening(backend: LocalContainerBackend, sandbox_id: str) -> None:
    container, proxy, network, egress = _names(backend, sandbox_id)
    sandbox = _inspect(container)
    sidecar = _inspect(proxy)
    assert sandbox is not None and sidecar is not None
    host = sandbox["HostConfig"]
    assert host["Runtime"] == "runsc"
    assert host["NanoCpus"] == 1_000_000_000, "the released profile caps every sandbox at one CPU"
    assert host["Memory"] == 1024 * 1024 * 1024 and host["MemorySwap"] == host["Memory"]
    assert host["PidsLimit"] == 384
    assert host["CapDrop"] == ["ALL"] and not host.get("CapAdd")
    assert "no-new-privileges" in host["SecurityOpt"] and "seccomp=builtin" in host["SecurityOpt"]
    assert sandbox["Config"]["User"] == "1000:1000"
    assert not host.get("PortBindings"), "the sandbox publishes no port of its own; the relay does"
    assert set(sandbox["NetworkSettings"]["Networks"]) == {network}, "the sandbox sits on its internal network only"
    assert set(sidecar["NetworkSettings"]["Networks"]) == {network, egress}
    assert sidecar["HostConfig"]["Runtime"] != "runsc", "the sidecar keeps the daemon's default runtime"
    bindings = sidecar["HostConfig"]["PortBindings"]
    assert set(bindings) == {"8080/tcp"}
    assert all(binding["HostIp"] in {"127.0.0.1", "::1", "[::1]"} for binding in bindings["8080/tcp"]), "the relay is published on loopback only"


def _record(**fields: object) -> None:
    print("READINESS_RECORD " + json.dumps(fields, sort_keys=True), flush=True)


# ── Healthy slow startup, both acquisition paths ────────────────────────────


@pytest.mark.parametrize("path", ["sync", "async"])
def test_restricted_runsc_sandbox_becomes_ready_within_the_production_budget(monkeypatch: pytest.MonkeyPatch, path: str) -> None:
    """A released-profile cold start reaches ``/v1/sandbox`` through the
    authenticated relay inside the Gateway's own budget, on both paths."""
    _require_runsc_daemon()
    _apply_released_environment(monkeypatch)
    section = _released_sandbox_section()
    budget = _production_budget()
    backend = _released_backend(section)
    provider = _live_provider(backend, budget, monkeypatch)
    timer = _CreateTimer(backend)
    for sample in range(SAMPLES):
        _one_healthy_cold_start(monkeypatch, provider, backend, timer, budget, path=path, sandbox_id=f"pz-{path}-{sample}", sample=sample)


def _one_healthy_cold_start(monkeypatch: pytest.MonkeyPatch, provider, backend: LocalContainerBackend, timer: _CreateTimer, budget: float, *, path: str, sandbox_id: str, sample: int) -> None:
    probes = _ProbeCounter(monkeypatch)
    thread_id = f"thread-{sandbox_id}"

    started = time.monotonic()
    try:
        if path == "sync":
            assert provider._create_sandbox(thread_id, sandbox_id, user_id="user-pz") == sandbox_id
        else:
            assert asyncio.run(provider._create_sandbox_async(thread_id, sandbox_id, user_id="user-pz")) == sandbox_id
    except RuntimeError as exc:
        dump = _diagnostics(backend, sandbox_id, secrets=timer.tokens()) if timer.infos else "<the backend never returned from create>"
        pytest.fail(f"{path} acquisition did not become ready within the production budget ({budget:g}s): {exc}\n{dump}")
    total = time.monotonic() - started
    create_seconds = timer.seconds[sandbox_id]
    readiness_seconds = total - create_seconds
    _record(
        path=path,
        sample=sample,
        runtime="runsc",
        cpus=1,
        create_seconds=round(create_seconds, 3),
        readiness_seconds=round(readiness_seconds, 3),
        polls=probes.count,
        budget_seconds=budget,
        margin_seconds=round(budget - readiness_seconds, 3),
    )
    try:
        assert readiness_seconds <= budget
        info = provider._sandbox_infos[sandbox_id]
        assert sandbox_id in provider._sandboxes and sandbox_id not in provider._warm_pool
        assert provider._starting == set()
        assert provider._ownership.owner(sandbox_id) == provider._owner_id
        _assert_released_hardening(backend, sandbox_id)
        # The relay refuses a missing and a wrong token, and admits the right one.
        assert _probe(info.sandbox_url, {}) in {401, 403}
        assert _probe(info.sandbox_url, {RELAY_AUTH_HEADER: "wrong-" + "x" * 40}) in {401, 403}
        assert _probe(info.sandbox_url, dict(info.request_headers)) == 200
        assert backend.is_alive(info)
    finally:
        provider.destroy(sandbox_id)
    present = _resources_present(backend, sandbox_id)
    assert not any(present.values()), f"resources survived destroy: {present}"
    assert sandbox_id not in provider._sandboxes and provider._local_teardown == set()


# ── Two cold starts at once, one CPU each ───────────────────────────────────


def test_two_concurrent_restricted_runsc_cold_starts_each_fit_the_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    """The profile's ceiling is two sandboxes; both starting at once, each on
    its own CPU quota, must each fit the budget on their own."""
    _require_runsc_daemon()
    _apply_released_environment(monkeypatch)
    section = _released_sandbox_section()
    budget = _production_budget()
    backend = _released_backend(section, base_port=18420)
    provider = _live_provider(backend, budget, monkeypatch)
    timer = _CreateTimer(backend)
    for sample in range(SAMPLES):
        _one_concurrent_pair(provider, backend, timer, budget, sample=sample)


def _one_concurrent_pair(provider, backend: LocalContainerBackend, timer: _CreateTimer, budget: float, *, sample: int) -> None:
    ids = [f"pz-conc-{sample}-a", f"pz-conc-{sample}-b"]
    outcomes: dict[str, dict[str, object]] = {sid: {} for sid in ids}

    def start(sid: str) -> None:
        began = time.monotonic()
        try:
            provider._create_sandbox(f"thread-{sid}", sid, user_id="user-pz")
            outcomes[sid]["total"] = time.monotonic() - began
        except BaseException as exc:  # noqa: BLE001 -- reported through the outcome
            outcomes[sid]["error"] = repr(exc)

    workers = [threading.Thread(target=start, args=(sid,), name=sid) for sid in ids]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=budget + CLEANUP_ALLOWANCE_SECONDS + 60)
    try:
        for sid in ids:
            if "error" in outcomes[sid]:
                dump = _diagnostics(backend, sid, secrets=timer.tokens()) if sid in timer.infos else "<create never returned>"
                pytest.fail(f"{sid} failed under concurrent start: {outcomes[sid]['error']}\n{dump}")
            create_seconds = timer.seconds[sid]
            readiness_seconds = float(outcomes[sid]["total"]) - create_seconds
            _record(
                path="sync-concurrent",
                sample=sample,
                sandbox=sid,
                runtime="runsc",
                cpus=1,
                create_seconds=round(create_seconds, 3),
                readiness_seconds=round(readiness_seconds, 3),
                budget_seconds=budget,
                margin_seconds=round(budget - readiness_seconds, 3),
            )
            assert readiness_seconds <= budget
            _assert_released_hardening(backend, sid)
    finally:
        for sid in ids:
            if sid in provider._sandboxes:
                provider.destroy(sid)
    for sid in ids:
        present = _resources_present(backend, sid)
        assert not any(present.values()), f"{sid}: resources survived destroy: {present}"


# ── Never-ready control ─────────────────────────────────────────────────────


@pytest.mark.parametrize("path", ["sync", "async"])
def test_never_ready_control_fails_within_the_budget_and_cleans_up(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture, path: str) -> None:
    """A sandbox whose service can never listen must fail the acquisition
    within the budget plus the cleanup allowance, and leave nothing behind.

    The control is the released image with its service port set to 1: as uid
    1000 with every capability dropped the python server cannot bind it, so
    the inner nginx never has an upstream and ``/v1/sandbox`` never answers.
    The diagnostics are taken while it is still running, which is also how
    this test proves the dump carries the inner state and not the relay token.
    """
    _require_runsc_daemon()
    _apply_released_environment(monkeypatch)
    section = _released_sandbox_section()
    budget = _production_budget()
    backend = _released_backend(section, base_port=18440, extra_environment={"SANDBOX_SRV_PORT": "1"})
    provider = _live_provider(backend, budget, monkeypatch)
    timer = _CreateTimer(backend)
    sandbox_id = f"pz-dead-{path}"
    container = _names(backend, sandbox_id)[0]
    outcome: dict[str, object] = {}

    def acquire() -> None:
        began = time.monotonic()
        try:
            if path == "sync":
                provider._create_sandbox(f"thread-{sandbox_id}", sandbox_id, user_id="user-pz")
            else:
                asyncio.run(provider._create_sandbox_async(f"thread-{sandbox_id}", sandbox_id, user_id="user-pz"))
            outcome["result"] = "ready"
        except RuntimeError as exc:
            outcome["error"] = str(exc)
        except BaseException as exc:  # noqa: BLE001 -- reported through the outcome
            outcome["unexpected"] = repr(exc)
        finally:
            outcome["elapsed"] = time.monotonic() - began

    worker = threading.Thread(target=acquire, name=f"acquire-{sandbox_id}", daemon=True)
    worker.start()
    assert timer.ready.wait(timeout=300), "the backend never returned from create"
    settle_until = timer.started[sandbox_id] + timer.seconds[sandbox_id] + SETTLE_SECONDS
    time.sleep(max(0.0, settle_until - time.monotonic()))
    tokens = timer.tokens()
    assert tokens, "the restricted backend must have minted a relay token"
    dump = _diagnostics(backend, sandbox_id, secrets=tokens)
    for token in tokens:
        assert token not in dump, "the diagnostics dump leaked the relay token"
    assert "--- sandbox listeners ---" in dump and "python-server.log" in dump, dump

    worker.join(timeout=timer.seconds[sandbox_id] + budget + CLEANUP_ALLOWANCE_SECONDS + 30)
    assert not worker.is_alive(), f"the {path} acquisition did not return within the budget plus the cleanup allowance\n{dump}"
    assert "unexpected" not in outcome, f"{outcome}\n{dump}"
    assert "result" not in outcome, f"a sandbox whose service cannot listen became ready: {outcome}\n{dump}"
    assert f"within {budget:g}s" in str(outcome["error"]), outcome
    elapsed = float(outcome["elapsed"])
    create_seconds = timer.seconds[sandbox_id]
    _record(
        path=f"{path}-never-ready",
        runtime="runsc",
        cpus=1,
        create_seconds=round(create_seconds, 3),
        elapsed_seconds=round(elapsed, 3),
        budget_seconds=budget,
        cleanup_seconds=round(elapsed - create_seconds - budget, 3),
        cleanup_allowance_seconds=CLEANUP_ALLOWANCE_SECONDS,
    )
    assert elapsed >= budget, "the acquisition gave up before the budget ran out"
    assert elapsed <= create_seconds + budget + CLEANUP_ALLOWANCE_SECONDS, f"cleanup exceeded its allowance: {elapsed - create_seconds - budget:.1f}s"

    present = _resources_present(backend, sandbox_id)
    refused = [record.getMessage() for record in caplog.records if "Not destroying unready sandbox" in record.getMessage()]
    if any(present.values()):
        if refused:
            pytest.fail(f"ownership fencing refused the teardown, so these resources were left in place on purpose: {present}; {refused[0]}")
        pytest.fail(f"cleanup was claimed but resources remain: {present}\n{dump}")
    assert not refused, f"nothing else owns this container, so the fences must not refuse: {refused}"
    assert provider._starting == set() and provider._local_teardown == set()
    assert sandbox_id not in provider._sandboxes and sandbox_id not in provider._warm_pool
    assert provider._ownership.owner(sandbox_id) is None
    assert _inspect(container) is None
