"""Destroy means confirmed absent, not "the commands were attempted".

The gap after PR #74: ``LocalContainerBackend.destroy`` swallowed a failed
``docker stop`` and logged failed proxy and network removals, then returned
normally. The provider took that return as a confirmed teardown, forgot the
parked entry, counted the set as gone and -- on the input-drift path -- went
on to create, rediscovered the very container it had just rejected, and
handed it back active with its old configuration.

These tests compose the *real* ``LocalContainerBackend.destroy`` control flow
(runtime ``docker``, network mode ``allowlist``, the provider's container
prefix) with the committed fake backend for creation, liveness, readiness and
rediscovery. Every subprocess call is intercepted by ``_FakeDocker``, whose
inventory of containers and networks is the independent record the journal
is reconciled against: a stop or remove that "succeeds" removes the resource
from the inventory, a refused one leaves it, and inspection answers from the
inventory (or refuses to, for a daemon fault). No real subprocess, Docker
resource, binding or model is involved.

One resource set is the sandbox container, its network sidecar and both of
its networks. It is counted absent once, on the attempt that established
every member gone, never per command and never per retry.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import threading
import time

import pytest
from test_sandbox_warm_reuse_latency import ACCEPTED_USER, _acquire_accepted, _aio_mod, _binding, _make_provider

from deerflow.community.aio_sandbox import aio_sandbox_provider as provider_mod
from deerflow.community.aio_sandbox import backend as backend_mod
from deerflow.community.aio_sandbox.aio_sandbox_provider import ACCEPTED_SANDBOX_ID_SUFFIX, SandboxBeingDestroyedError, SandboxIdentityCollisionError
from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
from deerflow.community.aio_sandbox.ownership.factory import compute_lease_ttl
from deerflow.runtime.turn_phases import AcquisitionSource, turn_phases
from deerflow.sandbox.accepted_material import AcceptedSkillSandboxBindingError

# Resolved leniently so this file still collects on a tree that predates the
# contract: the lifecycle tests then fail on behaviour (the rejected container
# is handed back, the unrelated set is stopped), not on an import.
DestroyOutcome = getattr(backend_mod, "DestroyOutcome", None)
SandboxCleanupIncompleteError = getattr(provider_mod, "SandboxCleanupIncompleteError", RuntimeError)

PREFIX = "deer-flow-sandbox"
REFUSAL = "synthetic Docker daemon refusal"
DAEMON_DOWN = "Cannot connect to the Docker daemon at unix:///var/run/docker.sock"


def _real_destroy_backend() -> LocalContainerBackend:
    """A real local backend for ``destroy`` only; nothing here reaches Docker."""
    backend = LocalContainerBackend.__new__(LocalContainerBackend)
    backend._image = "sandbox:latest"
    backend._base_port = 8080
    backend._container_prefix = PREFIX
    backend._config_mounts = []
    backend._environment = {}
    backend._runtime = "docker"
    backend._network_mode = "allowlist"
    backend._network_config = {"mode": "allowlist", "allow_domains": [], "approval": "prompt", "temporary_grant_ttl": 300, "proxy_image": "proxy:latest"}
    backend._allow_synthetic_dns = False
    return backend


class _FakeDocker:
    """Inventory-backed stand-in for every ``subprocess.run`` the backend makes.

    ``faults`` maps a command key to ``"refuse"`` (non-zero exit / raised
    ``CalledProcessError`` for checked calls), ``"timeout"`` or ``"daemon"``
    (an unavailable daemon: commands and inspections alike cannot answer).
    Keys: ``stop:<name>``, ``rm:<name>``, ``network_rm:<name>``, and
    ``inspect`` / ``network_inspect`` for observation faults.
    """

    def __init__(self, real: LocalContainerBackend) -> None:
        self.real = real
        self.containers: set[str] = set()
        self.networks: set[str] = set()
        self.faults: dict[str, str] = {}
        self.commands: list[list[str]] = []
        self.lock = threading.Lock()

    # ── inventory ──
    def members(self, sandbox_id: str) -> tuple[str, str, str, str]:
        proxy, network = self.real._resource_names(sandbox_id)
        return f"{PREFIX}-{sandbox_id}", proxy, network, self.real._egress_network_name(sandbox_id)

    def add_set(self, sandbox_id: str) -> None:
        sandbox, proxy, network, egress = self.members(sandbox_id)
        with self.lock:
            self.containers.update({sandbox, proxy})
            self.networks.update({network, egress})

    def present(self, sandbox_id: str) -> set[str]:
        sandbox, proxy, network, egress = self.members(sandbox_id)
        with self.lock:
            return ({sandbox, proxy} & self.containers) | ({network, egress} & self.networks)

    def sets_alive(self) -> int:
        with self.lock:
            return len([name for name in self.containers if not name.endswith("-proxy") and name.startswith(f"{PREFIX}-")])

    # ── the subprocess seam ──
    def run(self, cmd, **kwargs):
        self.commands.append(list(cmd))
        verb = cmd[1]
        if verb == "stop":
            return self._command(f"stop:{cmd[2]}", cmd, kwargs, remove=("container", cmd[2]))
        if verb == "rm":
            return self._command(f"rm:{cmd[-1]}", cmd, kwargs, remove=("container", cmd[-1]), absent_text="Error: No such container: " + cmd[-1])
        if verb == "network" and cmd[2] == "rm":
            return self._command(f"network_rm:{cmd[3]}", cmd, kwargs, remove=("network", cmd[3]), absent_text=f"Error: No such network: {cmd[3]} not found")
        if verb == "inspect":
            return self._inspect_container(cmd[-1], cmd)
        if verb == "network" and cmd[2] == "inspect":
            return self._inspect_network(cmd[3], cmd)
        raise AssertionError(f"unexpected docker command in test: {cmd}")

    def _fault(self, key: str) -> str | None:
        # ``all`` faults every mutation command; observations stay answerable
        # unless ``daemon`` is set, which faults everything.
        return self.faults.get(key) or self.faults.get("all")

    def _observation_fault(self, key: str) -> str | None:
        return self.faults.get(key) or self.faults.get("daemon")

    def _command(self, key, cmd, kwargs, *, remove, absent_text=None):
        fault = self._fault(key) or (self._fault("daemon") and "daemon")
        checked = bool(kwargs.get("check"))
        if fault == "timeout":
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=kwargs.get("timeout", 1))
        if fault in ("refuse", "daemon"):
            text = DAEMON_DOWN if fault == "daemon" else REFUSAL
            if checked:
                raise subprocess.CalledProcessError(1, cmd, stderr=text)
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr=text)
        kind, name = remove
        with self.lock:
            inventory = self.containers if kind == "container" else self.networks
            if name in inventory:
                inventory.discard(name)
                return subprocess.CompletedProcess(cmd, 0, stdout=f"{name}\n", stderr="")
        if checked:
            raise subprocess.CalledProcessError(1, cmd, stderr=absent_text or f"Error response from daemon: No such container: {name}")
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr=absent_text or f"No such container: {name}")

    def _inspect_container(self, name, cmd):
        fault = self._observation_fault("inspect")
        if fault == "timeout":
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=5)
        if fault:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr=DAEMON_DOWN)
        with self.lock:
            exists = name in self.containers
        if exists:
            return subprocess.CompletedProcess(cmd, 0, stdout="sha256:" + name + "\n", stderr="")
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr=f"Error: No such object: {name}")

    def _inspect_network(self, name, cmd):
        fault = self._observation_fault("network_inspect")
        if fault == "timeout":
            raise subprocess.TimeoutExpired(cmd=cmd, timeout=10)
        if fault:
            return subprocess.CompletedProcess(cmd, 1, stdout="", stderr=DAEMON_DOWN)
        with self.lock:
            exists = name in self.networks
        if exists:
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps([{"Driver": "bridge", "Internal": True, "Labels": {}, "Options": {}}]), stderr="")
        return subprocess.CompletedProcess(cmd, 1, stdout="", stderr=f"Error: No such network: {name} not found")


def _make(tmp_path, monkeypatch, *, replicas: int = 2):
    """Provider with fake creation and the real local destroy over a fake daemon."""
    provider, backend = _make_provider(tmp_path, monkeypatch, replicas=replicas, adopt_on_conflict=True)
    real = _real_destroy_backend()
    docker = _FakeDocker(real)
    real_create = backend.create

    def _create(thread_id, sandbox_id, **kwargs):
        # The fake's liveness follows the daemon inventory: a container the
        # real destroy removed is not there to be rediscovered.
        if f"{PREFIX}-{sandbox_id}" not in docker.containers:
            backend.alive.pop(sandbox_id, None)
        info = real_create(thread_id, sandbox_id, **kwargs)
        if info.provenance == "created":
            docker.add_set(sandbox_id)
        return info

    monkeypatch.setattr(backend, "create", _create)
    monkeypatch.setattr(backend, "destroy", real.destroy)
    monkeypatch.setattr(subprocess, "run", docker.run)
    return provider, backend, docker


def _drift(monkeypatch) -> None:
    aio = _aio_mod()
    monkeypatch.setattr(aio.AioSandboxProvider, "_lark_integration_active", lambda *_a, **_k: True)


def _two_parked(provider) -> tuple[str, str]:
    a = _acquire_accepted(provider, "thread-a")
    provider.release(a)
    b = _acquire_accepted(provider, "thread-b")
    provider.release(b)
    return a, b


def _counts(snapshot) -> dict[str, int]:
    return {
        "attempts": snapshot.teardown_attempts,
        "teardowns": snapshot.resource_teardowns,
        "refusals": snapshot.teardown_refusals,
        "failures": snapshot.teardown_failures,
        "creates": snapshot.resource_creates,
        "rediscoveries": snapshot.resource_rediscoveries,
    }


# ── The counterexample and its controls ──────────────────────────────────


def test_all_cleanup_commands_refused_refuses_the_acquisition_and_keeps_the_set(tmp_path, monkeypatch):
    """The brief's counterexample: five refusals, nothing stopped, no hand-out."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, b = _two_parked(provider)
    before = docker.present(a)
    assert len(before) == 4
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)

    with turn_phases(correlation_id="all-refused") as journal, pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_inputs_changed"):
        _acquire_accepted(provider, "thread-a", run_id="run-2")

    assert docker.present(a) == before, "nothing was stopped"
    assert backend.adopted == [], "the rejected container was not rediscovered and handed back"
    assert backend.created == [a, b]
    assert a in provider._warm_pool and b in provider._warm_pool, "both sets stay tracked"
    assert a in provider._cleanup_pending
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}
    assert docker.sets_alive() == 2


def test_a_stop_timeout_on_the_replacement_path_is_refused_like_a_refusal(tmp_path, monkeypatch):
    """A timed-out stop is caught inside the local destroy (it does not raise) and reads unknown."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    docker.faults[f"stop:{PREFIX}-{a}"] = "timeout"
    _drift(monkeypatch)

    with turn_phases(correlation_id="timeout") as journal, pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_inputs_changed"):
        _acquire_accepted(provider, "thread-a", run_id="run-2")

    assert a in provider._warm_pool
    assert provider._cleanup_pending[a] is DestroyOutcome.UNKNOWN
    assert backend.adopted == []
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}


def test_a_raising_destroy_is_treated_the_same_as_a_swallowed_failure(tmp_path, monkeypatch):
    """Control from the brief: an explicit failure was already refused before the repair; the two must agree.

    ``OSError`` is the only class that escapes the local destroy (a missing
    ``docker`` binary); the provider maps it to ``unknown`` on this path too.
    """
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    before = docker.present(a)

    def _raise(_info):
        raise FileNotFoundError("docker")

    monkeypatch.setattr(backend, "destroy", _raise)
    _drift(monkeypatch)

    with turn_phases(correlation_id="raising") as journal, pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_inputs_changed"):
        _acquire_accepted(provider, "thread-a", run_id="run-2")

    assert docker.present(a) == before
    assert backend.adopted == []
    assert a in provider._warm_pool
    assert provider._cleanup_pending[a] is DestroyOutcome.UNKNOWN
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}


def test_successful_replacement_stops_only_the_target_and_creates_one(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, b = _two_parked(provider)
    _drift(monkeypatch)

    with turn_phases(correlation_id="ok") as journal:
        again = _acquire_accepted(provider, "thread-a", run_id="run-2")

    assert again == a
    assert backend.created == [a, b, a]
    assert docker.present(b) and len(docker.present(b)) == 4
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 1, "rediscoveries": 0}
    assert provider._cleanup_pending == {}
    assert docker.sets_alive() == 2


def test_compatible_reuse_invokes_no_teardown_and_creates_nothing(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, b = _two_parked(provider)
    with turn_phases(correlation_id="reuse") as journal:
        assert _acquire_accepted(provider, "thread-a", run_id="run-2") == a
    assert docker.commands == []
    assert _counts(journal.snapshot())["attempts"] == 0
    assert backend.created == [a, b]


# ── Each failing command separately, together, partially ─────────────────


@pytest.mark.parametrize("member", ["sandbox", "proxy", "network", "egress"])
def test_each_failing_member_alone_leaves_the_set_pending(tmp_path, monkeypatch, member):
    """One member that will not go: the set is not absent, whatever the others did.

    The sidecar has two commands (stop, then ``rm -f``); either alone still
    removes it, so a stuck sidecar means both refused.
    """
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    sandbox, proxy, network, egress = docker.members(a)
    keys = {"sandbox": [f"stop:{sandbox}"], "proxy": [f"stop:{proxy}", f"rm:{proxy}"], "network": [f"network_rm:{network}"], "egress": [f"network_rm:{egress}"]}[member]
    for key in keys:
        docker.faults[key] = "refuse"
    _drift(monkeypatch)

    with turn_phases(correlation_id=member) as journal, pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")

    refused_member = {"sandbox": sandbox, "proxy": proxy, "network": network, "egress": egress}[member]
    assert docker.present(a) == {refused_member}, "exactly the refused member remains"
    assert backend.adopted == []
    assert a in provider._warm_pool
    assert provider._cleanup_pending[a] is DestroyOutcome.PARTIAL
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}


@pytest.mark.parametrize("refused", ["stop", "rm"])
def test_a_single_refused_sidecar_command_is_covered_by_the_other(tmp_path, monkeypatch, refused):
    """``docker stop`` and ``docker rm -f`` each remove the sidecar; one refusal is not a leftover."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, b = _two_parked(provider)
    _sandbox, proxy, *_ = docker.members(a)
    docker.faults[f"{refused}:{proxy}"] = "refuse"
    _drift(monkeypatch)

    with turn_phases(correlation_id=f"sidecar-{refused}") as journal:
        assert _acquire_accepted(provider, "thread-a", run_id="run-2") == a

    assert backend.created == [a, b, a]
    assert _counts(journal.snapshot())["teardowns"] == 1
    assert _counts(journal.snapshot())["failures"] == 0


def test_partial_cleanup_is_reported_as_partial_and_the_leftover_is_retried_to_absence(tmp_path, monkeypatch):
    """The sandbox goes, the sidecar stays: unusable, tracked, and finished later."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, b = _two_parked(provider)
    sandbox, proxy, network, egress = docker.members(a)
    docker.faults[f"stop:{proxy}"] = "refuse"
    docker.faults[f"rm:{proxy}"] = "refuse"
    _drift(monkeypatch)

    with turn_phases(correlation_id="partial") as first, pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")

    assert docker.present(a) == {proxy}, "sandbox and networks gone, the sidecar left behind"
    assert provider._cleanup_pending[a] is DestroyOutcome.PARTIAL
    assert _counts(first.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}
    replicas, total = provider._replica_count()
    assert total == replicas, "the half-removed set still occupies its slot"

    # The fault clears; the next acquisition finishes the cleanup and builds one replacement.
    docker.faults.clear()
    with turn_phases(correlation_id="recovered") as second:
        again = _acquire_accepted(provider, "thread-a", run_id="run-3")

    assert again == a
    assert docker.present(a) == set(docker.members(a)), "the rebuilt set, all four members"
    assert ["docker", "rm", "-f", proxy] in docker.commands, "the retry issued the sidecar removal"
    assert backend.created == [a, b, a]
    assert a not in provider._cleanup_pending
    assert _counts(second.snapshot()) == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 1, "rediscoveries": 0}
    assert docker.sets_alive() == 2


def test_a_stop_timeout_is_unknown_not_absent(tmp_path, monkeypatch):
    """The stop timed out and the container still answers inspect: unknown, kept."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    sandbox, *_ = docker.members(a)
    docker.faults[f"stop:{sandbox}"] = "timeout"
    _drift(monkeypatch)

    with turn_phases(correlation_id="timeout") as journal, pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")

    assert provider._cleanup_pending[a] is DestroyOutcome.UNKNOWN
    assert sandbox in docker.present(a)
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}
    assert a in provider._warm_pool


def test_daemon_unavailability_is_unknown_not_absent(tmp_path, monkeypatch):
    """Every command fails with the daemon's error and nothing can be observed: unknown, kept, not handed back."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    before = docker.present(a)
    docker.faults["daemon"] = "1"
    _drift(monkeypatch)

    with turn_phases(correlation_id="daemon") as journal, pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_inputs_changed"):
        _acquire_accepted(provider, "thread-a", run_id="run-2")

    assert docker.present(a) == before
    assert backend.adopted == []
    assert provider._cleanup_pending[a] is DestroyOutcome.UNKNOWN
    assert a in provider._warm_pool
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}
    assert any(cmd[1] == "stop" for cmd in docker.commands), "the stop was attempted and answered by the daemon error"


def test_genuinely_absent_resources_are_cleaned_idempotently(tmp_path, monkeypatch):
    """Not-found answers are absence; the set is confirmed gone with nothing to do."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, b = _two_parked(provider)
    for name in docker.members(a):
        docker.containers.discard(name)
        docker.networks.discard(name)
    _drift(monkeypatch)

    with turn_phases(correlation_id="absent") as journal:
        again = _acquire_accepted(provider, "thread-a", run_id="run-2")

    assert again == a
    assert backend.created == [a, b, a]
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 1, "rediscoveries": 0}


def test_a_refused_removal_is_not_mistaken_for_not_found(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    sandbox, proxy, network, egress = docker.members(a)
    # Permission failure text that does not say "not found".
    docker.faults[f"network_rm:{network}"] = "refuse"
    _drift(monkeypatch)
    with pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    assert network in docker.present(a)
    assert a in provider._cleanup_pending


# ── Capacity, leftovers, ownership, races ────────────────────────────────


def test_failed_replacement_at_capacity_neither_evicts_the_unrelated_set_nor_frees_a_slot(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch, replicas=2)
    a, b = _two_parked(provider)
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)

    with pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")

    assert len(docker.present(b)) == 4
    assert a in provider._cleanup_pending
    replicas, total = provider._replica_count()
    assert total == replicas == 2, "the failed set is not a freed slot"
    # A third thread at capacity retries the pending set first. When that retry
    # fails again the pending set stays (tracked, still a slot), and the room
    # comes from the healthy oldest unrelated set: the documented cost of a
    # set that will not go.
    docker.faults.clear()
    docker.faults[f"stop:{PREFIX}-{a}"] = "refuse"
    with turn_phases(correlation_id="third") as journal:
        c = _acquire_accepted(provider, "thread-c")
    assert c not in (a, b)
    assert docker.present(a) == {f"{PREFIX}-{a}"}, "the retry removed what it could; the container itself still refuses"
    assert a in provider._cleanup_pending and a in provider._warm_pool
    assert docker.present(b) == set(), "the healthy oldest unrelated set paid for the slot"
    assert _counts(journal.snapshot()) == {"attempts": 2, "teardowns": 1, "refusals": 0, "failures": 1, "creates": 1, "rediscoveries": 0}


def test_eviction_retries_a_pending_set_before_a_healthy_one(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch, replicas=2)
    a, b = _two_parked(provider)
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)
    with pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    docker.faults.clear()

    with turn_phases(correlation_id="evict") as journal:
        c = _acquire_accepted(provider, "thread-c")

    assert c not in (a, b)
    assert docker.present(a) == set(), "the pending set was finished first"
    assert len(docker.present(b)) == 4, "the healthy set was spared"
    assert a not in provider._cleanup_pending and a not in provider._warm_pool
    assert _counts(journal.snapshot())["teardowns"] == 1
    assert _counts(journal.snapshot())["failures"] == 0


def test_idle_checker_retries_pending_cleanup_and_counts_the_set_once(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)
    with pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")

    with turn_phases(correlation_id="idle-1") as still_failing:
        provider._retry_all_pending_cleanup()
    assert a in provider._cleanup_pending
    assert _counts(still_failing.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}

    docker.faults.clear()
    with turn_phases(correlation_id="idle-2") as recovered:
        provider._retry_all_pending_cleanup()
    assert a not in provider._cleanup_pending and a not in provider._warm_pool
    assert docker.present(a) == set()
    assert _counts(recovered.snapshot()) == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}


def test_one_idle_pass_attempts_an_expired_pending_set_once(tmp_path, monkeypatch):
    """The composed idle path: retry-all and the expiry reaper together cost one attempt."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    sandbox, *_ = docker.members(a)
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)
    with pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    # Age the pending entry past the idle timeout so the reaper would also select it.
    entry, _ts = provider._warm_pool[a]
    provider._warm_pool[a] = (entry, 0.0)
    docker.commands.clear()

    with turn_phases(correlation_id="idle-pass-1") as still_failing:
        provider._cleanup_idle_sandboxes(1.0)
    assert _counts(still_failing.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}
    assert [cmd for cmd in docker.commands if cmd[:2] == ["docker", "stop"] and cmd[2] == sandbox] == [["docker", "stop", sandbox]], "one stop per pass"
    assert a in provider._cleanup_pending and a in provider._warm_pool

    docker.faults.clear()
    with turn_phases(correlation_id="idle-pass-2") as recovered:
        provider._cleanup_idle_sandboxes(1.0)
    assert _counts(recovered.snapshot()) == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert a not in provider._cleanup_pending and a not in provider._warm_pool
    assert docker.present(a) == set()


def test_ownership_lost_at_quarantine_time_keeps_the_set_tracked_until_the_renewal_drops_it(tmp_path, monkeypatch):
    """A peer holds the lease when the failed cleanup is quarantined: no forced drop under the reservation."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    docker.faults["all"] = "refuse"
    monkeypatch.setattr(provider, "_refresh_ownership", lambda _sid: False)
    _drift(monkeypatch)

    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_inputs_changed"):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    assert a in provider._warm_pool and a in provider._cleanup_pending, "tracked and pending, not dropped inside the reservation"
    assert len(docker.present(a)) == 4

    # A retry against the peer's lease refuses and stops nothing.
    monkeypatch.setattr(provider, "_claim_ownership", lambda *_a, **_k: False)
    docker.faults.clear()
    docker.commands.clear()
    with turn_phases(correlation_id="peer-owned") as journal:
        provider._retry_all_pending_cleanup()
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 1, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert docker.commands == []
    assert len(docker.present(a)) == 4

    # The renewal thread, outside any reservation, drops the handle without touching the daemon.
    provider._renew_owned_leases()
    assert a not in provider._warm_pool and a not in provider._cleanup_pending
    assert docker.commands == []
    assert len(docker.present(a)) == 4, "the peer's set is left to the peer"


def test_ownership_loss_during_pending_cleanup_hands_the_set_to_its_new_owner(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)
    with pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    assert a in provider._cleanup_pending

    # A peer takes the lease while the set is pending here.
    monkeypatch.setattr(provider, "_claim_ownership", lambda *_a, **_k: False)
    docker.faults.clear()
    with turn_phases(correlation_id="lost") as journal:
        provider._retry_all_pending_cleanup()

    assert len(docker.present(a)) == 4, "a retry never stops a set another instance now owns"
    assert _counts(journal.snapshot())["refusals"] == 1
    assert _counts(journal.snapshot())["teardowns"] == 0


def test_a_pending_set_is_never_handed_out_by_the_ordinary_reclaim(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    sandbox_id = provider.acquire("thread-ordinary", user_id="user-1")
    docker.add_set(sandbox_id)
    provider.release(sandbox_id)
    docker.faults["all"] = "refuse"
    provider._evict_oldest_warm()

    with pytest.raises(SandboxBeingDestroyedError):
        provider.acquire("thread-ordinary", user_id="user-1")
    assert sandbox_id not in provider._sandboxes
    assert sandbox_id in provider._cleanup_pending and sandbox_id in provider._warm_pool
    assert len(docker.present(sandbox_id)) == 4


def test_explicit_destroy_of_a_failed_set_raises_and_keeps_it_tracked(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a = _acquire_accepted(provider, "thread-a")
    docker.faults["all"] = "refuse"

    with turn_phases(correlation_id="explicit") as journal, pytest.raises(SandboxCleanupIncompleteError):
        provider.destroy(a)

    assert a in provider._warm_pool and a in provider._cleanup_pending, "not untracked, not a freed slot"
    assert a not in provider._sandboxes
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}
    assert len(docker.present(a)) == 4


def test_unready_rollback_that_fails_keeps_the_started_set_tracked_for_retry(tmp_path, monkeypatch):
    """A container we started but never registered is still ours to finish when its stop fails."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    info = backend.create("thread-unready", "unready01")
    assert len(docker.present("unready01")) == 4
    docker.faults["all"] = "refuse"

    with turn_phases(correlation_id="unready") as journal:
        provider._destroy_unready_sandbox("unready01", info)

    assert "unready01" in provider._warm_pool and "unready01" in provider._cleanup_pending
    assert "unready01" not in provider._sandboxes
    assert len(docker.present("unready01")) == 4
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}

    docker.faults.clear()
    with turn_phases(correlation_id="unready-retry") as recovered:
        provider._retry_all_pending_cleanup()
    assert "unready01" not in provider._warm_pool and "unready01" not in provider._cleanup_pending
    assert docker.present("unready01") == set()
    assert _counts(recovered.snapshot()) == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}


@pytest.mark.anyio
async def test_cancellation_rollback_that_fails_keeps_the_created_set_tracked_and_stays_bounded(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    acquired = threading.Event()
    allow = threading.Event()

    def _bind(*_args, **_kwargs):
        acquired.set()
        assert allow.wait(timeout=5)

    monkeypatch.setattr(provider, "bind_accepted_skill_snapshot", _bind)
    docker.faults["all"] = "refuse"

    with turn_phases(correlation_id="cancel-rollback") as journal:
        task = asyncio.create_task(provider.provision_accepted_skills_async("thread-cancel", user_id=ACCEPTED_USER, binding=_binding()))
        assert await asyncio.to_thread(acquired.wait, 5)
        task.cancel("caller went away")
        allow.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    created = backend.created[0]
    assert created in provider._warm_pool and created in provider._cleanup_pending
    assert len(docker.present(created)) == 4
    assert _counts(journal.snapshot())["attempts"] == 1, "one bounded attempt, no retry loop"
    assert _counts(journal.snapshot())["teardowns"] == 0


def test_reacquisition_race_during_retry_is_refused_not_stopped_underneath(tmp_path, monkeypatch):
    """A reclaim that lands while the retry holds the reservation is refused."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)
    with pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    docker.faults.clear()
    provider._local_teardown.add(a)  # a reaper in this process holds the reservation

    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_cleanup_pending"):
        _acquire_accepted(provider, "thread-a", run_id="run-3")
    assert len(docker.present(a)) == 4


def test_compatible_aba_switching_is_untouched(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch, replicas=2)
    a, b = _two_parked(provider)
    for thread_id, expected in (("thread-a", a), ("thread-b", b), ("thread-a", a)):
        got = _acquire_accepted(provider, thread_id, run_id=f"run-{thread_id}")
        assert got == expected
        provider.release(got)
    assert backend.created == [a, b]
    assert docker.commands == []
    assert docker.sets_alive() == 2


def test_journal_source_after_a_recovered_replacement_is_created(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)
    with pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    docker.faults.clear()
    with turn_phases(correlation_id="recovered") as journal:
        assert _acquire_accepted(provider, "thread-a", run_id="run-3") == a
    assert journal.snapshot().acquisition_source is AcquisitionSource.CREATED


# ── Reconciliation / acquisition boundary ─────────────────────────────────
#
# The accepted-orphan branch of reconciliation observes an unowned container,
# then claims a teardown lease and stops it. The store's claim succeeds
# against this process's own lease, so it cannot see an acquisition that
# registered the same id between the observation and the claim. The tests
# here schedule that acquisition deterministically, in both orders, with the
# real reconciliation entry point and the real local destroy over the fake
# daemon. The memory ownership store is used with its cross-process flag
# raised only so the real owner/grace decision runs; nothing here is a Redis
# or multi-process claim.


def _seed_accepted_orphan(provider, backend, docker, monkeypatch, thread_id: str = "thread-a") -> tuple[str, object]:
    """An untracked accepted-id resource set, visible to reconciliation and past its grace."""
    sandbox_id = f"{provider._sandbox_id_for_thread(thread_id, ACCEPTED_USER)}{ACCEPTED_SANDBOX_ID_SUFFIX}"
    info = backend.create(thread_id, sandbox_id)
    assert len(docker.present(sandbox_id)) == 4
    # Inventory-backed, so a later pass sees the daemon's truth, not a frozen listing.
    monkeypatch.setattr(backend, "list_running", lambda: [info] if f"{PREFIX}-{sandbox_id}" in docker.containers else [])
    monkeypatch.setattr(provider._ownership, "supports_cross_process", True)
    provider._unowned_since[sandbox_id] = time.time() - compute_lease_ttl(provider._ownership_config) - 1
    return sandbox_id, info


def _acquire_after_observation(provider, monkeypatch, thread_id: str, *, hook: str = "_adoptable_after_grace", park: bool = False):
    """Run an accepted acquisition (optionally parking it again) between reconciliation's observation and its claim."""
    real = getattr(provider, hook)
    seen: list[str] = []

    def _wrapped(*args, **kwargs):
        result = real(*args, **kwargs)
        if not seen:
            seen.append("acquired")
            acquired = _acquire_accepted(provider, thread_id)
            seen.append(acquired)
            if park:
                provider.release(acquired)
        return result

    monkeypatch.setattr(provider, hook, _wrapped)
    return seen


@pytest.mark.parametrize("fault", ["all", "sidecar"])
def test_acquisition_between_orphan_observation_and_teardown_claim_is_not_torn_down(tmp_path, monkeypatch, fault):
    """Acquisition wins: the obsolete orphan decision must not stop or quarantine the live holder."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    sandbox_id, _info = _seed_accepted_orphan(provider, backend, docker, monkeypatch)
    _sandbox, proxy, *_ = docker.members(sandbox_id)
    if fault == "all":
        docker.faults["all"] = "refuse"
    else:
        docker.faults[f"stop:{proxy}"] = "refuse"
        docker.faults[f"rm:{proxy}"] = "refuse"
    seen = _acquire_after_observation(provider, monkeypatch, "thread-a")

    with turn_phases(correlation_id=f"race-{fault}") as journal:
        provider._reconcile_orphans()

    assert seen == ["acquired", sandbox_id], "the acquisition completed between observation and claim"
    assert len(docker.present(sandbox_id)) == 4, "nothing of the live holder's set was stopped"
    assert provider._thread_sandboxes[(ACCEPTED_USER, "thread-a")] == sandbox_id
    assert sandbox_id in provider._sandboxes and sandbox_id not in provider._warm_pool
    assert sandbox_id not in provider._cleanup_pending
    assert provider.get(sandbox_id) is not None
    assert provider._ownership.owner(sandbox_id) == provider._owner_id
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 1, "failures": 0, "creates": 0, "rediscoveries": 1}
    assert not any(cmd[1] in ("stop", "rm") or (cmd[1] == "network" and cmd[2] == "rm") for cmd in docker.commands), "no mutation was issued"
    assert _acquire_accepted(provider, "thread-a", run_id="run-2") == sandbox_id


def test_acquisition_and_park_between_observation_and_claim_leaves_the_warm_set_alone(tmp_path, monkeypatch):
    """Warm transition: acquired and parked again after the observation, the set is ours, not an orphan."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    sandbox_id, _info = _seed_accepted_orphan(provider, backend, docker, monkeypatch)
    seen = _acquire_after_observation(provider, monkeypatch, "thread-a", park=True)

    with turn_phases(correlation_id="warm-transition") as journal:
        provider._reconcile_orphans()

    assert seen == ["acquired", sandbox_id]
    assert len(docker.present(sandbox_id)) == 4
    assert sandbox_id in provider._warm_pool and sandbox_id not in provider._sandboxes
    assert sandbox_id not in provider._cleanup_pending
    assert provider._warm_pool_identity[sandbox_id] == (ACCEPTED_USER, "thread-a")
    assert provider._ownership.owner(sandbox_id) == provider._owner_id
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 1, "failures": 0, "creates": 0, "rediscoveries": 1}
    assert _mutations(docker) == []
    assert _acquire_accepted(provider, "thread-a", run_id="run-2") == sandbox_id


def test_orphan_observed_before_the_create_marks_starting_is_not_torn_down_mid_readiness(tmp_path, monkeypatch):
    """The claim would land while the creation waits for readiness: the reservation predicate refuses it."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    sandbox_id, _info = _seed_accepted_orphan(provider, backend, docker, monkeypatch)
    aio = _aio_mod()
    in_ready = threading.Event()
    go = threading.Event()

    def _ready(_url, timeout=60, **_kw):
        in_ready.set()
        assert go.wait(timeout=5)
        return True

    monkeypatch.setattr(aio, "wait_for_sandbox_ready", _ready)
    acquired: dict[str, object] = {}

    def _acquire():
        try:
            acquired["result"] = _acquire_accepted(provider, "thread-a")
        except Exception as exc:  # noqa: BLE001 - reported through the assertion below
            acquired["result"] = exc

    acquirer = threading.Thread(target=_acquire, name="acquirer")
    real = provider._adoptable_after_grace
    started: list[int] = []

    def _observed(*args, **kwargs):
        result = real(*args, **kwargs)
        if not started:
            started.append(1)
            acquirer.start()
            assert in_ready.wait(timeout=5), "the creation reached its readiness wait"
        return result

    monkeypatch.setattr(provider, "_adoptable_after_grace", _observed)
    try:
        with turn_phases(correlation_id="mid-readiness") as journal:
            provider._reconcile_orphans()
    finally:
        go.set()
        acquirer.join(timeout=5)
    assert not acquirer.is_alive()

    assert acquired["result"] == sandbox_id
    assert len(docker.present(sandbox_id)) == 4
    assert provider.get(sandbox_id) is not None and sandbox_id not in provider._cleanup_pending
    assert provider._ownership.owner(sandbox_id) == provider._owner_id
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 1, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert _mutations(docker) == []


def test_stale_incompatible_replacement_decision_does_not_stop_a_creation_in_flight(tmp_path, monkeypatch):
    """The incompatible-policy replacement carries the same starting fence as the accepted branch."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    monkeypatch.setattr(provider._ownership, "supports_cross_process", True)
    aio = _aio_mod()
    sandbox_id = f"{provider._sandbox_id_for_thread('thread-a', ACCEPTED_USER)}{ACCEPTED_SANDBOX_ID_SUFFIX}"
    stale = backend._unused_info(sandbox_id)
    stale.requires_replacement = True
    monkeypatch.setattr(backend, "list_running", lambda: [stale])
    provider._unowned_since[sandbox_id] = time.time() - compute_lease_ttl(provider._ownership_config) - 1
    observed = threading.Event()
    proceed = threading.Event()
    reconciled = threading.Event()
    real = provider._adoptable_after_grace

    def _observe(*args, **kwargs):
        result = real(*args, **kwargs)
        if not observed.is_set():
            observed.set()
            assert proceed.wait(timeout=5)
        return result

    monkeypatch.setattr(provider, "_adoptable_after_grace", _observe)
    counts: dict[str, dict[str, int]] = {}

    def _reconcile():
        try:
            with turn_phases(correlation_id="stale-replacement") as journal:
                provider._reconcile_orphans()
            counts["mid"] = _counts(journal.snapshot())
        finally:
            reconciled.set()

    def _ready(_url, timeout=60, **_kw):
        proceed.set()
        assert reconciled.wait(timeout=5)
        return True

    monkeypatch.setattr(aio, "wait_for_sandbox_ready", _ready)
    worker = threading.Thread(target=_reconcile, name="reaper")
    worker.start()
    assert observed.wait(timeout=5)
    try:
        acquired = _acquire_accepted(provider, "thread-a")
    finally:
        proceed.set()
        worker.join(timeout=5)
    assert not worker.is_alive()

    assert acquired == sandbox_id
    assert counts["mid"] == {"attempts": 1, "teardowns": 0, "refusals": 1, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert len(docker.present(sandbox_id)) == 4
    assert provider.get(sandbox_id) is not None and sandbox_id not in provider._cleanup_pending
    assert _mutations(docker) == []


def test_acquisition_between_reservation_and_teardown_claim_is_refused_not_handed_out(tmp_path, monkeypatch):
    """Teardown wins at the reservation: an acquisition that lands before the claim does not get the set."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    sandbox_id, _info = _seed_accepted_orphan(provider, backend, docker, monkeypatch)
    outcome: dict[str, object] = {}

    real_claim = provider._claim_ownership

    def _claim(sid, *, for_destroy=False):
        if sid == sandbox_id and for_destroy and "acquire" not in outcome:
            try:
                outcome["acquire"] = _acquire_accepted(provider, "thread-a")
            except Exception as exc:  # noqa: BLE001 - the refusal class is the assertion
                outcome["acquire"] = exc
        return real_claim(sid, for_destroy=for_destroy)

    monkeypatch.setattr(provider, "_claim_ownership", _claim)

    with turn_phases(correlation_id="reserved-first") as journal:
        provider._reconcile_orphans()

    assert isinstance(outcome["acquire"], SandboxBeingDestroyedError), outcome["acquire"]
    assert docker.present(sandbox_id) == set(), "the teardown finished"
    assert sandbox_id not in provider._sandboxes and sandbox_id not in provider._warm_pool
    assert provider.get(sandbox_id) is None
    assert sandbox_id not in provider._cleanup_pending
    assert provider._ownership.owner(sandbox_id) is None, "the teardown marker was released with the stop"
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert backend.adopted == [], "the refused acquisition did not adopt the container being stopped"
    assert backend.created == [sandbox_id], "the refused acquisition created nothing"
    # The next acquisition builds a fresh generation.
    with turn_phases(correlation_id="after") as after:
        assert _acquire_accepted(provider, "thread-a", run_id="run-2") == sandbox_id
    assert after.snapshot().acquisition_source is AcquisitionSource.CREATED
    assert backend.created == [sandbox_id, sandbox_id]
    assert provider._ownership.owner(sandbox_id) == provider._owner_id
    assert len(docker.present(sandbox_id)) == 4


def _reaper_gated_destroy(backend, monkeypatch, *, gate_thread: threading.Thread | None = None):
    """Hold the *reaper's* destroy open; any other caller's destroy runs at once."""
    in_stop = threading.Event()
    allow = threading.Event()
    real_destroy = backend.destroy
    gated: list[int] = []

    def _slow_destroy(info):
        mine = threading.current_thread() is gate_thread if gate_thread is not None else not gated
        if mine:
            gated.append(1)
            in_stop.set()
            assert allow.wait(timeout=5)
        return real_destroy(info)

    monkeypatch.setattr(backend, "destroy", _slow_destroy)
    return in_stop, allow


def test_acquisition_during_the_teardown_stop_is_refused_and_recovers_after(tmp_path, monkeypatch):
    """Teardown wins first: while the stop is in flight the id is refused at once; afterwards one create."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    sandbox_id, _info = _seed_accepted_orphan(provider, backend, docker, monkeypatch)
    result: dict[str, object] = {}

    def _reconcile():
        try:
            with turn_phases(correlation_id="stop-in-flight") as journal:
                provider._reconcile_orphans()
            result["counts"] = _counts(journal.snapshot())
        except Exception as exc:  # noqa: BLE001 - surfaced by the assertions below
            result["error"] = exc

    worker = threading.Thread(target=_reconcile, name="reaper")
    in_stop, allow = _reaper_gated_destroy(backend, monkeypatch, gate_thread=worker)
    worker.start()
    try:
        assert in_stop.wait(timeout=5)
        started = time.monotonic()
        with pytest.raises(SandboxBeingDestroyedError):
            _acquire_accepted(provider, "thread-a")
        assert time.monotonic() - started < 1.0, "refused promptly, not by a timeout"
        assert backend.created == [sandbox_id], "no second generation was started under the name"
        assert provider.get(sandbox_id) is None
    finally:
        allow.set()
        worker.join(timeout=5)
    assert not worker.is_alive() and "error" not in result, result.get("error")

    assert docker.present(sandbox_id) == set()
    assert result["counts"] == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert backend.adopted == []
    assert provider._ownership.owner(sandbox_id) is None
    assert _acquire_accepted(provider, "thread-a", run_id="run-2") == sandbox_id
    assert backend.created == [sandbox_id, sandbox_id]
    assert provider._ownership.owner(sandbox_id) == provider._owner_id
    assert len(docker.present(sandbox_id)) == 4


@pytest.mark.anyio
async def test_async_acquisition_during_the_teardown_stop_is_refused_and_recovers_after(tmp_path, monkeypatch):
    """Teardown wins first on the async accepted route."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    sandbox_id, _info = _seed_accepted_orphan(provider, backend, docker, monkeypatch)
    monkeypatch.setattr(provider, "bind_accepted_skill_snapshot", lambda *_a, **_k: None)
    in_stop, allow = _reaper_gated_destroy(backend, monkeypatch)
    result: dict[str, object] = {}

    def _reconcile():
        with turn_phases(correlation_id="async-stop-in-flight") as journal:
            provider._reconcile_orphans()
        result["counts"] = _counts(journal.snapshot())

    reconcile = asyncio.ensure_future(asyncio.to_thread(_reconcile))
    try:
        assert await asyncio.to_thread(in_stop.wait, 5)
        with pytest.raises(SandboxBeingDestroyedError):
            await provider.provision_accepted_skills_async("thread-a", user_id=ACCEPTED_USER, binding=_binding())
        assert backend.created == [sandbox_id]
        assert provider.get(sandbox_id) is None
    finally:
        allow.set()
        await reconcile

    assert docker.present(sandbox_id) == set()
    assert result["counts"] == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert backend.adopted == []
    assert await provider.provision_accepted_skills_async("thread-a", user_id=ACCEPTED_USER, binding=_binding(run_id="run-2")) == sandbox_id
    assert backend.created == [sandbox_id, sandbox_id]
    assert len(docker.present(sandbox_id)) == 4


@pytest.mark.anyio
async def test_async_acquisition_between_observation_and_claim_is_not_torn_down(tmp_path, monkeypatch):
    """The async accepted route wins the same race the same way."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    sandbox_id, _info = _seed_accepted_orphan(provider, backend, docker, monkeypatch)
    docker.faults["all"] = "refuse"
    monkeypatch.setattr(provider, "bind_accepted_skill_snapshot", lambda *_a, **_k: None)
    observed = threading.Event()
    proceed = threading.Event()
    real = provider._adoptable_after_grace

    def _wrapped(*args, **kwargs):
        result = real(*args, **kwargs)
        if not observed.is_set():
            observed.set()
            assert proceed.wait(timeout=5)
        return result

    monkeypatch.setattr(provider, "_adoptable_after_grace", _wrapped)
    result: dict[str, dict[str, int]] = {}

    def _reconcile():
        with turn_phases(correlation_id="async-race") as journal:
            provider._reconcile_orphans()
        result["counts"] = _counts(journal.snapshot())

    reconcile = asyncio.ensure_future(asyncio.to_thread(_reconcile))
    try:
        assert await asyncio.to_thread(observed.wait, 5)
        acquired = await provider.provision_accepted_skills_async("thread-a", user_id=ACCEPTED_USER, binding=_binding())
    finally:
        proceed.set()
        await reconcile

    assert acquired == sandbox_id
    assert len(docker.present(sandbox_id)) == 4
    assert _mutations(docker) == []
    assert sandbox_id in provider._sandboxes and sandbox_id not in provider._cleanup_pending
    assert provider.get(sandbox_id) is not None
    assert provider._ownership.owner(sandbox_id) == provider._owner_id
    # The acquisition ran on the event loop under its own journal context; the
    # reaper's journal sees only its refused attempt.
    assert result["counts"] == {"attempts": 1, "teardowns": 0, "refusals": 1, "failures": 0, "creates": 0, "rediscoveries": 0}


def test_reconciliation_defers_a_creation_still_waiting_for_readiness(tmp_path, monkeypatch):
    """A container this process is starting is neither an orphan nor replaceable, whatever the store says."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    monkeypatch.setattr(provider._ownership, "supports_cross_process", True)
    aio = _aio_mod()
    counts: dict[str, dict[str, int]] = {}

    def _ready(url, timeout=60, **_kw):
        sid = next(iter(provider._starting))
        info = backend.infos[sid]
        monkeypatch.setattr(backend, "list_running", lambda: [info])
        provider._unowned_since[sid] = time.time() - compute_lease_ttl(provider._ownership_config) - 1
        with turn_phases(correlation_id="mid-start") as journal:
            provider._reconcile_orphans()
        counts["mid"] = _counts(journal.snapshot())
        return True

    monkeypatch.setattr(aio, "wait_for_sandbox_ready", _ready)
    a = _acquire_accepted(provider, "thread-a")
    assert counts["mid"]["attempts"] == 0 and counts["mid"]["teardowns"] == 0
    assert len(docker.present(a)) == 4
    assert provider.get(a) is not None and a not in provider._cleanup_pending


@pytest.mark.parametrize("fault", ["all", "sidecar"])
def test_control_reconciliation_without_interleaving_parks_pending_and_finishes_later(tmp_path, monkeypatch, fault):
    """No race: the failed orphan teardown parks a pending entry, refuses acquisition, then finishes."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    sandbox_id, _info = _seed_accepted_orphan(provider, backend, docker, monkeypatch)
    _sandbox, proxy, *_ = docker.members(sandbox_id)
    if fault == "all":
        docker.faults["all"] = "refuse"
    else:
        docker.faults[f"stop:{proxy}"] = "refuse"
        docker.faults[f"rm:{proxy}"] = "refuse"

    with turn_phases(correlation_id="control") as journal:
        provider._reconcile_orphans()

    expected_left = 4 if fault == "all" else 1
    assert len(docker.present(sandbox_id)) == expected_left
    if fault == "sidecar":
        assert docker.present(sandbox_id) == {proxy}
    assert sandbox_id in provider._warm_pool and sandbox_id not in provider._sandboxes
    assert provider._warm_pool_identity[sandbox_id] is None, "an orphan's identity is unknown, never the requester's"
    assert provider._cleanup_pending[sandbox_id] is (DestroyOutcome.FAILED if fault == "all" else DestroyOutcome.PARTIAL)
    assert provider.get(sandbox_id) is None
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}

    # Unknown identity, still pending: the accepted acquisition is refused, never given the leftover.
    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_cleanup_pending"):
        _acquire_accepted(provider, "thread-a")
    assert len(docker.present(sandbox_id)) == expected_left

    docker.faults.clear()
    with turn_phases(correlation_id="recovered") as recovered:
        assert _acquire_accepted(provider, "thread-a", run_id="run-2") == sandbox_id
    assert _counts(recovered.snapshot()) == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 1, "rediscoveries": 0}
    assert recovered.snapshot().acquisition_source is AcquisitionSource.CREATED
    assert len(docker.present(sandbox_id)) == 4 and sandbox_id not in provider._cleanup_pending


def test_a_stale_retry_never_stops_a_later_generation_under_the_same_name(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)
    with pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    docker.faults.clear()
    assert _acquire_accepted(provider, "thread-a", run_id="run-3") == a, "cleanup finished and generation two was built"
    docker.commands.clear()

    assert provider._retry_pending_cleanup(a, reason="cleanup_retry") is None, "nothing pending under the id any more"
    provider._retry_all_pending_cleanup()

    assert docker.commands == []
    assert len(docker.present(a)) == 4 and provider.get(a) is not None


def test_a_retry_decided_on_one_generation_never_lands_on_the_next_parked_under_the_same_name(tmp_path, monkeypatch):
    """The retry's snapshot goes stale between its lock section and its reservation; entry identity refuses it."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, _b = _two_parked(provider)
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)
    with pytest.raises(AcceptedSkillSandboxBindingError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    assert a in provider._cleanup_pending
    docker.faults.clear()
    real_destroy_warm = provider._destroy_warm_entry
    raced: dict[str, object] = {}

    def _destroy_warm(sandbox_id, entry, *, reason, still_reapable):
        if reason == "cleanup_retry" and "gen2" not in raced:
            # The owning thread finishes the cleanup, builds generation two and parks it.
            assert _acquire_accepted(provider, "thread-a", run_id="run-3") == a
            provider.release(a)
            raced["gen2"] = provider._warm_pool[a][0]
            docker.commands.clear()
        return real_destroy_warm(sandbox_id, entry, reason=reason, still_reapable=still_reapable)

    monkeypatch.setattr(provider, "_destroy_warm_entry", _destroy_warm)
    with turn_phases(correlation_id="stale-retry") as journal:
        provider._retry_all_pending_cleanup()

    assert len(docker.present(a)) == 4, "generation two was not stopped by the stale retry"
    assert _mutations(docker) == []
    assert provider._warm_pool[a][0] is raced["gen2"]
    assert a not in provider._cleanup_pending
    # Same thread, same journal: the recovery's confirmed teardown and create
    # are counted once, and the stale retry is a refusal, not a second teardown.
    assert _counts(journal.snapshot()) == {"attempts": 2, "teardowns": 1, "refusals": 1, "failures": 0, "creates": 1, "rediscoveries": 0}


def test_a_create_refused_for_a_reserved_id_evicts_nothing_and_counts_no_attempt(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch, replicas=2)
    a, b = _two_parked(provider)
    sandbox_id, _info = _seed_accepted_orphan(provider, backend, docker, monkeypatch, thread_id="thread-c")
    outcome: dict[str, object] = {}
    real_claim = provider._claim_ownership

    def _claim(sid, *, for_destroy=False):
        if sid == sandbox_id and for_destroy and "acquire" not in outcome:
            with turn_phases(correlation_id="refused-create") as journal:
                with pytest.raises(SandboxBeingDestroyedError):
                    _acquire_accepted(provider, "thread-c")
                outcome["acquire"] = "refused"
            outcome["snapshot"] = journal.snapshot()
        return real_claim(sid, for_destroy=for_destroy)

    monkeypatch.setattr(provider, "_claim_ownership", _claim)
    provider._reconcile_orphans()

    assert outcome["acquire"] == "refused"
    assert outcome["snapshot"].create_attempts == 0
    assert _counts(outcome["snapshot"])["creates"] == 0
    assert len(docker.present(a)) == 4 and len(docker.present(b)) == 4, "no unrelated warm set paid for a refused create"
    assert a in provider._warm_pool and b in provider._warm_pool


def test_a_reservation_taken_while_registration_publishes_ownership_is_honoured(tmp_path, monkeypatch):
    """Registration re-checks the reservation after its store round trip (an explicit-destroy shape)."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    sandbox_id = f"{provider._sandbox_id_for_thread('thread-r', ACCEPTED_USER)}{ACCEPTED_SANDBOX_ID_SUFFIX}"
    real_publish = provider._publish_ownership
    publishes: list[str] = []

    def _publish(sid):
        publishes.append(sid)
        if sid == sandbox_id and len(publishes) == 2:
            # Own-before-readiness was the first; registration's is the second.
            assert provider._reserve_local_teardown(sid, lambda: True)
        return real_publish(sid)

    monkeypatch.setattr(provider, "_publish_ownership", _publish)
    with turn_phases(correlation_id="reserved-during-publish") as journal, pytest.raises(SandboxBeingDestroyedError):
        _acquire_accepted(provider, "thread-r")

    assert backend.created == [sandbox_id] and backend.adopted == []
    assert len(docker.present(sandbox_id)) == 4, "left to the reservation holder, not stopped by the rollback"
    assert sandbox_id not in provider._sandboxes and sandbox_id not in provider._warm_pool
    assert provider.get(sandbox_id) is None
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 1, "failures": 0, "creates": 1, "rediscoveries": 0}
    provider._finish_local_teardown(sandbox_id)


def _shutdown_quietly(provider, monkeypatch) -> None:
    monkeypatch.setattr(provider, "_assert_no_invocation_owned_skill_projections", lambda: None)
    monkeypatch.setattr(provider, "_stop_idle_checker", lambda: None)
    monkeypatch.setattr(provider, "_stop_lease_renewal", lambda: None)


def test_shutdown_stops_every_parked_set_and_counts_each_once(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, b = _two_parked(provider)
    _shutdown_quietly(provider, monkeypatch)

    with turn_phases(correlation_id="shutdown") as journal:
        provider.shutdown()

    assert docker.present(a) == set() and docker.present(b) == set()
    assert provider._warm_pool == {} and provider._cleanup_pending == {}
    assert _counts(journal.snapshot()) == {"attempts": 2, "teardowns": 2, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}


def test_shutdown_keeps_a_set_whose_stop_did_not_confirm_parked_and_pending(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a, b = _two_parked(provider)
    docker.faults[f"stop:{PREFIX}-{a}"] = "refuse"
    _shutdown_quietly(provider, monkeypatch)

    with turn_phases(correlation_id="shutdown-failed") as journal:
        provider.shutdown()

    assert docker.present(a) == {f"{PREFIX}-{a}"} and docker.present(b) == set()
    assert a in provider._warm_pool and provider._cleanup_pending[a] is DestroyOutcome.PARTIAL
    assert b not in provider._warm_pool
    assert _counts(journal.snapshot()) == {"attempts": 2, "teardowns": 1, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}


def test_an_active_id_carrying_a_pending_mark_is_refused_everywhere_and_never_reaped(tmp_path, monkeypatch):
    """Regression coverage for the quarantine guards, not acceptance evidence: the fences keep this state unreachable."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a = _acquire_accepted(provider, "thread-a")
    provider._cleanup_pending[a] = DestroyOutcome.PARTIAL
    docker.commands.clear()

    assert provider.get(a) is None
    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_cleanup_pending"):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    with turn_phases(correlation_id="active-pending") as journal:
        provider._retry_all_pending_cleanup()

    assert docker.commands == []
    assert a in provider._cleanup_pending and a in provider._sandboxes
    assert _counts(journal.snapshot()) == {"attempts": 0, "teardowns": 0, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}


# ── Identity before cleanup on the accepted reclaim ─────────────────────


def _collide(provider, monkeypatch, sandbox_id: str = "c0111de5c0111de5") -> None:
    """Two identities map to one id: exercises the collision safeguard, not a real hash collision."""
    monkeypatch.setattr(provider, "_sandbox_id_for_thread", lambda *_a, **_k: sandbox_id)


def _mutations(docker) -> list[list[str]]:
    return [cmd for cmd in docker.commands if cmd[1] in ("stop", "rm") or (cmd[1] == "network" and cmd[2] == "rm")]


def test_conflicting_known_identity_is_refused_before_any_cleanup_without_pending(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    _collide(provider, monkeypatch)
    a = _acquire_accepted(provider, "thread-x")
    provider.release(a)
    docker.commands.clear()

    with pytest.raises(SandboxIdentityCollisionError):
        _acquire_accepted(provider, "thread-y")

    assert _mutations(docker) == []
    assert a in provider._warm_pool and len(docker.present(a)) == 4
    assert backend.created == [a]


def test_conflicting_known_identity_is_refused_before_any_cleanup_with_pending(tmp_path, monkeypatch):
    """Pending cleanup under a colliding id is not the second identity's to drive."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    _collide(provider, monkeypatch)
    a = _acquire_accepted(provider, "thread-x")
    provider.release(a)
    docker.faults["all"] = "refuse"
    _drift(monkeypatch)
    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_inputs_changed"):
        _acquire_accepted(provider, "thread-x", run_id="run-2")
    assert a in provider._cleanup_pending
    docker.faults.clear()
    docker.commands.clear()

    with turn_phases(correlation_id="collide-pending") as journal, pytest.raises(SandboxIdentityCollisionError):
        _acquire_accepted(provider, "thread-y")

    assert _mutations(docker) == [], "the colliding identity drove no cleanup"
    assert a in provider._cleanup_pending and a in provider._warm_pool
    assert len(docker.present(a)) == 4
    assert backend.created == [a]
    assert _counts(journal.snapshot()) == {"attempts": 0, "teardowns": 0, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert provider._warm_pool_identity[a] == (ACCEPTED_USER, "thread-x")

    # The owning identity may still finish its own cleanup and rebuild.
    assert _acquire_accepted(provider, "thread-x", run_id="run-3") == a
    assert a not in provider._cleanup_pending


def test_conflicting_known_identity_is_refused_while_the_id_is_active(tmp_path, monkeypatch):
    provider, backend, docker = _make(tmp_path, monkeypatch)
    _collide(provider, monkeypatch)
    a = _acquire_accepted(provider, "thread-x")
    docker.commands.clear()

    with pytest.raises(SandboxIdentityCollisionError):
        _acquire_accepted(provider, "thread-y")

    assert _mutations(docker) == []
    assert provider.get(a) is not None and backend.created == [a]


# ── Active lookup versus the teardown reservation ───────────────────────
#
# The idle checker reserves an active id (`_destroy_tracked`) and then claims
# ownership before it untracks, so a refused claim can recover. In that
# interval `get` and the accepted active shortcut used to answer from the
# active map alone and hand out the very set the reservation is authorized
# to stop. These tests schedule the lookups deterministically inside that
# interval through the real idle pass; ownership is the committed memory
# store, creation/readiness/Docker are the fakes, and the accepted
# acquisition entry point runs without a real accepted-material binding or
# durable invocation. The skill-projection coordinator reports nothing busy
# here, so the predicate's owner term is not exercised; nothing below is a
# public-route or durable-runtime claim.


def _idle_active(provider, thread_id: str = "thread-a") -> str:
    """An active (not parked) accepted set whose activity is aged so the real idle pass selects it."""
    a = _acquire_accepted(provider, thread_id)
    provider._last_activity[a] = 0.0
    return a


def _lookups_inside_claim(provider, monkeypatch, sandbox_id: str, *, refuse_claim: bool = False) -> dict[str, object]:
    """Run `get` and an accepted re-acquisition once the teardown reservation is held, before the claim."""
    seen: dict[str, object] = {}
    real_claim = provider._claim_ownership

    def _claim(sid, *, for_destroy=False):
        if sid == sandbox_id and for_destroy and "reserved" not in seen:
            seen["reserved"] = sid in provider._local_teardown
            seen["get"] = provider.get(sid)
            try:
                seen["acquire"] = _acquire_accepted(provider, "thread-a", run_id="run-2")
            except Exception as exc:  # noqa: BLE001 - the class is asserted by the caller
                seen["acquire"] = exc
            seen["activity"] = provider._last_activity.get(sid)
            if refuse_claim:
                # Injected refusal, the shape `_claim_ownership` itself takes on
                # a store error or a peer's lease: the destroy path must recover.
                return False
        return real_claim(sid, for_destroy=for_destroy)

    monkeypatch.setattr(provider, "_claim_ownership", _claim)
    return seen


def test_lookups_after_the_idle_teardown_reservation_do_not_hand_out_the_set(tmp_path, monkeypatch):
    """Teardown wins first: once reserved, `get` and the accepted active reuse refuse; the stop then confirms."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a = _idle_active(provider)
    seen = _lookups_inside_claim(provider, monkeypatch, a)

    with turn_phases(correlation_id="idle-race") as journal:
        provider._cleanup_idle_sandboxes(1.0)

    assert seen["reserved"] is True, "the lookups ran with the reservation held"
    assert seen["get"] is None, "a lookup after the teardown decision does not get the set"
    assert isinstance(seen["acquire"], SandboxBeingDestroyedError), seen["acquire"]
    assert seen["activity"] == 0.0, "refused lookups refreshed no activity"
    assert docker.present(a) == set(), "the reservation's stop confirmed"
    assert a not in provider._sandboxes and a not in provider._warm_pool and a not in provider._cleanup_pending
    assert a not in provider._local_teardown
    assert provider.get(a) is None
    assert provider._ownership.owner(a) is None
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert backend.created == [a]
    # Recovery: the next acquisition builds one fresh generation.
    with turn_phases(correlation_id="after") as after:
        assert _acquire_accepted(provider, "thread-a", run_id="run-3") == a
    assert after.snapshot().acquisition_source is AcquisitionSource.CREATED
    assert backend.created == [a, a] and len(docker.present(a)) == 4
    assert provider.get(a) is not None


def test_control_activity_before_the_reservation_makes_idle_cleanup_refuse(tmp_path, monkeypatch):
    """Activity wins first: a `get` and a reuse before the reservation are respected by the predicate."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a = _idle_active(provider)
    seen: dict[str, object] = {}
    real_destroy_tracked = provider._destroy_tracked

    def _destroy_tracked(sandbox_id, *, still_reapable):
        if sandbox_id == a and "get" not in seen:
            seen["get"] = provider.get(a)
            seen["acquire"] = _acquire_accepted(provider, "thread-a", run_id="run-2")
        return real_destroy_tracked(sandbox_id, still_reapable=still_reapable)

    monkeypatch.setattr(provider, "_destroy_tracked", _destroy_tracked)
    with turn_phases(correlation_id="idle-control") as journal:
        provider._cleanup_idle_sandboxes(1.0)

    assert seen["get"] is not None and seen["acquire"] == a
    assert len(docker.present(a)) == 4
    assert provider.get(a) is not None and a in provider._sandboxes
    assert provider._ownership.owner(a) == provider._owner_id
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 1, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert _mutations(docker) == []


def test_control_accepted_reuse_alone_before_the_reservation_is_activity(tmp_path, monkeypatch):
    """A reuse that wins before the teardown decision is protected by the predicate on its own, without a `get`."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a = _idle_active(provider)
    seen: dict[str, object] = {}
    real_destroy_tracked = provider._destroy_tracked

    def _destroy_tracked(sandbox_id, *, still_reapable):
        if sandbox_id == a and "acquire" not in seen:
            seen["acquire"] = _acquire_accepted(provider, "thread-a", run_id="run-2")
        return real_destroy_tracked(sandbox_id, still_reapable=still_reapable)

    monkeypatch.setattr(provider, "_destroy_tracked", _destroy_tracked)
    with turn_phases(correlation_id="idle-reuse-control") as journal:
        provider._cleanup_idle_sandboxes(1.0)

    assert seen["acquire"] == a
    assert provider._last_activity[a] > 0.0, "the reuse counted as activity"
    assert len(docker.present(a)) == 4 and _mutations(docker) == []
    assert provider.get(a) is not None and a in provider._sandboxes
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 1, "failures": 0, "creates": 0, "rediscoveries": 0}


def test_a_refused_ownership_claim_after_refused_lookups_leaves_the_set_usable(tmp_path, monkeypatch):
    """The lookups refuse while reserved; the claim then refuses; nothing is stopped and the handle is back."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a = _idle_active(provider)
    seen = _lookups_inside_claim(provider, monkeypatch, a, refuse_claim=True)

    with turn_phases(correlation_id="idle-claim-refused") as journal:
        provider._cleanup_idle_sandboxes(1.0)

    assert seen["get"] is None and isinstance(seen["acquire"], SandboxBeingDestroyedError)
    assert seen["activity"] == 0.0 and provider._last_activity[a] == 0.0, "nothing refreshed on the refused lookups' behalf"
    assert len(docker.present(a)) == 4 and _mutations(docker) == []
    assert a in provider._sandboxes and a not in provider._local_teardown and a not in provider._cleanup_pending
    assert provider.get(a) is not None, "usable again once the reservation is released"
    assert _acquire_accepted(provider, "thread-a", run_id="run-3") == a
    assert provider._ownership.owner(a) == provider._owner_id, "still ours; nothing took ownership on the refused lookup's behalf"
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 1, "failures": 0, "creates": 0, "rediscoveries": 0}


def test_a_partial_idle_cleanup_after_refused_lookups_stays_quarantined(tmp_path, monkeypatch):
    """The pass that quarantines a set does not retry it: one attempt per trigger on the active-idle path."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a = _idle_active(provider)
    _sandbox, proxy, *_ = docker.members(a)
    docker.faults[f"stop:{proxy}"] = "refuse"
    docker.faults[f"rm:{proxy}"] = "refuse"
    seen = _lookups_inside_claim(provider, monkeypatch, a)

    with turn_phases(correlation_id="idle-partial") as journal:
        provider._cleanup_idle_sandboxes(1.0)

    assert seen["get"] is None and isinstance(seen["acquire"], SandboxBeingDestroyedError)
    assert seen["activity"] == 0.0
    assert docker.present(a) == {proxy}
    assert a in provider._warm_pool and provider._cleanup_pending[a] is DestroyOutcome.PARTIAL
    assert provider.get(a) is None
    with pytest.raises(AcceptedSkillSandboxBindingError, match="accepted_sandbox_cleanup_pending"):
        _acquire_accepted(provider, "thread-a", run_id="run-3")
    assert _counts(journal.snapshot()) == {"attempts": 1, "teardowns": 0, "refusals": 0, "failures": 1, "creates": 0, "rediscoveries": 0}

    docker.faults.clear()
    with turn_phases(correlation_id="idle-recovered") as recovered:
        assert _acquire_accepted(provider, "thread-a", run_id="run-4") == a
    assert _counts(recovered.snapshot()) == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 1, "rediscoveries": 0}
    assert len(docker.present(a)) == 4 and a not in provider._cleanup_pending


def _claim_gated(provider, monkeypatch, sandbox_id: str, *, gate_thread: threading.Thread | None = None):
    """Hold the idle pass open inside its teardown claim (reservation held, set still tracked)."""
    in_claim = threading.Event()
    allow = threading.Event()
    real_claim = provider._claim_ownership
    gated: list[int] = []

    def _claim(sid, *, for_destroy=False):
        mine = threading.current_thread() is gate_thread if gate_thread is not None else not gated
        if sid == sandbox_id and for_destroy and mine:
            gated.append(1)
            in_claim.set()
            assert allow.wait(timeout=5)
        return real_claim(sid, for_destroy=for_destroy)

    monkeypatch.setattr(provider, "_claim_ownership", _claim)
    return in_claim, allow


def test_lookups_during_the_idle_claim_round_trip_are_refused(tmp_path, monkeypatch):
    """Concurrent form: the idle pass is held inside its claim on its own thread; lookups on this one refuse."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a = _idle_active(provider)
    result: dict[str, object] = {}

    def _idle():
        try:
            with turn_phases(correlation_id="idle-claim") as journal:
                provider._cleanup_idle_sandboxes(1.0)
            result["counts"] = _counts(journal.snapshot())
        except Exception as exc:  # noqa: BLE001 - surfaced below
            result["error"] = exc

    worker = threading.Thread(target=_idle, name="idle-checker")
    in_claim, allow = _claim_gated(provider, monkeypatch, a, gate_thread=worker)
    worker.start()
    try:
        assert in_claim.wait(timeout=5)
        assert a in provider._local_teardown and a in provider._sandboxes, "reserved, still tracked"
        started = time.monotonic()
        with pytest.raises(SandboxBeingDestroyedError):
            _acquire_accepted(provider, "thread-a", run_id="run-2")
        assert time.monotonic() - started < 1.0
        assert provider.get(a) is None
        assert provider._last_activity[a] == 0.0
        assert backend.created == [a] and _mutations(docker) == []
    finally:
        allow.set()
        worker.join(timeout=5)
    assert not worker.is_alive() and "error" not in result, result.get("error")
    assert docker.present(a) == set() and provider.get(a) is None
    assert result["counts"] == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert _acquire_accepted(provider, "thread-a", run_id="run-3") == a
    assert len(docker.present(a)) == 4


@pytest.mark.anyio
async def test_async_accepted_reuse_during_the_idle_claim_round_trip_is_refused(tmp_path, monkeypatch):
    """The async accepted route (binding stubbed to a no-op) refuses the same window."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a = _idle_active(provider)
    monkeypatch.setattr(provider, "bind_accepted_skill_snapshot", lambda *_a, **_k: None)
    in_claim, allow = _claim_gated(provider, monkeypatch, a)
    result: dict[str, object] = {}

    def _idle():
        with turn_phases(correlation_id="async-idle-claim") as journal:
            provider._cleanup_idle_sandboxes(1.0)
        result["counts"] = _counts(journal.snapshot())

    idle = asyncio.ensure_future(asyncio.to_thread(_idle))
    try:
        assert await asyncio.to_thread(in_claim.wait, 5)
        assert a in provider._local_teardown and a in provider._sandboxes
        with pytest.raises(SandboxBeingDestroyedError):
            await provider.provision_accepted_skills_async("thread-a", user_id=ACCEPTED_USER, binding=_binding(run_id="run-2"))
        assert provider.get(a) is None
        assert provider._last_activity[a] == 0.0
        assert backend.created == [a] and _mutations(docker) == []
    finally:
        allow.set()
        await idle
    assert docker.present(a) == set()
    assert result["counts"] == {"attempts": 1, "teardowns": 1, "refusals": 0, "failures": 0, "creates": 0, "rediscoveries": 0}
    assert await provider.provision_accepted_skills_async("thread-a", user_id=ACCEPTED_USER, binding=_binding(run_id="run-3")) == a
    assert len(docker.present(a)) == 4


def test_reservation_flag_alone_refuses_get_and_accepted_reuse(tmp_path, monkeypatch):
    """Defense-in-depth at the unit level, not the composed acceptance proof."""
    provider, backend, docker = _make(tmp_path, monkeypatch)
    a = _acquire_accepted(provider, "thread-a")
    before = provider._last_activity[a]
    provider._local_teardown.add(a)

    assert provider.get(a) is None
    assert provider._last_activity[a] == before, "a refused lookup does not refresh activity"
    with pytest.raises(SandboxBeingDestroyedError):
        _acquire_accepted(provider, "thread-a", run_id="run-2")
    provider._local_teardown.discard(a)
    assert provider.get(a) is not None
    assert _acquire_accepted(provider, "thread-a", run_id="run-3") == a
    assert _mutations(docker) == [] and backend.created == [a]
