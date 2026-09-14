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

import pytest
from test_sandbox_warm_reuse_latency import ACCEPTED_USER, _acquire_accepted, _aio_mod, _binding, _make_provider

from deerflow.community.aio_sandbox import aio_sandbox_provider as provider_mod
from deerflow.community.aio_sandbox import backend as backend_mod
from deerflow.community.aio_sandbox.aio_sandbox_provider import SandboxBeingDestroyedError
from deerflow.community.aio_sandbox.local_backend import LocalContainerBackend
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
