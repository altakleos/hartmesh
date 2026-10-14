"""Private resident submission boundary; chat journal identities stay chat-only."""

import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Lock
from types import MappingProxyType

from deerflow.spaces.docker import ATTACHMENT_LABEL, HOST_LABEL, StorageHostUnqualified
from deerflow.spaces.preparation import current_preparation
from deerflow.spaces.runtime_owner import PoolRuntimeOwner

from .native_creation import NativeCreateBridge

_submission = ContextVar("native_resident_submission", default=None)
_SLOT_PREFIX = "hartmesh.storage.space."


def _configured_mounts(backend):
    mounts = tuple((str(m.host_path), str(m.container_path), m.read_only) for m in backend._config_mounts)
    if any(mode is not True for _source, _destination, mode in mounts):
        raise StorageHostUnqualified("Workspaces refuse a writable configured sandbox mount")
    return mounts


@dataclass(eq=False)
class _Submission:
    backend: object
    name: str
    labels: object
    mounts: tuple
    configured: tuple
    network_mode: str
    primary: object
    owner: object = None
    bridge: object = None
    live: bool = True
    submitted: bool = False
    lock: object = field(default_factory=Lock, repr=False)

    def assert_current(self):
        if not self.live:
            raise StorageHostUnqualified("Resident submission capability expired")
        expected = dict(self.labels) if self.backend.network_mode == "open" else {**self.backend._restricted_labels(self.labels[ATTACHMENT_LABEL], "resident"), **self.labels}
        if self.backend.network_mode != self.network_mode or expected != dict(self.primary) or _configured_mounts(self.backend) != self.configured:
            raise StorageHostUnqualified("Resident submission topology changed after admission")
        if self.owner is not None:
            runner = getattr(self.backend, "_runtime_runner", None)
            provider = self.owner.provider
            if (
                getattr(runner, "__self__", None) is not self.owner
                or getattr(runner, "__func__", None) is not PoolRuntimeOwner.run
                or getattr(provider, "_backend", None) is not self.backend
                or self.backend._native_creation is not self.bridge
                or not self.bridge.ready
            ):
                raise StorageHostUnqualified("Resident submission lost its exact runtime binding")
            self.owner.assert_provider(provider)
            self.bridge.assert_current()

    def primary_labels(self):
        return dict(self.primary)


@contextmanager
def resident_submission(backend, sandbox_id, name, mounts, labels):
    if not isinstance(sandbox_id, str) or not re.fullmatch(r"[0-9a-f]{32}", sandbox_id) or name != "hartmesh-space-" + sandbox_id:
        raise StorageHostUnqualified("Resident preparation requires its exact attachment identity")
    if (
        not isinstance(labels, dict)
        or labels.get(ATTACHMENT_LABEL) != sandbox_id
        or not isinstance(labels.get(HOST_LABEL), str)
        or not labels[HOST_LABEL]
        or any(not isinstance(key, str) or not isinstance(value, str) for key, value in labels.items())
        or any(key in labels for key in ("deerflow.owner_user_id", "deerflow.thread_id", "hartmesh.chat.storage"))
        or labels.get("deerflow.role") not in (None, "resident")
    ):
        raise StorageHostUnqualified("Resident preparation requires storage attribution without chat identity")
    owner = bridge = None
    if getattr(backend, "_managed_native_required", False):
        token = current_preparation()
        if token is None or getattr(token.provider, "_backend", None) is not backend:
            raise StorageHostUnqualified("Resident preparation requires its committed attachment token")
        plan = token.plan
        if plan.id != sandbox_id or plan.host_id != token.provider.host_id:
            raise StorageHostUnqualified("Resident preparation differs from committed attachment admission")
        if any(not isinstance(view.slot_id, str) or not re.fullmatch(r"[0-9a-f]{32}", view.slot_id) for view in plan.views):
            raise StorageHostUnqualified("Resident preparation lacks qualified physical slot identities")
        projection = getattr(token.provider, "_resident_projection", None)
        if not callable(projection):
            raise StorageHostUnqualified("Resident preparation lacks its exact admitted projection")
        expected_mounts, expected_labels = projection(plan)
        if tuple(mounts) != tuple(expected_mounts) or labels != expected_labels:
            raise StorageHostUnqualified("Resident mounts or labels differ from the committed preparation")
        runner = getattr(backend, "_runtime_runner", None)
        owner = getattr(runner, "__self__", None)
        bridge = getattr(backend, "_native_creation", None)
        if not isinstance(owner, PoolRuntimeOwner) or getattr(runner, "__func__", None) is not PoolRuntimeOwner.run or not isinstance(bridge, NativeCreateBridge):
            raise StorageHostUnqualified("Resident preparation requires the exact owning runtime runner")
        if plan.host_id != bridge.evidence.daemon_id:
            raise StorageHostUnqualified("Resident preparation names another Docker host")
        try:
            token.claim()
        except ValueError as exc:
            raise StorageHostUnqualified("Resident preparation token has already been consumed") from exc
    resident_labels = {**labels, "deerflow.role": "resident"}
    primary = resident_labels if backend.network_mode == "open" else {**backend._restricted_labels(sandbox_id, "resident"), **resident_labels}
    proof = _Submission(backend, name, MappingProxyType(resident_labels), tuple(mounts), _configured_mounts(backend), backend.network_mode, MappingProxyType(primary), owner, bridge)
    proof.assert_current()
    if owner is not None:
        # An opaque coordination label, never a resolving human principal.
        # The caller retains this pure slot reference through CID commit.
        gate = owner.provider._owner_admission_gate()
        reference = gate.enter("resident:" + sandbox_id)
        try:
            gate.register_slots(reference, [key[len(_SLOT_PREFIX) :] for key in labels if key.startswith(_SLOT_PREFIX)])
            token.retain_release(lambda: gate.end(reference))
        except BaseException:
            gate.end(reference)
            raise
    reset = _submission.set(proof)
    try:
        yield proof
    finally:
        with proof.lock:
            proof.live = False
        _submission.reset(reset)


def require_resident_submission(backend, labels):
    proof = _submission.get()
    if not isinstance(proof, _Submission) or proof.backend is not backend or labels not in (dict(proof.labels), proof.primary_labels()):
        raise StorageHostUnqualified("Resident labels require a live committed submission capability")
    proof.assert_current()
    return proof


def consume_resident_command(backend, name, mounts, labels, *, start, publish):
    proof = require_resident_submission(backend, labels)
    if start is not False or publish is not False or name != proof.name or tuple(mounts or ()) != proof.mounts or _configured_mounts(backend) != proof.configured or labels != proof.primary_labels():
        raise StorageHostUnqualified("Resident command differs from its stopped admitted view")
    with proof.lock:
        if not proof.live or proof.submitted:
            raise StorageHostUnqualified("Resident primary submission has already been consumed")
        proof.submitted = True
    return proof
