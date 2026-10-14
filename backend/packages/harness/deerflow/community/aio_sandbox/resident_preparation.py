"""Private resident submission boundary; chat journal identities stay chat-only."""

import os
import re
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from threading import Lock
from types import MappingProxyType
from uuid import uuid4

from deerflow.spaces.docker import ATTACHMENT_LABEL, HOST_LABEL, StorageHostUnqualified
from deerflow.spaces.native_resident_evidence import NativeResidentRecord, NativeResidentRoot
from deerflow.spaces.native_teardown_evidence import NativeTeardownResource
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


def _network_target(backend, attachment_id):
    if backend.network_mode != "open":
        return backend._resource_names(attachment_id)[1]
    from .local_backend import _effective_docker_network_target

    target = _effective_docker_network_target(os.environ.get("DEER_FLOW_SANDBOX_NETWORK", "").strip())
    return "bridge" if target in ("", "default") else target


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
    incarnation: str | None = None
    roots: tuple = ()
    network_target: str | None = None
    record: object = None
    live: bool = True
    submitted: bool = False
    lock: object = field(default_factory=Lock, repr=False)

    def assert_current(self):
        if not self.live:
            raise StorageHostUnqualified("Resident submission capability expired")
        expected = dict(self.labels) if self.backend.network_mode == "open" else {**self.backend._restricted_labels(self.labels[ATTACHMENT_LABEL], "resident"), **self.labels}
        if self.backend.network_mode != self.network_mode or expected != dict(self.primary) or _configured_mounts(self.backend) != self.configured:
            raise StorageHostUnqualified("Resident submission topology changed after admission")
        if _network_target(self.backend, self.labels[ATTACHMENT_LABEL]) != self.network_target:
            raise StorageHostUnqualified("Resident declared network target changed after admission")
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
            existing = self.bridge.residents.for_attachment(self.labels[ATTACHMENT_LABEL])
            if existing is not None and (existing != self.record or existing.phase != "prepared"):
                raise StorageHostUnqualified("Resident preparation intersects a retained original graph")

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
    roots = ()
    incarnation = None
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
        captured = []
        for view in plan.views:
            root = view.native_root
            if not isinstance(root, NativeResidentRoot) or (root.slot_id, root.source, root.destination, root.writable, root.space_id, root.generation) != (
                view.slot_id,
                view.source,
                view.destination,
                view.writable,
                view.space_id,
                view.generation,
            ):
                raise StorageHostUnqualified("Resident preparation lacks its exact qualified root facts")
            captured.append(NativeResidentRoot.parse(root.__dict__))
        captured.extend(NativeResidentRoot.from_readonly(root) for root in plan.readonly_roots)
        roots, incarnation = tuple(captured), plan.incarnation
        try:
            token.claim()
        except ValueError as exc:
            raise StorageHostUnqualified("Resident preparation token has already been consumed") from exc
    resident_labels = {**labels, "deerflow.role": "resident"}
    primary = resident_labels if backend.network_mode == "open" else {**backend._restricted_labels(sandbox_id, "resident"), **resident_labels}
    proof = _Submission(backend, name, MappingProxyType(resident_labels), tuple(mounts), _configured_mounts(backend), backend.network_mode, MappingProxyType(primary), owner, bridge)
    proof.roots, proof.incarnation = roots, incarnation
    proof.network_target = _network_target(backend, sandbox_id)
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


def capture_resident_resources(proof, container):
    """One stopped result is durable before it can cross the CID boundary."""
    if proof.owner is None:
        return
    from .native_resource_set import NativeResourceSet

    proof.assert_current()
    reader = NativeResourceSet(proof.backend, guard=proof.bridge.assert_current)
    try:
        primary = reader.inspect("container", container)
        if primary is None or reader._state(primary) or primary["State"]["Status"] != "created" or primary["Name"] != "/" + proof.name:
            raise StorageHostUnqualified("Resident original stopped primary is unconfirmed")
        paired = reader.inventory_paired()
        attachment = proof.labels[ATTACHMENT_LABEL]
        resources = tuple(item.fact for item in paired if item.sandbox_id == attachment)
        shared = None
        if proof.network_mode == "open" and proof.network_target != "none":
            network = reader.inspect("network", proof.network_target)
            if network is None:
                raise StorageHostUnqualified("Resident declared shared network is absent")
            shared = NativeTeardownResource("network", "shared-network", network["Id"], network["Name"], network.get("Created"))
        record = NativeResidentRecord(
            uuid4().hex,
            proof.bridge.evidence.daemon_id,
            attachment,
            proof.incarnation,
            container,
            proof.name,
            primary.get("Created"),
            "prepared",
            proof.network_mode,
            proof.primary.get("deerflow.network_policy_digest"),
            proof.roots,
            tuple((source, destination, False) for source, destination, _readonly in proof.configured),
            resources,
            network_target=proof.network_target,
            shared_network=shared,
        )
        entries = reader.qualify_resident_record(record)
        if any(value is None for value in entries.values()):
            raise StorageHostUnqualified("Resident original capture requires a complete graph")
        proof.record = proof.bridge.evidence.begin_resident(record)
        proof.bridge.qualify()
        proof.assert_current()
        if any(value is None for value in reader.qualify_resident_record(proof.record).values()):
            raise StorageHostUnqualified("Resident original graph changed after durable publication")
    except Exception as exc:
        proof.bridge._check_failure_evidence()
        raise StorageHostUnqualified("Resident original graph publication is unconfirmed; retain its preparation and accounting") from exc
