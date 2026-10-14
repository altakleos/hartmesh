"""Cleanup-only host capabilities around canonical fences and durable evidence."""

import logging
import time
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from uuid import uuid4

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_teardown_evidence import NativeMountlessTeardownRecord, NativeTeardownRecord

from .backend import DestroyOutcome
from .native_resource_set import NativeResourceSet
from .native_teardown import current_native_lease

_RETIREMENT = ContextVar("native_teardown_retirement", default=None)
_CLEANUP = ContextVar("native_teardown_capability", default=None)
logger = logging.getLogger(__name__)


@contextmanager
def _cleanup_admission(provider, owner_id, slots):
    """Cleanup retains its own owner; never borrows another acquirer's reference."""
    from .owner_admission import OWNER_ADMISSION

    inherited = OWNER_ADMISSION.get()
    separate = False
    if inherited is not None:
        gate = provider._owner_admission_gate()
        with provider._lock:
            if not gate.admission_current_locked(inherited):
                raise StorageHostUnqualified("Native cleanup inherited an expired or foreign admission reference")
            separate = inherited.owner_id != owner_id
    context = OWNER_ADMISSION.set(None) if separate else None
    try:
        with provider._native_admission(owner_id, slots):
            yield
    finally:
        if context is not None:
            OWNER_ADMISSION.reset(context)


def recover_teardowns(provider):
    """Bounded ordinary recovery; durable correlation never becomes a usable client."""
    from .local_backend import _NATIVE_SLOT_INVENTORY_DEADLINE, LocalContainerBackend, NativeSlotContainer
    from .sandbox_info import SandboxInfo

    bridge = getattr(provider, "_native_creation", None)
    if bridge is None or not provider._managed_pool_required() or not isinstance(provider._backend, LocalContainerBackend) or provider._backend._runtime != "docker" or not provider._pool_runtime_current():
        return
    inherited = _NATIVE_SLOT_INVENTORY_DEADLINE.get()
    deadline = min(time.monotonic() + 60, inherited) if inherited is not None else time.monotonic() + 60
    token = _NATIVE_SLOT_INVENTORY_DEADLINE.set(deadline)
    try:
        bridge.assert_current()
        records = sorted((record for record in bridge.teardowns.records() if record.daemon_id == bridge.evidence.daemon_id and record.target_slot is None), key=lambda record: record.id)
        if not records:
            return
        with provider._lock:
            offset = getattr(provider, "_native_teardown_cursor", 0) % len(records)
        candidates = (records[offset:] + records[:offset])[:4]
        for index, record in enumerate(candidates):
            if time.monotonic() >= deadline:
                break
            with provider._lock:
                provider._native_teardown_cursor = (offset + index + 1) % len(records)
            try:
                bridge.assert_current()
                if record not in bridge.teardowns.records():
                    continue
                with _cleanup_admission(provider, record.owner_id, record.slots):
                    bridge.assert_current()
                    if record not in bridge.teardowns.records() or time.monotonic() >= deadline:
                        continue
                    actual = NativeSlotContainer(
                        record.container_id, record.name, record.sandbox_id, (record.owner_id, record.thread_id), record.slots, record.daemon_id, False, False, record.created_at, record.network_mode, record.policy_digest, record.resources
                    )
                    info = SandboxInfo(record.sandbox_id, "", container_id=record.container_id, container_name=record.name, owner=actual.owner, chat_storage_enabled=True, native_container=actual, native_slots=record.slots)
                    provider._retire_unheld_container(record.sandbox_id, info, actual.owner, _expected_teardown=record)
            except Exception as exc:
                logger.warning("Ordinary native teardown remains pending for %s: %s", record.sandbox_id, type(exc).__name__)
    except Exception as exc:
        logger.warning("Ordinary native teardown reconciliation is unqualified: %s", type(exc).__name__)
    finally:
        _NATIVE_SLOT_INVENTORY_DEADLINE.reset(token)


@dataclass(frozen=True)
class _Completion:
    record: NativeTeardownRecord
    evidence: object


@contextmanager
def retirement_scope(scope):
    scope.assert_current()
    token = _RETIREMENT.set(scope)
    try:
        yield
    finally:
        _RETIREMENT.reset(token)


def eligible_teardowns(scope):
    scope.assert_current()
    reservation = scope.reservation
    relevant = tuple(record for record in scope.bridge.teardowns.records() if record.owner_id == reservation.owner_id or reservation.slot_id in record.slots)
    if any(record.owner_id != reservation.owner_id or record.daemon_id != scope.bridge.evidence.daemon_id or reservation.slot_id not in record.slots or record.target_slot not in (None, reservation.slot_id) for record in relevant):
        raise StorageHostUnqualified("Native containment intersects incompatible original teardown evidence")
    return relevant


class _Capability:
    def __init__(self, provider, info, proof):
        self.provider, self.info, self.proof = provider, info, proof
        self.bridge = provider._native_creation
        self.origin = (info.sandbox_id, info.container_id, info.container_name, info.owner)
        self.outer = _RETIREMENT.get()
        self.record = None
        self.completed = False
        self.active = True
        self.owned = ExitStack()
        candidates = [record for record in self.bridge.teardowns.records() if record.container_id == info.container_id or record.name == info.container_name]
        if candidates:
            if len(candidates) != 1:
                raise StorageHostUnqualified("Native cleanup has conflicting primary correlation")
            self.bind(candidates[0])
        completion = info.native_completion
        if completion is not None:
            if not isinstance(completion, _Completion) or completion.evidence is not self.bridge.evidence or candidates:
                raise StorageHostUnqualified("Native completion handoff differs from this exact owning journal")
            self.bind(completion.record)
            self.completed = True

    def assert_current(self):
        if (
            not self.active
            or _CLEANUP.get() is not self
            or current_native_lease(self.provider, self.info.sandbox_id) is not self.proof
            or self.provider._native_creation is not self.bridge
            or self.provider._backend._native_creation is not self.bridge
        ):
            raise StorageHostUnqualified("Native cleanup lost its exact holder, lease or runtime lifetime")
        if (self.info.sandbox_id, self.info.container_id, self.info.container_name, self.info.owner) != self.origin:
            raise StorageHostUnqualified("Native cleanup changed its exact candidate incarnation")
        pending = self.bridge.assert_current(teardown=True)
        if self.outer is not None:
            if self.outer.provider is not self.provider or self.outer.bridge is not self.bridge:
                raise StorageHostUnqualified("Native cleanup has a foreign retirement scope")
            self.outer.assert_current()
        for record in pending:
            target = self.outer.reservation.slot_id if self.outer is not None else None
            if record.owner_id == self.info.owner[0] or record.name == self.info.container_name or (target is not None and target in record.slots):
                if self.outer is not None or (record.owner_id, record.thread_id, record.name) != (*self.info.owner, self.info.container_name) or not self.bridge.handoffs.known_returned(self.info):
                    raise StorageHostUnqualified("An unrelated or unreturned native request prevents cleanup")

    def bind(self, record):
        if (record.daemon_id, record.sandbox_id, record.container_id, record.name, (record.owner_id, record.thread_id)) != (self.bridge.evidence.daemon_id, *self.origin[:3], self.origin[3]) or self.info.native_slots not in (
            None,
            record.slots,
        ):
            raise StorageHostUnqualified("Native cleanup record differs from its exact candidate")
        if self.outer is None:
            if record.target_slot is not None:
                raise StorageHostUnqualified("Ordinary cleanup cannot resume a targeted retirement record")
        else:
            reservation = self.outer.reservation
            if record.owner_id != reservation.owner_id or reservation.slot_id not in record.slots or record.target_slot not in (None, reservation.slot_id):
                raise StorageHostUnqualified("Native cleanup record has no current exact target reservation")
        self.record = record

    def permits(self, info):
        if info is not self.info:
            return False
        self.assert_current()
        identities = {info.container_id}
        names = {("container", info.container_name)}
        if self.record is not None:
            identities.update(item.identity for item in self.record.resources)
            names.update((item.kind, item.name) for item in self.record.resources)
        for other in self.bridge.teardowns.records():
            if self.record is not None and other == self.record:
                continue
            other_ids = {other.container_id, *(item.identity for item in other.resources)}
            other_names = {("container", other.name), *((item.kind, item.name) for item in other.resources)}
            if identities & other_ids or names & other_names:
                raise StorageHostUnqualified("Native cleanup intersects another immutable resource set")
            intersects = other.owner_id == info.owner[0] or (other.target_slot is not None and other.target_slot in (info.native_slots or ()))
            if intersects and (other.daemon_id != self.bridge.evidence.daemon_id or other.owner_id != info.owner[0]):
                raise StorageHostUnqualified("Native cleanup intersects foreign teardown evidence")
        return True

    def _admit(self, slots):
        if self.outer is None:
            self.owned.enter_context(_cleanup_admission(self.provider, self.info.owner[0], slots))
        else:
            self.outer.assert_current()
            if self.outer.reservation.owner_id != self.info.owner[0] or self.outer.reservation.slot_id not in slots:
                raise StorageHostUnqualified("Native cleanup lacks its actual target slot")

    def prepare(self):
        self.assert_current()
        backend = self.provider._backend
        if self.record is None:
            actual = self.info.native_container
            if actual is None:
                actual = backend.native_resource_candidate(self.info)
            if (actual.container_id, actual.container_name, actual.sandbox_id, actual.owner) != (self.info.container_id, self.info.container_name, self.info.sandbox_id, self.info.owner):
                raise StorageHostUnqualified("Native cleanup lacks exact actual primary attribution")
            self._admit(actual.slots)
            with self.provider._lock:
                self.info.native_container, self.info.native_slots = actual, actual.slots
            if actual.network_mode == "open":
                return
            target = self.outer.reservation.slot_id if self.outer is not None else None
            record_type = NativeTeardownRecord if actual.slots else NativeMountlessTeardownRecord
            self.bind(
                record_type(uuid4().hex, actual.daemon_id, *actual.owner, actual.sandbox_id, actual.slots, target, actual.container_id, actual.container_name, actual.created_at, actual.network_mode, actual.policy_digest, actual.resources)
            )
            # A captured DTO alone does not permit publication or effects.
            survivors = NativeResourceSet(backend, guard=self.assert_current).qualify(self.record)
            if any(entry is None for entry in survivors.values()):
                raise StorageHostUnqualified("Initial teardown publication requires the complete actual set")
        else:
            self._admit(self.record.slots)
            with self.provider._lock:
                self.info.native_slots = self.record.slots
            survivors = NativeResourceSet(backend, guard=self.assert_current).qualify(self.record)
            if all(entry is None for entry in survivors.values()):
                return
            if self.completed:
                raise StorageHostUnqualified("Completed native handoff no longer has exact full-set absence")
        try:
            saved = self.bridge.evidence.begin_teardown(self.record)
            self.bind(saved)
            self.bridge.teardowns.add(saved)
            self.bridge.qualify(teardown=True)
        except BaseException:
            self.bridge._check_failure_evidence(teardown=True)
            raise
        self.assert_current()
        self.permits(self.info)

    def destroy(self):
        self.prepare()
        backend = self.provider._backend
        if self.record is None:
            return backend._destroy_native_primary(self.info)
        if self.completed:
            if not NativeResourceSet(backend, guard=self.assert_current).prove_absent(self.record):
                raise StorageHostUnqualified("Completed native handoff no longer has exact full-set absence")
            self.assert_current()
            return DestroyOutcome.ABSENT
        by_role = {item.role: item for item in self.record.resources}
        members = [("container", self.record.container_id), *((by_role[role].kind, by_role[role].identity) for role in ("network-proxy", "network", "egress-network") if role in by_role)]
        for kind, identity in members:
            entries = NativeResourceSet(backend, guard=self.assert_current).qualify(self.record)
            entry = entries[(kind, identity)]
            if entry is None:
                continue
            if kind == "container" and entry["State"]["Running"]:
                self.assert_current()
                backend._stop_container(identity)
                entries = NativeResourceSet(backend, guard=self.assert_current).qualify(self.record)
                entry = entries[(kind, identity)]
                if entry is None:
                    continue
            if kind == "container" and entry["State"]["Running"]:
                raise StorageHostUnqualified("Native container remains running after stop")
            if kind == "network" and entry["Containers"]:
                raise StorageHostUnqualified("Native network retains an actual member")
            self.assert_current()
            result = backend._run(["docker", *(["network"] if kind == "network" else []), "rm", identity], capture_output=True, text=True, timeout=15)
            if result.returncode:
                raise StorageHostUnqualified("Native immutable member removal is unconfirmed")
        if not NativeResourceSet(backend, guard=self.assert_current).prove_absent(self.record):
            raise StorageHostUnqualified("Original native resource set remains present")
        self.assert_current()
        try:
            self.bridge.evidence.finish_teardown(self.record)
            self.bridge.teardowns.remove(self.record)
            self.info.native_completion = _Completion(self.record, self.bridge.evidence)
            self.bridge.qualify(teardown=True)
        except BaseException:
            self.bridge._check_failure_evidence(teardown=True)
            raise
        self.assert_current()
        return DestroyOutcome.ABSENT


@contextmanager
def destructive_scope(provider, info):
    from .local_backend import LocalContainerBackend
    from .native_creation import NativeCreateBridge

    if not provider._managed_pool_required() or not isinstance(provider._backend, LocalContainerBackend):
        yield None
        return
    if info.mountless_provider is provider:
        captured = getattr(info, "_native_journal_bridge", None)
        bridge = getattr(provider, "_native_creation", None)
        if captured is not None and captured is not bridge:
            raise StorageHostUnqualified("Native mountless cleanup lost its original journal binding")
        if captured is None and not (isinstance(bridge, NativeCreateBridge) and bridge._ever_installed):
            yield None
            return
    proof = current_native_lease(provider, info.sandbox_id)
    if proof is None or info.owner is None or not info.container_id or not info.container_name:
        raise StorageHostUnqualified("Managed native cleanup requires its live canonical holder and lease proofs")
    capability = _Capability(provider, info, proof)
    token = _CLEANUP.set(capability)
    try:
        capability.assert_current()
        yield capability
    finally:
        capability.active = False
        try:
            capability.owned.close()
        finally:
            _CLEANUP.reset(token)


def current_cleanup(backend, info):
    capability = _CLEANUP.get()
    if isinstance(capability, _Capability) and capability.active and capability.provider._backend is backend and capability.info is info:
        capability.assert_current()
        return capability
    return None


def permits_teardown(bridge, info):
    capability = _CLEANUP.get()
    return isinstance(capability, _Capability) and capability.bridge is bridge and capability.permits(info)
