"""Private memory accounting for durable teardown correlation; no IO or authority."""

from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import wraps
from threading import Event

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_create_evidence import MAX_RECORDS
from deerflow.spaces.native_teardown_evidence import NativeTeardownRecord


@dataclass(eq=False)
class _LeaseProof:
    provider: object
    sandbox_id: str
    active: bool = True
    lost: Event = field(default_factory=Event)


_NATIVE_LEASE = ContextVar("native_teardown_lease", default=None)


def begin_native_lease(provider, sandbox_id):
    proof = _LeaseProof(provider, sandbox_id)
    return proof, _NATIVE_LEASE.set(proof)


def end_native_lease(proof, context):
    proof.active = False
    _NATIVE_LEASE.reset(context)


def current_native_lease(provider, sandbox_id):
    from deerflow.sandbox.lease import current_cleanup_reservation

    proof = _NATIVE_LEASE.get()
    if not isinstance(proof, _LeaseProof) or proof.provider is not provider or proof.sandbox_id != sandbox_id or not proof.active or proof.lost.is_set() or current_cleanup_reservation(provider, sandbox_id) is None:
        return None
    with provider._lock:
        if sandbox_id not in provider._local_teardown:
            return None
    return proof


def native_cleanup(*, refused=None):
    """Managed cleanup owns the holder fence before closing or untracking a client."""

    def decorate(function):
        @wraps(function)
        def call(provider, candidate, *arguments, **keywords):
            from deerflow.sandbox.lease import cleanup_unheld_sandbox, current_cleanup_reservation

            from .local_backend import LocalContainerBackend

            sandbox_id = candidate if isinstance(candidate, str) else candidate.sandbox_id
            if not provider._managed_pool_required() or not isinstance(provider._backend, LocalContainerBackend) or current_cleanup_reservation(provider, sandbox_id) is not None:
                return function(provider, candidate, *arguments, **keywords)
            result = []
            if cleanup_unheld_sandbox(provider, sandbox_id, lambda: result.append(function(provider, candidate, *arguments, **keywords)), defer_release=False):
                return result[0]
            return refused

        return call

    return decorate


@dataclass(eq=False)
class _RestoreToken:
    touched: dict[str, NativeTeardownRecord] = field(default_factory=dict)
    overflowed: bool = False


class NativeTeardownIndex:
    """Shares the provider metadata lock; filesystem operations belong elsewhere."""

    def __init__(self, *, daemon_id, lock):
        self.daemon_id, self.lock = daemon_id, lock
        self._items = {}
        self._restore = None
        self._installed = False

    @property
    def ready(self):
        return self._installed

    def _touch(self, record):
        if self._restore is not None:
            touched = self._restore.touched
            if record.id in touched or len(touched) < MAX_RECORDS * 2:
                touched[record.id] = record
            else:
                self._restore.overflowed = True
                self._installed = False

    @staticmethod
    def _validate(record):
        if not isinstance(record, NativeTeardownRecord) or NativeTeardownRecord.parse(record.document(), record.id) != record:
            raise StorageHostUnqualified("Native teardown accounting lacks exact validated correlation")

    @staticmethod
    def _members(record):
        return {record.container_id, *(item.identity for item in record.resources)}, {("container", record.name), *((item.kind, item.name) for item in record.resources)}

    @staticmethod
    def _insert(items, record):
        current = items.get(record.id)
        if current is not None and current != record:
            raise StorageHostUnqualified("Native teardown accounting changed immutable correlation")
        identities, names = NativeTeardownIndex._members(record)
        for other in items.values():
            if other.id == record.id:
                continue
            other_identities, other_names = NativeTeardownIndex._members(other)
            if identities & other_identities or names & other_names:
                raise StorageHostUnqualified("Native teardown accounting has overlapping immutable resource sets")
        if current is None and len(items) >= MAX_RECORDS:
            raise StorageHostUnqualified("Native teardown accounting exceeds its bounded capacity")
        items[record.id] = record

    def begin_restore(self):
        with self.lock:
            if self._restore is not None:
                raise StorageHostUnqualified("Native teardown restoration is already owned")
            self._restore = _RestoreToken()
            return self._restore

    def finish_restore(self, token, records):
        with self.lock:
            try:
                if not isinstance(token, _RestoreToken) or token is not self._restore or token.overflowed:
                    raise StorageHostUnqualified("Native teardown snapshot exceeded its restoration window")
                items = dict(self._items)
                for record in records:
                    self._validate(record)
                    expected = items.get(record.id) or token.touched.get(record.id)
                    if expected is not None and expected != record:
                        raise StorageHostUnqualified("Native teardown snapshot changed immutable correlation")
                    if record.id not in token.touched:
                        self._insert(items, record)
                self._items = items
                self._restore = None
                self._installed = True
            except BaseException:
                self._installed = False
                raise

    def abort_restore(self, token):
        with self.lock:
            if self._restore is token:
                self._restore = None
                self._installed = False

    def add(self, record):
        with self.lock:
            self._validate(record)
            if record.daemon_id != self.daemon_id:
                raise StorageHostUnqualified("Foreign daemon teardown cannot be published by this runtime")
            self._insert(self._items, record)
            self._touch(record)

    def remove(self, record):
        """Trusted caller already completed physical absence and durable journal removal."""
        with self.lock:
            if not isinstance(record, NativeTeardownRecord) or record.daemon_id != self.daemon_id or self._items.get(record.id) != record:
                raise StorageHostUnqualified("Native teardown completion lost its exact retained record")
            self._items.pop(record.id)
            self._touch(record)

    def records(self):
        with self.lock:
            return tuple(self._items.values())

    def blocks(self, *, owner_id=None, name=None, container_id=None, slots=()):
        with self.lock:
            return self.blocks_locked(owner_id=owner_id, name=name, container_id=container_id, slots=slots)

    def blocks_locked(self, *, owner_id=None, name=None, container_id=None, slots=()):
        return any(record.owner_id == owner_id or record.name == name or record.container_id == container_id or (record.target_slot is not None and record.target_slot in slots) for record in self._items.values())

    def extra_slots_locked(self, *, known_container_ids):
        """Caller holds provider lock and counts active/warm in the same section."""
        return sum(record.container_id not in known_container_ids for record in self._items.values())
