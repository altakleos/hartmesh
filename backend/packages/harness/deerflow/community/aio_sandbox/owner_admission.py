"""Private pure owner/slot admission; no business policy or IO.

Every borrow is a distinct live reference. References share the growing slot
set so an actual future can outlive caller/worker contexts without an unguarded
publication window. Copied expired ContextVar references grant no admission.
"""

import re
from contextvars import ContextVar
from dataclasses import dataclass, field
from uuid import uuid4

from deerflow.spaces.docker import StorageHostUnqualified

OWNER_ADMISSION = ContextVar("native_owner_admission", default=None)


class NativeRetirementBusy(StorageHostUnqualified):
    """Reservation refused before physical work because a live reference remains."""


@dataclass(eq=False)
class _Group:
    slots: set[str] = field(default_factory=set)


@dataclass(eq=False, frozen=True)
class OwnerAdmission:
    owner_id: str
    _group: _Group = field(repr=False)


@dataclass(eq=False, frozen=True)
class OwnerRetirement:
    owner_id: str
    slot_id: str


@dataclass(eq=False, frozen=True)
class ResidentRetirementBatch:
    owners: frozenset[str]
    writable_slots: frozenset[str]
    reference: OwnerAdmission = field(repr=False)


class OwnerAdmissions:
    """All transitions share the provider metadata lock; none performs IO."""

    def __init__(self, *, lock):
        self.lock = lock
        self._active = {}
        self._owners = {}
        self._slots = {}

    @staticmethod
    def _owner(owner_id):
        if not isinstance(owner_id, str) or not owner_id.strip() or len(owner_id) > 512:
            raise StorageHostUnqualified("Native admission requires an opaque owner identity")

    @staticmethod
    def _slot(slot_id):
        if not isinstance(slot_id, str) or re.fullmatch(r"[0-9a-f]{32}", slot_id) is None:
            raise StorageHostUnqualified("Native admission requires an exact physical slot")

    def allowed_locked(self, owner_id, slots, *, retirement=None):
        """Caller already holds the metadata lock; pure final publication gate."""
        if retirement is not None and (not self.current_locked(retirement) or retirement.owner_id != owner_id):
            return False
        owner = self._owners.get(owner_id)
        if owner is not None and owner is not retirement:
            return False
        return all(self._slots.get(slot) is None or self._slots[slot] is retirement for slot in slots)

    def enter(self, owner_id, *, borrow=None):
        self._owner(owner_id)
        with self.lock:
            group = _Group()
            if borrow is not None:
                if not isinstance(borrow, OwnerAdmission) or borrow not in self._active or borrow.owner_id != owner_id:
                    raise StorageHostUnqualified("Native admission cannot borrow an expired or foreign reference")
                group = self._active[borrow]
            if not self.allowed_locked(owner_id, group.slots):
                raise StorageHostUnqualified("Native owner or slot is reserved for retirement")
            token = OwnerAdmission(owner_id, group)
            self._active[token] = group
            return token

    def register_slots(self, token, slots):
        slots = tuple(slots)
        for slot in slots:
            self._slot(slot)
        with self.lock:
            if not isinstance(token, OwnerAdmission) or token not in self._active:
                raise StorageHostUnqualified("Native slot registration requires its live admission reference")
            if not self.allowed_locked(token.owner_id, slots):
                raise StorageHostUnqualified("Native requested slot is reserved for retirement")
            self._active[token].slots.update(slots)

    def end(self, token):
        with self.lock:
            if isinstance(token, OwnerAdmission):
                self._active.pop(token, None)

    def admission_current_locked(self, token):
        """Caller holds metadata lock; copied stale references prove nothing."""
        return isinstance(token, OwnerAdmission) and token in self._active

    def has_active_locked(self):
        """Caller holds metadata lock; producer drainage is a separate boundary."""
        return bool(self._active)

    def reserve(self, owner_id, slot_id):
        self._owner(owner_id)
        self._slot(slot_id)
        with self.lock:
            if not self.allowed_locked(owner_id, (slot_id,)) or any(token.owner_id == owner_id or slot_id in group.slots for token, group in self._active.items()):
                raise NativeRetirementBusy("Native retirement is busy with admitted owner or slot work")
            token = OwnerRetirement(owner_id, slot_id)
            self._owners[owner_id] = self._slots[slot_id] = token
            return token

    def current(self, token):
        with self.lock:
            return self.current_locked(token)

    def current_locked(self, token):
        return isinstance(token, OwnerRetirement) and self._owners.get(token.owner_id) is token and self._slots.get(token.slot_id) is token

    def end_retirement(self, token):
        with self.lock:
            if self.current_locked(token):
                self._owners.pop(token.owner_id)
                self._slots.pop(token.slot_id)

    def reserve_resident_batch(self, scopes):
        """Atomically exclude original owners/writers and retain read-only slots."""
        if not isinstance(scopes, (tuple, list)) or not 1 <= len(scopes) <= 256:
            raise StorageHostUnqualified("Resident containment requires a bounded explicit batch")
        owners, writable, readonly = set(), set(), set()
        for scope in scopes:
            if not isinstance(scope, (tuple, list)) or len(scope) != 3:
                raise StorageHostUnqualified("Resident containment has malformed scope")
            owner, writes, reads = scope
            self._owner(owner)
            if owner in owners or any(not isinstance(slots, (tuple, list, set, frozenset)) or len(slots) > 128 for slots in (writes, reads)):
                raise StorageHostUnqualified("Resident containment has duplicate owners or unbounded slots")
            owners.add(owner)
            for slot in (*writes, *reads):
                self._slot(slot)
            writable.update(writes)
            readonly.update(reads)
        slots = writable | readonly
        if not slots:
            raise StorageHostUnqualified("Resident containment requires exact physical slots")
        with self.lock:
            if any(not self.allowed_locked(owner, slots) for owner in owners) or any(token.owner_id in owners or writable.intersection(group.slots) for token, group in self._active.items()):
                raise NativeRetirementBusy("Resident containment is busy with admitted owner or writer work")
            reference = OwnerAdmission("resident-containment:" + uuid4().hex, _Group(slots))
            token = ResidentRetirementBatch(frozenset(owners), frozenset(writable), reference)
            for owner in owners:
                self._owners[owner] = token
            for slot in writable:
                self._slots[slot] = token
            self._active[reference] = reference._group
            return token

    def current_resident_batch_locked(self, token):
        return isinstance(token, ResidentRetirementBatch) and token.reference in self._active and all(self._owners.get(owner) is token for owner in token.owners) and all(self._slots.get(slot) is token for slot in token.writable_slots)

    def current_resident_batch(self, token):
        with self.lock:
            return self.current_resident_batch_locked(token)

    def end_resident_batch(self, token):
        with self.lock:
            if self.current_resident_batch_locked(token):
                for owner in token.owners:
                    self._owners.pop(owner)
                for slot in token.writable_slots:
                    self._slots.pop(slot)
                self._active.pop(token.reference)
