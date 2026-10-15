"""Private actorless original resource accounting; no IO or effect authority."""

from dataclasses import dataclass, field, replace

from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.native_create_evidence import MAX_RECORDS
from deerflow.spaces.native_resident_evidence import NativeResidentRecord


@dataclass(eq=False)
class _RestoreToken:
    touched: dict[str, NativeResidentRecord] = field(default_factory=dict)
    overflowed: bool = False


class NativeResidentIndex:
    """All transitions share the provider lock; original uncertainty stays charged."""

    def __init__(self, *, daemon_id, lock):
        self.daemon_id, self.lock = daemon_id, lock
        self._items = {}
        self._restore = None
        self._installed = False

    @property
    def ready(self):
        return self._installed

    @staticmethod
    def _validate(record):
        if not isinstance(record, NativeResidentRecord) or NativeResidentRecord.parse(record.document(), record.id) != record:
            raise StorageHostUnqualified("Resident accounting lacks exact actorless original facts")

    @staticmethod
    def _merge(current, incoming, *, allow_stale=False):
        if replace(current, phase=incoming.phase) != incoming:
            raise StorageHostUnqualified("Resident accounting changed its immutable original graph")
        if current.phase == "retiring" and incoming.phase == "prepared":
            if not allow_stale:
                raise StorageHostUnqualified("Resident accounting cannot regress retirement")
            return current
        return incoming

    @staticmethod
    def _members(record):
        return {record.container_id, *(member.identity for member in record.resources)}, {("container", record.name), *((member.kind, member.name) for member in record.resources)}

    @classmethod
    def _insert(cls, items, record, *, allow_stale=False):
        current = items.get(record.id)
        if current is not None:
            record = cls._merge(current, record, allow_stale=allow_stale)
        identities, names = cls._members(record)
        for other in items.values():
            if other.id == record.id:
                continue
            other_ids, other_names = cls._members(other)
            if other.attachment_id == record.attachment_id or identities & other_ids or names & other_names:
                raise StorageHostUnqualified("Resident accounting has overlapping original identities")
        if current is None and len(items) >= MAX_RECORDS:
            raise StorageHostUnqualified("Resident accounting exceeds its bounded capacity")
        items[record.id] = record

    def _touch(self, record):
        if self._restore is not None:
            touched = self._restore.touched
            if record.id in touched or len(touched) < MAX_RECORDS * 2:
                touched[record.id] = record
            else:
                self._restore.overflowed = True
                self._installed = False

    def begin_restore(self):
        with self.lock:
            if self._restore is not None:
                raise StorageHostUnqualified("Resident accounting restoration is already owned")
            self._restore = _RestoreToken()
            return self._restore

    def finish_restore(self, token, records):
        with self.lock:
            try:
                if not isinstance(token, _RestoreToken) or token is not self._restore or token.overflowed:
                    raise StorageHostUnqualified("Resident accounting exceeded its restoration window")
                items = dict(self._items)
                for record in records:
                    self._validate(record)
                    expected = items.get(record.id) or token.touched.get(record.id)
                    if expected is not None:
                        self._merge(expected, record, allow_stale=True)
                    if record.id not in token.touched:
                        self._insert(items, record, allow_stale=True)
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
                raise StorageHostUnqualified("Foreign resident resources cannot be published")
            self._insert(self._items, record)
            self._touch(record)

    def remove(self, record):
        """Trusted caller completed full original absence and journal fsync first."""
        with self.lock:
            if not isinstance(record, NativeResidentRecord) or record.daemon_id != self.daemon_id or record.phase != "retiring" or self._items.get(record.id) != record:
                raise StorageHostUnqualified("Resident completion lacks its exact retained retiring graph")
            self._items.pop(record.id)
            self._touch(record)

    def _remove_restored(self, record, completion):
        """Only the owning Gateway's durable absence completion may admit a foreign original."""
        from deerflow.spaces.resident_restoration import _CompletionPermit

        with self.lock:
            if type(completion) is not _CompletionPermit or not isinstance(record, NativeResidentRecord) or record.phase != "retiring" or self._items.get(record.id) != record:
                raise StorageHostUnqualified("Restored completion lacks its exact retained original")
            completion.require_index(self, record)
            self._items.pop(record.id)
            self._touch(record)

    def records(self):
        with self.lock:
            return tuple(self._items.values())

    def extra_slots_locked(self, *, known_claims):
        """Caller holds provider lock over both journal and exact SQL correlations."""
        return sum(record.correlation not in known_claims for record in self._items.values())

    def for_attachment(self, attachment_id):
        with self.lock:
            values = [record for record in self._items.values() if record.attachment_id == attachment_id]
            if len(values) > 1:
                raise StorageHostUnqualified("Resident original attachment correlation is ambiguous")
            return values[0] if values else None
