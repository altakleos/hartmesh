"""Private durable uncertainty for host-submitted native container creation.

A returned, verified create acknowledgment is request-completion evidence,
never container absence or resource authorization. Pending facts survive restart.
"""

import json
import os
import re
import stat
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from threading import RLock
from uuid import uuid4

from deerflow.files.store import StoreError, open_directory_source
from deerflow.spaces.docker import StorageHostUnqualified
from deerflow.spaces.filesystem import ConfinedFilesystem
from deerflow.spaces.native_teardown_evidence import NativeTeardownRecord

DIRECTORY = ".native-creates"
ATTEMPT_LABEL = "hartmesh.native.create-attempt"
MAX_RECORD_BYTES = 65536
MAX_RECORDS = 256
MAX_TOTAL_BYTES = MAX_RECORD_BYTES * MAX_RECORDS
_FIELDS = frozenset({"schema_version", "id", "daemon_id", "owner_id", "thread_id", "name", "slots", "phase", "container_id"})
_HEX32 = re.compile(r"[0-9a-f]{32}\Z")
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,511}\Z")


def _identity(metadata):
    return metadata.st_dev, metadata.st_ino


def _text(value, limit=256):
    return isinstance(value, str) and 0 < len(value) <= limit and not any(ord(char) < 32 for char in value)


def _unique_object(pairs):
    document = {}
    for key, value in pairs:
        if key in document:
            raise StorageHostUnqualified("Native create evidence has duplicate JSON fields")
        document[key] = value
    return document


@dataclass(frozen=True)
class NativeCreateAttempt:
    id: str
    daemon_id: str
    owner_id: str
    thread_id: str
    name: str
    slots: tuple[str, ...]
    phase: str = "pending"
    container_id: str | None = None


@dataclass(frozen=True)
class NativeContainerIdentity:
    attempt_id: str
    daemon_id: str
    owner_id: str
    thread_id: str
    name: str
    slots: tuple[str, ...]
    container_id: str


@dataclass(frozen=True)
class NativeEvidenceSnapshot:
    creates: tuple[NativeCreateAttempt, ...]
    teardowns: tuple[NativeTeardownRecord, ...]

    def pending_creates(self, daemon_id):
        return tuple(record for record in self.creates if record.phase == "pending" or record.daemon_id != daemon_id)


@dataclass(frozen=True)
class _TeardownRemoval:
    record: NativeTeardownRecord
    metadata: object
    descriptor: int


class NativeCreateEvidence:
    """One lifecycle retains its exact qualified directory, never repairs it."""

    def __init__(self, pool: Path, *, daemon_id: str, _expected_directory=None):
        if not _text(daemon_id, 128):
            raise StorageHostUnqualified("Native creation requires a qualified daemon identity")
        self.pool, self.daemon_id = Path(pool), daemon_id
        self._lock = RLock()
        self._stages = {}
        self._teardown_removals = {}
        self._has_teardown = False
        self._pool_fd = self._directory_fd = None
        self._closed = False
        try:
            self._pool_fd = open_directory_source(self.pool)
            self._directory_fd = os.open(DIRECTORY, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self._pool_fd)
            if _expected_directory is not None and _identity(os.fstat(self._directory_fd)) != _expected_directory:
                raise StorageHostUnqualified("Native create evidence changed before namespace qualification")
            self._assert_current()
            records, _ = self._scan()
            self._prune_acknowledged(records)
        except BaseException as exc:
            self.close()
            if isinstance(exc, (OSError, StoreError)):
                raise StorageHostUnqualified("Native create evidence is unavailable") from exc
            raise

    @property
    def ready(self):
        """Memory-only lifetime fact; fresh physical proof precedes effects."""
        return not self._closed and self._directory_fd is not None

    @property
    def creation_ready(self):
        """Memory-only publication gate; a fresh mixed snapshot precedes effects."""
        return self.ready and not self._has_teardown

    def _assert_current(self):
        if not self.ready or self._pool_fd is None:
            raise StorageHostUnqualified("Native create evidence is closed")
        current = open_directory_source(self.pool)
        try:
            pool = os.fstat(current)
            if pool.st_uid not in (0, os.getuid()) or pool.st_mode & 0o022 or _identity(pool) != _identity(os.fstat(self._pool_fd)):
                raise StorageHostUnqualified("Native create evidence pool changed")
        finally:
            os.close(current)
        opened = os.fstat(self._directory_fd)
        named = os.stat(DIRECTORY, dir_fd=self._pool_fd, follow_symlinks=False)
        if not stat.S_ISDIR(opened.st_mode) or not stat.S_ISDIR(named.st_mode) or _identity(named) != _identity(opened) or opened.st_dev != pool.st_dev:
            raise StorageHostUnqualified("Native create evidence directory changed")
        for metadata in (opened, named):
            if (metadata.st_uid, metadata.st_gid, stat.S_IMODE(metadata.st_mode)) != (os.getuid(), os.getgid(), 0o700):
                raise StorageHostUnqualified("Native create evidence directory is not private host storage")

    @staticmethod
    def _validate(document, identifier):
        if isinstance(document, dict) and type(document.get("schema_version")) is int and document["schema_version"] == 2:
            return NativeTeardownRecord.parse(document, identifier)
        if not isinstance(document, dict) or set(document) != _FIELDS or type(document["schema_version"]) is not int or document["schema_version"] != 1:
            raise StorageHostUnqualified("Native create evidence schema is unknown")
        slots = document["slots"]
        if (
            document["id"] != identifier
            or not _HEX32.fullmatch(identifier)
            or not _text(document["daemon_id"], 128)
            or not _text(document["owner_id"])
            or not _text(document["thread_id"])
            or not isinstance(document["name"], str)
            or not _NAME.fullmatch(document["name"])
            or not isinstance(slots, list)
            or not 1 <= len(slots) <= 128
            or any(not isinstance(slot, str) or not _HEX32.fullmatch(slot) for slot in slots)
            or slots != sorted(set(slots))
            or document["phase"] not in ("pending", "acknowledged")
            or (document["phase"] == "pending" and document["container_id"] is not None)
            or (document["phase"] == "acknowledged" and (not isinstance(document["container_id"], str) or not _HEX64.fullmatch(document["container_id"])))
        ):
            raise StorageHostUnqualified("Native create evidence facts are malformed")
        return NativeCreateAttempt(**{key: tuple(value) if key == "slots" else value for key, value in document.items() if key != "schema_version"})

    def _read(self, name):
        if not name.endswith(".json") or not _HEX32.fullmatch(name[:-5]):
            raise StorageHostUnqualified("Unknown native create evidence must be inspected")
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=self._directory_fd)
        try:
            metadata = os.fstat(descriptor)
            named = os.stat(name, dir_fd=self._directory_fd, follow_symlinks=False)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or (metadata.st_uid, metadata.st_gid, stat.S_IMODE(metadata.st_mode)) != (os.getuid(), os.getgid(), 0o600)
                or not 0 < metadata.st_size <= MAX_RECORD_BYTES
                or _identity(named) != _identity(metadata)
            ):
                raise StorageHostUnqualified("Native create evidence record is not a bounded private file")
            raw = os.read(descriptor, MAX_RECORD_BYTES + 1)
            if len(raw) != metadata.st_size:
                raise StorageHostUnqualified("Native create evidence record changed during inspection")
            document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
            record = self._validate(document, name[:-5])
            return record, metadata
        finally:
            os.close(descriptor)

    def _scan(self):
        records, total = [], 0
        try:
            self._assert_current()
            with os.scandir(self._directory_fd) as entries:
                for entry in entries:
                    if len(records) >= MAX_RECORDS:
                        raise StorageHostUnqualified("Native create evidence record capacity is exhausted")
                    record, metadata = self._read(entry.name)
                    total += metadata.st_size
                    if total > MAX_TOTAL_BYTES:
                        raise StorageHostUnqualified("Native create evidence byte capacity is exhausted")
                    records.append((record, metadata))
            self._assert_current()
            by_id = {record.id: (record, metadata) for record, metadata in records}
            for identifier, removal in self._teardown_removals.items():
                if identifier in by_id:
                    record, metadata = by_id[identifier]
                    if record != removal.record or _identity(metadata) != _identity(removal.metadata):
                        raise StorageHostUnqualified("Unconfirmed native teardown removal identity changed")
                else:
                    records.append((removal.record, None))
                    total += len(self._encode_teardown(removal.record))
            if len(records) > MAX_RECORDS or total > MAX_TOTAL_BYTES:
                raise StorageHostUnqualified("Native lifecycle evidence capacity is exhausted")
            teardowns = [record for record, _ in records if isinstance(record, NativeTeardownRecord)]
            if len({record.container_id for record in teardowns}) != len(teardowns):
                raise StorageHostUnqualified("Native teardown primary correlation is ambiguous")
            self._has_teardown = bool(teardowns)
        except (OSError, StoreError, ValueError, UnicodeError, RecursionError) as exc:
            raise StorageHostUnqualified("Native create evidence cannot be inspected") from exc
        return records, total

    def snapshot(self):
        """One bounded mixed view, never a cleanup or actor capability."""
        with self._lock:
            records, _ = self._scan()
            return NativeEvidenceSnapshot(tuple(record for record, _ in records if isinstance(record, NativeCreateAttempt)), tuple(record for record, _ in records if isinstance(record, NativeTeardownRecord)))

    def pending(self, *, slots=None, owner_id=None):
        with self._lock:
            records, _ = self._scan()
            return tuple(
                record
                for record, _metadata in records
                if isinstance(record, NativeCreateAttempt)
                and (record.phase == "pending" or record.daemon_id != self.daemon_id)
                and (slots is None or set(slots).intersection(record.slots))
                and (owner_id is None or owner_id == record.owner_id)
            )

    def require_admissible(self, *, owner_id, name, slots=()):
        with self._lock:
            snapshot = self.snapshot()
            if any(record.owner_id == owner_id or record.name == name for record in snapshot.pending_creates(self.daemon_id)):
                raise StorageHostUnqualified("A previous native creation is unresolved for this owner or name")
            if any(record.owner_id == owner_id or record.name == name or (record.target_slot is not None and record.target_slot in slots) for record in snapshot.teardowns):
                raise StorageHostUnqualified("A previous native teardown is unresolved for this owner, name or target slot")

    @staticmethod
    def _encode(record):
        if not isinstance(record, NativeCreateAttempt):
            raise StorageHostUnqualified("Create encoding requires an exact create record")
        raw = json.dumps({"schema_version": 1, **asdict(record)}, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        if len(raw) > MAX_RECORD_BYTES:
            raise StorageHostUnqualified("Native create evidence record exceeds its bound")
        return raw

    def _prune_acknowledged(self, records):
        # Validate the whole namespace before removing anything. A valid
        # acknowledgment for a different daemon still fences its slot.
        for record, metadata in records:
            if not isinstance(record, NativeCreateAttempt) or record.phase != "acknowledged" or record.daemon_id != self.daemon_id:
                continue
            name = record.id + ".json"
            descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=self._directory_fd)
            try:
                saved, current = self._read(name)
                if saved != record or _identity(current) != _identity(metadata) or _identity(os.fstat(descriptor)) != _identity(metadata):
                    raise StorageHostUnqualified("Native acknowledgment changed during restart inspection")
                os.fsync(descriptor)
                os.fsync(self._directory_fd)
                self._assert_current()
                if _identity(os.stat(name, dir_fd=self._directory_fd, follow_symlinks=False)) != _identity(metadata):
                    raise StorageHostUnqualified("Native acknowledgment inode changed during restart cleanup")
                os.unlink(name, dir_fd=self._directory_fd)
                os.fsync(self._directory_fd)
            finally:
                os.close(descriptor)

    def _write_stage(self, raw):
        name = ".write-" + uuid4().hex
        descriptor = os.open(name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self._directory_fd)
        try:
            os.fchmod(descriptor, 0o600)
            remaining = memoryview(raw)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise OSError("Native create evidence write did not progress")
                remaining = remaining[written:]
            os.fsync(descriptor)
        except BaseException:
            os.close(descriptor)
            raise
        self._stages[name] = descriptor
        return name

    def _assert_stage(self, name, raw):
        descriptor = self._stages.get(name)
        if descriptor is None:
            raise StorageHostUnqualified("Native evidence stage has no retained descriptor")
        opened = os.fstat(descriptor)
        named = os.stat(name, dir_fd=self._directory_fd, follow_symlinks=False)
        if not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1 or (opened.st_uid, opened.st_gid, stat.S_IMODE(opened.st_mode)) != (os.getuid(), os.getgid(), 0o600) or opened.st_size != len(raw) or _identity(named) != _identity(opened):
            raise StorageHostUnqualified("Native evidence stage changed before publication")
        os.lseek(descriptor, 0, os.SEEK_SET)
        if os.read(descriptor, MAX_RECORD_BYTES + 1) != raw:
            raise StorageHostUnqualified("Native evidence stage bytes changed before publication")

    def _close_stage(self, name):
        descriptor = self._stages.pop(name, None)
        if descriptor is not None:
            os.close(descriptor)

    def begin(self, *, owner_id, thread_id, name, slots):
        with self._lock:
            record = NativeCreateAttempt(uuid4().hex, self.daemon_id, owner_id, thread_id, name, tuple(sorted(set(slots))))
            raw = self._encode(record)
            self._validate(json.loads(raw), record.id)
            self.require_admissible(owner_id=owner_id, name=name, slots=record.slots)
            records, total = self._scan()
            if len(records) >= MAX_RECORDS or total + len(raw) > MAX_TOTAL_BYTES:
                raise StorageHostUnqualified("Native create evidence is full; submission is refused")
            stage = self._write_stage(raw)
            try:
                self._assert_current()
                self._assert_stage(stage, raw)
                ConfinedFilesystem._rename_no_replace(self._directory_fd, stage, self._directory_fd, record.id + ".json")
                os.fsync(self._directory_fd)
                self._assert_current()
                published, metadata = self._read(record.id + ".json")
                if published != record or _identity(metadata) != _identity(os.fstat(self._stages[stage])):
                    raise StorageHostUnqualified("Native pending evidence changed during publication")
                self._assert_current()
            finally:
                self._close_stage(stage)
            return record

    def acknowledge(self, attempt, identity):
        with self._lock:
            self._assert_current()
            if not isinstance(attempt, NativeCreateAttempt) or not isinstance(identity, NativeContainerIdentity):
                raise StorageHostUnqualified("Native creation acknowledgment is not a verified host identity")
            if attempt.daemon_id != self.daemon_id:
                raise StorageHostUnqualified("A different daemon cannot acknowledge retained native creation")
            actual = (identity.attempt_id, identity.daemon_id, identity.owner_id, identity.thread_id, identity.name, tuple(identity.slots))
            expected = (attempt.id, self.daemon_id, attempt.owner_id, attempt.thread_id, attempt.name, attempt.slots)
            if actual != expected or not isinstance(identity.container_id, str) or not _HEX64.fullmatch(identity.container_id):
                raise StorageHostUnqualified("Native creation acknowledgment identity does not match its attempt")
            current, metadata = self._read(attempt.id + ".json")
            if current != attempt:
                raise StorageHostUnqualified("Native create attempt changed before acknowledgment")
            acknowledged = replace(attempt, phase="acknowledged", container_id=identity.container_id)
            raw = self._encode(acknowledged)
            stage = self._write_stage(raw)
            try:
                self._assert_current()
                self._assert_stage(stage, raw)
                if _identity(os.stat(attempt.id + ".json", dir_fd=self._directory_fd, follow_symlinks=False)) != _identity(metadata):
                    raise StorageHostUnqualified("Native create attempt inode changed before acknowledgment")
                os.replace(stage, attempt.id + ".json", src_dir_fd=self._directory_fd, dst_dir_fd=self._directory_fd)
                os.fsync(self._directory_fd)
            finally:
                self._close_stage(stage)
            self._assert_current()
            saved, saved_metadata = self._read(attempt.id + ".json")
            if saved != acknowledged:
                raise StorageHostUnqualified("Native create acknowledgment changed before cleanup")
            if _identity(os.stat(attempt.id + ".json", dir_fd=self._directory_fd, follow_symlinks=False)) != _identity(saved_metadata):
                raise StorageHostUnqualified("Native create acknowledgment inode changed before cleanup")
            os.unlink(attempt.id + ".json", dir_fd=self._directory_fd)
            os.fsync(self._directory_fd)

    @staticmethod
    def _encode_teardown(record):
        if not isinstance(record, NativeTeardownRecord):
            raise StorageHostUnqualified("Teardown encoding requires an exact teardown record")
        try:
            raw = json.dumps(record.document(), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
            if len(raw) > MAX_RECORD_BYTES or NativeTeardownRecord.parse(json.loads(raw), record.id) != record:
                raise StorageHostUnqualified("Native teardown record exceeds its bound or changes shape")
            return raw
        except (TypeError, ValueError, RecursionError) as exc:
            raise StorageHostUnqualified("Native teardown record is malformed") from exc

    def begin_teardown(self, record):
        """Persist correlation before effects; the record grants no authority."""
        with self._lock:
            raw = self._encode_teardown(record)
            if record.daemon_id != self.daemon_id:
                raise StorageHostUnqualified("Cannot publish teardown evidence for another daemon")
            records, total = self._scan()
            for previous, metadata in records:
                if isinstance(previous, NativeTeardownRecord) and previous.container_id == record.container_id:
                    if previous == replace(record, id=previous.id):
                        if metadata is None or previous.id in self._teardown_removals:
                            raise StorageHostUnqualified("Native teardown removal durability remains pending")
                        return self._confirm_teardown_publication(previous, metadata)
                    raise StorageHostUnqualified("Native teardown primary already has different immutable correlation")
            if len(records) >= MAX_RECORDS or total + len(raw) > MAX_TOTAL_BYTES:
                raise StorageHostUnqualified("Native lifecycle evidence is full; teardown is refused")
            self._has_teardown = True
            stage = self._write_stage(raw)
            try:
                self._assert_current()
                self._assert_stage(stage, raw)
                ConfinedFilesystem._rename_no_replace(self._directory_fd, stage, self._directory_fd, record.id + ".json")
                os.fsync(self._directory_fd)
                self._assert_current()
                saved, metadata = self._read(record.id + ".json")
                if saved != record or _identity(metadata) != _identity(os.fstat(self._stages[stage])):
                    raise StorageHostUnqualified("Native teardown publication identity changed")
                self._assert_current()
            finally:
                self._close_stage(stage)
            return record

    def _confirm_teardown_publication(self, record, metadata):
        """Retry an uncertain publication without treating readability as fsync."""
        name = record.id + ".json"
        descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=self._directory_fd)
        try:
            saved, current = self._read(name)
            if saved != record or _identity(current) != _identity(metadata) or _identity(os.fstat(descriptor)) != _identity(metadata):
                raise StorageHostUnqualified("Native teardown publication retry identity changed")
            os.fsync(descriptor)
            os.fsync(self._directory_fd)
            self._assert_current()
            saved, current = self._read(name)
            if saved != record or _identity(current) != _identity(metadata):
                raise StorageHostUnqualified("Native teardown publication changed after durability retry")
            return record
        finally:
            os.close(descriptor)

    def finish_teardown(self, record):
        """Trusted caller must first prove the complete captured set absent."""
        with self._lock:
            self._encode_teardown(record)
            if record.daemon_id != self.daemon_id:
                raise StorageHostUnqualified("Cannot remove teardown evidence for another daemon")
            records, _ = self._scan()
            if not any(saved == record for saved, _metadata in records):
                raise StorageHostUnqualified("Native teardown completion lacks its exact retained record")
            name = record.id + ".json"
            removal = self._teardown_removals.get(record.id)
            if removal is None:
                descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=self._directory_fd)
                try:
                    saved, metadata = self._read(name)
                    if saved != record or _identity(os.fstat(descriptor)) != _identity(metadata):
                        raise StorageHostUnqualified("Native teardown record changed before completion")
                    os.fsync(descriptor)
                    removal = _TeardownRemoval(record, metadata, descriptor)
                    self._teardown_removals[record.id] = removal
                except BaseException:
                    os.close(descriptor)
                    raise
            self._assert_current()
            try:
                saved, metadata = self._read(name)
            except FileNotFoundError:
                pass
            else:
                if saved != removal.record or _identity(metadata) != _identity(removal.metadata):
                    raise StorageHostUnqualified("Native teardown completion cannot remove a replacement")
                os.unlink(name, dir_fd=self._directory_fd)
            os.fsync(self._directory_fd)
            self._assert_current()
            try:
                os.stat(name, dir_fd=self._directory_fd, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise StorageHostUnqualified("Native teardown evidence removal is unconfirmed")
            self._teardown_removals.pop(record.id)
            os.close(removal.descriptor)
            self._scan()

    def close(self):
        with self._lock:
            self._closed = True
            for stage in tuple(self._stages):
                self._close_stage(stage)
            for removal in self._teardown_removals.values():
                os.close(removal.descriptor)
            self._teardown_removals.clear()
            for name in ("_directory_fd", "_pool_fd"):
                descriptor = getattr(self, name, None)
                if descriptor is not None:
                    setattr(self, name, None)
                    os.close(descriptor)
