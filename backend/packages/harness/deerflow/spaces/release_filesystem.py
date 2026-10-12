"""Private descriptor-owned retirement; SQL intent/checkpoint authorize retries.

No principal, business role or container policy belongs here. Callers qualify
the physical provider, own the resource parent and retain external containment.
"""

import ctypes
import errno
import fcntl
import hashlib
import json
import os
import stat

from deerflow.spaces.backings import BackingUnavailable, _mount_record
from deerflow.spaces.filesystem import _OpenHow
from deerflow.spaces.incarnations import BINDING_MARKER, validate_marker
from deerflow.spaces.workflows import _LOCK

PREFIX = ".space-release-"
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
_STATE = "state.json"
_TEMP = ".state.tmp"
_JOURNAL_LOCK = "lock"
_LIMIT = 65536


class ReleaseParentUnavailable(BackingUnavailable):
    """Physical storage is readable but cannot safely renew its roots."""


def _parent_capability(facts):
    if facts["uid"] == os.geteuid():
        permission = 0o300
    elif facts["gid"] in {*os.getgroups(), os.getegid()}:
        permission = 0o030
    else:
        permission = 0
    if not permission or facts["mode"] & permission != permission or facts["mode"] & 0o002:
        raise ReleaseParentUnavailable("Provider parent needs safe host write/execute permissions before root renewal")


def _facts(metadata):
    return {"device": metadata.st_dev, "inode": metadata.st_ino, "uid": metadata.st_uid, "gid": metadata.st_gid, "mode": stat.S_IMODE(metadata.st_mode)}


def _digest(request):
    return hashlib.sha256(json.dumps(request, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")).hexdigest()


def _child(parent, name, flags=_DIRECTORY):
    # Internal retirement also supports native filenames outside browser UTF-8.
    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    how = _OpenHow(flags | os.O_CLOEXEC, 0, 0x08 | 0x02 | 0x01 | 0x04)
    result = libc.syscall(437, parent, ctypes.c_char_p(os.fsencode(name)), ctypes.byref(how), ctypes.sizeof(how))
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return result


def _exchange(parent, name, journal):
    libc = ctypes.CDLL(None, use_errno=True)
    rename = libc.renameat2
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(parent, os.fsencode(name), journal, os.fsencode(name), 2) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _purge(root, *, preserve=frozenset()):
    """Stream a pinned tree; never open special nodes or cross child mounts."""
    stack = [(os.dup(root), None, None)]
    try:
        while stack:
            fd, iterator, name = stack[-1]
            if iterator is None:
                iterator = os.scandir(fd)
                stack[-1] = fd, iterator, name
            entry = next(iterator, None)
            if entry is None:
                os.fsync(fd)
                iterator.close()
                os.close(fd)
                stack.pop()
                if stack:
                    os.rmdir(name, dir_fd=stack[-1][0])
                continue
            if len(stack) == 1 and entry.name in preserve:
                continue
            node = _child(fd, entry.name, os.O_PATH | os.O_NOFOLLOW)
            try:
                metadata = os.fstat(node)
                directory = stat.S_ISDIR(metadata.st_mode)
            finally:
                os.close(node)
            if directory:
                if len(stack) >= 2048:
                    raise BackingUnavailable("Retired tree exceeds the confined traversal depth")
                stack.append((_child(fd, entry.name), None, entry.name))
            else:
                os.unlink(entry.name, dir_fd=fd)
    finally:
        for fd, iterator, _name in reversed(stack):
            try:
                if iterator is not None:
                    iterator.close()
            finally:
                os.close(fd)


def _nonempty(fd):
    with os.scandir(fd) as entries:
        return any(entries)


def _unexpected(fd, allowed):
    with os.scandir(fd) as entries:
        return any(entry.name not in allowed for entry in entries)


def _private_file(fd, *, limit=_LIMIT):
    metadata = os.fstat(fd)
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_nlink != 1 or metadata.st_size > limit:
        raise BackingUnavailable("Private release evidence is not qualified")
    return metadata


class ReleaseFilesystem:
    """Pinned owner retained across intent, checkpoints and final SQL commit."""

    def __init__(self, volume):
        self.volume = volume
        self.parent = self.journal = self.lock = self.workflow = None
        self.request = self.state = None
        self.name = None
        try:
            if volume.control_path.parent != volume.data_path.parent or (volume.data_path.name, volume.control_path.name) != ("data", "control"):
                raise BackingUnavailable("Provider roots do not have the fixed release layout")
            self.parent = os.open(volume.data_path.parent, _DIRECTORY)
            facts = _facts(os.fstat(self.parent))
            if facts["device"] != volume.device:
                raise BackingUnavailable("Provider parent changed filesystem")
            if hasattr(volume, "mount_id") and _mount_record(self.parent)["mount_id"] != volume.mount_id:
                raise BackingUnavailable("Provider parent changed mount")
            _parent_capability(facts)
            self.parent_facts = facts
            self.current_parent()
        except BaseException:
            self.close()
            raise

    def foreign_journals(self):
        self.current_parent()
        with os.scandir(self.parent) as entries:
            return any(entry.name.startswith(PREFIX) and entry.name != self.name for entry in entries)

    def current_parent(self):
        _parent_capability(_facts(os.fstat(self.parent)))
        current = os.stat(self.volume.data_path.parent, follow_symlinks=False)
        if (current.st_dev, current.st_ino) != (self.parent_facts["device"], self.parent_facts["inode"]):
            raise BackingUnavailable("Provider parent pathname changed")
        if self.journal is not None:
            named = os.stat(self.name, dir_fd=self.parent, follow_symlinks=False)
            owned = os.fstat(self.journal)
            if (named.st_dev, named.st_ino) != (owned.st_dev, owned.st_ino) or not stat.S_ISDIR(named.st_mode) or named.st_uid != os.geteuid() or stat.S_IMODE(named.st_mode) != 0o700:
                raise BackingUnavailable("Owned journal pathname changed")

    def original_facts(self, handle):
        with self.volume.filesystem() as filesystem:
            validate_marker(filesystem, handle)
            roots = {"data": _facts(os.fstat(filesystem._data_fd)), "control": _facts(os.fstat(filesystem._control_fd))}
            for name, facts in roots.items():
                if self._at(self.parent, name) != facts:
                    raise BackingUnavailable("Original roots changed during qualification")
        return {"parent": self.parent_facts, "roots": roots}

    def configure(self, request, operation_id):
        self.request = request
        self.name = PREFIX + operation_id
        if any(request["physical"]["parent"][key] != self.parent_facts[key] for key in ("device", "inode")):
            raise BackingUnavailable("Release parent identity changed")

    def _open_expected(self, parent, name, expected):
        fd = _child(parent, name)
        try:
            if _facts(os.fstat(fd)) != expected:
                raise BackingUnavailable("Owned release root differs from the durable identity")
            return fd
        except BaseException:
            os.close(fd)
            raise

    def _at(self, parent, name):
        try:
            fd = _child(parent, name)
        except FileNotFoundError:
            return None
        try:
            return _facts(os.fstat(fd))
        finally:
            os.close(fd)

    def _original(self):
        for name, facts in self.request["physical"]["roots"].items():
            if self._at(self.parent, name) != facts:
                raise BackingUnavailable("Original release roots changed")

    def _open_journal(self):
        if self.journal is not None:
            self.current_parent()
            return True
        try:
            self.journal = _child(self.parent, self.name)
        except FileNotFoundError:
            return False
        facts = _facts(os.fstat(self.journal))
        if facts["uid"] != os.geteuid() or facts["mode"] != 0o700:
            raise BackingUnavailable("Release journal is not private")
        self.current_parent()
        return True

    def _lock(self, root, name):
        fd = os.open(name, os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=root)
        try:
            metadata = _private_file(fd, limit=0)
            current = os.stat(name, dir_fd=root, follow_symlinks=False)
            if (metadata.st_dev, metadata.st_ino) != (current.st_dev, current.st_ino):
                raise BackingUnavailable("Release ownership pathname changed")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except BaseException:
            os.close(fd)
            raise

    def acquire_existing(self):
        self.current_parent()
        if self._open_journal():
            if self.lock is None:
                try:
                    self.lock = self._lock(self.journal, _JOURNAL_LOCK)
                except FileNotFoundError:
                    self._acquire_workflow()
                    return False
            self._acquire_workflow()
            return True
        self._acquire_workflow()
        return False

    def _acquire_workflow(self):
        if self.workflow is not None:
            return
        expected = self.request["physical"]["roots"]["control"]
        locations = [(self.parent, "control")]
        if self.journal is not None:
            locations.append((self.journal, "control"))
        for parent, name in locations:
            if self._at(parent, name) != expected:
                continue
            control = _child(parent, name)
            try:
                try:
                    self.workflow = self._lock(control, _LOCK)
                except FileNotFoundError:
                    pass
            finally:
                os.close(control)
            return

    def _initial_entries(self):
        if _unexpected(self.journal, {_JOURNAL_LOCK, "data", "control", _TEMP, _STATE}):
            raise BackingUnavailable("Incomplete journal has unexpected evidence")
        for name in ("data", "control"):
            facts = self._at(self.journal, name)
            if facts is None:
                continue
            original = self.request["physical"]["roots"][name]
            fd = _child(self.journal, name)
            try:
                if facts["uid"] not in (os.geteuid(), original["uid"]) or _nonempty(fd):
                    raise BackingUnavailable("Incomplete replacement has unexpected contents")
            finally:
                os.close(fd)
        for name, limit in ((_JOURNAL_LOCK, 0), (_TEMP, _LIMIT)):
            try:
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=self.journal)
            except FileNotFoundError:
                continue
            try:
                _private_file(fd, limit=limit)
            finally:
                os.close(fd)

    def bootstrap(self):
        """Only under a fresh owning SQL parent, after committed release intent."""
        self.current_parent()
        self._original()
        # Never bootstrap past a pre-existing live workflow.
        self.acquire_existing()
        if self.journal is not None:
            self._read_state()
            if self.state is None:
                self._initial_entries()
        with self.volume.filesystem() as filesystem:
            validate_marker(filesystem, self.request["binding"]["backing_handle"])
        for name in ("data", "control"):
            fd = self._open_expected(self.parent, name, self.request["physical"]["roots"][name])
            try:
                _purge(fd, preserve=frozenset((BINDING_MARKER, _LOCK)) if name == "control" else frozenset())
            finally:
                os.close(fd)

    def initialize(self):
        """Caller retains fresh SQL parent until durable ownership exists."""
        self.current_parent()
        if not self._open_journal():
            self._original()
            try:
                os.mkdir(self.name, mode=0o700, dir_fd=self.parent)
            except OSError as exc:
                if exc.errno != errno.ENOSPC:
                    raise
                self.bootstrap()
                os.mkdir(self.name, mode=0o700, dir_fd=self.parent)
            os.fsync(self.parent)
            self._open_journal()
        if self.lock is None:
            try:
                fd = os.open(_JOURNAL_LOCK, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self.journal)
            except FileExistsError:
                pass
            except OSError as exc:
                if exc.errno != errno.ENOSPC:
                    raise
                self.bootstrap()
                fd = os.open(_JOURNAL_LOCK, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self.journal)
                os.close(fd)
            else:
                os.close(fd)
            os.fsync(self.journal)
            self.lock = self._lock(self.journal, _JOURNAL_LOCK)
        self._read_state()
        if self.state is not None:
            return
        self._original()
        allowed = {_JOURNAL_LOCK, "data", "control", _TEMP}
        if _unexpected(self.journal, allowed):
            raise BackingUnavailable("Incomplete journal has unexpected evidence")
        roots = {}
        for name, original in self.request["physical"]["roots"].items():
            try:
                os.mkdir(name, mode=0o700, dir_fd=self.journal)
            except FileExistsError:
                pass
            except OSError as exc:
                if exc.errno != errno.ENOSPC:
                    raise
                self.bootstrap()
                os.mkdir(name, mode=0o700, dir_fd=self.journal)
            fd = _child(self.journal, name)
            try:
                facts = _facts(os.fstat(fd))
                if facts["uid"] not in (os.geteuid(), original["uid"]) or _nonempty(fd):
                    raise BackingUnavailable("Incomplete replacement is not owned and empty")
                os.fchown(fd, original["uid"], original["gid"])
                os.fchmod(fd, original["mode"])
                os.fsync(fd)
                roots[name] = _facts(os.fstat(fd))
            finally:
                os.close(fd)
        self.state = {"schema_version": 1, "request_sha256": _digest(self.request), "roots": roots}
        try:
            self._write_state()
        except OSError as exc:
            if exc.errno != errno.ENOSPC:
                raise
            intended_state = self.state
            self.bootstrap()
            self.state = intended_state
            self._write_state()

    def _read_state(self):
        self.current_parent()
        try:
            fd = os.open(_STATE, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=self.journal)
        except FileNotFoundError:
            self.state = None
            return
        try:
            before = _private_file(fd)
            raw = os.read(fd, _LIMIT + 1)
            after = _private_file(fd)
            current = os.stat(_STATE, dir_fd=self.journal, follow_symlinks=False)
            if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns) or (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            ) != (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns, current.st_ctime_ns):
                raise BackingUnavailable("Release journal changed during inspection")
            state = json.loads(raw)
            if (
                not isinstance(state, dict)
                or set(state) != {"schema_version", "request_sha256", "roots"}
                or type(state["schema_version"]) is not int
                or state["schema_version"] != 1
                or state["request_sha256"] != _digest(self.request)
                or not isinstance(state["roots"], dict)
                or set(state["roots"]) != {"data", "control"}
            ):
                raise BackingUnavailable("Release journal does not match its durable intent")
            for facts in state["roots"].values():
                if not isinstance(facts, dict) or set(facts) != {"device", "inode", "uid", "gid", "mode"} or any(type(value) is not int for value in facts.values()) or facts["device"] != self.parent_facts["device"] or facts["inode"] <= 0:
                    raise BackingUnavailable("Replacement identity is malformed")
            for name, facts in state["roots"].items():
                old = self.request["physical"]["roots"][name]
                if facts["inode"] == old["inode"] or any(facts[key] != old[key] for key in ("device", "uid", "gid", "mode")):
                    raise BackingUnavailable("Replacement identity does not renew the original root")
            if state["roots"]["data"]["inode"] == state["roots"]["control"]["inode"]:
                raise BackingUnavailable("Replacement roots overlap")
            if _unexpected(self.journal, {_STATE, _TEMP, _JOURNAL_LOCK, "data", "control"}):
                raise BackingUnavailable("Release journal has unexpected evidence")
            self.state = state
        finally:
            os.close(fd)

    def _remove_temp(self):
        try:
            fd = os.open(_TEMP, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=self.journal)
        except FileNotFoundError:
            return
        try:
            _private_file(fd)
            os.unlink(_TEMP, dir_fd=self.journal)
        finally:
            os.close(fd)

    def _write_state(self):
        self._remove_temp()
        raw = json.dumps(self.state, separators=(",", ":"), ensure_ascii=True).encode("ascii")
        fd = os.open(_TEMP, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self.journal)
        try:
            offset = 0
            while offset < len(raw):
                written = os.write(fd, raw[offset:])
                if written <= 0:
                    raise OSError("Release journal write made no progress")
                offset += written
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(_TEMP, _STATE, src_dir_fd=self.journal, dst_dir_fd=self.journal)
        os.fsync(self.journal)

    def rotate(self, name):
        self.current_parent()
        self._read_state()
        old, new = self.request["physical"]["roots"][name], self.state["roots"][name]
        public, staged = self._at(self.parent, name), self._at(self.journal, name)
        if public == old and staged == new:
            _exchange(self.parent, name, self.journal)
        elif public != new or staged not in (old, None):
            raise BackingUnavailable("Release root placement is ambiguous")
        os.fsync(self.parent)
        os.fsync(self.journal)

    def purge_retired(self, name):
        self.current_parent()
        if self._at(self.parent, name) != self.state["roots"][name]:
            raise BackingUnavailable("Replacement root changed")
        original = self.request["physical"]["roots"][name]
        retired = self._at(self.journal, name)
        if retired is None:
            return
        if retired != original:
            raise BackingUnavailable("Retired root identity changed")
        fd = self._open_expected(self.journal, name, original)
        try:
            _purge(fd)
            if self._at(self.journal, name) != original:
                raise BackingUnavailable("Retired root pathname changed during purge")
            os.rmdir(name, dir_fd=self.journal)
            os.fsync(self.journal)
        finally:
            os.close(fd)

    def checkpoint(self):
        self.assert_empty(self.state["roots"])
        if any(self._at(self.journal, name) is not None for name in ("data", "control")):
            raise BackingUnavailable("Retired roots remain")
        return {"schema_version": 1, "stage": "filesystem-complete", "parent": self.request["physical"]["parent"], "roots": self.state["roots"], "journal": _facts(os.fstat(self.journal)), "request_sha256": _digest(self.request)}

    def assert_empty(self, roots):
        self.current_parent()
        for name, expected in roots.items():
            if self._at(self.parent, name) != expected:
                raise BackingUnavailable("Completed replacement root changed")
            fd = self._open_expected(self.parent, name, expected)
            try:
                if _nonempty(fd):
                    raise BackingUnavailable("Completed replacement is not empty")
            finally:
                os.close(fd)

    def finish(self, checkpoint):
        self.assert_empty(checkpoint["roots"])
        if not self._open_journal():
            return  # Previous final SQL rollback after owned journal removal.
        if _facts(os.fstat(self.journal)) != checkpoint["journal"]:
            raise BackingUnavailable("Completed journal identity changed")
        # SQL checkpoint, exact journal identity and the owning resource parent
        # also cover interrupted state/lock unlink. Never recreate either here.
        try:
            if self.lock is None:
                self.lock = self._lock(self.journal, _JOURNAL_LOCK)
        except FileNotFoundError:
            pass
        self._read_state()
        if self.state is not None and self.state["roots"] != checkpoint["roots"]:
            raise BackingUnavailable("Completed journal state changed")
        if _unexpected(self.journal, {_STATE, _TEMP, _JOURNAL_LOCK}):
            raise BackingUnavailable("Completed journal has unexpected contents")
        self._remove_temp()
        for name in (_STATE, _JOURNAL_LOCK):
            try:
                os.unlink(name, dir_fd=self.journal)
            except FileNotFoundError:
                pass
        os.rmdir(self.name, dir_fd=self.parent)
        os.fsync(self.parent)

    def close(self):
        for name in ("workflow", "lock", "journal", "parent"):
            fd = getattr(self, name, None)
            if fd is not None:
                setattr(self, name, None)
                os.close(fd)


def unresolved_journals(volume):
    # Capacity observation does not require the root-renewal write capability.
    # Ordinary older prepared roots remain usable until explicit operator repair.
    path = volume.data_path.parent
    fd = os.open(path, _DIRECTORY)
    try:
        owned, current = os.fstat(fd), os.stat(path, follow_symlinks=False)
        if (owned.st_dev, owned.st_ino) != (current.st_dev, current.st_ino) or owned.st_dev != volume.device:
            raise BackingUnavailable("Capacity parent changed during inspection")
        if hasattr(volume, "mount_id") and _mount_record(fd)["mount_id"] != volume.mount_id:
            raise BackingUnavailable("Capacity parent changed mount")
        with os.scandir(fd) as entries:
            return any(entry.name.startswith(PREFIX) for entry in entries)
    finally:
        os.close(fd)
