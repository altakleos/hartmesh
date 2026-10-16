"""Private process-lifetime observation for operator reports; no admission authority."""

import json
import logging
import os
import stat
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from deerflow.files.store import open_directory_source
from deerflow.utils.file_io import await_drained, run_file_io

_DIRECTORY, _RECEIPT, _LOCK = ".gateway-runtime", "start.json", "writer.lock"
_LIMIT = 4096
_FIELDS = frozenset(("version", "started_at", "lifetime_id", "pid", "process_start", "boot_id", "pid_namespace"))
logger = logging.getLogger(__name__)


class GatewayObservationUnavailable(RuntimeError):
    def __init__(self):
        super().__init__("The running Gateway start cannot be confirmed; no lifetime counters were reported")


def _private(metadata, *, directory=False):
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if not kind(metadata.st_mode) or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != (0o700 if directory else 0o600) or (not directory and metadata.st_nlink != 1):
        raise GatewayObservationUnavailable()


def _revision(metadata):
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns


def _directory(home, *, create=False):
    parent = open_directory_source(Path(home))
    try:
        if create:
            try:
                os.mkdir(_DIRECTORY, 0o700, dir_fd=parent)
            except FileExistsError:
                pass
        descriptor = os.open(_DIRECTORY, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
        try:
            _private(os.fstat(descriptor), directory=True)
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise
    finally:
        os.close(parent)


def _process(pid):
    if type(pid) is not int or pid <= 0:
        raise GatewayObservationUnavailable()
    process = Path("/proc") / str(pid)
    if process.stat().st_uid != os.getuid():
        raise GatewayObservationUnavailable()
    facts = (process / "stat").read_text(encoding="utf-8").rsplit(") ", 1)[1].split()
    if facts[0] in ("Z", "X"):
        raise GatewayObservationUnavailable()
    boot = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    UUID(boot)
    return int(facts[19]), boot, (process / "ns/pid").stat().st_ino


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise GatewayObservationUnavailable()
        value[key] = item
    return value


def _load(directory):
    descriptor = os.open(_RECEIPT, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=directory)
    try:
        before = os.fstat(descriptor)
        _private(before)
        if before.st_size > _LIMIT:
            raise GatewayObservationUnavailable()
        raw = os.read(descriptor, _LIMIT + 1)
        if len(raw) > _LIMIT or len(raw) != before.st_size or _revision(before) != _revision(os.fstat(descriptor)) or _revision(before) != _revision(os.stat(_RECEIPT, dir_fd=directory, follow_symlinks=False)):
            raise GatewayObservationUnavailable()
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique)
        if not isinstance(document, dict) or set(document) != _FIELDS or type(document["version"]) is not int or document["version"] != 1:
            raise GatewayObservationUnavailable()
        if not isinstance(document["lifetime_id"], str) or UUID(hex=document["lifetime_id"]).hex != document["lifetime_id"]:
            raise GatewayObservationUnavailable()
        started = datetime.fromisoformat(document["started_at"])
        if started.tzinfo is None or started.utcoffset() != UTC.utcoffset(started):
            raise GatewayObservationUnavailable()
        if type(document["process_start"]) is not int or type(document["pid_namespace"]) is not int:
            raise GatewayObservationUnavailable()
        return document, started, _revision(before)
    finally:
        os.close(descriptor)


def _lifetime_lock(lifetime_id):
    return ".lifetime-" + lifetime_id + ".lock"


def _active(directory, lifetime_id):
    import fcntl

    name = _lifetime_lock(lifetime_id)
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=directory)
    try:
        metadata = os.fstat(descriptor)
        _private(metadata)
        if metadata.st_size or _revision(metadata) != _revision(os.stat(name, dir_fd=directory, follow_symlinks=False)):
            raise GatewayObservationUnavailable()
        try:
            fcntl.flock(descriptor, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        raise GatewayObservationUnavailable()
    finally:
        os.close(descriptor)


@dataclass(frozen=True)
class GatewayObservation:
    started_at: datetime
    _proof: tuple

    def require_current(self, home):
        if read(home) != self:
            raise GatewayObservationUnavailable()


def read(home):
    directory = None
    try:
        directory = _directory(home)
        document, started, revision = _load(directory)
        _active(directory, document["lifetime_id"])
        if _process(document["pid"]) != (document["process_start"], document["boot_id"], document["pid_namespace"]):
            raise GatewayObservationUnavailable()
        _active(directory, document["lifetime_id"])
        return GatewayObservation(started, (revision, tuple(sorted(document.items()))))
    except Exception:
        raise GatewayObservationUnavailable() from None
    finally:
        if directory is not None:
            os.close(directory)


class _Lifetime:
    def __init__(self, home, directory, lock):
        self.path = Path(home) / _DIRECTORY / _RECEIPT
        self._directory, self._lock, self._published = directory, lock, None
        self._proof_lock, self._proof_name = None, None

    def close(self):
        if self._directory is None:
            return
        try:
            if self._published is not None:
                document, _started, revision = _load(self._directory)
                if (revision, tuple(sorted(document.items()))) == self._published:
                    os.unlink(_RECEIPT, dir_fd=self._directory)
                    os.fsync(self._directory)
        except Exception:
            pass  # An unconfirmed observation is retained, never repaired.
        finally:
            if self._proof_lock is not None:
                try:
                    if _revision(os.fstat(self._proof_lock)) == _revision(os.stat(self._proof_name, dir_fd=self._directory, follow_symlinks=False)):
                        os.unlink(self._proof_name, dir_fd=self._directory)
                except OSError:
                    pass
                finally:
                    os.close(self._proof_lock)
                    self._proof_lock = None
            os.close(self._lock)
            os.close(self._directory)
            self._lock = self._directory = None


def publish(home, *, started_at):
    import fcntl

    directory = lock = lifetime = None
    stage = ".stage-" + uuid4().hex
    try:
        if not isinstance(started_at, datetime) or started_at.tzinfo is None or started_at.utcoffset() != UTC.utcoffset(started_at):
            raise GatewayObservationUnavailable()
        directory = _directory(home, create=True)
        lock = os.open(_LOCK, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, 0o600, dir_fd=directory)
        metadata = os.fstat(lock)
        _private(metadata)
        if metadata.st_size:
            raise GatewayObservationUnavailable()
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        lifetime = _Lifetime(home, directory, lock)
        lifetime_id = uuid4().hex
        lifetime._proof_name = _lifetime_lock(lifetime_id)
        lifetime._proof_lock = os.open(lifetime._proof_name, os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        fcntl.flock(lifetime._proof_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        start, boot, namespace = _process(os.getpid())
        document = {"version": 1, "started_at": started_at.isoformat(), "lifetime_id": lifetime_id, "pid": os.getpid(), "process_start": start, "boot_id": boot, "pid_namespace": namespace}
        descriptor = os.open(stage, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=directory)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(document, sort_keys=True) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(stage, _RECEIPT, src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
        saved, _started, revision = _load(directory)
        if saved != document:
            raise GatewayObservationUnavailable()
        lifetime._published = revision, tuple(sorted(saved.items()))
        return lifetime
    except BaseException as exc:
        if directory is not None:
            try:
                os.unlink(stage, dir_fd=directory)
            except OSError:
                pass
        if lifetime is not None:
            lifetime.close()
            directory = lock = None
        if lock is not None:
            os.close(lock)
        if isinstance(exc, Exception):
            raise GatewayObservationUnavailable() from None
        raise
    finally:
        if directory is not None and lifetime is None:
            os.close(directory)


@asynccontextmanager
async def gateway_observation_lifetime(*, enabled, started_at, home=None):
    if enabled is not True:
        yield
        return
    owned = []

    def begin():
        from deerflow.config.paths import get_paths

        # Capture inside the worker so cancellation cannot lose a late writer.
        owned.append(publish(get_paths().base_dir if home is None else home, started_at=started_at))

    try:
        try:
            await await_drained(run_file_io(begin))
        except Exception as exc:
            logger.warning("Gateway observation unavailable (%s)", type(exc).__name__)
        yield
    finally:
        if owned:
            try:
                await await_drained(run_file_io(owned[0].close))
            except Exception as exc:
                logger.warning("Gateway observation cleanup unavailable (%s)", type(exc).__name__)
