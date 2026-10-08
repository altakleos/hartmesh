"""Private host-operation locks complement durable cross-session admissions."""

import fcntl
import os
import stat
from contextvars import ContextVar

from deerflow.files.store import open_directory_source

_owner = ContextVar("storage_host_workflow_owner", default=None)
_LOCK = ".host-workflow.lock"
_effect = ContextVar("storage_host_workflow_effect", default=None)
_read_only = ContextVar("storage_host_read_only", default=False)


class WorkflowRejected(Exception):
    """Trusted domain proof of rejection before any owned effect."""

    def __init__(self, error):
        self.error = error


def mark_effect():
    if _read_only.get():
        from deerflow.spaces.contract import SpaceDenied

        raise SpaceDenied("A read admission cannot mutate feature data")
    effect = _effect.get()
    if effect is not None:
        effect[0] = True


def _open(volume, *, create):
    root = open_directory_source(volume.control_path)
    try:
        try:
            fd = os.open(_LOCK, os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC | (os.O_CREAT if create else 0), 0o600, dir_fd=root)
        except FileNotFoundError:
            return None
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_nlink != 1:
            os.close(fd)
            raise OSError("Private host-operation lock changed")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            from deerflow.spaces.service import SpaceOperationPending

            raise SpaceOperationPending("A host workflow is still running; containment is required before recovery") from None
        return fd
    finally:
        os.close(root)


def probe(volume):
    fd = _open(volume, create=False)
    if fd is not None:
        os.close(fd)


def acquire(volumes):
    descriptors = []
    try:
        for key in sorted(volumes):
            descriptors.append(_open(volumes[key], create=True))
        return descriptors
    except BaseException:
        close(descriptors)
        raise


def close(descriptors):
    for fd in descriptors:
        os.close(fd)


# Receipts contain only bounded controller facts, outside every data mount.
_receipt = ContextVar("storage_host_workflow_receipt", default=None)


class WorkflowReceipt:
    def __init__(self, volume, operation_id, request):
        self.root = volume.control_path
        self.operation_id = operation_id
        self.value = {"schema_version": 1, "operation_id": operation_id, "request": request, "facts": {}}

    @property
    def path(self):
        return self.root / f"host-workflow-{self.operation_id}.json"

    def save(self):
        import json
        from uuid import uuid4

        raw = json.dumps(self.value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(raw) > 65536:
            raise ValueError("Host workflow receipt exceeds64KiB")
        root = open_directory_source(self.root)
        temporary = f".host-workflow-{uuid4().hex}.tmp"
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600, dir_fd=root)
            with os.fdopen(fd, "wb") as target:
                target.write(raw)
                target.flush()
                os.fsync(target.fileno())
            os.replace(temporary, self.path.name, src_dir_fd=root, dst_dir_fd=root)
            os.fsync(root)
        finally:
            try:
                os.unlink(temporary, dir_fd=root)
            except FileNotFoundError:
                pass
            os.close(root)

    def load(self):
        import json

        root = open_directory_source(self.root)
        try:
            fd = os.open(self.path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=root)
            with os.fdopen(fd, "rb") as source:
                metadata = os.fstat(source.fileno())
                if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_nlink != 1:
                    raise ValueError("Invalid private host workflow receipt")
                raw = source.read(65537)
            if len(raw) > 65536:
                raise ValueError("Host workflow receipt exceeds64KiB")
            value = json.loads(raw)
            if value.get("schema_version") != 1 or value.get("operation_id") != self.operation_id or value.get("request") != self.value["request"] or not isinstance(value.get("facts"), dict):
                raise ValueError("Host workflow receipt does not match its durable intent")
            self.value = value
        finally:
            os.close(root)

    def remove(self):
        root = open_directory_source(self.root)
        try:
            os.unlink(self.path.name, dir_fd=root)
            os.fsync(root)
        finally:
            os.close(root)


def record_fact(key, value):
    receipt = _receipt.get()
    if receipt is not None:
        if not isinstance(key, str) or len(key) > 64:
            raise ValueError("A bounded workflow fact name is required")
        receipt.value["facts"][key] = value
        receipt.save()
