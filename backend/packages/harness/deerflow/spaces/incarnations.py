"""Private binding identity complements physical filesystem qualification."""

import os
import re
import stat
from dataclasses import dataclass, field
from pathlib import Path

from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.filesystem import ConfinedFilesystem, FilesystemUnavailable

BINDING_MARKER = ".space-binding"


def _handle(value):
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{32}", value) is None:
        raise BackingUnavailable("The durable backing identity is invalid")
    return value.encode("ascii")


def validate_marker(filesystem, backing_handle):
    _validate_marker_at(filesystem._control_fd, backing_handle)


def _validate_marker_at(control_fd, backing_handle):
    expected = _handle(backing_handle)
    fd = None
    try:
        fd = os.open(BINDING_MARKER, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=control_fd)
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_uid != os.geteuid() or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1 or before.st_size != 32:
            raise BackingUnavailable("The private binding marker is not qualified")
        raw = os.read(fd, 33)
        after = os.fstat(fd)
        current = os.stat(BINDING_MARKER, dir_fd=control_fd, follow_symlinks=False)

        def identity(metadata):
            return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns

        if raw != expected or identity(before) != identity(after) or identity(after) != identity(current):
            raise BackingUnavailable("The private binding incarnation changed")
    except OSError as exc:
        raise BackingUnavailable("The private binding identity is unavailable") from exc
    finally:
        if fd is not None:
            os.close(fd)


def repair_home_mode(volume, root_fd, data_fd, control_fd, backing_handle, inodes):
    """Caller holds the current Home lifecycle lock and has qualified this mount."""
    from deerflow.spaces.backings import _mount_record
    from deerflow.spaces.filesystem import _openat2
    from deerflow.spaces.release_filesystem import unresolved_journals
    from deerflow.spaces.workflows import acquire, close

    root = os.fstat(root_fd)
    mount = _mount_record(root_fd)["mount_id"]
    for fd, name, inode in zip((data_fd, control_fd), ("data", "control"), inodes, strict=True):
        held = os.fstat(fd)
        current = os.stat(name, dir_fd=root_fd, follow_symlinks=False)
        if (held.st_dev, held.st_ino) != (root.st_dev, inode) or not os.path.samestat(held, current) or not stat.S_ISDIR(held.st_mode) or held.st_uid != os.geteuid() or _mount_record(fd)["mount_id"] != mount:
            raise BackingUnavailable("The bound Home root or owner changed")
    if os.fstat(control_fd).st_mode & 0o077:
        raise BackingUnavailable("Private Home control must remain host-only")
    os.close(_openat2(control_fd, ".", os.O_PATH | os.O_DIRECTORY, follow_symlinks=False))
    _validate_marker_at(control_fd, backing_handle)
    if unresolved_journals(volume):
        raise BackingUnavailable("Home release is still in progress")
    if os.fstat(data_fd).st_mode & 0o700 == 0o700:
        return
    # The current SQL binding authorizes creation of a missing lock only
    # after the private marker has been validated. A live writer wins.
    held = acquire({backing_handle: volume}, control_fds={backing_handle: control_fd})
    try:
        # Linux fchmod does not accept O_PATH. procfs references the pinned inode,
        # including a mode-000 directory which cannot be opened for reading.
        os.chmod(f"/proc/self/fd/{data_fd}", 0o770)
        if stat.S_IMODE(os.fstat(data_fd).st_mode) != 0o770:
            raise BackingUnavailable("Home directory access changed during repair")
    finally:
        close(held)


def publish_marker(filesystem, backing_handle):
    """Host ownership boundary only: never replace an existing marker."""
    raw = _handle(backing_handle)
    try:
        fd = os.open(BINDING_MARKER, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=filesystem._control_fd)
    except FileExistsError:
        validate_marker(filesystem, backing_handle)
        return
    try:
        offset = 0
        while offset < len(raw):
            written = os.write(fd, raw[offset:])
            if written <= 0:
                raise OSError("Private binding write made no progress")
            offset += written
        os.fsync(fd)
        os.fsync(filesystem._control_fd)
    finally:
        os.close(fd)


@dataclass(frozen=True)
class BoundVolume:
    """An immutable resource expectation layered over physical catalog facts."""

    physical: object
    backing_handle: str
    managed_home: bool = False
    device: int = field(init=False)
    root_inode: int = field(init=False)
    control_inode: int = field(init=False)
    data_path: Path = field(init=False)
    control_path: Path = field(init=False)

    def __post_init__(self):
        _handle(self.backing_handle)
        for name in ("device", "root_inode", "control_inode", "data_path", "control_path"):
            object.__setattr__(self, name, getattr(self.physical, name))

    def __getattr__(self, name):
        return getattr(object.__getattribute__(self, "physical"), name)

    def filesystem(self):
        try:
            filesystem = ConfinedFilesystem(self.data_path, self.control_path, expected_data=(self.device, self.root_inode), expected_control=(self.device, self.control_inode))
        except FilesystemUnavailable as exc:
            raise BackingUnavailable("The bound resource roots are unavailable") from exc
        try:
            validate_marker(filesystem, self.backing_handle)
            if self.managed_home or getattr(getattr(self.physical, "spec", None), "kind", None) in ("employee", "assistant"):
                if os.fstat(filesystem._data_fd).st_uid != os.fstat(filesystem._control_fd).st_uid:
                    raise BackingUnavailable("The bound Home root has an unqualified owner")
        except BaseException:
            filesystem.close()
            raise
        return filesystem


def prepare_marker(volume, backing_handle):
    with volume.filesystem() as filesystem:
        publish_marker(filesystem, backing_handle)


def verify_bound(volume, backing_handle, *, managed_home=False):
    bound = BoundVolume(volume, backing_handle, managed_home=managed_home)
    with bound.filesystem():
        pass
    return bound
