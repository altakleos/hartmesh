"""Linux descriptor-confined ordinary files, below the host authorization layer.

This class supplies neither quotas nor writer exclusion. Production callers
must select a qualified backing and hold their admitted operation/edit window.
Hashes detect stale browser revisions; they do not coordinate native writers.
Private staging must be a sibling on the same filesystem, outside the view.
"""

from __future__ import annotations

import ctypes
import errno
import hashlib
import os
import re
import stat
import sys
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from deerflow.spaces.contract import SpaceConflict


class UnsafeSpacePath(ValueError):
    """The relative name, link or node cannot be safely operated on."""


class FilesystemUnavailable(RuntimeError):
    """This platform cannot supply the required confined operation."""


class FileConflict(SpaceConflict):
    """The existing file differs from the revision the caller admitted."""


class FileTooLarge(ValueError):
    """Use the streaming download rather than a bounded in-memory read."""


@dataclass(frozen=True)
class FileEntry:
    name: str
    path: str
    kind: Literal["file", "directory", "symlink", "unsupported"]
    size: int
    modified: float
    accessible: bool


class _OpenHow(ctypes.Structure):
    _fields_ = [("flags", ctypes.c_uint64), ("mode", ctypes.c_uint64), ("resolve", ctypes.c_uint64)]


def _path(value: str, *, allow_root: bool = False) -> str:
    if not isinstance(value, str) or len(value) > 4096 or "\0" in value:
        raise UnsafeSpacePath("Invalid resource-relative path")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeError as exc:
        raise UnsafeSpacePath("The browser API requires UTF-8 file names") from exc
    if allow_root and not value:
        return "."
    if any(part in ("", ".", "..") for part in value.split("/")):
        raise UnsafeSpacePath("Use an unambiguous resource-relative path")
    return value


def _openat2(root_fd: int, path: str, flags: int) -> int:
    # Linux assigns openat2 number437 on both qualified 64-bit architectures.
    # No lexical resolve/open fallback: it would reopen link-race escapes.
    if sys.platform != "linux" or os.uname().machine not in ("x86_64", "aarch64"):
        raise FilesystemUnavailable("Confined files require qualified Linux openat2")
    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    # BENEATH | NO_MAGICLINKS | NO_XDEV. Relative internal links remain valid;
    # absolute links and nested mount transitions are explicitly unsupported.
    how = _OpenHow(flags | os.O_CLOEXEC, 0, 0x08 | 0x02 | 0x01)
    result = libc.syscall(437, root_fd, ctypes.c_char_p(path.encode("utf-8")), ctypes.byref(how), ctypes.sizeof(how))
    if result < 0:
        error = ctypes.get_errno()
        if error in (errno.ENOSYS, errno.EINVAL, errno.EPERM):
            raise FilesystemUnavailable("The host cannot supply qualified Linux openat2")
        if error in (errno.EXDEV, errno.ELOOP, errno.ENOTDIR):
            raise UnsafeSpacePath("Link or mount cannot be followed within this space")
        raise OSError(error, os.strerror(error), path)
    return result


class ConfinedFilesystem:
    def __init__(self, data_root: Path, control_root: Path, *, expected_data: tuple[int, int] | None = None, expected_control: tuple[int, int] | None = None) -> None:
        self._data_fd: int | None = None
        self._control_fd: int | None = None
        if sys.platform != "linux":
            raise FilesystemUnavailable("This filesystem adapter requires Linux")
        data, control = Path(data_root).resolve(), Path(control_root).resolve()
        if data == control or data.is_relative_to(control) or control.is_relative_to(data):
            raise UnsafeSpacePath("Private staging and visible data cannot overlap")
        try:
            self._data_fd = os.open(data_root, os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            self._control_fd = os.open(control_root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
            data_stat, control_stat = os.fstat(self._data_fd), os.fstat(self._control_fd)
            if (expected_data is not None and (data_stat.st_dev, data_stat.st_ino) != expected_data) or (expected_control is not None and (control_stat.st_dev, control_stat.st_ino) != expected_control):
                raise FilesystemUnavailable("Resource root incarnation changed; revalidate provider containment")
            if data_stat.st_dev != control_stat.st_dev:
                raise UnsafeSpacePath("Atomic staging must use the resource filesystem")
            if control_stat.st_mode & 0o077 or (os.geteuid() != 0 and control_stat.st_uid != os.geteuid()):
                raise UnsafeSpacePath("Private staging must be owned and accessible only by the host")
            probe = _openat2(self._data_fd, ".", os.O_PATH | os.O_DIRECTORY)
            os.close(probe)
        except BaseException:
            self.close()
            raise

    def __enter__(self) -> ConfinedFilesystem:
        return self

    def __exit__(self, *_args) -> None:
        self.close()

    def close(self) -> None:
        for attribute in ("_data_fd", "_control_fd"):
            fd = getattr(self, attribute)
            if fd is not None:
                setattr(self, attribute, None)
                os.close(fd)

    def _open(self, path: str, flags: int, *, allow_root: bool = False) -> int:
        if self._data_fd is None:
            raise FilesystemUnavailable("The resource descriptor has closed")
        return _openat2(self._data_fd, _path(path, allow_root=allow_root), flags)

    def open_regular(self, path: str) -> int:
        # O_PATH avoids opening a planted FIFO/device. The host-owned proc-fd
        # reopen targets that already-verified inode, never the mutable name.
        descriptor = self._open(path, os.O_PATH)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise UnsafeSpacePath("Only regular files can be read")
            return os.open(f"/proc/self/fd/{descriptor}", os.O_RDONLY | os.O_CLOEXEC | os.O_NONBLOCK | os.O_NOCTTY)
        finally:
            os.close(descriptor)

    def list_directory(self, path: str = "", *, limit: int = 200) -> tuple[list[FileEntry], bool]:
        if type(limit) is not int or not 1 <= limit <= 500:
            raise ValueError("Directory limit must be between1 and500")
        fd = self._open(path, os.O_RDONLY | os.O_DIRECTORY, allow_root=True)
        entries: list[FileEntry] = []
        truncated = False
        try:
            with os.scandir(fd) as directory:
                for child in directory:
                    if len(entries) == limit:
                        truncated = True
                        break
                    relative = f"{path}/{child.name}" if path else child.name
                    try:
                        _path(relative)
                        metadata = child.stat(follow_symlinks=False)
                    except FileNotFoundError:
                        continue  # Native rename removed this observation.
                    mode = metadata.st_mode
                    kind = "symlink" if stat.S_ISLNK(mode) else "directory" if stat.S_ISDIR(mode) else "file" if stat.S_ISREG(mode) else "unsupported"
                    accessible = kind != "unsupported"
                    if kind == "symlink":
                        target = None
                        try:
                            target = self._open(relative, os.O_PATH)
                            target_mode = os.fstat(target).st_mode
                            accessible = stat.S_ISREG(target_mode) or stat.S_ISDIR(target_mode)
                        except (OSError, UnsafeSpacePath):
                            accessible = False
                        finally:
                            if target is not None:
                                os.close(target)
                    entries.append(FileEntry(child.name, relative, kind, metadata.st_size, metadata.st_mtime, accessible))
        finally:
            os.close(fd)
        return sorted(entries, key=lambda entry: entry.name), truncated

    def read_bytes(self, path: str, *, max_bytes: int) -> bytes:
        if type(max_bytes) is not int or not 1 <= max_bytes <= 64 * 1024 * 1024:
            raise ValueError("Bounded read requires a positive limit up to64MiB")
        fd = self.open_regular(path)
        try:
            before = os.fstat(fd)
            if before.st_size > max_bytes:
                raise FileTooLarge("File exceeds the preview limit; use streaming download")
            chunks, remaining = [], max_bytes + 1
            while remaining:
                chunk = os.read(fd, min(remaining, 64 * 1024))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            content = b"".join(chunks)
            if len(content) > max_bytes:
                raise FileTooLarge("File exceeds the preview limit; use streaming download")
            after = os.fstat(fd)
            if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (after.st_size, after.st_mtime_ns, after.st_ctime_ns) or len(content) != after.st_size:
                raise FileConflict("File changed during its bounded read; retry")
            return content
        finally:
            os.close(fd)

    def _parent(self, path: str) -> tuple[int, str]:
        path = _path(path)
        parent, _, name = path.rpartition("/")
        return self._open(parent, os.O_RDONLY | os.O_DIRECTORY, allow_root=True), name

    def write_atomic(self, path: str, content: bytes, *, expected_sha256: str | None, create: bool = False) -> str:
        if not isinstance(content, bytes) or type(create) is not bool:
            raise ValueError("Atomic writes require bytes and an explicit create flag")
        if not create and (not isinstance(expected_sha256, str) or not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)):
            raise ValueError("Editing requires the loaded SHA-256 revision")
        if create and expected_sha256 is not None:
            raise ValueError("Creation cannot also name an existing revision")
        parent, name = self._parent(path)
        stage, stage_fd = "stage-" + uuid.uuid4().hex, None
        try:
            old = None
            try:
                old = os.stat(name, dir_fd=parent, follow_symlinks=False)
            except FileNotFoundError:
                if not create:
                    raise
            if old is not None and not stat.S_ISREG(old.st_mode):
                raise UnsafeSpacePath("Browser edits do not replace link or directory nodes")
            if create and old is not None:
                raise FileConflict("Destination already exists")
            if not create and hashlib.sha256(self.read_bytes(path, max_bytes=64 * 1024 * 1024)).hexdigest() != expected_sha256:
                raise FileConflict("File differs from the loaded revision")
            stage_fd = os.open(stage, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=self._control_fd)
            offset = 0
            while offset < len(content):
                written = os.write(stage_fd, content[offset : offset + 64 * 1024])
                if written <= 0:
                    raise OSError(errno.EIO, "File write made no progress")
                offset += written
            if old is not None:
                os.fchmod(stage_fd, stat.S_IMODE(old.st_mode) & 0o777)
                stage_stat = os.fstat(stage_fd)
                if (stage_stat.st_uid, stage_stat.st_gid) != (old.st_uid, old.st_gid):
                    os.fchown(stage_fd, old.st_uid, old.st_gid)
            else:
                owner = os.fstat(self._data_fd)
                stage_stat = os.fstat(stage_fd)
                if (stage_stat.st_uid, stage_stat.st_gid) != (owner.st_uid, owner.st_gid):
                    os.fchown(stage_fd, owner.st_uid, owner.st_gid)
            os.fsync(stage_fd)
            if create:
                self._rename_no_replace(self._control_fd, stage, parent, name)
            else:
                current = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns, current.st_ctime_ns) != (old.st_dev, old.st_ino, old.st_size, old.st_mtime_ns, old.st_ctime_ns):
                    raise FileConflict("File changed before replacement")
                os.replace(stage, name, src_dir_fd=self._control_fd, dst_dir_fd=parent)
            os.fsync(parent)
            os.fsync(self._control_fd)
            return hashlib.sha256(content).hexdigest()
        finally:
            if stage_fd is not None:
                os.close(stage_fd)
            try:
                os.unlink(stage, dir_fd=self._control_fd)
            except FileNotFoundError:
                pass
            os.close(parent)

    @staticmethod
    def _rename_no_replace(source_fd: int, source: str, destination_fd: int, destination: str) -> None:
        libc = ctypes.CDLL(None, use_errno=True)
        operation = getattr(libc, "renameat2", None)
        if operation is None:
            raise FilesystemUnavailable("Atomic no-replace publication requires Linux renameat2")
        result = operation(source_fd, ctypes.c_char_p(source.encode("utf-8")), destination_fd, ctypes.c_char_p(destination.encode("utf-8")), 1)
        if result:
            error = ctypes.get_errno()
            if error == errno.EEXIST:
                raise FileConflict("Destination already exists")
            if error in (errno.ENOSYS, errno.EINVAL):
                raise FilesystemUnavailable("The host cannot supply atomic no-replace publication")
            raise OSError(error, os.strerror(error))

    def mkdir(self, path: str) -> None:
        parent, name = self._parent(path)
        try:
            os.mkdir(name, mode=0o700, dir_fd=parent)
            owner = os.fstat(self._data_fd)
            os.chown(name, owner.st_uid, owner.st_gid, dir_fd=parent, follow_symlinks=False)
            os.fsync(parent)
        finally:
            os.close(parent)

    def rename(self, source: str, destination: str) -> None:
        source_fd, name = self._parent(source)
        destination_fd = None
        try:
            destination_fd, new_name = self._parent(destination)
            self._rename_no_replace(source_fd, name, destination_fd, new_name)
            os.fsync(source_fd)
            os.fsync(destination_fd)
        finally:
            os.close(source_fd)
            if destination_fd is not None:
                os.close(destination_fd)

    def remove(self, path: str) -> None:
        parent, name = self._parent(path)
        try:
            metadata = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode):
                os.rmdir(name, dir_fd=parent)
            else:
                os.unlink(name, dir_fd=parent)
            os.fsync(parent)
        finally:
            os.close(parent)
