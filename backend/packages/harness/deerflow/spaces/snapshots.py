"""Quiesced same-filesystem archives; caller must fence every native mount.

Backups, restore staging and displaced data all consume the resource's fixed
byte/inode budget. No changing-database-file copy or coherent-live-tree claim.
Root directory identity stays fixed through restore, including uncertain work.
"""

import hashlib
import os
import shutil
import stat
import tarfile
from pathlib import Path

from deerflow.spaces.filesystem import FileConflict

MAX_ARCHIVE_ENTRIES = 100_000


def _digest(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise FileConflict("Backup is not a regular private file")
        hasher = hashlib.sha256()
        while chunk := os.read(fd, 64 * 1024):
            hasher.update(chunk)
        return hasher.hexdigest(), os.fstat(fd).st_size
    finally:
        os.close(fd)


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def backup(volume, backup_id):
    target = volume.control_path / ("backup-" + backup_id + ".tar")
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=False) as output, tarfile.open(fileobj=output, mode="w", dereference=False) as archive:
            pending = [volume.data_path]
            count = 0
            while pending:
                directory = pending.pop()
                for child in sorted(directory.iterdir()):
                    metadata = child.lstat()
                    if not (stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode)):
                        raise FileConflict("Backup does not support special filesystem nodes")
                    count += 1
                    if count > MAX_ARCHIVE_ENTRIES:
                        raise FileConflict("Backup exceeds the supported entry count")
                    archive.add(child, arcname=child.relative_to(volume.data_path).as_posix(), recursive=False)
                    if stat.S_ISDIR(metadata.st_mode):
                        pending.append(child)
            output.flush()
        os.fsync(fd)
        _sync_directory(volume.control_path)
        return _digest(target)
    except BaseException:
        # No public bytes have changed. This exact file belongs to this call.
        target.unlink(missing_ok=True)
        raise
    finally:
        os.close(fd)


def _member_path(name):
    if not isinstance(name, str) or name.startswith("/") or "\x00" in name:
        raise FileConflict("Backup contains an invalid path")
    parts = name.rstrip("/").split("/")
    if not parts or any(part in ("", ".", "..") for part in parts):
        raise FileConflict("Backup contains an invalid path")
    return Path(*parts)


def restore(volume, backup_id, expected_sha256, expected_size, operation_id):
    source = volume.control_path / ("backup-" + backup_id + ".tar")
    if _digest(source) != (expected_sha256, expected_size):
        raise FileConflict("Backup integrity differs from its durable record")
    stage = volume.control_path / ("restore-" + operation_id)
    stage.mkdir(mode=0o700)
    fresh, old = stage / "new", stage / "old"
    fresh.mkdir(mode=0o700)
    old.mkdir(mode=0o700)
    published = False
    try:
        seen, directories, hardlinks = set(), [], []
        with tarfile.open(source, mode="r:") as archive:
            for member in archive:
                relative = _member_path(member.name)
                if relative in seen or len(seen) >= MAX_ARCHIVE_ENTRIES or member.sparse or member.size < 0 or not (member.isdir() or member.isfile() or member.issym() or member.islnk()):
                    raise FileConflict("Backup nodes are duplicate or unsupported")
                seen.add(relative)
                target = fresh / relative
                # Parents must already be declared directories. Never traverse
                # an extracted link, even for an operator-tampered archive.
                parent = fresh
                for part in relative.parts[:-1]:
                    parent = parent / part
                    if not stat.S_ISDIR(parent.lstat().st_mode):
                        raise FileConflict("Backup parent is not a directory")
                if member.isdir():
                    target.mkdir(mode=0o700)
                    directories.append((target, member))
                elif member.issym():
                    if "\x00" in member.linkname:
                        raise FileConflict("Backup link target is invalid")
                    target.symlink_to(member.linkname)
                elif member.islnk():
                    hardlinks.append((target, _member_path(member.linkname)))
                else:
                    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
                    try:
                        stream = archive.extractfile(member)
                        remaining = member.size
                        while remaining:
                            chunk = stream.read(min(64 * 1024, remaining))
                            if not chunk:
                                raise FileConflict("Backup payload is truncated")
                            offset = 0
                            while offset < len(chunk):
                                written = os.write(fd, chunk[offset:])
                                if written <= 0:
                                    raise OSError("Restore write made no progress")
                                offset += written
                            remaining -= len(chunk)
                        os.fchmod(fd, member.mode & 0o777)
                        os.fchown(fd, member.uid, member.gid)
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                if not member.islnk():
                    os.chown(target, member.uid, member.gid, follow_symlinks=False)
                    os.utime(target, (member.mtime, member.mtime), follow_symlinks=False)
            for target, relative in hardlinks:
                original = fresh / relative
                parent = fresh
                for part in relative.parts[:-1]:
                    parent = parent / part
                    if not stat.S_ISDIR(parent.lstat().st_mode):
                        raise FileConflict("Backup hardlink crosses a link parent")
                if relative not in seen or not stat.S_ISREG(original.lstat().st_mode):
                    raise FileConflict("Backup hardlink must reference an archived regular file")
                os.link(original, target, follow_symlinks=False)
            for target, member in reversed(directories):
                os.chmod(target, member.mode & 0o777)
                os.utime(target, (member.mtime, member.mtime), follow_symlinks=False)
                _sync_directory(target)
            _sync_directory(fresh)
        # Mark uncertainty before the first public namespace mutation. A
        # retained old/new stage lets an operator inspect partial publication.
        published = True
        for child in volume.data_path.iterdir():
            os.rename(child, old / child.name)
        _sync_directory(old)
        _sync_directory(volume.data_path)
        for child in fresh.iterdir():
            os.rename(child, volume.data_path / child.name)
        _sync_directory(volume.data_path)
        _sync_directory(fresh)
        _sync_directory(volume.control_path)
    except (tarfile.TarError, ValueError) as exc:
        if published:
            raise
        raise FileConflict("Backup structure is invalid") from exc
    finally:
        if not published:
            shutil.rmtree(stage)


def cleanup_restore(volume, operation_id):
    stage = volume.control_path / ("restore-" + operation_id)
    if stage.exists():
        shutil.rmtree(stage)
        _sync_directory(volume.control_path)


def delete_visible(volume):
    # Only a fenced, admitted host can call this. Never follow a visible link.
    for child in volume.data_path.iterdir():
        if stat.S_ISDIR(child.lstat().st_mode):
            shutil.rmtree(child)
        else:
            child.unlink()
    _sync_directory(volume.data_path)
