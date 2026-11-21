"""Bounded direct children of the file-backed virtual folder tree."""

import errno
import os
import stat
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass

from deerflow.config.paths import get_paths
from deerflow.features.paths import FeaturePaths
from deerflow.files.store import MAX_PATH_DEPTH, SafeFileAccessUnavailable, StoredFile, StoreError, normalize_relative_path, open_directory_source
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.filesystem import FilesystemUnavailable, UnsafeSpacePath, _openat2

MAX_FOLDER_SCAN = 10_000
DIRECTORY = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
MISSING = (errno.ENOENT, errno.ENOTDIR, errno.ELOOP)


@dataclass(frozen=True)
class FolderPage:
    files: list[StoredFile]
    folders: list[str]
    truncated: bool


class _ScanLimit(Exception):
    pass


@contextmanager
def _folder_descriptor(paths, owner, path):
    namespace, prefix = ("hm.my-files", "files") if owner is not None else ("hm.shared", "shared")
    if isinstance(paths, FeaturePaths):
        volume = paths._volume(namespace, owner)
        with ExitStack() as stack:
            # An unavailable shelf is distinct from a missing virtual folder.
            try:
                filesystem = stack.enter_context(volume.filesystem())
                shelf = filesystem.open_directory(prefix)
            except (UnsafeSpacePath, FilesystemUnavailable, OSError) as exc:
                raise BackingUnavailable("The managed file root is unavailable") from exc
            stack.callback(os.close, shelf)
            if path:
                folder = _openat2(shelf, path, DIRECTORY, follow_symlinks=False)
                stack.callback(os.close, folder)
            else:
                folder = shelf
            yield folder, True
    else:
        root = paths.user_files_dir(owner) if owner is not None else paths.shared_dir()
        fd = open_directory_source(root / path)
        try:
            yield fd, False
        finally:
            os.close(fd)


def read_folder(owner: str | None, path: str, limit: int) -> FolderPage:
    """Only a reachable regular file can establish a virtual group's existence."""
    if normalize_relative_path(path, allow_empty=True) != path or not 1 <= limit <= 500:
        raise StoreError("Use a canonical folder path and a limit from 1 to 500")
    root_parts = len(path.split("/")) if path else 0
    if root_parts >= MAX_PATH_DEPTH:
        raise FileNotFoundError
    remaining = MAX_FOLDER_SCAN
    files: list[StoredFile] = []
    folders: list[str] = []

    def scan(fd, prefix, depth, *, witness, managed):
        nonlocal remaining
        with os.scandir(fd) as entries:
            for entry in entries:
                if not remaining:
                    raise _ScanLimit
                remaining -= 1
                name = entry.name
                try:
                    if not name or name.startswith(".") or normalize_relative_path(name) != name:
                        continue
                    name.encode("utf-8")
                    directory = entry.is_dir(follow_symlinks=False)
                    if directory and depth + 1 >= MAX_PATH_DEPTH:
                        continue
                    if not directory and not entry.is_file(follow_symlinks=False):
                        continue
                    child = None
                    if managed:
                        child = _openat2(fd, name, DIRECTORY if directory else os.O_PATH, follow_symlinks=False)
                    elif directory:
                        child = os.open(name, DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                    try:
                        relative = prefix + "/" + name if prefix else name
                        if directory:
                            if not scan(child, relative, depth + 1, witness=True, managed=managed):
                                continue
                        else:
                            metadata = os.fstat(child) if child is not None else entry.stat(follow_symlinks=False)
                            if not stat.S_ISREG(metadata.st_mode):
                                continue
                        if witness:
                            return True
                        if len(files) + len(folders) >= limit:
                            raise _ScanLimit
                        if directory:
                            folders.append(relative)
                        else:
                            files.append(StoredFile(relative, name, metadata.st_size, metadata.st_mtime))
                    finally:
                        if child is not None:
                            os.close(child)
                except (UnicodeError, StoreError, UnsafeSpacePath):
                    continue
                except OSError as error:
                    if error.errno not in (*MISSING, errno.EACCES, errno.EPERM):
                        raise
        return False

    try:
        with _folder_descriptor(get_paths(), owner, path) as (fd, managed):
            scan(fd, path, root_parts, witness=False, managed=managed)
    except _ScanLimit:
        return FolderPage(files, folders, True)
    except SafeFileAccessUnavailable:
        raise
    except FileNotFoundError:
        if path:
            raise
        if isinstance(get_paths(), FeaturePaths):
            raise BackingUnavailable("The managed file root is unavailable") from None
        return FolderPage([], [], False)
    except (UnsafeSpacePath, StoreError):
        if path:
            raise FileNotFoundError from None
        raise
    if path and not (files or folders):
        raise FileNotFoundError
    return FolderPage(files, folders, False)
