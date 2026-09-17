"""The person's own files, kept across conversations.

Pure filesystem logic with no HTTP dependencies. The directory is
``{base_dir}/users/{user_id}/files`` (``Paths.user_files_dir``), mounted
read-write at ``/mnt/user-data/files`` in every sandbox of that user, so a
file kept in one conversation is on the disk of the next. The Gateway's files
router and the sandbox both write here; the person reaches it through the
Files page. Uploads and outputs stay per conversation until the person keeps
one ("Keep in my files", "Save to my files"), which copies the exact bytes.
"""

from __future__ import annotations

import errno
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from deerflow.config.paths import USER_FILES_VIRTUAL_PREFIX, get_paths
from deerflow.uploads.manager import claim_unique_filename, normalize_filename

__all__ = [
    "MAX_LISTED_FILES",
    "MAX_PATH_DEPTH",
    "UserFile",
    "UserFileError",
    "delete_user_file",
    "keep_file",
    "list_user_files",
    "normalize_relative_path",
    "resolve_user_file",
]

#: How deep a folder path may go. A person's files are a shallow tree; a
#: deeper one is a path somebody constructed.
MAX_PATH_DEPTH = 16
#: How many entries one listing returns before it says it stopped.
MAX_LISTED_FILES = 10_000
_COPY_CHUNK_BYTES = 1 << 20


class UserFileError(ValueError):
    """A path that cannot name a file in the person's files."""


@dataclass(frozen=True)
class UserFile:
    """One file, addressed by its path relative to the person's files root."""

    path: str
    name: str
    size: int
    modified: float

    @property
    def virtual_path(self) -> str:
        """Where the sandbox sees this file."""
        return f"{USER_FILES_VIRTUAL_PREFIX}/{self.path}"

    @property
    def url(self) -> str:
        """Where the browser fetches this file (the Gateway's files route)."""
        return f"/api/files/{quote(self.path, safe='/')}"


def normalize_relative_path(path: str, *, allow_empty: bool = False) -> str:
    """Reduce *path* to ``segment/segment/name`` or refuse it.

    Every segment must be a plain name (``normalize_filename``: no ``.``,
    ``..``, backslash or over-long name); hidden names are refused because the
    listing never shows them, so nothing addressable is invisible. With
    *allow_empty* the empty path names the root, for a folder argument.
    """
    if "\x00" in path:
        raise UserFileError("Path contains a null byte")
    segments = [segment for segment in path.split("/") if segment != ""]
    if not segments:
        if allow_empty:
            return ""
        raise UserFileError("Path is empty")
    if len(segments) > MAX_PATH_DEPTH:
        raise UserFileError(f"Path is deeper than {MAX_PATH_DEPTH} folders")
    normalized: list[str] = []
    for segment in segments:
        try:
            name = normalize_filename(segment)
        except ValueError as exc:
            raise UserFileError(str(exc)) from None
        if name != segment or name.startswith("."):
            raise UserFileError(f"Path segment is not a plain name: {segment!r}")
        normalized.append(name)
    return "/".join(normalized)


def _files_root(user_id: str) -> Path:
    return get_paths().user_files_dir(user_id)


def _entry(root: Path, actual: Path, metadata: os.stat_result) -> UserFile:
    return UserFile(
        path=actual.relative_to(root).as_posix(),
        name=actual.name,
        size=metadata.st_size,
        modified=metadata.st_mtime,
    )


def list_user_files(user_id: str) -> tuple[list[UserFile], bool]:
    """Every regular file under the person's root, sorted by path.

    Hidden names and symlinks are skipped, never followed. The second value
    says whether the listing stopped at ``MAX_LISTED_FILES``.
    """
    root = _files_root(user_id)
    if not root.is_dir():
        return [], False
    entries: list[UserFile] = []
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames[:] = sorted(name for name in dirnames if not name.startswith("."))
        for filename in sorted(filenames):
            if filename.startswith("."):
                continue
            actual = Path(directory) / filename
            try:
                metadata = os.lstat(actual)
            except OSError:
                continue
            if not stat.S_ISREG(metadata.st_mode):
                continue
            if len(entries) >= MAX_LISTED_FILES:
                return entries, True
            entries.append(_entry(root, actual, metadata))
    entries.sort(key=lambda entry: entry.path)
    return entries, False


def resolve_user_file(user_id: str, path: str) -> Path:
    """The host path *path* names, refusing any symlinked segment on the way.

    The file itself need not exist; the caller decides what a missing file
    means. Raises ``UserFileError`` for a path that cannot name a file here.
    """
    relative = normalize_relative_path(path)
    root = _files_root(user_id)
    actual = root
    for segment in relative.split("/"):
        actual = actual / segment
        try:
            metadata = os.lstat(actual)
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode):
            raise UserFileError(f"Path goes through a link: {relative}")
    try:
        actual.resolve().relative_to(root.resolve())
    except ValueError:
        raise UserFileError("Path traversal detected") from None
    return actual


def delete_user_file(user_id: str, path: str) -> None:
    """Remove one regular file. Folders stay; a missing file raises ``FileNotFoundError``."""
    actual = resolve_user_file(user_id, path)
    metadata = os.lstat(actual)
    if not stat.S_ISREG(metadata.st_mode):
        raise UserFileError(f"Not a file: {path}")
    os.unlink(actual)


def _copy_bytes(source: Path, fd: int) -> None:
    with open(source, "rb") as handle:
        while chunk := handle.read(_COPY_CHUNK_BYTES):
            os.write(fd, chunk)
    os.fsync(fd)


def _ensure_sandbox_writable_dir(directory: Path) -> None:
    """Create *directory* the way the thread directories are created (0o777)."""
    directory.mkdir(parents=True, exist_ok=True)
    directory.chmod(0o777)


def keep_file(user_id: str, source: Path, *, name: str, folder: str | None = None) -> UserFile:
    """Copy *source*'s exact bytes into the person's files as *name* under *folder*.

    A name that already exists is kept: the copy takes the next free
    ``_N`` suffix, never overwriting. The name is claimed with an exclusive
    create so two keeps racing for one name both land; a copy that fails
    leaves nothing behind.
    """
    relative_folder = normalize_relative_path(folder or "", allow_empty=True)
    try:
        safe_name = normalize_filename(name)
    except ValueError as exc:
        raise UserFileError(str(exc)) from None
    if safe_name.startswith("."):
        raise UserFileError(f"Name is hidden: {name!r}")

    root = _files_root(user_id)
    directory = root / relative_folder if relative_folder else root
    _ensure_sandbox_writable_dir(root)
    if relative_folder:
        for depth in range(1, relative_folder.count("/") + 2):
            _ensure_sandbox_writable_dir(root.joinpath(*relative_folder.split("/")[:depth]))

    seen = {entry.name for entry in os.scandir(directory)}
    while True:
        candidate = claim_unique_filename(safe_name, seen)
        target = directory / candidate
        try:
            fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        except FileExistsError:
            continue
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                continue
            raise
        try:
            try:
                _copy_bytes(source, fd)
            finally:
                os.close(fd)
            # The umask took bits away at create; the sandbox rewrites the
            # copy as its own uid, like an upload.
            os.chmod(target, 0o666)
        except BaseException:
            try:
                os.unlink(target)
            except OSError:
                pass
            raise
        return _entry(root, target, os.lstat(target))
