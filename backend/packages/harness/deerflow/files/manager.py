"""The person's own files, kept across conversations.

Pure filesystem logic with no HTTP dependencies. The directory is
``{base_dir}/users/{user_id}/files`` (``Paths.user_files_dir``), mounted
read-write at ``/mnt/user-data/files`` in every sandbox of that user, so a
file kept in one conversation is on the disk of the next. The Gateway's files
router and the sandbox both write here; the person reaches it through the
Files page. Uploads and outputs stay per conversation until the person keeps
one ("Save to My files"), which copies the exact bytes.

The sandbox writes here as its own uid and can plant links, so every path the
Gateway acts on is walked one segment at a time without following a link:
through directory descriptors (``dir_fd``) where the platform has them, and
otherwise by refusing any segment that ``lstat`` reports as a link.
"""

from __future__ import annotations

import errno
import os
import shutil
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
#: deeper one is a path somebody constructed. The listing stops at the same
#: depth so nothing it shows is unaddressable.
MAX_PATH_DEPTH = 16
#: How many entries one listing returns before it says it stopped.
MAX_LISTED_FILES = 10_000
_COPY_CHUNK_BYTES = 1 << 20
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
#: Whether the platform lets every step be taken relative to an open
#: directory, which closes the gap between checking a segment and using it.
_DIR_FD = os.open in os.supports_dir_fd and os.mkdir in os.supports_dir_fd and os.unlink in os.supports_dir_fd and os.stat in os.supports_dir_fd and _O_DIRECTORY != 0


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


def _entry(relative: str, metadata: os.stat_result) -> UserFile:
    return UserFile(path=relative, name=relative.rsplit("/", 1)[-1], size=metadata.st_size, modified=metadata.st_mtime)


def list_user_files(user_id: str) -> tuple[list[UserFile], bool]:
    """Every regular file under the person's root, sorted by path.

    Hidden names and symlinks are skipped, never followed, and the walk stops
    at ``MAX_PATH_DEPTH`` so everything listed can be addressed. The second
    value says whether the listing stopped at ``MAX_LISTED_FILES``.
    """
    root = _files_root(user_id)
    if not root.is_dir():
        return [], False
    entries: list[UserFile] = []
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        depth = len(Path(directory).relative_to(root).parts)
        if depth >= MAX_PATH_DEPTH:
            dirnames[:] = []
        else:
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
            relative = actual.relative_to(root).as_posix()
            try:
                # What the listing shows, the address rules must accept, or the
                # person sees a row they can neither open nor remove.
                normalize_relative_path(relative)
            except UserFileError:
                continue
            if len(entries) >= MAX_LISTED_FILES:
                entries.sort(key=lambda entry: entry.path)
                return entries, True
            entries.append(_entry(relative, metadata))
    entries.sort(key=lambda entry: entry.path)
    return entries, False


def _refuse_links_along(root: Path, relative: str) -> Path:
    """The host path *relative* names under *root*, refusing any segment that is a link."""
    actual = root
    for segment in relative.split("/") if relative else []:
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


def resolve_user_file(user_id: str, path: str) -> Path:
    """The host path *path* names, refusing any symlinked segment on the way.

    The file itself need not exist; the caller decides what a missing file
    means. Raises ``UserFileError`` for a path that cannot name a file here.
    """
    return _refuse_links_along(_files_root(user_id), normalize_relative_path(path))


class _DirWalk:
    """Open the person's folders one segment at a time, never through a link.

    Holds the descriptor of the deepest directory reached; every filesystem
    call the caller makes is relative to it, so nothing between the check and
    the use can be swapped for a link. Only on platforms with ``dir_fd``.
    """

    def __init__(self, root: Path) -> None:
        self.fd = os.open(root, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW)

    def descend(self, segment: str, *, create: bool) -> None:
        if create:
            try:
                os.mkdir(segment, 0o777, dir_fd=self.fd)
            except FileExistsError:
                pass
        try:
            child = os.open(segment, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW, dir_fd=self.fd)
        except FileNotFoundError:
            raise
        except OSError as exc:
            if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                raise UserFileError(f"Path goes through a link or a file: {segment}") from None
            raise
        if create:
            # The umask took bits away at mkdir; the sandbox writes here as its own uid.
            os.fchmod(child, 0o777)
        os.close(self.fd)
        self.fd = child

    def close(self) -> None:
        os.close(self.fd)


def delete_user_file(user_id: str, path: str) -> None:
    """Remove one regular file. Folders stay; a missing file raises ``FileNotFoundError``."""
    relative = normalize_relative_path(path)
    root = _files_root(user_id)
    *folders, name = relative.split("/")
    if not _DIR_FD:
        actual = _refuse_links_along(root, relative)
        metadata = os.lstat(actual)
        if not stat.S_ISREG(metadata.st_mode):
            raise UserFileError(f"Not a file: {path}")
        os.unlink(actual)
        return
    walk = _DirWalk(root)
    try:
        for folder in folders:
            walk.descend(folder, create=False)
        metadata = os.stat(name, dir_fd=walk.fd, follow_symlinks=False)
        if not stat.S_ISREG(metadata.st_mode):
            raise UserFileError(f"Not a file: {path}")
        os.unlink(name, dir_fd=walk.fd)
    finally:
        walk.close()


def _open_regular_source(source: Path) -> int:
    """A descriptor on *source* itself, refusing a link and anything but a regular file."""
    try:
        fd = os.open(source, os.O_RDONLY | _O_NOFOLLOW)
    except OSError as exc:
        if exc.errno == errno.ELOOP:
            raise UserFileError(f"Not a file: {source.name}") from None
        raise
    try:
        metadata = os.fstat(fd)
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise UserFileError(f"Not a file: {source.name}")
    except BaseException:
        os.close(fd)
        raise
    return fd


def _copy_bytes(source_fd: int, target_fd: int) -> None:
    """Copy every byte; a short write is retried by the file object rather than dropped."""
    with os.fdopen(source_fd, "rb", closefd=False) as reader, os.fdopen(target_fd, "wb", closefd=False) as writer:
        shutil.copyfileobj(reader, writer, _COPY_CHUNK_BYTES)
        writer.flush()
        os.fsync(target_fd)


def _create_exclusive(directory: Path, safe_name: str, *, dir_fd: int | None) -> tuple[str, int]:
    """Claim the next free name in *directory* with an exclusive create."""
    seen = set(os.listdir(dir_fd if dir_fd is not None else directory))
    while True:
        candidate = claim_unique_filename(safe_name, seen)
        try:
            if dir_fd is not None:
                return candidate, os.open(candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666, dir_fd=dir_fd)
            return candidate, os.open(directory / candidate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o666)
        except FileExistsError:
            continue
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                continue
            raise


def keep_file(user_id: str, source: Path, *, name: str, folder: str | None = None) -> UserFile:
    """Copy *source*'s exact bytes into the person's files as *name* under *folder*.

    A name that already exists is kept: the copy takes the next free
    ``_N`` suffix, never overwriting. The name is claimed with an exclusive
    create so two keeps racing for one name both land; a copy that fails
    leaves nothing behind. *source* is opened without following a link and
    must be a regular file; the folder is walked without following a link.
    """
    relative_folder = normalize_relative_path(folder or "", allow_empty=True)
    try:
        safe_name = normalize_filename(name)
    except ValueError as exc:
        raise UserFileError(str(exc)) from None
    if safe_name.startswith("."):
        raise UserFileError(f"Name is hidden: {name!r}")

    root = get_paths().ensure_user_files_dir(user_id)
    folders = relative_folder.split("/") if relative_folder else []
    relative_dir = relative_folder + "/" if relative_folder else ""

    source_fd = _open_regular_source(source)
    try:
        if _DIR_FD:
            walk = _DirWalk(root)
            try:
                for segment in folders:
                    walk.descend(segment, create=True)
                candidate, target_fd = _create_exclusive(root / relative_folder, safe_name, dir_fd=walk.fd)
                try:
                    try:
                        _copy_bytes(source_fd, target_fd)
                        os.fchmod(target_fd, 0o666)
                        metadata = os.fstat(target_fd)
                    finally:
                        os.close(target_fd)
                except BaseException:
                    try:
                        os.unlink(candidate, dir_fd=walk.fd)
                    except OSError:
                        pass
                    raise
            finally:
                walk.close()
        else:
            directory = root
            for segment in folders:
                directory = directory / segment
                try:
                    metadata = os.lstat(directory)
                except FileNotFoundError:
                    directory.mkdir()
                    directory.chmod(0o777)
                    continue
                if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                    raise UserFileError(f"Path goes through a link or a file: {segment}")
            _refuse_links_along(root, relative_folder)
            candidate, target_fd = _create_exclusive(directory, safe_name, dir_fd=None)
            try:
                try:
                    _copy_bytes(source_fd, target_fd)
                    os.fchmod(target_fd, 0o666)
                    metadata = os.fstat(target_fd)
                finally:
                    os.close(target_fd)
            except BaseException:
                try:
                    os.unlink(directory / candidate)
                except OSError:
                    pass
                raise
    finally:
        os.close(source_fd)
    return _entry(f"{relative_dir}{candidate}", metadata)
