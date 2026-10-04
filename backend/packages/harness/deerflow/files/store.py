"""A directory of files addressed by relative path, walked without following links.

The person's own files (:mod:`deerflow.files.manager`) and the company's
Shared area (:mod:`deerflow.files.shared`) are the same thing at this level:
a root the Gateway lists, streams, copies into and removes from, with the
sandbox able to plant links in it (read-write for the person's files, and
the Gateway must not trust the tree either way). So every path acted on is
walked one segment at a time without following a link. Reads and copies
require directory descriptors (``dir_fd``) and no-follow opens; unsupported
platforms refuse source access. What differs between
the two areas -- who may write, where the sandbox sees it, what URL serves
it, what is recorded -- lives in the two modules above this one.
"""

from __future__ import annotations

import errno
import hashlib
import logging
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

__all__ = [
    "MAX_LISTED_FILES",
    "MAX_PATH_DEPTH",
    "StoreError",
    "SafeFileAccessUnavailable",
    "StoredFile",
    "copy_into",
    "delete_under",
    "digest_and_stat",
    "list_under",
    "normalize_relative_path",
    "open_directory_source",
    "open_regular_source",
    "resolve_under",
    "sha256_of",
]

#: How deep a folder path may go. These are shallow trees; a deeper path is
#: one somebody constructed. The listing stops at the same depth so nothing
#: it shows is unaddressable.
MAX_PATH_DEPTH = 16
#: How many entries one listing returns before it says it stopped.
MAX_LISTED_FILES = 10_000
_COPY_CHUNK_BYTES = 1 << 20
_O_NOFOLLOW = getattr(os, "O_NOFOLLOW", 0)
_O_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
#: Whether the platform lets every step be taken relative to an open
#: directory, which closes the gap between checking a segment and using it.
_DIR_FD = os.open in os.supports_dir_fd and os.mkdir in os.supports_dir_fd and os.unlink in os.supports_dir_fd and os.stat in os.supports_dir_fd and _O_DIRECTORY != 0
_ATOMIC_PUBLICATION = os.link in os.supports_dir_fd and os.rmdir in os.supports_dir_fd
logger = logging.getLogger(__name__)


class StoreError(ValueError):
    """A path that cannot name a file in this area."""


class SafeFileAccessUnavailable(StoreError):
    """The host cannot provide the descriptor semantics required for safe reads."""


@dataclass(frozen=True)
class StoredFile:
    """One file, addressed by its path relative to the area's root."""

    path: str
    name: str
    size: int
    modified: float
    #: The digest of the bytes, known when they were just written; a listing
    #: does not read every file to say it.
    sha256: str | None = None


def normalize_relative_path(path: str, *, allow_empty: bool = False) -> str:
    """Reduce *path* to ``segment/segment/name`` or refuse it.

    Every segment must be a plain name (``normalize_filename``: no ``.``,
    ``..``, backslash or over-long name); hidden names are refused because the
    listing never shows them, so nothing addressable is invisible. With
    *allow_empty* the empty path names the root, for a folder argument.
    """
    # The upload package imports the runtime, whose workspace scanner uses
    # this primitive. Delay filename helpers until this module is complete.
    from deerflow.uploads.manager import normalize_filename

    if "\x00" in path:
        raise StoreError("Path contains a null byte")
    segments = [segment for segment in path.split("/") if segment != ""]
    if not segments:
        if allow_empty:
            return ""
        raise StoreError("Path is empty")
    if len(segments) > MAX_PATH_DEPTH:
        raise StoreError(f"Path is deeper than {MAX_PATH_DEPTH} folders")
    normalized: list[str] = []
    for segment in segments:
        try:
            name = normalize_filename(segment)
        except ValueError as exc:
            raise StoreError(str(exc)) from None
        if name != segment or name.startswith("."):
            raise StoreError(f"Path segment is not a plain name: {segment!r}")
        normalized.append(name)
    return "/".join(normalized)


def _entry(relative: str, metadata: os.stat_result, *, sha256: str | None = None) -> StoredFile:
    return StoredFile(path=relative, name=relative.rsplit("/", 1)[-1], size=metadata.st_size, modified=metadata.st_mtime, sha256=sha256)


def list_under(root: Path) -> tuple[list[StoredFile], bool]:
    """Every regular file under *root*, sorted by path.

    Hidden names and symlinks are skipped, never followed, and the walk stops
    at ``MAX_PATH_DEPTH`` so everything listed can be addressed. The second
    value says whether the listing stopped at ``MAX_LISTED_FILES``.
    """
    if not root.is_dir():
        return [], False
    entries: list[StoredFile] = []
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
            except StoreError:
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
            raise StoreError(f"Path goes through a link: {relative}")
    try:
        actual.resolve().relative_to(root.resolve())
    except ValueError:
        raise StoreError("Path traversal detected") from None
    return actual


def resolve_under(root: Path, path: str) -> Path:
    """The host path *path* names under *root*, refusing any symlinked segment on the way.

    The file itself need not exist; the caller decides what a missing file
    means. Raises ``StoreError`` for a path that cannot name a file here.
    """
    return _refuse_links_along(root, normalize_relative_path(path))


class _DirWalk:
    """Open folders one segment at a time, never through a link.

    Holds the descriptor of the deepest directory reached; every filesystem
    call the caller makes is relative to it, so nothing between the check and
    the use can be swapped for a link. Only on platforms with ``dir_fd``.
    """

    def __init__(self, root: Path) -> None:
        self.fd = os.open(root, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW)

    def descend(self, segment: str, *, create: bool, mode: int) -> None:
        if create:
            try:
                os.mkdir(segment, mode, dir_fd=self.fd)
            except FileExistsError:
                pass
        try:
            child = os.open(segment, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW, dir_fd=self.fd)
        except FileNotFoundError:
            raise
        except OSError as exc:
            if exc.errno in (errno.ELOOP, errno.ENOTDIR):
                raise StoreError(f"Path goes through a link or a file: {segment}") from None
            raise
        if create:
            # The umask took bits away at mkdir.
            os.fchmod(child, mode)
        os.close(self.fd)
        self.fd = child

    def close(self) -> None:
        os.close(self.fd)


def delete_under(root: Path, path: str) -> None:
    """Remove one regular file. Folders stay; a missing file raises ``FileNotFoundError``."""
    relative = normalize_relative_path(path)
    *folders, name = relative.split("/")
    if not _DIR_FD:
        actual = _refuse_links_along(root, relative)
        metadata = os.lstat(actual)
        if not stat.S_ISREG(metadata.st_mode):
            raise StoreError(f"Not a file: {path}")
        os.unlink(actual)
        return
    walk = _DirWalk(root)
    try:
        for folder in folders:
            walk.descend(folder, create=False, mode=0o777)
        metadata = os.stat(name, dir_fd=walk.fd, follow_symlinks=False)
        if not stat.S_ISREG(metadata.st_mode):
            raise StoreError(f"Not a file: {path}")
        os.unlink(name, dir_fd=walk.fd)
    finally:
        walk.close()


def open_directory_source(source: Path) -> int:
    """Open a lexical directory through no-follow ancestors; caller owns it."""
    if not _DIR_FD or not _O_NOFOLLOW:
        raise SafeFileAccessUnavailable("Safe file access requires directory descriptors and no-follow opens")
    absolute = source.absolute()
    if ".." in absolute.parts:
        raise StoreError("Path traversal detected")
    walk = _DirWalk(Path(absolute.anchor))
    try:
        for folder in absolute.parts[1:]:
            walk.descend(folder, create=False, mode=0o777)
        return os.dup(walk.fd)
    finally:
        walk.close()


def open_regular_source(source: Path) -> int:
    """Open a regular file without following a link in any path component.

    Keep this descriptor through the operation: a preflighted path alone is
    not authority to reopen it after a sandbox renames one of its parents.
    The caller owns the descriptor. Platforms without descriptor-relative
    opens fail closed rather than reverting to a check-then-open sequence.
    """
    if not _DIR_FD or not _O_NOFOLLOW:
        raise SafeFileAccessUnavailable("Safe file access requires directory descriptors and no-follow opens")
    absolute = source.absolute()
    if ".." in absolute.parts:
        raise StoreError("Path traversal detected")
    walk = _DirWalk(Path(absolute.anchor))
    try:
        for folder in absolute.parts[1:-1]:
            walk.descend(folder, create=False, mode=0o777)
        try:
            # O_NONBLOCK keeps an unexpected FIFO from hanging before fstat
            # has a chance to refuse it; it has no effect on regular files.
            fd = os.open(absolute.name, os.O_RDONLY | _O_NOFOLLOW | os.O_NONBLOCK, dir_fd=walk.fd)
        except OSError as exc:
            if exc.errno in (errno.ELOOP, errno.EISDIR, errno.ENXIO, errno.ENOTDIR):
                raise StoreError(f"Not a file: {source.name}") from None
            raise
    finally:
        walk.close()
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise StoreError(f"Not a file: {source.name}")
    except BaseException:
        os.close(fd)
        raise
    return fd


def _copy_bytes(source_fd: int, target_fd: int) -> str:
    """Copy every byte and return its SHA-256; a short write is retried by the file object rather than dropped."""
    digest = hashlib.sha256()
    with os.fdopen(source_fd, "rb", closefd=False) as reader, os.fdopen(target_fd, "wb", closefd=False) as writer:
        while chunk := reader.read(_COPY_CHUNK_BYTES):
            digest.update(chunk)
            writer.write(chunk)
        writer.flush()
        os.fsync(target_fd)
    return digest.hexdigest()


def sha256_of(source: Path) -> str:
    """The SHA-256 of *source*'s bytes; a link or anything but a regular file is refused as ``copy_into`` refuses it."""
    return digest_and_stat(source)[0]


def digest_and_stat(source: Path) -> tuple[str, os.stat_result]:
    """*source*'s SHA-256 and the metadata of the very file that was hashed, from one descriptor."""
    fd = open_regular_source(source)
    try:
        digest = hashlib.sha256()
        with os.fdopen(fd, "rb", closefd=False) as reader:
            while chunk := reader.read(_COPY_CHUNK_BYTES):
                digest.update(chunk)
        metadata = os.fstat(fd)
    finally:
        os.close(fd)
    return digest.hexdigest(), metadata


@contextmanager
def _staged_copy(parent_fd: int) -> Iterator[tuple[int, int]]:
    """Own hidden staging through completion, protected from other sandbox users.

    The private directory prevents replacement of the hard-link source while
    the surrounding personal-files directory remains sandbox-writable. All
    operations remain bound to descriptors if a parent is renamed meanwhile.
    """
    while True:
        staging = f".copy-{uuid4().hex}"
        try:
            os.mkdir(staging, 0o700, dir_fd=parent_fd)
            break
        except FileExistsError:
            continue
    stage_fd = target_fd = None
    try:
        stage_fd = os.open(staging, os.O_RDONLY | _O_DIRECTORY | _O_NOFOLLOW, dir_fd=parent_fd)
        metadata = os.fstat(stage_fd)
        if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
            raise StoreError("Private staging directory was replaced or has unsafe permissions")
        target_fd = os.open("data", os.O_WRONLY | os.O_CREAT | os.O_EXCL | _O_NOFOLLOW, 0o600, dir_fd=stage_fd)
        yield stage_fd, target_fd
    finally:
        if target_fd is not None:
            os.close(target_fd)
            try:
                os.unlink("data", dir_fd=stage_fd)
            except OSError:
                logger.warning("Could not remove staged file in %s", staging, exc_info=True)
        if stage_fd is not None:
            os.close(stage_fd)
        try:
            os.rmdir(staging, dir_fd=parent_fd)
        except OSError:
            logger.warning("Could not remove staging directory %s", staging, exc_info=True)


def _publish_exclusive(stage_fd: int, parent_fd: int, safe_name: str) -> str:
    """Atomically give a finished copy its next free name, without overwriting."""
    from deerflow.uploads.manager import claim_unique_filename

    seen = set(os.listdir(parent_fd))
    while True:
        candidate = claim_unique_filename(safe_name, seen)
        try:
            os.link("data", candidate, src_dir_fd=stage_fd, dst_dir_fd=parent_fd, follow_symlinks=False)
            return candidate
        except FileExistsError:
            continue
        except OSError as exc:
            if exc.errno == errno.EEXIST:
                continue
            raise


def copy_into(root: Path, source: Path, *, name: str, folder: str | None, folder_mode: int, file_mode: int) -> StoredFile:
    """Copy *source*'s exact bytes under *root* as *name* in *folder*.

    A name that already exists is kept: the copy takes the next free
    ``_N`` suffix, never overwriting. Hidden staging holds the bytes until
    copying finishes; an exclusive hard link publishes them atomically, so
    racing copies both land and readers never see partial content. A failed
    copy removes its staging. *source* is opened without following a link and
    must be a regular file; the folder is walked without following a link.
    *folder_mode* and *file_mode* are what the area's readers and writers
    need: the person's files are written by the sandbox too, the Shared area
    only by the Gateway.
    """
    from deerflow.uploads.manager import normalize_filename

    relative_folder = normalize_relative_path(folder or "", allow_empty=True)
    try:
        safe_name = normalize_filename(name)
    except ValueError as exc:
        raise StoreError(str(exc)) from None
    if safe_name.startswith("."):
        raise StoreError(f"Name is hidden: {name!r}")
    if not _ATOMIC_PUBLICATION:
        raise StoreError("Atomic file publication requires descriptor-relative hard links")

    folders = relative_folder.split("/") if relative_folder else []
    relative_dir = relative_folder + "/" if relative_folder else ""

    source_fd = open_regular_source(source)
    try:
        walk = _DirWalk(root)
        try:
            for segment in folders:
                walk.descend(segment, create=True, mode=folder_mode)
            with _staged_copy(walk.fd) as (stage_fd, target_fd):
                sha256 = _copy_bytes(source_fd, target_fd)
                os.fchmod(target_fd, file_mode)
                metadata = os.fstat(target_fd)
                candidate = _publish_exclusive(stage_fd, walk.fd, safe_name)
        finally:
            walk.close()
    finally:
        os.close(source_fd)
    return _entry(f"{relative_dir}{candidate}", metadata, sha256=sha256)
