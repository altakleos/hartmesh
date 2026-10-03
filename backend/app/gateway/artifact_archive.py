"""Fail-closed ZIP construction for files presented by one run."""

from __future__ import annotations

import os
import stat
import sys
import tempfile
import threading
import time
import unicodedata
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO

from deerflow.constants import BROWSER_FRAMES_DIRNAME, TOOL_RESULTS_DIRNAME

_VIRTUAL_PREFIX = "mnt/user-data/outputs/"
_EDIT_TEMP_PREFIX = ".artifact-edit-"
_ALLOWED_FORMAT_CHARS = frozenset({"\u200c", "\u200d"})
_WINDOWS_INVALID_CHARS = frozenset('<>:"|?*')
_WINDOWS_DEVICE_NAMES = frozenset({"con", "prn", "aux", "nul"} | {f"com{number}" for number in range(1, 10)} | {f"lpt{number}" for number in range(1, 10)})
MAX_FILES = 50
MAX_FILE_BYTES = 50 * 1024 * 1024
MAX_TOTAL_BYTES = 100 * 1024 * 1024
MAX_ENTRY_BYTES = 1024
BUILD_TIMEOUT_SECONDS = 60.0
_CHUNK_BYTES = 1024 * 1024


class ArtifactArchiveError(ValueError):
    def __init__(
        self,
        detail: str,
        status_code: int = 409,
        code: str = "artifact_unsafe",
    ) -> None:
        super().__init__(detail)
        self.detail = detail
        self.status_code = status_code
        self.code = code


@dataclass(frozen=True)
class ArtifactArchiveResult:
    file: BinaryIO
    size: int
    member_count: int
    input_bytes: int
    manifest_digest: str | None = None
    bundle_ref: str | None = None


@dataclass(frozen=True)
class _ArchiveMember:
    path: Path
    entry: str
    initial: os.stat_result
    components: tuple[tuple[Path, int, int], ...]

    @property
    def identity(self) -> tuple[int, int]:
        return self.initial.st_dev, self.initial.st_ino


@dataclass(frozen=True)
class _VettedFile:
    """A file a caller found and checked itself, as ``copy_file`` copies it."""

    path: Path
    entry: str
    identity: tuple[int, int]
    components: tuple[tuple[Path, int, int], ...]


@dataclass(frozen=True)
class _CopiedArchiveMember:
    entry: str
    size: int
    sha256: str


def _reject() -> ArtifactArchiveError:
    return ArtifactArchiveError("The files listed by this response are not available for archive download")


def _changed() -> ArtifactArchiveError:
    return ArtifactArchiveError(
        "The files listed by this response are not available for archive download",
        code="artifact_changed",
    )


def _too_large(detail: str) -> ArtifactArchiveError:
    return ArtifactArchiveError(detail, 413, "bundle_limit_exceeded")


def _check_deadline(
    deadline: float,
    cancel_event: threading.Event | None = None,
) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise ArtifactArchiveError(
            "Artifact archive creation was cancelled",
            499,
            "bundle_generation_cancelled",
        )
    if time.monotonic() > deadline:
        raise ArtifactArchiveError(
            "Artifact archive creation timed out",
            503,
            "bundle_generation_timeout",
        )


def _is_link_like(path: Path, metadata: os.stat_result) -> bool:
    return stat.S_ISLNK(metadata.st_mode) or path.is_junction()


def unsafe_entry_parts(parts: list[str], reserved: frozenset[str]) -> bool:
    """Whether a path, split into its parts, may not be written to an archive.

    Refused: an empty, ``.`` or ``..`` part; a name Windows cannot hold (its
    reserved characters, a trailing space or dot, a device name); a control
    or format character other than the joiners; a reserved directory or an
    in-progress edit's temporary name; a path inside a ``.skill`` directory.
    ``reserved`` is casefolded.
    """
    if any(part in {"", ".", ".."} for part in parts):
        return True
    if any(any(char in _WINDOWS_INVALID_CHARS for char in part) or part.endswith((" ", ".")) or part.split(".", 1)[0].rstrip().casefold() in _WINDOWS_DEVICE_NAMES for part in parts):
        return True
    if any(any(unicodedata.category(char).startswith("C") and char not in _ALLOWED_FORMAT_CHARS for char in part) for part in parts):
        return True
    if any(part.casefold() in reserved or part.casefold().startswith(_EDIT_TEMP_PREFIX) for part in parts):
        return True
    return any(part.casefold().endswith(".skill") for part in parts[:-1])


def reserved_dir_names(extra: Iterable[str] = ()) -> frozenset[str]:
    """The directories no archive includes: tool-result spill, browser frames, and ``extra``; casefolded."""
    return frozenset(name.casefold() for name in {BROWSER_FRAMES_DIRNAME, TOOL_RESULTS_DIRNAME, *extra})


def _member(
    root: Path,
    virtual_path: str,
    reserved: frozenset[str],
    deadline: float,
    root_components: tuple[tuple[Path, int, int], ...],
    cancel_event: threading.Event | None,
) -> _ArchiveMember:
    _check_deadline(deadline, cancel_event)
    if not virtual_path or virtual_path.startswith("//") or "\\" in virtual_path or "\x00" in virtual_path:
        raise _reject()
    stripped = virtual_path.removeprefix("/")
    if not stripped.startswith(_VIRTUAL_PREFIX):
        raise _reject()

    parts = stripped.removeprefix(_VIRTUAL_PREFIX).split("/")
    if unsafe_entry_parts(parts, reserved):
        raise _reject()

    entry = "/".join(parts)
    if len(entry.encode()) > MAX_ENTRY_BYTES:
        raise _too_large("An artifact path is too long to include in an archive")

    candidate = root.joinpath(*parts)
    current = root
    components = list(root_components)
    try:
        for part in parts:
            _check_deadline(deadline, cancel_event)
            current /= part
            metadata = os.lstat(current)
            if _is_link_like(current, metadata):
                raise _reject()
            components.append((current, metadata.st_dev, metadata.st_ino))
        initial = os.lstat(candidate)
        if not stat.S_ISREG(initial.st_mode) or initial.st_nlink != 1:
            raise _reject()
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except ArtifactArchiveError:
        raise
    except (OSError, ValueError) as exc:
        raise _reject() from exc
    _check_deadline(deadline, cancel_event)
    return _ArchiveMember(resolved, entry, initial, tuple(components))


def _hash_descriptor(
    descriptor: int,
    size: int,
    deadline: float,
    cancel_event: threading.Event | None,
) -> bytes:
    digest = sha256()
    remaining = size
    try:
        os.lseek(descriptor, 0, os.SEEK_SET)
        while remaining:
            _check_deadline(deadline, cancel_event)
            chunk = os.read(descriptor, min(_CHUNK_BYTES, remaining))
            if not chunk:
                raise _changed()
            digest.update(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise _changed()
    except OSError as exc:
        raise _changed() from exc
    return digest.digest()


def _copy_member(
    archive: zipfile.ZipFile,
    member: _ArchiveMember | _VettedFile,
    deadline: float,
    remaining_total_bytes: int,
    cancel_event: threading.Event | None,
    max_file_bytes: int | None = None,
    expected_size: int | None = None,
) -> _CopiedArchiveMember:
    _check_deadline(deadline, cancel_event)
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(member.path, flags)
    except OSError as exc:
        raise _changed() from exc

    try:
        _check_deadline(deadline, cancel_event)
        before = os.fstat(descriptor)
        identity = (before.st_dev, before.st_ino)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or identity != member.identity:
            raise _changed()
        if expected_size is not None and before.st_size != expected_size:
            raise _changed()
        file_limit = MAX_FILE_BYTES if max_file_bytes is None else max_file_bytes
        if before.st_size > file_limit:
            raise _too_large(f"Each archived artifact must be at most {file_limit} bytes")
        if before.st_size > remaining_total_bytes:
            raise _too_large(f"Archived artifacts must total at most {MAX_TOTAL_BYTES} bytes")

        info = zipfile.ZipInfo(member.entry)
        info.create_system = 0
        info.compress_type = zipfile.ZIP_STORED
        remaining = before.st_size
        copied_digest = sha256()
        with archive.open(info, "w", force_zip64=archive._allowZip64 and before.st_size >= zipfile.ZIP64_LIMIT) as destination:
            while remaining:
                _check_deadline(deadline, cancel_event)
                chunk = os.read(descriptor, min(_CHUNK_BYTES, remaining))
                if not chunk:
                    raise _changed()
                destination.write(chunk)
                copied_digest.update(chunk)
                remaining -= len(chunk)
            if os.read(descriptor, 1):
                raise _changed()

        _check_deadline(deadline, cancel_event)
        after = os.fstat(descriptor)
        if after.st_nlink != 1 or (after.st_dev, after.st_ino) != identity or (after.st_size, after.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
            raise _changed()
        if (
            _hash_descriptor(
                descriptor,
                before.st_size,
                deadline,
                cancel_event,
            )
            != copied_digest.digest()
        ):
            raise _changed()
        after_verification = os.fstat(descriptor)
        if after_verification.st_nlink != 1 or (after_verification.st_dev, after_verification.st_ino) != identity or (after_verification.st_size, after_verification.st_mtime_ns) != (before.st_size, before.st_mtime_ns):
            raise _changed()
        try:
            for component, device, inode in member.components:
                current = os.lstat(component)
                if _is_link_like(component, current) or (current.st_dev, current.st_ino) != (device, inode):
                    raise _changed()
        except OSError as exc:
            raise _changed() from exc
        return _CopiedArchiveMember(
            entry=member.entry,
            size=before.st_size,
            sha256=copied_digest.hexdigest(),
        )
    except ArtifactArchiveError:
        raise
    except OSError as exc:
        raise _changed() from exc
    finally:
        os.close(descriptor)


def _validated_members(
    outputs_dir: Path,
    virtual_paths: Iterable[str],
    *,
    user_data_dir: Path,
    extra_reserved_dir_names: Iterable[str],
    deadline: float,
    allow_empty: bool,
    cancel_event: threading.Event | None,
) -> list[_ArchiveMember]:
    try:
        if outputs_dir.parent != user_data_dir:
            raise _reject()
        user_data_metadata = os.lstat(user_data_dir)
        outputs_metadata = os.lstat(outputs_dir)
        if _is_link_like(user_data_dir, user_data_metadata) or not stat.S_ISDIR(user_data_metadata.st_mode) or _is_link_like(outputs_dir, outputs_metadata) or not stat.S_ISDIR(outputs_metadata.st_mode):
            raise _reject()
        user_data_root = user_data_dir.resolve(strict=True)
        root = outputs_dir.resolve(strict=True)
        if root.parent != user_data_root:
            raise _reject()
    except ArtifactArchiveError:
        raise
    except OSError as exc:
        raise _reject() from exc
    _check_deadline(deadline, cancel_event)

    root_components = (
        (user_data_dir, user_data_metadata.st_dev, user_data_metadata.st_ino),
        (outputs_dir, outputs_metadata.st_dev, outputs_metadata.st_ino),
    )

    paths = list(dict.fromkeys(virtual_paths))
    if not paths and not allow_empty:
        raise _reject()
    if len(paths) > MAX_FILES:
        raise _too_large(f"An artifact archive can contain at most {MAX_FILES} files")

    reserved = reserved_dir_names(extra_reserved_dir_names)
    members = [
        _member(
            root,
            path,
            reserved,
            deadline,
            root_components,
            cancel_event,
        )
        for path in paths
    ]
    collision_keys = [unicodedata.normalize("NFC", member.entry).casefold() for member in members]
    if len(collision_keys) != len(set(collision_keys)):
        raise _reject()

    sizes = [member.initial.st_size for member in members]
    if any(size > MAX_FILE_BYTES for size in sizes):
        raise _too_large(f"Each archived artifact must be at most {MAX_FILE_BYTES} bytes")
    if sum(sizes) > MAX_TOTAL_BYTES:
        raise _too_large(f"Archived artifacts must total at most {MAX_TOTAL_BYTES} bytes")
    return members


@dataclass(frozen=True)
class CopiedFile:
    """One file as written to an archive: its entry, its size and the SHA-256 of the bytes written."""

    entry: str
    size: int
    sha256: str


def copy_file(
    archive: zipfile.ZipFile,
    path: Path,
    entry: str,
    *,
    identity: tuple[int, int],
    components: tuple[tuple[Path, int, int], ...],
    cancel_event: threading.Event | None = None,
    expected_size: int | None = None,
    expected_sha256: str | None = None,
) -> CopiedFile:
    """Copy one regular file the caller already vetted, with the same guards as an artifact archive and no size limit.

    ``identity`` is the file's ``(device, inode)`` when it was found and
    ``components`` the ``(path, device, inode)`` of every directory from the
    archive's root down to it: a file that changed, was replaced, or whose
    directory was swapped for a link while it was copied raises
    ``ArtifactArchiveError`` with code ``artifact_changed`` (the ``OSError``
    behind it, if any, as its cause).

    A copy that raises leaves nothing behind: whatever it wrote is taken out
    of the archive again, so the archive holds only files that were copied
    whole and verified. That needs ``archive`` written to a seekable file. A
    file over ``zipfile.ZIP64_LIMIT`` needs an archive that allows ZIP64.
    Optional expected size/hash bind immutable source records to the copied
    bytes; a mismatch uses the same rollback and ``artifact_changed`` error.
    """
    start = archive.start_dir
    try:
        copied = _copy_member(
            archive,
            _VettedFile(path=path, entry=entry, identity=identity, components=components),
            float("inf"),
            sys.maxsize,
            cancel_event,
            max_file_bytes=sys.maxsize,
            expected_size=expected_size,
        )
        if (expected_size is not None and copied.size != expected_size) or (expected_sha256 is not None and copied.sha256 != expected_sha256):
            raise _changed()
    except BaseException:
        _take_back(archive, start)
        raise
    return CopiedFile(entry=copied.entry, size=copied.size, sha256=copied.sha256)


def _take_back(archive: zipfile.ZipFile, offset: int) -> None:
    """Remove every entry written at or after ``offset``, as if it had never been added.

    ``zipfile`` commits an entry when its writer closes, even when the copy
    into it raised, and has no way to remove one; the entries after
    ``offset`` are dropped from the central directory it will write, and the
    file is cut back to where they began.
    """
    dropped = [info for info in archive.filelist if info.header_offset >= offset]
    for info in dropped:
        archive.filelist.remove(info)
        if archive.NameToInfo.get(info.filename) is info:
            del archive.NameToInfo[info.filename]
    if archive.fp is not None:
        archive.fp.seek(offset)
        archive.fp.truncate()
    archive.start_dir = offset


def build_artifact_archive(
    outputs_dir: Path,
    virtual_paths: Iterable[str],
    *,
    user_data_dir: Path,
    extra_reserved_dir_names: Iterable[str] = (),
    cancel_event: threading.Event | None = None,
) -> ArtifactArchiveResult:
    deadline = time.monotonic() + BUILD_TIMEOUT_SECONDS
    members = _validated_members(
        outputs_dir,
        virtual_paths,
        user_data_dir=user_data_dir,
        extra_reserved_dir_names=extra_reserved_dir_names,
        deadline=deadline,
        allow_empty=False,
        cancel_event=cancel_event,
    )

    _check_deadline(deadline, cancel_event)
    output = tempfile.TemporaryFile("w+b")
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(output.fileno(), 0o600)
        input_bytes = 0
        with zipfile.ZipFile(output, "w", zipfile.ZIP_STORED, allowZip64=False) as archive:
            for member in members:
                copied = _copy_member(
                    archive,
                    member,
                    deadline,
                    MAX_TOTAL_BYTES - input_bytes,
                    cancel_event,
                )
                input_bytes += copied.size
        _check_deadline(deadline, cancel_event)
        size = output.tell()
        output.seek(0)
        return ArtifactArchiveResult(output, size, len(members), input_bytes)
    except Exception:
        output.close()
        raise
