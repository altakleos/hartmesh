"""Fail-closed ZIP construction for files presented by one run."""

from __future__ import annotations

import os
import stat
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
from deerflow.runtime.run_evidence import (
    MAX_MANIFEST_BYTES,
    RUN_EVIDENCE_MANIFEST_PATH,
    EvidenceArtifactV1,
    RunEvidenceSnapshotV1,
)

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
    if any(part in {"", ".", ".."} for part in parts):
        raise _reject()
    if any(any(char in _WINDOWS_INVALID_CHARS for char in part) or part.endswith((" ", ".")) or part.split(".", 1)[0].rstrip().casefold() in _WINDOWS_DEVICE_NAMES for part in parts):
        raise _reject()
    if any(any(unicodedata.category(char).startswith("C") and char not in _ALLOWED_FORMAT_CHARS for char in part) for part in parts):
        raise _reject()
    if any(part.casefold() in reserved or part.casefold().startswith(_EDIT_TEMP_PREFIX) for part in parts):
        raise _reject()
    if any(part.casefold().endswith(".skill") for part in parts[:-1]):
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
    member: _ArchiveMember,
    deadline: float,
    remaining_total_bytes: int,
    cancel_event: threading.Event | None,
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
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or identity != (member.initial.st_dev, member.initial.st_ino):
            raise _changed()
        if before.st_size > MAX_FILE_BYTES:
            raise _too_large(f"Each archived artifact must be at most {MAX_FILE_BYTES} bytes")
        if before.st_size > remaining_total_bytes:
            raise _too_large(f"Archived artifacts must total at most {MAX_TOTAL_BYTES} bytes")

        info = zipfile.ZipInfo(member.entry)
        info.create_system = 0
        info.compress_type = zipfile.ZIP_STORED
        remaining = before.st_size
        copied_digest = sha256()
        with archive.open(info, "w", force_zip64=False) as destination:
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

    reserved = frozenset(
        name.casefold()
        for name in {
            BROWSER_FRAMES_DIRNAME,
            TOOL_RESULTS_DIRNAME,
            *extra_reserved_dir_names,
        }
    )
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


def build_run_evidence_archive(
    outputs_dir: Path,
    virtual_paths: Iterable[str],
    *,
    snapshot: RunEvidenceSnapshotV1,
    user_data_dir: Path,
    extra_reserved_dir_names: Iterable[str] = (),
    deadline_monotonic: float | None = None,
    cancel_event: threading.Event | None = None,
) -> ArtifactArchiveResult:
    """Build a manifest-bearing archive from one immutable runtime snapshot.

    Artifact entries are hashed while their bytes are copied from the same
    open descriptors.  The manifest is created only from those copy results,
    so its size and digest claims describe exactly the bytes already written
    to the ZIP.
    """

    if not isinstance(snapshot, RunEvidenceSnapshotV1):
        raise ArtifactArchiveError("The evidence snapshot is invalid")
    paths = list(dict.fromkeys(virtual_paths))
    if tuple(paths) != snapshot.artifact_paths:
        raise ArtifactArchiveError("The evidence snapshot does not match the requested artifacts")
    deadline = time.monotonic() + BUILD_TIMEOUT_SECONDS if deadline_monotonic is None else deadline_monotonic
    members = _validated_members(
        outputs_dir,
        paths,
        user_data_dir=user_data_dir,
        extra_reserved_dir_names={
            *extra_reserved_dir_names,
            RUN_EVIDENCE_MANIFEST_PATH.split("/", 1)[0],
        },
        deadline=deadline,
        allow_empty=True,
        cancel_event=cancel_event,
    )
    members = [
        _ArchiveMember(
            path=member.path,
            entry=unicodedata.normalize("NFC", member.entry),
            initial=member.initial,
            components=member.components,
        )
        for member in members
    ]
    members.sort(key=lambda member: member.entry)

    output = tempfile.TemporaryFile("w+b")
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(output.fileno(), 0o600)
        input_bytes = 0
        copied_members: list[_CopiedArchiveMember] = []
        with zipfile.ZipFile(
            output,
            "w",
            zipfile.ZIP_STORED,
            allowZip64=False,
        ) as archive:
            for member in members:
                copied = _copy_member(
                    archive,
                    member,
                    deadline,
                    MAX_TOTAL_BYTES - input_bytes,
                    cancel_event,
                )
                copied_members.append(copied)
                input_bytes += copied.size

            manifest = snapshot.to_manifest(
                tuple(
                    EvidenceArtifactV1(
                        path=copied.entry,
                        size=copied.size,
                        sha256=copied.sha256,
                    )
                    for copied in copied_members
                )
            )
            manifest_bytes = manifest.canonical_bytes()
            if len(manifest_bytes) > MAX_MANIFEST_BYTES:
                raise _too_large(f"An evidence manifest must be at most {MAX_MANIFEST_BYTES} bytes")
            _check_deadline(deadline, cancel_event)
            info = zipfile.ZipInfo(RUN_EVIDENCE_MANIFEST_PATH)
            info.date_time = (1980, 1, 1, 0, 0, 0)
            info.create_system = 0
            info.compress_type = zipfile.ZIP_STORED
            archive.writestr(info, manifest_bytes)

        _check_deadline(deadline, cancel_event)
        size = output.tell()
        output.seek(0)
        return ArtifactArchiveResult(
            file=output,
            size=size,
            member_count=len(copied_members),
            input_bytes=input_bytes,
            manifest_digest=manifest.manifest_digest,
            bundle_ref=manifest.bundle_ref,
        )
    except Exception:
        output.close()
        raise
