"""Bounded immutable snapshots of skill trees accepted for one invocation.

The module is the sole owner of skill-tree copying, hashing, publication,
verification, leases, and cleanup. Callers receive snapshot-backed ``Skill``
records and never need to reason about mutable registry paths.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import threading
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from deerflow.config.paths import get_paths
from deerflow.skills.parser import parse_skill_file
from deerflow.skills.types import SKILL_MD_FILE, Skill, SkillCategory

if TYPE_CHECKING:
    from deerflow.runtime.skill_projection import SkillProjectionEvidence

SNAPSHOT_CONTAINER_NAMESPACE = ".accepted"


@dataclass(frozen=True, slots=True)
class SkillSnapshotLimits:
    """Hard bounds applied to every accepted invocation's skill material."""

    max_skills: int = 64
    max_files_per_skill: int = 256
    max_total_files: int = 2_048
    max_file_bytes: int = 2 * 1024 * 1024
    max_skill_bytes: int = 8 * 1024 * 1024
    max_total_bytes: int = 32 * 1024 * 1024
    max_relative_path_bytes: int = 512


DEFAULT_SKILL_SNAPSHOT_LIMITS = SkillSnapshotLimits()


class SkillSnapshotError(RuntimeError):
    """Safe fail-closed snapshot error with a stable bounded reason code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class SkillSnapshotProjection:
    """Persistable bounded evidence derived from copied execution bytes."""

    name: str
    category: str
    relative_path: str
    manifest_digest: str
    content_digest: str
    file_count: int
    total_bytes: int

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category,
            "relative_path": self.relative_path,
            "manifest_digest": self.manifest_digest,
            "content_digest": self.content_digest,
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
        }


_leases_lock = threading.RLock()
_lease_counts: dict[Path, int] = {}
_active_views_lock = threading.RLock()
_active_view_bindings: dict[Path, tuple[str, int, str | None]] = {}
_SNAPSHOT_ID_PATTERN = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class _CapturedFile:
    relative_path: Path
    data: bytes
    executable: bool


def _remove_tree(path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if path.is_symlink() or not path.is_dir():
        path.unlink(missing_ok=True)
        return
    for current, directories, _files in os.walk(
        path,
        topdown=True,
        followlinks=False,
    ):
        try:
            Path(current).chmod(0o700)
        except OSError:
            pass
        for name in directories:
            directory = Path(current) / name
            if not directory.is_symlink():
                try:
                    directory.chmod(0o700)
                except OSError:
                    pass
    for current, directories, files in os.walk(path, topdown=False, followlinks=False):
        current_path = Path(current)
        for name in files:
            file_path = current_path / name
            # Never chmod an attacker-inserted symlink: some POSIX builds do
            # not implement ``chmod(..., follow_symlinks=False)`` and raising
            # here would strand the immutable lease. Unlinking a symlink does
            # not touch its target and needs only the writable parent we
            # prepared in the first walk.
            if file_path.is_symlink():
                file_path.unlink(missing_ok=True)
                continue
            try:
                file_path.chmod(0o600, follow_symlinks=False)
            except (NotImplementedError, OSError):
                pass
            file_path.unlink(missing_ok=True)
        for name in directories:
            directory = current_path / name
            if directory.is_symlink():
                directory.unlink(missing_ok=True)
            else:
                try:
                    directory.chmod(0o700)
                except OSError:
                    pass
                directory.rmdir()
    try:
        path.chmod(0o700)
    except OSError:
        pass
    path.rmdir()


@dataclass(slots=True)
class _SnapshotLease:
    path: Path
    _released: bool = False

    def release(self) -> None:
        with _leases_lock:
            if self._released:
                return
            self._released = True
            count = _lease_counts.get(self.path, 0)
            if count > 1:
                _lease_counts[self.path] = count - 1
                return
            _lease_counts.pop(self.path, None)
            _remove_tree(self.path)


@dataclass(frozen=True, slots=True)
class AcceptedSkillSnapshot:
    """One immutable process-local skill snapshot retained by a lease."""

    snapshot_id: str
    content_digest: str
    skills: tuple[Skill, ...]
    projections: tuple[SkillSnapshotProjection, ...]
    file_count: int
    total_bytes: int
    root: Path = field(repr=False, compare=False)
    _lease: _SnapshotLease = field(repr=False, compare=False)

    def release(self) -> None:
        """Release this invocation's idempotent lease and delete when unused."""
        self._lease.release()

    def retain(self) -> AcceptedSkillSnapshot:
        """Acquire a child-worker lease for the same immutable material."""
        with _leases_lock:
            count = _lease_counts.get(self.root, 0)
            if count < 1:
                raise SkillSnapshotError("skill_snapshot_unavailable")
            _lease_counts[self.root] = count + 1
        return replace(self, _lease=_SnapshotLease(self.root))

    def verify(self) -> None:
        """Fail closed if process-local snapshot material has drifted."""
        digest, file_count, total_bytes = _digest_published_snapshot(
            self.root,
            self.projections,
            DEFAULT_SKILL_SNAPSHOT_LIMITS,
        )
        if digest != self.content_digest or file_count != self.file_count or total_bytes != self.total_bytes:
            raise SkillSnapshotError("skill_snapshot_drift")


def _bounded_relative(path: Path, *, limits: SkillSnapshotLimits) -> str:
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise SkillSnapshotError("skill_snapshot_path_invalid")
    value = path.as_posix()
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise SkillSnapshotError("skill_snapshot_path_invalid") from exc
    if len(encoded) > limits.max_relative_path_bytes:
        raise SkillSnapshotError("skill_snapshot_path_too_long")
    return value


def _same_file(before: os.stat_result, after: os.stat_result) -> bool:
    return (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    ) == (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )


def _read_stable_regular_file(
    path: Path,
    *,
    limits: SkillSnapshotLimits,
) -> tuple[bytes, bool]:
    try:
        before = path.lstat()
    except OSError as exc:
        raise SkillSnapshotError("skill_snapshot_file_unreadable") from exc
    if stat.S_ISLNK(before.st_mode):
        raise SkillSnapshotError("skill_snapshot_symlink")
    if not stat.S_ISREG(before.st_mode):
        raise SkillSnapshotError("skill_snapshot_special_file")
    if before.st_size > limits.max_file_bytes:
        raise SkillSnapshotError("skill_snapshot_file_too_large")

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            opened = os.fstat(stream.fileno())
            if not _same_file(before, opened):
                raise SkillSnapshotError("skill_snapshot_changed")
            data = stream.read(limits.max_file_bytes + 1)
            after_read = os.fstat(stream.fileno())
    except SkillSnapshotError:
        raise
    except OSError as exc:
        raise SkillSnapshotError("skill_snapshot_file_unreadable") from exc
    if len(data) > limits.max_file_bytes:
        raise SkillSnapshotError("skill_snapshot_file_too_large")
    try:
        after_path = path.lstat()
    except OSError as exc:
        raise SkillSnapshotError("skill_snapshot_changed") from exc
    if not _same_file(opened, after_read) or not _same_file(after_read, after_path):
        raise SkillSnapshotError("skill_snapshot_changed")
    return data, bool(before.st_mode & 0o111)


def _walk_skill_files(
    root: Path,
    *,
    limits: SkillSnapshotLimits,
) -> list[_CapturedFile]:
    try:
        root_stat = root.lstat()
    except OSError as exc:
        raise SkillSnapshotError("skill_snapshot_tree_unreadable") from exc
    if stat.S_ISLNK(root_stat.st_mode):
        raise SkillSnapshotError("skill_snapshot_symlink")
    if not stat.S_ISDIR(root_stat.st_mode):
        raise SkillSnapshotError("skill_snapshot_tree_invalid")

    files: list[_CapturedFile] = []
    seen: set[str] = set()
    stack: list[tuple[Path, Path]] = [(root, Path())]
    while stack:
        directory, relative_root = stack.pop()
        try:
            entries = sorted(os.scandir(directory), key=lambda item: item.name)
        except OSError as exc:
            raise SkillSnapshotError("skill_snapshot_tree_unreadable") from exc
        child_dirs: list[tuple[Path, Path]] = []
        for entry in entries:
            relative = relative_root / entry.name
            normalized = _bounded_relative(relative, limits=limits)
            if normalized in seen:
                raise SkillSnapshotError("skill_snapshot_duplicate_path")
            seen.add(normalized)
            try:
                metadata = entry.stat(follow_symlinks=False)
            except OSError as exc:
                raise SkillSnapshotError("skill_snapshot_file_unreadable") from exc
            if stat.S_ISLNK(metadata.st_mode):
                raise SkillSnapshotError("skill_snapshot_symlink")
            if stat.S_ISDIR(metadata.st_mode):
                child_dirs.append((Path(entry.path), relative))
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise SkillSnapshotError("skill_snapshot_special_file")
            data, executable = _read_stable_regular_file(
                Path(entry.path),
                limits=limits,
            )
            files.append(
                _CapturedFile(
                    relative_path=relative,
                    data=data,
                    executable=executable,
                )
            )
            if len(files) > limits.max_files_per_skill:
                raise SkillSnapshotError("skill_snapshot_too_many_files")
        stack.extend(reversed(child_dirs))
    if not any(captured.relative_path.as_posix() == SKILL_MD_FILE for captured in files):
        raise SkillSnapshotError("skill_snapshot_manifest_missing")
    return sorted(files, key=lambda item: item.relative_path.as_posix())


def _write_private_file(path: Path, data: bytes, *, executable: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o700 if executable else 0o600,
        )
        with os.fdopen(descriptor, "wb", closefd=True) as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise SkillSnapshotError("skill_snapshot_duplicate_path") from exc


def _skill_tree_digest(
    category: str,
    relative_path: str,
    files: list[_CapturedFile],
) -> str:
    digest = hashlib.sha256()
    for captured in files:
        header = json.dumps(
            [
                category,
                relative_path,
                captured.relative_path.as_posix(),
                "executable" if captured.executable else "regular",
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        digest.update(len(header).to_bytes(4, "big"))
        digest.update(header)
        digest.update(len(captured.data).to_bytes(8, "big"))
        digest.update(captured.data)
    return digest.hexdigest()


def _snapshot_digest(projections: list[SkillSnapshotProjection]) -> str:
    payload = [projection.to_json() for projection in projections]
    return hashlib.sha256(
        json.dumps(
            {"version": 1, "skills": payload},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _make_read_only(root: Path) -> None:
    for current, directories, files in os.walk(root, topdown=False, followlinks=False):
        current_path = Path(current)
        for name in files:
            file_path = current_path / name
            executable = bool(file_path.stat().st_mode & 0o111)
            file_path.chmod(0o500 if executable else 0o400)
        for name in directories:
            (current_path / name).chmod(0o500)
        current_path.chmod(0o500)


def _digest_published_snapshot(
    root: Path,
    projections: tuple[SkillSnapshotProjection, ...],
    limits: SkillSnapshotLimits,
) -> tuple[str, int, int]:
    rebuilt: list[SkillSnapshotProjection] = []
    file_count = 0
    total_bytes = 0
    for projection in projections:
        skill_root = root / projection.category / Path(projection.relative_path)
        files = _walk_skill_files(skill_root, limits=limits)
        skill_bytes = sum(len(captured.data) for captured in files)
        manifest = next(captured.data for captured in files if captured.relative_path.as_posix() == SKILL_MD_FILE)
        rebuilt.append(
            SkillSnapshotProjection(
                name=projection.name,
                category=projection.category,
                relative_path=projection.relative_path,
                manifest_digest=hashlib.sha256(manifest).hexdigest(),
                content_digest=_skill_tree_digest(
                    projection.category,
                    projection.relative_path,
                    files,
                ),
                file_count=len(files),
                total_bytes=skill_bytes,
            )
        )
        file_count += len(files)
        total_bytes += skill_bytes
    return _snapshot_digest(rebuilt), file_count, total_bytes


def _capture_verified_projection(
    root: Path,
    evidence: SkillProjectionEvidence,
    limits: SkillSnapshotLimits,
) -> tuple[tuple[tuple[SkillSnapshotProjection, tuple[_CapturedFile, ...]], ...], str, int, int]:
    """Stable-capture every accepted file and prove the immutable manifest."""
    from deerflow.runtime.skill_projection import SkillProjectionEvidence

    if not isinstance(evidence, SkillProjectionEvidence):
        raise SkillSnapshotError("skill_snapshot_evidence_invalid")
    if evidence.snapshot_id is None:
        if root.exists():
            raise SkillSnapshotError("skill_snapshot_evidence_invalid")
        return (), "", 0, 0
    try:
        root_metadata = root.lstat()
    except OSError as exc:
        raise SkillSnapshotError("skill_snapshot_unavailable") from exc
    if stat.S_ISLNK(root_metadata.st_mode):
        raise SkillSnapshotError("skill_snapshot_symlink")
    if not stat.S_ISDIR(root_metadata.st_mode):
        raise SkillSnapshotError("skill_snapshot_tree_invalid")

    captured: list[tuple[SkillSnapshotProjection, tuple[_CapturedFile, ...]]] = []
    rebuilt: list[SkillSnapshotProjection] = []
    total_files = 0
    total_bytes = 0
    for projection in evidence.projections:
        relative_path = _bounded_relative(Path(projection.relative_path), limits=limits)
        current = root
        for component in (projection.category, *Path(relative_path).parts):
            current = current / component
            try:
                component_metadata = current.lstat()
            except OSError as exc:
                raise SkillSnapshotError("skill_snapshot_unavailable") from exc
            if stat.S_ISLNK(component_metadata.st_mode):
                raise SkillSnapshotError("skill_snapshot_symlink")
        files = tuple(_walk_skill_files(current, limits=limits))
        skill_bytes = sum(len(item.data) for item in files)
        manifest = next(item.data for item in files if item.relative_path.as_posix() == SKILL_MD_FILE)
        rebuilt_projection = SkillSnapshotProjection(
            name=projection.name,
            category=projection.category,
            relative_path=relative_path,
            manifest_digest=hashlib.sha256(manifest).hexdigest(),
            content_digest=_skill_tree_digest(
                projection.category,
                relative_path,
                list(files),
            ),
            file_count=len(files),
            total_bytes=skill_bytes,
        )
        if rebuilt_projection != projection:
            raise SkillSnapshotError("skill_snapshot_drift")
        captured.append((projection, files))
        rebuilt.append(rebuilt_projection)
        total_files += len(files)
        total_bytes += skill_bytes
        if total_files > limits.max_total_files:
            raise SkillSnapshotError("skill_snapshot_too_many_files")
        if total_bytes > limits.max_total_bytes:
            raise SkillSnapshotError("skill_snapshot_total_too_large")
    digest = _snapshot_digest(rebuilt)
    if digest != evidence.content_digest or digest != evidence.snapshot_id or total_files != evidence.file_count or total_bytes != evidence.total_bytes:
        raise SkillSnapshotError("skill_snapshot_drift")
    return tuple(captured), digest, total_files, total_bytes


def load_skill_projection_evidence(
    *,
    user_id: str | None,
    snapshot_id: str | None,
    limits: SkillSnapshotLimits = DEFAULT_SKILL_SNAPSHOT_LIMITS,
) -> SkillProjectionEvidence:
    """Reconstruct and verify legacy process-local snapshot evidence."""
    from deerflow.runtime.skill_projection import SkillProjectionEvidence

    if snapshot_id is None:
        return SkillProjectionEvidence.from_snapshot(None)
    if _SNAPSHOT_ID_PATTERN.fullmatch(snapshot_id) is None:
        raise SkillSnapshotError("skill_snapshot_id_invalid")
    root = get_paths().skill_snapshot_scope_dir(user_id) / snapshot_id
    try:
        root_stat = root.lstat()
    except OSError as exc:
        raise SkillSnapshotError("skill_snapshot_unavailable") from exc
    if stat.S_ISLNK(root_stat.st_mode):
        raise SkillSnapshotError("skill_snapshot_symlink")
    if not stat.S_ISDIR(root_stat.st_mode):
        raise SkillSnapshotError("skill_snapshot_tree_invalid")

    manifest_paths: list[Path] = []
    all_files: set[str] = set()
    stack = [root]
    while stack:
        directory = stack.pop()
        try:
            entries = sorted(os.scandir(directory), key=lambda item: item.name)
        except OSError as exc:
            raise SkillSnapshotError("skill_snapshot_tree_unreadable") from exc
        for entry in entries:
            path = Path(entry.path)
            relative = _bounded_relative(path.relative_to(root), limits=limits)
            metadata = entry.stat(follow_symlinks=False)
            if stat.S_ISLNK(metadata.st_mode):
                raise SkillSnapshotError("skill_snapshot_symlink")
            if stat.S_ISDIR(metadata.st_mode):
                stack.append(path)
            elif stat.S_ISREG(metadata.st_mode):
                all_files.add(relative)
                if entry.name == SKILL_MD_FILE:
                    manifest_paths.append(path)
            else:
                raise SkillSnapshotError("skill_snapshot_special_file")
            if len(all_files) > limits.max_total_files:
                raise SkillSnapshotError("skill_snapshot_too_many_files")
    if not manifest_paths:
        raise SkillSnapshotError("skill_snapshot_manifest_missing")

    projections: list[SkillSnapshotProjection] = []
    captured_files: set[str] = set()
    for manifest_path in sorted(manifest_paths):
        relative_manifest = manifest_path.relative_to(root)
        if len(relative_manifest.parts) < 3:
            raise SkillSnapshotError("skill_snapshot_path_invalid")
        category = relative_manifest.parts[0]
        relative_path = Path(*relative_manifest.parts[1:-1])
        parsed = parse_skill_file(
            manifest_path,
            SkillCategory(category),
            relative_path=relative_path,
        )
        if parsed is None:
            raise SkillSnapshotError("skill_snapshot_manifest_invalid")
        files = _walk_skill_files(manifest_path.parent, limits=limits)
        for item in files:
            full_relative = (Path(category) / relative_path / item.relative_path).as_posix()
            if full_relative in captured_files:
                raise SkillSnapshotError("skill_snapshot_duplicate_path")
            captured_files.add(full_relative)
        skill_bytes = sum(len(item.data) for item in files)
        manifest = next(item.data for item in files if item.relative_path.as_posix() == SKILL_MD_FILE)
        projections.append(
            SkillSnapshotProjection(
                name=parsed.name,
                category=category,
                relative_path=relative_path.as_posix(),
                manifest_digest=hashlib.sha256(manifest).hexdigest(),
                content_digest=_skill_tree_digest(
                    category,
                    relative_path.as_posix(),
                    files,
                ),
                file_count=len(files),
                total_bytes=skill_bytes,
            )
        )
    if captured_files != all_files:
        raise SkillSnapshotError("skill_snapshot_unbound_file")
    projections.sort(key=lambda item: (item.category, item.relative_path, item.name))
    digest = _snapshot_digest(projections)
    if digest != snapshot_id:
        raise SkillSnapshotError("skill_snapshot_drift")
    return SkillProjectionEvidence(
        snapshot_id=snapshot_id,
        content_digest=digest,
        projections=tuple(projections),
        file_count=sum(item.file_count for item in projections),
        total_bytes=sum(item.total_bytes for item in projections),
    )


def snapshot_effective_skills(
    skills: tuple[Skill, ...],
    *,
    user_id: str | None,
    limits: SkillSnapshotLimits = DEFAULT_SKILL_SNAPSHOT_LIMITS,
) -> AcceptedSkillSnapshot | None:
    """Copy effective skill bytes, publish atomically, and acquire one lease."""
    if not skills:
        return None
    if len(skills) > limits.max_skills:
        raise SkillSnapshotError("skill_snapshot_too_many_skills")

    ordered = sorted(
        skills,
        key=lambda item: (str(item.category), item.relative_path.as_posix(), item.name),
    )
    identities: set[tuple[str, str]] = set()
    scope_root = get_paths().skill_snapshot_scope_dir(user_id)
    scope_root.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".building-", dir=scope_root))
    projections: list[SkillSnapshotProjection] = []
    parsed_by_identity: dict[tuple[str, str], Skill] = {}
    total_files = 0
    total_bytes = 0
    try:
        for source_skill in ordered:
            category = str(source_skill.category)
            relative_path = _bounded_relative(
                source_skill.relative_path,
                limits=limits,
            )
            identity = (category, relative_path)
            if identity in identities:
                raise SkillSnapshotError("skill_snapshot_duplicate_path")
            identities.add(identity)
            files = _walk_skill_files(Path(source_skill.skill_dir), limits=limits)
            skill_bytes = sum(len(captured.data) for captured in files)
            if skill_bytes > limits.max_skill_bytes:
                raise SkillSnapshotError("skill_snapshot_skill_too_large")
            total_files += len(files)
            total_bytes += skill_bytes
            if total_files > limits.max_total_files:
                raise SkillSnapshotError("skill_snapshot_too_many_files")
            if total_bytes > limits.max_total_bytes:
                raise SkillSnapshotError("skill_snapshot_total_too_large")

            target = stage / category / Path(relative_path)
            for captured in files:
                _write_private_file(
                    target / captured.relative_path,
                    captured.data,
                    executable=captured.executable,
                )
            staged_files = _walk_skill_files(target, limits=limits)
            if staged_files != files:
                raise SkillSnapshotError("skill_snapshot_write_mismatch")
            # Re-read the complete source tree after the staged copy. A file
            # changed after its individual stable read but before publication
            # must not let metadata from one generation accompany bytes from
            # another.
            confirmed_files = _walk_skill_files(
                Path(source_skill.skill_dir),
                limits=limits,
            )
            if confirmed_files != files:
                raise SkillSnapshotError("skill_snapshot_changed")
            parsed = parse_skill_file(
                target / SKILL_MD_FILE,
                SkillCategory(category),
                relative_path=Path(relative_path),
            )
            if parsed is None or parsed.name != source_skill.name:
                raise SkillSnapshotError("skill_snapshot_manifest_invalid")
            parsed_by_identity[identity] = replace(parsed, enabled=True)
            manifest = next(captured.data for captured in staged_files if captured.relative_path.as_posix() == SKILL_MD_FILE)
            projections.append(
                SkillSnapshotProjection(
                    name=parsed.name,
                    category=category,
                    relative_path=relative_path,
                    manifest_digest=hashlib.sha256(manifest).hexdigest(),
                    content_digest=_skill_tree_digest(
                        category,
                        relative_path,
                        staged_files,
                    ),
                    file_count=len(staged_files),
                    total_bytes=skill_bytes,
                )
            )

        content_digest = _snapshot_digest(projections)
        final_root = scope_root / content_digest
        with _leases_lock:
            published_new = False
            if final_root.exists():
                _remove_tree(stage)
            else:
                _make_read_only(stage)
                stage.replace(final_root)
                published_new = True
            published_digest, published_files, published_bytes = _digest_published_snapshot(
                final_root,
                tuple(projections),
                limits,
            )
            if published_digest != content_digest or published_files != total_files or published_bytes != total_bytes:
                if published_new:
                    _remove_tree(final_root)
                raise SkillSnapshotError("skill_snapshot_drift")
            _lease_counts[final_root] = _lease_counts.get(final_root, 0) + 1
        lease = _SnapshotLease(final_root)
    except Exception:
        try:
            _remove_tree(stage)
        except OSError:
            pass
        raise

    projection_by_identity = {(projection.category, projection.relative_path): projection for projection in projections}
    snapshot_skills: list[Skill] = []
    for source_skill in skills:
        identity = (str(source_skill.category), source_skill.relative_path.as_posix())
        projection = projection_by_identity[identity]
        parsed = parsed_by_identity[identity]
        skill_root = final_root / projection.category / Path(projection.relative_path)
        snapshot_skills.append(
            replace(
                parsed,
                skill_dir=skill_root,
                skill_file=skill_root / SKILL_MD_FILE,
                container_relative_path=(f"{SNAPSHOT_CONTAINER_NAMESPACE}/{content_digest}/{projection.category}/{projection.relative_path}"),
            )
        )
    return AcceptedSkillSnapshot(
        snapshot_id=content_digest,
        content_digest=content_digest,
        skills=tuple(snapshot_skills),
        projections=tuple(projections),
        file_count=total_files,
        total_bytes=total_bytes,
        root=final_root,
        _lease=lease,
    )


def bind_skill_snapshot_active_view(
    *,
    user_id: str | None,
    thread_id: str,
    snapshot_id: str | None,
    run_id: str = "legacy",
    generation: int = 0,
    evidence: object | None = None,
) -> Path:
    """Expose exactly one accepted snapshot through a stable thread mount.

    Local and container sandboxes reuse a stable mount across turns.  The
    mounted directory therefore contains a copied, read-only projection of
    only the currently accepted digest instead of the subject-wide snapshot
    cache.  ``snapshot_id=None`` deliberately publishes an empty view.
    """
    if snapshot_id is not None and _SNAPSHOT_ID_PATTERN.fullmatch(snapshot_id) is None:
        raise SkillSnapshotError("skill_snapshot_id_invalid")
    paths = get_paths()
    view = paths.skill_snapshot_active_view_dir(user_id, thread_id)
    view.parent.mkdir(parents=True, exist_ok=True)
    view.mkdir(parents=True, exist_ok=True)

    stage: Path | None = None
    if snapshot_id is not None:
        source = paths.skill_snapshot_scope_dir(user_id) / snapshot_id
        if not source.is_dir() or source.is_symlink():
            raise SkillSnapshotError("skill_snapshot_unavailable")
        if evidence is None:
            evidence = load_skill_projection_evidence(
                user_id=user_id,
                snapshot_id=snapshot_id,
            )
        captured, _, _, _ = _capture_verified_projection(
            source,
            evidence,
            DEFAULT_SKILL_SNAPSHOT_LIMITS,
        )
        stage = Path(tempfile.mkdtemp(prefix=".binding-", dir=view.parent))
        try:
            staged_snapshot = stage / snapshot_id
            for projection, files in captured:
                target = staged_snapshot / projection.category / Path(projection.relative_path)
                for captured_file in files:
                    _write_private_file(
                        target / captured_file.relative_path,
                        captured_file.data,
                        executable=captured_file.executable,
                    )
            confirmed, _, _, _ = _capture_verified_projection(
                source,
                evidence,
                DEFAULT_SKILL_SNAPSHOT_LIMITS,
            )
            if confirmed != captured:
                raise SkillSnapshotError("skill_snapshot_changed")
            staged, _, _, _ = _capture_verified_projection(
                staged_snapshot,
                evidence,
                DEFAULT_SKILL_SNAPSHOT_LIMITS,
            )
            if staged != captured:
                raise SkillSnapshotError("skill_snapshot_write_mismatch")
        except Exception:
            _remove_tree(stage)
            raise
    elif evidence is not None:
        from deerflow.runtime.skill_projection import SkillProjectionEvidence

        if not isinstance(evidence, SkillProjectionEvidence) or evidence.snapshot_id is not None:
            raise SkillSnapshotError("skill_snapshot_evidence_invalid")

    binding_identity = (run_id, generation, snapshot_id)
    with _active_views_lock:
        if view in _active_view_bindings:
            current = _active_view_bindings[view]
            if current == binding_identity:
                if stage is not None:
                    _remove_tree(stage)
                return view
            if generation <= current[1] or generation == 0:
                if stage is not None:
                    _remove_tree(stage)
                raise SkillSnapshotError("skill_snapshot_binding_conflict")
        for child in list(view.iterdir()):
            _remove_tree(child)
        if stage is not None and snapshot_id is not None:
            staged_snapshot = stage / snapshot_id
            # macOS refuses to rename a directory whose own mode is read-only,
            # even when both parents are writable. Publication still happens
            # within the same filesystem and the destination is made read-only
            # immediately after the atomic rename.
            destination = view / snapshot_id
            try:
                os.chmod(
                    staged_snapshot,
                    stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                )
                os.replace(staged_snapshot, destination)
                _make_read_only(destination)
            except Exception:
                _remove_tree(destination)
                _remove_tree(stage)
                raise
            _remove_tree(stage)
        _active_view_bindings[view] = binding_identity
    return view


def clear_skill_snapshot_active_view(
    *,
    user_id: str | None,
    thread_id: str,
    run_id: str,
    generation: int,
) -> bool:
    """Compare-and-clear one exact invocation projection."""
    view = get_paths().skill_snapshot_active_view_dir(user_id, thread_id)
    with _active_views_lock:
        current = _active_view_bindings.get(view)
        if current is None or current[:2] != (run_id, generation):
            return False
        if view.exists():
            for child in list(view.iterdir()):
                _remove_tree(child)
        _active_view_bindings.pop(view, None)
        return True


def prove_skill_snapshot_active_view_absent(
    *,
    user_id: str | None,
    thread_id: str,
) -> bool:
    """Prove a failed pre-publication bind left no reachable accepted bytes."""
    view = get_paths().skill_snapshot_active_view_dir(user_id, thread_id)
    with _active_views_lock:
        if view in _active_view_bindings:
            return False
        try:
            if not view.exists():
                return True
            if view.is_symlink() or not view.is_dir():
                return False
            return next(view.iterdir(), None) is None
        except OSError:
            return False


def force_clear_skill_snapshot_active_view(
    *,
    user_id: str | None,
    thread_id: str,
) -> None:
    """Clear a destroyed/reset sandbox view regardless of invocation owner."""
    view = get_paths().skill_snapshot_active_view_dir(user_id, thread_id)
    with _active_views_lock:
        if view.exists():
            for child in list(view.iterdir()):
                _remove_tree(child)
        _active_view_bindings.pop(view, None)


def cleanup_abandoned_skill_snapshots() -> int:
    """Remove process-local snapshots left by a prior Gateway process."""
    root = get_paths().skill_snapshots_dir
    removed = 0
    if root.exists():
        with _leases_lock:
            for scope in list(root.iterdir()):
                if scope.is_symlink() or not scope.is_dir():
                    _remove_tree(scope)
                    removed += 1
                    continue
                for snapshot in list(scope.iterdir()):
                    if snapshot in _lease_counts:
                        continue
                    _remove_tree(snapshot)
                    removed += 1
                try:
                    scope.rmdir()
                except OSError:
                    pass
    active_views = get_paths().skill_snapshot_active_views_dir
    if active_views.exists():
        with _active_views_lock:
            for scope in list(active_views.iterdir()):
                if scope.is_symlink() or not scope.is_dir():
                    _remove_tree(scope)
                    removed += 1
                    continue
                for view in list(scope.iterdir()):
                    _remove_tree(view)
                    _active_view_bindings.pop(view, None)
                    removed += 1
                try:
                    scope.rmdir()
                except OSError:
                    pass
    return removed


__all__ = [
    "AcceptedSkillSnapshot",
    "DEFAULT_SKILL_SNAPSHOT_LIMITS",
    "SNAPSHOT_CONTAINER_NAMESPACE",
    "SkillSnapshotError",
    "SkillSnapshotLimits",
    "SkillSnapshotProjection",
    "bind_skill_snapshot_active_view",
    "clear_skill_snapshot_active_view",
    "force_clear_skill_snapshot_active_view",
    "load_skill_projection_evidence",
    "prove_skill_snapshot_active_view_absent",
    "cleanup_abandoned_skill_snapshots",
    "snapshot_effective_skills",
]
