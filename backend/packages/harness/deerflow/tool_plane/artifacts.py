"""Protected, content-addressed staging for untrusted skill archives."""

from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import tempfile
import uuid
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

from deerflow.skills.frontmatter import split_skill_markdown
from deerflow.skills.installer import (
    resolve_skill_dir_from_archive,
    safe_extract_skill_archive,
)
from deerflow.tool_plane.contracts import ToolPlaneRevisionError

_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_SKILL_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z")
_MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
_MAX_EXPANDED_BYTES = 512 * 1024 * 1024
_MAX_ENTRIES = 4096


def _now() -> datetime:
    return datetime.now(UTC)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _tree_digest(root: Path) -> str:
    records: list[bytes] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise ToolPlaneRevisionError("unsafe_archive")
        if not path.is_file():
            continue
        relative = path.relative_to(root).as_posix().encode("utf-8")
        content_digest = _sha256_file(path).encode("ascii")
        size = str(path.stat().st_size).encode("ascii")
        records.append(b"\0".join((relative, size, content_digest)))
    digest = hashlib.sha256()
    for record in records:
        digest.update(len(record).to_bytes(8, "big"))
        digest.update(record)
    return digest.hexdigest()


def compute_skill_tree_digest(root: Path) -> str:
    """Return the canonical package-tree digest used by revision manifests."""

    return _tree_digest(Path(root))


def _declared_name(skill_md: Path, tree_digest: str) -> str:
    try:
        content = skill_md.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return f"candidate-{tree_digest[:16]}"
    parts, _ = split_skill_markdown(content)
    if parts is None:
        return f"candidate-{tree_digest[:16]}"
    name = parts.metadata.get("name")
    if not isinstance(name, str):
        return f"candidate-{tree_digest[:16]}"
    normalized = name.strip()
    if _SAFE_SKILL_NAME.fullmatch(normalized) is None:
        return f"candidate-{tree_digest[:16]}"
    return normalized


@dataclass(frozen=True, slots=True)
class StagedSkillArtifactV1:
    artifact_ref: str
    skill_name: str
    archive_digest: str
    tree_digest: str
    manifest_digest: str
    entry_points: tuple[str, ...]
    staged_at: datetime

    def to_safe_json(self) -> dict[str, object]:
        return {
            "version": 1,
            "artifact_ref": self.artifact_ref,
            "skill_name": self.skill_name,
            "archive_digest": self.archive_digest,
            "tree_digest": self.tree_digest,
            "manifest_digest": self.manifest_digest,
            "entry_points": list(self.entry_points),
            "staged_at": self.staged_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class VerifiedSkillArtifact:
    metadata: StagedSkillArtifactV1
    package_root: Path


class GovernedSkillArtifactStore:
    """Store exact candidate bytes outside active skill roots.

    Public values are digests and an opaque reference only.  Filesystem paths
    remain private to validators and projection adapters.
    """

    def __init__(
        self,
        root: Path,
        *,
        max_archive_bytes: int = _MAX_ARCHIVE_BYTES,
        max_expanded_bytes: int = _MAX_EXPANDED_BYTES,
        max_entries: int = _MAX_ENTRIES,
    ) -> None:
        self._root = Path(root)
        self._max_archive_bytes = max_archive_bytes
        self._max_expanded_bytes = max_expanded_bytes
        self._max_entries = max_entries

    def _object_root(self, tree_digest: str) -> Path:
        if _DIGEST.fullmatch(tree_digest) is None:
            raise ToolPlaneRevisionError("validation_failed")
        return self._root / "objects" / tree_digest[:2] / tree_digest

    @staticmethod
    def _metadata(value: object) -> StagedSkillArtifactV1:
        if not isinstance(value, dict) or value.get("version") != 1:
            raise ToolPlaneRevisionError("skill_artifact_not_staged")
        try:
            staged_at = datetime.fromisoformat(str(value["staged_at"]).replace("Z", "+00:00"))
            entry_points = tuple(str(item) for item in value["entry_points"])
            metadata = StagedSkillArtifactV1(
                artifact_ref=str(value["artifact_ref"]),
                skill_name=str(value["skill_name"]),
                archive_digest=str(value["archive_digest"]),
                tree_digest=str(value["tree_digest"]),
                manifest_digest=str(value["manifest_digest"]),
                entry_points=entry_points,
                staged_at=staged_at,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ToolPlaneRevisionError("skill_artifact_not_staged") from exc
        if any(
            _DIGEST.fullmatch(digest) is None
            for digest in (
                metadata.archive_digest,
                metadata.tree_digest,
                metadata.manifest_digest,
            )
        ):
            raise ToolPlaneRevisionError("skill_artifact_not_staged")
        return metadata

    def stage_archive(self, source: BinaryIO | Path) -> StagedSkillArtifactV1:
        self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
        staging_root = Path(tempfile.mkdtemp(prefix="candidate-", dir=self._root))
        try:
            archive_path = staging_root / "candidate.skill"
            digest = hashlib.sha256()
            total = 0
            input_stream: BinaryIO
            close_input = False
            if isinstance(source, Path):
                input_stream = source.open("rb")
                close_input = True
            elif isinstance(source, io.BufferedIOBase) or hasattr(source, "read"):
                input_stream = source
            else:
                raise TypeError("source must be a binary stream or Path")
            try:
                with archive_path.open("wb") as target:
                    while chunk := input_stream.read(1024 * 1024):
                        if not isinstance(chunk, bytes):
                            raise ToolPlaneRevisionError("unsafe_archive")
                        total += len(chunk)
                        if total > self._max_archive_bytes:
                            raise ToolPlaneRevisionError("unsafe_archive")
                        digest.update(chunk)
                        target.write(chunk)
                    target.flush()
                    os.fsync(target.fileno())
            finally:
                if close_input:
                    input_stream.close()
            archive_digest = digest.hexdigest()
            extract_root = staging_root / "extracted"
            extract_root.mkdir(mode=0o700)
            try:
                with zipfile.ZipFile(archive_path) as archive:
                    safe_extract_skill_archive(
                        archive,
                        extract_root,
                        max_total_size=self._max_expanded_bytes,
                        max_entries=self._max_entries,
                    )
                package_root = resolve_skill_dir_from_archive(extract_root)
            except (OSError, ValueError, zipfile.BadZipFile) as exc:
                raise ToolPlaneRevisionError("unsafe_archive") from exc

            tree_digest = _tree_digest(package_root)
            skill_md = package_root / "SKILL.md"
            try:
                manifest_digest = hashlib.sha256(skill_md.read_bytes()).hexdigest()
                entry_points = ("SKILL.md",)
            except OSError:
                manifest_digest = hashlib.sha256(b"").hexdigest()
                entry_points = ()
            staged_at = _now()
            metadata = StagedSkillArtifactV1(
                artifact_ref=f"skill-artifact:{uuid.uuid4()}",
                skill_name=_declared_name(skill_md, tree_digest),
                archive_digest=archive_digest,
                tree_digest=tree_digest,
                manifest_digest=manifest_digest,
                entry_points=entry_points,
                staged_at=staged_at,
            )
            object_root = self._object_root(tree_digest)
            if object_root.exists():
                existing = self.verify(
                    tree_digest=tree_digest,
                    archive_digest=archive_digest,
                    manifest_digest=manifest_digest,
                )
                return existing.metadata
            pending = object_root.with_name(f".{tree_digest}.{uuid.uuid4().hex}.tmp")
            pending.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            pending.mkdir(mode=0o700)
            shutil.copy2(archive_path, pending / "archive.skill")
            shutil.copytree(package_root, pending / "package")
            metadata_path = pending / "metadata.json"
            metadata_path.write_text(
                json.dumps(
                    metadata.to_safe_json(),
                    ensure_ascii=False,
                    allow_nan=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                encoding="utf-8",
            )
            os.replace(pending, object_root)
            return metadata
        except ToolPlaneRevisionError:
            raise
        except Exception as exc:
            raise ToolPlaneRevisionError("unsafe_archive") from exc
        finally:
            shutil.rmtree(staging_root, ignore_errors=True)

    def stage_directory(self, source: Path) -> StagedSkillArtifactV1:
        """Capture an installed package as deterministic archive material."""

        root = Path(source)
        if not root.is_dir() or root.is_symlink():
            raise ToolPlaneRevisionError("unsafe_archive")
        files: list[Path] = []
        total = 0
        for path in sorted(root.rglob("*")):
            if path.is_symlink() or (not path.is_file() and not path.is_dir()):
                raise ToolPlaneRevisionError("unsafe_archive")
            if path.is_file():
                files.append(path)
                total += path.stat().st_size
        if len(files) > self._max_entries or total > self._max_expanded_bytes:
            raise ToolPlaneRevisionError("unsafe_archive")
        with tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024) as stream:
            with zipfile.ZipFile(stream, "w", zipfile.ZIP_DEFLATED) as archive:
                for path in files:
                    relative = path.relative_to(root).as_posix()
                    info = zipfile.ZipInfo(
                        f"package/{relative}",
                        date_time=(1980, 1, 1, 0, 0, 0),
                    )
                    info.create_system = 3
                    info.external_attr = (0o100600 & 0xFFFF) << 16
                    info.compress_type = zipfile.ZIP_DEFLATED
                    archive.writestr(info, path.read_bytes())
            stream.seek(0)
            return self.stage_archive(stream)

    def verify(
        self,
        *,
        tree_digest: str,
        archive_digest: str,
        manifest_digest: str,
    ) -> VerifiedSkillArtifact:
        object_root = self._object_root(tree_digest)
        try:
            metadata = self._metadata(json.loads((object_root / "metadata.json").read_text(encoding="utf-8")))
            package_root = object_root / "package"
            observed_archive = _sha256_file(object_root / "archive.skill")
            observed_tree = _tree_digest(package_root)
            skill_md = package_root / "SKILL.md"
            observed_manifest = hashlib.sha256(skill_md.read_bytes()).hexdigest()
        except ToolPlaneRevisionError:
            raise
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise ToolPlaneRevisionError("skill_artifact_not_staged") from exc
        if (
            metadata.tree_digest != tree_digest
            or metadata.archive_digest != archive_digest
            or metadata.manifest_digest != manifest_digest
            or observed_archive != archive_digest
            or observed_tree != tree_digest
            or observed_manifest != manifest_digest
        ):
            raise ToolPlaneRevisionError("skill_artifact_not_staged")
        return VerifiedSkillArtifact(metadata=metadata, package_root=package_root)


__all__ = [
    "GovernedSkillArtifactStore",
    "StagedSkillArtifactV1",
    "VerifiedSkillArtifact",
    "compute_skill_tree_digest",
]
