"""Provider-neutral accepted skill materialization contracts.

The durable runtime persists only these bounded, canonical values. Provider
SDK objects, credentials, and renewal handles remain process-local.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import threading
import unicodedata
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, NoReturn, Protocol, Self

from deerflow_extension_api import TenantReferenceV1

from deerflow.sandbox.sandbox import Sandbox

if TYPE_CHECKING:
    from deerflow.runtime.skill_projection import SkillProjectionConsumerToken

_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_MAX_REFERENCE_BYTES = 512
_MAX_PATH_BYTES = 512
_MAX_PATH_DEPTH = 32
_MAX_FILE_BYTES = 2 * 1024 * 1024
_MAX_TOTAL_FILES = 2_048
_MAX_TOTAL_BYTES = 32 * 1024 * 1024


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        ).encode("utf-8"),
    ).hexdigest()


def _require_digest(value: object, field_name: str) -> str:
    if not isinstance(value, str) or _DIGEST_PATTERN.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return value


def _require_reference(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > _MAX_REFERENCE_BYTES or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise ValueError(f"{field_name} must be a bounded control-free string")
    return value


def _canonical_timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("lease_expires_at must be timezone-aware")
    rendered = value.astimezone(UTC).isoformat()
    return rendered.removesuffix("+00:00") + "Z"


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("lease_expires_at must be canonical UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("lease_expires_at must be canonical UTC") from exc
    if _canonical_timestamp(parsed) != value:
        raise ValueError("lease_expires_at must be canonical UTC")
    return parsed


def _validate_relative_path(value: object, *, field_name: str = "path") -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError(f"accepted material {field_name} is invalid")
    if unicodedata.normalize("NFC", value) != value:
        raise ValueError(f"accepted material {field_name} changes under normalization")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"accepted material {field_name} is invalid") from exc
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or value != path.as_posix()
        or len(encoded) > _MAX_PATH_BYTES
        or len(path.parts) > _MAX_PATH_DEPTH
        or any(part in {"", ".", ".."} for part in path.parts)
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise ValueError(f"accepted material {field_name} is invalid")
    return value


def _validate_manifest_closure(entries: tuple[AcceptedFileV1, ...]) -> None:
    by_path = {entry.path: entry for entry in entries}
    for entry in entries:
        for parent in PurePosixPath(entry.path).parents:
            parent_path = parent.as_posix()
            if parent_path == ".":
                continue
            parent_entry = by_path.get(parent_path)
            if parent_entry is None or parent_entry.file_type is not AcceptedFileType.DIRECTORY:
                raise ValueError(
                    "accepted material file manifest has a missing or non-directory parent",
                )

    for entry in entries:
        if entry.file_type is not AcceptedFileType.SYMLINK:
            continue
        seen = {entry.path}
        current = entry
        for _ in range(_MAX_PATH_DEPTH):
            assert current.link_target is not None
            target = PurePosixPath(current.path).parent / current.link_target
            target_path = target.as_posix()
            _validate_relative_path(target_path, field_name="resolved link target")
            if target_path in seen:
                raise ValueError("accepted material file manifest has a symlink cycle")
            seen.add(target_path)
            target_entry = by_path.get(target_path)
            if target_entry is None:
                raise ValueError("accepted material file manifest has a dangling symlink")
            if target_entry.file_type is not AcceptedFileType.SYMLINK:
                break
            current = target_entry
        else:
            raise ValueError("accepted material file manifest has an excessive symlink chain")


class AcceptedFileType(StrEnum):
    """File kinds representable by the provider-neutral manifest."""

    REGULAR = "regular"
    DIRECTORY = "directory"
    SYMLINK = "symlink"


class AcceptedMaterialCapability(StrEnum):
    """Strongest accepted-material boundary an adapter can prove."""

    EMPTY_ONLY = "empty_only"
    IMMUTABLE_READ_ONLY = "immutable_read_only"


class AcceptedSkillSandboxBindingError(RuntimeError):
    """Fail-closed error for an unavailable accepted-skill projection."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class AcceptedMaterialError(RuntimeError):
    """Stable fail-closed error raised by a materializer adapter."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class AcceptedSkillExecutionEvidenceV1:
    """Legacy bounded proof retained for persisted v1 AIO records."""

    profile: str
    attempt_id: str
    snapshot_id: str
    run_id: str
    generation: int
    pod_uid: str
    lease_uid: str
    runtime_image_ids_digest: str
    verifier_receipt_digest: str
    materialization_evidence_digest: str

    def __post_init__(self) -> None:
        if self.profile != "rwx_verified_copy_v1":
            raise ValueError("accepted skill profile is invalid")
        for value, field_name, maximum in (
            (self.attempt_id, "attempt_id", 128),
            (self.run_id, "run_id", 512),
            (self.pod_uid, "pod_uid", 128),
            (self.lease_uid, "lease_uid", 128),
        ):
            if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum or any(ord(character) < 32 for character in value):
                raise ValueError(f"accepted skill {field_name} is invalid")
        for value, field_name in (
            (self.snapshot_id, "snapshot_id"),
            (self.runtime_image_ids_digest, "runtime_image_ids_digest"),
            (self.verifier_receipt_digest, "verifier_receipt_digest"),
            (
                self.materialization_evidence_digest,
                "materialization_evidence_digest",
            ),
        ):
            if not isinstance(value, str) or _DIGEST_PATTERN.fullmatch(value) is None:
                raise ValueError(f"accepted skill {field_name} is invalid")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("accepted skill generation is invalid")

    def to_persisted(self) -> dict[str, object]:
        return {
            "version": 1,
            "profile": self.profile,
            "attempt_id": self.attempt_id,
            "snapshot_id": self.snapshot_id,
            "run_id": self.run_id,
            "generation": self.generation,
            "pod_uid": self.pod_uid,
            "lease_uid": self.lease_uid,
            "runtime_image_ids_digest": self.runtime_image_ids_digest,
            "verifier_receipt_digest": self.verifier_receipt_digest,
            "materialization_evidence_digest": self.materialization_evidence_digest,
        }

    @property
    def digest(self) -> str:
        return _canonical_digest(self.to_persisted())


@dataclass(frozen=True, slots=True)
class AcceptedSkillExecutionEvidenceV2:
    """Complete non-secret proof of one v2 Kubernetes execution tuple."""

    profile: str
    attempt_id: str
    snapshot_id: str
    run_id: str
    generation: int
    pod_uid: str
    pod_isolation_digest: str
    lease_uid: str
    network_policy_uid: str
    network_policy_spec_digest: str
    evidence_secret_uid: str
    evidence_secret_digest: str
    capability_secret_uid: str
    capability_secret_digest: str
    sandbox_image_digest: str
    accepted_skill_runtime_image_digest: str
    runtime_image_ids_digest: str
    verifier_receipt_digest: str
    materialization_evidence_digest: str

    def __post_init__(self) -> None:
        if self.profile != "rwx_verified_copy_v2":
            raise ValueError("accepted skill profile is invalid")
        for value, field_name, maximum in (
            (self.attempt_id, "attempt_id", 128),
            (self.run_id, "run_id", 512),
            (self.pod_uid, "pod_uid", 128),
            (self.lease_uid, "lease_uid", 128),
            (self.network_policy_uid, "network_policy_uid", 128),
            (self.evidence_secret_uid, "evidence_secret_uid", 128),
            (self.capability_secret_uid, "capability_secret_uid", 128),
        ):
            if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum or any(ord(character) < 32 for character in value):
                raise ValueError(f"accepted skill {field_name} is invalid")
        for field_name in (
            "snapshot_id",
            "pod_isolation_digest",
            "network_policy_spec_digest",
            "evidence_secret_digest",
            "capability_secret_digest",
            "sandbox_image_digest",
            "accepted_skill_runtime_image_digest",
            "runtime_image_ids_digest",
            "verifier_receipt_digest",
            "materialization_evidence_digest",
        ):
            value = getattr(self, field_name)
            if not isinstance(value, str) or _DIGEST_PATTERN.fullmatch(value) is None:
                raise ValueError(f"accepted skill {field_name} is invalid")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError("accepted skill generation is invalid")
        materialization_wire = {
            "version": 2,
            **{name: getattr(self, name) for name in self.__dataclass_fields__ if name != "materialization_evidence_digest"},
            "content_digest": self.snapshot_id,
        }
        if self.materialization_evidence_digest != _canonical_digest(
            materialization_wire,
        ):
            raise ValueError("accepted skill materialization evidence digest is invalid")

    def to_persisted(self) -> dict[str, object]:
        return {
            "version": 2,
            **{name: getattr(self, name) for name in self.__dataclass_fields__},
        }

    @property
    def digest(self) -> str:
        return _canonical_digest(self.to_persisted())


AcceptedSkillExecutionEvidence = AcceptedSkillExecutionEvidenceV1 | AcceptedSkillExecutionEvidenceV2


@dataclass(frozen=True, slots=True)
class AcceptedSkillSandboxBindingV1:
    """Compatibility binding consumed by the existing AIO adapter."""

    snapshot_id: str | None
    run_id: str = "legacy"
    generation: int = 0
    evidence: object | None = None

    def __post_init__(self) -> None:
        if self.snapshot_id is not None and _DIGEST_PATTERN.fullmatch(self.snapshot_id) is None:
            raise ValueError(
                "accepted skill snapshot_id must be a lowercase SHA-256 digest",
            )
        if not isinstance(self.run_id, str) or not self.run_id:
            raise ValueError("accepted skill run_id must be non-empty")
        if type(self.generation) is not int or self.generation < 0:
            raise ValueError(
                "accepted skill generation must be a non-negative integer",
            )

    @classmethod
    def from_consumer_token(
        cls,
        token: SkillProjectionConsumerToken,
    ) -> Self:
        from deerflow.runtime.skill_projection import SkillProjectionConsumerToken

        if not isinstance(token, SkillProjectionConsumerToken):
            raise TypeError("accepted skill projection token is invalid")
        return cls(
            snapshot_id=token.snapshot_id,
            run_id=token.run_id,
            generation=token.generation,
            evidence=token.evidence,
        )


@dataclass(frozen=True, slots=True)
class AcceptedFileV1:
    """One bounded manifest entry in an accepted immutable snapshot."""

    version: Literal[1]
    path: str
    file_type: AcceptedFileType
    size: int
    mode: int
    digest: str | None
    link_target: str | None = None

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted material file version must be 1")
        _validate_relative_path(self.path)
        try:
            file_type = AcceptedFileType(self.file_type)
        except (TypeError, ValueError) as exc:
            raise ValueError("accepted material file type is invalid") from exc
        object.__setattr__(self, "file_type", file_type)
        if type(self.size) is not int or self.size < 0 or self.size > _MAX_FILE_BYTES:
            raise ValueError("accepted material file size is invalid")
        if type(self.mode) is not int or self.mode < 0 or self.mode > 0o7777 or self.mode & 0o7222:
            raise ValueError("accepted material mode must be read-only without special bits")

        if file_type is AcceptedFileType.REGULAR:
            _require_digest(self.digest, "accepted material file digest")
            if self.link_target is not None:
                raise ValueError("regular accepted material cannot have a link target")
        elif file_type is AcceptedFileType.DIRECTORY:
            if self.size != 0 or self.digest is not None or self.link_target is not None:
                raise ValueError("accepted material directory metadata is invalid")
        else:
            target = _validate_relative_path(self.link_target, field_name="link_target")
            if self.mode != 0 or self.size != len(target.encode("utf-8")):
                raise ValueError("accepted material symlink metadata is invalid")
            if self.digest != hashlib.sha256(target.encode("utf-8")).hexdigest():
                raise ValueError("accepted material symlink digest is invalid")

    def to_persisted(self) -> dict[str, object]:
        return {
            "version": self.version,
            "path": self.path,
            "file_type": self.file_type.value,
            "size": self.size,
            "mode": self.mode,
            "digest": self.digest,
            "link_target": self.link_target,
        }

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "path",
            "file_type",
            "size",
            "mode",
            "digest",
            "link_target",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("accepted material file has unknown or missing fields")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            path=value["path"],  # type: ignore[arg-type]
            file_type=value["file_type"],  # type: ignore[arg-type]
            size=value["size"],  # type: ignore[arg-type]
            mode=value["mode"],  # type: ignore[arg-type]
            digest=value["digest"],  # type: ignore[arg-type]
            link_target=value["link_target"],  # type: ignore[arg-type]
        )


def capture_accepted_file_manifest(root: Path) -> tuple[AcceptedFileV1, ...]:
    """Capture exact immutable snapshot bytes into the neutral manifest shape."""

    if not isinstance(root, Path):
        raise TypeError("accepted material root must be a Path")
    try:
        root_stat = root.lstat()
    except OSError as exc:
        raise AcceptedMaterialError("accepted_material_manifest_unavailable") from exc
    if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
        raise AcceptedMaterialError("accepted_material_manifest_root_invalid")

    entries: list[AcceptedFileV1] = []
    total_bytes = 0
    pending = [root]
    try:
        while pending:
            directory = pending.pop()
            with os.scandir(directory) as scanned:
                children = sorted(scanned, key=lambda child: child.name)
            for child in children:
                child_path = Path(child.path)
                relative = child_path.relative_to(root).as_posix()
                _validate_relative_path(relative)
                metadata = child.stat(follow_symlinks=False)
                mode = stat.S_IMODE(metadata.st_mode)
                if stat.S_ISDIR(metadata.st_mode):
                    entry = AcceptedFileV1(
                        version=1,
                        path=relative,
                        file_type=AcceptedFileType.DIRECTORY,
                        size=0,
                        mode=mode,
                        digest=None,
                    )
                    pending.append(child_path)
                elif stat.S_ISLNK(metadata.st_mode):
                    target = os.readlink(child_path)
                    target_bytes = target.encode("utf-8")
                    entry = AcceptedFileV1(
                        version=1,
                        path=relative,
                        file_type=AcceptedFileType.SYMLINK,
                        size=len(target_bytes),
                        mode=0,
                        digest=hashlib.sha256(target_bytes).hexdigest(),
                        link_target=target,
                    )
                elif stat.S_ISREG(metadata.st_mode):
                    if metadata.st_nlink != 1 or metadata.st_size > _MAX_FILE_BYTES:
                        raise AcceptedMaterialError("accepted_material_manifest_file_invalid")
                    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
                    descriptor = os.open(child_path, flags)
                    try:
                        opened = os.fstat(descriptor)
                        identity = (
                            metadata.st_dev,
                            metadata.st_ino,
                            metadata.st_size,
                            metadata.st_mtime_ns,
                            stat.S_IMODE(metadata.st_mode),
                        )
                        opened_identity = (
                            opened.st_dev,
                            opened.st_ino,
                            opened.st_size,
                            opened.st_mtime_ns,
                            stat.S_IMODE(opened.st_mode),
                        )
                        if identity != opened_identity or opened.st_nlink != 1:
                            raise AcceptedMaterialError("accepted_material_manifest_changed")
                        digest = hashlib.sha256()
                        remaining = opened.st_size
                        while remaining:
                            chunk = os.read(descriptor, min(64 * 1024, remaining))
                            if not chunk:
                                raise AcceptedMaterialError("accepted_material_manifest_changed")
                            digest.update(chunk)
                            remaining -= len(chunk)
                        if os.read(descriptor, 1):
                            raise AcceptedMaterialError("accepted_material_manifest_changed")
                    finally:
                        os.close(descriptor)
                    entry = AcceptedFileV1(
                        version=1,
                        path=relative,
                        file_type=AcceptedFileType.REGULAR,
                        size=metadata.st_size,
                        mode=mode,
                        digest=digest.hexdigest(),
                    )
                    total_bytes += metadata.st_size
                else:
                    raise AcceptedMaterialError("accepted_material_manifest_file_invalid")
                entries.append(entry)
                if len(entries) > _MAX_TOTAL_FILES or total_bytes > _MAX_TOTAL_BYTES:
                    raise AcceptedMaterialError("accepted_material_manifest_too_large")
    except AcceptedMaterialError:
        raise
    except (OSError, UnicodeError, ValueError) as exc:
        raise AcceptedMaterialError("accepted_material_manifest_invalid") from exc

    manifest = tuple(sorted(entries, key=lambda entry: entry.path))
    _validate_manifest_closure(manifest)
    return manifest


@dataclass(frozen=True, slots=True)
class AcceptedMaterialRequestV1:
    """Canonical provider-neutral request for one accepted snapshot."""

    version: Literal[1]
    run_id: str
    attempt_id: str
    tenant: TenantReferenceV1
    user_ref: str
    thread_ref: str
    agent_revision_digest: str
    skill_snapshot_digest: str
    skill_scope_digest: str
    file_manifest: tuple[AcceptedFileV1, ...]
    runtime_image_digest: str
    lease_expires_at: datetime
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted material request version must be 1")
        for field_name in ("run_id", "attempt_id", "user_ref", "thread_ref"):
            _require_reference(getattr(self, field_name), field_name)
        if not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        if type(self.tenant.version) is not int:
            raise ValueError("tenant version must be 1")
        for field_name in (
            "agent_revision_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "runtime_image_digest",
        ):
            _require_digest(getattr(self, field_name), field_name)
        if not isinstance(self.file_manifest, tuple) or len(self.file_manifest) > _MAX_TOTAL_FILES:
            raise ValueError("accepted material file manifest is invalid")
        if any(not isinstance(entry, AcceptedFileV1) for entry in self.file_manifest):
            raise TypeError("file_manifest must contain AcceptedFileV1 entries")
        paths = [entry.path for entry in self.file_manifest]
        if paths != sorted(paths):
            raise ValueError("accepted material file manifest must be sorted")
        if len(paths) != len(set(paths)) or len(paths) != len({path.casefold() for path in paths}):
            raise ValueError("accepted material file manifest has duplicate or case-conflicting paths")
        _validate_manifest_closure(self.file_manifest)
        total_bytes = sum(entry.size for entry in self.file_manifest)
        if total_bytes > _MAX_TOTAL_BYTES:
            raise ValueError("accepted material file manifest is too large")
        _canonical_timestamp(self.lease_expires_at)
        _require_digest(self.digest, "accepted material request digest")
        if self.digest != _canonical_digest(self._digest_payload()):
            raise ValueError("accepted material request digest is invalid")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "tenant": self.tenant.to_json(),
            "user_ref": self.user_ref,
            "thread_ref": self.thread_ref,
            "agent_revision_digest": self.agent_revision_digest,
            "skill_snapshot_digest": self.skill_snapshot_digest,
            "skill_scope_digest": self.skill_scope_digest,
            "file_manifest": [entry.to_persisted() for entry in self.file_manifest],
            "runtime_image_digest": self.runtime_image_digest,
            "lease_expires_at": _canonical_timestamp(self.lease_expires_at),
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    @classmethod
    def build(
        cls,
        *,
        run_id: str,
        attempt_id: str,
        tenant: TenantReferenceV1,
        user_ref: str,
        thread_ref: str,
        agent_revision_digest: str,
        skill_snapshot_digest: str,
        skill_scope_digest: str,
        file_manifest: Sequence[AcceptedFileV1],
        runtime_image_digest: str,
        lease_expires_at: datetime,
    ) -> Self:
        if not isinstance(file_manifest, Sequence) or isinstance(file_manifest, (str, bytes, bytearray)) or len(file_manifest) > _MAX_TOTAL_FILES:
            raise ValueError("accepted material file manifest is invalid")
        if any(not isinstance(entry, AcceptedFileV1) for entry in file_manifest):
            raise TypeError("file_manifest must contain AcceptedFileV1 entries")
        manifest = tuple(sorted(file_manifest, key=lambda entry: entry.path))
        payload = {
            "version": 1,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "tenant": tenant.to_json(),
            "user_ref": user_ref,
            "thread_ref": thread_ref,
            "agent_revision_digest": agent_revision_digest,
            "skill_snapshot_digest": skill_snapshot_digest,
            "skill_scope_digest": skill_scope_digest,
            "file_manifest": [entry.to_persisted() for entry in manifest],
            "runtime_image_digest": runtime_image_digest,
            "lease_expires_at": _canonical_timestamp(lease_expires_at),
        }
        return cls(
            version=1,
            run_id=run_id,
            attempt_id=attempt_id,
            tenant=tenant,
            user_ref=user_ref,
            thread_ref=thread_ref,
            agent_revision_digest=agent_revision_digest,
            skill_snapshot_digest=skill_snapshot_digest,
            skill_scope_digest=skill_scope_digest,
            file_manifest=manifest,
            runtime_image_digest=runtime_image_digest,
            lease_expires_at=lease_expires_at,
            digest=_canonical_digest(payload),
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "run_id",
            "attempt_id",
            "tenant",
            "user_ref",
            "thread_ref",
            "agent_revision_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "file_manifest",
            "runtime_image_digest",
            "lease_expires_at",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("accepted material request has unknown or missing fields")
        raw_manifest = value["file_manifest"]
        if (
            not isinstance(raw_manifest, Sequence)
            or isinstance(
                raw_manifest,
                (str, bytes, bytearray),
            )
            or len(raw_manifest) > _MAX_TOTAL_FILES
        ):
            raise ValueError("accepted material file manifest is invalid")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            run_id=value["run_id"],  # type: ignore[arg-type]
            attempt_id=value["attempt_id"],  # type: ignore[arg-type]
            tenant=TenantReferenceV1.from_json(value["tenant"]),
            user_ref=value["user_ref"],  # type: ignore[arg-type]
            thread_ref=value["thread_ref"],  # type: ignore[arg-type]
            agent_revision_digest=value["agent_revision_digest"],  # type: ignore[arg-type]
            skill_snapshot_digest=value["skill_snapshot_digest"],  # type: ignore[arg-type]
            skill_scope_digest=value["skill_scope_digest"],  # type: ignore[arg-type]
            file_manifest=tuple(AcceptedFileV1.from_persisted(entry) for entry in raw_manifest),
            runtime_image_digest=value["runtime_image_digest"],  # type: ignore[arg-type]
            lease_expires_at=_parse_timestamp(value["lease_expires_at"]),
            digest=value["digest"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class AcceptedMaterialLeaseV1:
    """Epoch-fenced provider lease with a process-local renewal handle."""

    version: Literal[1]
    provider_kind: str
    provider_instance_ref: str
    ownership_epoch: int
    lease_expires_at: datetime
    opaque_renewal_handle: object = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted material lease version must be 1")
        _require_reference(self.provider_kind, "provider_kind")
        _require_reference(self.provider_instance_ref, "provider_instance_ref")
        if type(self.ownership_epoch) is not int or self.ownership_epoch < 0:
            raise ValueError("ownership_epoch must be a non-negative integer")
        _canonical_timestamp(self.lease_expires_at)

    def to_persisted(self) -> dict[str, object]:
        return {
            "version": self.version,
            "provider_kind": self.provider_kind,
            "provider_instance_ref": self.provider_instance_ref,
            "ownership_epoch": self.ownership_epoch,
            "lease_expires_at": _canonical_timestamp(self.lease_expires_at),
        }


@dataclass(frozen=True, slots=True)
class AcceptedExecutionEvidenceV1:
    """Canonical execution evidence joined to a durable run start."""

    version: Literal[1]
    run_id: str
    attempt_id: str
    tenant: TenantReferenceV1
    provider_kind: str
    provider_instance_ref: str
    ownership_epoch: int
    runtime_image_digest: str
    skill_snapshot_digest: str
    skill_scope_digest: str
    materialization_digest: str
    verifier_image_digest: str
    verifier_contract_version: str
    read_only_proof_digest: str
    qualification_scope: str
    digest: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("accepted execution evidence version must be 1")
        for field_name in (
            "run_id",
            "attempt_id",
            "provider_kind",
            "provider_instance_ref",
            "verifier_contract_version",
            "qualification_scope",
        ):
            _require_reference(getattr(self, field_name), field_name)
        if not isinstance(self.tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        if type(self.tenant.version) is not int:
            raise ValueError("tenant version must be 1")
        if type(self.ownership_epoch) is not int or self.ownership_epoch < 0:
            raise ValueError("ownership_epoch must be a non-negative integer")
        for field_name in (
            "runtime_image_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "materialization_digest",
            "verifier_image_digest",
            "read_only_proof_digest",
        ):
            _require_digest(getattr(self, field_name), field_name)
        _require_digest(self.digest, "accepted execution evidence digest")
        if self.digest != _canonical_digest(self._digest_payload()):
            raise ValueError("accepted execution evidence digest is invalid")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "tenant": self.tenant.to_json(),
            "provider_kind": self.provider_kind,
            "provider_instance_ref": self.provider_instance_ref,
            "ownership_epoch": self.ownership_epoch,
            "runtime_image_digest": self.runtime_image_digest,
            "skill_snapshot_digest": self.skill_snapshot_digest,
            "skill_scope_digest": self.skill_scope_digest,
            "materialization_digest": self.materialization_digest,
            "verifier_image_digest": self.verifier_image_digest,
            "verifier_contract_version": self.verifier_contract_version,
            "read_only_proof_digest": self.read_only_proof_digest,
            "qualification_scope": self.qualification_scope,
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    def binds(
        self,
        request: AcceptedMaterialRequestV1,
        lease: AcceptedMaterialLeaseV1,
    ) -> bool:
        """Return whether all cross-boundary identity anchors agree."""

        return (
            self.run_id == request.run_id
            and self.attempt_id == request.attempt_id
            and self.tenant == request.tenant
            and self.provider_kind == lease.provider_kind
            and self.provider_instance_ref == lease.provider_instance_ref
            and self.ownership_epoch == lease.ownership_epoch
            and self.runtime_image_digest == request.runtime_image_digest
            and self.skill_snapshot_digest == request.skill_snapshot_digest
            and self.skill_scope_digest == request.skill_scope_digest
        )

    @classmethod
    def build(
        cls,
        *,
        run_id: str,
        attempt_id: str,
        tenant: TenantReferenceV1,
        provider_kind: str,
        provider_instance_ref: str,
        ownership_epoch: int,
        runtime_image_digest: str,
        skill_snapshot_digest: str,
        skill_scope_digest: str,
        materialization_digest: str,
        verifier_image_digest: str,
        verifier_contract_version: str,
        read_only_proof_digest: str,
        qualification_scope: str,
    ) -> Self:
        payload = {
            "version": 1,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "tenant": tenant.to_json(),
            "provider_kind": provider_kind,
            "provider_instance_ref": provider_instance_ref,
            "ownership_epoch": ownership_epoch,
            "runtime_image_digest": runtime_image_digest,
            "skill_snapshot_digest": skill_snapshot_digest,
            "skill_scope_digest": skill_scope_digest,
            "materialization_digest": materialization_digest,
            "verifier_image_digest": verifier_image_digest,
            "verifier_contract_version": verifier_contract_version,
            "read_only_proof_digest": read_only_proof_digest,
            "qualification_scope": qualification_scope,
        }
        return cls(
            version=1,
            run_id=run_id,
            attempt_id=attempt_id,
            tenant=tenant,
            provider_kind=provider_kind,
            provider_instance_ref=provider_instance_ref,
            ownership_epoch=ownership_epoch,
            runtime_image_digest=runtime_image_digest,
            skill_snapshot_digest=skill_snapshot_digest,
            skill_scope_digest=skill_scope_digest,
            materialization_digest=materialization_digest,
            verifier_image_digest=verifier_image_digest,
            verifier_contract_version=verifier_contract_version,
            read_only_proof_digest=read_only_proof_digest,
            qualification_scope=qualification_scope,
            digest=_canonical_digest(payload),
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "version",
            "run_id",
            "attempt_id",
            "tenant",
            "provider_kind",
            "provider_instance_ref",
            "ownership_epoch",
            "runtime_image_digest",
            "skill_snapshot_digest",
            "skill_scope_digest",
            "materialization_digest",
            "verifier_image_digest",
            "verifier_contract_version",
            "read_only_proof_digest",
            "qualification_scope",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("accepted execution evidence has unknown or missing fields")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            run_id=value["run_id"],  # type: ignore[arg-type]
            attempt_id=value["attempt_id"],  # type: ignore[arg-type]
            tenant=TenantReferenceV1.from_json(value["tenant"]),
            provider_kind=value["provider_kind"],  # type: ignore[arg-type]
            provider_instance_ref=value["provider_instance_ref"],  # type: ignore[arg-type]
            ownership_epoch=value["ownership_epoch"],  # type: ignore[arg-type]
            runtime_image_digest=value["runtime_image_digest"],  # type: ignore[arg-type]
            skill_snapshot_digest=value["skill_snapshot_digest"],  # type: ignore[arg-type]
            skill_scope_digest=value["skill_scope_digest"],  # type: ignore[arg-type]
            materialization_digest=value["materialization_digest"],  # type: ignore[arg-type]
            verifier_image_digest=value["verifier_image_digest"],  # type: ignore[arg-type]
            verifier_contract_version=value["verifier_contract_version"],  # type: ignore[arg-type]
            read_only_proof_digest=value["read_only_proof_digest"],  # type: ignore[arg-type]
            qualification_scope=value["qualification_scope"],  # type: ignore[arg-type]
            digest=value["digest"],  # type: ignore[arg-type]
        )


class AcceptedMaterializer(Protocol):
    """Provider adapter for accepted immutable material and its lease."""

    def capability(self) -> AcceptedMaterialCapability: ...

    async def acquire_and_materialize(
        self,
        request: AcceptedMaterialRequestV1,
    ) -> tuple[Sandbox, AcceptedMaterialLeaseV1, AcceptedExecutionEvidenceV1]: ...

    async def validate(
        self,
        lease: AcceptedMaterialLeaseV1,
        evidence: AcceptedExecutionEvidenceV1,
    ) -> bool: ...

    async def renew(
        self,
        lease: AcceptedMaterialLeaseV1,
    ) -> AcceptedMaterialLeaseV1: ...

    async def release(self, lease: AcceptedMaterialLeaseV1) -> None: ...


@dataclass(frozen=True, slots=True)
class AcceptedMaterializerSelection:
    """Process-local adapter selection returned by an opted-in provider."""

    materializer: AcceptedMaterializer
    runtime_image_digest: str
    lease_duration: timedelta

    def __post_init__(self) -> None:
        if self.materializer.capability() is not AcceptedMaterialCapability.IMMUTABLE_READ_ONLY:
            raise ValueError(
                "accepted materializer selection must provide immutable read-only material",
            )
        _require_digest(
            self.runtime_image_digest,
            "accepted materializer runtime image digest",
        )
        if not isinstance(self.lease_duration, timedelta) or self.lease_duration <= timedelta(0) or self.lease_duration > timedelta(hours=1):
            raise ValueError(
                "accepted materializer lease duration must be between zero and one hour",
            )


async def resolve_accepted_materializer(
    provider: object,
    *,
    binding: AcceptedSkillSandboxBindingV1,
    thread_id: str,
    user_id: str,
) -> AcceptedMaterializerSelection | None:
    """Resolve an optional provider adapter without exposing its concrete type."""

    hook = getattr(provider, "accepted_materializer_selection", None)
    if hook is None:
        return None
    if not callable(hook):
        raise TypeError("accepted_materializer_selection must be callable")
    selection = await hook(
        binding=binding,
        thread_id=thread_id,
        user_id=user_id,
    )
    if selection is not None and not isinstance(
        selection,
        AcceptedMaterializerSelection,
    ):
        raise TypeError(
            "accepted_materializer_selection must return AcceptedMaterializerSelection or None",
        )
    return selection


class _InMemoryAcceptedSandbox(Sandbox):
    """Identity-only sandbox used exclusively by contract tests."""

    @staticmethod
    def _unsupported() -> NoReturn:
        raise RuntimeError("in-memory accepted material sandbox is contract-test-only")

    def execute_command(
        self,
        command: str,
        env: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> str:
        del command, env, timeout
        self._unsupported()

    def read_file(
        self,
        path: str,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        del path, start_line, end_line
        self._unsupported()

    def download_file(self, path: str) -> bytes:
        del path
        self._unsupported()

    def list_dir(self, path: str, max_depth: int = 2) -> list[str]:
        del path, max_depth
        self._unsupported()

    def write_file(self, path: str, content: str, append: bool = False) -> None:
        del path, content, append
        self._unsupported()

    def glob(
        self,
        path: str,
        pattern: str,
        *,
        include_dirs: bool = False,
        max_results: int = 200,
    ) -> tuple[list[str], bool]:
        del path, pattern, include_dirs, max_results
        self._unsupported()

    def grep(
        self,
        path: str,
        pattern: str,
        *,
        glob: str | None = None,
        literal: bool = False,
        case_sensitive: bool = False,
        max_results: int = 100,
    ) -> tuple[list[Any], bool]:
        del path, pattern, glob, literal, case_sensitive, max_results
        self._unsupported()

    def update_file(self, path: str, content: bytes) -> None:
        del path, content
        self._unsupported()


@dataclass(slots=True)
class _InMemoryAcceptedRecord:
    request: AcceptedMaterialRequestV1
    sandbox: Sandbox
    owner: str
    lease: AcceptedMaterialLeaseV1
    evidence: AcceptedExecutionEvidenceV1
    released: bool = False


class InMemoryAcceptedMaterialState:
    """Shared state used to exercise process-recovery ownership fencing."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.records: dict[
            tuple[str, str, str],
            _InMemoryAcceptedRecord,
        ] = {}
        self.records_by_provider_ref: dict[str, _InMemoryAcceptedRecord] = {}


class InMemoryAcceptedMaterializer:
    """Stateful contract-test adapter; never a production provider."""

    def __init__(
        self,
        *,
        owner: str,
        state: InMemoryAcceptedMaterialState | None = None,
        clock: Callable[[], datetime] | None = None,
        lease_duration: timedelta = timedelta(minutes=5),
    ) -> None:
        _require_reference(owner, "owner")
        if not isinstance(lease_duration, timedelta) or lease_duration <= timedelta(0) or lease_duration > timedelta(hours=1):
            raise ValueError("lease_duration must be between zero and one hour")
        self._owner = owner
        self._state = state or InMemoryAcceptedMaterialState()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._lease_duration = lease_duration

    def capability(self) -> AcceptedMaterialCapability:
        return AcceptedMaterialCapability.IMMUTABLE_READ_ONLY

    @staticmethod
    def _key(
        request: AcceptedMaterialRequestV1,
    ) -> tuple[str, str, str]:
        return (request.tenant.digest, request.run_id, request.attempt_id)

    @staticmethod
    def _provider_ref(request: AcceptedMaterialRequestV1) -> str:
        suffix = _canonical_digest(
            {
                "version": 1,
                "tenant_digest": request.tenant.digest,
                "run_id": request.run_id,
                "attempt_id": request.attempt_id,
            },
        )[:32]
        return f"in-memory-{suffix}"

    def _new_tuple(
        self,
        *,
        request: AcceptedMaterialRequestV1,
        sandbox: Sandbox,
        epoch: int,
        expires_at: datetime,
    ) -> tuple[AcceptedMaterialLeaseV1, AcceptedExecutionEvidenceV1]:
        provider_ref = sandbox.id
        handle = object()
        lease = AcceptedMaterialLeaseV1(
            version=1,
            provider_kind="in_memory",
            provider_instance_ref=provider_ref,
            ownership_epoch=epoch,
            lease_expires_at=expires_at,
            opaque_renewal_handle=handle,
        )
        evidence = AcceptedExecutionEvidenceV1.build(
            run_id=request.run_id,
            attempt_id=request.attempt_id,
            tenant=request.tenant,
            provider_kind=lease.provider_kind,
            provider_instance_ref=provider_ref,
            ownership_epoch=epoch,
            runtime_image_digest=request.runtime_image_digest,
            skill_snapshot_digest=request.skill_snapshot_digest,
            skill_scope_digest=request.skill_scope_digest,
            materialization_digest=request.digest,
            verifier_image_digest=hashlib.sha256(
                b"in-memory-accepted-material-verifier-v1",
            ).hexdigest(),
            verifier_contract_version="in_memory_contract_v1",
            read_only_proof_digest=_canonical_digest(
                {
                    "version": 1,
                    "request_digest": request.digest,
                    "provider_instance_ref": provider_ref,
                    "ownership_epoch": epoch,
                    "proof": "contract-test-fence",
                },
            ),
            qualification_scope="contract_test_only",
        )
        return lease, evidence

    async def acquire_and_materialize(
        self,
        request: AcceptedMaterialRequestV1,
    ) -> tuple[Sandbox, AcceptedMaterialLeaseV1, AcceptedExecutionEvidenceV1]:
        if not isinstance(request, AcceptedMaterialRequestV1):
            raise TypeError("request must be AcceptedMaterialRequestV1")
        now = self._clock()
        _canonical_timestamp(now)
        key = self._key(request)
        with self._state.lock:
            current = self._state.records.get(key)
            if current is not None and current.request.digest != request.digest:
                raise AcceptedMaterialError("accepted_material_request_conflict")
            if current is not None and not current.released and current.lease.lease_expires_at > now:
                if current.owner != self._owner:
                    raise AcceptedMaterialError("accepted_material_claim_conflict")
                return current.sandbox, current.lease, current.evidence

            if current is None:
                if request.lease_expires_at <= now:
                    raise AcceptedMaterialError("accepted_material_lease_expired")
                sandbox = _InMemoryAcceptedSandbox(self._provider_ref(request))
                epoch = 1
                expires_at = request.lease_expires_at
            else:
                sandbox = current.sandbox
                epoch = current.lease.ownership_epoch + 1
                expires_at = now + self._lease_duration
            lease, evidence = self._new_tuple(
                request=request,
                sandbox=sandbox,
                epoch=epoch,
                expires_at=expires_at,
            )
            record = _InMemoryAcceptedRecord(
                request=request,
                sandbox=sandbox,
                owner=self._owner,
                lease=lease,
                evidence=evidence,
            )
            self._state.records[key] = record
            self._state.records_by_provider_ref[sandbox.id] = record
            return sandbox, lease, evidence

    async def validate(
        self,
        lease: AcceptedMaterialLeaseV1,
        evidence: AcceptedExecutionEvidenceV1,
    ) -> bool:
        if not isinstance(lease, AcceptedMaterialLeaseV1) or not isinstance(
            evidence,
            AcceptedExecutionEvidenceV1,
        ):
            return False
        now = self._clock()
        with self._state.lock:
            current = self._state.records_by_provider_ref.get(
                lease.provider_instance_ref,
            )
            return bool(
                current is not None and not current.released and current.owner == self._owner and current.lease is lease and current.evidence == evidence and current.lease.lease_expires_at > now and evidence.binds(current.request, lease)
            )

    async def renew(
        self,
        lease: AcceptedMaterialLeaseV1,
    ) -> AcceptedMaterialLeaseV1:
        if not isinstance(lease, AcceptedMaterialLeaseV1):
            raise TypeError("lease must be AcceptedMaterialLeaseV1")
        now = self._clock()
        with self._state.lock:
            current = self._state.records_by_provider_ref.get(
                lease.provider_instance_ref,
            )
            if current is None or current.released or current.owner != self._owner or current.lease is not lease or current.lease.lease_expires_at <= now:
                raise AcceptedMaterialError("accepted_material_lease_lost")
            renewed = AcceptedMaterialLeaseV1(
                version=1,
                provider_kind=lease.provider_kind,
                provider_instance_ref=lease.provider_instance_ref,
                ownership_epoch=lease.ownership_epoch,
                lease_expires_at=now + self._lease_duration,
                opaque_renewal_handle=lease.opaque_renewal_handle,
            )
            current.lease = renewed
            return renewed

    async def release(self, lease: AcceptedMaterialLeaseV1) -> None:
        if not isinstance(lease, AcceptedMaterialLeaseV1):
            raise TypeError("lease must be AcceptedMaterialLeaseV1")
        with self._state.lock:
            current = self._state.records_by_provider_ref.get(
                lease.provider_instance_ref,
            )
            if current is not None and current.owner == self._owner and current.lease is lease:
                current.released = True


__all__ = [
    "AcceptedExecutionEvidenceV1",
    "AcceptedFileType",
    "AcceptedFileV1",
    "AcceptedMaterialCapability",
    "AcceptedMaterialError",
    "AcceptedMaterialLeaseV1",
    "AcceptedMaterialRequestV1",
    "AcceptedMaterializer",
    "AcceptedMaterializerSelection",
    "AcceptedSkillExecutionEvidence",
    "AcceptedSkillExecutionEvidenceV1",
    "AcceptedSkillExecutionEvidenceV2",
    "AcceptedSkillSandboxBindingError",
    "AcceptedSkillSandboxBindingV1",
    "capture_accepted_file_manifest",
    "InMemoryAcceptedMaterialState",
    "InMemoryAcceptedMaterializer",
    "resolve_accepted_materializer",
]
