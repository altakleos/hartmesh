"""Canonical provenance for managed extension sources and installed bytes.

This module is the sole parser and verifier for extension source locks and
installed artifact manifests.  Serialized documents contain only normalized,
bounded identifiers and SHA-256 digests; paths used while verifying an
installation never enter the documents.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import stat
import tomllib
import urllib.parse
import urllib.request
import uuid
from base64 import urlsafe_b64decode
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Any, Literal, Self

from packaging.requirements import InvalidRequirement, Requirement
from packaging.tags import sys_tags

_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$", re.ASCII)
_DISTRIBUTION = re.compile(
    r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$",
    re.ASCII,
)
_VERSION = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._+!-]*[A-Za-z0-9])?$", re.ASCII)
_ENTRY_POINT_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$", re.ASCII)
_GIT_REVISION = re.compile(r"^[0-9a-f]{40,64}$", re.ASCII)
_MAX_REFERENCE_BYTES = 2048
_MAX_PROVENANCE_DOCUMENT_BYTES = 4 * 1024 * 1024
_MAX_EXTENSION_ENTRIES = 256
SNAPSHOT_IGNORED_DIRECTORY_NAMES = frozenset({".git", ".ruff_cache", ".venv", "venv", "__pycache__"})
SNAPSHOT_IGNORED_FILE_SUFFIXES = (".pyc",)
_SENSITIVE_FILENAMES = frozenset({".npmrc", ".pypirc", "credentials.json"})
_SENSITIVE_SUFFIXES = frozenset({".key", ".pem", ".p12", ".pfx"})
_MAX_SNAPSHOT_FILES = 50_000
_MAX_SNAPSHOT_BYTES = 1024 * 1024 * 1024
_MAX_INSTALLED_FILES = 50_000
_MAX_INSTALLED_BYTES = 1024 * 1024 * 1024
_MAX_CONFIG_DEPTH = 8
_MAX_CONFIG_ITEMS = 256
_MAX_CONFIG_PLUGINS = 256
_MAX_CONFIG_STRING_BYTES = 4096
_MAX_CONFIG_CANONICAL_BYTES = 256 * 1024
_EXECUTABLE_SUFFIXES = frozenset({".py", ".pyi", ".pyw", ".so", ".pyd", ".dll", ".dylib"})
_SECRET_FIELD_TOKENS = frozenset(
    {
        "auth",
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "header",
        "headers",
        "passwd",
        "password",
        "pwd",
        "secret",
        "session",
        "token",
    }
)
_SECRET_FIELD_PAIRS = frozenset(
    {
        ("access", "key"),
        ("api", "key"),
        ("private", "key"),
        ("secret", "key"),
        ("session", "key"),
    }
)
_SECRET_FIELD_COMPOUNDS = frozenset({"accesskey", "apikey", "privatekey", "secretkey", "sessionkey"})
_SECRET_FIELD_EMBEDDED_TERMS = frozenset(
    {
        "authorization",
        "cookie",
        "credential",
        "credentials",
        "header",
        "headers",
        "passwd",
        "password",
        "secret",
        "session",
        "token",
    }
)
_PLUGIN_FIELDS = frozenset({"enabled", "name", "package", "use", "config", "required", "table_prefix"})


class ExtensionArtifactVerificationError(RuntimeError):
    """A stable, non-secret extension provenance verification failure."""

    def __init__(
        self,
        code: str,
        *,
        distribution: str | None = None,
        expected_digest: str | None = None,
        actual_digest: str | None = None,
        correlation_id: str | None = None,
    ) -> None:
        self.code = code
        self.distribution = normalize_distribution_name(distribution) if distribution is not None else None
        self.expected_digest = expected_digest
        self.actual_digest = actual_digest
        self.correlation_id = correlation_id or uuid.uuid4().hex
        facts = [code]
        if self.distribution is not None:
            facts.append(f"distribution={self.distribution}")
        if expected_digest is not None:
            facts.append(f"expected={expected_digest[:19]}")
        if actual_digest is not None:
            facts.append(f"actual={actual_digest[:19]}")
        facts.append(f"correlation_id={self.correlation_id}")
        super().__init__(" ".join(facts))


class _DuplicateJsonKeyError(ValueError):
    pass


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKeyError("duplicate JSON object key")
        result[key] = value
    return result


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _digest(value: object) -> str:
    return f"sha256:{hashlib.sha256(_canonical_json_bytes(value)).hexdigest()}"


UNVERIFIED_EXTENSION_ARTIFACT_MANIFEST_DIGEST = _digest({"version": 1, "status": "unverified_local_development"})


def _require_digest(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be sha256 followed by 64 lowercase hexadecimal characters")
    return value


def normalize_distribution_name(value: str) -> str:
    """Return the PEP 503 comparison key for a bounded distribution name."""

    if not isinstance(value, str) or _DISTRIBUTION.fullmatch(value) is None:
        raise ValueError("extension distribution must be a valid Python distribution name")
    return re.sub(r"[-_.]+", "-", value).lower()


def _require_text(value: object, *, field_name: str, max_bytes: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > max_bytes or any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ValueError(f"{field_name} must be a bounded non-empty string")
    return value


def _require_entry_point(name: object, value: object) -> tuple[str, str]:
    if not isinstance(name, str) or _ENTRY_POINT_NAME.fullmatch(name) is None:
        raise ValueError("extension entry-point name is invalid")
    target = _require_text(value, field_name="extension entry-point value")
    module, separator, attribute = target.rpartition(":")
    if separator != ":" or not attribute.isidentifier() or not module or any(not part.isidentifier() for part in module.split(".")):
        raise ValueError("extension entry-point value is invalid")
    return name, target


def _require_safe_source_reference(value: object, *, source_kind: str) -> str:
    reference = _require_text(
        value,
        field_name="extension source reference",
        max_bytes=_MAX_REFERENCE_BYTES,
    )
    if source_kind == "local_snapshot":
        if reference.startswith(("/", "\\")) or "\\" in reference:
            raise ValueError("local snapshot source reference must be a normalized relative path")
        parts = reference.split("/")
        if any(part in {"", ".", ".."} for part in parts):
            raise ValueError("local snapshot source reference must be a normalized relative path")
        return reference
    parsed = urllib.parse.urlsplit(reference.removeprefix("git+"))
    loopback_http = parsed.scheme == "http" and parsed.hostname in {
        "localhost",
        "127.0.0.1",
        "::1",
    }
    if (parsed.scheme != "https" and not loopback_http) or parsed.hostname is None:
        raise ValueError("remote extension provenance must use HTTPS")
    if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
        raise ValueError("remote extension provenance must not contain credentials, query parameters, or fragments")
    scheme = "http" if loopback_http else "https"
    return urllib.parse.urlunsplit((scheme, parsed.netloc.lower(), parsed.path, "", ""))


def _is_link_like(path: Path) -> bool:
    return path.is_symlink() or path.is_junction()


def _is_ignored_snapshot_file(name: str) -> bool:
    return name.endswith(SNAPSHOT_IGNORED_FILE_SUFFIXES)


def validate_local_snapshot(source: str | Path) -> Path:
    """Validate the exact tree policy used for managed source snapshots."""

    root = Path(source).resolve()
    if not root.is_dir():
        raise ValueError("local extension snapshot source must be a directory")
    file_count = 0
    total_size = 0
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        retained_dirs: list[str] = []
        for name in sorted(dirnames):
            if name in SNAPSHOT_IGNORED_DIRECTORY_NAMES:
                continue
            candidate = directory_path / name
            if _is_link_like(candidate):
                raise ValueError("local extension snapshots cannot contain symbolic links or junctions")
            if not candidate.is_dir():
                raise ValueError("local extension snapshots may contain only directories and regular files")
            retained_dirs.append(name)
        dirnames[:] = retained_dirs
        for name in sorted(filenames):
            if _is_ignored_snapshot_file(name):
                continue
            candidate = directory_path / name
            if name == ".env" or name.startswith(".env.") or name in _SENSITIVE_FILENAMES or candidate.suffix.lower() in _SENSITIVE_SUFFIXES:
                raise ValueError(f"local extension snapshot contains a likely sensitive file: {name}")
            if _is_link_like(candidate):
                raise ValueError("local extension snapshots cannot contain symbolic links or junctions")
            metadata = candidate.stat()
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("local extension snapshots may contain only directories and regular files")
            file_count += 1
            total_size += metadata.st_size
            if file_count > _MAX_SNAPSHOT_FILES or total_size > _MAX_SNAPSHOT_BYTES:
                raise ValueError("local extension snapshot exceeds the bounded tree policy")
    return root


def hash_local_snapshot_tree(source: str | Path) -> str:
    """Hash a validated snapshot independently of walk order or host paths."""

    root = validate_local_snapshot(source)
    entries: list[dict[str, object]] = []
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        directory_path = Path(directory)
        dirnames[:] = sorted(name for name in dirnames if name not in SNAPSHOT_IGNORED_DIRECTORY_NAMES)
        for name in dirnames:
            candidate = directory_path / name
            relative = candidate.relative_to(root).as_posix()
            entries.append(
                {
                    "path": relative,
                    "type": "directory",
                    "mode_class": "directory",
                    "size": 0,
                    "content_digest": None,
                }
            )
        for name in sorted(filenames):
            if _is_ignored_snapshot_file(name):
                continue
            candidate = directory_path / name
            metadata = candidate.stat()
            entries.append(
                {
                    "path": candidate.relative_to(root).as_posix(),
                    "type": "file",
                    "mode_class": ("executable" if metadata.st_mode & (stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH) else "regular"),
                    "size": metadata.st_size,
                    "content_digest": f"sha256:{hashlib.sha256(candidate.read_bytes()).hexdigest()}",
                }
            )
    entries.sort(key=lambda item: str(item["path"]).encode("utf-8"))
    return _digest({"version": 1, "entries": entries})


@dataclass(frozen=True, slots=True)
class ExtensionSourceLockEntryV1:
    """Canonical source identity for one managed extension distribution."""

    distribution: str
    distribution_version: str
    entry_point_name: str
    entry_point_value: str
    source_kind: Literal["registry", "git", "local_snapshot"]
    source_reference: str
    source_revision: str | None
    locked_artifact_hashes: tuple[str, ...]
    local_tree_digest: str | None
    entry_digest: str

    def __post_init__(self) -> None:
        normalize_distribution_name(self.distribution)
        if not isinstance(self.distribution_version, str) or _VERSION.fullmatch(self.distribution_version) is None:
            raise ValueError("extension distribution version is invalid")
        _require_entry_point(self.entry_point_name, self.entry_point_value)
        if self.source_kind not in ("registry", "git", "local_snapshot"):
            raise ValueError("extension source kind is invalid")
        normalized_reference = _require_safe_source_reference(
            self.source_reference,
            source_kind=self.source_kind,
        )
        object.__setattr__(self, "source_reference", normalized_reference)
        hashes = tuple(sorted(self.locked_artifact_hashes))
        if len(hashes) != len(set(hashes)):
            raise ValueError("locked extension artifact hashes must be unique")
        for item in hashes:
            _require_digest(item, field_name="locked artifact hash")
        object.__setattr__(self, "locked_artifact_hashes", hashes)
        if self.source_kind == "registry":
            if self.source_revision is not None or self.local_tree_digest is not None or not hashes:
                raise ValueError("registry provenance requires artifact hashes only")
        elif self.source_kind == "git":
            if not isinstance(self.source_revision, str) or _GIT_REVISION.fullmatch(self.source_revision) is None:
                raise ValueError("Git extension provenance requires an immutable full commit revision")
            if self.local_tree_digest is not None:
                raise ValueError("Git extension provenance cannot contain a local tree digest")
        else:
            if self.source_revision is not None or hashes:
                raise ValueError("local snapshot provenance cannot contain remote artifact facts")
            _require_digest(self.local_tree_digest, field_name="local snapshot tree digest")
        _require_digest(self.entry_digest, field_name="source entry digest")
        if self.entry_digest != _digest(self._without_digest()):
            raise ValueError("extension source entry digest does not match its canonical fields")

    @classmethod
    def create(cls, **values: Any) -> Self:
        source_kind = values["source_kind"]
        payload = {
            "distribution": values["distribution"],
            "distribution_version": values["distribution_version"],
            "entry_point_name": values["entry_point_name"],
            "entry_point_value": values["entry_point_value"],
            "source_kind": source_kind,
            "source_reference": _require_safe_source_reference(
                values["source_reference"],
                source_kind=source_kind,
            ),
            "source_revision": values["source_revision"],
            "locked_artifact_hashes": sorted(values["locked_artifact_hashes"]),
            "local_tree_digest": values["local_tree_digest"],
        }
        return cls(**payload, entry_digest=_digest(payload))

    def _without_digest(self) -> dict[str, object]:
        return {
            "distribution": self.distribution,
            "distribution_version": self.distribution_version,
            "entry_point_name": self.entry_point_name,
            "entry_point_value": self.entry_point_value,
            "source_kind": self.source_kind,
            "source_reference": self.source_reference,
            "source_revision": self.source_revision,
            "locked_artifact_hashes": list(self.locked_artifact_hashes),
            "local_tree_digest": self.local_tree_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self._without_digest(), "entry_digest": self.entry_digest}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> Self:
        expected = {
            "distribution",
            "distribution_version",
            "entry_point_name",
            "entry_point_value",
            "source_kind",
            "source_reference",
            "source_revision",
            "locked_artifact_hashes",
            "local_tree_digest",
            "entry_digest",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ValueError("extension source entry has unknown or missing fields")
        hashes = value["locked_artifact_hashes"]
        if not isinstance(hashes, list):
            raise ValueError("locked extension artifact hashes must be a list")
        return cls(
            distribution=value["distribution"],  # type: ignore[arg-type]
            distribution_version=value["distribution_version"],  # type: ignore[arg-type]
            entry_point_name=value["entry_point_name"],  # type: ignore[arg-type]
            entry_point_value=value["entry_point_value"],  # type: ignore[arg-type]
            source_kind=value["source_kind"],  # type: ignore[arg-type]
            source_reference=value["source_reference"],  # type: ignore[arg-type]
            source_revision=value["source_revision"],  # type: ignore[arg-type]
            locked_artifact_hashes=tuple(hashes),  # type: ignore[arg-type]
            local_tree_digest=value["local_tree_digest"],  # type: ignore[arg-type]
            entry_digest=value["entry_digest"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class ExtensionSourceLockV1:
    """Platform-neutral source provenance for one managed extension set."""

    version: Literal[1]
    extension_api_version: str
    entries: tuple[ExtensionSourceLockEntryV1, ...]
    digest: str

    def __post_init__(self) -> None:
        if self.version != 1 or type(self.version) is not int:
            raise ValueError("extension source lock version must be 1")
        if not isinstance(self.extension_api_version, str) or not self.extension_api_version:
            raise ValueError("extension_api_version must be a non-empty string")
        entries = tuple(self.entries)
        if len(entries) > _MAX_EXTENSION_ENTRIES:
            raise ValueError("extension source lock exceeds the entry limit")
        if any(not isinstance(item, ExtensionSourceLockEntryV1) for item in entries):
            raise TypeError("extension source lock entries must use ExtensionSourceLockEntryV1")
        sorted_entries = tuple(
            sorted(
                entries,
                key=lambda item: (
                    normalize_distribution_name(item.distribution),
                    item.entry_point_name,
                ),
            )
        )
        if entries != sorted_entries:
            raise ValueError("extension source lock entries must use canonical order")
        identities = [(normalize_distribution_name(item.distribution), item.entry_point_name) for item in entries]
        if len(identities) != len(set(identities)):
            raise ValueError("extension source lock contains a duplicate entry")
        object.__setattr__(self, "entries", entries)
        _require_digest(self.digest, field_name="source lock digest")
        if self.digest != _digest(self._without_digest()):
            raise ValueError("extension source lock digest does not match its canonical fields")

    @classmethod
    def create(
        cls,
        *,
        extension_api_version: str,
        entries: Sequence[ExtensionSourceLockEntryV1],
    ) -> Self:
        normalized_entries = tuple(
            sorted(
                entries,
                key=lambda item: (
                    normalize_distribution_name(item.distribution),
                    item.entry_point_name,
                ),
            )
        )
        payload = {
            "version": 1,
            "extension_api_version": extension_api_version,
            "entries": [item.to_dict() for item in normalized_entries],
        }
        return cls(
            version=1,
            extension_api_version=extension_api_version,
            entries=normalized_entries,
            digest=_digest(payload),
        )

    def _without_digest(self) -> dict[str, object]:
        return {
            "version": self.version,
            "extension_api_version": self.extension_api_version,
            "entries": [item.to_dict() for item in self.entries],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self._without_digest(), "digest": self.digest}

    def to_json(self) -> str:
        return (
            json.dumps(
                self.to_dict(),
                ensure_ascii=False,
                indent=2,
            )
            + "\n"
        )

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> Self:
        expected = {"version", "extension_api_version", "entries", "digest"}
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ValueError("extension source lock has unknown or missing fields")
        entries = value["entries"]
        if not isinstance(entries, list):
            raise ValueError("extension source lock entries are malformed")
        if any(not isinstance(item, Mapping) for item in entries):
            raise ValueError("extension source lock entries are malformed")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            extension_api_version=value["extension_api_version"],  # type: ignore[arg-type]
            entries=tuple(ExtensionSourceLockEntryV1.from_dict(item) for item in entries),
            digest=value["digest"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class InstalledExtensionArtifactV1:
    """Verified installed bytes for one source-locked extension."""

    source_entry_digest: str
    distribution: str
    distribution_version: str
    entry_point_name: str
    entry_point_value: str
    selected_artifact_hash: str | None
    installed_record_digest: str
    entry_digest: str

    def __post_init__(self) -> None:
        _require_digest(self.source_entry_digest, field_name="source entry digest")
        normalize_distribution_name(self.distribution)
        if not isinstance(self.distribution_version, str) or _VERSION.fullmatch(self.distribution_version) is None:
            raise ValueError("installed extension distribution version is invalid")
        _require_entry_point(self.entry_point_name, self.entry_point_value)
        if self.selected_artifact_hash is not None:
            _require_digest(
                self.selected_artifact_hash,
                field_name="selected extension artifact hash",
            )
        _require_digest(
            self.installed_record_digest,
            field_name="installed extension RECORD digest",
        )
        _require_digest(self.entry_digest, field_name="installed artifact entry digest")
        if self.entry_digest != _digest(self._without_digest()):
            raise ValueError("installed extension artifact entry digest does not match its canonical fields")

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "source_entry_digest": values["source_entry_digest"],
            "distribution": values["distribution"],
            "distribution_version": values["distribution_version"],
            "entry_point_name": values["entry_point_name"],
            "entry_point_value": values["entry_point_value"],
            "selected_artifact_hash": values["selected_artifact_hash"],
            "installed_record_digest": values["installed_record_digest"],
        }
        return cls(**payload, entry_digest=_digest(payload))

    def _without_digest(self) -> dict[str, object]:
        return {
            "source_entry_digest": self.source_entry_digest,
            "distribution": self.distribution,
            "distribution_version": self.distribution_version,
            "entry_point_name": self.entry_point_name,
            "entry_point_value": self.entry_point_value,
            "selected_artifact_hash": self.selected_artifact_hash,
            "installed_record_digest": self.installed_record_digest,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self._without_digest(), "entry_digest": self.entry_digest}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> Self:
        expected = {
            "source_entry_digest",
            "distribution",
            "distribution_version",
            "entry_point_name",
            "entry_point_value",
            "selected_artifact_hash",
            "installed_record_digest",
            "entry_digest",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ValueError("installed extension artifact entry has unknown or missing fields")
        return cls(
            source_entry_digest=value["source_entry_digest"],  # type: ignore[arg-type]
            distribution=value["distribution"],  # type: ignore[arg-type]
            distribution_version=value["distribution_version"],  # type: ignore[arg-type]
            entry_point_name=value["entry_point_name"],  # type: ignore[arg-type]
            entry_point_value=value["entry_point_value"],  # type: ignore[arg-type]
            selected_artifact_hash=value["selected_artifact_hash"],  # type: ignore[arg-type]
            installed_record_digest=value["installed_record_digest"],  # type: ignore[arg-type]
            entry_digest=value["entry_digest"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class ExtensionArtifactManifestV1:
    """Canonical platform-specific manifest of installed extension bytes."""

    version: Literal[1]
    source_lock_digest: str
    extension_api_version: str
    platform_tag: str
    entries: tuple[InstalledExtensionArtifactV1, ...]
    digest: str

    def __post_init__(self) -> None:
        if self.version != 1 or type(self.version) is not int:
            raise ValueError("extension artifact manifest version must be 1")
        _require_digest(self.source_lock_digest, field_name="source lock digest")
        _require_text(
            self.extension_api_version,
            field_name="extension_api_version",
            max_bytes=64,
        )
        platform_tag = _require_text(
            self.platform_tag,
            field_name="extension artifact platform tag",
            max_bytes=256,
        )
        if platform_tag != platform_tag.lower() or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789_.-" for character in platform_tag):
            raise ValueError("extension artifact platform tag is invalid")
        entries = tuple(self.entries)
        if len(entries) > _MAX_EXTENSION_ENTRIES:
            raise ValueError("extension artifact manifest exceeds the entry limit")
        if any(not isinstance(item, InstalledExtensionArtifactV1) for item in entries):
            raise TypeError("artifact manifest entries must use InstalledExtensionArtifactV1")
        canonical_entries = tuple(
            sorted(
                entries,
                key=lambda item: (
                    normalize_distribution_name(item.distribution),
                    item.entry_point_name,
                ),
            )
        )
        if entries != canonical_entries:
            raise ValueError("extension artifact manifest entries must use canonical order")
        identities = [(normalize_distribution_name(item.distribution), item.entry_point_name) for item in entries]
        if len(identities) != len(set(identities)):
            raise ValueError("extension artifact manifest contains a duplicate entry")
        object.__setattr__(self, "entries", entries)
        _require_digest(self.digest, field_name="extension artifact manifest digest")
        if self.digest != _digest(self._without_digest()):
            raise ValueError("extension artifact manifest digest does not match its canonical fields")

    @classmethod
    def create(
        cls,
        *,
        source_lock_digest: str,
        extension_api_version: str,
        platform_tag: str,
        entries: Sequence[InstalledExtensionArtifactV1],
    ) -> Self:
        normalized_entries = tuple(
            sorted(
                entries,
                key=lambda item: (
                    normalize_distribution_name(item.distribution),
                    item.entry_point_name,
                ),
            )
        )
        payload = {
            "version": 1,
            "source_lock_digest": source_lock_digest,
            "extension_api_version": extension_api_version,
            "platform_tag": platform_tag,
            "entries": [item.to_dict() for item in normalized_entries],
        }
        return cls(
            version=1,
            source_lock_digest=source_lock_digest,
            extension_api_version=extension_api_version,
            platform_tag=platform_tag,
            entries=normalized_entries,
            digest=_digest(payload),
        )

    def _without_digest(self) -> dict[str, object]:
        return {
            "version": self.version,
            "source_lock_digest": self.source_lock_digest,
            "extension_api_version": self.extension_api_version,
            "platform_tag": self.platform_tag,
            "entries": [item.to_dict() for item in self.entries],
        }

    def to_dict(self) -> dict[str, object]:
        return {**self._without_digest(), "digest": self.digest}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2) + "\n"

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> Self:
        expected = {
            "version",
            "source_lock_digest",
            "extension_api_version",
            "platform_tag",
            "entries",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            raise ValueError("extension artifact manifest has unknown or missing fields")
        entries = value["entries"]
        if not isinstance(entries, list) or any(not isinstance(item, Mapping) for item in entries):
            raise ValueError("extension artifact manifest entries are malformed")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            source_lock_digest=value["source_lock_digest"],  # type: ignore[arg-type]
            extension_api_version=value["extension_api_version"],  # type: ignore[arg-type]
            platform_tag=value["platform_tag"],  # type: ignore[arg-type]
            entries=tuple(InstalledExtensionArtifactV1.from_dict(item) for item in entries),
            digest=value["digest"],  # type: ignore[arg-type]
        )


def _extension_dependency_names(pyproject_path: Path) -> frozenset[str]:
    with pyproject_path.open("rb") as stream:
        document = tomllib.load(stream)
    groups = document.get("dependency-groups")
    dependencies = groups.get("extensions", []) if isinstance(groups, dict) else []
    if not isinstance(dependencies, list):
        raise ValueError("extension dependency group must be a list")
    names: set[str] = set()
    for item in dependencies:
        if not isinstance(item, str):
            raise ValueError("extension dependency declarations must be strings")
        try:
            requirement = Requirement(item)
        except InvalidRequirement as exc:
            raise ValueError("extension dependency declaration is invalid") from exc
        normalized = normalize_distribution_name(requirement.name)
        if normalized in names:
            raise ValueError("extension dependency group contains a duplicate distribution")
        names.add(normalized)
    return frozenset(names)


def _locked_packages(lock_path: Path) -> tuple[Mapping[str, object], ...]:
    with lock_path.open("rb") as stream:
        document = tomllib.load(stream)
    packages = document.get("package", [])
    if not isinstance(packages, list) or any(not isinstance(item, dict) for item in packages):
        raise ValueError("uv.lock package inventory is malformed")
    return tuple(packages)


def _hashes_from_lock_package(package: Mapping[str, object]) -> tuple[str, ...]:
    hashes: set[str] = set()
    sdist = package.get("sdist")
    archives: list[object] = []
    if isinstance(sdist, Mapping):
        archives.append(sdist)
    wheels = package.get("wheels")
    if isinstance(wheels, list):
        archives.extend(wheels)
    for archive in archives:
        if not isinstance(archive, Mapping):
            raise ValueError("uv.lock extension archive metadata is malformed")
        value = archive.get("hash")
        if value is None:
            continue
        hashes.add(_require_digest(value, field_name="locked artifact hash"))
    return tuple(sorted(hashes))


def _plugin_source_identity(
    plugins: Sequence[Mapping[str, object]],
    distribution: str,
) -> tuple[str, str]:
    matching = [item for item in plugins if isinstance(item.get("package"), str) and normalize_distribution_name(item["package"]) == distribution]
    if not matching:
        raise ValueError(f"extension dependency {distribution!r} has no managed plugin declaration")
    identities: set[tuple[str, str]] = set()
    for plugin in matching:
        name = plugin.get("name")
        use = plugin.get("use")
        if not isinstance(name, str) or not isinstance(use, str):
            raise ValueError("managed plugin declarations require name, package, and use")
        identities.add(_require_entry_point(name, use))
    if len(identities) != 1:
        raise ValueError(f"extension distribution {distribution!r} has conflicting entry-point declarations")
    return next(iter(identities))


def _source_entry_from_locked_package(
    *,
    backend_dir: Path,
    package: Mapping[str, object],
    entry_point_name: str,
    entry_point_value: str,
) -> ExtensionSourceLockEntryV1:
    distribution = package.get("name")
    distribution_version = package.get("version")
    if not isinstance(distribution, str) or not isinstance(distribution_version, str):
        raise ValueError("uv.lock extension package identity is malformed")
    source = package.get("source")
    if not isinstance(source, Mapping):
        raise ValueError("uv.lock extension source metadata is missing")
    hashes = _hashes_from_lock_package(package)

    registry = source.get("registry")
    git = source.get("git")
    local_values = [source.get(key) for key in ("directory", "path", "editable") if source.get(key) is not None]
    kinds = sum((registry is not None, git is not None, bool(local_values)))
    if kinds != 1:
        raise ValueError("uv.lock extension source metadata is ambiguous")
    if registry is not None:
        if not isinstance(registry, str):
            raise ValueError("uv.lock registry source is malformed")
        return ExtensionSourceLockEntryV1.create(
            distribution=distribution,
            distribution_version=distribution_version,
            entry_point_name=entry_point_name,
            entry_point_value=entry_point_value,
            source_kind="registry",
            source_reference=registry,
            source_revision=None,
            locked_artifact_hashes=hashes,
            local_tree_digest=None,
        )
    if git is not None:
        if not isinstance(git, str):
            raise ValueError("uv.lock Git source is malformed")
        requested_revision = source.get("rev")
        revision = source.get("precise")
        if revision is None:
            revision = requested_revision
        parsed_git = urllib.parse.urlsplit(git.removeprefix("git+"))
        query_revision = dict(urllib.parse.parse_qsl(parsed_git.query, keep_blank_values=True)).get("rev")
        if revision is None:
            revision = query_revision or parsed_git.fragment
        if not isinstance(revision, str) or _GIT_REVISION.fullmatch(revision) is None:
            raise ValueError("extension_source_not_immutable")
        if requested_revision is not None and (not isinstance(requested_revision, str) or _GIT_REVISION.fullmatch(requested_revision) is None):
            raise ValueError("extension_source_not_immutable")
        if query_revision is not None and _GIT_REVISION.fullmatch(query_revision) is None:
            raise ValueError("extension_source_not_immutable")
        normalized_git = urllib.parse.urlunsplit((parsed_git.scheme, parsed_git.netloc, parsed_git.path, "", ""))
        return ExtensionSourceLockEntryV1.create(
            distribution=distribution,
            distribution_version=distribution_version,
            entry_point_name=entry_point_name,
            entry_point_value=entry_point_value,
            source_kind="git",
            source_reference=normalized_git,
            source_revision=revision,
            locked_artifact_hashes=hashes,
            local_tree_digest=None,
        )

    if len(local_values) != 1 or not isinstance(local_values[0], str):
        raise ValueError("uv.lock local extension source is malformed")
    raw_path = local_values[0]
    if Path(raw_path).is_absolute() or "\\" in raw_path:
        raise ValueError("local extension source must use a normalized backend-relative path")
    source_path = (backend_dir / raw_path).resolve()
    snapshots_root = (backend_dir / "extensions" / "sources").resolve()
    if not source_path.is_relative_to(snapshots_root):
        raise ValueError("local extension source is outside the managed snapshots directory")
    reference = source_path.relative_to(backend_dir.resolve()).as_posix()
    return ExtensionSourceLockEntryV1.create(
        distribution=distribution,
        distribution_version=distribution_version,
        entry_point_name=entry_point_name,
        entry_point_value=entry_point_value,
        source_kind="local_snapshot",
        source_reference=reference,
        source_revision=None,
        locked_artifact_hashes=(),
        local_tree_digest=hash_local_snapshot_tree(source_path),
    )


def build_source_lock(
    backend_dir: str | Path,
    *,
    plugins: Sequence[Mapping[str, object]],
    extension_api_version: str,
) -> ExtensionSourceLockV1:
    """Build source provenance from the manager-owned declarations and uv lock."""

    backend = Path(backend_dir).resolve()
    dependency_names = _extension_dependency_names(backend / "pyproject.toml")
    packages_by_name: dict[str, list[Mapping[str, object]]] = {}
    for package in _locked_packages(backend / "uv.lock"):
        name = package.get("name")
        if isinstance(name, str):
            packages_by_name.setdefault(normalize_distribution_name(name), []).append(package)
    declared_plugin_names = {normalize_distribution_name(package) for item in plugins if isinstance((package := item.get("package")), str)}
    if declared_plugin_names != dependency_names:
        raise ValueError("managed plugin declarations and extension dependency group differ")
    entries: list[ExtensionSourceLockEntryV1] = []
    for distribution in sorted(dependency_names):
        matches = packages_by_name.get(distribution, [])
        if len(matches) != 1:
            raise ValueError(f"uv.lock must contain exactly one resolved package for extension {distribution!r}")
        name, use = _plugin_source_identity(plugins, distribution)
        entries.append(
            _source_entry_from_locked_package(
                backend_dir=backend,
                package=matches[0],
                entry_point_name=name,
                entry_point_value=use,
            )
        )
    return ExtensionSourceLockV1.create(
        extension_api_version=extension_api_version,
        entries=entries,
    )


def verify_source_lock_current(
    source_lock: ExtensionSourceLockV1,
    backend_dir: str | Path,
) -> ExtensionSourceLockV1:
    """Rebuild and compare the manager-owned source lock without writing it."""

    plugins = tuple(
        {
            "name": entry.entry_point_name,
            "package": entry.distribution,
            "use": entry.entry_point_value,
        }
        for entry in source_lock.entries
    )
    try:
        rebuilt = build_source_lock(
            backend_dir,
            plugins=plugins,
            extension_api_version=source_lock.extension_api_version,
        )
    except (OSError, TypeError, ValueError) as exc:
        raise ExtensionArtifactVerificationError(
            "extension_artifact_digest_mismatch",
            expected_digest=source_lock.digest,
        ) from exc
    if rebuilt != source_lock:
        raise ExtensionArtifactVerificationError(
            "extension_artifact_digest_mismatch",
            expected_digest=source_lock.digest,
            actual_digest=rebuilt.digest,
        )
    return source_lock


def read_source_lock(path: str | Path) -> ExtensionSourceLockV1:
    """Parse and validate a source lock without accepting non-canonical fields."""

    try:
        source = Path(path)
        if _is_link_like(source) or not source.is_file() or source.stat().st_size > _MAX_PROVENANCE_DOCUMENT_BYTES:
            raise OSError("invalid source-lock file")
        value = json.loads(
            source.read_bytes().decode("utf-8"),
            object_pairs_hook=_strict_json_object,
        )
    except (OSError, UnicodeError, json.JSONDecodeError, _DuplicateJsonKeyError) as exc:
        raise ExtensionArtifactVerificationError("extension_artifact_manifest_invalid") from exc
    if not isinstance(value, Mapping):
        raise ExtensionArtifactVerificationError("extension_artifact_manifest_invalid")
    try:
        return ExtensionSourceLockV1.from_dict(value)
    except (TypeError, ValueError) as exc:
        raise ExtensionArtifactVerificationError("extension_artifact_manifest_invalid") from exc


def read_artifact_manifest(path: str | Path) -> ExtensionArtifactManifestV1:
    """Parse and validate an installed artifact manifest."""

    try:
        source = Path(path)
        if _is_link_like(source):
            raise OSError("invalid artifact-manifest file")
        if source.stat().st_size > _MAX_PROVENANCE_DOCUMENT_BYTES:
            raise OSError("oversized artifact-manifest file")
        value = json.loads(
            source.read_bytes().decode("utf-8"),
            object_pairs_hook=_strict_json_object,
        )
    except FileNotFoundError as exc:
        raise ExtensionArtifactVerificationError("extension_artifact_manifest_missing") from exc
    except (OSError, UnicodeError, json.JSONDecodeError, _DuplicateJsonKeyError) as exc:
        raise ExtensionArtifactVerificationError("extension_artifact_manifest_invalid") from exc
    if not isinstance(value, Mapping):
        raise ExtensionArtifactVerificationError("extension_artifact_manifest_invalid")
    try:
        return ExtensionArtifactManifestV1.from_dict(value)
    except (TypeError, ValueError) as exc:
        raise ExtensionArtifactVerificationError("extension_artifact_manifest_invalid") from exc


def write_source_lock(path: str | Path, lock: ExtensionSourceLockV1) -> None:
    _write_document(Path(path), lock.to_json())


def write_artifact_manifest(
    path: str | Path,
    manifest: ExtensionArtifactManifestV1,
) -> None:
    _write_document(Path(path), manifest.to_json())


def _write_document(path: Path, content: str) -> None:
    """Atomically replace one manager/build-owned canonical document."""

    import tempfile

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            newline="\n",
            dir=path.parent,
            prefix=f".{path.name}.",
            delete=False,
        ) as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
            temporary = Path(stream.name)
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def canonical_platform_tag() -> str:
    """Return this interpreter's first normalized wheel compatibility tag."""

    try:
        return str(next(sys_tags())).lower()
    except StopIteration as exc:  # pragma: no cover - packaging always supplies tags
        raise RuntimeError("Python environment has no wheel compatibility tag") from exc


def _secret_field_tokens(key: str) -> tuple[str, ...]:
    camel_split = re.sub(
        r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])",
        "_",
        key,
    )
    return tuple(part for part in re.sub(r"[^A-Za-z0-9]+", "_", camel_split).lower().split("_") if part)


def _secret_shaped_config_key(key: str) -> bool:
    tokens = _secret_field_tokens(key)
    if any(token in _SECRET_FIELD_TOKENS for token in tokens):
        return True
    if any(pair in _SECRET_FIELD_PAIRS for pair in zip(tokens, tokens[1:])):
        return True
    compact = "".join(tokens)
    return any(compound in compact for compound in _SECRET_FIELD_COMPOUNDS) or any(term in compact for term in _SECRET_FIELD_EMBEDDED_TERMS)


def _secret_handle(path: tuple[str, ...]) -> dict[str, str]:
    path_digest = hashlib.sha256(_canonical_json_bytes({"version": 1, "field_path": list(path)})).hexdigest()
    return {"secret_handle": f"extension-config:{path_digest}"}


def _project_config_value(
    value: object,
    *,
    path: tuple[str, ...],
    depth: int,
) -> object:
    if depth > _MAX_CONFIG_DEPTH:
        raise ValueError("extension configuration exceeds the projection depth limit")
    if value is None or type(value) in {bool, int}:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("extension configuration contains a non-finite number")
        return value
    if type(value) is str:
        if len(value.encode("utf-8")) > _MAX_CONFIG_STRING_BYTES:
            raise ValueError("extension configuration contains an oversized string")
        return value
    if isinstance(value, Mapping):
        if len(value) > _MAX_CONFIG_ITEMS:
            raise ValueError("extension configuration mapping exceeds the item limit")
        projected: dict[str, object] = {}
        for key in sorted(value):
            if not isinstance(key, str) or not key or len(key.encode("utf-8")) > 256:
                raise ValueError("extension configuration keys must be bounded strings")
            field_path = (*path, key)
            projected[key] = (
                _secret_handle(field_path)
                if _secret_shaped_config_key(key)
                else _project_config_value(
                    value[key],
                    path=field_path,
                    depth=depth + 1,
                )
            )
        return projected
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        if len(value) > _MAX_CONFIG_ITEMS:
            raise ValueError("extension configuration list exceeds the item limit")
        return [
            _project_config_value(
                child,
                path=(*path, str(index)),
                depth=depth + 1,
            )
            for index, child in enumerate(value)
        ]
    raise ValueError(f"extension configuration contains unsupported {type(value).__name__} value")


def extension_configuration_projection(
    plugins: Sequence[Mapping[str, object] | object],
) -> dict[str, object]:
    """Return the complete ordered, secret-safe activation projection.

    Secret handles derive only from their field path, never the secret value.
    Consequently secret rotation does not disclose a value hash or silently
    change the deployment identity; operators version a non-secret binding when
    rotation must fence recovery.
    """

    if isinstance(plugins, (str, bytes, bytearray)) or len(plugins) > _MAX_CONFIG_PLUGINS:
        raise ValueError("extension plugin configuration exceeds the plugin limit")
    projected_plugins: list[dict[str, object]] = []
    for index, raw_plugin in enumerate(plugins):
        if isinstance(raw_plugin, Mapping):
            plugin = dict(raw_plugin)
        else:
            model_dump = getattr(raw_plugin, "model_dump", None)
            if not callable(model_dump):
                raise ValueError("extension plugin configuration must be a mapping")
            plugin = model_dump(mode="python")
            if not isinstance(plugin, dict):
                raise ValueError("extension plugin configuration must be a mapping")
        unknown = set(plugin) - _PLUGIN_FIELDS
        if unknown:
            raise ValueError("extension plugin configuration has unclassified fields")
        enabled = plugin.get("enabled", True)
        required = plugin.get("required", False)
        if type(enabled) is not bool or type(required) is not bool:
            raise ValueError("extension enabled and required flags must be booleans")
        name = plugin.get("name")
        if name is not None:
            _require_text(name, field_name="extension plugin name")
        package = plugin.get("package")
        if package is not None:
            if not isinstance(package, str):
                raise ValueError("extension plugin package must be a distribution name")
            normalize_distribution_name(package)
        table_prefix = plugin.get("table_prefix")
        if table_prefix is not None:
            _require_text(
                table_prefix,
                field_name="extension table prefix",
                max_bytes=256,
            )
        use = plugin.get("use")
        if not isinstance(use, str):
            raise ValueError("extension plugin configuration requires an entry point")
        _require_entry_point(str(name or f"plugin-{index}"), use)
        config = plugin.get("config", {})
        if not isinstance(config, Mapping):
            raise ValueError("extension plugin config must be a mapping")
        projected_plugins.append(
            {
                "enabled": enabled,
                "name": name,
                "package": package,
                "use": use,
                "required": required,
                "table_prefix": table_prefix,
                "config": _project_config_value(
                    config,
                    path=("plugins", str(index), "config"),
                    depth=0,
                ),
            }
        )
    projection: dict[str, object] = {"version": 1, "plugins": projected_plugins}
    if len(_canonical_json_bytes(projection)) > _MAX_CONFIG_CANONICAL_BYTES:
        raise ValueError("extension configuration projection exceeds the byte limit")
    return projection


def extension_configuration_digest(
    plugins: Sequence[Mapping[str, object] | object],
) -> str:
    """Digest the same non-secret configuration projection used at startup."""

    return _digest(extension_configuration_projection(plugins))


def _record_path(value: object) -> str:
    path = str(value).replace("\\", "/")
    if not path or path.startswith("/") or len(path.encode("utf-8")) > 2048 or any(part in {"", ".", ".."} for part in path.split("/")) or any(ord(character) < 32 or ord(character) == 127 for character in path):
        raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
    return path


def _sha256_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    try:
        if _is_link_like(path) or not path.is_file():
            raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
        with path.open("rb") as stream:
            while chunk := stream.read(1024 * 1024):
                size += len(chunk)
                if size > _MAX_INSTALLED_BYTES:
                    raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
                digest.update(chunk)
    except OSError as exc:
        raise ExtensionArtifactVerificationError("extension_installed_record_mismatch") from exc
    return f"sha256:{digest.hexdigest()}", size


def _record_hash(record: object) -> str | None:
    file_hash = getattr(record, "hash", None)
    if file_hash is None:
        return None
    mode = getattr(file_hash, "mode", None)
    encoded = getattr(file_hash, "value", None)
    if mode != "sha256" or not isinstance(encoded, str):
        raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
    try:
        raw = urlsafe_b64decode(encoded + ("=" * (-len(encoded) % 4)))
    except (ValueError, TypeError) as exc:
        raise ExtensionArtifactVerificationError("extension_installed_record_mismatch") from exc
    if len(raw) != 32:
        raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
    return f"sha256:{raw.hex()}"


def _is_owned_executable(path: str, top_level_module: str) -> bool:
    candidate = Path(path)
    if candidate.suffix.lower() not in _EXECUTABLE_SUFFIXES:
        return False
    first = path.partition("/")[0]
    return first == top_level_module or first == f"{top_level_module}.py" or ("/" not in path and first.startswith(f"{top_level_module}.") and candidate.suffix.lower() in _EXECUTABLE_SUFFIXES)


def _actual_owned_executables(
    root: Path,
    *,
    top_level_module: str,
) -> frozenset[str]:
    candidates: list[Path] = []
    package_root = root / top_level_module
    module_file = root / f"{top_level_module}.py"
    if package_root.exists():
        candidates.append(package_root)
    if module_file.exists():
        candidates.append(module_file)
    candidates.extend(child for child in root.glob(f"{top_level_module}.*") if child != module_file and child.suffix.lower() in _EXECUTABLE_SUFFIXES)
    owned: set[str] = set()
    for candidate in candidates:
        if _is_link_like(candidate):
            raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
        if candidate.is_file():
            owned.add(_record_path(candidate.relative_to(root).as_posix()))
            continue
        if not candidate.is_dir():
            raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
        for directory, dirnames, filenames in os.walk(candidate, followlinks=False):
            directory_path = Path(directory)
            retained: list[str] = []
            for dirname in sorted(dirnames):
                child = directory_path / dirname
                if dirname == "__pycache__":
                    continue
                if _is_link_like(child):
                    raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
                retained.append(dirname)
            dirnames[:] = retained
            for filename in sorted(filenames):
                child = directory_path / filename
                if child.suffix.lower() not in _EXECUTABLE_SUFFIXES:
                    continue
                if _is_link_like(child) or not child.is_file():
                    raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
                owned.add(_record_path(child.relative_to(root).as_posix()))
                if len(owned) > _MAX_INSTALLED_FILES:
                    raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
    return frozenset(owned)


def _direct_url_payload(
    distribution: metadata.Distribution,
) -> Mapping[str, object] | None:
    try:
        direct_url = distribution.read_text("direct_url.json")
    except Exception as exc:
        raise ExtensionArtifactVerificationError("extension_artifact_digest_mismatch") from exc
    if direct_url is None:
        return None
    if not isinstance(direct_url, str) or len(direct_url.encode("utf-8")) > 64 * 1024:
        raise ExtensionArtifactVerificationError("extension_artifact_digest_mismatch")
    try:
        payload = json.loads(direct_url, object_pairs_hook=_strict_json_object)
    except (json.JSONDecodeError, _DuplicateJsonKeyError) as exc:
        raise ExtensionArtifactVerificationError("extension_artifact_digest_mismatch") from exc
    if not isinstance(payload, Mapping):
        raise ExtensionArtifactVerificationError("extension_artifact_digest_mismatch")
    return payload


def _selected_artifact_hash(payload: Mapping[str, object] | None) -> str | None:
    if payload is None:
        return None
    archive_info = payload.get("archive_info")
    if archive_info is None:
        return None
    if not isinstance(archive_info, Mapping):
        raise ExtensionArtifactVerificationError("extension_artifact_digest_mismatch")
    raw_hash: object = archive_info.get("hash")
    hashes = archive_info.get("hashes")
    if raw_hash is None and isinstance(hashes, Mapping):
        raw_hash = hashes.get("sha256")
        if isinstance(raw_hash, str):
            raw_hash = f"sha256:{raw_hash}"
    if raw_hash is None:
        return None
    if isinstance(raw_hash, str) and raw_hash.startswith("sha256="):
        raw_hash = f"sha256:{raw_hash.removeprefix('sha256=')}"
    try:
        return _require_digest(raw_hash, field_name="selected artifact hash")
    except ValueError as exc:
        raise ExtensionArtifactVerificationError("extension_artifact_digest_mismatch") from exc


def _verify_direct_source_identity(
    source_entry: ExtensionSourceLockEntryV1,
    payload: Mapping[str, object] | None,
    *,
    selected_artifact_hash: str | None,
    backend_dir: Path | None,
) -> None:
    if source_entry.source_kind == "registry":
        if payload is not None and selected_artifact_hash is None:
            raise ExtensionArtifactVerificationError(
                "extension_artifact_digest_mismatch",
                distribution=source_entry.distribution,
            )
        return
    if source_entry.source_kind == "local_snapshot":
        if payload is None or backend_dir is None:
            raise ExtensionArtifactVerificationError(
                "extension_artifact_digest_mismatch",
                distribution=source_entry.distribution,
            )
        url = payload.get("url")
        dir_info = payload.get("dir_info")
        if not isinstance(url, str) or not isinstance(dir_info, Mapping):
            raise ExtensionArtifactVerificationError(
                "extension_artifact_digest_mismatch",
                distribution=source_entry.distribution,
            )
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "file" or parsed.netloc not in {"", "localhost"} or parsed.query or parsed.fragment:
            raise ExtensionArtifactVerificationError(
                "extension_artifact_digest_mismatch",
                distribution=source_entry.distribution,
            )
        expected_source = (backend_dir / source_entry.source_reference).resolve()
        installed_source = Path(urllib.request.url2pathname(parsed.path)).resolve()
        if installed_source != expected_source:
            raise ExtensionArtifactVerificationError(
                "extension_artifact_digest_mismatch",
                distribution=source_entry.distribution,
            )
        return
    if source_entry.source_kind != "git":
        return
    if payload is None:
        raise ExtensionArtifactVerificationError(
            "extension_artifact_digest_mismatch",
            distribution=source_entry.distribution,
        )
    url = payload.get("url")
    vcs_info = payload.get("vcs_info")
    if not isinstance(url, str) or not isinstance(vcs_info, Mapping):
        raise ExtensionArtifactVerificationError(
            "extension_artifact_digest_mismatch",
            distribution=source_entry.distribution,
        )
    try:
        normalized_url = _require_safe_source_reference(url, source_kind="git")
    except ValueError as exc:
        raise ExtensionArtifactVerificationError(
            "extension_artifact_digest_mismatch",
            distribution=source_entry.distribution,
        ) from exc
    if normalized_url != source_entry.source_reference or vcs_info.get("vcs") != "git" or vcs_info.get("commit_id") != source_entry.source_revision:
        raise ExtensionArtifactVerificationError(
            "extension_artifact_digest_mismatch",
            distribution=source_entry.distribution,
        )


def _distribution_identity(
    distribution: metadata.Distribution,
) -> tuple[str, str]:
    name = distribution.metadata.get("Name")
    version = distribution.version
    if not isinstance(name, str) or not isinstance(version, str):
        raise ExtensionArtifactVerificationError("extension_installed_record_mismatch")
    return name, version


def _verify_installed_distribution(
    source_entry: ExtensionSourceLockEntryV1,
    *,
    find_distribution: Callable[[str], metadata.Distribution],
    backend_dir: Path | None,
) -> InstalledExtensionArtifactV1:
    normalized = normalize_distribution_name(source_entry.distribution)
    try:
        distribution = find_distribution(source_entry.distribution)
    except metadata.PackageNotFoundError as exc:
        raise ExtensionArtifactVerificationError(
            "extension_installed_record_mismatch",
            distribution=normalized,
        ) from exc
    installed_name, installed_version = _distribution_identity(distribution)
    if normalize_distribution_name(installed_name) != normalized or installed_version != source_entry.distribution_version:
        raise ExtensionArtifactVerificationError(
            "extension_installed_record_mismatch",
            distribution=normalized,
        )
    entry_points = tuple(entry_point for entry_point in distribution.entry_points if entry_point.group == "deerflow.extensions")
    if len(entry_points) != 1 or (
        entry_points[0].name,
        entry_points[0].value,
    ) != (source_entry.entry_point_name, source_entry.entry_point_value):
        raise ExtensionArtifactVerificationError(
            "extension_entry_point_mismatch",
            distribution=normalized,
        )

    records = distribution.files
    if records is None or len(records) > _MAX_INSTALLED_FILES:
        raise ExtensionArtifactVerificationError(
            "extension_installed_record_mismatch",
            distribution=normalized,
        )
    root = Path(distribution.locate_file("")).resolve()
    module = source_entry.entry_point_value.partition(":")[0]
    top_level_module = module.partition(".")[0]
    verified: list[dict[str, object]] = []
    recorded_paths: set[str] = set()
    total_size = 0
    for record in records:
        path = _record_path(record)
        if path in recorded_paths:
            raise ExtensionArtifactVerificationError(
                "extension_installed_record_mismatch",
                distribution=normalized,
            )
        recorded_paths.add(path)
        expected = _record_hash(record)
        if expected is None:
            if _is_owned_executable(path, top_level_module):
                raise ExtensionArtifactVerificationError(
                    "extension_installed_record_mismatch",
                    distribution=normalized,
                )
            continue
        located = Path(distribution.locate_file(record))
        resolved = located.resolve()
        if _is_link_like(located) or not resolved.is_relative_to(root):
            raise ExtensionArtifactVerificationError(
                "extension_installed_record_mismatch",
                distribution=normalized,
            )
        actual, actual_size = _sha256_file(located)
        declared_size = getattr(record, "size", None)
        if expected != actual or (declared_size is not None and declared_size != actual_size):
            raise ExtensionArtifactVerificationError(
                "extension_installed_record_mismatch",
                distribution=normalized,
                expected_digest=expected,
                actual_digest=actual,
            )
        total_size += actual_size
        if total_size > _MAX_INSTALLED_BYTES:
            raise ExtensionArtifactVerificationError(
                "extension_installed_record_mismatch",
                distribution=normalized,
            )
        verified.append({"path": path, "content_digest": actual, "size": actual_size})
    verified.sort(key=lambda item: str(item["path"]).encode("utf-8"))
    recorded_owned = frozenset(path for path in recorded_paths if _is_owned_executable(path, top_level_module))
    actual_owned = _actual_owned_executables(
        root,
        top_level_module=top_level_module,
    )
    if recorded_owned != actual_owned:
        raise ExtensionArtifactVerificationError(
            "extension_installed_record_mismatch",
            distribution=normalized,
        )

    direct_url_payload = _direct_url_payload(distribution)
    selected_hash = _selected_artifact_hash(direct_url_payload)
    _verify_direct_source_identity(
        source_entry,
        direct_url_payload,
        selected_artifact_hash=selected_hash,
        backend_dir=backend_dir,
    )
    if selected_hash is not None and selected_hash not in source_entry.locked_artifact_hashes:
        raise ExtensionArtifactVerificationError(
            "extension_artifact_digest_mismatch",
            distribution=normalized,
            actual_digest=selected_hash,
        )
    return InstalledExtensionArtifactV1.create(
        source_entry_digest=source_entry.entry_digest,
        distribution=source_entry.distribution,
        distribution_version=source_entry.distribution_version,
        entry_point_name=source_entry.entry_point_name,
        entry_point_value=source_entry.entry_point_value,
        selected_artifact_hash=selected_hash,
        installed_record_digest=_digest({"version": 1, "files": verified}),
    )


def _verify_local_source_entries(
    source_lock: ExtensionSourceLockV1,
    *,
    backend_dir: Path | None,
) -> None:
    local_entries = tuple(entry for entry in source_lock.entries if entry.source_kind == "local_snapshot")
    if not local_entries:
        return
    if backend_dir is None:
        raise ExtensionArtifactVerificationError("extension_artifact_digest_mismatch")
    backend = backend_dir.resolve()
    for entry in local_entries:
        source_path = (backend / entry.source_reference).resolve()
        snapshots_root = (backend / "extensions" / "sources").resolve()
        if not source_path.is_relative_to(snapshots_root):
            raise ExtensionArtifactVerificationError(
                "extension_artifact_digest_mismatch",
                distribution=entry.distribution,
            )
        try:
            actual = hash_local_snapshot_tree(source_path)
        except (OSError, ValueError) as exc:
            raise ExtensionArtifactVerificationError(
                "extension_artifact_digest_mismatch",
                distribution=entry.distribution,
            ) from exc
        if actual != entry.local_tree_digest:
            raise ExtensionArtifactVerificationError(
                "extension_artifact_digest_mismatch",
                distribution=entry.distribution,
                expected_digest=entry.local_tree_digest,
                actual_digest=actual,
            )


def build_installed_artifact_manifest(
    source_lock: ExtensionSourceLockV1,
    *,
    backend_dir: str | Path | None = None,
    platform_tag: str | None = None,
    expected_extension_api_version: str | None = None,
    find_distribution: Callable[[str], metadata.Distribution] = metadata.distribution,
) -> ExtensionArtifactManifestV1:
    """Verify installed packages and construct their platform manifest."""

    if expected_extension_api_version is not None and source_lock.extension_api_version != expected_extension_api_version:
        raise ExtensionArtifactVerificationError("extension_artifact_digest_mismatch")
    resolved_backend_dir = Path(backend_dir).resolve() if backend_dir is not None else None
    _verify_local_source_entries(
        source_lock,
        backend_dir=resolved_backend_dir,
    )
    entries = tuple(
        _verify_installed_distribution(
            source_entry,
            find_distribution=find_distribution,
            backend_dir=resolved_backend_dir,
        )
        for source_entry in source_lock.entries
    )
    return ExtensionArtifactManifestV1.create(
        source_lock_digest=source_lock.digest,
        extension_api_version=source_lock.extension_api_version,
        platform_tag=platform_tag or canonical_platform_tag(),
        entries=entries,
    )


def verify_installed_artifact_manifest(
    source_lock: ExtensionSourceLockV1,
    manifest: ExtensionArtifactManifestV1,
    *,
    backend_dir: str | Path | None = None,
    expected_extension_api_version: str | None = None,
    find_distribution: Callable[[str], metadata.Distribution] = metadata.distribution,
) -> ExtensionArtifactManifestV1:
    """Repeat bounded installed verification and return the trusted manifest."""

    if manifest.source_lock_digest != source_lock.digest or manifest.extension_api_version != source_lock.extension_api_version:
        raise ExtensionArtifactVerificationError(
            "extension_artifact_digest_mismatch",
            expected_digest=source_lock.digest,
            actual_digest=manifest.source_lock_digest,
        )
    current_platform_tag = canonical_platform_tag()
    if manifest.platform_tag != current_platform_tag:
        raise ExtensionArtifactVerificationError("extension_artifact_digest_mismatch")
    rebuilt = build_installed_artifact_manifest(
        source_lock,
        backend_dir=backend_dir,
        platform_tag=current_platform_tag,
        expected_extension_api_version=expected_extension_api_version,
        find_distribution=find_distribution,
    )
    if rebuilt != manifest:
        raise ExtensionArtifactVerificationError(
            "extension_artifact_digest_mismatch",
            expected_digest=manifest.digest,
            actual_digest=rebuilt.digest,
        )
    return manifest


__all__ = [
    "ExtensionArtifactManifestV1",
    "ExtensionArtifactVerificationError",
    "ExtensionSourceLockEntryV1",
    "ExtensionSourceLockV1",
    "InstalledExtensionArtifactV1",
    "SNAPSHOT_IGNORED_DIRECTORY_NAMES",
    "SNAPSHOT_IGNORED_FILE_SUFFIXES",
    "UNVERIFIED_EXTENSION_ARTIFACT_MANIFEST_DIGEST",
    "build_installed_artifact_manifest",
    "build_source_lock",
    "canonical_platform_tag",
    "extension_configuration_digest",
    "extension_configuration_projection",
    "hash_local_snapshot_tree",
    "normalize_distribution_name",
    "read_artifact_manifest",
    "read_source_lock",
    "validate_local_snapshot",
    "verify_installed_artifact_manifest",
    "verify_source_lock_current",
    "write_artifact_manifest",
    "write_source_lock",
]
