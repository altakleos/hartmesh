"""Portable, secret-safe evidence snapshots for terminal durable runs.

This module owns the transport-neutral manifest contract.  It deliberately
does not know about FastAPI, ZIP files, database rows, or filesystem paths.
Adapters first capture and validate an :class:`EvidenceSnapshotSourceV1`, then
the archive layer supplies entries derived from the exact descriptors copied
into a bundle.

The manifest is digest-bound.  It is not signed and it makes no authenticity
or external-attestation claim.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Literal, Protocol

from deerflow_extension_api import TenantReferenceV1

RUN_EVIDENCE_MANIFEST_PATH = "hartmesh-evidence/manifest.v1.json"
RUN_EVIDENCE_SCHEMA = "hartmesh.run-evidence-bundle"
RUN_EVIDENCE_SCHEMA_VERSION = 1
RUN_EVIDENCE_CANONICALIZATION = "utf8-nfc-sorted-json"
RUN_EVIDENCE_CANONICALIZATION_VERSION = 1

MAX_MANIFEST_BYTES = 1024 * 1024
MAX_ARTIFACTS = 50
MAX_ARTIFACT_PATH_BYTES = 1024
MAX_ARTIFACT_BYTES = 50 * 1024 * 1024
MAX_TOTAL_ARTIFACT_BYTES = 100 * 1024 * 1024
MAX_EVIDENCE_REFERENCES = 4096
MAX_EVIDENCE_LINKS = 4096
MAX_LIFECYCLE_EVENTS = 100_000
MAX_SAFE_COUNTS = 32
MAX_SAFE_STRING_BYTES = 256

MANIFEST_DIGEST_DOMAIN = b"hartmesh.run-evidence-bundle.manifest.v1\x00"
BUNDLE_REFERENCE_DOMAIN = b"hartmesh.run-evidence-bundle.reference.v1\x00"
PUBLIC_REFERENCE_DOMAIN = b"hartmesh.run-evidence-bundle.public-reference.v1\x00"
EVIDENCE_ROOT_DOMAIN = b"hartmesh.run-evidence-bundle.section-root.v1\x00"

TERMINAL_RUN_STATUSES = frozenset({"success", "error", "timeout", "interrupted"})
SECTION_STATES = frozenset(
    {
        "complete",
        "absent_by_design",
        "unsupported",
        "legacy",
        "pruned",
        "unavailable",
        "unqualified",
    }
)
EXPECTED_EVIDENCE_SECTIONS = (
    "accepted_invocation",
    "actor_credential",
    "assembly",
    "subagent_catalog",
    "skill_material",
    "extension_material",
    "tool_plane",
    "lifecycle",
    "tool_receipts",
    "mcp_tasks",
    "subagent_batches",
    "sandbox_execution",
    "retrieval_observations",
    "qualification",
)

_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_SHA256_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")
_PUBLIC_REFERENCE_RE = re.compile(r"^(?:tenant|run|thread|bundle)-[a-z0-9]{16,32}$")
_MEDIA_TYPE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+/-]{0,126}$")
_SAFE_COUNT_KEYS = frozenset({"lifecycle", "tools", "artifacts", "retrieval", "mcp", "batches", "sandbox", "other"})
_ALLOWED_ARTIFACT_FORMAT_CHARS = frozenset({"\u200c", "\u200d"})
_WINDOWS_INVALID_CHARS = frozenset('<>:"|?*')
_WINDOWS_DEVICE_NAMES = frozenset({"con", "prn", "aux", "nul"} | {f"com{number}" for number in range(1, 10)} | {f"lpt{number}" for number in range(1, 10)})
RUN_EVIDENCE_LIMITATIONS = (
    "artifact_content_is_not_sanitized",
    "bundle_copy_outlives_server_retention",
    "internal_integrity_only_not_signed",
)

_EVIDENCE_LINK_SECTIONS = {
    "mcp_task_to_tool_receipt": ("mcp_tasks", "tool_receipts"),
    "retrieval_observation_to_tool_receipt": (
        "retrieval_observations",
        "tool_receipts",
    ),
    "subagent_batch_to_tool_receipt": (
        "subagent_batches",
        "tool_receipts",
    ),
}

SectionState = Literal[
    "complete",
    "absent_by_design",
    "unsupported",
    "legacy",
    "pruned",
    "unavailable",
    "unqualified",
]


class RunEvidenceBundleError(ValueError):
    """A bounded machine-readable evidence export failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _fail(code: str) -> None:
    raise RunEvidenceBundleError(code)


def _canonical_value(value: object) -> object:
    """Return the one JSON projection accepted by canonicalization V1."""

    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            _fail("manifest_not_canonical")
        normalized_keys = [unicodedata.normalize("NFC", key) for key in value]
        if len(normalized_keys) != len(set(normalized_keys)):
            _fail("manifest_not_canonical")
        return {unicodedata.normalize("NFC", key): _canonical_value(child) for key, child in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray | str):
        return [_canonical_value(child) for child in value]
    _fail("manifest_not_canonical")


def canonical_json_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            _canonical_value(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise RunEvidenceBundleError("manifest_not_canonical") from exc


def _domain_digest(domain: bytes, value: object) -> str:
    return hashlib.sha256(domain + canonical_json_bytes(value)).hexdigest()


def _require_digest(value: object, code: str = "evidence_cross_link_invalid") -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        _fail(code)
    return value


def _optional_digest(value: object, code: str = "evidence_cross_link_invalid") -> str | None:
    if value is None:
        return None
    return _require_digest(value, code)


def _optional_sha256_digest(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or _SHA256_DIGEST_RE.fullmatch(value) is None:
        _fail("evidence_cross_link_invalid")
    return value


def _safe_identifier(value: object, code: str = "evidence_cross_link_invalid") -> str:
    if not isinstance(value, str) or _SAFE_IDENTIFIER_RE.fullmatch(value) is None:
        _fail(code)
    return value


def _timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        _fail("evidence_cross_link_invalid")
    normalized = value.astimezone(UTC)
    if normalized.microsecond:
        rendered = normalized.isoformat(timespec="microseconds")
    else:
        rendered = normalized.isoformat(timespec="seconds")
    return rendered.replace("+00:00", "Z")


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z") or len(value.encode("utf-8")) > 40:
        _fail("manifest_fields_invalid")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise RunEvidenceBundleError("manifest_fields_invalid") from exc
    if _timestamp(parsed) != value:
        _fail("manifest_not_canonical")
    return parsed


def public_evidence_reference(kind: Literal["run", "thread"], tenant_digest: str, raw_identifier: str) -> str:
    _require_digest(tenant_digest)
    if not isinstance(raw_identifier, str) or not raw_identifier or len(raw_identifier.encode("utf-8")) > 256:
        _fail("evidence_cross_link_invalid")
    digest = _domain_digest(
        PUBLIC_REFERENCE_DOMAIN,
        {"kind": kind, "tenant_digest": tenant_digest, "identifier": raw_identifier},
    )
    return f"{kind}-{digest[:24]}"


def canonical_evidence_root(section_name: str, references: Sequence[str]) -> str:
    """Return an order-independent root over already-safe evidence digests."""

    name = _safe_identifier(section_name)
    normalized = sorted({_require_digest(reference) for reference in references})
    if len(normalized) != len(references) or len(normalized) > MAX_EVIDENCE_REFERENCES:
        _fail("bundle_limit_exceeded")
    return _domain_digest(
        EVIDENCE_ROOT_DOMAIN,
        {"section": name, "references": normalized},
    )


@dataclass(frozen=True, slots=True)
class EvidenceSectionV1:
    name: str
    state: SectionState
    required: bool
    item_count: int
    references: tuple[str, ...]
    root_digest: str | None
    reason_code: str | None

    def __post_init__(self) -> None:
        if self.name not in EXPECTED_EVIDENCE_SECTIONS:
            _fail("evidence_section_invalid")
        if self.state not in SECTION_STATES or type(self.required) is not bool:
            _fail("evidence_section_invalid")
        if type(self.item_count) is not int or self.item_count < 0 or self.item_count > MAX_EVIDENCE_REFERENCES:
            _fail("bundle_limit_exceeded")
        references = tuple(sorted(self.references))
        if len(references) > MAX_EVIDENCE_REFERENCES or len(set(references)) != len(references):
            _fail("bundle_limit_exceeded")
        for reference in references:
            _require_digest(reference, "evidence_section_invalid")
        object.__setattr__(self, "references", references)
        if self.state == "complete":
            if self.item_count != len(references) or self.reason_code is not None:
                _fail("evidence_section_invalid")
            expected_root = canonical_evidence_root(self.name, references)
            if self.root_digest != expected_root:
                _fail("evidence_section_invalid")
        else:
            if references or self.root_digest is not None or self.item_count != 0:
                _fail("evidence_section_invalid")
            _safe_identifier(self.reason_code, "evidence_section_invalid")

    @classmethod
    def complete(
        cls,
        name: str,
        references: Sequence[str] = (),
        *,
        required: bool = True,
    ) -> EvidenceSectionV1:
        normalized = tuple(sorted(references))
        return cls(
            name=name,
            state="complete",
            required=required,
            item_count=len(normalized),
            references=normalized,
            root_digest=canonical_evidence_root(name, normalized),
            reason_code=None,
        )

    @classmethod
    def _omitted(
        cls,
        name: str,
        state: SectionState,
        *,
        required: bool,
        reason_code: str,
    ) -> EvidenceSectionV1:
        return cls(
            name=name,
            state=state,
            required=required,
            item_count=0,
            references=(),
            root_digest=None,
            reason_code=reason_code,
        )

    @classmethod
    def absent_by_design(cls, name: str) -> EvidenceSectionV1:
        return cls._omitted(name, "absent_by_design", required=False, reason_code="capability_not_accepted")

    @classmethod
    def unsupported(cls, name: str, *, required: bool = False) -> EvidenceSectionV1:
        return cls._omitted(name, "unsupported", required=required, reason_code="evidence_unsupported")

    @classmethod
    def legacy(cls, name: str, *, required: bool = True) -> EvidenceSectionV1:
        return cls._omitted(name, "legacy", required=required, reason_code="evidence_legacy_unbound")

    @classmethod
    def pruned(cls, name: str, *, required: bool = True) -> EvidenceSectionV1:
        return cls._omitted(name, "pruned", required=required, reason_code="evidence_pruned")

    @classmethod
    def unavailable(cls, name: str, *, required: bool = True) -> EvidenceSectionV1:
        return cls._omitted(name, "unavailable", required=required, reason_code="evidence_unavailable")

    @classmethod
    def unqualified(cls, name: str, *, required: bool = False) -> EvidenceSectionV1:
        return cls._omitted(name, "unqualified", required=required, reason_code="deployment_not_qualified")

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "state": self.state,
            "required": self.required,
            "item_count": self.item_count,
            "references": list(self.references),
            "root_digest": self.root_digest,
            "reason_code": self.reason_code,
        }

    @classmethod
    def from_dict(cls, value: object) -> EvidenceSectionV1:
        expected = {"name", "state", "required", "item_count", "references", "root_digest", "reason_code"}
        if not isinstance(value, Mapping) or set(value) != expected or not isinstance(value.get("references"), list):
            _fail("manifest_fields_invalid")
        try:
            return cls(
                name=value["name"],  # type: ignore[arg-type]
                state=value["state"],  # type: ignore[arg-type]
                required=value["required"],  # type: ignore[arg-type]
                item_count=value["item_count"],  # type: ignore[arg-type]
                references=tuple(value["references"]),  # type: ignore[arg-type]
                root_digest=value["root_digest"],  # type: ignore[arg-type]
                reason_code=value["reason_code"],  # type: ignore[arg-type]
            )
        except RunEvidenceBundleError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise RunEvidenceBundleError("manifest_fields_invalid") from exc


@dataclass(frozen=True, slots=True)
class EvidenceLinkV1:
    """One digest-only relationship between two included evidence sections."""

    kind: str
    subject_section: str
    subject_digest: str
    object_section: str
    object_digest: str

    def __post_init__(self) -> None:
        expected_sections = _EVIDENCE_LINK_SECTIONS.get(self.kind)
        if expected_sections != (self.subject_section, self.object_section):
            _fail("evidence_cross_link_invalid")
        _require_digest(self.subject_digest)
        _require_digest(self.object_digest)

    def sort_key(self) -> tuple[str, str, str, str, str]:
        return (
            self.kind,
            self.subject_section,
            self.subject_digest,
            self.object_section,
            self.object_digest,
        )

    def to_dict(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "subject_section": self.subject_section,
            "subject_digest": self.subject_digest,
            "object_section": self.object_section,
            "object_digest": self.object_digest,
        }

    @classmethod
    def from_dict(cls, value: object) -> EvidenceLinkV1:
        expected = {
            "kind",
            "subject_section",
            "subject_digest",
            "object_section",
            "object_digest",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            _fail("manifest_fields_invalid")
        try:
            return cls(
                kind=value["kind"],  # type: ignore[arg-type]
                subject_section=value["subject_section"],  # type: ignore[arg-type]
                subject_digest=value["subject_digest"],  # type: ignore[arg-type]
                object_section=value["object_section"],  # type: ignore[arg-type]
                object_digest=value["object_digest"],  # type: ignore[arg-type]
            )
        except RunEvidenceBundleError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise RunEvidenceBundleError("manifest_fields_invalid") from exc


def _validate_evidence_links(
    sections: Sequence[EvidenceSectionV1],
    links: Sequence[EvidenceLinkV1],
) -> tuple[EvidenceLinkV1, ...]:
    if any(not isinstance(link, EvidenceLinkV1) for link in links):
        _fail("evidence_cross_link_invalid")
    normalized = tuple(sorted(links, key=EvidenceLinkV1.sort_key))
    if len(normalized) > MAX_EVIDENCE_LINKS or len({link.sort_key() for link in normalized}) != len(normalized):
        _fail("bundle_limit_exceeded")
    by_name = {section.name: section for section in sections}
    for link in normalized:
        if not isinstance(link, EvidenceLinkV1):
            _fail("evidence_cross_link_invalid")
        subject = by_name.get(link.subject_section)
        object_section = by_name.get(link.object_section)
        if subject is None or object_section is None or subject.state != "complete" or object_section.state != "complete" or link.subject_digest not in subject.references or link.object_digest not in object_section.references:
            _fail("evidence_cross_link_invalid")
    for kind, (subject_name, _object_name) in _EVIDENCE_LINK_SECTIONS.items():
        section = by_name.get(subject_name)
        if section is None:
            _fail("evidence_cross_link_invalid")
        linked_subjects = [link.subject_digest for link in normalized if link.kind == kind]
        if tuple(sorted(linked_subjects)) != section.references:
            _fail("evidence_cross_link_invalid")
    return normalized


def _validate_section_anchors(
    sections: Sequence[EvidenceSectionV1],
    anchors: Mapping[str, Sequence[str]],
) -> None:
    """Require each projected anchor set to exactly match its V1 section."""

    by_name = {section.name: section for section in sections}
    for name, references in anchors.items():
        expected = tuple(sorted(references))
        section = by_name.get(name)
        if section is None:
            _fail("evidence_cross_link_invalid")
        if expected:
            if section.state != "complete" or section.references != expected:
                _fail("evidence_cross_link_invalid")
        elif section.state != "absent_by_design":
            _fail("evidence_cross_link_invalid")


@dataclass(frozen=True, slots=True)
class EvidenceArtifactV1:
    path: str
    size: int
    sha256: str
    media_type: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.path, str):
            _fail("artifact_path_invalid")
        path = unicodedata.normalize("NFC", self.path)
        parts = path.split("/")
        if (
            not path
            or path.startswith("/")
            or "\\" in path
            or "\x00" in path
            or len(path.encode("utf-8")) > MAX_ARTIFACT_PATH_BYTES
            or any(part in {"", ".", ".."} for part in parts)
            or any(any(char in _WINDOWS_INVALID_CHARS for char in part) or part.endswith((" ", ".")) or part.split(".", 1)[0].rstrip().casefold() in _WINDOWS_DEVICE_NAMES for part in parts)
            or any(any(unicodedata.category(char).startswith("C") and char not in _ALLOWED_ARTIFACT_FORMAT_CHARS for char in part) for part in parts)
            or parts[0].casefold() == "hartmesh-evidence"
        ):
            _fail("artifact_path_invalid")
        object.__setattr__(self, "path", path)
        if type(self.size) is not int or self.size < 0 or self.size > MAX_ARTIFACT_BYTES:
            _fail("bundle_limit_exceeded")
        _require_digest(self.sha256, "artifact_digest_invalid")
        if self.media_type is not None and (not isinstance(self.media_type, str) or _MEDIA_TYPE_RE.fullmatch(self.media_type) is None):
            _fail("artifact_media_type_invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "size": self.size,
            "sha256": self.sha256,
            "media_type": self.media_type,
        }

    @classmethod
    def from_dict(cls, value: object) -> EvidenceArtifactV1:
        if not isinstance(value, Mapping) or set(value) != {"path", "size", "sha256", "media_type"}:
            _fail("manifest_fields_invalid")
        try:
            return cls(
                path=value["path"],  # type: ignore[arg-type]
                size=value["size"],  # type: ignore[arg-type]
                sha256=value["sha256"],  # type: ignore[arg-type]
                media_type=value["media_type"],  # type: ignore[arg-type]
            )
        except RunEvidenceBundleError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise RunEvidenceBundleError("manifest_fields_invalid") from exc


@dataclass(frozen=True, slots=True)
class EvidenceSnapshotRequest:
    tenant: TenantReferenceV1
    thread_id: str
    run_id: str
    owner_id: str
    profile: Literal["complete_durable"] = "complete_durable"

    def __post_init__(self) -> None:
        if not isinstance(self.tenant, TenantReferenceV1):
            _fail("evidence_cross_link_invalid")
        for value in (self.thread_id, self.run_id, self.owner_id):
            if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 256:
                _fail("evidence_cross_link_invalid")
        if self.profile != "complete_durable":
            _fail("evidence_profile_unsupported")


@dataclass(frozen=True, slots=True)
class EvidenceSnapshotSourceV1:
    """One bounded repository read before public-reference projection."""

    tenant: TenantReferenceV1
    thread_id: str
    run_id: str
    terminal_status: str
    safe_stop_reason: str
    accepted_at: datetime
    completed_at: datetime
    accepted_invocation_digest: str
    accepted_invocation_version: int
    accepted_context_digest: str
    agent_revision_digest: str
    assembly_evidence_digest: str
    assembly_fingerprint: str
    lifecycle_high_water_mark: int
    terminal_event_digest: str
    lifecycle_event_count: int
    lifecycle_counts: Mapping[str, int]
    sections: tuple[EvidenceSectionV1, ...]
    artifact_paths: tuple[str, ...]
    links: tuple[EvidenceLinkV1, ...] = ()
    subagent_catalog_digest: str | None = None
    subagent_catalog_entry_count: int = 0
    skill_scopes_digest: str | None = None
    skill_scope_count: int = 0
    capability_manifest_digest: str | None = None
    extension_artifact_manifest_digest: str | None = None
    extension_configuration_digest: str | None = None
    tool_plane_revision_digest: str | None = None
    tool_plane_base_revision_digest: str | None = None
    tool_plane_user_overlay_digest: str | None = None
    tool_plane_projection_digest: str | None = None
    credential_evidence_ref: str | None = None
    credential_evidence_digest: str | None = None
    mutation_active: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "sections", tuple(self.sections))
        object.__setattr__(self, "artifact_paths", tuple(self.artifact_paths))
        object.__setattr__(self, "links", tuple(self.links))
        counts = dict(self.lifecycle_counts)
        object.__setattr__(self, "lifecycle_counts", MappingProxyType(counts))
        if not isinstance(self.tenant, TenantReferenceV1):
            _fail("evidence_cross_link_invalid")
        if type(self.mutation_active) is not bool:
            _fail("evidence_cross_link_invalid")

    def as_kwargs(self) -> dict[str, object]:
        return {field.name: getattr(self, field.name) for field in fields(self)}


@dataclass(frozen=True, slots=True)
class RunEvidenceSnapshotV1:
    tenant_ref: str
    thread_ref: str
    run_ref: str
    terminal_status: str
    safe_stop_reason: str
    accepted_at: str
    completed_at: str
    admission: Mapping[str, object]
    assembly: Mapping[str, object]
    lifecycle: Mapping[str, object]
    sections: tuple[EvidenceSectionV1, ...]
    artifact_paths: tuple[str, ...]
    links: tuple[EvidenceLinkV1, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "admission", MappingProxyType(dict(self.admission)))
        object.__setattr__(self, "assembly", MappingProxyType(dict(self.assembly)))
        object.__setattr__(self, "lifecycle", MappingProxyType(dict(self.lifecycle)))
        object.__setattr__(self, "sections", tuple(self.sections))
        object.__setattr__(self, "artifact_paths", tuple(self.artifact_paths))
        object.__setattr__(self, "links", tuple(self.links))

    def to_manifest(self, artifacts: Sequence[EvidenceArtifactV1]) -> RunEvidenceBundleManifestV1:
        qualification = next(section for section in self.sections if section.name == "qualification")
        return RunEvidenceBundleManifestV1.create(
            tenant_ref=self.tenant_ref,
            thread_ref=self.thread_ref,
            run_ref=self.run_ref,
            terminal={
                "status": self.terminal_status,
                "stop_reason": self.safe_stop_reason,
                "accepted_at": self.accepted_at,
                "completed_at": self.completed_at,
            },
            admission=self.admission,
            assembly=self.assembly,
            lifecycle=self.lifecycle,
            evidence_sections=self.sections,
            evidence_links=self.links,
            artifacts=artifacts,
            qualification={
                "state": qualification.state,
                "root_digest": qualification.root_digest,
                "reason_code": qualification.reason_code,
            },
        )


class RunEvidenceSnapshotReader(Protocol):
    async def read(self, request: EvidenceSnapshotRequest) -> EvidenceSnapshotSourceV1: ...

    async def revalidate(
        self,
        request: EvidenceSnapshotRequest,
        source: EvidenceSnapshotSourceV1,
    ) -> bool: ...


class RunEvidenceSnapshotService:
    """Build a coherent terminal snapshot through one injected repository port."""

    def __init__(self, reader: RunEvidenceSnapshotReader, *, max_attempts: int = 2) -> None:
        if type(max_attempts) is not int or not 1 <= max_attempts <= 3:
            raise ValueError("max_attempts must be between 1 and 3")
        self._reader = reader
        self._max_attempts = max_attempts

    async def build(self, request: EvidenceSnapshotRequest) -> RunEvidenceSnapshotV1:
        for _attempt in range(self._max_attempts):
            source = await self._reader.read(request)
            if source.tenant != request.tenant or source.thread_id != request.thread_id or source.run_id != request.run_id:
                _fail("evidence_cross_link_invalid")
            snapshot = self.validate_source(source)
            if await self._reader.revalidate(request, source):
                return snapshot
        _fail("evidence_snapshot_changed")

    @staticmethod
    def validate_source(source: EvidenceSnapshotSourceV1) -> RunEvidenceSnapshotV1:
        if not isinstance(source, EvidenceSnapshotSourceV1):
            _fail("evidence_cross_link_invalid")
        if source.mutation_active:
            _fail("run_operation_active")
        if source.terminal_status not in TERMINAL_RUN_STATUSES:
            _fail("run_not_terminal")
        _safe_identifier(source.safe_stop_reason)
        accepted_at = _timestamp(source.accepted_at)
        completed_at = _timestamp(source.completed_at)
        if source.completed_at.astimezone(UTC) < source.accepted_at.astimezone(UTC):
            _fail("evidence_cross_link_invalid")
        for digest in (
            source.accepted_invocation_digest,
            source.accepted_context_digest,
            source.agent_revision_digest,
            source.assembly_evidence_digest,
            source.assembly_fingerprint,
            source.terminal_event_digest,
        ):
            _require_digest(digest)
        for digest in (
            source.subagent_catalog_digest,
            source.skill_scopes_digest,
            source.capability_manifest_digest,
            source.tool_plane_revision_digest,
            source.tool_plane_base_revision_digest,
            source.tool_plane_user_overlay_digest,
            source.tool_plane_projection_digest,
            source.credential_evidence_digest,
        ):
            _optional_digest(digest)
        tool_plane_digests = (
            source.tool_plane_base_revision_digest,
            source.tool_plane_user_overlay_digest,
            source.tool_plane_projection_digest,
            source.tool_plane_revision_digest,
        )
        if any(digest is None for digest in tool_plane_digests) != all(digest is None for digest in tool_plane_digests):
            _fail("evidence_cross_link_invalid")
        if source.credential_evidence_ref is not None:
            _safe_identifier(
                source.credential_evidence_ref,
                "evidence_cross_link_invalid",
            )
            if source.credential_evidence_digest is None:
                _fail("evidence_cross_link_invalid")
        for count, digest in (
            (
                source.subagent_catalog_entry_count,
                source.subagent_catalog_digest,
            ),
            (source.skill_scope_count, source.skill_scopes_digest),
        ):
            if type(count) is not int or count < 0 or count > MAX_EVIDENCE_REFERENCES or (digest is None and count != 0):
                _fail("evidence_cross_link_invalid")
        _optional_sha256_digest(source.extension_artifact_manifest_digest)
        _optional_sha256_digest(source.extension_configuration_digest)
        if (source.extension_artifact_manifest_digest is None) != (source.extension_configuration_digest is None):
            _fail("evidence_cross_link_invalid")
        if type(source.accepted_invocation_version) is not int or source.accepted_invocation_version < 1:
            _fail("evidence_cross_link_invalid")
        if type(source.lifecycle_high_water_mark) is not int or source.lifecycle_high_water_mark < 1:
            _fail("evidence_cross_link_invalid")
        if type(source.lifecycle_event_count) is not int or not 1 <= source.lifecycle_event_count <= MAX_LIFECYCLE_EVENTS:
            _fail("bundle_limit_exceeded")
        counts = dict(source.lifecycle_counts)
        if len(counts) > MAX_SAFE_COUNTS or set(counts) - _SAFE_COUNT_KEYS:
            _fail("evidence_cross_link_invalid")
        if any(type(value) is not int or value < 0 for value in counts.values()) or sum(counts.values()) != source.lifecycle_event_count:
            _fail("evidence_cross_link_invalid")
        sections = tuple(sorted(source.sections, key=lambda section: section.name))
        if tuple(section.name for section in sections) != tuple(sorted(EXPECTED_EVIDENCE_SECTIONS)):
            _fail("evidence_incomplete")
        links = _validate_evidence_links(sections, source.links)
        _validate_section_anchors(
            sections,
            {
                "accepted_invocation": (source.accepted_invocation_digest,),
                "actor_credential": (() if source.credential_evidence_digest is None else (source.credential_evidence_digest,)),
                "assembly": (source.assembly_evidence_digest,),
                "subagent_catalog": (() if source.subagent_catalog_digest is None else (source.subagent_catalog_digest,)),
                "skill_material": (() if source.skill_scopes_digest is None else (source.skill_scopes_digest,)),
                "extension_material": tuple(
                    reference
                    for reference in (
                        source.capability_manifest_digest,
                        None if source.extension_artifact_manifest_digest is None else source.extension_artifact_manifest_digest.removeprefix("sha256:"),
                        None if source.extension_configuration_digest is None else source.extension_configuration_digest.removeprefix("sha256:"),
                    )
                    if reference is not None
                ),
                "tool_plane": tuple(digest for digest in tool_plane_digests if digest is not None),
                "lifecycle": (source.terminal_event_digest,),
            },
        )
        incomplete = [section for section in sections if section.required and section.state != "complete"]
        if incomplete:
            states = {section.state for section in incomplete}
            if "pruned" in states:
                _fail("evidence_pruned")
            if "legacy" in states:
                _fail("evidence_legacy_unbound")
            _fail("evidence_incomplete")
        if len(source.artifact_paths) > MAX_ARTIFACTS or len(set(source.artifact_paths)) != len(source.artifact_paths):
            _fail("bundle_limit_exceeded")

        admission = {
            "accepted_invocation_digest": source.accepted_invocation_digest,
            "accepted_invocation_version": source.accepted_invocation_version,
            "accepted_context_digest": source.accepted_context_digest,
            "agent_revision_digest": source.agent_revision_digest,
            "subagent_catalog_digest": source.subagent_catalog_digest,
            "subagent_catalog_entry_count": source.subagent_catalog_entry_count,
            "skill_scopes_digest": source.skill_scopes_digest,
            "skill_scope_count": source.skill_scope_count,
            "capability_manifest_digest": source.capability_manifest_digest,
            "extension_artifact_manifest_digest": source.extension_artifact_manifest_digest,
            "extension_configuration_digest": source.extension_configuration_digest,
            "tool_plane_revision_digest": source.tool_plane_revision_digest,
            "tool_plane_base_revision_digest": source.tool_plane_base_revision_digest,
            "tool_plane_user_overlay_digest": source.tool_plane_user_overlay_digest,
            "tool_plane_projection_digest": source.tool_plane_projection_digest,
            "credential_evidence_ref": source.credential_evidence_ref,
            "credential_evidence_digest": source.credential_evidence_digest,
        }
        assembly = {
            "evidence_digest": source.assembly_evidence_digest,
            "fingerprint": source.assembly_fingerprint,
        }
        lifecycle = {
            "high_water_mark": source.lifecycle_high_water_mark,
            "terminal_event_digest": source.terminal_event_digest,
            "event_count": source.lifecycle_event_count,
            "safe_counts": {key: counts[key] for key in sorted(counts)},
        }
        return RunEvidenceSnapshotV1(
            tenant_ref=source.tenant.public_ref,
            thread_ref=public_evidence_reference("thread", source.tenant.digest, source.thread_id),
            run_ref=public_evidence_reference("run", source.tenant.digest, source.run_id),
            terminal_status=source.terminal_status,
            safe_stop_reason=source.safe_stop_reason,
            accepted_at=accepted_at,
            completed_at=completed_at,
            admission=admission,
            assembly=assembly,
            lifecycle=lifecycle,
            sections=sections,
            artifact_paths=source.artifact_paths,
            links=links,
        )


@dataclass(frozen=True, slots=True)
class RunEvidenceBundleManifestV1:
    schema: str
    schema_version: int
    canonicalization: str
    canonicalization_version: int
    bundle_ref: str
    tenant_ref: str
    thread_ref: str
    run_ref: str
    terminal: Mapping[str, object]
    admission: Mapping[str, object]
    assembly: Mapping[str, object]
    lifecycle: Mapping[str, object]
    evidence_sections: tuple[EvidenceSectionV1, ...]
    evidence_links: tuple[EvidenceLinkV1, ...]
    artifacts: tuple[EvidenceArtifactV1, ...]
    qualification: Mapping[str, object]
    completeness: Mapping[str, object]
    limitations: tuple[str, ...]
    manifest_digest: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "terminal", MappingProxyType(dict(self.terminal)))
        object.__setattr__(self, "admission", MappingProxyType(dict(self.admission)))
        object.__setattr__(self, "assembly", MappingProxyType(dict(self.assembly)))
        lifecycle = dict(self.lifecycle)
        if isinstance(lifecycle.get("safe_counts"), Mapping):
            lifecycle["safe_counts"] = MappingProxyType(dict(lifecycle["safe_counts"]))
        object.__setattr__(self, "lifecycle", MappingProxyType(lifecycle))
        object.__setattr__(self, "qualification", MappingProxyType(dict(self.qualification)))
        completeness = dict(self.completeness)
        object.__setattr__(self, "completeness", MappingProxyType(completeness))
        object.__setattr__(self, "evidence_sections", tuple(self.evidence_sections))
        object.__setattr__(self, "evidence_links", tuple(self.evidence_links))
        object.__setattr__(self, "artifacts", tuple(self.artifacts))
        object.__setattr__(self, "limitations", tuple(self.limitations))
        self._validate()

    @classmethod
    def create(
        cls,
        *,
        tenant_ref: str,
        thread_ref: str,
        run_ref: str,
        terminal: Mapping[str, object],
        admission: Mapping[str, object],
        assembly: Mapping[str, object],
        lifecycle: Mapping[str, object],
        evidence_sections: Sequence[EvidenceSectionV1],
        evidence_links: Sequence[EvidenceLinkV1],
        artifacts: Sequence[EvidenceArtifactV1],
        qualification: Mapping[str, object],
    ) -> RunEvidenceBundleManifestV1:
        sorted_sections = tuple(sorted(evidence_sections, key=lambda item: item.name))
        sorted_links = _validate_evidence_links(sorted_sections, evidence_links)
        sorted_artifacts = tuple(sorted(artifacts, key=lambda item: item.path))
        completeness = {
            "profile": "complete_durable",
            "state": "complete",
            "section_count": len(sorted_sections),
        }
        base: dict[str, object] = {
            "schema": RUN_EVIDENCE_SCHEMA,
            "schema_version": RUN_EVIDENCE_SCHEMA_VERSION,
            "canonicalization": RUN_EVIDENCE_CANONICALIZATION,
            "canonicalization_version": RUN_EVIDENCE_CANONICALIZATION_VERSION,
            "tenant_ref": tenant_ref,
            "thread_ref": thread_ref,
            "run_ref": run_ref,
            "terminal": dict(terminal),
            "admission": dict(admission),
            "assembly": dict(assembly),
            "lifecycle": {
                **dict(lifecycle),
                "safe_counts": dict(lifecycle["safe_counts"]),
            },
            "evidence_sections": [section.to_dict() for section in sorted_sections],
            "evidence_links": [link.to_dict() for link in sorted_links],
            "artifacts": [artifact.to_dict() for artifact in sorted_artifacts],
            "qualification": dict(qualification),
            "completeness": completeness,
            "limitations": list(RUN_EVIDENCE_LIMITATIONS),
            "manifest_digest": None,
        }
        bundle_ref = (
            "bundle-"
            + _domain_digest(
                BUNDLE_REFERENCE_DOMAIN,
                base,
            )[:24]
        )
        manifest_digest = _domain_digest(
            MANIFEST_DIGEST_DOMAIN,
            {**base, "bundle_ref": bundle_ref},
        )
        return cls(
            schema=RUN_EVIDENCE_SCHEMA,
            schema_version=RUN_EVIDENCE_SCHEMA_VERSION,
            canonicalization=RUN_EVIDENCE_CANONICALIZATION,
            canonicalization_version=RUN_EVIDENCE_CANONICALIZATION_VERSION,
            bundle_ref=bundle_ref,
            tenant_ref=tenant_ref,
            thread_ref=thread_ref,
            run_ref=run_ref,
            terminal=terminal,
            admission=admission,
            assembly=assembly,
            lifecycle=lifecycle,
            evidence_sections=sorted_sections,
            evidence_links=sorted_links,
            artifacts=sorted_artifacts,
            qualification=qualification,
            completeness=completeness,
            limitations=RUN_EVIDENCE_LIMITATIONS,
            manifest_digest=manifest_digest,
        )

    def _projection(self, *, manifest_digest: str | None, include_bundle_ref: bool = True) -> dict[str, object]:
        result = {
            "schema": self.schema,
            "schema_version": self.schema_version,
            "canonicalization": self.canonicalization,
            "canonicalization_version": self.canonicalization_version,
            "tenant_ref": self.tenant_ref,
            "thread_ref": self.thread_ref,
            "run_ref": self.run_ref,
            "terminal": dict(self.terminal),
            "admission": dict(self.admission),
            "assembly": dict(self.assembly),
            "lifecycle": {**dict(self.lifecycle), "safe_counts": dict(self.lifecycle["safe_counts"])},
            "evidence_sections": [section.to_dict() for section in self.evidence_sections],
            "evidence_links": [link.to_dict() for link in self.evidence_links],
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "qualification": dict(self.qualification),
            "completeness": dict(self.completeness),
            "limitations": list(self.limitations),
            "manifest_digest": manifest_digest,
        }
        if include_bundle_ref:
            result["bundle_ref"] = self.bundle_ref
        return result

    def _expected_bundle_ref(self) -> str:
        digest = _domain_digest(
            BUNDLE_REFERENCE_DOMAIN,
            self._projection(manifest_digest=None, include_bundle_ref=False),
        )
        return f"bundle-{digest[:24]}"

    def _expected_manifest_digest(self) -> str:
        return _domain_digest(MANIFEST_DIGEST_DOMAIN, self._projection(manifest_digest=None))

    def _validate(self) -> None:
        if (
            self.schema != RUN_EVIDENCE_SCHEMA
            or type(self.schema_version) is not int
            or self.schema_version != RUN_EVIDENCE_SCHEMA_VERSION
            or self.canonicalization != RUN_EVIDENCE_CANONICALIZATION
            or type(self.canonicalization_version) is not int
            or self.canonicalization_version != RUN_EVIDENCE_CANONICALIZATION_VERSION
        ):
            _fail("manifest_version_unsupported")
        public_references = {
            "bundle": self.bundle_ref,
            "tenant": self.tenant_ref,
            "thread": self.thread_ref,
            "run": self.run_ref,
        }
        if any(_PUBLIC_REFERENCE_RE.fullmatch(reference) is None or not reference.startswith(f"{kind}-") for kind, reference in public_references.items()):
            _fail("manifest_fields_invalid")
        expected_terminal = {"status", "stop_reason", "accepted_at", "completed_at"}
        if set(self.terminal) != expected_terminal or self.terminal.get("status") not in TERMINAL_RUN_STATUSES:
            _fail("manifest_fields_invalid")
        _safe_identifier(self.terminal.get("stop_reason"), "manifest_fields_invalid")
        accepted = _parse_timestamp(self.terminal.get("accepted_at"))
        completed = _parse_timestamp(self.terminal.get("completed_at"))
        if completed < accepted:
            _fail("manifest_fields_invalid")
        expected_admission = {
            "accepted_invocation_digest",
            "accepted_invocation_version",
            "accepted_context_digest",
            "agent_revision_digest",
            "subagent_catalog_digest",
            "subagent_catalog_entry_count",
            "skill_scopes_digest",
            "skill_scope_count",
            "capability_manifest_digest",
            "extension_artifact_manifest_digest",
            "extension_configuration_digest",
            "tool_plane_revision_digest",
            "tool_plane_base_revision_digest",
            "tool_plane_user_overlay_digest",
            "tool_plane_projection_digest",
            "credential_evidence_ref",
            "credential_evidence_digest",
        }
        if set(self.admission) != expected_admission:
            _fail("manifest_fields_invalid")
        for key in (
            "accepted_invocation_digest",
            "accepted_context_digest",
            "agent_revision_digest",
        ):
            _require_digest(self.admission.get(key), "manifest_fields_invalid")
        if type(self.admission.get("accepted_invocation_version")) is not int or self.admission["accepted_invocation_version"] < 1:  # type: ignore[operator]
            _fail("manifest_fields_invalid")
        for count_key, digest_key in (
            ("subagent_catalog_entry_count", "subagent_catalog_digest"),
            ("skill_scope_count", "skill_scopes_digest"),
        ):
            count = self.admission.get(count_key)
            if type(count) is not int or count < 0 or count > MAX_EVIDENCE_REFERENCES or (self.admission.get(digest_key) is None and count != 0):
                _fail("manifest_fields_invalid")
        credential_ref = self.admission.get("credential_evidence_ref")
        if credential_ref is not None:
            _safe_identifier(credential_ref, "manifest_fields_invalid")
            if self.admission.get("credential_evidence_digest") is None:
                _fail("evidence_cross_link_invalid")
        for key in (
            "subagent_catalog_digest",
            "skill_scopes_digest",
            "capability_manifest_digest",
            "tool_plane_revision_digest",
            "tool_plane_base_revision_digest",
            "tool_plane_user_overlay_digest",
            "tool_plane_projection_digest",
            "credential_evidence_digest",
        ):
            if self.admission.get(key) is not None:
                _require_digest(self.admission[key], "manifest_fields_invalid")
        for key in ("extension_artifact_manifest_digest", "extension_configuration_digest"):
            value = self.admission.get(key)
            if value is not None and (not isinstance(value, str) or _SHA256_DIGEST_RE.fullmatch(value) is None):
                _fail("manifest_fields_invalid")
        if (self.admission.get("extension_artifact_manifest_digest") is None) != (self.admission.get("extension_configuration_digest") is None):
            _fail("evidence_cross_link_invalid")
        if self.admission.get("extension_artifact_manifest_digest") is not None and self.admission.get("capability_manifest_digest") is None:
            _fail("evidence_cross_link_invalid")
        tool_plane_digests = (
            self.admission.get("tool_plane_base_revision_digest"),
            self.admission.get("tool_plane_user_overlay_digest"),
            self.admission.get("tool_plane_projection_digest"),
            self.admission.get("tool_plane_revision_digest"),
        )
        if any(digest is None for digest in tool_plane_digests) != all(digest is None for digest in tool_plane_digests):
            _fail("evidence_cross_link_invalid")
        if set(self.assembly) != {"evidence_digest", "fingerprint"}:
            _fail("manifest_fields_invalid")
        _require_digest(self.assembly.get("evidence_digest"), "manifest_fields_invalid")
        _require_digest(self.assembly.get("fingerprint"), "manifest_fields_invalid")
        if set(self.lifecycle) != {"high_water_mark", "terminal_event_digest", "event_count", "safe_counts"}:
            _fail("manifest_fields_invalid")
        high_water = self.lifecycle.get("high_water_mark")
        event_count = self.lifecycle.get("event_count")
        safe_counts = self.lifecycle.get("safe_counts")
        if type(high_water) is not int or high_water < 1 or type(event_count) is not int or not 1 <= event_count <= MAX_LIFECYCLE_EVENTS or not isinstance(safe_counts, Mapping):
            if type(event_count) is int and event_count > MAX_LIFECYCLE_EVENTS:
                _fail("bundle_limit_exceeded")
            _fail("manifest_fields_invalid")
        _require_digest(self.lifecycle.get("terminal_event_digest"), "manifest_fields_invalid")
        if set(safe_counts) - _SAFE_COUNT_KEYS or any(type(value) is not int or value < 0 for value in safe_counts.values()) or sum(safe_counts.values()) != event_count:
            _fail("manifest_fields_invalid")
        if tuple(section.name for section in self.evidence_sections) != tuple(sorted(EXPECTED_EVIDENCE_SECTIONS)):
            _fail("manifest_fields_invalid")
        if self.evidence_links != _validate_evidence_links(
            self.evidence_sections,
            self.evidence_links,
        ):
            _fail("manifest_fields_invalid")
        _validate_section_anchors(
            self.evidence_sections,
            {
                "accepted_invocation": (self.admission["accepted_invocation_digest"],),  # type: ignore[dict-item]
                "actor_credential": (() if self.admission["credential_evidence_digest"] is None else (self.admission["credential_evidence_digest"],)),  # type: ignore[dict-item]
                "assembly": (self.assembly["evidence_digest"],),  # type: ignore[dict-item]
                "subagent_catalog": (() if self.admission["subagent_catalog_digest"] is None else (self.admission["subagent_catalog_digest"],)),  # type: ignore[dict-item]
                "skill_material": (() if self.admission["skill_scopes_digest"] is None else (self.admission["skill_scopes_digest"],)),  # type: ignore[dict-item]
                "extension_material": tuple(
                    reference
                    for reference in (
                        self.admission["capability_manifest_digest"],
                        None if self.admission["extension_artifact_manifest_digest"] is None else str(self.admission["extension_artifact_manifest_digest"]).removeprefix("sha256:"),
                        None if self.admission["extension_configuration_digest"] is None else str(self.admission["extension_configuration_digest"]).removeprefix("sha256:"),
                    )
                    if reference is not None
                ),  # type: ignore[dict-item]
                "tool_plane": tuple(digest for digest in tool_plane_digests if digest is not None),  # type: ignore[misc]
                "lifecycle": (self.lifecycle["terminal_event_digest"],),  # type: ignore[dict-item]
            },
        )
        if any(section.required and section.state != "complete" for section in self.evidence_sections):
            _fail("evidence_incomplete")
        if len(self.artifacts) > MAX_ARTIFACTS or tuple(artifact.path for artifact in self.artifacts) != tuple(sorted(artifact.path for artifact in self.artifacts)):
            _fail("manifest_fields_invalid")
        collision_keys = [unicodedata.normalize("NFC", artifact.path).casefold() for artifact in self.artifacts]
        if len(collision_keys) != len(set(collision_keys)) or sum(artifact.size for artifact in self.artifacts) > MAX_TOTAL_ARTIFACT_BYTES:
            _fail("bundle_limit_exceeded")
        qualification = next(section for section in self.evidence_sections if section.name == "qualification")
        if set(self.qualification) != {"state", "root_digest", "reason_code"} or self.qualification != {
            "state": qualification.state,
            "root_digest": qualification.root_digest,
            "reason_code": qualification.reason_code,
        }:
            _fail("evidence_cross_link_invalid")
        if self.completeness != {"profile": "complete_durable", "state": "complete", "section_count": len(EXPECTED_EVIDENCE_SECTIONS)}:
            _fail("manifest_fields_invalid")
        if self.limitations != RUN_EVIDENCE_LIMITATIONS:
            _fail("manifest_fields_invalid")
        if self.bundle_ref != self._expected_bundle_ref():
            _fail("manifest_digest_invalid")
        if self.manifest_digest != self._expected_manifest_digest():
            _fail("manifest_digest_invalid")
        if len(canonical_json_bytes(self.to_dict())) > MAX_MANIFEST_BYTES:
            _fail("bundle_limit_exceeded")

    def to_dict(self) -> dict[str, object]:
        return self._projection(manifest_digest=self.manifest_digest)

    def canonical_bytes(self) -> bytes:
        return canonical_json_bytes(self.to_dict())

    @classmethod
    def from_dict(cls, value: object) -> RunEvidenceBundleManifestV1:
        expected = {
            "schema",
            "schema_version",
            "canonicalization",
            "canonicalization_version",
            "bundle_ref",
            "tenant_ref",
            "thread_ref",
            "run_ref",
            "terminal",
            "admission",
            "assembly",
            "lifecycle",
            "evidence_sections",
            "evidence_links",
            "artifacts",
            "qualification",
            "completeness",
            "limitations",
            "manifest_digest",
        }
        if not isinstance(value, Mapping) or set(value) != expected:
            _fail("manifest_fields_invalid")
        if (
            type(value.get("schema_version")) is not int
            or value.get("schema_version") != RUN_EVIDENCE_SCHEMA_VERSION
            or type(value.get("canonicalization_version")) is not int
            or value.get("canonicalization_version") != RUN_EVIDENCE_CANONICALIZATION_VERSION
        ):
            _fail("manifest_version_unsupported")
        sections = value.get("evidence_sections")
        links = value.get("evidence_links")
        artifacts = value.get("artifacts")
        limitations = value.get("limitations")
        if not isinstance(sections, list) or not isinstance(links, list) or not isinstance(artifacts, list) or not isinstance(limitations, list):
            _fail("manifest_fields_invalid")
        mappings = (value.get("terminal"), value.get("admission"), value.get("assembly"), value.get("lifecycle"), value.get("qualification"), value.get("completeness"))
        if any(not isinstance(item, Mapping) for item in mappings):
            _fail("manifest_fields_invalid")
        try:
            return cls(
                schema=value["schema"],  # type: ignore[arg-type]
                schema_version=value["schema_version"],  # type: ignore[arg-type]
                canonicalization=value["canonicalization"],  # type: ignore[arg-type]
                canonicalization_version=value["canonicalization_version"],  # type: ignore[arg-type]
                bundle_ref=value["bundle_ref"],  # type: ignore[arg-type]
                tenant_ref=value["tenant_ref"],  # type: ignore[arg-type]
                thread_ref=value["thread_ref"],  # type: ignore[arg-type]
                run_ref=value["run_ref"],  # type: ignore[arg-type]
                terminal=value["terminal"],  # type: ignore[arg-type]
                admission=value["admission"],  # type: ignore[arg-type]
                assembly=value["assembly"],  # type: ignore[arg-type]
                lifecycle=value["lifecycle"],  # type: ignore[arg-type]
                evidence_sections=tuple(EvidenceSectionV1.from_dict(item) for item in sections),
                evidence_links=tuple(EvidenceLinkV1.from_dict(item) for item in links),
                artifacts=tuple(EvidenceArtifactV1.from_dict(item) for item in artifacts),
                qualification=value["qualification"],  # type: ignore[arg-type]
                completeness=value["completeness"],  # type: ignore[arg-type]
                limitations=tuple(limitations),  # type: ignore[arg-type]
                manifest_digest=value["manifest_digest"],  # type: ignore[arg-type]
            )
        except RunEvidenceBundleError:
            raise
        except (KeyError, TypeError, ValueError) as exc:
            raise RunEvidenceBundleError("manifest_fields_invalid") from exc

    @classmethod
    def from_bytes(cls, value: bytes) -> RunEvidenceBundleManifestV1:
        if not isinstance(value, bytes) or len(value) > MAX_MANIFEST_BYTES:
            _fail("bundle_limit_exceeded")
        try:
            document = json.loads(value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RunEvidenceBundleError("manifest_not_canonical") from exc
        manifest = cls.from_dict(document)
        if value != manifest.canonical_bytes():
            _fail("manifest_not_canonical")
        return manifest


__all__ = [
    "BUNDLE_REFERENCE_DOMAIN",
    "EVIDENCE_ROOT_DOMAIN",
    "EXPECTED_EVIDENCE_SECTIONS",
    "EvidenceArtifactV1",
    "EvidenceLinkV1",
    "EvidenceSectionV1",
    "EvidenceSnapshotRequest",
    "EvidenceSnapshotSourceV1",
    "MANIFEST_DIGEST_DOMAIN",
    "MAX_ARTIFACTS",
    "MAX_ARTIFACT_BYTES",
    "MAX_EVIDENCE_LINKS",
    "MAX_EVIDENCE_REFERENCES",
    "MAX_LIFECYCLE_EVENTS",
    "MAX_MANIFEST_BYTES",
    "MAX_TOTAL_ARTIFACT_BYTES",
    "RUN_EVIDENCE_CANONICALIZATION",
    "RUN_EVIDENCE_CANONICALIZATION_VERSION",
    "RUN_EVIDENCE_LIMITATIONS",
    "RUN_EVIDENCE_MANIFEST_PATH",
    "RUN_EVIDENCE_SCHEMA",
    "RUN_EVIDENCE_SCHEMA_VERSION",
    "RunEvidenceBundleError",
    "RunEvidenceBundleManifestV1",
    "RunEvidenceSnapshotReader",
    "RunEvidenceSnapshotService",
    "RunEvidenceSnapshotV1",
    "canonical_evidence_root",
    "canonical_json_bytes",
    "public_evidence_reference",
]
