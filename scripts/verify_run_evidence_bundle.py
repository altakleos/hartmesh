#!/usr/bin/env python3
"""Verify the internal integrity of a HartMesh run evidence bundle.

This command intentionally uses only the Python standard library.  Successful
verification proves that the ZIP, canonical manifest, declared references,
and artifact bytes agree with one another.  It does not verify who created the
bundle: evidence bundles are digest-bound, not signed or independently
attested.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import sys
import unicodedata
import zipfile
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

MANIFEST_PATH = "hartmesh-evidence/manifest.v1.json"
SCHEMA = "hartmesh.run-evidence-bundle"
SCHEMA_VERSION = 1
CANONICALIZATION = "utf8-nfc-sorted-json"
CANONICALIZATION_VERSION = 1
MANIFEST_DIGEST_DOMAIN = b"hartmesh.run-evidence-bundle.manifest.v1\x00"
BUNDLE_REFERENCE_DOMAIN = b"hartmesh.run-evidence-bundle.reference.v1\x00"
EVIDENCE_ROOT_DOMAIN = b"hartmesh.run-evidence-bundle.section-root.v1\x00"

MAX_MANIFEST_BYTES = 1024 * 1024
MAX_ARTIFACTS = 50
MAX_ENTRIES = MAX_ARTIFACTS + 1
MAX_ARTIFACT_BYTES = 50 * 1024 * 1024
MAX_TOTAL_ARTIFACT_BYTES = 100 * 1024 * 1024
MAX_LIFECYCLE_EVENTS = 100_000
MAX_ARCHIVE_BYTES = MAX_TOTAL_ARTIFACT_BYTES + MAX_MANIFEST_BYTES + 256 * 1024
MAX_PATH_BYTES = 1024
MAX_REFERENCES = 4096
MAX_EVIDENCE_LINKS = 4096
CHUNK_BYTES = 1024 * 1024

EXPECTED_SECTIONS = (
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
SECTION_STATES = {
    "complete",
    "absent_by_design",
    "unsupported",
    "legacy",
    "pruned",
    "unavailable",
    "unqualified",
}
TERMINAL_STATUSES = {"success", "error", "timeout", "interrupted"}
SAFE_COUNT_KEYS = {
    "lifecycle",
    "tools",
    "artifacts",
    "retrieval",
    "mcp",
    "batches",
    "sandbox",
    "other",
}
TOP_LEVEL_FIELDS = {
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
ADMISSION_FIELDS = {
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
SECTION_FIELDS = {
    "name",
    "state",
    "required",
    "item_count",
    "references",
    "root_digest",
    "reason_code",
}
EVIDENCE_LINK_FIELDS = {
    "kind",
    "subject_section",
    "subject_digest",
    "object_section",
    "object_digest",
}
EVIDENCE_LINK_SECTIONS = {
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
ARTIFACT_FIELDS = {"path", "size", "sha256", "media_type"}
LIMITATIONS = (
    "artifact_content_is_not_sanitized",
    "bundle_copy_outlives_server_retention",
    "internal_integrity_only_not_signed",
)

DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
SHA256_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
PUBLIC_REF_RE = re.compile(r"^(?:tenant|run|thread|bundle)-[a-z0-9]{16,32}$")
SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}$")
MEDIA_TYPE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9!#$&^_.+/-]{0,126}$")
ALLOWED_FORMAT_CHARS = {"\u200c", "\u200d"}
WINDOWS_INVALID_CHARS = set('<>:"|?*')
WINDOWS_DEVICE_NAMES = {
    "con",
    "prn",
    "aux",
    "nul",
    *(f"com{number}" for number in range(1, 10)),
    *(f"lpt{number}" for number in range(1, 10)),
}


class VerificationError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def fail(code: str) -> None:
    raise VerificationError(code)


def canonical_value(value: object) -> object:
    if value is None or isinstance(value, bool | int):
        return value
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value)
    if isinstance(value, list):
        return [canonical_value(item) for item in value]
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            fail("manifest_not_canonical")
        normalized_keys = [unicodedata.normalize("NFC", key) for key in value]
        if len(normalized_keys) != len(set(normalized_keys)):
            fail("manifest_not_canonical")
        return {unicodedata.normalize("NFC", key): canonical_value(item) for key, item in value.items()}
    fail("manifest_not_canonical")


def canonical_bytes(value: object) -> bytes:
    try:
        return json.dumps(
            canonical_value(value),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise VerificationError("manifest_not_canonical") from exc


def domain_digest(domain: bytes, value: object) -> str:
    return hashlib.sha256(domain + canonical_bytes(value)).hexdigest()


def require_digest(value: object, code: str = "manifest_fields_invalid") -> str:
    if not isinstance(value, str) or DIGEST_RE.fullmatch(value) is None:
        fail(code)
    return value


def parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z") or len(value.encode("utf-8")) > 40:
        fail("manifest_fields_invalid")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise VerificationError("manifest_fields_invalid") from exc
    normalized = parsed.astimezone(UTC)
    rendered = normalized.isoformat(timespec="microseconds" if normalized.microsecond else "seconds").replace("+00:00", "Z")
    if rendered != value:
        fail("manifest_not_canonical")
    return normalized


def safe_path(name: object, *, manifest: bool = False) -> str:
    if not isinstance(name, str):
        fail("zip_path_unsafe")
    normalized = unicodedata.normalize("NFC", name)
    parts = name.split("/")
    if (
        name != normalized
        or not name
        or name.startswith("/")
        or name.startswith("//")
        or "\\" in name
        or "\x00" in name
        or len(name.encode("utf-8")) > MAX_PATH_BYTES
        or any(part in {"", ".", ".."} for part in parts)
        or any(any(char in WINDOWS_INVALID_CHARS for char in part) or part.endswith((" ", ".")) or part.split(".", 1)[0].rstrip().casefold() in WINDOWS_DEVICE_NAMES for part in parts)
        or any(any(unicodedata.category(char).startswith("C") and char not in ALLOWED_FORMAT_CHARS for char in part) for part in parts)
    ):
        fail("zip_path_unsafe")
    if not manifest and parts[0].casefold() == "hartmesh-evidence":
        fail("zip_path_unsafe")
    return name


def evidence_root(name: str, references: list[str]) -> str:
    normalized = sorted({require_digest(item) for item in references})
    if len(normalized) != len(references) or len(normalized) > MAX_REFERENCES:
        fail("manifest_fields_invalid")
    return domain_digest(
        EVIDENCE_ROOT_DOMAIN,
        {"section": name, "references": normalized},
    )


def validate_section(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != SECTION_FIELDS:
        fail("manifest_fields_invalid")
    name = value.get("name")
    state = value.get("state")
    required = value.get("required")
    count = value.get("item_count")
    references = value.get("references")
    root = value.get("root_digest")
    reason = value.get("reason_code")
    if name not in EXPECTED_SECTIONS or state not in SECTION_STATES or type(required) is not bool:
        fail("manifest_fields_invalid")
    if type(count) is not int or count < 0 or count > MAX_REFERENCES or not isinstance(references, list):
        fail("manifest_fields_invalid")
    if references != sorted(references) or len(set(references)) != len(references):
        fail("manifest_fields_invalid")
    if state == "complete":
        if count != len(references) or reason is not None or root != evidence_root(name, references):
            fail("evidence_cross_link_invalid")
    elif count != 0 or references or root is not None or not isinstance(reason, str) or SAFE_IDENTIFIER_RE.fullmatch(reason) is None:
        fail("manifest_fields_invalid")
    if required and state != "complete":
        fail("evidence_incomplete")
    return value


def validate_artifact(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != ARTIFACT_FIELDS:
        fail("manifest_fields_invalid")
    path = safe_path(value.get("path"))
    size = value.get("size")
    if type(size) is not int or size < 0 or size > MAX_ARTIFACT_BYTES:
        fail("bundle_limit_exceeded")
    require_digest(value.get("sha256"), "artifact_digest_invalid")
    media_type = value.get("media_type")
    if media_type is not None and (not isinstance(media_type, str) or MEDIA_TYPE_RE.fullmatch(media_type) is None):
        fail("manifest_fields_invalid")
    return {**value, "path": path}


def validate_evidence_links(
    value: object,
    sections: Mapping[str, dict[str, object]],
) -> list[dict[str, object]]:
    if not isinstance(value, list) or len(value) > MAX_EVIDENCE_LINKS:
        fail("bundle_limit_exceeded")
    links: list[dict[str, object]] = []
    keys: list[tuple[str, str, str, str, str]] = []
    for raw in value:
        if not isinstance(raw, dict) or set(raw) != EVIDENCE_LINK_FIELDS:
            fail("manifest_fields_invalid")
        kind = raw.get("kind")
        subject_section = raw.get("subject_section")
        object_section = raw.get("object_section")
        if not isinstance(kind, str) or EVIDENCE_LINK_SECTIONS.get(kind) != (subject_section, object_section):
            fail("evidence_cross_link_invalid")
        subject_digest = require_digest(raw.get("subject_digest"))
        object_digest = require_digest(raw.get("object_digest"))
        subject = sections.get(subject_section)  # type: ignore[arg-type]
        object_value = sections.get(object_section)  # type: ignore[arg-type]
        if subject is None or object_value is None or subject.get("state") != "complete" or object_value.get("state") != "complete" or subject_digest not in subject["references"] or object_digest not in object_value["references"]:
            fail("evidence_cross_link_invalid")
        links.append(raw)
        keys.append(
            (
                kind,
                subject_section,  # type: ignore[arg-type]
                subject_digest,
                object_section,  # type: ignore[arg-type]
                object_digest,
            )
        )
    if keys != sorted(keys) or len(set(keys)) != len(keys):
        fail("manifest_fields_invalid")
    for kind, (subject_name, _object_name) in EVIDENCE_LINK_SECTIONS.items():
        linked_subjects = sorted(link["subject_digest"] for link in links if link["kind"] == kind)
        if linked_subjects != sections[subject_name]["references"]:
            fail("evidence_cross_link_invalid")
    return links


def validate_manifest(raw: bytes) -> dict[str, object]:
    if len(raw) > MAX_MANIFEST_BYTES:
        fail("bundle_limit_exceeded")
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise VerificationError("manifest_not_canonical") from exc
    if not isinstance(document, dict):
        fail("manifest_fields_invalid")
    if type(document.get("schema_version")) is not int or document.get("schema_version") != SCHEMA_VERSION or type(document.get("canonicalization_version")) is not int or document.get("canonicalization_version") != CANONICALIZATION_VERSION:
        fail("manifest_version_unsupported")
    if set(document) != TOP_LEVEL_FIELDS:
        fail("manifest_fields_invalid")
    if document.get("schema") != SCHEMA or document.get("canonicalization") != CANONICALIZATION:
        fail("manifest_version_unsupported")
    if raw != canonical_bytes(document):
        fail("manifest_not_canonical")

    for key, kind in (
        ("bundle_ref", "bundle"),
        ("tenant_ref", "tenant"),
        ("thread_ref", "thread"),
        ("run_ref", "run"),
    ):
        value = document.get(key)
        if not isinstance(value, str) or PUBLIC_REF_RE.fullmatch(value) is None or not value.startswith(f"{kind}-"):
            fail("manifest_fields_invalid")

    terminal = document.get("terminal")
    if not isinstance(terminal, dict) or set(terminal) != {"status", "stop_reason", "accepted_at", "completed_at"} or terminal.get("status") not in TERMINAL_STATUSES:
        fail("manifest_fields_invalid")
    reason = terminal.get("stop_reason")
    if not isinstance(reason, str) or SAFE_IDENTIFIER_RE.fullmatch(reason) is None:
        fail("manifest_fields_invalid")
    accepted_at = parse_timestamp(terminal.get("accepted_at"))
    completed_at = parse_timestamp(terminal.get("completed_at"))
    if completed_at < accepted_at:
        fail("manifest_fields_invalid")

    admission = document.get("admission")
    if not isinstance(admission, dict) or set(admission) != ADMISSION_FIELDS:
        fail("manifest_fields_invalid")
    for key in (
        "accepted_invocation_digest",
        "accepted_context_digest",
        "agent_revision_digest",
    ):
        require_digest(admission.get(key))
    if type(admission.get("accepted_invocation_version")) is not int or admission["accepted_invocation_version"] < 1:
        fail("manifest_fields_invalid")
    for count_key, digest_key in (
        ("subagent_catalog_entry_count", "subagent_catalog_digest"),
        ("skill_scope_count", "skill_scopes_digest"),
    ):
        count = admission.get(count_key)
        if type(count) is not int or count < 0 or count > MAX_REFERENCES or (admission.get(digest_key) is None and count != 0):
            fail("manifest_fields_invalid")
    credential_ref = admission.get("credential_evidence_ref")
    if credential_ref is not None and (not isinstance(credential_ref, str) or SAFE_IDENTIFIER_RE.fullmatch(credential_ref) is None):
        fail("manifest_fields_invalid")
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
        if admission.get(key) is not None:
            require_digest(admission[key])
    if admission.get("credential_evidence_digest") is None and credential_ref is not None:
        fail("evidence_cross_link_invalid")
    tool_plane_digests = (
        admission["tool_plane_base_revision_digest"],
        admission["tool_plane_user_overlay_digest"],
        admission["tool_plane_projection_digest"],
        admission["tool_plane_revision_digest"],
    )
    if any(digest is None for digest in tool_plane_digests) != all(digest is None for digest in tool_plane_digests):
        fail("evidence_cross_link_invalid")
    for key in (
        "extension_artifact_manifest_digest",
        "extension_configuration_digest",
    ):
        value = admission.get(key)
        if value is not None and (not isinstance(value, str) or SHA256_DIGEST_RE.fullmatch(value) is None):
            fail("manifest_fields_invalid")
    if (admission["extension_artifact_manifest_digest"] is None) != (admission["extension_configuration_digest"] is None):
        fail("evidence_cross_link_invalid")
    if admission["extension_artifact_manifest_digest"] is not None and admission["capability_manifest_digest"] is None:
        fail("evidence_cross_link_invalid")

    assembly = document.get("assembly")
    if not isinstance(assembly, dict) or set(assembly) != {"evidence_digest", "fingerprint"}:
        fail("manifest_fields_invalid")
    require_digest(assembly.get("evidence_digest"))
    require_digest(assembly.get("fingerprint"))

    lifecycle = document.get("lifecycle")
    if not isinstance(lifecycle, dict) or set(lifecycle) != {"high_water_mark", "terminal_event_digest", "event_count", "safe_counts"}:
        fail("manifest_fields_invalid")
    high_water = lifecycle.get("high_water_mark")
    event_count = lifecycle.get("event_count")
    counts = lifecycle.get("safe_counts")
    if type(high_water) is not int or high_water < 1 or type(event_count) is not int or not 1 <= event_count <= MAX_LIFECYCLE_EVENTS or not isinstance(counts, dict):
        if type(event_count) is int and event_count > MAX_LIFECYCLE_EVENTS:
            fail("bundle_limit_exceeded")
        fail("manifest_fields_invalid")
    require_digest(lifecycle.get("terminal_event_digest"))
    if set(counts) - SAFE_COUNT_KEYS or any(type(item) is not int or item < 0 for item in counts.values()) or sum(counts.values()) != event_count:
        fail("manifest_fields_invalid")

    raw_sections = document.get("evidence_sections")
    if not isinstance(raw_sections, list):
        fail("manifest_fields_invalid")
    sections = [validate_section(item) for item in raw_sections]
    if [item["name"] for item in sections] != sorted(EXPECTED_SECTIONS):
        fail("manifest_fields_invalid")
    by_name = {item["name"]: item for item in sections}
    for name, anchor in (
        ("accepted_invocation", admission["accepted_invocation_digest"]),
        ("assembly", assembly["evidence_digest"]),
        ("lifecycle", lifecycle["terminal_event_digest"]),
    ):
        if by_name[name]["state"] != "complete" or by_name[name]["references"] != [anchor]:
            fail("evidence_cross_link_invalid")
    optional_anchors = (
        (
            "actor_credential",
            () if admission["credential_evidence_digest"] is None else (admission["credential_evidence_digest"],),
        ),
        (
            "subagent_catalog",
            () if admission["subagent_catalog_digest"] is None else (admission["subagent_catalog_digest"],),
        ),
        (
            "skill_material",
            () if admission["skill_scopes_digest"] is None else (admission["skill_scopes_digest"],),
        ),
        (
            "extension_material",
            tuple(
                reference
                for reference in (
                    admission["capability_manifest_digest"],
                    None if admission["extension_artifact_manifest_digest"] is None else admission["extension_artifact_manifest_digest"].removeprefix("sha256:"),
                    None if admission["extension_configuration_digest"] is None else admission["extension_configuration_digest"].removeprefix("sha256:"),
                )
                if reference is not None
            ),
        ),
        (
            "tool_plane",
            tuple(digest for digest in tool_plane_digests if digest is not None),
        ),
    )
    for name, anchors in optional_anchors:
        section = by_name[name]
        if anchors:
            if section["state"] != "complete" or section["references"] != sorted(anchors):
                fail("evidence_cross_link_invalid")
        elif section["state"] != "absent_by_design":
            fail("evidence_cross_link_invalid")

    validate_evidence_links(document.get("evidence_links"), by_name)

    raw_artifacts = document.get("artifacts")
    if not isinstance(raw_artifacts, list) or len(raw_artifacts) > MAX_ARTIFACTS:
        fail("bundle_limit_exceeded")
    artifacts = [validate_artifact(item) for item in raw_artifacts]
    paths = [item["path"] for item in artifacts]
    if paths != sorted(paths) or len({path.casefold() for path in paths}) != len(paths):
        fail("manifest_fields_invalid")
    if sum(item["size"] for item in artifacts) > MAX_TOTAL_ARTIFACT_BYTES:
        fail("bundle_limit_exceeded")

    qualification = document.get("qualification")
    expected_qualification = {
        "state": by_name["qualification"]["state"],
        "root_digest": by_name["qualification"]["root_digest"],
        "reason_code": by_name["qualification"]["reason_code"],
    }
    if qualification != expected_qualification:
        fail("evidence_cross_link_invalid")
    completeness = document.get("completeness")
    if completeness != {
        "profile": "complete_durable",
        "state": "complete",
        "section_count": len(EXPECTED_SECTIONS),
    }:
        fail("manifest_fields_invalid")
    limitations = document.get("limitations")
    if limitations != list(LIMITATIONS):
        fail("manifest_fields_invalid")

    supplied_digest = require_digest(
        document.get("manifest_digest"),
        "manifest_digest_invalid",
    )
    digest_projection = dict(document)
    digest_projection["manifest_digest"] = None
    if supplied_digest != domain_digest(MANIFEST_DIGEST_DOMAIN, digest_projection):
        fail("manifest_digest_invalid")
    bundle_projection = dict(digest_projection)
    bundle_projection.pop("bundle_ref")
    expected_bundle_ref = (
        "bundle-"
        + domain_digest(
            BUNDLE_REFERENCE_DOMAIN,
            bundle_projection,
        )[:24]
    )
    if document["bundle_ref"] != expected_bundle_ref:
        fail("manifest_digest_invalid")
    return document


def verify(path: Path) -> dict[str, object]:
    try:
        archive_size = path.stat().st_size
    except OSError as exc:
        raise VerificationError("bundle_unreadable") from exc
    if archive_size > MAX_ARCHIVE_BYTES:
        fail("bundle_limit_exceeded")
    try:
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            if not 1 <= len(infos) <= MAX_ENTRIES:
                fail("bundle_limit_exceeded")
            names: list[str] = []
            collision_names: list[str] = []
            total_artifact_size = 0
            for info in infos:
                name = safe_path(
                    info.filename,
                    manifest=info.filename == MANIFEST_PATH,
                )
                names.append(name)
                collision_names.append(unicodedata.normalize("NFC", name).casefold())
                if info.flag_bits & 0x1:
                    fail("zip_encryption_unsupported")
                if info.compress_type != zipfile.ZIP_STORED:
                    fail("zip_compression_unsupported")
                if info.is_dir():
                    fail("zip_path_unsafe")
                unix_mode = (info.external_attr >> 16) & 0o170000
                if unix_mode not in {0, stat.S_IFREG}:
                    fail("zip_entry_unsafe")
                limit = MAX_MANIFEST_BYTES if name == MANIFEST_PATH else MAX_ARTIFACT_BYTES
                if info.file_size < 0 or info.file_size > limit:
                    fail("bundle_limit_exceeded")
                if name != MANIFEST_PATH:
                    total_artifact_size += info.file_size
            if len(names) != len(set(names)) or len(collision_names) != len(set(collision_names)):
                fail("zip_path_duplicate")
            if names.count(MANIFEST_PATH) != 1:
                fail("manifest_missing")
            if total_artifact_size > MAX_TOTAL_ARTIFACT_BYTES:
                fail("bundle_limit_exceeded")

            manifest = validate_manifest(archive.read(MANIFEST_PATH))
            declared = {item["path"]: item for item in manifest["artifacts"]}
            actual = set(names) - {MANIFEST_PATH}
            if actual - set(declared):
                fail("undeclared_entry")
            if set(declared) - actual:
                fail("artifact_missing")

            for name in sorted(actual):
                expected = declared[name]
                digest = hashlib.sha256()
                size = 0
                with archive.open(name) as source:
                    while chunk := source.read(CHUNK_BYTES):
                        size += len(chunk)
                        if size > MAX_ARTIFACT_BYTES:
                            fail("bundle_limit_exceeded")
                        digest.update(chunk)
                if size != expected["size"]:
                    fail("artifact_size_invalid")
                if digest.hexdigest() != expected["sha256"]:
                    fail("artifact_digest_invalid")
    except VerificationError:
        raise
    except (OSError, ValueError, zipfile.BadZipFile, RuntimeError) as exc:
        raise VerificationError("bundle_invalid") from exc

    return {
        "status": "valid",
        "schema_version": SCHEMA_VERSION,
        "bundle_ref": manifest["bundle_ref"],
        "manifest_digest": manifest["manifest_digest"],
        "artifact_count": len(manifest["artifacts"]),
        "authenticity": "not_signed",
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify a HartMesh run evidence bundle (internal digests only; not signed).")
    parser.add_argument("bundle", type=Path)
    args = parser.parse_args(argv)
    try:
        output = verify(args.bundle)
        exit_code = 0
    except VerificationError as exc:
        output = {
            "status": "invalid",
            "code": exc.code,
            "authenticity": "not_signed",
        }
        exit_code = 1
    print(json.dumps(output, sort_keys=True, separators=(",", ":")))
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
