"""Bounded Phase 0 probe for the exact pinned OpenSandbox contracts.

This probe cannot qualify a deployment. It verifies the complete installed SDK
package surface and vendored server OpenAPI bytes before deriving whether the
external primitives needed by a future live qualification can exist.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.metadata
import inspect
import json
import stat
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

EXPECTED_OPEN_SANDBOX_SDK_VERSION = "0.1.15"
EXPECTED_OPEN_SANDBOX_SERVER_VERSION = "0.1.14"
EXPECTED_OPEN_SANDBOX_SERVER_REVISION = "ef13d88f8479089ab6773556d7782b5d92fea53f"
EXPECTED_OPEN_SANDBOX_SDK_REVISION = "e9d0a63919739b1bed05914373acbacb11e37d43"
EXPECTED_OPEN_SANDBOX_SDK_WHEEL_SHA256 = "992b01490551f4d8e3f99caa25e34cb9d1690f0c5027eeebab912738291957d1"
EXPECTED_OPEN_SANDBOX_SDK_FILE_MANIFEST_SHA256 = "6f8bcf0afe74c86046223c433b37085aa4098b3f3df772c4b49d8bda43a479f5"
EXPECTED_OPEN_SANDBOX_SDK_FILE_COUNT = 336
EXPECTED_OPEN_SANDBOX_SDK_TOTAL_BYTES = 1_662_982
EXPECTED_OPEN_SANDBOX_LIFECYCLE_SPEC_SHA256 = "ec59c874d6368fbb00271ad56db5decb08eb538995ee9ed9a07b9a12c776fa08"
EXPECTED_OPEN_SANDBOX_EXECD_SPEC_SHA256 = "6dec86d5e510233b91c2cec5d5451bc862750b7aba3353da5eb498b3a692d100"

_SCHEMA = "hartmesh.opensandbox.accepted_material.feasibility"
_PROBE_KIND = "sdk_surface"
_MAX_ARTIFACT_BYTES = 16 * 1024
_MAX_SPEC_BYTES = 128 * 1024
_MAX_SDK_FILES = 1_024
_MAX_SDK_FILE_BYTES = 4 * 1024 * 1024
_MAX_SDK_TOTAL_BYTES = 16 * 1024 * 1024
_PINNED_SPEC_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "opensandbox_server_0_1_14"
_PRIMITIVE_NAMES = (
    "image_digest_readback",
    "metadata_rediscovery",
    "ownership_compare_and_set",
    "metadata_preserving_renewal",
    "trusted_setup_separation",
    "uid1000_read_only_negative_probes",
    "destroy_and_orphan_reconcile",
)


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _require_sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field_name} must be a lowercase SHA-256 digest")
    return value


def _timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    return value.astimezone(UTC).isoformat().removesuffix("+00:00") + "Z"


def _parse_timestamp(value: object) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError("observed_at must be canonical UTC")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("observed_at must be canonical UTC") from exc
    if _timestamp(parsed) != value:
        raise ValueError("observed_at must be canonical UTC")
    return parsed


class FeasibilityStatus(StrEnum):
    """A surface result never substitutes for a live pass."""

    SURFACE_PRESENT = "surface_present"
    UNSUPPORTED = "unsupported"
    NOT_RUN = "not_run"


@dataclass(frozen=True, slots=True)
class FeasibilityPrimitiveV1:
    """One required primitive derived from the exact pinned contracts."""

    version: Literal[1]
    name: str
    status: FeasibilityStatus
    code: str

    def __post_init__(self) -> None:
        if type(self.version) is not int or self.version != 1:
            raise ValueError("OpenSandbox feasibility primitive is invalid")
        if self.name not in _PRIMITIVE_NAMES:
            raise ValueError("OpenSandbox feasibility primitive is invalid")
        try:
            status = FeasibilityStatus(self.status)
        except (TypeError, ValueError) as exc:
            raise ValueError("OpenSandbox feasibility status is invalid") from exc
        object.__setattr__(self, "status", status)
        if not isinstance(self.code, str) or not self.code or len(self.code.encode("utf-8")) > 128 or any(ord(character) < 0x20 or ord(character) == 0x7F for character in self.code):
            raise ValueError("OpenSandbox feasibility code is invalid")

    def to_persisted(self) -> dict[str, object]:
        return {
            "version": self.version,
            "name": self.name,
            "status": self.status.value,
            "code": self.code,
        }

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        if not isinstance(value, Mapping) or set(value) != {
            "version",
            "name",
            "status",
            "code",
        }:
            raise ValueError("OpenSandbox feasibility primitive has invalid fields")
        return cls(
            version=value["version"],  # type: ignore[arg-type]
            name=value["name"],  # type: ignore[arg-type]
            status=value["status"],  # type: ignore[arg-type]
            code=value["code"],  # type: ignore[arg-type]
        )


@dataclass(frozen=True, slots=True)
class OpenSandboxFeasibilityArtifactV1:
    """Strict secret-free result tied to exact server and SDK bytes."""

    schema: Literal["hartmesh.opensandbox.accepted_material.feasibility"]
    version: Literal[1]
    probe_kind: Literal["sdk_surface"]
    sdk_version: str
    sdk_source_revision: str
    sdk_wheel_sha256: str
    sdk_file_manifest_sha256: str
    sdk_file_count: int
    sdk_total_bytes: int
    server_version: str
    server_source_revision: str
    server_lifecycle_spec_sha256: str
    server_execd_spec_sha256: str
    observed_at: datetime
    primitives: tuple[FeasibilityPrimitiveV1, ...]
    decision: Literal["no_go", "unpassed"]
    blocking_codes: tuple[str, ...]
    digest: str

    def __post_init__(self) -> None:
        if self.schema != _SCHEMA or type(self.version) is not int or self.version != 1 or self.probe_kind != _PROBE_KIND:
            raise ValueError("OpenSandbox feasibility artifact identity is invalid")
        expected_values = {
            "sdk_version": EXPECTED_OPEN_SANDBOX_SDK_VERSION,
            "sdk_source_revision": EXPECTED_OPEN_SANDBOX_SDK_REVISION,
            "sdk_wheel_sha256": EXPECTED_OPEN_SANDBOX_SDK_WHEEL_SHA256,
            "sdk_file_manifest_sha256": (EXPECTED_OPEN_SANDBOX_SDK_FILE_MANIFEST_SHA256),
            "server_version": EXPECTED_OPEN_SANDBOX_SERVER_VERSION,
            "server_source_revision": EXPECTED_OPEN_SANDBOX_SERVER_REVISION,
            "server_lifecycle_spec_sha256": (EXPECTED_OPEN_SANDBOX_LIFECYCLE_SPEC_SHA256),
            "server_execd_spec_sha256": EXPECTED_OPEN_SANDBOX_EXECD_SPEC_SHA256,
        }
        for field_name, expected in expected_values.items():
            if getattr(self, field_name) != expected:
                raise ValueError(
                    f"OpenSandbox feasibility {field_name} is invalid",
                )
        for field_name in (
            "sdk_wheel_sha256",
            "sdk_file_manifest_sha256",
            "server_lifecycle_spec_sha256",
            "server_execd_spec_sha256",
        ):
            _require_sha256(getattr(self, field_name), field_name)
        if type(self.sdk_file_count) is not int or self.sdk_file_count != EXPECTED_OPEN_SANDBOX_SDK_FILE_COUNT or type(self.sdk_total_bytes) is not int or self.sdk_total_bytes != EXPECTED_OPEN_SANDBOX_SDK_TOTAL_BYTES:
            raise ValueError("OpenSandbox feasibility SDK manifest bounds are invalid")
        _timestamp(self.observed_at)
        if not isinstance(self.primitives, tuple) or tuple(item.name for item in self.primitives) != _PRIMITIVE_NAMES:
            raise ValueError("OpenSandbox feasibility primitives are incomplete")
        if any(not isinstance(item, FeasibilityPrimitiveV1) for item in self.primitives):
            raise TypeError("primitives must contain FeasibilityPrimitiveV1")
        expected_blockers = tuple(
            sorted(item.code for item in self.primitives if item.status is FeasibilityStatus.UNSUPPORTED),
        )
        if self.blocking_codes != expected_blockers:
            raise ValueError("OpenSandbox feasibility blocking codes are invalid")
        expected_decision = "no_go" if expected_blockers else "unpassed"
        if self.decision != expected_decision:
            raise ValueError("OpenSandbox feasibility decision is invalid")
        _require_sha256(self.digest, "OpenSandbox feasibility artifact digest")
        if self.digest != _digest(self._digest_payload()):
            raise ValueError("OpenSandbox feasibility artifact digest is invalid")
        if len(self.canonical_bytes()) > _MAX_ARTIFACT_BYTES:
            raise ValueError("OpenSandbox feasibility artifact is too large")

    def _digest_payload(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "version": self.version,
            "probe_kind": self.probe_kind,
            "sdk_version": self.sdk_version,
            "sdk_source_revision": self.sdk_source_revision,
            "sdk_wheel_sha256": self.sdk_wheel_sha256,
            "sdk_file_manifest_sha256": self.sdk_file_manifest_sha256,
            "sdk_file_count": self.sdk_file_count,
            "sdk_total_bytes": self.sdk_total_bytes,
            "server_version": self.server_version,
            "server_source_revision": self.server_source_revision,
            "server_lifecycle_spec_sha256": self.server_lifecycle_spec_sha256,
            "server_execd_spec_sha256": self.server_execd_spec_sha256,
            "observed_at": _timestamp(self.observed_at),
            "primitives": [item.to_persisted() for item in self.primitives],
            "decision": self.decision,
            "blocking_codes": list(self.blocking_codes),
        }

    def to_persisted(self) -> dict[str, object]:
        return {**self._digest_payload(), "digest": self.digest}

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self.to_persisted())

    @classmethod
    def build(
        cls,
        *,
        observed_at: datetime,
        primitives: Sequence[FeasibilityPrimitiveV1],
    ) -> Self:
        primitive_tuple = tuple(primitives)
        blockers = tuple(
            sorted(item.code for item in primitive_tuple if item.status is FeasibilityStatus.UNSUPPORTED),
        )
        payload = {
            "schema": _SCHEMA,
            "version": 1,
            "probe_kind": _PROBE_KIND,
            "sdk_version": EXPECTED_OPEN_SANDBOX_SDK_VERSION,
            "sdk_source_revision": EXPECTED_OPEN_SANDBOX_SDK_REVISION,
            "sdk_wheel_sha256": EXPECTED_OPEN_SANDBOX_SDK_WHEEL_SHA256,
            "sdk_file_manifest_sha256": (EXPECTED_OPEN_SANDBOX_SDK_FILE_MANIFEST_SHA256),
            "sdk_file_count": EXPECTED_OPEN_SANDBOX_SDK_FILE_COUNT,
            "sdk_total_bytes": EXPECTED_OPEN_SANDBOX_SDK_TOTAL_BYTES,
            "server_version": EXPECTED_OPEN_SANDBOX_SERVER_VERSION,
            "server_source_revision": EXPECTED_OPEN_SANDBOX_SERVER_REVISION,
            "server_lifecycle_spec_sha256": (EXPECTED_OPEN_SANDBOX_LIFECYCLE_SPEC_SHA256),
            "server_execd_spec_sha256": EXPECTED_OPEN_SANDBOX_EXECD_SPEC_SHA256,
            "observed_at": _timestamp(observed_at),
            "primitives": [item.to_persisted() for item in primitive_tuple],
            "decision": "no_go" if blockers else "unpassed",
            "blocking_codes": list(blockers),
        }
        return cls(
            schema=_SCHEMA,
            version=1,
            probe_kind=_PROBE_KIND,
            sdk_version=EXPECTED_OPEN_SANDBOX_SDK_VERSION,
            sdk_source_revision=EXPECTED_OPEN_SANDBOX_SDK_REVISION,
            sdk_wheel_sha256=EXPECTED_OPEN_SANDBOX_SDK_WHEEL_SHA256,
            sdk_file_manifest_sha256=(EXPECTED_OPEN_SANDBOX_SDK_FILE_MANIFEST_SHA256),
            sdk_file_count=EXPECTED_OPEN_SANDBOX_SDK_FILE_COUNT,
            sdk_total_bytes=EXPECTED_OPEN_SANDBOX_SDK_TOTAL_BYTES,
            server_version=EXPECTED_OPEN_SANDBOX_SERVER_VERSION,
            server_source_revision=EXPECTED_OPEN_SANDBOX_SERVER_REVISION,
            server_lifecycle_spec_sha256=(EXPECTED_OPEN_SANDBOX_LIFECYCLE_SPEC_SHA256),
            server_execd_spec_sha256=EXPECTED_OPEN_SANDBOX_EXECD_SPEC_SHA256,
            observed_at=observed_at,
            primitives=primitive_tuple,
            decision=payload["decision"],  # type: ignore[arg-type]
            blocking_codes=blockers,
            digest=_digest(payload),
        )

    @classmethod
    def from_persisted(cls, value: object) -> Self:
        fields = {
            "schema",
            "version",
            "probe_kind",
            "sdk_version",
            "sdk_source_revision",
            "sdk_wheel_sha256",
            "sdk_file_manifest_sha256",
            "sdk_file_count",
            "sdk_total_bytes",
            "server_version",
            "server_source_revision",
            "server_lifecycle_spec_sha256",
            "server_execd_spec_sha256",
            "observed_at",
            "primitives",
            "decision",
            "blocking_codes",
            "digest",
        }
        if not isinstance(value, Mapping) or set(value) != fields:
            raise ValueError("OpenSandbox feasibility artifact has invalid fields")
        raw_primitives = value["primitives"]
        raw_blockers = value["blocking_codes"]
        if not isinstance(raw_primitives, Sequence) or isinstance(raw_primitives, (str, bytes, bytearray)) or len(raw_primitives) != len(_PRIMITIVE_NAMES):
            raise ValueError("OpenSandbox feasibility primitives are invalid")
        if not isinstance(raw_blockers, Sequence) or isinstance(raw_blockers, (str, bytes, bytearray)) or len(raw_blockers) > len(_PRIMITIVE_NAMES):
            raise ValueError("OpenSandbox feasibility blocking codes are invalid")
        return cls(
            schema=value["schema"],  # type: ignore[arg-type]
            version=value["version"],  # type: ignore[arg-type]
            probe_kind=value["probe_kind"],  # type: ignore[arg-type]
            sdk_version=value["sdk_version"],  # type: ignore[arg-type]
            sdk_source_revision=value["sdk_source_revision"],  # type: ignore[arg-type]
            sdk_wheel_sha256=value["sdk_wheel_sha256"],  # type: ignore[arg-type]
            sdk_file_manifest_sha256=value["sdk_file_manifest_sha256"],  # type: ignore[arg-type]
            sdk_file_count=value["sdk_file_count"],  # type: ignore[arg-type]
            sdk_total_bytes=value["sdk_total_bytes"],  # type: ignore[arg-type]
            server_version=value["server_version"],  # type: ignore[arg-type]
            server_source_revision=value["server_source_revision"],  # type: ignore[arg-type]
            server_lifecycle_spec_sha256=value["server_lifecycle_spec_sha256"],  # type: ignore[arg-type]
            server_execd_spec_sha256=value["server_execd_spec_sha256"],  # type: ignore[arg-type]
            observed_at=_parse_timestamp(value["observed_at"]),
            primitives=tuple(FeasibilityPrimitiveV1.from_persisted(item) for item in raw_primitives),
            decision=value["decision"],  # type: ignore[arg-type]
            blocking_codes=tuple(raw_blockers),  # type: ignore[arg-type]
            digest=value["digest"],  # type: ignore[arg-type]
        )


def _verify_installed_sdk() -> None:
    try:
        distribution = importlib.metadata.distribution("opensandbox")
    except importlib.metadata.PackageNotFoundError as exc:
        raise RuntimeError("opensandbox_probe_sdk_unavailable") from exc
    if distribution.version != EXPECTED_OPEN_SANDBOX_SDK_VERSION:
        raise RuntimeError("opensandbox_probe_sdk_version_mismatch")
    files = distribution.files
    if files is None or len(files) > _MAX_SDK_FILES:
        raise RuntimeError("opensandbox_probe_sdk_manifest_invalid")
    rows: list[dict[str, object]] = []
    total_bytes = 0
    for entry in files:
        relative = str(entry).replace("\\", "/")
        if not relative.startswith("opensandbox/"):
            continue
        if entry.hash is None or entry.hash.mode != "sha256":
            raise RuntimeError("opensandbox_probe_sdk_manifest_invalid")
        path = Path(entry.locate())
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise RuntimeError("opensandbox_probe_sdk_manifest_invalid") from exc
        if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode) or metadata.st_size > _MAX_SDK_FILE_BYTES:
            raise RuntimeError("opensandbox_probe_sdk_manifest_invalid")
        try:
            content = path.read_bytes()
        except OSError as exc:
            raise RuntimeError("opensandbox_probe_sdk_manifest_invalid") from exc
        if len(content) != metadata.st_size:
            raise RuntimeError("opensandbox_probe_sdk_manifest_invalid")
        digest_bytes = hashlib.sha256(content).digest()
        record_digest = base64.urlsafe_b64encode(digest_bytes).decode("ascii").rstrip("=")
        if record_digest != entry.hash.value:
            raise RuntimeError("opensandbox_probe_sdk_manifest_digest_mismatch")
        rows.append(
            {
                "path": relative,
                "sha256": digest_bytes.hex(),
                "size": len(content),
            },
        )
        total_bytes += len(content)
        if len(rows) > _MAX_SDK_FILES or total_bytes > _MAX_SDK_TOTAL_BYTES:
            raise RuntimeError("opensandbox_probe_sdk_manifest_invalid")
    rows.sort(key=lambda row: str(row["path"]))
    if len(rows) != EXPECTED_OPEN_SANDBOX_SDK_FILE_COUNT or total_bytes != EXPECTED_OPEN_SANDBOX_SDK_TOTAL_BYTES or _digest(rows) != EXPECTED_OPEN_SANDBOX_SDK_FILE_MANIFEST_SHA256:
        raise RuntimeError("opensandbox_probe_sdk_distribution_mismatch")


def _load_exact_spec(
    path: Path,
    *,
    expected_digest: str,
    name: str,
) -> Mapping[str, object]:
    try:
        size = path.stat().st_size
        if size < 1 or size > _MAX_SPEC_BYTES:
            raise RuntimeError(f"opensandbox_probe_{name}_spec_invalid")
        content = path.read_bytes()
    except OSError as exc:
        raise RuntimeError(f"opensandbox_probe_{name}_spec_unavailable") from exc
    if len(content) != size or hashlib.sha256(content).hexdigest() != expected_digest:
        raise RuntimeError(f"opensandbox_probe_{name}_spec_digest_mismatch")
    try:
        import yaml

        parsed = yaml.safe_load(content)
    except (ImportError, UnicodeError, ValueError) as exc:
        raise RuntimeError(f"opensandbox_probe_{name}_spec_invalid") from exc
    if not isinstance(parsed, Mapping):
        raise RuntimeError(f"opensandbox_probe_{name}_spec_invalid")
    return parsed


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _parameters(*operations: object) -> set[str]:
    names: set[str] = set()
    for operation in operations:
        raw = _mapping(operation).get("parameters", ())
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes, bytearray)):
            continue
        for parameter in raw:
            name = _mapping(parameter).get("name")
            if isinstance(name, str):
                names.add(name.casefold().replace("-", "").replace("_", ""))
    return names


def _probe_primitives(
    lifecycle: Mapping[str, object],
    execd: Mapping[str, object],
) -> tuple[FeasibilityPrimitiveV1, ...]:
    from opensandbox import SandboxSync
    from opensandbox.models.sandboxes import SandboxFilter, SandboxInfo
    from opensandbox.sync.services.sandbox import SandboxesSync

    lifecycle_paths = _mapping(lifecycle.get("paths"))
    lifecycle_schemas = _mapping(
        _mapping(lifecycle.get("components")).get("schemas"),
    )
    execd_schemas = _mapping(_mapping(execd.get("components")).get("schemas"))

    sandboxes_path = _mapping(lifecycle_paths.get("/sandboxes"))
    sandbox_path = _mapping(lifecycle_paths.get("/sandboxes/{sandboxId}"))
    metadata_path = _mapping(
        lifecycle_paths.get("/sandboxes/{sandboxId}/metadata"),
    )
    renew_path = _mapping(
        lifecycle_paths.get("/sandboxes/{sandboxId}/renew-expiration"),
    )
    sandbox_properties = _mapping(
        _mapping(lifecycle_schemas.get("Sandbox")).get("properties"),
    )
    image_properties = _mapping(
        _mapping(lifecycle_schemas.get("ImageSpec")).get("properties"),
    )
    resolved_names = {
        "digest",
        "resolveddigest",
        "resolvedimagedigest",
        "imageid",
        "imagedigest",
    }
    reported_image_fields = {name.casefold().replace("_", "") for name in sandbox_properties} | {name.casefold().replace("_", "") for name in image_properties}
    sdk_info_fields = {name.casefold().replace("_", "") for name in getattr(SandboxInfo, "model_fields", {})}
    has_resolved_digest = bool(
        resolved_names & reported_image_fields and resolved_names & sdk_info_fields,
    )

    has_metadata_discovery = (
        "metadata" in _parameters(sandboxes_path.get("get"))
        and "get" in sandbox_path
        and "patch" in metadata_path
        and "metadata" in inspect.signature(SandboxFilter).parameters
        and hasattr(SandboxesSync, "list_sandboxes")
        and hasattr(SandboxSync, "patch_metadata")
    )

    patch_parameters = _parameters(
        metadata_path,
        metadata_path.get("patch"),
    )
    patch_request = _mapping(
        _mapping(metadata_path.get("patch")).get("requestBody"),
    )
    patch_request_text = json.dumps(
        patch_request,
        sort_keys=True,
        separators=(",", ":"),
    ).casefold()
    has_claim_cas = bool(
        {
            "expectedepoch",
            "resourceversion",
            "ifmatch",
            "etag",
        }
        & patch_parameters
    ) or any(
        marker in patch_request_text
        for marker in (
            '"expectedepoch"',
            '"resourceversion"',
            '"ifmatch"',
            '"etag"',
        )
    )

    has_renewal_surface = "post" in renew_path and hasattr(SandboxSync, "renew") and "metadata" in sandbox_properties
    volume_properties = _mapping(
        _mapping(lifecycle_schemas.get("Volume")).get("properties"),
    )
    command_properties = _mapping(
        _mapping(execd_schemas.get("RunCommandRequest")).get("properties"),
    )
    create_properties = _mapping(
        _mapping(lifecycle_schemas.get("CreateSandboxRequest")).get("properties"),
    )
    has_setup_candidate = "readOnly" in volume_properties and "volumes" in create_properties and {"uid", "gid"} <= set(command_properties)
    has_destroy_surface = "delete" in sandbox_path and "get" in sandboxes_path and hasattr(SandboxesSync, "kill_sandbox") and hasattr(SandboxesSync, "list_sandboxes")

    return (
        FeasibilityPrimitiveV1(
            version=1,
            name="image_digest_readback",
            status=(FeasibilityStatus.SURFACE_PRESENT if has_resolved_digest else FeasibilityStatus.UNSUPPORTED),
            code=("opensandbox_image_digest_readback_present" if has_resolved_digest else "opensandbox_image_digest_readback_unsupported"),
        ),
        FeasibilityPrimitiveV1(
            version=1,
            name="metadata_rediscovery",
            status=(FeasibilityStatus.SURFACE_PRESENT if has_metadata_discovery else FeasibilityStatus.UNSUPPORTED),
            code=("opensandbox_metadata_discovery_surface_present" if has_metadata_discovery else "opensandbox_discovery_unsupported"),
        ),
        FeasibilityPrimitiveV1(
            version=1,
            name="ownership_compare_and_set",
            status=(FeasibilityStatus.SURFACE_PRESENT if has_claim_cas else FeasibilityStatus.UNSUPPORTED),
            code=("opensandbox_accepted_claim_cas_surface_present" if has_claim_cas else "opensandbox_accepted_claim_cas_unsupported"),
        ),
        FeasibilityPrimitiveV1(
            version=1,
            name="metadata_preserving_renewal",
            status=(FeasibilityStatus.SURFACE_PRESENT if has_renewal_surface else FeasibilityStatus.UNSUPPORTED),
            code=("opensandbox_renewal_surface_present" if has_renewal_surface else "opensandbox_renewal_unsupported"),
        ),
        FeasibilityPrimitiveV1(
            version=1,
            name="trusted_setup_separation",
            status=(FeasibilityStatus.NOT_RUN if has_setup_candidate else FeasibilityStatus.UNSUPPORTED),
            code=("opensandbox_trusted_setup_live_probe_not_run" if has_setup_candidate else "opensandbox_trusted_setup_unsupported"),
        ),
        FeasibilityPrimitiveV1(
            version=1,
            name="uid1000_read_only_negative_probes",
            status=FeasibilityStatus.NOT_RUN,
            code="opensandbox_read_only_live_probe_not_run",
        ),
        FeasibilityPrimitiveV1(
            version=1,
            name="destroy_and_orphan_reconcile",
            status=(FeasibilityStatus.SURFACE_PRESENT if has_destroy_surface else FeasibilityStatus.UNSUPPORTED),
            code=("opensandbox_destroy_reconcile_surface_present" if has_destroy_surface else "opensandbox_destroy_reconcile_unsupported"),
        ),
    )


def probe_sdk_surface(
    *,
    observed_at: datetime | None = None,
    lifecycle_spec: Path | None = None,
    execd_spec: Path | None = None,
) -> OpenSandboxFeasibilityArtifactV1:
    """Verify and inspect the exact SDK wheel and server specification bytes."""

    _verify_installed_sdk()
    lifecycle = _load_exact_spec(
        lifecycle_spec or _PINNED_SPEC_DIR / "sandbox-lifecycle.yml",
        expected_digest=EXPECTED_OPEN_SANDBOX_LIFECYCLE_SPEC_SHA256,
        name="lifecycle",
    )
    execd = _load_exact_spec(
        execd_spec or _PINNED_SPEC_DIR / "execd-api.yaml",
        expected_digest=EXPECTED_OPEN_SANDBOX_EXECD_SPEC_SHA256,
        name="execd",
    )
    return OpenSandboxFeasibilityArtifactV1.build(
        observed_at=observed_at or datetime.now(UTC),
        primitives=_probe_primitives(lifecycle, execd),
    )


def _read_artifact(path: Path) -> OpenSandboxFeasibilityArtifactV1:
    try:
        size = path.stat().st_size
        if size < 1 or size > _MAX_ARTIFACT_BYTES:
            raise ValueError("OpenSandbox feasibility artifact is too large")
        raw_bytes = path.read_bytes()
        if len(raw_bytes) != size:
            raise ValueError("OpenSandbox feasibility artifact changed while reading")
        raw = json.loads(raw_bytes)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("OpenSandbox feasibility artifact is unreadable") from exc
    return OpenSandboxFeasibilityArtifactV1.from_persisted(raw)


def main(argv: Sequence[str] | None = None) -> int:
    """Probe exact installed contracts or strictly verify a saved artifact."""

    parser = argparse.ArgumentParser(
        description="Probe or verify pinned OpenSandbox feasibility evidence.",
    )
    parser.add_argument("--verify", type=Path)
    args = parser.parse_args(argv)
    try:
        artifact = _read_artifact(args.verify) if args.verify is not None else probe_sdk_surface()
    except (RuntimeError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(artifact.canonical_bytes().decode("utf-8"))
    return 1 if artifact.decision == "no_go" else 2


if __name__ == "__main__":  # pragma: no cover - exercised as a support command
    raise SystemExit(main())
