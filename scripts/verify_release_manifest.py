#!/usr/bin/env python3
"""Offline structural and digest verification for release-manifest.json."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_DISTRIBUTION = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?\Z",
    re.ASCII,
)
_DISTRIBUTION_VERSION = re.compile(
    r"[A-Za-z0-9](?:[A-Za-z0-9._+!-]*[A-Za-z0-9])?\Z",
    re.ASCII,
)
_ENTRY_POINT_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", re.ASCII)
_MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
_ARTIFACT_FIELDS = {
    "version",
    "source_lock_digest",
    "extension_api_version",
    "platform_tag",
    "entries",
    "digest",
}
_ARTIFACT_ENTRY_FIELDS = {
    "source_entry_digest",
    "distribution",
    "distribution_version",
    "entry_point_name",
    "entry_point_value",
    "selected_artifact_hash",
    "installed_record_digest",
    "entry_digest",
}
_COMPOSE_REFERENCE = re.compile(r"[a-z0-9./_-]+@sha256:[0-9a-f]{64}\Z")
_IMAGE_NAMES = ("backend", "frontend", "provisioner", "sandbox", "sandbox_network_proxy")
# Images the tenant VM compose profile runs; the provisioner is cluster-only.
_COMPOSE_PROFILE_IMAGE_NAMES = ("backend", "frontend", "sandbox", "sandbox_network_proxy")
_MAX_COMPOSE_PROFILE_IMAGES = 32
_IMAGE_FIELDS = {"repository", "tag", "digest", "revision_check"}
_GATEWAY_IMAGE_FIELDS = _IMAGE_FIELDS | {
    "extension_artifact_manifest_digest",
    "extension_api_version",
    "extension_entry_count",
    "provenance_reference",
}


class ReleaseManifestError(ValueError):
    """One bounded release verification failure."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class _DuplicateJsonKeyError(ValueError):
    pass


def _strict_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateJsonKeyError("duplicate JSON object key")
        result[key] = value
    return result


def _canonical_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _object(value: object, fields: set[str], code: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ReleaseManifestError(code)
    return value


def _digest(value: object, code: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise ReleaseManifestError(code)
    return value


def _text(value: object, code: str, *, max_bytes: int = 256) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > max_bytes or any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise ReleaseManifestError(code)
    return value


def _read_json(path: Path, code: str) -> Mapping[str, Any]:
    try:
        if path.is_symlink() or not path.is_file():
            raise OSError("provenance document is not a regular file")
        if path.stat().st_size > _MAX_DOCUMENT_BYTES:
            raise OSError("provenance document exceeds the size limit")
        payload = json.loads(
            path.read_bytes().decode("utf-8"),
            object_pairs_hook=_strict_json_object,
        )
    except (
        OSError,
        UnicodeError,
        json.JSONDecodeError,
        _DuplicateJsonKeyError,
    ) as exc:
        raise ReleaseManifestError(code) from exc
    if not isinstance(payload, Mapping):
        raise ReleaseManifestError(code)
    return payload


def _normalize_distribution(value: object) -> str:
    distribution = _text(
        value,
        "extension_artifact_manifest_invalid",
        max_bytes=256,
    )
    if _DISTRIBUTION.fullmatch(distribution) is None:
        raise ReleaseManifestError("extension_artifact_manifest_invalid")
    return re.sub(r"[-_.]+", "-", distribution).lower()


def _entry_point(name: object, value: object) -> tuple[str, str]:
    entry_name = _text(
        name,
        "extension_artifact_manifest_invalid",
        max_bytes=128,
    )
    if _ENTRY_POINT_NAME.fullmatch(entry_name) is None:
        raise ReleaseManifestError("extension_artifact_manifest_invalid")
    target = _text(value, "extension_artifact_manifest_invalid")
    module, separator, attribute = target.rpartition(":")
    if separator != ":" or not attribute.isidentifier() or not module or any(not part.isidentifier() for part in module.split(".")):
        raise ReleaseManifestError("extension_artifact_manifest_invalid")
    return entry_name, target


def verify_artifact_manifest(path: Path) -> tuple[str, str, int]:
    artifact = _object(
        _read_json(path, "extension_artifact_manifest_invalid"),
        _ARTIFACT_FIELDS,
        "extension_artifact_manifest_invalid",
    )
    if artifact["version"] != 1 or type(artifact["version"]) is not int:
        raise ReleaseManifestError("extension_artifact_manifest_invalid")
    _digest(artifact["source_lock_digest"], "extension_artifact_manifest_invalid")
    extension_api_version = _text(
        artifact["extension_api_version"],
        "extension_artifact_manifest_invalid",
        max_bytes=64,
    )
    platform_tag = _text(
        artifact["platform_tag"],
        "extension_artifact_manifest_invalid",
    )
    if platform_tag != platform_tag.lower() or any(character not in "abcdefghijklmnopqrstuvwxyz0123456789_.-" for character in platform_tag):
        raise ReleaseManifestError("extension_artifact_manifest_invalid")
    entries = artifact["entries"]
    if not isinstance(entries, list) or len(entries) > 256:
        raise ReleaseManifestError("extension_artifact_manifest_invalid")
    identities: list[tuple[str, str]] = []
    for entry in entries:
        item = _object(
            entry,
            _ARTIFACT_ENTRY_FIELDS,
            "extension_artifact_manifest_invalid",
        )
        for name in (
            "source_entry_digest",
            "installed_record_digest",
            "entry_digest",
        ):
            _digest(item[name], "extension_artifact_manifest_invalid")
        selected = item["selected_artifact_hash"]
        if selected is not None:
            _digest(selected, "extension_artifact_manifest_invalid")
        normalized_distribution = _normalize_distribution(item["distribution"])
        distribution_version = _text(
            item["distribution_version"],
            "extension_artifact_manifest_invalid",
        )
        if _DISTRIBUTION_VERSION.fullmatch(distribution_version) is None:
            raise ReleaseManifestError("extension_artifact_manifest_invalid")
        entry_name, _ = _entry_point(
            item["entry_point_name"],
            item["entry_point_value"],
        )
        identities.append((normalized_distribution, entry_name))
        canonical_entry = dict(item)
        canonical_entry.pop("entry_digest")
        if _canonical_digest(canonical_entry) != item["entry_digest"]:
            raise ReleaseManifestError("extension_artifact_digest_mismatch")
    if identities != sorted(identities) or len(identities) != len(set(identities)):
        raise ReleaseManifestError("extension_artifact_manifest_invalid")
    stated_digest = _digest(
        artifact["digest"],
        "extension_artifact_manifest_invalid",
    )
    canonical = dict(artifact)
    canonical.pop("digest")
    actual_digest = _canonical_digest(canonical)
    if actual_digest != stated_digest:
        raise ReleaseManifestError("extension_artifact_digest_mismatch")
    return stated_digest, extension_api_version, len(entries)


def verify_release_manifest(
    path: Path,
    *,
    artifact_manifest_path: Path | None = None,
    gateway_image_digest: str | None = None,
) -> Mapping[str, Any]:
    manifest = _object(
        _read_json(path, "release_manifest_invalid"),
        {"schema", "version", "tag", "commit", "images", "compose_profile", "chart"},
        "release_manifest_invalid",
    )
    if manifest["schema"] != 3 or type(manifest["schema"]) is not int:
        raise ReleaseManifestError("release_manifest_schema_unsupported")
    version = _text(manifest["version"], "release_manifest_invalid", max_bytes=128)
    if manifest["tag"] != f"v{version}":
        raise ReleaseManifestError("release_manifest_invalid")
    if not isinstance(manifest["commit"], str) or _COMMIT.fullmatch(manifest["commit"]) is None:
        raise ReleaseManifestError("release_manifest_invalid")
    images = _object(
        manifest["images"],
        set(_IMAGE_NAMES),
        "release_manifest_invalid",
    )
    for name in _IMAGE_NAMES:
        fields = _GATEWAY_IMAGE_FIELDS if name == "backend" else _IMAGE_FIELDS
        image = _object(images[name], fields, "release_manifest_invalid")
        repository = _text(image["repository"], "release_manifest_invalid")
        if any(token in repository for token in ("@", "://", "\n", "\r")):
            raise ReleaseManifestError("release_manifest_invalid")
        _text(image["tag"], "release_manifest_invalid", max_bytes=128)
        _digest(image["digest"], "release_manifest_invalid")
        if image["revision_check"] not in {"verified", "tag-not-found"}:
            raise ReleaseManifestError("release_manifest_invalid")
    backend = images["backend"]
    artifact_digest = _digest(
        backend["extension_artifact_manifest_digest"],
        "extension_artifact_manifest_invalid",
    )
    extension_api_version = _text(
        backend["extension_api_version"],
        "release_manifest_invalid",
        max_bytes=64,
    )
    entry_count = backend["extension_entry_count"]
    if type(entry_count) is not int or not 0 <= entry_count <= 256:
        raise ReleaseManifestError("release_manifest_invalid")
    provenance_reference = _text(
        backend["provenance_reference"],
        "release_manifest_invalid",
        max_bytes=768,
    )
    expected_reference = f"oci://{backend['repository']}@{backend['digest']}"
    if provenance_reference != expected_reference:
        raise ReleaseManifestError("release_manifest_provenance_mismatch")
    if (
        gateway_image_digest is not None
        and _digest(
            gateway_image_digest,
            "gateway_image_digest_invalid",
        )
        != backend["digest"]
    ):
        raise ReleaseManifestError("gateway_image_digest_mismatch")
    if artifact_manifest_path is not None:
        actual_artifact_digest, actual_api_version, actual_entry_count = verify_artifact_manifest(artifact_manifest_path)
        if actual_artifact_digest != artifact_digest:
            raise ReleaseManifestError("extension_artifact_digest_mismatch")
        if actual_api_version != extension_api_version:
            raise ReleaseManifestError("extension_api_version_mismatch")
        if actual_entry_count != entry_count:
            raise ReleaseManifestError("extension_entry_count_mismatch")
    compose_profile = _object(
        manifest["compose_profile"],
        {"images_txt_sha256", "images"},
        "release_manifest_invalid",
    )
    if not isinstance(compose_profile["images_txt_sha256"], str) or re.fullmatch(r"[0-9a-f]{64}", compose_profile["images_txt_sha256"]) is None:
        raise ReleaseManifestError("release_manifest_invalid")
    compose_images = compose_profile["images"]
    if not isinstance(compose_images, list) or not compose_images or len(compose_images) > _MAX_COMPOSE_PROFILE_IMAGES or len(set(compose_images)) != len(compose_images):
        raise ReleaseManifestError("release_manifest_invalid")
    for reference in compose_images:
        if not isinstance(reference, str) or _COMPOSE_REFERENCE.fullmatch(reference) is None:
            raise ReleaseManifestError("release_manifest_invalid")
    for name in _COMPOSE_PROFILE_IMAGE_NAMES:
        pinned = f"{images[name]['repository']}@{images[name]['digest']}"
        if pinned not in compose_images:
            raise ReleaseManifestError("release_manifest_compose_profile_mismatch")
    chart = _object(
        manifest["chart"],
        {
            "repository",
            "version",
            "oci_tag",
            "manifest_digest",
            "package_sha256",
        },
        "release_manifest_invalid",
    )
    _text(chart["repository"], "release_manifest_invalid")
    if chart["version"] != version:
        raise ReleaseManifestError("release_manifest_invalid")
    _text(chart["oci_tag"], "release_manifest_invalid", max_bytes=128)
    _digest(chart["manifest_digest"], "release_manifest_invalid")
    if (
        not isinstance(chart["package_sha256"], str)
        or re.fullmatch(
            r"[0-9a-f]{64}",
            chart["package_sha256"],
        )
        is None
    ):
        raise ReleaseManifestError("release_manifest_invalid")
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify a HartMesh release manifest without network access.")
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--artifact-manifest", type=Path)
    parser.add_argument("--gateway-image-digest")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        verify_release_manifest(
            args.manifest,
            artifact_manifest_path=args.artifact_manifest,
            gateway_image_digest=args.gateway_image_digest,
        )
    except ReleaseManifestError as exc:
        print(f"release manifest verification failed: {exc.code}", file=sys.stderr)
        return 1
    print("release manifest verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
