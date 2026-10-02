#!/usr/bin/env python3
"""Offline structural and digest verification for release-manifest.json.

The manifest names what one release published: the five container images by
digest, and the compose profile's `images.txt`, whose own image lines must be
those digests. It is what a golden VM image build pins against.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_MAX_DOCUMENT_BYTES = 4 * 1024 * 1024
_COMPOSE_REFERENCE = re.compile(r"[a-z0-9./_-]+@sha256:[0-9a-f]{64}\Z")
_IMAGE_NAMES = (
    "backend",
    "frontend",
    "provisioner",
    "sandbox",
    "sandbox_network_proxy",
)
# Images the tenant VM compose profile runs; the provisioner is cluster-only.
_COMPOSE_PROFILE_IMAGE_NAMES = (
    "backend",
    "frontend",
    "sandbox",
    "sandbox_network_proxy",
)
_MAX_COMPOSE_PROFILE_IMAGES = 32
_IMAGE_FIELDS = {"repository", "tag", "digest", "revision_check"}
SCHEMA = 4


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


def verify_release_manifest(path: Path, *, gateway_image_digest: str | None = None) -> Mapping[str, Any]:
    manifest = _object(
        _read_json(path, "release_manifest_invalid"),
        {"schema", "version", "tag", "commit", "images", "compose_profile"},
        "release_manifest_invalid",
    )
    if manifest["schema"] != SCHEMA or type(manifest["schema"]) is not int:
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
        image = _object(images[name], _IMAGE_FIELDS, "release_manifest_invalid")
        repository = _text(image["repository"], "release_manifest_invalid")
        if any(token in repository for token in ("@", "://", "\n", "\r")):
            raise ReleaseManifestError("release_manifest_invalid")
        _text(image["tag"], "release_manifest_invalid", max_bytes=128)
        _digest(image["digest"], "release_manifest_invalid")
        if image["revision_check"] not in {"verified", "tag-not-found"}:
            raise ReleaseManifestError("release_manifest_invalid")
    backend = images["backend"]
    if (
        gateway_image_digest is not None
        and _digest(
            gateway_image_digest,
            "gateway_image_digest_invalid",
        )
        != backend["digest"]
    ):
        raise ReleaseManifestError("gateway_image_digest_mismatch")
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
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verify a HartMesh release manifest without network access.")
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--gateway-image-digest")
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        verify_release_manifest(args.manifest, gateway_image_digest=args.gateway_image_digest)
    except ReleaseManifestError as exc:
        print(f"release manifest verification failed: {exc.code}", file=sys.stderr)
        return 1
    print("release manifest verification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
