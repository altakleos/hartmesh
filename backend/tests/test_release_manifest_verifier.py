from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

from deerflow.extensions.artifacts import (
    ExtensionSourceLockV1,
    build_installed_artifact_manifest,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO_ROOT / "scripts" / "verify_release_manifest.py"
_SPEC = importlib.util.spec_from_file_location("verify_release_manifest", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_VERIFIER = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_VERIFIER)


def _canonical_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _documents(tmp_path: Path) -> tuple[Path, Path, dict[str, object]]:
    source_lock = ExtensionSourceLockV1.create(
        extension_api_version="0.13.0",
        entries=(),
    )
    artifact = build_installed_artifact_manifest(
        source_lock,
        platform_tag="py3-none-any",
    )
    artifact_path = tmp_path / "extension-artifacts.json"
    artifact_path.write_text(artifact.to_json(), encoding="utf-8")
    gateway_digest = "sha256:" + ("a" * 64)
    image = {
        "repository": "ghcr.io/acme/hartmesh-backend",
        "tag": "2.1.0_hartmesh.1",
        "digest": gateway_digest,
        "revision_check": "verified",
    }
    proxy_digest = "sha256:" + ("9" * 64)
    release: dict[str, object] = {
        "schema": 3,
        "version": "2.1.0+hartmesh.1",
        "tag": "v2.1.0+hartmesh.1",
        "commit": "b" * 40,
        "images": {
            "backend": {
                **image,
                "extension_artifact_manifest_digest": artifact.digest,
                "extension_api_version": artifact.extension_api_version,
                "extension_entry_count": len(artifact.entries),
                "provenance_reference": (f"oci://{image['repository']}@{gateway_digest}"),
            },
            "frontend": {**image, "repository": "ghcr.io/acme/hartmesh-frontend"},
            "provisioner": {
                **image,
                "repository": "ghcr.io/acme/hartmesh-provisioner",
            },
            "sandbox": {**image, "repository": "ghcr.io/acme/hartmesh-sandbox"},
            "sandbox_network_proxy": {**image, "repository": "ghcr.io/acme/hartmesh-sandbox-network-proxy", "digest": proxy_digest},
        },
        "compose_profile": {
            "images_txt_sha256": "e" * 64,
            "images": [
                f"ghcr.io/acme/hartmesh-backend@{gateway_digest}",
                f"ghcr.io/acme/hartmesh-frontend@{gateway_digest}",
                f"ghcr.io/acme/hartmesh-sandbox@{gateway_digest}",
                f"ghcr.io/acme/hartmesh-sandbox-network-proxy@{proxy_digest}",
                "postgres@sha256:" + ("1" * 64),
                "redis@sha256:" + ("2" * 64),
                "nginx@sha256:" + ("3" * 64),
            ],
        },
        "chart": {
            "repository": "oci://ghcr.io/acme/charts/deer-flow",
            "version": "2.1.0+hartmesh.1",
            "oci_tag": "2.1.0_hartmesh.1",
            "manifest_digest": "sha256:" + ("c" * 64),
            "package_sha256": "d" * 64,
        },
    }
    release_path = tmp_path / "release-manifest.json"
    release_path.write_text(json.dumps(release), encoding="utf-8")
    return release_path, artifact_path, release


def test_offline_verifier_binds_gateway_image_and_embedded_artifact(
    tmp_path: Path,
) -> None:
    release_path, artifact_path, release = _documents(tmp_path)
    gateway_digest = release["images"]["backend"]["digest"]

    verified = _VERIFIER.verify_release_manifest(
        release_path,
        artifact_manifest_path=artifact_path,
        gateway_image_digest=gateway_digest,
    )

    assert verified["schema"] == 3
    assert verified["images"]["sandbox_network_proxy"]["digest"] == "sha256:" + ("9" * 64)


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (
            lambda release: release.update(schema=2),
            "release_manifest_schema_unsupported",
        ),
        (
            lambda release: release["images"].pop("sandbox_network_proxy"),
            "release_manifest_invalid",
        ),
        (
            lambda release: release.pop("compose_profile"),
            "release_manifest_invalid",
        ),
        (
            lambda release: release["compose_profile"]["images"].append("postgres:16@sha256:" + ("4" * 64)),
            "release_manifest_invalid",
        ),
        (
            lambda release: release["compose_profile"]["images"].remove(f"ghcr.io/acme/hartmesh-sandbox-network-proxy@{'sha256:' + ('9' * 64)}"),
            "release_manifest_compose_profile_mismatch",
        ),
        (
            lambda release: release["compose_profile"]["images"].__setitem__(0, "ghcr.io/acme/hartmesh-backend@sha256:" + ("5" * 64)),
            "release_manifest_compose_profile_mismatch",
        ),
        (
            lambda release: release["images"]["backend"].update(extension_artifact_manifest_digest="sha256:" + ("e" * 64)),
            "extension_artifact_digest_mismatch",
        ),
        (
            lambda release: release["images"]["backend"].pop("extension_api_version"),
            "release_manifest_invalid",
        ),
    ],
)
def test_offline_verifier_rejects_stale_or_incomplete_release_identity(
    tmp_path: Path,
    mutation,
    code: str,
) -> None:
    release_path, artifact_path, release = _documents(tmp_path)
    mutation(release)
    release_path.write_text(json.dumps(release), encoding="utf-8")

    with pytest.raises(_VERIFIER.ReleaseManifestError, match=code):
        _VERIFIER.verify_release_manifest(
            release_path,
            artifact_manifest_path=artifact_path,
        )


def test_offline_verifier_rejects_invalid_entry_digest_even_when_outer_digests_match(
    tmp_path: Path,
) -> None:
    release_path, artifact_path, release = _documents(tmp_path)
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    artifact["entries"] = [
        {
            "source_entry_digest": "sha256:" + ("e" * 64),
            "distribution": "acme-extension",
            "distribution_version": "1.0.0",
            "entry_point_name": "acme",
            "entry_point_value": "acme_extension:install",
            "selected_artifact_hash": None,
            "installed_record_digest": "sha256:" + ("f" * 64),
            "entry_digest": "sha256:" + ("0" * 64),
        }
    ]
    artifact_without_digest = dict(artifact)
    artifact_without_digest.pop("digest")
    artifact["digest"] = _canonical_digest(artifact_without_digest)
    artifact_path.write_text(json.dumps(artifact), encoding="utf-8")
    backend = release["images"]["backend"]
    backend["extension_artifact_manifest_digest"] = artifact["digest"]
    backend["extension_entry_count"] = 1
    release_path.write_text(json.dumps(release), encoding="utf-8")

    with pytest.raises(
        _VERIFIER.ReleaseManifestError,
        match="extension_artifact_digest_mismatch",
    ):
        _VERIFIER.verify_release_manifest(
            release_path,
            artifact_manifest_path=artifact_path,
        )


def test_offline_verifier_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    release_path = tmp_path / "release-manifest.json"
    release_path.write_text('{"schema":3,"schema":3}', encoding="utf-8")

    with pytest.raises(_VERIFIER.ReleaseManifestError, match="release_manifest_invalid"):
        _VERIFIER.verify_release_manifest(release_path)
