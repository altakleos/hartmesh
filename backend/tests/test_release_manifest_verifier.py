"""The offline check of ``release-manifest.json``: what one release published.

The manifest is what a golden VM image build pins against, so the verifier
refuses anything that is not exactly the five images by digest plus the
compose profile's own pins, and refuses a profile whose pins are not those
images.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCRIPT = _REPO_ROOT / "scripts" / "verify_release_manifest.py"
_SPEC = importlib.util.spec_from_file_location("verify_release_manifest", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
_VERIFIER = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_VERIFIER)

_GATEWAY_DIGEST = "sha256:" + ("a" * 64)
_PROXY_DIGEST = "sha256:" + ("9" * 64)


def _release() -> dict[str, object]:
    image = {
        "repository": "ghcr.io/acme/hartmesh-backend",
        "tag": "v2.1.0-hartmesh.1",
        "digest": _GATEWAY_DIGEST,
        "revision_check": "verified",
    }
    return {
        "schema": 4,
        "version": "2.1.0+hartmesh.1",
        "tag": "v2.1.0+hartmesh.1",
        "commit": "b" * 40,
        "images": {
            "backend": dict(image),
            "frontend": {**image, "repository": "ghcr.io/acme/hartmesh-frontend"},
            "provisioner": {**image, "repository": "ghcr.io/acme/hartmesh-provisioner"},
            "sandbox": {**image, "repository": "ghcr.io/acme/hartmesh-sandbox"},
            "sandbox_network_proxy": {**image, "repository": "ghcr.io/acme/hartmesh-sandbox-network-proxy", "digest": _PROXY_DIGEST},
        },
        "compose_profile": {
            "images_txt_sha256": "e" * 64,
            "images": [
                f"ghcr.io/acme/hartmesh-backend@{_GATEWAY_DIGEST}",
                f"ghcr.io/acme/hartmesh-frontend@{_GATEWAY_DIGEST}",
                f"ghcr.io/acme/hartmesh-sandbox@{_GATEWAY_DIGEST}",
                f"ghcr.io/acme/hartmesh-sandbox-network-proxy@{_PROXY_DIGEST}",
                "postgres@sha256:" + ("1" * 64),
                "redis@sha256:" + ("2" * 64),
                "nginx@sha256:" + ("3" * 64),
            ],
        },
    }


def _written(tmp_path: Path, release: object) -> Path:
    path = tmp_path / "release-manifest.json"
    path.write_text(json.dumps(release), encoding="utf-8")
    return path


def test_a_complete_manifest_is_accepted_and_bound_to_the_gateway_image(tmp_path: Path) -> None:
    verified = _VERIFIER.verify_release_manifest(_written(tmp_path, _release()), gateway_image_digest=_GATEWAY_DIGEST)

    assert verified["schema"] == 4
    assert verified["images"]["sandbox_network_proxy"]["digest"] == _PROXY_DIGEST


def test_a_manifest_for_another_gateway_image_is_refused(tmp_path: Path) -> None:
    with pytest.raises(_VERIFIER.ReleaseManifestError, match="gateway_image_digest_mismatch"):
        _VERIFIER.verify_release_manifest(_written(tmp_path, _release()), gateway_image_digest="sha256:" + ("5" * 64))


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (lambda release: release.update(schema=3), "release_manifest_schema_unsupported"),
        (lambda release: release.update(tag="v2.1.0+hartmesh.2"), "release_manifest_invalid"),
        (lambda release: release.update(commit="b" * 7), "release_manifest_invalid"),
        (lambda release: release["images"].pop("sandbox_network_proxy"), "release_manifest_invalid"),
        (lambda release: release["images"]["frontend"].update(digest="latest"), "release_manifest_invalid"),
        (lambda release: release["images"]["frontend"].update(revision_check="assumed"), "release_manifest_invalid"),
        (lambda release: release["images"]["backend"].update(provenance_reference="oci://elsewhere"), "release_manifest_invalid"),
        (lambda release: release.update(chart={"version": "2.1.0+hartmesh.1"}), "release_manifest_invalid"),
        (lambda release: release.pop("compose_profile"), "release_manifest_invalid"),
        (lambda release: release["compose_profile"]["images"].append("postgres:16@sha256:" + ("4" * 64)), "release_manifest_invalid"),
        (lambda release: release["compose_profile"]["images"].append("postgres@sha256:" + ("1" * 64)), "release_manifest_invalid"),
        (lambda release: release["compose_profile"]["images"].remove(f"ghcr.io/acme/hartmesh-sandbox-network-proxy@{_PROXY_DIGEST}"), "release_manifest_compose_profile_mismatch"),
        (lambda release: release["compose_profile"]["images"].__setitem__(0, "ghcr.io/acme/hartmesh-backend@sha256:" + ("5" * 64)), "release_manifest_compose_profile_mismatch"),
    ],
    ids=[
        "older-schema",
        "tag-for-another-version",
        "short-commit",
        "image-missing",
        "digest-not-a-digest",
        "unknown-revision-check",
        "unknown-image-field",
        "unknown-top-level-field",
        "no-compose-profile",
        "pin-with-a-tag",
        "pin-repeated",
        "profile-lacks-a-published-image",
        "profile-pins-another-build",
    ],
)
def test_a_stale_or_incomplete_release_identity_is_refused(tmp_path: Path, mutation, code: str) -> None:
    release = _release()
    mutation(release)

    with pytest.raises(_VERIFIER.ReleaseManifestError, match=code):
        _VERIFIER.verify_release_manifest(_written(tmp_path, release))


def test_duplicate_json_keys_are_refused(tmp_path: Path) -> None:
    path = tmp_path / "release-manifest.json"
    path.write_text('{"schema":4,"schema":4}', encoding="utf-8")

    with pytest.raises(_VERIFIER.ReleaseManifestError, match="release_manifest_invalid"):
        _VERIFIER.verify_release_manifest(path)


def test_the_command_reports_a_refusal_by_its_code_and_exit_status(tmp_path: Path) -> None:
    accepted = subprocess.run([sys.executable, str(_SCRIPT), str(_written(tmp_path, _release())), "--gateway-image-digest", _GATEWAY_DIGEST], capture_output=True, text=True)
    assert (accepted.returncode, accepted.stdout.strip()) == (0, "release manifest verification passed")

    stale = _release()
    stale["schema"] = 3
    refused = subprocess.run([sys.executable, str(_SCRIPT), str(_written(tmp_path, stale))], capture_output=True, text=True)
    assert refused.returncode == 1
    assert "release_manifest_schema_unsupported" in refused.stderr


def test_the_release_workflow_writes_the_schema_the_verifier_reads() -> None:
    workflow = (_REPO_ROOT / ".github" / "workflows" / "release-manifest.yaml").read_text(encoding="utf-8")

    assert f'"schema": {_VERIFIER.SCHEMA},' in workflow
    assert "scripts/verify_release_manifest.py release-manifest.json" in workflow
    # This distribution releases the compose profile; nothing here publishes or records a chart.
    assert "chart" not in workflow.lower()
