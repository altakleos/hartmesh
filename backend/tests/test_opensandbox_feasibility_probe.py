from __future__ import annotations

import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import support.opensandbox_feasibility as feasibility
from support.opensandbox_feasibility import (
    EXPECTED_OPEN_SANDBOX_EXECD_SPEC_SHA256,
    EXPECTED_OPEN_SANDBOX_LIFECYCLE_SPEC_SHA256,
    EXPECTED_OPEN_SANDBOX_SDK_FILE_MANIFEST_SHA256,
    EXPECTED_OPEN_SANDBOX_SDK_REVISION,
    EXPECTED_OPEN_SANDBOX_SDK_VERSION,
    EXPECTED_OPEN_SANDBOX_SERVER_REVISION,
    EXPECTED_OPEN_SANDBOX_SERVER_VERSION,
    FeasibilityStatus,
    OpenSandboxFeasibilityArtifactV1,
    probe_sdk_surface,
)


def test_pinned_sdk_surface_produces_a_strict_no_go_artifact() -> None:
    artifact = probe_sdk_surface(
        observed_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )

    assert artifact.sdk_version == EXPECTED_OPEN_SANDBOX_SDK_VERSION == "0.1.15"
    assert artifact.server_version == EXPECTED_OPEN_SANDBOX_SERVER_VERSION == "0.1.14"
    assert artifact.sdk_source_revision == EXPECTED_OPEN_SANDBOX_SDK_REVISION
    assert artifact.server_source_revision == EXPECTED_OPEN_SANDBOX_SERVER_REVISION
    assert artifact.sdk_file_manifest_sha256 == EXPECTED_OPEN_SANDBOX_SDK_FILE_MANIFEST_SHA256
    assert artifact.server_lifecycle_spec_sha256 == EXPECTED_OPEN_SANDBOX_LIFECYCLE_SPEC_SHA256
    assert artifact.server_execd_spec_sha256 == EXPECTED_OPEN_SANDBOX_EXECD_SPEC_SHA256
    assert artifact.decision == "no_go"
    by_name = {primitive.name: primitive for primitive in artifact.primitives}
    assert by_name["metadata_rediscovery"].status is FeasibilityStatus.SURFACE_PRESENT
    assert by_name["ownership_compare_and_set"].status is FeasibilityStatus.UNSUPPORTED
    assert by_name["trusted_setup_separation"].status is FeasibilityStatus.NOT_RUN
    assert artifact.blocking_codes == (
        "opensandbox_accepted_claim_cas_unsupported",
        "opensandbox_image_digest_readback_unsupported",
    )

    persisted = artifact.to_persisted()
    assert len(artifact.canonical_bytes()) < 8192
    assert "api_key" not in artifact.canonical_bytes().decode("utf-8").lower()
    assert OpenSandboxFeasibilityArtifactV1.from_persisted(persisted) == artifact


def test_feasibility_parser_rejects_tampering() -> None:
    artifact = probe_sdk_surface(
        observed_at=datetime(2026, 8, 31, 12, tzinfo=UTC),
    )
    tampered = copy.deepcopy(artifact.to_persisted())
    tampered["decision"] = "go"

    with pytest.raises(ValueError, match="invalid"):
        OpenSandboxFeasibilityArtifactV1.from_persisted(tampered)


def test_probe_rejects_server_spec_bytes_outside_the_exact_pinned_revision(
    tmp_path: Path,
) -> None:
    source = Path(__file__).parent / "fixtures" / "opensandbox_server_0_1_14" / "sandbox-lifecycle.yml"
    tampered = tmp_path / "sandbox-lifecycle.yml"
    tampered.write_bytes(source.read_bytes() + b"\n")

    with pytest.raises(
        RuntimeError,
        match="opensandbox_probe_lifecycle_spec_digest_mismatch",
    ):
        probe_sdk_surface(lifecycle_spec=tampered)


def test_probe_rejects_installed_sdk_bytes_outside_the_pinned_wheel_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        feasibility,
        "EXPECTED_OPEN_SANDBOX_SDK_FILE_MANIFEST_SHA256",
        "0" * 64,
    )

    with pytest.raises(
        RuntimeError,
        match="opensandbox_probe_sdk_distribution_mismatch",
    ):
        feasibility.probe_sdk_surface()


def test_committed_phase_zero_evidence_is_canonical_and_still_no_go() -> None:
    fixture = Path(__file__).parent / "fixtures" / "opensandbox_feasibility_0_1_15.json"
    raw_bytes = fixture.read_bytes()
    artifact = OpenSandboxFeasibilityArtifactV1.from_persisted(
        json.loads(raw_bytes),
    )

    assert artifact.decision == "no_go"
    assert raw_bytes == artifact.canonical_bytes() + b"\n"
