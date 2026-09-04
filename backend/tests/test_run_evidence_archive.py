from __future__ import annotations

import hashlib
import zipfile
from datetime import UTC, datetime

import pytest
from deerflow_extension_api import TenantReferenceV1

from app.gateway import artifact_archive
from app.gateway.artifact_archive import (
    ArtifactArchiveError,
    build_run_evidence_archive,
)
from deerflow.runtime.run_evidence import (
    EXPECTED_EVIDENCE_SECTIONS,
    RUN_EVIDENCE_MANIFEST_PATH,
    EvidenceSectionV1,
    EvidenceSnapshotSourceV1,
    RunEvidenceBundleManifestV1,
    RunEvidenceSnapshotService,
)


def _snapshot(*, paths: tuple[str, ...]):
    tenant_digest = "1" * 64
    accepted_digest = "2" * 64
    assembly_digest = "3" * 64
    terminal_digest = "4" * 64
    anchors = {
        "accepted_invocation": accepted_digest,
        "assembly": assembly_digest,
        "lifecycle": terminal_digest,
    }
    sections = tuple(
        EvidenceSectionV1.complete(name, (anchors[name],)) if name in anchors else (EvidenceSectionV1.unqualified(name) if name == "qualification" else EvidenceSectionV1.absent_by_design(name)) for name in EXPECTED_EVIDENCE_SECTIONS
    )
    return RunEvidenceSnapshotService.validate_source(
        EvidenceSnapshotSourceV1(
            tenant=TenantReferenceV1(
                version=1,
                public_ref=f"tenant-{tenant_digest[:16]}",
                digest=tenant_digest,
            ),
            thread_id="thread",
            run_id="run",
            terminal_status="success",
            safe_stop_reason="completed",
            accepted_at=datetime(2026, 9, 4, 12, tzinfo=UTC),
            completed_at=datetime(2026, 9, 4, 12, 1, tzinfo=UTC),
            accepted_invocation_digest=accepted_digest,
            accepted_invocation_version=4,
            accepted_context_digest="5" * 64,
            agent_revision_digest="6" * 64,
            assembly_evidence_digest=assembly_digest,
            assembly_fingerprint="7" * 64,
            lifecycle_high_water_mark=12,
            terminal_event_digest=terminal_digest,
            lifecycle_event_count=2,
            lifecycle_counts={"lifecycle": 1, "artifacts": 1},
            sections=sections,
            artifact_paths=paths,
        )
    )


def test_evidence_archive_manifest_commits_to_exact_copied_bytes(tmp_path) -> None:
    outputs = tmp_path / "outputs"
    (outputs / "z").mkdir(parents=True)
    (outputs / "z" / "report.txt").write_bytes(b"report bytes")
    (outputs / "a.json").write_bytes(b"{}")
    paths = (
        "/mnt/user-data/outputs/z/report.txt",
        "/mnt/user-data/outputs/a.json",
    )

    result = build_run_evidence_archive(
        outputs,
        paths,
        snapshot=_snapshot(paths=paths),
        user_data_dir=outputs.parent,
    )

    with result.file, zipfile.ZipFile(result.file) as archive:
        assert archive.namelist() == ["a.json", "z/report.txt", RUN_EVIDENCE_MANIFEST_PATH]
        manifest_bytes = archive.read(RUN_EVIDENCE_MANIFEST_PATH)
        manifest = RunEvidenceBundleManifestV1.from_bytes(manifest_bytes)
        assert [(entry.path, entry.size, entry.sha256) for entry in manifest.artifacts] == [
            ("a.json", 2, hashlib.sha256(b"{}").hexdigest()),
            ("z/report.txt", 12, hashlib.sha256(b"report bytes").hexdigest()),
        ]
        assert result.manifest_digest == manifest.manifest_digest
        assert result.bundle_ref == manifest.bundle_ref
        for info in archive.infolist():
            assert info.date_time == (1980, 1, 1, 0, 0, 0)
            assert info.compress_type == zipfile.ZIP_STORED


def test_evidence_archive_rejects_a_replaced_path_and_closes_partial_output(
    tmp_path,
    monkeypatch,
) -> None:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    artifact = outputs / "report.txt"
    artifact.write_bytes(b"accepted bytes")
    replacement = outputs / "replacement.txt"
    replacement.write_bytes(b"changed bytes")
    path = "/mnt/user-data/outputs/report.txt"
    original_read = artifact_archive.os.read
    original_temporary_file = artifact_archive.tempfile.TemporaryFile
    temporary_files = []
    replaced = False

    def replace_after_read(descriptor: int, count: int) -> bytes:
        nonlocal replaced
        data = original_read(descriptor, count)
        if not replaced:
            replaced = True
            replacement.replace(artifact)
        return data

    def record_temporary_file(*args, **kwargs):
        temporary = original_temporary_file(*args, **kwargs)
        temporary_files.append(temporary)
        return temporary

    monkeypatch.setattr(artifact_archive.os, "read", replace_after_read)
    monkeypatch.setattr(
        artifact_archive.tempfile,
        "TemporaryFile",
        record_temporary_file,
    )

    with pytest.raises(ArtifactArchiveError) as raised:
        build_run_evidence_archive(
            outputs,
            [path],
            snapshot=_snapshot(paths=(path,)),
            user_data_dir=outputs.parent,
        )

    assert raised.value.code == "artifact_changed"
    assert temporary_files and all(item.closed for item in temporary_files)


def test_evidence_archive_rejects_reserved_namespace_collision(tmp_path) -> None:
    outputs = tmp_path / "outputs"
    target = outputs / "hartmesh-evidence" / "user-created.txt"
    target.parent.mkdir(parents=True)
    target.write_text("collision", encoding="utf-8")
    path = "/mnt/user-data/outputs/hartmesh-evidence/user-created.txt"

    with pytest.raises(ArtifactArchiveError):
        build_run_evidence_archive(
            outputs,
            [path],
            snapshot=_snapshot(paths=(path,)),
            user_data_dir=outputs.parent,
        )


def test_evidence_archive_requires_snapshot_paths_to_match_archive_input(tmp_path) -> None:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    (outputs / "report.txt").write_text("report", encoding="utf-8")
    actual = "/mnt/user-data/outputs/report.txt"

    with pytest.raises(ArtifactArchiveError, match="snapshot"):
        build_run_evidence_archive(
            outputs,
            [actual],
            snapshot=_snapshot(paths=("/mnt/user-data/outputs/other.txt",)),
            user_data_dir=outputs.parent,
        )


def test_evidence_archive_normalizes_member_names_to_canonical_nfc(tmp_path) -> None:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    decomposed_name = "re\u0301sume\u0301.txt"
    normalized_name = "r\u00e9sum\u00e9.txt"
    (outputs / decomposed_name).write_text("report", encoding="utf-8")
    path = f"/mnt/user-data/outputs/{decomposed_name}"

    result = build_run_evidence_archive(
        outputs,
        [path],
        snapshot=_snapshot(paths=(path,)),
        user_data_dir=outputs.parent,
    )

    with result.file, zipfile.ZipFile(result.file) as archive:
        assert archive.namelist() == [
            normalized_name,
            RUN_EVIDENCE_MANIFEST_PATH,
        ]
        manifest = RunEvidenceBundleManifestV1.from_bytes(archive.read(RUN_EVIDENCE_MANIFEST_PATH))
        assert manifest.artifacts[0].path == normalized_name


def test_evidence_archive_bytes_are_deterministic_for_an_unchanged_snapshot(
    tmp_path,
) -> None:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    (outputs / "report.txt").write_bytes(b"stable")
    path = "/mnt/user-data/outputs/report.txt"
    snapshot = _snapshot(paths=(path,))

    bundles = []
    for _ in range(2):
        result = build_run_evidence_archive(
            outputs,
            [path],
            snapshot=snapshot,
            user_data_dir=outputs.parent,
        )
        with result.file:
            bundles.append(result.file.read())

    assert bundles[0] == bundles[1]
