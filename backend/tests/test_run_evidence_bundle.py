from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import pytest
from deerflow_extension_api import TenantReferenceV1

from deerflow.runtime.run_evidence import (
    EvidenceArtifactV1,
    EvidenceSectionV1,
    EvidenceSnapshotRequest,
    EvidenceSnapshotSourceV1,
    RunEvidenceBundleError,
    RunEvidenceBundleManifestV1,
    RunEvidenceSnapshotService,
    canonical_evidence_root,
    canonical_json_bytes,
)

_D1 = "1" * 64
_D2 = "2" * 64
_D3 = "3" * 64
_TENANT = TenantReferenceV1(
    version=1,
    public_ref=f"tenant-{_D1[:16]}",
    digest=_D1,
)


def _section(name: str, *references: str, required: bool = True) -> EvidenceSectionV1:
    return EvidenceSectionV1.complete(
        name=name,
        required=required,
        references=references,
    )


def _source(*, status: str = "success") -> EvidenceSnapshotSourceV1:
    return EvidenceSnapshotSourceV1(
        tenant=_TENANT,
        thread_id="thread-raw-id",
        run_id="run-raw-id",
        terminal_status=status,
        safe_stop_reason="completed",
        accepted_at=datetime(2026, 9, 4, 12, 30, tzinfo=UTC),
        completed_at=datetime(2026, 9, 4, 12, 31, tzinfo=UTC),
        accepted_invocation_digest=_D2,
        accepted_invocation_version=4,
        accepted_context_digest=_D3,
        agent_revision_digest="4" * 64,
        assembly_evidence_digest="5" * 64,
        assembly_fingerprint="6" * 64,
        lifecycle_high_water_mark=42,
        terminal_event_digest="7" * 64,
        lifecycle_event_count=12,
        lifecycle_counts={"lifecycle": 4, "tools": 6, "artifacts": 2},
        sections=(
            _section("accepted_invocation", _D2),
            EvidenceSectionV1.absent_by_design("actor_credential"),
            _section("assembly", "5" * 64),
            EvidenceSectionV1.absent_by_design("subagent_catalog"),
            EvidenceSectionV1.absent_by_design("skill_material"),
            EvidenceSectionV1.absent_by_design("extension_material"),
            EvidenceSectionV1.absent_by_design("tool_plane"),
            _section("lifecycle", "7" * 64),
            _section("tool_receipts", "8" * 64, "9" * 64),
            EvidenceSectionV1.absent_by_design("mcp_tasks"),
            EvidenceSectionV1.absent_by_design("subagent_batches"),
            EvidenceSectionV1.absent_by_design("sandbox_execution"),
            EvidenceSectionV1.absent_by_design("retrieval_observations"),
            EvidenceSectionV1.unqualified("qualification"),
        ),
        artifact_paths=("/mnt/user-data/outputs/report.txt",),
    )


class _Reader:
    def __init__(self, sources: list[EvidenceSnapshotSourceV1], valid: list[bool]) -> None:
        self.sources = sources
        self.valid = valid
        self.reads = 0

    async def read(self, request: EvidenceSnapshotRequest) -> EvidenceSnapshotSourceV1:
        source = self.sources[min(self.reads, len(self.sources) - 1)]
        self.reads += 1
        return source

    async def revalidate(
        self,
        request: EvidenceSnapshotRequest,
        source: EvidenceSnapshotSourceV1,
    ) -> bool:
        return self.valid[min(self.reads - 1, len(self.valid) - 1)]


@pytest.mark.asyncio
async def test_snapshot_service_builds_immutable_terminal_snapshot_and_hides_raw_ids() -> None:
    service = RunEvidenceSnapshotService(_Reader([_source()], [True]))

    snapshot = await service.build(
        EvidenceSnapshotRequest(
            tenant=_TENANT,
            thread_id="thread-raw-id",
            run_id="run-raw-id",
            owner_id="user-must-never-serialize",
        )
    )

    assert snapshot.run_ref.startswith("run-")
    assert snapshot.thread_ref.startswith("thread-")
    serialized = json.dumps(snapshot.to_manifest(()).to_dict())
    assert "thread-raw-id" not in serialized
    assert "run-raw-id" not in serialized
    assert "user-must-never-serialize" not in serialized


@pytest.mark.asyncio
async def test_snapshot_service_retries_changed_read_then_fails_with_stable_code() -> None:
    reader = _Reader([_source(), _source()], [False, False])
    service = RunEvidenceSnapshotService(reader, max_attempts=2)

    with pytest.raises(RunEvidenceBundleError, match="evidence_snapshot_changed") as raised:
        await service.build(
            EvidenceSnapshotRequest(
                tenant=_TENANT,
                thread_id="thread-raw-id",
                run_id="run-raw-id",
                owner_id="user",
            )
        )

    assert raised.value.code == "evidence_snapshot_changed"
    assert reader.reads == 2


@pytest.mark.asyncio
async def test_snapshot_service_retries_changed_read_then_returns_coherent_snapshot() -> None:
    reader = _Reader([_source(), _source()], [False, True])

    snapshot = await RunEvidenceSnapshotService(reader, max_attempts=2).build(
        EvidenceSnapshotRequest(
            tenant=_TENANT,
            thread_id="thread-raw-id",
            run_id="run-raw-id",
            owner_id="user",
        )
    )

    assert snapshot.terminal_status == "success"
    assert reader.reads == 2


@pytest.mark.asyncio
async def test_snapshot_service_refuses_nonterminal_and_incomplete_required_evidence() -> None:
    request = EvidenceSnapshotRequest(
        tenant=_TENANT,
        thread_id="thread-raw-id",
        run_id="run-raw-id",
        owner_id="user",
    )
    with pytest.raises(RunEvidenceBundleError, match="run_not_terminal"):
        await RunEvidenceSnapshotService(_Reader([_source(status="running")], [True])).build(request)

    source = _source()
    incomplete = EvidenceSnapshotSourceV1(
        **{
            **source.as_kwargs(),
            "sections": (
                *source.sections[:-1],
                EvidenceSectionV1.unavailable("qualification", required=True),
            ),
        }
    )
    with pytest.raises(RunEvidenceBundleError, match="evidence_incomplete"):
        await RunEvidenceSnapshotService(_Reader([incomplete], [True])).build(request)

    active = EvidenceSnapshotSourceV1(
        **{
            **source.as_kwargs(),
            "mutation_active": True,
        }
    )
    with pytest.raises(RunEvidenceBundleError, match="run_operation_active"):
        await RunEvidenceSnapshotService(_Reader([active], [True])).build(request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("section", "code"),
    [
        (EvidenceSectionV1.pruned("qualification"), "evidence_pruned"),
        (EvidenceSectionV1.legacy("qualification"), "evidence_legacy_unbound"),
    ],
)
async def test_snapshot_service_preserves_distinct_missing_evidence_codes(
    section: EvidenceSectionV1,
    code: str,
) -> None:
    source = _source()
    changed = EvidenceSnapshotSourceV1(
        **{
            **source.as_kwargs(),
            "sections": (*source.sections[:-1], section),
        }
    )

    with pytest.raises(RunEvidenceBundleError, match=code):
        await RunEvidenceSnapshotService(_Reader([changed], [True])).build(
            EvidenceSnapshotRequest(
                tenant=_TENANT,
                thread_id="thread-raw-id",
                run_id="run-raw-id",
                owner_id="user",
            )
        )


def test_manifest_canonical_bytes_digest_and_reference_root_are_stable() -> None:
    source = _source()
    snapshot = RunEvidenceSnapshotService.validate_source(source)
    manifest = snapshot.to_manifest(
        (
            EvidenceArtifactV1(path="z/report.txt", size=3, sha256=_D1),
            EvidenceArtifactV1(path="a/summary.json", size=2, sha256=_D2, media_type="application/json"),
        )
    )

    parsed = RunEvidenceBundleManifestV1.from_bytes(manifest.canonical_bytes())

    assert parsed == manifest
    assert parsed.artifacts[0].path == "a/summary.json"
    assert parsed.manifest_digest == manifest.manifest_digest
    assert canonical_evidence_root("tool_receipts", (_D2, _D1)) == canonical_evidence_root("tool_receipts", (_D1, _D2))
    assert manifest.manifest_digest == "9cf072871aae448690d1a9bb9b961a4197ce842c1dd0130119e6fa856a373619"
    assert manifest.bundle_ref == "bundle-36bacbff9daeeed521f8d33b"
    assert hashlib.sha256(manifest.canonical_bytes()).hexdigest() == "6e658ee2b7685887cc41f4a72226e1a37e67b48032ff760a3924a972bb4a298e"

    reordered_source = EvidenceSnapshotSourceV1(
        **{
            **source.as_kwargs(),
            "sections": tuple(reversed(source.sections)),
            "lifecycle_counts": {
                "artifacts": 2,
                "tools": 6,
                "lifecycle": 4,
            },
        }
    )
    reordered = RunEvidenceSnapshotService.validate_source(reordered_source).to_manifest(tuple(reversed(manifest.artifacts)))
    assert reordered.canonical_bytes() == manifest.canonical_bytes()


def test_manifest_parser_rejects_unknown_versions_and_noncanonical_bytes() -> None:
    manifest = RunEvidenceSnapshotService.validate_source(_source()).to_manifest(())
    document = manifest.to_dict()
    document["schema_version"] = 2
    with pytest.raises(RunEvidenceBundleError, match="manifest_version_unsupported"):
        RunEvidenceBundleManifestV1.from_dict(document)

    pretty = json.dumps(manifest.to_dict(), indent=2).encode()
    with pytest.raises(RunEvidenceBundleError, match="manifest_not_canonical"):
        RunEvidenceBundleManifestV1.from_bytes(pretty)


def test_manifest_contains_only_bounded_safe_contract_fields() -> None:
    manifest = RunEvidenceSnapshotService.validate_source(_source()).to_manifest(())
    serialized = manifest.canonical_bytes()

    for forbidden in (
        b"prompt",
        b"message",
        b"argument",
        b"result_projection",
        b"retrieval_content",
        b"user_id",
        b"provider_handle",
        b"password",
        b"secret",
    ):
        assert forbidden not in serialized.lower()

    with pytest.raises(RunEvidenceBundleError, match="artifact_path_invalid"):
        EvidenceArtifactV1(path="../escape", size=1, sha256=_D1)
    with pytest.raises(RunEvidenceBundleError, match="artifact_path_invalid"):
        EvidenceArtifactV1(path="CON.txt", size=1, sha256=_D1)

    with pytest.raises(RunEvidenceBundleError, match="manifest_not_canonical"):
        canonical_json_bytes({"\u00e9": 1, "e\u0301": 2})

    with pytest.raises(RunEvidenceBundleError, match="bundle_limit_exceeded"):
        EvidenceSectionV1.complete(
            "tool_receipts",
            tuple(f"{index:064x}" for index in range(4097)),
        )


def test_manifest_rejects_admission_section_cross_link_drift() -> None:
    source = _source()
    changed = EvidenceSnapshotSourceV1(
        **{
            **source.as_kwargs(),
            "subagent_catalog_digest": _D1,
        }
    )

    with pytest.raises(RunEvidenceBundleError, match="evidence_cross_link_invalid"):
        RunEvidenceSnapshotService.validate_source(changed)
