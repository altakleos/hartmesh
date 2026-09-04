from __future__ import annotations

import asyncio
import io
import threading
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_artifact_archive import RUN_ID, THREAD_ID, _archive_app
from test_run_evidence_archive import _snapshot

from app.gateway.artifact_archive import ArtifactArchiveError
from app.gateway.auth.pat import required_pat_scope
from app.gateway.routers import thread_runs
from deerflow.runtime.run_evidence import (
    RUN_EVIDENCE_MANIFEST_PATH,
    RunEvidenceBundleError,
    RunEvidenceBundleManifestV1,
)
from deerflow.runtime.tenant_identity import TenantIdentityV1

EVIDENCE_URL = f"/api/threads/{THREAD_ID}/runs/{RUN_ID}/artifacts/evidence-bundle"


def test_evidence_bundle_status_and_download_are_additive_and_no_store(
    tmp_path,
    monkeypatch,
) -> None:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    (outputs / "report.txt").write_bytes(b"report")
    artifact_path = "/mnt/user-data/outputs/report.txt"
    snapshot = _snapshot(paths=(artifact_path,))
    client, _, _ = _archive_app(
        monkeypatch,
        outputs,
        paths=[artifact_path],
    )
    client.app.state.tenant_identity = TenantIdentityV1.from_canonical_id("local")

    async def build_snapshot(**_kwargs):
        return snapshot

    async def app_config():
        return SimpleNamespace(tool_output=None)

    monkeypatch.setattr(
        thread_runs,
        "build_gateway_run_evidence_snapshot",
        build_snapshot,
    )
    monkeypatch.setattr(thread_runs, "safe_app_config_async", app_config)

    with client:
        status = client.get(EVIDENCE_URL)
        response = client.post(EVIDENCE_URL, json={"ignored": "body"})

    assert status.status_code == 200
    assert status.json()["profile"] == "complete_durable"
    assert status.json()["artifact_count"] == 1
    assert status.json()["authenticity"] == "not_signed"
    assert response.status_code == 200
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["x-hartmesh-evidence-authenticity"] == "not-signed"
    assert response.headers["x-hartmesh-evidence-bundle"].startswith("bundle-")
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        manifest = RunEvidenceBundleManifestV1.from_bytes(archive.read(RUN_EVIDENCE_MANIFEST_PATH))
        assert archive.read("report.txt") == b"report"
        assert manifest.run_ref == snapshot.run_ref
        assert manifest.artifacts[0].sha256
    assert client.app.state.run_evidence_bundle_metrics == {
        "requested": 2,
        "completed": 2,
    }


def test_evidence_bundle_returns_safe_snapshot_failure_code(tmp_path, monkeypatch) -> None:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    client, _, _ = _archive_app(monkeypatch, outputs, paths=[])
    client.app.state.tenant_identity = TenantIdentityV1.from_canonical_id("local")

    async def fail_snapshot(**_kwargs):
        raise RunEvidenceBundleError("evidence_pruned")

    monkeypatch.setattr(
        thread_runs,
        "build_gateway_run_evidence_snapshot",
        fail_snapshot,
    )

    with client:
        response = client.get(EVIDENCE_URL)

    assert response.status_code == 409
    assert response.json() == {"detail": "evidence_pruned"}
    assert client.app.state.run_evidence_bundle_metrics == {
        "requested": 1,
        "refused": 1,
    }


def test_evidence_bundle_returns_safe_artifact_race_code(tmp_path, monkeypatch) -> None:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    path = "/mnt/user-data/outputs/report.txt"
    snapshot = _snapshot(paths=(path,))
    client, _, _ = _archive_app(monkeypatch, outputs, paths=[path])
    client.app.state.tenant_identity = TenantIdentityV1.from_canonical_id("local")

    async def build_snapshot(**_kwargs):
        return snapshot

    def fail_archive(*_args, **_kwargs):
        raise ArtifactArchiveError(
            "private filesystem detail",
            code="artifact_changed",
        )

    async def app_config():
        return SimpleNamespace(tool_output=None)

    monkeypatch.setattr(
        thread_runs,
        "build_gateway_run_evidence_snapshot",
        build_snapshot,
    )
    monkeypatch.setattr(thread_runs, "build_run_evidence_archive", fail_archive)
    monkeypatch.setattr(thread_runs, "safe_app_config_async", app_config)

    with client:
        response = client.post(EVIDENCE_URL)

    assert response.status_code == 409
    assert response.json() == {"detail": "artifact_changed"}
    assert "private filesystem detail" not in response.text
    assert client.app.state.run_evidence_bundle_metrics == {
        "requested": 1,
        "failed": 1,
    }


def test_evidence_bundle_owner_denial_is_generic_for_status_and_download(
    tmp_path,
    monkeypatch,
) -> None:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    client, _, _ = _archive_app(
        monkeypatch,
        outputs,
        paths=[],
        owner_check_passes=False,
    )

    with client:
        status = client.get(EVIDENCE_URL)
        download = client.post(EVIDENCE_URL)

    assert status.status_code == download.status_code == 404
    assert status.json() == download.json() == {"detail": f"Thread {THREAD_ID} not found"}


def test_evidence_bundle_pat_policy_is_explicit_and_method_bounded() -> None:
    assert required_pat_scope("GET", EVIDENCE_URL) == "runs:read"
    assert required_pat_scope("POST", EVIDENCE_URL) == "runs:read"
    assert required_pat_scope("DELETE", EVIDENCE_URL) is None


@pytest.mark.asyncio
async def test_repeated_cancellation_keeps_evidence_archive_slot_until_worker_exit(
    monkeypatch,
) -> None:
    started = threading.Event()
    release = threading.Event()

    def blocking_build(*_args, **_kwargs):
        started.set()
        release.wait(timeout=5)
        return thread_runs.ArtifactArchiveResult(
            io.BytesIO(),
            0,
            0,
            0,
            manifest_digest="1" * 64,
            bundle_ref="bundle-1234567890abcdef",
        )

    slots = asyncio.Semaphore(1)
    monkeypatch.setattr(thread_runs, "_artifact_archive_slots", slots)
    monkeypatch.setattr(thread_runs, "build_run_evidence_archive", blocking_build)
    task = asyncio.create_task(
        thread_runs._build_evidence_archive_without_abandoning_worker(
            Path("unused"),
            Path("unused-parent"),
            _snapshot(paths=()),
            extra_reserved_dir_names=set(),
        )
    )
    assert await asyncio.to_thread(started.wait, 1)

    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    done, _ = await asyncio.wait({task}, timeout=0.1)

    try:
        assert not done
        assert slots.locked()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_evidence_archive_reports_bounded_busy_code(monkeypatch) -> None:
    slots = asyncio.Semaphore(0)
    monkeypatch.setattr(thread_runs, "_artifact_archive_slots", slots)

    with pytest.raises(RunEvidenceBundleError, match="bundle_generation_busy"):
        await thread_runs._build_evidence_archive_without_abandoning_worker(
            Path("unused"),
            Path("unused-parent"),
            _snapshot(paths=()),
            extra_reserved_dir_names=set(),
        )
