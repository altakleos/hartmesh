"""A report card is a bounded projection of one owner's immutable read."""

import hashlib
import json
import os
from pathlib import Path

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.routers import artifacts
from deerflow.config import paths as paths_module
from deerflow.runtime.user_context import get_effective_user_id

URL = "/api/threads/report-thread/artifacts/mnt/user-data/outputs/month.report.json"
FIXTURE = Path(__file__).resolve().parents[2] / "frontend-hm/tests/fixtures/business-report/2026-08-business-review.report.json"


@pytest.fixture
def report_file(tmp_path, monkeypatch):
    paths = paths_module.Paths(tmp_path)
    monkeypatch.setattr(paths_module, "_paths", paths)
    outputs = paths.sandbox_outputs_dir("report-thread", user_id=get_effective_user_id())
    outputs.mkdir(parents=True)
    report = json.loads(FIXTURE.read_text(encoding="utf-8"))
    report["raw_rows"] = [{"private_row": "x" * 200} for _ in range(5000)]
    report["meta"]["build"] = {"internal_build_detail": "retained in source"}
    path = outputs / "month.report.json"
    path.write_text(json.dumps(report), encoding="utf-8")
    return path


def client_for(*, owner=True):
    app = make_authed_test_app(owner_check_passes=owner)
    app.include_router(artifacts.router)
    return TestClient(app)


@pytest.mark.skipif(os.name == "nt", reason="descriptor-relative projection reads require POSIX")
def test_projection_preserves_card_fields_revision_and_full_download(report_file):
    original = report_file.read_bytes()
    source = json.loads(original)
    with client_for() as client:
        preview = client.get(URL, params={"report_preview": True})
        full = client.get(URL, params={"download": True, "report_preview": True})
    assert preview.status_code == 200
    assert preview.headers["x-artifact-projection"] == "business-report-v1"
    assert preview.headers["etag"] == f'"{hashlib.sha256(original).hexdigest()}"'
    assert preview.headers["cache-control"] == "no-store"
    assert int(preview.headers["x-artifact-source-bytes"]) == len(original)
    projected = preview.json()
    assert "raw_rows" not in projected
    assert "build" not in projected["meta"]
    for key in ("version", "kpis", "sections", "charts", "checks", "notes"):
        assert projected[key] == source[key]
    assert projected["meta"]["inputs"] == source["meta"]["inputs"]
    assert len(preview.content) < len(original) / 10
    assert full.content == original == report_file.read_bytes()
    source["meta"]["draft"] += 1
    report_file.write_text(json.dumps(source), encoding="utf-8")
    with client_for() as client:
        revised = client.get(URL, params={"report_preview": True})
    assert revised.headers["etag"] != preview.headers["etag"]
    assert revised.json()["meta"]["draft"] == projected["meta"]["draft"] + 1


def test_projection_keeps_the_thread_owner_gate(report_file, monkeypatch):
    def forbidden_read(*args, **kwargs):
        pytest.fail("must authorize before reading")

    monkeypatch.setattr(artifacts, "report_projection", forbidden_read)
    with client_for(owner=False) as client:
        assert client.get(URL, params={"report_preview": True}).status_code in (403, 404)


@pytest.mark.skipif(os.name == "nt", reason="descriptor-relative projection reads require POSIX")
@pytest.mark.parametrize("payload, status", [(b"{bad", 422), (b"[1,2]", 422), (b'{"meta": {}, "version": NaN}', 422), (b"x" * (16 * 1024 * 1024 + 1), 413)], ids=["malformed", "not-object", "nonfinite", "oversized"])
def test_projection_rejects_invalid_or_oversized_source(report_file, payload, status):
    report_file.write_bytes(payload)
    with client_for() as client:
        assert client.get(URL, params={"report_preview": True}).status_code == status


@pytest.mark.skipif(os.name == "nt", reason="descriptor-relative projection reads require POSIX")
def test_projection_refuses_symlinked_output_root(report_file, tmp_path):
    outputs = report_file.parent
    parked = outputs.with_name("parked")
    outputs.rename(parked)
    outputs.symlink_to(parked, target_is_directory=True)
    with client_for() as client:
        assert client.get(URL, params={"report_preview": True}).status_code == 403


@pytest.mark.skipif(os.name == "nt", reason="descriptor-relative projection reads require POSIX")
def test_projection_has_its_own_response_budget(report_file):
    report = json.loads(report_file.read_bytes())
    report["notes"] = ["n" * (1024 * 1024)]
    report_file.write_text(json.dumps(report), encoding="utf-8")
    with client_for() as client:
        assert client.get(URL, params={"report_preview": True}).status_code == 413
