"""Installed previews preserve canonical bytes and the existing owner/read fence."""

import hashlib
import os

import pytest
from _router_auth_helpers import make_authed_test_app
from deerflow_extension_api import ArtifactPresentation, PluginContribution
from fastapi.testclient import TestClient

from app.gateway.routers import artifacts
from deerflow.config import paths as paths_module
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.runtime.user_context import get_effective_user_id

URL = "/api/threads/summary-thread/artifacts/mnt/user-data/outputs/a.summary.json"
KEY = "example.summary/summary"


@pytest.fixture
def source_file(tmp_path, monkeypatch):
    paths = paths_module.Paths(tmp_path)
    monkeypatch.setattr(paths_module, "_paths", paths)
    outputs = paths.sandbox_outputs_dir("summary-thread", user_id=get_effective_user_id())
    outputs.mkdir(parents=True)
    path = outputs / "a.summary.json"
    path.write_bytes(b'{"source":"original"}')
    return path


def client_for(*, enabled=True, owner=True, project=lambda raw: b'{"summary":"ready"}', source_max_bytes=4096, preview_max_bytes=256, installed=True):
    registry = ExtensionRegistry()
    with registry.attributed_to("example:install"):
        declaration = ArtifactPresentation(
            id="summary", suffixes=(".summary.json",), source_max_bytes=source_max_bytes, preview_max_bytes=preview_max_bytes, project=project, projection_marker="example-summary-v1", compat_queries=("legacy_preview",)
        )
        if installed:
            registry.plugin(PluginContribution(namespace="example.summary", title="Summary", enabled=enabled, api_version=2, artifacts=(declaration,)))
    app = make_authed_test_app(owner_check_passes=owner)
    app.state.extensions = registry.build()
    app.include_router(artifacts.router)
    return TestClient(app)


def test_generic_preview_and_installed_alias_keep_canonical_revision_and_download(source_file):
    original = source_file.read_bytes()
    with client_for() as client:
        for params in ({"preview": KEY}, {"legacy_preview": True}):
            result = client.get(URL, params=params)
            assert result.status_code == 200
            assert result.json() == {"summary": "ready"}
            assert result.headers["x-artifact-projection"] == "example-summary-v1"
            assert result.headers["etag"] == f'"{hashlib.sha256(original).hexdigest()}"'
            assert result.headers["x-artifact-source-bytes"] == str(len(original))
            assert result.headers["cache-control"] == "no-store"
        assert client.get(URL, params={"preview": KEY, "download": True}).content == original
        assert client.get(URL).content == original
    assert source_file.read_bytes() == original


def test_generic_preview_preserves_owner_gate_before_invoking_package_code(source_file):
    called = []
    with client_for(owner=False, project=lambda raw: called.append(raw) or b"{}") as client:
        assert client.get(URL, params={"preview": KEY}).status_code in (403, 404)
    assert called == []


@pytest.mark.parametrize("enabled,installed", [(False, True), (True, False)])
def test_unavailable_preview_capability_keeps_plain_source_available(source_file, enabled, installed):
    called = []
    with client_for(enabled=enabled, installed=installed, project=lambda raw: called.append(raw) or b"{}") as client:
        assert client.get(URL, params={"preview": KEY}).status_code == 501
        assert client.get(URL).content == source_file.read_bytes()
    assert called == []


@pytest.mark.parametrize("failure,status", [("source-limit", 413), ("preview-limit", 413), ("handler", 422), ("wrong-type", 422)])
def test_projection_failures_stay_bounded_without_rewriting_source(source_file, failure, status):
    def project(raw):
        if failure == "handler":
            raise ValueError("private-details")
        if failure == "wrong-type":
            return "bad"
        return b"x" * (257 if failure == "preview-limit" else 1)

    original = source_file.read_bytes()
    with client_for(project=project, source_max_bytes=4 if failure == "source-limit" else 4096) as client:
        result = client.get(URL, params={"preview": KEY})
        assert result.status_code == status
        assert "private-details" not in result.text
        assert client.get(URL).content == original


@pytest.mark.skipif(os.name == "nt", reason="descriptor-relative source reads require POSIX")
def test_preview_refuses_linked_parents(source_file):
    root = source_file.parent
    parked = root.with_name("parked")
    root.rename(parked)
    root.symlink_to(parked, target_is_directory=True)
    with client_for() as client:
        assert client.get(URL, params={"preview": KEY}).status_code == 403


def test_preview_hashes_the_captured_source_even_when_the_path_changes_in_the_handler(source_file):
    original = source_file.read_bytes()

    def project(raw):
        replacement = source_file.with_name("replacement")
        replacement.write_bytes(b'{"source":"replacement"}')
        replacement.replace(source_file)
        return raw

    with client_for(project=project) as client:
        result = client.get(URL, params={"preview": KEY})
    assert result.content == original
    assert result.headers["etag"] == f'"{hashlib.sha256(original).hexdigest()}"'
    assert source_file.read_bytes() != original
