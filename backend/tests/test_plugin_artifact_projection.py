"""Installed previews preserve canonical bytes and the existing owner/read fence."""

import hashlib
import os

import pytest
from _projection_support import ready_projection_host
from _router_auth_helpers import make_authed_test_app
from deerflow_extension_api import ArtifactPresentation, PluginContribution
from fastapi.testclient import TestClient
from support import artifact_projectors as projectors

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


def client_for(*, enabled=True, owner=True, signed_in=False, project=projectors.summary, source_max_bytes=4096, preview_max_bytes=256, installed=True):
    registry = ExtensionRegistry()
    with registry.attributed_to("example:install"):
        declaration = ArtifactPresentation(
            id="summary", suffixes=(".summary.json",), source_max_bytes=source_max_bytes, preview_max_bytes=preview_max_bytes, project=project, projection_marker="example-summary-v1", compat_queries=("legacy_preview",)
        )
        if installed:
            registry.plugin(PluginContribution(namespace="example.summary", title="Summary", enabled=enabled, api_version=2, artifacts=(declaration,)))
    app = make_authed_test_app(owner_check_passes=owner, signed_in=signed_in)
    app.state.extensions = registry.build()
    app.include_router(artifacts.router)
    app.router.lifespan_context = ready_projection_host
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
    with client_for(owner=False, project=projectors.refused) as client:
        assert client.get(URL, params={"preview": KEY}).status_code in (403, 404)


@pytest.mark.parametrize("enabled,installed", [(False, True), (True, False)])
def test_unavailable_preview_capability_keeps_plain_source_available(source_file, enabled, installed):
    with client_for(enabled=enabled, installed=installed, project=projectors.refused) as client:
        assert client.get(URL, params={"preview": KEY}).status_code == 501
        assert client.get(URL).content == source_file.read_bytes()


@pytest.mark.parametrize("failure,status", [("source-limit", 413), ("preview-limit", 413), ("handler", 422), ("wrong-type", 422)])
def test_projection_failures_stay_bounded_without_rewriting_source(source_file, failure, status):
    project = {"handler": projectors.refused, "wrong-type": projectors.invalid_type, "preview-limit": projectors.too_large, "source-limit": projectors.summary}[failure]

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


def test_preview_hashes_the_actual_snapshot_if_file_changes_between_reads(source_file, monkeypatch):
    from app.gateway.routers import _artifact_projection as adapter

    original_fingerprint = adapter._fingerprint
    replacement = b'{"source":"replacement"}'

    def replace_after_hash(source, budget):
        result = original_fingerprint(source, budget)
        staging = source_file.with_suffix(".new")
        staging.write_bytes(replacement)
        staging.replace(source_file)
        return result

    monkeypatch.setattr(adapter, "_fingerprint", replace_after_hash)
    with client_for(project=projectors.echo_source) as client:
        result = client.get(URL, params={"preview": KEY})
    assert result.content == replacement
    assert result.headers["etag"] == f'"{hashlib.sha256(replacement).hexdigest()}"'


def test_projection_rechecks_thread_ownership_before_returning_bytes(source_file):
    with client_for() as client:
        client.app.state.thread_store.check_access.side_effect = [True, False]
        result = client.get(URL, params={"preview": KEY})
        assert result.status_code == 404
        assert "x-artifact-projection" not in result.headers


@pytest.mark.parametrize("warm", [False, True])
def test_managed_preview_refreshes_space_on_delivery_even_on_a_memo_hit(tmp_path, monkeypatch, warm):
    from unittest.mock import AsyncMock

    from fastapi import HTTPException
    from test_compose_chat_storage import pinned_chat_volume

    from app.gateway import chat_files, chat_storage
    from deerflow.spaces.chat_paths import ChatStoragePaths

    volume = pinned_chat_volume(tmp_path)
    paths = ChatStoragePaths(paths_module.Paths(tmp_path / "legacy"), user_id="alice", user=volume)
    paths.ensure_thread_dirs("summary-thread", user_id="alice")
    path = paths.sandbox_outputs_dir("summary-thread", user_id="alice") / "a.summary.json"
    path.write_bytes(b"{}")
    prepared = chat_storage.PreparedChatStorage(paths, paths.mounts("summary-thread", user_id="alice"))
    monkeypatch.setattr(chat_storage, "prepare_chat_request", AsyncMock(return_value=prepared))
    with client_for() as client:
        if warm:
            assert client.get(URL, params={"preview": KEY}).status_code == 200
        accessed = []
        read_with = chat_files.ChatFileAccess.read_with
        refresh = chat_files.ChatFileAccess.refresh

        async def count_reads(self, *args):
            result = await read_with(self, *args)
            accessed.append(result)
            return result

        async def revoke_at_delivery(self, *args, **kwargs):
            if len(accessed) == (1 if warm else 2):
                raise HTTPException(503, {"code": "storage-unavailable"})
            return await refresh(self, *args, **kwargs)

        monkeypatch.setattr(chat_files.ChatFileAccess, "read_with", count_reads)
        monkeypatch.setattr(chat_files.ChatFileAccess, "refresh", revoke_at_delivery)
        denied = client.get(URL, params={"preview": KEY})
        assert denied.status_code == 503
        assert denied.json()["detail"]["code"] == "storage-unavailable"
        assert isinstance(accessed[0], tuple)
        assert len(accessed) == (1 if warm else 2)
        assert "x-artifact-projection" not in denied.headers


def test_expired_session_cannot_collect_a_finished_projection(source_file):
    with client_for(signed_in=True) as client:
        assert client.get(URL, params={"preview": KEY}).status_code == 401


@pytest.mark.parametrize("granted", [False, True])
def test_delivery_uses_current_session_permissions(source_file, monkeypatch, granted):
    from app.gateway.routers import _artifact_projection as adapter

    async def current_account(request):
        return request.state.user

    async def current_permissions(request, user):
        return ["threads:read"] if granted else []

    monkeypatch.setattr(adapter, "resolve_session_cookie", current_account)
    monkeypatch.setattr(adapter, "resolve_route_permissions_for_request", current_permissions)
    with client_for(signed_in=True) as client:
        result = client.get(URL, params={"preview": KEY})
        assert result.status_code == (200 if granted else 403)
        assert ("x-artifact-projection" in result.headers) == granted


def test_busy_response_is_typed_and_download_still_returns_original(source_file, monkeypatch):
    from deerflow.extensions.projection_host import ProjectionError

    async def busy(**kwargs):
        raise ProjectionError(503, 3)

    with client_for() as client:
        monkeypatch.setattr(client.app.state.projection_host, "project", busy)
        result = client.get(URL, params={"preview": KEY})
        assert result.status_code == 503
        assert result.json()["detail"]["code"] == "preview-busy"
        assert result.headers["retry-after"] == "3"
        assert client.get(URL, params={"download": True, "preview": KEY}).content == source_file.read_bytes()


@pytest.mark.parametrize(
    "before,after,status",
    [
        (None, None, 200),
        ({"incarnation": "a" * 32, "created_at": "early"}, {"incarnation": "a" * 32, "created_at": "later"}, 200),
        ({"incarnation": "a" * 32}, None, 404),
        ({"incarnation": "a" * 32}, {"incarnation": "b" * 32}, 404),
        (None, {"incarnation": "a" * 32}, 404),
    ],
)
def test_preview_delivery_matches_original_thread_generation(source_file, before, after, status):
    with client_for() as client:
        client.app.state.thread_store.get.side_effect = [before, after]
        assert client.get(URL, params={"preview": KEY}).status_code == status
