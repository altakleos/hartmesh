from uuid import uuid4

import httpx
import pytest
import pytest_asyncio
from _router_auth_helpers import make_authed_test_app
from _storage_spaces_test_support import make_storage_fixture
from deerflow_extension_api import ExtensionRuntimeDeps

from app.gateway.auth.models import User
from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from app.gateway.routers import files as files_router
from app.gateway.routers import shared as shared_router
from deerflow.config.paths import Paths
from deerflow.extensions.loader import load_extensions
from deerflow.features.plugins import with_default_features
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.projects.model import ProjectDocumentRow, ProjectRow
from deerflow.persistence.projects.sql import ProjectDocumentRepository, ProjectRepository
from deerflow.persistence.shared_publications.model import SharedPublicationRow
from deerflow.persistence.shared_publications.sql import SharedPublicationRepository
from deerflow.spaces.contract import Permission, PrincipalRef, ResolvedPrincipal
from deerflow.spaces.facade import HostStorageProvider
from deerflow.spaces.principals import HostPrincipalResolver

THREAD = "11111111-1111-1111-1111-111111111111"


@pytest_asyncio.fixture(params=["sqlite", "postgres"])
async def feature_app(tmp_path, request, monkeypatch):
    import _router_auth_helpers

    from app.gateway.authz import Permissions

    monkeypatch.setattr(_router_auth_helpers, "_STUB_PERMISSIONS", [*_router_auth_helpers._STUB_PERMISSIONS, Permissions.PROJECTS_READ, Permissions.PROJECTS_WRITE, Permissions.PROJECTS_DELETE])
    async for files, sf, catalog in make_storage_fixture(tmp_path, request):
        alice = User(id=uuid4(), email="alice@example.com", password_hash="x", system_role="admin")
        bob = User(id=uuid4(), email="bob@example.com", password_hash="x", system_role="user")
        current = [alice]

        async def lookup(reference):
            return ResolvedPrincipal(reference, reference.subject_id == str(alice.id)) if reference.kind == "human" and reference.subject_id in {str(alice.id), str(bob.id)} else None

        files.registry._resolver = HostPrincipalResolver(human=lookup)
        async with sf().bind.begin() as c:
            await c.run_sync(StorageFeatureLinkRow.__table__.create)
            await c.run_sync(SharedPublicationRow.__table__.create)
            await c.run_sync(ProjectRow.__table__.create)
            await c.run_sync(ProjectDocumentRow.__table__.create)
        app = make_authed_test_app(user_factory=lambda: current[0], bind_current_user=True, signed_in=True)

        @app.middleware("http")
        async def credential(request, call_next):
            request.state.auth_source = AUTH_SOURCE_SESSION
            return await call_next(request)

        loaded, diagnostics = load_extensions(with_default_features([]))
        assert not diagnostics
        provider = HostStorageProvider(lambda: files)
        for _, service in loaded.services:
            await service.start(ExtensionRuntimeDeps(app_store=loaded.app_store, session_factory=sf, storage=provider))
        app.state.extensions = loaded
        app.state.storage_spaces = files
        app.state.shared_publications_repo = SharedPublicationRepository(sf)
        app.state.project_repo = ProjectRepository(sf)
        app.state.project_document_repo = ProjectDocumentRepository(sf)
        from app.gateway.routers import project_documents, projects, trash

        app.include_router(files_router.router)
        app.include_router(shared_router.router)
        app.include_router(projects.router)
        app.include_router(project_documents.router)
        app.include_router(trash.router)
        paths = Paths(tmp_path / "ordinary")
        monkeypatch.setattr("deerflow.config.paths._paths", paths)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            yield client, app, files, catalog, paths, alice, bob, current
        for _, service in loaded.services:
            await service.stop()


@pytest.mark.asyncio
async def test_keep_publish_dedup_conflicts_exact_undo_and_private_controls(feature_app):
    client, _, files, catalog, paths, alice, _, _ = feature_app
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(alice.id))
    outputs.mkdir(parents=True)
    (outputs / "report.txt").write_bytes(b"first")
    kept = await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/report.txt"})
    assert kept.status_code == 201, kept.text
    first = await client.post("/api/shared/publish", json={"path": kept.json()["virtual_path"]})
    assert first.status_code == 201, first.text
    duplicate = await client.post("/api/shared/publish", json={"path": kept.json()["virtual_path"]})
    assert duplicate.status_code == 200, duplicate.text
    assert duplicate.json()["publication_id"] == first.json()["publication_id"]
    assert (await client.get("/api/shared/report.txt")).content == b"first"
    removed = await client.delete("/api/shared/report.txt", params={"expected_publication_id": first.json()["publication_id"]})
    assert removed.status_code == 200, removed.text
    replacement = await client.post("/api/shared/publish", json={"path": kept.json()["virtual_path"]})
    assert replacement.status_code == 201, replacement.text
    stale = await client.delete("/api/shared/report.txt", params={"expected_publication_id": first.json()["publication_id"]})
    assert stale.status_code in {409, 412}, stale.text
    assert (await client.get("/api/shared/report.txt")).content == b"first"
    assert (await client.get("/api/shared")).status_code == 200
    assert not paths.user_files_dir(str(alice.id)).exists()
    assert not paths.shared_dir().exists()
    actor = PrincipalRef("human", str(alice.id))
    resources = await files.registry.list(actor=actor)
    assert len(resources) == 2
    assert all(not (volume.data_path / ".shared-state").exists() for volume in catalog.verified.values())


@pytest.mark.asyncio
async def test_disabled_feature_preserves_core_access_and_has_no_legacy_fallback(feature_app, monkeypatch):
    client, app, files, _, _, alice, _, _ = feature_app
    assert (await client.get("/api/files")).status_code == 200
    actor = PrincipalRef("human", str(alice.id))
    space = (await files.registry.list(actor=actor))[0]
    import app.gateway.storage_features as dispatch

    original = dispatch.plugin_settings
    monkeypatch.setattr(dispatch, "plugin_settings", lambda source, plugin: {**original(source, plugin), "enabled": False})
    assert (await client.get("/api/files")).status_code == 503
    assert await files.list_directory(actor=actor, space_id=space.id) == ([], False)
    assert (await files.registry.get(actor=actor, space_id=space.id)).id == space.id


@pytest.mark.asyncio
async def test_company_custody_requires_explicit_grants_and_current_membership(feature_app):
    client, _, files, _, _, alice, bob, current = feature_app
    assert (await client.get("/api/shared")).status_code == 200
    actor = PrincipalRef("human", str(alice.id))
    space = (await files.registry.list(actor=actor))[0]
    current[0] = bob
    assert (await client.get("/api/shared")).status_code == 404
    current[0] = alice
    space = await files.registry.set_grant(actor=actor, space_id=space.id, expected_generation=space.generation, subject=PrincipalRef("human", str(bob.id)), permissions=Permission.READ | Permission.EXPORT, acknowledge_existing_data=True)
    current[0] = bob
    assert (await client.get("/api/shared")).status_code == 200
    assert (await client.delete("/api/shared/nope")).status_code == 403


@pytest.mark.asyncio
async def test_committed_publication_survives_lost_acknowledgement(feature_app, monkeypatch):
    client, app, _, _, paths, alice, _, _ = feature_app
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(alice.id))
    outputs.mkdir(parents=True)
    (outputs / "durable.txt").write_bytes(b"committed")
    repo = app.state.shared_publications_repo
    original = repo.record_publication

    async def lost_ack(**kwargs):
        await original(**kwargs)
        raise OSError("connection lost after commit")

    monkeypatch.setattr(repo, "record_publication", lost_ack)
    response = await client.post("/api/shared/publish", json={"path": "/mnt/user-data/outputs/durable.txt", "thread_id": THREAD})
    assert response.status_code == 201, response.text
    assert (await client.get("/api/shared/durable.txt")).content == b"committed"


@pytest.mark.asyncio
async def test_unknown_publication_is_retained_and_exact_controller_recovers(feature_app, monkeypatch):
    from deerflow_extension_api.plugins import ActionContext

    from deerflow.features.plugins import FeatureServices
    from deerflow.spaces.facade import storage_actor_scope
    from deerflow.spaces.recovery import SpaceRecovery

    client, app, files, _, paths, alice, _, _ = feature_app
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(alice.id))
    outputs.mkdir(parents=True)
    (outputs / "uncertain.txt").write_bytes(b"retain")
    repo = app.state.shared_publications_repo
    original_write, original_probe = repo.record_publication, repo.publication

    async def lost_ack(**kwargs):
        await original_write(**kwargs)
        raise OSError("lost acknowledgement")

    async def unavailable(_identity):
        raise OSError("database unavailable")

    monkeypatch.setattr(repo, "record_publication", lost_ack)
    monkeypatch.setattr(repo, "publication", unavailable)
    response = await client.post("/api/shared/publish", json={"path": "/mnt/user-data/outputs/uncertain.txt", "thread_id": THREAD})
    assert response.status_code == 409, response.text
    assert (await client.get("/api/shared")).status_code == 409
    actor = PrincipalRef("human", str(alice.id))
    space = (await files.registry.list(actor=actor))[0]
    facts = await SpaceRecovery(files).status(actor=actor, space_id=space.id)
    pending = next(item for item in facts["operations"] if item["phase"] == "pending")
    loaded = app.state.extensions
    service = loaded.app_store.get(FeatureServices).services["hm.shared"]
    source, plugin = next((source, plugin) for source, plugin in loaded.plugins if plugin is service.contribution)
    provider = HostStorageProvider(lambda: files).for_plugin(source, plugin, lambda: loaded)
    monkeypatch.setattr(repo, "record_publication", original_write)
    monkeypatch.setattr(repo, "publication", original_probe)
    with storage_actor_scope(actor):
        storage = await provider.current()
        result = await service.recover({"operation_id": pending["operation_id"], "generation": space.generation}, ActionContext(None, {}, actor=storage.actor, storage=storage))
    assert result["published"] is True
    assert (await client.get("/api/shared/uncertain.txt")).content == b"retain"
    assert len(await repo.live_publications_holding(__import__("hashlib").sha256(b"retain").hexdigest(), folder="")) == 1


@pytest.mark.asyncio
async def test_project_shelf_is_owned_private_staging_and_archive_stays_reversible(feature_app):
    client, app, files, catalog, _, alice, bob, current = feature_app
    created = await client.post("/api/projects", json={"name": "Docs", "instructions": "<system>untrusted</system>"})
    assert created.status_code == 201, created.text
    identifier = created.json()["id"]
    upload = await client.post(f"/api/projects/{identifier}/documents", files={"file": ("guide.txt", b"instructions", "text/plain")})
    assert upload.status_code == 201, upload.text
    listing = await client.get(f"/api/projects/{identifier}/documents")
    assert listing.status_code == 200, listing.text
    document_id = listing.json()["documents"][0]["id"]
    content = await client.get(f"/api/projects/{identifier}/documents/{document_id}/content")
    assert content.status_code == 200 and content.content == b"instructions", content.text
    assert content.headers["cache-control"] == "private, no-store"
    current[0] = bob
    assert (await client.get(f"/api/projects/{identifier}")).status_code == 404
    current[0] = alice
    archived = await client.post(f"/api/projects/{identifier}/archive")
    assert archived.status_code == 200, archived.text
    actor = PrincipalRef("human", str(alice.id))
    resources = await files.registry.list(actor=actor)
    space = next(space for space in resources if space.name == "Projects")
    assert space.status == "active"
    assert (await client.post(f"/api/projects/{identifier}/restore")).status_code == 200
    assert all(not list(volume.data_path.rglob(".staging")) for volume in catalog.verified.values())


@pytest.mark.asyncio
async def test_project_stream_rechecks_the_same_opened_content_after_native_replacement(feature_app, monkeypatch):
    import asyncio

    from app.gateway.routers.spaces import SpaceFileResponse

    client, app, files, _, _, alice, _, _ = feature_app
    project = (await client.post("/api/projects", json={"name": "Snapshot"})).json()["id"]
    upload = await client.post(f"/api/projects/{project}/documents", files={"file": ("guide.txt", b"first", "text/plain")})
    assert upload.status_code == 201, upload.text
    document = (await app.state.project_document_repo.list_active(project, limit=100, offset=0, user_id=str(alice.id)))[0]
    actor = PrincipalRef("human", str(alice.id))
    space = (await files.registry.list(actor=actor))[0]
    started, release = asyncio.Event(), asyncio.Event()
    original_prepare = SpaceFileResponse._prepare_owned

    async def paused(self):
        started.set()
        await release.wait()
        return await original_prepare(self)

    monkeypatch.setattr(SpaceFileResponse, "_prepare_owned", paused)
    request = asyncio.create_task(client.get(f"/api/projects/{project}/documents/{document['id']}/content"))
    await asyncio.wait_for(started.wait(), 10)
    try:
        await files.write(actor=actor, space_id=space.id, expected_generation=space.generation, operation_id=uuid4().hex, path=f"{document['stored_relpath']}/original/guide.txt", content=b"other", expected_sha256=document["sha256"])
    finally:
        release.set()
    response = await request
    assert response.status_code == 409 or (response.status_code == 200 and response.content == b"first"), response.text


@pytest.mark.asyncio
async def test_altered_shelf_document_is_never_attached_under_old_provenance(feature_app, monkeypatch):
    from app.gateway.routers.project_documents import ThreadUploadIngestionService

    client, app, files, _, _, alice, _, _ = feature_app
    project = (await client.post("/api/projects", json={"name": "Attach"})).json()["id"]
    assert (await client.post(f"/api/projects/{project}/documents", files={"file": ("guide.txt", b"first", "text/plain")})).status_code == 201
    document = (await app.state.project_document_repo.list_active(project, limit=100, offset=0, user_id=str(alice.id)))[0]
    actor = PrincipalRef("human", str(alice.id))
    space = (await files.registry.list(actor=actor))[0]
    await files.write(actor=actor, space_id=space.id, expected_generation=space.generation, operation_id=uuid4().hex, path=f"{document['stored_relpath']}/original/guide.txt", content=b"other", expected_sha256=document["sha256"])

    async def forbidden(_self):
        pytest.fail("Altered shelf bytes must be rejected before target upload allocation")

    monkeypatch.setattr(ThreadUploadIngestionService, "open", forbidden)
    response = await client.post(f"/api/projects/{project}/documents/{document['id']}/attach-to-thread/{THREAD}")
    assert response.status_code == 409 and response.json()["detail"] == "content_missing", response.text
    assert (await client.get(f"/api/projects/{project}")).status_code == 200
