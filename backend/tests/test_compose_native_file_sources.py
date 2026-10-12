"""Feature source admission borrows workflow SQL and never invents chat roots."""

import pytest
from starlette.requests import Request
from test_storage_feature_routes import feature_app as feature_app

from app.gateway.auth_disabled import AUTH_SOURCE_SESSION
from deerflow.spaces.contract import Permission, PrincipalRef, SpaceDenied
from deerflow.spaces.facade import storage_actor_scope


def native_request(app):
    from types import SimpleNamespace

    from app.gateway.refusals import refusal_handler
    from deerflow.agent_instances.contract import Refusal

    # The generic SQL fixture has no slot kinds until native roles are used.
    for volume in app.state.storage_spaces.catalog.verified.values():
        if (volume.data_path / "conversations").is_dir():
            volume.spec.kind = "user"
        elif (volume.data_path / "shared").is_dir():
            volume.spec.kind = "company"
    app.state.storage_spaces_enabled = True
    app.state.agent_execution = SimpleNamespace(storage_reason=None, fit=None)
    app.add_exception_handler(Refusal, refusal_handler)
    request = Request({"type": "http", "headers": [], "app": app})
    request.state.auth_source = AUTH_SOURCE_SESSION
    return request


@pytest.mark.asyncio
async def test_native_source_role_admits_without_a_conversation_or_legacy_files(feature_app):
    from app.gateway.native_file_sources import NativeFileSources

    _, app, files, _, base, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        assert sources.actor == actor
        assert sources.user is not None
        assert sources.requests == {sources.space_id: (Permission.READ | Permission.EXPORT, sources.generation)}
        assert (await files.registry.get(actor=actor, space_id=sources.space_id)).custody.principal == actor
    assert not base.user_files_dir(actor.subject_id).exists()
    assert not (base.base_dir / "users").exists()


@pytest.mark.asyncio
async def test_native_source_guard_uses_only_the_borrowed_workflow_session(feature_app, monkeypatch):
    from app.gateway.native_file_sources import NativeFileSources

    _, app, files, catalog, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        async with files.registry.admitted(actor=actor, requests=sources.requests) as (session, rows):

            def forbidden(*_args, **_kwargs):
                raise AssertionError("Nested SQL or filesystem qualification inside a borrowed source guard")

            with monkeypatch.context() as patch:
                patch.setattr(files.registry, "_sf", forbidden)
                patch.setattr(catalog, "verify", forbidden)
                await sources.guard(session, rows)
                with pytest.raises(SpaceDenied):
                    await sources.guard(session, {})


@pytest.mark.asyncio
async def test_native_source_without_an_assigned_user_role_refuses_without_legacy_fallback(feature_app):
    from fastapi import HTTPException
    from sqlalchemy import delete

    from app.gateway.native_file_sources import NativeFileSources
    from deerflow.features.resources import StorageFeatureLinkRow

    _, app, files, _, base, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    async with files.registry._sf() as session, session.begin():
        await session.execute(delete(StorageFeatureLinkRow).where(StorageFeatureLinkRow.namespace == "hm.user-space", StorageFeatureLinkRow.key == "human:" + actor.subject_id))
    with storage_actor_scope(actor), pytest.raises(HTTPException) as refused:
        await NativeFileSources.prepare(native_request(app), actor)
    assert refused.value.status_code == 503
    assert refused.value.detail["reason"] == "space-not-assigned"
    assert not base.user_files_dir(actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_file_source_pins_bytes_and_closes_owned_descriptors(feature_app):
    import os

    from app.gateway.native_file_sources import NativeFileSources
    from deerflow.files.store import digest_and_stat, open_regular_source
    from deerflow.utils.file_io import run_file_io

    _, app, _, _, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        original = sources.user.data_path / "files/report.txt"
        original.write_bytes(b"pinned")
        async with sources.open_file("files/report.txt") as source:
            assert (await run_file_io(digest_and_stat, source))[1].st_size == 6
            original.parent.rename(sources.user.data_path / "retired-files")
            original.parent.mkdir()
            original.write_bytes(b"foreign")
            descriptor = await run_file_io(open_regular_source, source)
            try:
                assert await run_file_io(os.read, descriptor, 64) == b"pinned"
            finally:
                await run_file_io(os.close, descriptor)
            held = source.descriptor
        with pytest.raises(OSError):
            os.fstat(held)


@pytest.mark.asyncio
async def test_native_file_source_refuses_links_without_reading_outside_the_role(feature_app, tmp_path):
    from app.gateway.native_file_sources import NativeFileSources
    from deerflow.spaces.filesystem import UnsafeSpacePath

    _, app, _, _, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"private elsewhere")
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        (sources.user.data_path / "files/link.txt").symlink_to(outside)
        with pytest.raises(UnsafeSpacePath):
            async with sources.open_file("files/link.txt"):
                pytest.fail("A source outside the qualified role was admitted")


@pytest.mark.asyncio
async def test_native_copy_rechecks_before_publication_and_keeps_existing_names(feature_app):
    import hashlib
    from unittest.mock import AsyncMock

    from app.gateway.native_file_sources import NativeFileSources

    _, app, _, _, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        root = sources.user.data_path / "files"
        (root / "input.txt").write_bytes(b"new bytes")
        (root / "report.txt").write_bytes(b"keep existing")
        admission = AsyncMock()
        async with sources.open_file("files/input.txt") as source:
            copied = await sources.copy_to(source, sources.user, root_relative="files", name="report.txt", admission=admission)
        assert copied.path == "report_1.txt"
        assert copied.sha256 == hashlib.sha256(b"new bytes").hexdigest()
        assert (root / copied.path).read_bytes() == b"new bytes"
        assert (root / "report.txt").read_bytes() == b"keep existing"
        assert admission.await_count >= 2
        assert not list(sources.user.control_path.glob("chat-upload-*"))


@pytest.mark.asyncio
async def test_native_copy_refused_after_staging_never_publishes_and_cleans_private_bytes(feature_app):
    from unittest.mock import AsyncMock

    from app.gateway.native_file_sources import NativeFileSources

    _, app, _, _, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        root = sources.user.data_path / "files"
        (root / "input.txt").write_bytes(b"private source")
        admission = AsyncMock(side_effect=[None, SpaceDenied("Source was revoked")])
        async with sources.open_file("files/input.txt") as source:
            with pytest.raises(SpaceDenied):
                await sources.copy_to(source, sources.user, root_relative="files", name="report.txt", admission=admission)
        assert not (root / "report.txt").exists()
        assert not list(sources.user.control_path.glob("chat-upload-*"))


@pytest.mark.asyncio
async def test_native_sources_allow_read_export_without_write(feature_app):
    from app.gateway.native_file_sources import NativeFileSources

    _, app, files, _, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    await files.registry.host_grant(user.id, actor, Permission.READ | Permission.EXPORT)
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        async with files.registry.admitted(actor=actor, requests=sources.requests) as (session, rows):
            await sources.guard(session, rows)


@pytest.mark.asyncio
async def test_native_sources_require_export_permission(feature_app):
    from fastapi import HTTPException

    from app.gateway.native_file_sources import NativeFileSources

    _, app, files, _, base, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    await files.registry.host_grant(user.id, actor, Permission.READ)
    with storage_actor_scope(actor), pytest.raises(HTTPException) as refused:
        await NativeFileSources.prepare(native_request(app), actor)
    assert refused.value.status_code == 503
    assert not base.user_files_dir(actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_sources_preserve_identity_refusal_after_held_verification(feature_app, monkeypatch):
    import asyncio
    import threading

    from app.gateway.native_file_sources import NativeFileSources
    from deerflow.spaces.contract import InvalidPrincipal

    _, app, files, catalog, base, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    entered, release = threading.Event(), threading.Event()
    original_verify, original_actor = catalog.verify, files.registry._actor
    disabled = False

    def held_verify(*args, **kwargs):
        entered.set()
        if not release.wait(5):
            raise AssertionError("Verification was not released")
        return original_verify(*args, **kwargs)

    async def current_actor(reference, **kwargs):
        if disabled:
            raise InvalidPrincipal("The human account was disabled")
        return await original_actor(reference, **kwargs)

    monkeypatch.setattr(catalog, "verify", held_verify)
    monkeypatch.setattr(files.registry, "_actor", current_actor)
    with storage_actor_scope(actor):
        task = asyncio.create_task(NativeFileSources.prepare(native_request(app), actor))
        try:
            async with asyncio.timeout(5):
                while not entered.is_set():
                    await asyncio.sleep(0.01)
            disabled = True
            release.set()
            with pytest.raises(InvalidPrincipal):
                await task
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
    assert not base.user_files_dir(actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_keep_and_share_copy_user_conversation_without_legacy_roots(feature_app):
    from unittest.mock import AsyncMock

    from test_storage_feature_routes import THREAD

    client, app, files, catalog, base, alice, _, _ = feature_app
    native_request(app)
    app.state.thread_store.get = AsyncMock(return_value={"id": THREAD, "user_id": str(alice.id)})
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    # Select Alice's actual binding rather than relying on fixture slot order.
    from deerflow.persistence.spaces.files import SpaceBackingRow

    async with files.registry._sf() as session:
        binding = await session.get(SpaceBackingRow, user.backing_handle)
    volume = catalog.verified[binding.slot_id]
    outputs = volume.data_path / f"conversations/{THREAD}/user-data/outputs"
    outputs.mkdir(parents=True)
    (outputs / "native.txt").write_bytes(b"native conversation")
    kept = await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/native.txt"})
    assert kept.status_code == 201, kept.text
    assert (volume.data_path / "files/native.txt").read_bytes() == b"native conversation"
    published = await client.post("/api/shared/publish", json={"path": kept.json()["virtual_path"]})
    assert published.status_code == 201, published.text
    assert published.json()["from_thread_id"] is None
    assert (await client.get("/api/shared/native.txt")).content == b"native conversation"
    duplicate = await client.post("/api/shared/publish", json={"path": "/mnt/user-data/outputs/native.txt", "thread_id": THREAD})
    assert duplicate.status_code == 200, duplicate.text
    assert duplicate.json()["publication_id"] == published.json()["publication_id"]
    assert not base.user_files_dir(actor.subject_id).exists()
    assert not base.sandbox_outputs_dir(THREAD, user_id=actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_share_from_my_files_requires_no_write_or_conversation(feature_app):
    from deerflow.persistence.spaces.files import SpaceBackingRow

    client, app, files, catalog, base, alice, _, _ = feature_app
    native_request(app)
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    async with files.registry._sf() as session:
        binding = await session.get(SpaceBackingRow, user.backing_handle)
    (catalog.verified[binding.slot_id].data_path / "files/read-only.txt").write_bytes(b"export allowed")
    await files.registry.host_grant(user.id, actor, Permission.READ | Permission.EXPORT)
    published = await client.post("/api/shared/publish", json={"path": "/mnt/user-data/files/read-only.txt"})
    assert published.status_code == 201, published.text
    assert published.json()["from_thread_id"] is None
    assert (await client.get("/api/shared/read-only.txt")).content == b"export allowed"
    assert not base.user_files_dir(actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_project_thread_files_reads_only_qualified_conversation_leaves(feature_app):
    from unittest.mock import AsyncMock

    from test_storage_feature_routes import THREAD

    from app.gateway.routers import project_thread_files
    from deerflow.persistence.spaces.files import SpaceBackingRow

    client, app, files, catalog, base, alice, _, _ = feature_app
    native_request(app)
    app.include_router(project_thread_files.router)
    project = await client.post("/api/projects", json={"name": "Native sources"})
    assert project.status_code == 201, project.text
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    async with files.registry._sf() as session:
        binding = await session.get(SpaceBackingRow, user.backing_handle)
    uploads = catalog.verified[binding.slot_id].data_path / f"conversations/{THREAD}/user-data/uploads"
    uploads.mkdir(parents=True)
    (uploads / "first.txt").write_bytes(b"first")
    (uploads / "second.txt").write_bytes(b"second")
    (uploads / "foreign.txt").symlink_to(base.base_dir / "private.txt")
    app.state.thread_store.search = AsyncMock(return_value=[{"thread_id": THREAD, "updated_at": "2026-10-11T00:00:00Z"}])
    response = await client.get(f"/api/projects/{project.json()['id']}/thread-files", params={"file_limit": 1})
    assert response.status_code == 200, response.text
    group = response.json()["groups"][0]
    assert group["files"][0]["name"] == "first.txt"
    assert group["files"][0]["size_bytes"] == 5
    assert group["truncated"] is True
    assert response.json()["truncated"] is True
    assert not base.sandbox_uploads_dir(THREAD, user_id=actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_keep_capacity_refusal_settles_507_without_pending_intent(feature_app, monkeypatch):
    from sqlalchemy import select
    from test_storage_feature_routes import THREAD

    from app.gateway.chat_files import QualifiedUploadStage
    from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
    from deerflow.spaces.chat_paths import ChatFilesFull

    client, app, files, catalog, base, alice, _, _ = feature_app
    native_request(app)
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    async with files.registry._sf() as session:
        binding = await session.get(SpaceBackingRow, user.backing_handle)
    volume = catalog.verified[binding.slot_id]
    outputs = volume.data_path / f"conversations/{THREAD}/user-data/outputs"
    outputs.mkdir(parents=True)
    (outputs / "full.txt").write_bytes(b"no room")

    def full(*_args, **_kwargs):
        raise ChatFilesFull()

    monkeypatch.setattr(QualifiedUploadStage, "check_capacity", full)
    response = await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/full.txt"})
    assert response.status_code == 507, response.text
    async with files.registry._sf() as session:
        pending = (await session.execute(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))).scalars().all()
    assert pending == []
    assert not (volume.data_path / "files/full.txt").exists()
    assert not list(volume.control_path.glob("chat-upload-*"))
    assert not base.user_files_dir(actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_copy_refuses_replaced_source_root_before_publication(feature_app):
    from app.gateway.native_file_sources import NativeFileSources
    from deerflow.spaces.backings import BackingUnavailable

    _, app, _, _, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        root = sources.user.data_path
        (root / "files/input.txt").write_bytes(b"old root bytes")
        calls = 0

        async def admission():
            nonlocal calls
            calls += 1
            if calls == 1:
                root.rename(root.with_name("retired-data"))
                root.mkdir()
                (root / "files").mkdir()

        async with sources.open_file("files/input.txt") as source:
            with pytest.raises(BackingUnavailable):
                await sources.copy_to(source, sources.user, root_relative="files", name="report.txt", admission=admission)
        assert not (root / "files/report.txt").exists()
        assert not list(sources.user.control_path.glob("chat-upload-*"))


@pytest.mark.asyncio
async def test_native_run_zip_download_contains_only_presented_qualified_outputs(feature_app):
    import io
    import zipfile

    from test_storage_feature_routes import THREAD

    from app.gateway.routers import thread_runs
    from deerflow.persistence.spaces.files import SpaceBackingRow
    from deerflow.runtime.events.store.memory import MemoryRunEventStore
    from deerflow.runtime.runs.manager import RunManager
    from deerflow.runtime.runs.store.memory import MemoryRunStore

    client, app, files, catalog, base, alice, _, _ = feature_app
    native_request(app)
    app.include_router(thread_runs.router)
    app.state.run_store = MemoryRunStore()
    app.state.run_event_store = MemoryRunEventStore()
    app.state.run_manager = RunManager(store=app.state.run_store)
    await app.state.run_store.put("native-zip", thread_id=THREAD, user_id=str(alice.id), status="success")
    virtual = "/mnt/user-data/outputs/report.txt"
    await app.state.run_event_store.put(thread_id=THREAD, run_id="native-zip", event_type="run.delivery", category="outputs", content={"presented": 1, "paths": [virtual], "by_tool": {"present_files": [virtual]}})
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    async with files.registry._sf() as session:
        binding = await session.get(SpaceBackingRow, user.backing_handle)
    outputs = catalog.verified[binding.slot_id].data_path / f"conversations/{THREAD}/user-data/outputs"
    outputs.mkdir(parents=True)
    (outputs / "report.txt").write_bytes(b"native ZIP bytes")
    (outputs / "not-presented.txt").write_bytes(b"excluded")
    response = await client.post(f"/api/threads/{THREAD}/runs/native-zip/artifacts/archive")
    assert response.status_code == 200, response.text
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert archive.namelist() == ["report.txt"]
        assert archive.read("report.txt") == b"native ZIP bytes"
    assert response.headers["cache-control"] == "private, no-store"
    assert not base.sandbox_outputs_dir(THREAD, user_id=actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_skill_install_uses_qualified_archive_and_owned_private_skill_storage(feature_app, monkeypatch, tmp_path):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from test_skills_custom_router import _make_skill_archive_bytes
    from test_storage_feature_routes import THREAD

    from app.gateway.deps import get_config
    from app.gateway.routers import skills
    from deerflow.config.authorization_config import AuthorizationConfig
    from deerflow.persistence.spaces.files import SpaceBackingRow
    from deerflow.runtime.customer_administration import CustomerAdministrationPolicy
    from deerflow.skills.security_scanner import ScanResult
    from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage

    client, app, files, catalog, base, alice, _, _ = feature_app
    native_request(app)
    app.include_router(skills.router)
    app.state.customer_administration_policy = CustomerAdministrationPolicy(local_skill_management=True)
    skill_root = tmp_path / "private-skills"
    storage = UserScopedSkillStorage(str(alice.id), host_path=str(skill_root))
    config = SimpleNamespace(
        skills=SimpleNamespace(get_skills_path=lambda: skill_root, container_path="/mnt/skills", use="deerflow.skills.storage.local_skill_storage:LocalSkillStorage"),
        skill_evolution=SimpleNamespace(enabled=True, moderation_model_name=None),
        authorization=AuthorizationConfig(enabled=False),
    )
    app.dependency_overrides[get_config] = lambda: config
    monkeypatch.setattr(skills, "_get_user_skill_storage", lambda _config: storage)
    monkeypatch.setattr(skills, "refresh_user_skills_system_prompt_cache_async", AsyncMock())
    scanner = AsyncMock(return_value=ScanResult(decision="allow", reason="test scan"))
    monkeypatch.setattr("deerflow.skills.installer.scan_skill_content", scanner)
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    async with files.registry._sf() as session:
        binding = await session.get(SpaceBackingRow, user.backing_handle)
    outputs = catalog.verified[binding.slot_id].data_path / f"conversations/{THREAD}/user-data/outputs"
    outputs.mkdir(parents=True)
    (outputs / "native-skill.skill").write_bytes(_make_skill_archive_bytes("native-skill"))
    response = await client.post("/api/skills/install", json={"thread_id": THREAD, "path": "/mnt/user-data/outputs/native-skill.skill"})
    assert response.status_code == 200, response.text
    assert response.json()["skill_name"] == "native-skill"
    assert (base.base_dir / "users" / actor.subject_id / "skills/custom/native-skill/SKILL.md").is_file()
    assert {call.kwargs["location"] for call in scanner.await_args_list} == {"native-skill/SKILL.md", "native-skill/references/guide.md"}
    assert not base.sandbox_outputs_dir(THREAD, user_id=actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_browser_http_screenshot_publishes_inside_user_volume(feature_app, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from test_storage_feature_routes import THREAD

    from app.gateway.routers import browser
    from deerflow.community.browser_automation import tools
    from deerflow.persistence.spaces.files import SpaceBackingRow

    client, app, files, catalog, base, alice, _, _ = feature_app
    native_request(app)
    app.include_router(browser.router)
    app.state.thread_store.get = AsyncMock(return_value={"thread_id": THREAD, "user_id": str(alice.id)})
    session = MagicMock()
    session.navigate = AsyncMock(return_value=SimpleNamespace(url="https://example.com/", title="Test page"))
    session.screenshot_bytes = AsyncMock(return_value=b"JPEG screenshot")
    manager = MagicMock()
    manager.acquire_session.return_value.__enter__.return_value = session
    monkeypatch.setattr(browser, "_browser_tools_enabled", lambda: True)
    monkeypatch.setattr(tools, "get_browser_session_manager", lambda: manager)
    monkeypatch.setattr(tools, "_get_tool_config", lambda _name: {})
    monkeypatch.setattr(tools, "_validate_url", lambda *_args: None)
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    async with files.registry._sf() as session_db:
        binding = await session_db.get(SpaceBackingRow, user.backing_handle)
    outputs = catalog.verified[binding.slot_id].data_path / f"conversations/{THREAD}/user-data/outputs"
    response = await client.post(f"/api/threads/{THREAD}/browser/navigate", json={"url": "https://example.com/"})
    assert response.status_code == 200, response.text
    screenshot = response.json()["screenshot"]
    assert screenshot.startswith("/mnt/user-data/outputs/.browser-frames/")
    assert (outputs / screenshot.removeprefix("/mnt/user-data/outputs/")).read_bytes() == b"JPEG screenshot"
    assert not base.sandbox_outputs_dir(THREAD, user_id=actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_project_promotion_preserves_dedup_and_source_provenance(feature_app):
    from test_storage_feature_routes import THREAD

    from deerflow.persistence.spaces.files import SpaceBackingRow

    client, app, files, catalog, base, alice, _, _ = feature_app
    native_request(app)
    project = await client.post("/api/projects", json={"name": "Native shelf"})
    assert project.status_code == 201, project.text
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    async with files.registry._sf() as session:
        binding = await session.get(SpaceBackingRow, user.backing_handle)
    volume = catalog.verified[binding.slot_id]
    outputs = volume.data_path / f"conversations/{THREAD}/user-data/outputs"
    outputs.mkdir(parents=True)
    (outputs / "guide.txt").write_bytes(b"qualified project guide")
    url = f"/api/projects/{project.json()['id']}/documents/from-thread"
    body = {"thread_id": THREAD, "kind": "output", "name": "guide.txt"}
    promoted = await client.post(url, json=body)
    assert promoted.status_code == 201, promoted.text
    document = promoted.json()["document"]
    assert document["source_thread_id"] == THREAD
    assert document["source_kind"] == "output"
    assert document["source_name"] == "guide.txt"
    duplicate = await client.post(url, json={**body, "shelf_name": "second-name.txt"})
    assert duplicate.status_code == 200, duplicate.text
    assert duplicate.json()["document"]["id"] == document["id"]
    content = await client.get(f"/api/projects/{project.json()['id']}/documents/{document['id']}/content")
    assert content.content == b"qualified project guide", content.text
    assert (outputs / "guide.txt").read_bytes() == b"qualified project guide"
    assert not list(volume.control_path.glob("chat-upload-*"))
    assert not base.sandbox_outputs_dir(THREAD, user_id=actor.subject_id).exists()


@pytest.mark.asyncio
async def test_borrowed_resource_admission_reuses_active_project_session(feature_app, monkeypatch):
    _, _, files, _, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    async with files.registry._sf() as session, session.begin():

        def nested(*_args, **_kwargs):
            pytest.fail("Borrowed resource admission opened another database session")

        monkeypatch.setattr(files.registry, "_sf", nested)
        rows = await files.registry.admit_in_session(session, actor=actor, requests={user.id: (Permission.READ | Permission.WRITE, user.generation)})
        assert rows[user.id][0].id == user.id
        with pytest.raises(SpaceDenied):
            await files.registry.admit_in_session(session, actor=actor, requests={user.id: (Permission.OPERATE, user.generation)})


@pytest.mark.asyncio
@pytest.mark.parametrize("replacement", ["symlink-leaf", "mounted-file"])
async def test_native_project_listing_refuses_unqualified_leaf_or_file_metadata(feature_app, monkeypatch, replacement):
    import os
    from unittest.mock import AsyncMock

    from test_storage_feature_routes import THREAD

    from app.gateway.routers import project_thread_files
    from deerflow.persistence.spaces.files import SpaceBackingRow
    from deerflow.spaces.filesystem import ConfinedFilesystem, UnsafeSpacePath

    client, app, files, catalog, _, alice, _, _ = feature_app
    native_request(app)
    app.include_router(project_thread_files.router)
    project = (await client.post("/api/projects", json={"name": "Confined listing"})).json()["id"]
    actor = PrincipalRef("human", str(alice.id))
    user = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    async with files.registry._sf() as session:
        binding = await session.get(SpaceBackingRow, user.backing_handle)
    root = catalog.verified[binding.slot_id].data_path
    uploads = root / f"conversations/{THREAD}/user-data/uploads"
    uploads.parent.mkdir(parents=True)
    if replacement == "symlink-leaf":
        (root / "files/private.txt").write_bytes(b"another area")
        uploads.symlink_to(root / "files")
    else:
        uploads.mkdir()
        (uploads / "private.txt").write_bytes(b"mounted elsewhere")
        original = ConfinedFilesystem._open

        def cross_mount(self, path, flags, **kwargs):
            if path.endswith("/private.txt") and flags & os.O_PATH:
                raise UnsafeSpacePath("The file crosses a qualified mount")
            return original(self, path, flags, **kwargs)

        monkeypatch.setattr(ConfinedFilesystem, "_open", cross_mount)
    app.state.thread_store.search = AsyncMock(return_value=[{"thread_id": THREAD, "updated_at": "2026-10-11T00:00:00Z"}])
    response = await client.get(f"/api/projects/{project}/thread-files")
    assert response.status_code == 403, response.text
    assert "private.txt" not in response.text


@pytest.mark.asyncio
async def test_native_shared_dedup_refuses_a_replaced_company_root(feature_app, monkeypatch):
    from sqlalchemy import select

    from app.gateway.native_file_sources import NativeFileSources
    from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow

    client, app, files, catalog, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        (sources.user.data_path / "files/dedup.txt").write_bytes(b"same bytes")
    first = await client.post("/api/shared/publish", json={"path": "/mnt/user-data/files/dedup.txt"})
    assert first.status_code == 201, first.text
    company = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "company")
    async with files.registry._sf() as session:
        binding = await session.get(SpaceBackingRow, company.backing_handle)
    root = catalog.verified[binding.slot_id].data_path
    repository = app.state.shared_publications_repo
    original = repository.live_publications_holding

    async def replace_after_query(*args, **kwargs):
        records = await original(*args, **kwargs)
        root.rename(root.with_name("retired-data"))
        (root / "shared").mkdir(parents=True)
        (root / "shared/dedup.txt").write_bytes(b"same bytes")
        return records

    monkeypatch.setattr(repository, "live_publications_holding", replace_after_query)
    response = await client.post("/api/shared/publish", json={"path": "/mnt/user-data/files/dedup.txt"})
    assert response.status_code == 503, response.text
    assert (root / "shared/dedup.txt").read_bytes() == b"same bytes"
    async with files.registry._sf() as session:
        assert not (await session.execute(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))).scalars().all()


@pytest.mark.asyncio
async def test_native_shared_known_rollback_preserves_replacement_with_identical_bytes(feature_app, monkeypatch):
    from app.gateway.native_file_sources import NativeFileSources
    from deerflow.persistence.spaces.files import SpaceBackingRow

    client, app, files, catalog, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
        (sources.user.data_path / "files/rollback.txt").write_bytes(b"same bytes")
    company = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "company")
    async with files.registry._sf() as session:
        binding = await session.get(SpaceBackingRow, company.backing_handle)
    root = catalog.verified[binding.slot_id].data_path / "shared"
    replacement_identity = []

    async def record_failed(**_kwargs):
        target = root / "rollback.txt"
        target.rename(root / "original-copy.txt")
        target.write_bytes(b"same bytes")
        replacement_identity.append(target.stat().st_ino)
        raise OSError("No publication record was committed")

    monkeypatch.setattr(app.state.shared_publications_repo, "record_publication", record_failed)
    response = await client.post("/api/shared/publish", json={"path": "/mnt/user-data/files/rollback.txt"})
    assert response.status_code == 409, response.text
    assert (root / "rollback.txt").read_bytes() == b"same bytes"
    assert (root / "rollback.txt").stat().st_ino == replacement_identity[0]
    assert (root / "original-copy.txt").read_bytes() == b"same bytes"


@pytest.mark.asyncio
@pytest.mark.parametrize("door", ["keep-folder", "project-namespace"])
async def test_native_quota_after_directory_creation_cleans_owned_folders_and_settles_full(feature_app, monkeypatch, door):
    from sqlalchemy import select
    from test_storage_feature_routes import THREAD

    from app.gateway.chat_files import QualifiedUploadStage
    from app.gateway.native_file_sources import NativeFileSources
    from deerflow.persistence.spaces.files import SpaceFileOperationRow
    from deerflow.spaces.chat_paths import ChatFilesFull

    client, app, files, _, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
    outputs = sources.user.data_path / f"conversations/{THREAD}/user-data/outputs"
    outputs.mkdir(parents=True)
    (outputs / "quota.txt").write_bytes(b"owned bytes")
    if door == "project-namespace":
        project = (await client.post("/api/projects", json={"name": "Quota shelf"})).json()["id"]
        first_directory = sources.user.data_path / "projects" / project
        url = f"/api/projects/{project}/documents/from-thread"
        body = {"thread_id": THREAD, "kind": "output", "name": "quota.txt"}
    else:
        existing = sources.user.data_path / "files/existing"
        existing.mkdir()
        (existing / "keep.txt").write_bytes(b"preserve")
        first_directory = existing / "new"
        url = f"/api/threads/{THREAD}/files"
        body = {"path": "/mnt/user-data/outputs/quota.txt", "folder": "existing/new/inner"}
    original = QualifiedUploadStage.check_capacity

    def full_after_mkdir(self, *args, **kwargs):
        if first_directory.exists():
            raise ChatFilesFull()
        return original(self, *args, **kwargs)

    monkeypatch.setattr(QualifiedUploadStage, "check_capacity", full_after_mkdir)
    response = await client.post(url, json=body)
    assert response.status_code == 507, response.text
    assert not first_directory.exists()
    if door == "keep-folder":
        assert (existing / "keep.txt").read_bytes() == b"preserve"
    async with files.registry._sf() as session:
        assert not (await session.execute(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))).scalars().all()
    assert not list(sources.user.control_path.glob("chat-upload-*"))


@pytest.mark.asyncio
async def test_native_agent_browser_tools_publish_fresh_chat_and_ignore_runtime_paths(feature_app, monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    from test_storage_feature_routes import THREAD

    from app.gateway.chat_storage import chat_storage_lifecycle
    from deerflow.community.browser_automation import tools
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    _, app, _, _, base, alice, _, _ = feature_app
    native_request(app)
    app.state.thread_store.get = AsyncMock(return_value={"thread_id": THREAD, "user_id": str(alice.id)})
    session = MagicMock()
    session.screenshot_bytes = AsyncMock(return_value=b"PNG tool bytes")
    manager = MagicMock()
    manager.get_session.return_value = session
    monkeypatch.setattr(tools, "get_browser_session_manager", lambda: manager)
    monkeypatch.setattr(tools, "_get_tool_config", lambda _name: {})
    unqualified = base.base_dir / "unqualified-caller-path"
    runtime = SimpleNamespace(context={"thread_id": THREAD}, state={"thread_data": {"outputs_path": str(unqualified)}})
    user_token = set_current_user(alice)
    try:
        async with chat_storage_lifecycle(app, enabled=True):
            result = await tools.browser_screenshot_tool.coroutine(runtime=runtime, tool_call_id="capture", filename="tool.png")
            assert result.update.get("artifacts") == ["/mnt/user-data/outputs/tool.png"], result.update
            progress = await tools._capture_step_screenshot(runtime, session, "step")
            assert progress.startswith("/mnt/user-data/outputs/.browser-frames/")
    finally:
        reset_current_user(user_token)
    assert not unqualified.exists()
    assert not base.sandbox_outputs_dir(THREAD, user_id=str(alice.id)).exists()


@pytest.mark.asyncio
async def test_native_quota_cleanup_preserves_a_replaced_directory_and_pending_recovery(feature_app, monkeypatch):
    from sqlalchemy import select
    from test_storage_feature_routes import THREAD

    from app.gateway.chat_files import QualifiedUploadStage
    from app.gateway.native_file_sources import NativeFileSources
    from deerflow.persistence.spaces.files import SpaceFileOperationRow
    from deerflow.spaces.chat_paths import ChatFilesFull

    client, app, files, _, _, alice, _, _ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        sources = await NativeFileSources.prepare(native_request(app), actor)
    outputs = sources.user.data_path / f"conversations/{THREAD}/user-data/outputs"
    outputs.mkdir(parents=True)
    (outputs / "quota.txt").write_bytes(b"source")
    target = sources.user.data_path / "files/new"
    original = QualifiedUploadStage.check_capacity
    replacement = []

    def replace_then_full(self, *args, **kwargs):
        if target.exists():
            target.rename(target.with_name("original-new"))
            target.mkdir()
            (target / "foreign.txt").write_bytes(b"another writer's bytes")
            replacement.append(target.stat().st_ino)
            raise ChatFilesFull()
        return original(self, *args, **kwargs)

    monkeypatch.setattr(QualifiedUploadStage, "check_capacity", replace_then_full)
    response = await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/quota.txt", "folder": "new/inner"})
    assert response.status_code == 409, response.text
    assert (target / "foreign.txt").read_bytes() == b"another writer's bytes"
    assert target.stat().st_ino == replacement[0]
    async with files.registry._sf() as session:
        assert (await session.execute(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))).scalars().all()
    assert not list(sources.user.control_path.glob("chat-upload-*"))
    assert not list(sources.user.control_path.glob("native-directory-*"))
