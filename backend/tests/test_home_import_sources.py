"""Check imported bytes and provenance through the installed storage and conversation services."""

import hashlib
from contextlib import AsyncExitStack
from types import SimpleNamespace

import httpx
import pytest
from _storage_spaces_test_support import ALICE, operation
from deerflow_extension_api import ExtensionRuntimeDeps
from fastapi import Request
from sqlalchemy import select
from test_agent_file_http import file_app
from test_agent_files import backing_root
from test_agent_instances import create
from test_agent_instances import instances as instances
from test_compose_native_file_sources import native_request

from app.gateway.conversation_file_import import SharedPublicationSource
from app.gateway.routers import agent_home, conversation_files
from deerflow.extensions.loader import load_extensions
from deerflow.features.plugins import with_default_features
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.shared_publications.model import SharedPublicationRow
from deerflow.persistence.shared_publications.sql import SharedPublicationRepository
from deerflow.persistence.spaces.files import SpaceBackingRow, SpaceFileOperationRow
from deerflow.runtime.user_context import bind_authenticated_user
from deerflow.spaces.contract import Custody, MutationMode, Permission
from deerflow.spaces.facade import HostStorageProvider


@pytest.mark.asyncio
async def test_shared_and_represented_ordinary_files_keep_only_their_immediate_source(instances, monkeypatch):
    app, agents, first, authority, sf = await file_app(instances, monkeypatch)
    files, catalog = instances[1], instances[3]
    async with sf.kw["bind"].begin() as connection:
        for model in (StorageFeatureLinkRow, SharedPublicationRow):
            await connection.run_sync(model.__table__.create)
    installed, diagnostics = load_extensions(with_default_features([]))
    assert not diagnostics
    monkeypatch.setattr(agent_home, "get_auth_config", lambda: SimpleNamespace(jwt_secret="isolated fixture key"))
    app.state.extensions = installed
    publications = app.state.shared_publications_repo = SharedPublicationRepository(sf)
    app.include_router(conversation_files.router)
    app.dependency_overrides[conversation_files.get_config] = lambda: SimpleNamespace(uploads={})
    async with AsyncExitStack() as stack:
        for _, service in installed.services:
            await service.start(ExtensionRuntimeDeps(app_store=installed.app_store, session_factory=sf, storage=HostStorageProvider(lambda: files)))
            stack.push_async_callback(service.stop)
        second = await create(agents)
        origin = (await authority.create(actor=ALICE, instance_id=first.id, creation_id=operation()))["thread_id"]
        target = (await authority.create(actor=ALICE, instance_id=second.id, creation_id=operation()))["thread_id"]
        personal_chat = (await app.state.thread_store.create(operation(), user_id=ALICE.subject_id))["thread_id"]
        user = await files.assign(kind="user", actor=ALICE, name="Files", custody=Custody.personal(ALICE), mode=MutationMode.NATIVE)
        company = await files.registry.host_create(name="Company", custody=Custody.company(), mode=MutationMode.NATIVE, feature=None, kind="company")
        await files.registry.host_grant(company, ALICE, Permission.READ | Permission.EXPORT)
        native_request(app)

        async def storage_root(space_id):
            async with sf() as session:
                slot_id = await session.scalar(select(SpaceBackingRow.slot_id).where(SpaceBackingRow.space_id == space_id))
            return catalog.verified[slot_id].data_path

        shared_root, user_root = await storage_root(company), await storage_root(user)
        target_root, origin_root = await backing_root(instances, second), await backing_root(instances, first)

        async def record_delivery(thread, path):
            run_id = operation()
            await app.state.run_store.put(run_id, thread_id=thread, user_id=ALICE.subject_id, status="success", created_at="2026-01-02T00:00:00+00:00")
            with bind_authenticated_user(ALICE.subject_id):
                await app.state.run_event_store.put(thread_id=thread, run_id=run_id, event_type="run.delivery", category="outputs", content={"presented_files": [path]})
            return {"source_thread_id": thread, "source_run_id": run_id, "path": path}

        @app.post("/test/publications/{operation_id}")
        async def import_shared(request: Request, operation_id: str, source: SharedPublicationSource):
            return await agent_home.import_into_home(request=request, instance_id=second.id, thread_id=target, operation_id=operation_id, source=source)

        published = b"Published meeting dates"
        publication_id = operation()
        shared_file = shared_root / "shared" / "dates.txt"
        shared_file.write_bytes(published)
        digest = hashlib.sha256(published).hexdigest()
        await publications.record_publication(publication_id=publication_id, path="dates.txt", size=len(published), sha256=digest, published_by=ALICE.subject_id, from_thread_id=None, from_path=None)
        selected = {"publication_id": publication_id, "sha256": digest}
        client = await stack.enter_async_context(httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", cookies={"access_token": "fixture"}))
        result = await client.post("/test/publications/" + operation(), json=selected)
        assert result.status_code == 200, result.text
        assert (target_root / "uploads/dates.txt").read_bytes() == published
        shared_file.write_bytes(b"Changed meeting dates")
        for state in ("replaced", "removed"):
            if state == "removed":
                await publications.record_removal(publication_id, removed_by=ALICE.subject_id)
            refused = await client.post("/test/publications/" + operation(), json=selected)
            assert refused.status_code == 409 and refused.json()["detail"]["code"] == "no-longer-shared", refused.text

        # An inspecting person can copy into a private conversation. Its later
        # presentation is a new source with that conversation's audience.
        data = b"Summary prepared by the first agent"
        output = "summary.txt"
        (origin_root / "outputs").mkdir(exist_ok=True)
        (origin_root / "outputs" / output).write_bytes(data)
        virtual = "/mnt/user-data/outputs/" + output
        from_agent = await record_delivery(origin, virtual)
        first_copy = await client.post(f"/api/threads/{personal_chat}/uploads/from-conversation", json=from_agent)
        assert first_copy.status_code == 200, first_copy.text
        personal_data = user_root / "conversations" / personal_chat / "user-data"
        original = (personal_data / "uploads" / output).read_bytes()
        assert original == data
        (personal_data / "outputs").mkdir(exist_ok=True)
        (personal_data / "outputs" / output).write_bytes(original)
        from_person = await record_delivery(personal_chat, virtual)
        final = await client.post(f"/api/agent-instances/{second.id}/conversations/{target}/files/from-conversation", params={"operation_id": operation()}, json={**from_person, "acknowledge_disclosure": True})
        assert final.status_code == 201, final.text
        assert (target_root / "uploads" / output).read_bytes() == data
        async with sf() as session:
            rows = list(await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == second.home_id)))
            assert {row.request["path"]: (row.phase, row.request.get("source")) for row in rows} == {
                "uploads/dates.txt": ("complete", {"kind": "shared_publication", "publication_id": publication_id}),
                "uploads/summary.txt": ("complete", {"kind": "own_conversation"}),
            }
            assert len(rows) == 2
            for row in rows:
                assert first.id not in str(row.request) and origin not in str(row.request) and personal_chat not in str(row.request)
