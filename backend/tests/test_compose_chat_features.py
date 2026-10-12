"""Convenience feature copies report quota exhaustion through the common refusal."""

from unittest.mock import AsyncMock

import pytest
from test_storage_feature_routes import THREAD
from test_storage_feature_routes import feature_app as feature_app


@pytest.mark.asyncio
async def test_feature_save_full_maps_to_structured_507_without_leaving_a_pending_write(feature_app, monkeypatch):
    from app.gateway.refusals import refusal_handler
    from deerflow.agent_instances.contract import Refusal
    from deerflow.features.plugins import FeatureServices
    from deerflow.spaces.filesystem import SpaceFull

    client, app, files, _, paths, alice, _, _ = feature_app
    app.add_exception_handler(Refusal, refusal_handler)
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(alice.id))
    outputs.mkdir(parents=True)
    (outputs / "report.txt").write_bytes(b"payload")
    service = app.state.extensions.app_store.get(FeatureServices).services["hm.my-files"]
    monkeypatch.setattr(service.behavior, "execute", AsyncMock(side_effect=SpaceFull("Space is full")))
    response = await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/report.txt"})
    assert response.status_code == 507, response.text
    assert response.json()["reason"] == "files-full"
    assert "Delete files or old conversations" in response.json()["detail"]
    from sqlalchemy import select

    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    async with files.registry._sf() as session:
        assert not (await session.execute(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))).scalars().all()


@pytest.mark.asyncio
@pytest.mark.parametrize("door", ["keep", "share", "promote", "project-list"])
async def test_native_feature_without_user_role_refuses_before_workflow_and_keeps_generic_access(feature_app, door):
    from uuid import uuid4

    from sqlalchemy import delete, select
    from test_compose_native_file_sources import native_request

    from app.gateway.routers import project_thread_files, spaces
    from deerflow.features.resources import StorageFeatureLinkRow
    from deerflow.persistence.spaces.files import SpaceFileOperationRow
    from deerflow.spaces.contract import PrincipalRef

    client, app, files, _, base, alice, _, _ = feature_app
    native_request(app)
    actor = PrincipalRef("human", str(alice.id))
    resource = next(space for space in await files.registry.list(actor=actor) if space.custody.kind == "personal")
    async with files.registry._sf() as session, session.begin():
        await session.execute(delete(StorageFeatureLinkRow).where(StorageFeatureLinkRow.namespace == "hm.user-space", StorageFeatureLinkRow.key == "human:" + actor.subject_id))
    app.include_router(spaces.router)
    app.include_router(project_thread_files.router)
    if door == "keep":
        response = await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/report.txt"})
    elif door == "share":
        response = await client.post("/api/shared/publish", json={"thread_id": THREAD, "path": "/mnt/user-data/outputs/report.txt"})
    elif door == "promote":
        response = await client.post("/api/projects/project-a/documents/from-thread", json={"thread_id": THREAD, "kind": "output", "name": "report.txt"})
    else:
        response = await client.get("/api/projects/project-a/thread-files")
    assert response.status_code == 503, response.text
    assert response.json()["detail"]["reason"] == "space-not-assigned"
    async with files.registry._sf() as session:
        assert not (await session.execute(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))).scalars().all()
    following = await client.put(f"/api/spaces/{resource.id}/content", params={"path": "files/after.txt", "generation": resource.generation, "operation_id": uuid4().hex, "create": "true"}, content=b"still writable")
    assert following.status_code == 200, following.text
    assert not base.sandbox_user_data_dir(THREAD, user_id=actor.subject_id).exists()


@pytest.mark.asyncio
async def test_native_shared_invalid_thread_id_is_rejected_before_storage_admission(feature_app, monkeypatch):
    from app.gateway import chat_storage

    client, app, files, _, _, _, _, _ = feature_app
    app.state.storage_spaces_enabled = True
    admission = AsyncMock(side_effect=AssertionError("Malformed conversation id reached storage"))
    monkeypatch.setattr(chat_storage, "prepare_chat_request", admission)
    response = await client.post("/api/shared/publish", json={"thread_id": "../other", "path": "/mnt/user-data/outputs/report.txt"})
    assert response.status_code == 400, response.text
    admission.assert_not_awaited()
