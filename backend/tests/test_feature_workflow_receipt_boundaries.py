"""First-party Personal and Projects workflows save their receipt before work."""

import json
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from test_storage_feature_routes import THREAD
from test_storage_feature_routes import feature_app as feature_app

from deerflow.extensions import EXTENSION_SNAPSHOT_CONTEXT_KEY
from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.projects.tools import read_project_document
from deerflow.runtime.context_keys import PROJECT_CONTEXT_KEY, STORAGE_PROVIDER_CONTEXT_KEY
from deerflow.spaces.contract import PrincipalRef
from deerflow.spaces.facade import HostStorageProvider, storage_actor_scope
from deerflow.spaces.workflows import WorkflowReceipt


@pytest.mark.asyncio
@pytest.mark.parametrize("saved", [False, True])
async def test_first_party_workflow_boundaries_use_the_host_receipt_before_domain_work(feature_app, monkeypatch, saved):
    client, app, files, _, paths, alice, *_ = feature_app
    actor = PrincipalRef("human", str(alice.id))
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=actor.subject_id)
    outputs.mkdir(parents=True)
    (outputs / "kept.txt").write_bytes(b"one")
    project_reply = await client.post("/api/projects", json={"name": "Documents"})
    assert project_reply.status_code == 201, project_reply.text
    project_id = project_reply.json()["id"]
    documents = f"/api/projects/{project_id}/documents"
    seed = await client.post(documents, files={"file": ("source.txt", b"source", "text/plain")})
    assert seed.status_code == 201, seed.text
    document_id = (await client.get(documents)).json()["documents"][0]["id"]
    runtime = SimpleNamespace(context={"user_id": actor.subject_id, PROJECT_CONTEXT_KEY: {"project_id": project_id}, STORAGE_PROVIDER_CONTEXT_KEY: HostStorageProvider(lambda: files), EXTENSION_SNAPSHOT_CONTEXT_KEY: app.state.extensions})

    async def invoke(case):
        if case == "keep":
            return await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/kept.txt"})
        if case == "remove":
            return await client.delete("/api/files/kept.txt")
        if case == "upload":
            return await client.post(documents, files={"file": ("more.txt", b"two", "text/plain")})
        if case == "trash":
            return await client.get("/api/trash/documents")
        with storage_actor_scope(actor):
            return json.loads(await read_project_document.coroutine(runtime, document_id))

    save = WorkflowReceipt.save

    def cut(receipt):
        if saved:
            save(receipt)
        raise RuntimeError("interrupted before the workflow starts")

    for case in ("keep", "remove", "upload", "trash", "tool"):
        with monkeypatch.context() as patch:
            patch.setattr(WorkflowReceipt, "save", cut)
            failed = await invoke(case)
            if case == "tool":
                assert "error" in failed, failed
            else:
                assert failed.status_code == 409, failed.text
        async with files.registry._sf() as session:
            row = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))).one()
            assert row.actor_id == actor.subject_id
            if case == "tool":
                assert row.request["operation"] == "read-document"
        assert await files.settle_interrupted() == 1
        async with files.registry._sf() as session:
            terminal = await session.get(SpaceFileOperationRow, (row.space_id, row.operation_id))
            assert terminal.phase == ("complete" if saved else "failed")
            assert terminal.result["resolution"] == "host-settled"
        retry = await invoke(case)
        if case == "tool":
            assert retry["content"] == "source", retry
        else:
            assert retry.is_success, retry.text
