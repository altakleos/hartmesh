"""The host recovers real native publish intents at individual writer boundaries."""

import errno

import pytest
from sqlalchemy import select
from test_compose_native_file_sources import native_request
from test_storage_feature_routes import feature_app as feature_app

from deerflow.persistence.spaces.files import SpaceFileOperationRow


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("cut", "phase"),
    [("full-after-folder", "failed"), ("interrupted-folder-cleanup", "failed"), ("before-name", "failed"), ("before-identity", "pending"), ("after-identity", "complete"), ("after-identity-removed", "failed")],
)
async def test_real_writer_leaves_only_its_provable_publication_for_the_reader(feature_app, monkeypatch, tmp_path, cut, phase):
    from app.gateway import native_shared_publications
    from app.gateway.chat_files import QualifiedUploadStage
    from deerflow.features import publications
    from deerflow.spaces.workflows import WorkflowReceipt

    client, app, files, catalog, _, alice, *_ = feature_app
    native_request(app)
    user = next(volume for volume in catalog.verified.values() if volume.spec.kind == "user")
    company = next(volume for volume in catalog.verified.values() if volume.spec.kind == "company")
    name = "résumé.tar.gz"
    payload = b"from this operation"
    (user.data_path / "files" / name).write_bytes(payload)
    body = {"path": "/mnt/user-data/files/" + name, "folder": "team"}
    if cut == "after-identity":
        # My Files can be opened from a chat, but its publication has no source conversation.
        body["thread_id"] = "selected-chat"
    folder = company.data_path / "shared/team"

    def interrupt(*args, **kwargs):
        if cut in {"full-after-folder", "interrupted-folder-cleanup"}:
            assert folder.is_dir()
            raise OSError(errno.ENOSPC, "simulated exhausted Company filesystem")
        raise OSError("simulated interruption at the publication boundary")

    with monkeypatch.context() as patch:
        if cut in {"before-name", "full-after-folder", "interrupted-folder-cleanup"}:
            patch.setattr(QualifiedUploadStage, "publish", interrupt)
        elif cut == "before-identity":
            patch.setattr(native_shared_publications, "record_fact", interrupt)
        else:
            patch.setattr(publications, "record_fact", interrupt)
        if cut == "interrupted-folder-cleanup":
            from app.gateway.native_directory_changes import NativeDirectoryChanges

            def stop_cleanup(self):
                raise OSError("interrupted before the new directory was removed")

            patch.setattr(NativeDirectoryChanges, "rollback", stop_cleanup)
        response = await client.post("/api/shared/publish", json=body)
        assert response.status_code == (507 if cut == "full-after-folder" else 409), response.text
    if cut == "full-after-folder":
        async with files.registry._sf() as session:
            rejected = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.request["operation"].as_string() == "publish"))).one()
            assert rejected.phase == "failed" and rejected.result["resolution"] == "rejected-before-effect"
        assert not folder.exists()
        assert (await client.post("/api/shared/publish", json=body)).status_code == 201
        return
    if cut == "interrupted-folder-cleanup":
        assert folder.is_dir() and list(folder.iterdir()) == []
        listing = await client.get("/api/shared")
        assert listing.status_code == 200 and listing.json()["files"] == []
    async with files.registry._sf() as session:
        operation = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))).one()
    assert operation.actor_id == str(alice.id) and operation.request["source"] == body
    receipt = WorkflowReceipt(company, operation.operation_id, operation.request)
    receipt.load()
    target = folder / name
    assert target.exists() is (cut not in {"before-name", "interrupted-folder-cleanup"})
    if cut.startswith("after-identity"):
        assert receipt.value["facts"] == {"publication_identity": {"inode": target.stat().st_ino, "device": target.stat().st_dev}}
    else:
        assert receipt.value["facts"] == {}
    if cut == "after-identity-removed":
        target.unlink()

    assert await files.settle_interrupted() == int(phase != "pending")
    async with files.registry._sf() as session:
        row = await session.get(SpaceFileOperationRow, (operation.space_id, operation.operation_id))
        assert row.phase == phase
    record = await app.state.shared_publications_repo.live_publication("team/" + name)
    if phase == "complete":
        assert record["from_path"] == body["path"] and record["published_by"] == str(alice.id)
        assert record["from_thread_id"] is None
        assert target.read_bytes() == payload
    else:
        assert record is None
    if phase == "pending":
        assert target.read_bytes() == payload
        assert (await client.delete("/api/shared/team/" + name)).status_code == 409
        from types import SimpleNamespace

        from app.gateway.agent_operator_services import OperatorCommands

        adapter = SimpleNamespace(host_id="current-daemon", _same_host=lambda: None)
        command = OperatorCommands(files=files, agents=None, repository=None, runs=None, config=None, adapter=adapter, home=tmp_path)
        decision = await command.run("settle", space_id=operation.space_id, operation_id=operation.operation_id, discard=True)
        assert decision["verdict"] == "discarded" and target.read_bytes() == payload
        assert decision["publication_matches"] == {name: False}
        assert (await client.delete("/api/shared/team/" + name)).status_code == 200
        assert not target.exists()
    else:
        retry = await client.post("/api/shared/publish", json=body)
        assert retry.status_code == (200 if phase == "complete" else 201), retry.text
        assert target.read_bytes() == payload


@pytest.mark.asyncio
async def test_maintenance_skips_active_shared_reader_then_finishes(feature_app, monkeypatch):
    import asyncio
    import threading

    from test_shared_publication_host_transaction import seed_publication

    from deerflow.files import shared

    client, _, files, *_ = feature_app
    await seed_publication(feature_app)
    entered, release = threading.Event(), threading.Event()
    original = shared.list_shared_files

    def read():
        entered.set()
        assert release.wait(15)
        return original()

    monkeypatch.setattr(shared, "list_shared_files", read)
    request = asyncio.create_task(client.get("/api/shared"))
    try:
        assert await asyncio.to_thread(entered.wait, 8)
        assert await files.settle_interrupted() == 0
    finally:
        release.set()
        reply = await request
    assert reply.status_code == 200, reply.text
    assert await files.settle_interrupted() == 1
