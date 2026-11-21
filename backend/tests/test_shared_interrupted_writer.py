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
    [
        ("full-after-folder", "failed"),
        ("interrupted-folder-cleanup", "failed"),
        ("before-name", "failed"),
        ("during-hash", "failed"),
        ("during-copy", "failed"),
        ("before-identity", "failed"),
        ("after-identity", "failed"),
        ("receipt-file-exists", "failed"),
        ("after-first-collision", "failed"),
        ("after-second-name", "complete"),
        ("after-name", "complete"),
        ("after-name-removed", "failed"),
        ("legacy-empty-receipt", "pending"),
    ],
)
async def test_real_writer_leaves_only_its_provable_publication_for_the_reader(feature_app, monkeypatch, tmp_path, cut, phase):
    from app.gateway import native_shared_publications
    from app.gateway.chat_files import QualifiedUploadStage
    from deerflow.features import publications
    from deerflow.files import shared
    from deerflow.spaces.workflows import WorkflowReceipt

    client, app, files, catalog, _, alice, *_ = feature_app
    from app.gateway.native_file_sources import NativeFileSources
    from deerflow.spaces.contract import PrincipalRef
    from deerflow.spaces.facade import storage_actor_scope

    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        user = (await NativeFileSources.prepare(native_request(app), actor)).user
    company = next(volume for volume in catalog.verified.values() if volume.spec.kind == "company")
    name = "résumé.tar.gz"
    payload = b"from this operation"
    (user.data_path / "files" / name).write_bytes(payload)
    body = {"path": "/mnt/user-data/files/" + name, "folder": "team"}
    if cut == "after-name":
        # My Files can be opened from a chat, but its publication has no source conversation.
        body["thread_id"] = "selected-chat"
    folder = company.data_path / "shared/team"
    collision = cut in {"after-first-collision", "after-second-name"}
    if collision:
        folder.mkdir(parents=True)
        (folder / name).write_bytes(payload)
        foreign_identity = (folder / name).stat()

    def interrupt(*args, **kwargs):
        if cut in {"full-after-folder", "interrupted-folder-cleanup"}:
            assert folder.is_dir()
            raise OSError(errno.ENOSPC, "simulated exhausted Company filesystem")
        raise OSError("simulated interruption at the publication boundary")

    with monkeypatch.context() as patch:
        if cut in {"before-name", "full-after-folder", "interrupted-folder-cleanup"}:
            patch.setattr(QualifiedUploadStage, "publish", interrupt)
        elif cut == "during-hash":
            patch.setattr(shared, "digest_of", interrupt)
        elif cut == "during-copy":
            patch.setattr(QualifiedUploadStage, "write", interrupt)
        elif cut == "before-identity":
            patch.setattr(native_shared_publications, "record_fact", interrupt)
        elif cut == "after-identity":
            patch.setattr(publications, "record_fact", interrupt)
        elif cut == "receipt-file-exists":
            save = WorkflowReceipt.save
            refused = False

            def fail_first_candidate(receipt):
                nonlocal refused
                if not refused and "publication" in receipt.value["facts"]:
                    refused = True
                    raise FileExistsError("private receipt save failed")
                return save(receipt)

            patch.setattr(WorkflowReceipt, "save", fail_first_candidate)
        else:
            publish = QualifiedUploadStage.publish

            def stop_after_link(stage, *args, **kwargs):
                try:
                    publish(stage, *args, **kwargs)
                except FileExistsError:
                    if cut == "after-first-collision":
                        interrupt()
                    raise
                interrupt()

            patch.setattr(QualifiedUploadStage, "publish", stop_after_link)
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
    target = folder / ("résumé_1.tar.gz" if collision else name)
    landed = cut in {"after-name", "after-name-removed", "after-second-name", "legacy-empty-receipt"}
    assert target.exists() is landed
    facts = receipt.value["facts"]
    assert facts["publication_order"] == "fact-before-name"
    if cut in {"during-hash", "during-copy", "before-identity"}:
        assert set(facts) == {"publication_order"}
    elif cut in {"after-identity", "receipt-file-exists"}:
        assert set(facts) == {"publication_order", "publication_identity"}
    else:
        recorded_name = target.name if cut == "after-second-name" else name
        assert facts["publication"]["path"] == "team/" + recorded_name
    if landed:
        assert facts["publication_identity"] == {"inode": target.stat().st_ino, "device": target.stat().st_dev}
    if cut == "legacy-empty-receipt":
        # Earlier writers linked a public file before recording its identity.
        receipt.value["facts"] = {}
        receipt.save()
    if cut == "after-name-removed":
        target.unlink()

    assert await files.settle_interrupted() == int(phase != "pending")
    async with files.registry._sf() as session:
        row = await session.get(SpaceFileOperationRow, (operation.space_id, operation.operation_id))
        assert row.phase == phase
    record = await app.state.shared_publications_repo.live_publication("team/" + target.name)
    if phase == "complete":
        assert record["publication_id"] == facts["publication"]["publication_id"]
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
    if collision:
        assert (folder / name).stat().st_ino == foreign_identity.st_ino
        assert (folder / name).read_bytes() == payload
        assert await app.state.shared_publications_repo.live_publication("team/" + name) is None


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
