"""A new press finishes the same snapshot after recovery of any one publication."""

from contextlib import asynccontextmanager
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select
from test_compose_native_file_sources import native_request
from test_storage_feature_routes import THREAD
from test_storage_feature_routes import feature_app as feature_app

from app.gateway import snapshot_publication
from app.gateway.chat_files import QualifiedUploadStage
from app.gateway.native_file_sources import NativeFileSources
from app.gateway.routers import shared_conversations
from app.gateway.snapshot_manifest import MANIFEST_SOURCE, TRANSCRIPT_SOURCE
from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.spaces.facade import storage_actor_scope
from deerflow.spaces.workflows import WorkflowReceipt


@pytest.mark.asyncio
@pytest.mark.parametrize(("target", "source"), [("report.txt", "/mnt/user-data/outputs/report.txt"), ("transcript.md", TRANSCRIPT_SOURCE), ("conversation.json", MANIFEST_SOURCE)])
async def test_interrupted_snapshot_member_uses_the_existing_writer_and_recovers(feature_app, monkeypatch, target, source):
    client, app, files, catalog, _, alice, *_ = feature_app
    request = native_request(app)
    app.include_router(shared_conversations.router)
    actor = PrincipalRef("human", str(alice.id))
    with storage_actor_scope(actor):
        user = (await NativeFileSources.prepare(request, actor)).user
    company = next(v for v in catalog.verified.values() if v.spec.kind == "company")
    outputs = user.data_path / f"conversations/{THREAD}/user-data/outputs"
    outputs.mkdir(parents=True)
    (outputs / "report.txt").write_bytes(b"Finished report")
    app.state.thread_store.get = AsyncMock(return_value={"thread_id": THREAD, "user_id": str(alice.id), "incarnation": "original"})
    app.state.run_store = SimpleNamespace(get=AsyncMock(return_value={"thread_id": THREAD, "user_id": str(alice.id), "status": "success"}))
    app.state.run_event_store = SimpleNamespace(list_events=AsyncMock(return_value=[{"content": {"presented_files": ["/mnt/user-data/outputs/report.txt"]}}]))

    async def words(*args, visible_runs, **kwargs):
        visible_runs.add("finished")
        return SimpleNamespace(title="Report", messages=[{"type": "ai", "content": "The report is ready."}])

    @asynccontextmanager
    async def reserve(*args, **kwargs):
        yield

    monkeypatch.setattr(snapshot_publication, "read_conversation", words)
    monkeypatch.setattr(snapshot_publication, "get_run_manager", lambda req: SimpleNamespace(reserve_thread_operation=reserve))
    prefix = "/api/shared-conversations"
    draft = await client.get(prefix + "/draft", params={"thread_id": THREAD})
    assert draft.status_code == 200, draft.text
    assert [r["path"] for r in draft.json()["files"]] == ["files/report.txt"]
    body = {"thread_id": THREAD, "snapshot": draft.json()["snapshot"]}
    if target == "report.txt":
        # A new run of the snapshot publisher has already verified the confirmed
        # bytes. Change the public source name immediately before its native writer.
        confirmed = snapshot_publication.publish_snapshot_file

        async def replace_source_once(**kwargs):
            if kwargs["body"].path == source:
                (outputs / "report.txt").write_bytes(b"updated after confirmation")
            return await confirmed(**kwargs)

        monkeypatch.setattr(snapshot_publication, "publish_snapshot_file", replace_source_once)
    publish = QualifiedUploadStage.publish

    app.state.snapshot_stopping = Event()

    def interrupted(stage, name, **kwargs):
        publish(stage, name, **kwargs)
        if name == target:
            app.state.snapshot_stopping.set()
            raise OSError("simulated stop after the selected public name landed")

    with monkeypatch.context() as cut:
        cut.setattr(QualifiedUploadStage, "publish", interrupted)
        stopped = await client.post(prefix, json=body)
        assert stopped.status_code == 503 and stopped.json()["detail"]["code"] == "gateway-stopping", stopped.text
    app.state.snapshot_stopping.clear()
    async with files.registry._sf() as session:
        pending = (await session.scalars(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending"))).one()
    receipt = WorkflowReceipt(company, pending.operation_id, pending.request)
    receipt.load()
    assert pending.request["operation"] == "publish"
    assert pending.request["source"]["path"] == source
    facts = receipt.value["facts"]
    assert facts["publication_order"] == "fact-before-name"
    path = facts["publication"]["path"]
    disk = company.data_path / "shared" / path
    assert facts["publication_identity"] == {"device": disk.stat().st_dev, "inode": disk.stat().st_ino}
    if target == "report.txt":
        assert disk.read_bytes() == b"Finished report"
        (outputs / "report.txt").write_bytes(b"Finished report")
        monkeypatch.setattr(snapshot_publication, "publish_snapshot_file", confirmed)
    assert path.rsplit("/", 1)[-1] == target
    assert (await client.get(prefix)).json()["conversations"] == []
    assert await files.settle_interrupted() == 1
    live = await app.state.shared_publications_repo.live_publication(path)
    assert (live["publication_id"], live["from_path"], live["from_thread_id"]) == (facts["publication"]["publication_id"], source, THREAD)
    resumed = await client.post(prefix, json=body)
    assert resumed.status_code == (200 if source == MANIFEST_SOURCE else 201), resumed.text
    detail = await client.get(prefix + "/" + resumed.json()["publication_id"])
    assert detail.status_code == 200, detail.text
    snapshot = detail.json()
    assert snapshot["transcript"]["available"] and snapshot["files"][0]["available"]
    assert (await client.get("/api/shared/" + snapshot["files"][0]["shared_path"])).content == b"Finished report"


@pytest.mark.asyncio
async def test_managed_member_links_require_current_grants_and_exact_publication(feature_app):
    from app.gateway.snapshot_manifest import SnapshotMember, digest_of_bytes, freeze_manifest
    from deerflow.spaces.contract import Permission

    client, app, files, catalog, _, alice, bob, current = feature_app
    native_request(app)
    app.include_router(shared_conversations.router)
    company = next(v for v in catalog.verified.values() if v.spec.kind == "company")
    repo = app.state.shared_publications_repo
    data = b"Finished report"
    member = SnapshotMember(path="files/report.txt", size=len(data), sha256=digest_of_bytes(data))
    frozen = freeze_manifest("Report", [{"type": "ai", "content": "Finished"}], [member])
    disk = company.data_path / "shared" / frozen.folder
    (disk / "files").mkdir(parents=True)
    member_path = f"{frozen.folder}/{member.path}"

    async def record(path, content, source):
        (company.data_path / "shared" / path).write_bytes(content)
        return await repo.record_publication(path=path, sha256=digest_of_bytes(content), size=len(content), published_by=str(alice.id), from_thread_id=THREAD, from_path=source)

    row = await record(member_path, data, "/mnt/user-data/outputs/report.txt")
    await record(f"{frozen.folder}/transcript.md", frozen.transcript, TRANSCRIPT_SOURCE)
    manifest = await record(f"{frozen.folder}/conversation.json", frozen.document, MANIFEST_SOURCE)
    snapshot_url = "/api/shared-conversations/" + manifest["publication_id"]
    detail = (await client.get(snapshot_url)).json()
    href = snapshot_url + "/members/" + detail["files"][0]["publication_id"]
    for download, disposition in ((False, "inline"), (True, "attachment")):
        opened = await client.get(href, params={"download": download})
        assert opened.status_code == 200 and opened.content == data
        assert opened.headers["content-disposition"].startswith(disposition)

    current[0] = bob
    for download in (False, True):
        assert (await client.get(href, params={"download": download})).status_code == 404
    (disk / "conversation.json").write_bytes(b"damaged")
    denied = await client.get(snapshot_url)
    assert denied.status_code in {403, 404} and frozen.folder not in denied.text
    (disk / "conversation.json").write_bytes(frozen.document)
    current[0] = alice
    actor = PrincipalRef("human", str(alice.id))
    space = next(s for s in await files.registry.list(actor=actor) if s.custody.kind == "company")
    await files.registry.host_grant(space.id, PrincipalRef("human", str(bob.id)), Permission.READ)
    current[0] = bob
    assert (await client.get(href)).content == data
    assert (await client.get(href, params={"download": True})).status_code in {403, 404}
    (disk / "conversation.json").write_bytes(b"damaged")
    available = await client.get(snapshot_url)
    assert available.status_code == 404
    assert available.json()["detail"] == {"code": "snapshot-folder-available", "publication_id": manifest["publication_id"], "folder": frozen.folder, "entries": ["conversation.json", "files/", "transcript.md"], "truncated": False}
    (disk / "conversation.json").write_bytes(frozen.document)
    current[0] = alice
    plain = "/api/shared/" + member_path
    (disk / member.path).write_bytes(b"Changed content")
    for download in (False, True):
        assert (await client.get(href, params={"download": download})).status_code == 410
    assert (await client.get(plain)).content == b"Changed content"
    (disk / member.path).unlink()
    assert (await client.get(href)).status_code == 410
    await repo.record_removal(row["publication_id"], removed_by=str(alice.id))
    await record(member_path, data, "/mnt/user-data/outputs/report.txt")
    for download in (False, True):
        refused = await client.get(href, params={"download": download})
        assert refused.status_code == 410 and "no longer available" in refused.text
    assert (await client.get(plain)).content == data
