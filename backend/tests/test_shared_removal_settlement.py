"""Verify a Shared removal's public outcome using its SQL row and private staging."""

from uuid import uuid4

import pytest
from test_shared_publication_host_transaction import seed_publication
from test_storage_feature_routes import feature_app as feature_app

from deerflow.files.shared_removals import SharedMutationState
from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.spaces.workflows import WorkflowReceipt, _receipt


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("point", "recorded", "phase"),
    [
        ("before-fact", True, "failed"),
        ("before-stage", True, "failed"),
        ("before-stage", False, "failed"),
        ("empty-stage", True, "failed"),
        ("empty-stage", False, "failed"),
        ("temporary-journal", True, "failed"),
        ("journal-only", True, "failed"),
        ("moved", True, "failed"),
        ("moved-removed", True, "complete"),
        ("moved", False, "failed"),
        ("moved-removed", False, "complete"),
        ("empty-after-finish", True, "complete"),
        ("empty-after-finish", False, "complete"),
        ("no-stage-after-finish", False, "complete"),
        ("missing-live-file", True, "pending"),
        ("foreign-stage", True, "pending"),
    ],
)
async def test_host_decides_remove_using_exact_record_and_public_path(feature_app, point, recorded, phase):
    client, app, files, *_ = feature_app
    _, space_id, operation_id, publication_id, publish_receipt, target = await seed_publication(feature_app)
    if recorded:
        await app.state.shared_publications_repo.record_publication(**publish_receipt.value["facts"]["publication"])
    else:
        publication_id = None
    volume = next(volume for volume in files._verified.values() if volume.control_path == publish_receipt.root)
    request = {"action": "feature-workflow", "by": "host", "namespace": "hm.shared", "operation": "remove_shared", "parameters": {"path": target.name, "expected_publication_id": publication_id or ""}, "source": {}}
    async with files.registry.host_locked([space_id]) as (session, _):
        row = await session.get(SpaceFileOperationRow, (space_id, operation_id))
        row.request = request
    receipt = WorkflowReceipt(volume, operation_id, request)
    receipt.save()
    state = SharedMutationState(target.parent, control_root=volume.control_path / "shared")
    state.acquire()
    directory = volume.control_path / "shared" / ("remove-" + uuid4().hex)
    try:
        if point.startswith("moved") or point == "journal-only":
            token = _receipt.set(receipt)
            try:
                removal = state.stage(target.name, publication_id)
                directory = volume.control_path / "shared" / removal.name
                if point == "journal-only":
                    (directory / "payload").rename(target)
                if point == "moved-removed" and not recorded:
                    removal.mark_removed()
                removal.close()
            finally:
                _receipt.reset(token)
        elif point != "before-fact":
            receipt.value["facts"]["removal"] = {"journal": directory.name, "path": target.name, "publication_id": publication_id}
            receipt.save()
            if point in ("empty-stage", "temporary-journal", "empty-after-finish", "foreign-stage"):
                directory.mkdir(mode=0o700)
            if point == "temporary-journal":
                (directory / "journal.tmp").write_bytes(b"partial")
            if point == "foreign-stage":
                (directory.parent / ("remove-" + uuid4().hex)).mkdir(mode=0o700)
        if point in ("empty-after-finish", "no-stage-after-finish", "missing-live-file"):
            target.unlink()
    finally:
        state.close()
    if recorded and point in ("moved-removed", "empty-after-finish", "no-stage-after-finish"):
        await app.state.shared_publications_repo.record_removal(publication_id, removed_by="alice")

    if point == "moved":
        sibling = target.with_name("other.txt")
        sibling.write_bytes(b"other")
        listing = await client.get("/api/shared")
        assert listing.status_code == 200, listing.text
        assert [item["path"] for item in listing.json()["files"]] == [sibling.name]
        assert (await client.get("/api/shared/other.txt")).content == b"other"
        assert (await client.get("/api/shared/pending.txt")).status_code == 404
        assert (directory / "payload").is_file() and not target.exists()

    assert await files.settle_interrupted() == int(phase != "pending")
    async with files.registry._sf() as session:
        row = await session.get(SpaceFileOperationRow, (space_id, operation_id))
        assert row.phase == phase
        if phase == "failed":
            assert row.result["resolution"] == "host-settled"
    if phase != "pending":
        assert not directory.exists() and not receipt.path.exists()
        assert target.is_file() is (phase == "failed")
    else:
        assert receipt.path.is_file()
