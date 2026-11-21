"""Recover host-generated publication receipts through the ordinary Shared reader."""

import pytest
from sqlalchemy import select
from test_compose_native_file_sources import native_request
from test_shared_publication_host_transaction import seed_publication
from test_storage_feature_routes import feature_app as feature_app

from deerflow.persistence.spaces.files import SpaceFileOperationRow


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "leaf", "variant", "phase"),
    [
        ("manifest", "conversation.json", "landed", "complete"),
        ("transcript", "transcript.md", "landed", "complete"),
        ("manifest", "conversation.json", "before-link", "failed"),
        ("manifest", "transcript.md", "landed", "pending"),
        ("other", "conversation.json", "landed", "pending"),
        ("manifest", "conversation.json", "different-thread", "pending"),
        ("manifest", "conversation.json", "no-order", "pending"),
    ],
)
async def test_generated_document_requires_exact_source_and_destination(feature_app, kind, leaf, variant, phase):
    _, space, operation, publication, receipt, original = await seed_publication(feature_app)
    _, app, files, *_ = feature_app
    source = "/conversation-snapshot/" + kind
    folder = "Conversations/Review · 0123abcd"
    request = receipt.value["request"]
    request["source"] = {**request["source"], "path": source, "folder": folder}
    async with files.registry.host_locked([space]) as (session, _):
        (await session.get(SpaceFileOperationRow, (space, operation))).request = request
    target = original.parent / folder / leaf
    target.parent.mkdir(parents=True)
    original.rename(target)
    facts = receipt.value["facts"]
    if variant != "no-order":
        facts["publication_order"] = "fact-before-name"
    metadata = target.stat()
    facts["publication_identity"] = {"device": metadata.st_dev, "inode": metadata.st_ino}
    facts["publication"].update(path=folder + "/" + leaf, from_path=source)
    if variant == "different-thread":
        facts["publication"]["from_thread_id"] = "another-thread"
    receipt.save()
    if variant == "before-link":
        target.unlink()

    assert await files.settle_interrupted() == int(phase != "pending")
    record = await app.state.shared_publications_repo.publication(publication)
    async with files.registry._sf() as session:
        row = await session.get(SpaceFileOperationRow, (space, operation))
        assert row.phase == phase
        if phase == "complete":
            assert row.result["facts"] == facts
            assert all(record[field] == value for field, value in facts["publication"].items())
        else:
            assert record is None
    assert receipt.path.exists() is (phase == "pending")


@pytest.mark.asyncio
async def test_member_publish_accepts_only_personal_or_conversation_file_sources(feature_app):
    client, app, files, *_ = feature_app
    native_request(app)
    for label in ("manifest", "transcript"):
        response = await client.post("/api/shared/publish", json={"path": "/conversation-snapshot/" + label, "thread_id": "sample-thread"})
        assert response.status_code == 400, response.text
    async with files.registry._sf() as session:
        assert not (await session.scalars(select(SpaceFileOperationRow))).all()
    assert not await app.state.shared_publications_repo.live_publications()
