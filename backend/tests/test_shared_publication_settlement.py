"""Recover interrupted Company publications through the host maintenance pass."""

import os
from hashlib import sha256

import pytest
from test_shared_publication_host_transaction import seed_publication
from test_storage_feature_routes import feature_app as feature_app

from deerflow.persistence.spaces.files import SpaceFileOperationRow


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("point", "phase", "published"),
    [
        ("missing-source-thread", "pending", False),
        ("missing-file", "failed", False),
        ("other-inode", "failed", False),
        ("changed-device", "complete", True),
        ("identity-only", "complete", True),
        ("identity-removed", "failed", False),
        ("before-name", "failed", False),
        ("unrecorded-file", "pending", False),
        ("known-order", "failed", False),
        ("future-order", "pending", False),
        ("malformed-identity", "pending", False),
        ("negative-device", "pending", False),
        ("matching-record", "failed", True),
        ("stale-record", "pending", True),
    ],
)
async def test_host_reconciles_only_the_publication_its_evidence_identifies(feature_app, point, phase, published):
    _, space_id, operation_id, publication_id, receipt, target = await seed_publication(feature_app)
    _, app, files, *_ = feature_app
    repo = app.state.shared_publications_repo
    facts = receipt.value["facts"]
    if point in {"matching-record", "stale-record"}:
        await repo.record_publication(**facts["publication"])
        if point == "stale-record":
            target.write_bytes(b"different contents")
    if point == "missing-source-thread":
        facts["publication"]["from_thread_id"] = None
    metadata = target.stat()
    if point in {"missing-file", "other-inode", "changed-device", "identity-only", "identity-removed", "malformed-identity", "negative-device"}:
        facts["publication_identity"] = {"device": metadata.st_dev + 1, "inode": metadata.st_ino}
    if point == "malformed-identity":
        facts["publication_identity"]["inode"] = None
    if point == "negative-device":
        facts["publication_identity"]["device"] = -1
    if point == "other-inode":
        other = target.with_suffix(".incoming")
        other.write_bytes(target.read_bytes())
        os.replace(other, target)
    if point not in {"missing-source-thread", "missing-file", "other-inode", "changed-device"}:
        facts.pop("publication")
    if point in {"missing-file", "before-name", "identity-removed"}:
        target.unlink()
    if point in {"known-order", "future-order"}:
        facts["publication_order"] = "fact-before-name" if point == "known-order" else "future"
    receipt.save()
    before = target.read_bytes() if target.is_file() else None

    assert await files.settle_interrupted() == int(phase != "pending")
    async with files.registry._sf() as session:
        row = await session.get(SpaceFileOperationRow, (space_id, operation_id))
        assert row.phase == phase
        if phase == "failed":
            assert row.result["resolution"] == "host-settled"
    assert receipt.path.exists() is (phase == "pending")
    assert (target.read_bytes() if target.is_file() else None) == before
    records = await repo.live_publications()
    assert bool(records) is published
    if point in {"changed-device", "identity-only"}:
        record = records["pending.txt"]
        assert record["published_by"] == str(feature_app[5].id)
        assert record["from_path"] == receipt.value["request"]["source"]["path"]
        if point == "changed-device":
            assert record["publication_id"] == publication_id


@pytest.mark.asyncio
@pytest.mark.parametrize("location", ["root-fact", "nested-fact", "nested-without-fact"])
async def test_operator_describes_exact_shared_folder_and_matching_records(feature_app, tmp_path, location):
    from app.gateway.agent_operator_services import OperatorCommands
    from deerflow.spaces.workflows import WorkflowReceipt

    _, space_id, operation_id, _, first, original = await seed_publication(feature_app)
    _, app, files, *_ = feature_app
    relative = "reports/" if location.startswith("nested") else ""
    folder = original.parent / relative
    folder.mkdir(exist_ok=True)
    target = folder / original.name
    if target != original:
        original.rename(target)
    sibling = folder / "pending_1.txt"
    sibling.write_bytes(b"kept")
    (folder / "other.txt").write_bytes(b"ordinary")
    await app.state.shared_publications_repo.record_publication(path=relative + sibling.name, size=4, sha256=sha256(b"kept").hexdigest(), published_by="alice", from_path=None, from_thread_id=None)
    req = first.value["request"]
    req = {**req, "source": {**req["source"], "folder": relative.rstrip("/")}}
    async with files.registry.host_locked([space_id]) as (session, _):
        row = await session.get(SpaceFileOperationRow, (space_id, operation_id))
        row.request = req
    volume = next(volume for volume in files._verified.values() if volume.control_path == first.root)
    receipt = WorkflowReceipt(volume, operation_id, req)
    if location != "nested-without-fact":
        receipt.value["facts"]["publication"] = {**first.value["facts"]["publication"], "path": relative + original.name}
    receipt.save()
    command = OperatorCommands(files=files, agents=None, repository=None, runs=None, config=None, home=tmp_path)
    answer = await command.run("settle", space_id=space_id, operation_id=operation_id)
    assert answer["files"] == ["other.txt", "pending.txt", "pending_1.txt"]
    assert answer["publication_matches"] == {"pending.txt": False, "pending_1.txt": True}
    assert answer["shared_folder"] == relative.rstrip("/")
    assert target.is_file() and sibling.read_bytes() == b"kept"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["listing", "record", "hash"])
async def test_optional_operator_diagnostics_cannot_block_explicit_discard(feature_app, tmp_path, monkeypatch, failure):
    from types import SimpleNamespace

    from sqlalchemy.exc import SQLAlchemyError

    from app.gateway.agent_operator_services import OperatorCommands
    from deerflow.features import qualified_publications
    from deerflow.persistence.shared_publications.sql import SharedPublicationRepository
    from deerflow.spaces.filesystem import ConfinedFilesystem, FilesystemUnavailable

    _, space_id, operation_id, _, receipt, target = await seed_publication(feature_app)
    _, app, files, *_ = feature_app
    await app.state.shared_publications_repo.record_publication(**receipt.value["facts"]["publication"])
    if failure == "listing":
        original = ConfinedFilesystem._open

        def unreadable(self, path, flags, **kwargs):
            if path == "shared" and flags & os.O_DIRECTORY:
                raise OSError("directory I/O unavailable")
            return original(self, path, flags, **kwargs)

        monkeypatch.setattr(ConfinedFilesystem, "_open", unreadable)
    elif failure == "record":

        async def database_error(*args, **kwargs):
            raise SQLAlchemyError("record observation unavailable")

        monkeypatch.setattr(SharedPublicationRepository, "live_publication", database_error)
    else:

        def read_error(*args, **kwargs):
            raise FilesystemUnavailable("hash observation unavailable")

        monkeypatch.setattr(qualified_publications, "shared_file_holding", read_error)
    adapter = SimpleNamespace(host_id="current-daemon", _same_host=lambda: None)
    command = OperatorCommands(files=files, agents=None, repository=None, runs=None, config=None, adapter=adapter, home=tmp_path)
    answer = await command.run("settle", space_id=space_id, operation_id=operation_id, discard=True)
    assert answer["verdict"] == "discarded" and target.read_bytes() == b"durable publication"
    if failure == "hash":
        assert answer["publication_matches"] == {target.name: None}
    else:
        assert answer["publication_matches"] is None


def test_receipt_name_candidates_follow_the_copy_rules_for_compound_and_long_names():
    from deerflow.features.qualified_publications import is_publication_name
    from deerflow.uploads.manager import numbered_upload_name

    for requested in ("Final_7.TAR.GZ", "表" * 82 + ".tar.gz", "archive.123"):
        assert is_publication_name(requested, requested.swapcase())
        for index in (1, 18, 999):
            candidate = numbered_upload_name(requested, index)
            assert len(candidate.encode("utf-8")) <= 255
            assert is_publication_name(requested, candidate.swapcase())
        assert not is_publication_name(requested, "other_1.tar.gz")
    assert not is_publication_name("file.txt", "file_01.txt")
    assert not is_publication_name("file.txt", "file_0.txt")


@pytest.mark.asyncio
async def test_unreadable_named_file_keeps_pending_receipt_until_it_can_be_verified(feature_app, monkeypatch):
    from deerflow.spaces.filesystem import ConfinedFilesystem

    _, space_id, operation_id, _, receipt, target = await seed_publication(feature_app)
    _, app, files, *_ = feature_app
    original = ConfinedFilesystem.open_regular

    def unreadable(self, path, **kwargs):
        if path == "shared/" + target.name:
            raise PermissionError("unreadable file")
        return original(self, path, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(ConfinedFilesystem, "open_regular", unreadable)
        assert await files.settle_interrupted() == 0
    async with files.registry._sf() as session:
        assert (await session.get(SpaceFileOperationRow, (space_id, operation_id))).phase == "pending"
    assert receipt.path.exists() and not await app.state.shared_publications_repo.live_publications()
    assert await files.settle_interrupted() == 1
    assert target.read_bytes() == b"durable publication"
