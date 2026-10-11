"""Publication facts share the host transaction without committing its caller."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import update
from test_storage_feature_routes import feature_app as feature_app

from deerflow.features.plugins import FeatureServices
from deerflow.features.resources import StorageFeatureLinkRow
from deerflow.persistence.shared_publications.sql import SharedPublicationRepository
from deerflow.persistence.spaces.files import SpaceFileOperationRow
from deerflow.persistence.spaces.model import SpaceRow
from deerflow.spaces.contract import SpaceDenied
from deerflow.spaces.workflows import WorkflowReceipt, _effect, _owner, _read_only, _receipt, acquire, close
from deerflow.utils.file_io import run_file_io


@pytest.mark.asyncio
@pytest.mark.parametrize("commit", [True, False])
async def test_borrowed_publication_session_commits_or_rolls_back_with_host(feature_app, commit):
    _client, app, files, _catalog, _paths, alice, _bob, _current = feature_app
    sf = files.registry._sf
    identifier = str(uuid4())
    async with sf() as session:
        space_id = (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id
    try:
        async with files.registry.host_locked([space_id]) as (session, rows):
            before = rows[space_id].name
            borrowed = SharedPublicationRepository(sf, session=session)
            await borrowed.record_publication(path="atomic.txt", size=4, sha256="a" * 64, published_by=str(alice.id), from_thread_id=None, from_path=None, publication_id=identifier)
            assert (await borrowed.publication(identifier))["path"] == "atomic.txt"
            assert session.in_transaction() and session.is_active
            rows[space_id].name = "Host transaction committed"
            if not commit:
                raise RuntimeError("rollback host transaction")
    except RuntimeError as exc:
        assert not commit and str(exc) == "rollback host transaction"
    async with files.registry.host_locked([space_id]) as (_session, rows):
        assert rows[space_id].name == ("Host transaction committed" if commit else before)
    record = await app.state.shared_publications_repo.publication(identifier)
    assert (record is not None) is commit


@pytest.mark.asyncio
async def test_cancelled_host_transaction_preserves_no_publication_fact(feature_app):
    _client, app, files, _catalog, _paths, alice, _bob, _current = feature_app
    sf = files.registry._sf
    ready = asyncio.Event()
    identifier = str(uuid4())
    async with sf() as session:
        space_id = (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id

    async def write():
        async with files.registry.host_locked([space_id]) as (session, _rows):
            borrowed = SharedPublicationRepository(sf, session=session)
            await borrowed.record_publication(path="cancelled.txt", size=1, sha256="b" * 64, published_by=str(alice.id), from_thread_id=None, from_path=None, publication_id=identifier)
            ready.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(write())
    try:
        await asyncio.wait_for(ready.wait(), timeout=5)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await app.state.shared_publications_repo.publication(identifier) is None
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_borrowed_session_requires_an_active_transaction(feature_app):
    _client, _app, files, _catalog, _paths, _alice, _bob, _current = feature_app
    sf = files.registry._sf
    async with sf() as session:
        with pytest.raises(ValueError, match="active transaction"):
            borrowed = SharedPublicationRepository(sf, session=session)
            await borrowed.publication(str(uuid4()))


@asynccontextmanager
async def host_settlement(files, space_id, operation_id, *, receipt_override=None):
    """Own the filesystem lock before the host transaction, as the settler must."""
    async with files.registry._sf() as reader:
        row = await reader.get(SpaceRow, space_id)
        volume = await files._volume(reader, row)
    descriptors = await run_file_io(acquire, {space_id: volume})
    tokens = []
    try:
        async with files.registry.host_locked([space_id]) as (session, _rows):
            intent = await session.get(SpaceFileOperationRow, (space_id, operation_id))
            receipt = receipt_override or WorkflowReceipt(volume, operation_id, deepcopy(intent.request))
            await run_file_io(receipt.load)
            for var, value in ((_owner, (files, frozenset([space_id]))), (_receipt, receipt), (_effect, [False]), (_read_only, False)):
                tokens.append((var, var.set(value)))
            yield session, intent, receipt
    finally:
        for var, token in reversed(tokens):
            var.reset(token)
        await run_file_io(close, descriptors)


async def seed_publication(feature_app):
    _client, app, files, _catalog, _paths, alice, _bob, _current = feature_app
    operation_id, publication_id = uuid4().hex, str(uuid4())
    request = {"action": "feature-workflow", "namespace": "hm.shared", "operation": "publish", "by": "host", "source": {"path": "/mnt/user-data/outputs/pending.txt"}, "parameters": {}}
    async with files.registry._sf() as session:
        space_id = (await session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))).space_id
        row = await session.get(SpaceRow, space_id)
        volume = await files._volume(session, row)
        session.add(SpaceFileOperationRow(space_id=space_id, operation_id=operation_id, actor_kind="human", actor_id=str(alice.id), generation=row.generation, phase="pending", request=request))
        await session.commit()
    target = volume.data_path / "shared" / "pending.txt"
    target.parent.mkdir(exist_ok=True)
    target.write_bytes(b"durable publication")
    receipt = WorkflowReceipt(volume, operation_id, request)
    receipt.value["facts"]["publication"] = {
        "publication_id": publication_id,
        "path": "pending.txt",
        "size": target.stat().st_size,
        "sha256": sha256(target.read_bytes()).hexdigest(),
        "published_by": str(alice.id),
        "from_thread_id": None,
        "from_path": request["source"]["path"],
    }
    receipt.save()
    service = app.state.extensions.app_store.get(FeatureServices).services["hm.shared"]
    return service, space_id, operation_id, publication_id, receipt, target


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["request", "actor_id", "generation"])
async def test_settlement_rejects_same_session_stale_intent(feature_app, changed):
    service, space_id, operation_id, publication_id, _receipt_on_disk, _target = await seed_publication(feature_app)
    _client, app, files, *_ = feature_app
    async with host_settlement(files, space_id, operation_id) as (session, intent, _receipt_value):
        value = {"request": {**intent.request, "parameters": {"folder": "other"}}, "actor_id": "another-human", "generation": intent.generation + 1}[changed]
        await session.execute(update(SpaceFileOperationRow).where(SpaceFileOperationRow.space_id == space_id, SpaceFileOperationRow.operation_id == operation_id).values(**{changed: value}).execution_options(synchronize_session=False))
        with pytest.raises(SpaceDenied, match="exact pending"):
            await service.settle_publication(intent, session=session)
    assert await app.state.shared_publications_repo.publication(publication_id) is None


@pytest.mark.asyncio
async def test_settlement_rejects_receipt_from_another_space(feature_app):
    service, space_id, operation_id, publication_id, receipt, _target = await seed_publication(feature_app)
    _client, app, files, _catalog, _paths, alice, *_ = feature_app
    async with files.registry._sf() as reader:
        personal = (await reader.get(StorageFeatureLinkRow, ("hm.user-space", "human:" + str(alice.id)))).space_id
        volume = await files._volume(reader, await reader.get(SpaceRow, personal))
    wrong = WorkflowReceipt(volume, operation_id, receipt.value["request"])
    wrong.value = deepcopy(receipt.value)
    wrong.save()
    async with host_settlement(files, space_id, operation_id, receipt_override=wrong) as (session, intent, _):
        with pytest.raises(SpaceDenied, match="receipt"):
            await service.settle_publication(intent, session=session)
    assert await app.state.shared_publications_repo.publication(publication_id) is None
    assert receipt.path.exists() and wrong.path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("commit", [True, False])
async def test_actual_settlement_rolls_back_with_operation_and_retains_receipt_until_commit(feature_app, commit):
    service, space_id, operation_id, publication_id, receipt, target = await seed_publication(feature_app)
    _client, app, files, *_ = feature_app
    try:
        async with host_settlement(files, space_id, operation_id) as (session, intent, loaded):
            result = await service.settle_publication(intent, session=session)
            assert result == {"publication_id": publication_id, "published": True}
            intent.phase, intent.result = "complete", result
            assert loaded.path.exists()
            if not commit:
                raise RuntimeError("outer host rollback")
    except RuntimeError:
        assert not commit
    async with files.registry._sf() as session:
        assert (await session.get(SpaceFileOperationRow, (space_id, operation_id))).phase == ("complete" if commit else "pending")
    assert (await app.state.shared_publications_repo.publication(publication_id) is not None) is commit
    assert receipt.path.exists() and target.read_bytes() == b"durable publication"
    if commit:
        await run_file_io(receipt.remove)
        assert not receipt.path.exists()


@pytest.mark.asyncio
async def test_settlement_loads_persisted_facts_and_does_not_resolve_original_human(feature_app, monkeypatch):
    service, space_id, operation_id, publication_id, _saved, _target = await seed_publication(feature_app)
    _client, app, files, *_ = feature_app

    async def departed(_reference):
        raise AssertionError("Host settlement must not resolve the original human")

    monkeypatch.setattr(files.registry, "_actor", departed)
    async with host_settlement(files, space_id, operation_id) as (session, intent, receipt):
        receipt.value["facts"] = {"publication": {"publication_id": "memory-is-not-persisted"}}
        result = await service.settle_publication(intent, session=session)
        assert result["publication_id"] == publication_id
        intent.phase, intent.result = "complete", result
    assert await app.state.shared_publications_repo.publication(publication_id) is not None


@pytest.mark.asyncio
async def test_cancelled_settlement_drains_filesystem_before_unlock_and_host_rollback(feature_app, monkeypatch):
    import threading

    import deerflow.features.qualified_publications as publications
    from deerflow.spaces.service import SpaceOperationPending
    from deerflow.spaces.workflows import probe

    service, space_id, operation_id, publication_id, receipt, target = await seed_publication(feature_app)
    _client, app, files, *_ = feature_app
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = publications.shared_file_holding

    def held(*args, **kwargs):
        entered.set()
        try:
            assert release.wait(10)
            return original(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(publications, "shared_file_holding", held)

    async def settle():
        async with host_settlement(files, space_id, operation_id) as (session, intent, _receipt_value):
            result = await service.settle_publication(intent, session=session)
            intent.phase, intent.result = "complete", result

    task = asyncio.create_task(settle())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0.03)
        assert not task.done()
        async with files.registry._sf() as reader:
            volume = await files._volume(reader, await reader.get(SpaceRow, space_id))
        with pytest.raises(SpaceOperationPending):
            await run_file_io(probe, volume)
    finally:
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert finished.is_set()
    assert await app.state.shared_publications_repo.publication(publication_id) is None
    async with files.registry._sf() as reader:
        assert (await reader.get(SpaceFileOperationRow, (space_id, operation_id))).phase == "pending"
    assert receipt.path.exists() and target.read_bytes() == b"durable publication"
