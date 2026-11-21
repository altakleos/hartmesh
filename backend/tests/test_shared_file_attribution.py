"""Shared ownership follows the verified bytes, including during removal staging."""

import os

import pytest
from test_shared_router import _client, _own_file, _user
from test_shared_router import paths as paths_fixture
from test_shared_router import repo as repo_fixture
from test_storage_feature_routes import THREAD
from test_storage_feature_routes import feature_app as feature_app_fixture

from deerflow.files.shared_removals import SharedMutationState

paths = paths_fixture
repo = repo_fixture
feature_app = feature_app_fixture


def replace_at_stage(monkeypatch, path, content=b"other"):
    original = SharedMutationState.stage

    def stage(state, relative, publication_id):
        replacement = path.with_name("replacement.tmp")
        replacement.write_bytes(content)
        os.replace(replacement, path)
        return original(state, relative, publication_id)

    monkeypatch.setattr(SharedMutationState, "stage", stage)


@pytest.mark.anyio
async def test_changed_same_size_bytes_lose_publication_and_authority(paths, repo):
    client, owner = _client(repo)
    source = _own_file(paths, owner, "paper.txt", b"first")
    record = client.post("/api/shared/publish", json={"path": source}).json()
    path = paths.shared_dir() / "paper.txt"
    assert client.get("/api/shared").json()["files"][0]["publication_id"] == record["publication_id"]
    stamp = path.stat()
    path.write_bytes(b"other")
    os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
    current = client.get("/api/shared").json()["files"][0]
    assert current["publication_id"] is None and current["published_at"] is None and not current["can_remove"]
    assert client.delete("/api/shared/paper.txt", params={"expected_publication_id": record["publication_id"]}).status_code == 409
    assert client.delete("/api/shared/paper.txt").status_code == 403
    admin, _ = _client(repo, _user("admin", role="admin"))
    assert admin.delete("/api/shared/paper.txt", params={"expected_publication_id": record["publication_id"]}).status_code == 409
    assert path.read_bytes() == b"other"
    assert admin.delete("/api/shared/paper.txt", params={"expected_publication_id": ""}).status_code == 200
    assert (await repo.publication(record["publication_id"]))["removed_at"] is None


@pytest.mark.anyio
@pytest.mark.parametrize("content", [b"other", b"first"])
async def test_changed_during_stage_restores_the_actual_payload(paths, repo, monkeypatch, content):
    client, owner = _client(repo)
    source = _own_file(paths, owner, "paper.txt", b"first")
    record = client.post("/api/shared/publish", json={"path": source}).json()
    path = paths.shared_dir() / "paper.txt"
    replace_at_stage(monkeypatch, path, content)
    result = client.delete("/api/shared/paper.txt", params={"expected_publication_id": record["publication_id"]})
    assert result.status_code == 409, result.text
    assert path.read_bytes() == content
    assert (await repo.publication(record["publication_id"]))["removed_at"] is None


async def publish_in_managed_shared(feature_app):
    client, _, _, catalog, paths, alice, _, _ = feature_app
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(alice.id))
    outputs.mkdir(parents=True)
    (outputs / "paper.txt").write_bytes(b"first")
    kept = await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/paper.txt"})
    published = await client.post("/api/shared/publish", json={"path": kept.json()["virtual_path"]})
    assert published.status_code == 201, published.text
    record = published.json()
    path = next(v.data_path / "shared/paper.txt" for v in catalog.verified.values() if (v.data_path / "shared/paper.txt").is_file())
    return client, record, path


@pytest.mark.asyncio
async def test_managed_shared_staged_replacement_loses_attribution(feature_app, monkeypatch):
    client, record, path = await publish_in_managed_shared(feature_app)
    with monkeypatch.context() as staged:
        replace_at_stage(staged, path)
        result = await client.delete("/api/shared/paper.txt", params={"expected_publication_id": record["publication_id"]})
    assert result.status_code == 409, result.text
    assert path.read_bytes() == b"other"
    listing = await client.get("/api/shared")
    assert listing.status_code == 200, listing.text
    row = listing.json()["files"][0]
    assert row["publication_id"] is None and row["published_at"] is None
    assert isinstance(result.json()["detail"], str) and "changed" in result.json()["detail"]
    # The confirmed restore releases the write slot immediately.
    removed = await client.delete("/api/shared/paper.txt", params={"expected_publication_id": ""})
    assert removed.status_code == 200, removed.text


@pytest.mark.anyio
async def test_repeated_list_uses_metadata_but_rechecks_new_bytes(paths, repo, monkeypatch):
    from deerflow.files import shared_attribution

    client, owner = _client(repo)
    source = _own_file(paths, owner, "cached.txt", b"first")
    record = client.post("/api/shared/publish", json={"path": source}).json()
    hashes = []
    original = shared_attribution.digest_open_file

    def count_hash(fd):
        hashes.append(os.fstat(fd).st_size)
        return original(fd)

    monkeypatch.setattr(shared_attribution, "digest_open_file", count_hash)
    for _ in range(2):
        assert client.get("/api/shared").json()["files"][0]["publication_id"] == record["publication_id"]
    assert hashes == [5]
    path = paths.shared_dir() / "cached.txt"
    replacement = paths.shared_dir() / "incoming.txt"
    replacement.write_bytes(b"other")
    original_mtime = path.stat().st_mtime_ns
    os.utime(replacement, ns=(original_mtime, original_mtime))
    os.replace(replacement, path)
    for _ in range(2):
        assert client.get("/api/shared").json()["files"][0]["publication_id"] is None
    assert hashes == [5, 5]


@pytest.mark.parametrize(("previous", "occupied"), [(False, False), (True, False), (False, True)])
def test_restore_only_discharges_its_own_confirmed_stage(tmp_path, monkeypatch, previous, occupied):
    from deerflow.files.shared_attribution import SharedPublicationChanged, observed_file, stage_observed
    from deerflow.files.store import StoreError
    from deerflow.spaces.workflows import _effect

    path = tmp_path / "report.txt"
    path.write_bytes(b"first")
    observed = observed_file(path)
    state = SharedMutationState(tmp_path)
    state.acquire()
    original = state.stage

    def staged_replacement(relative, record):
        replacement = tmp_path / "native.txt"
        replacement.write_bytes(b"second")
        replacement.replace(path)
        pending = original(relative, record)
        if occupied:
            path.write_bytes(b"third")
        return pending

    monkeypatch.setattr(state, "stage", staged_replacement)
    effect = [previous]
    token = _effect.set(effect)
    try:
        with pytest.raises(StoreError if occupied else SharedPublicationChanged):
            stage_observed(state, path.name, None, observed)
        assert effect == [previous or occupied]
        if occupied:
            assert path.read_bytes() == b"third"
            (name,) = state.pending_names()
            pending = state.open_removal(name)
            try:
                with os.fdopen(pending.open_payload(), "rb") as descriptor:
                    assert descriptor.read() == b"second"
            finally:
                pending.close()
        else:
            assert path.read_bytes() == b"second"
            assert state.pending_names() == []
    finally:
        _effect.reset(token)
        state.close()


@pytest.mark.asyncio
async def test_pre_stage_read_refusal_keeps_bytes_and_closes_managed_intent(feature_app, monkeypatch):
    from sqlalchemy import select

    from deerflow.features import behaviors
    from deerflow.persistence.spaces.files import SpaceFileOperationRow

    _, app, files, *_ = feature_app
    client, record, path = await publish_in_managed_shared(feature_app)

    def unreadable(*args, **kwargs):
        raise OSError("Cannot read the selected file")

    monkeypatch.setattr(behaviors, "observed_file", unreadable)
    result = await client.delete("/api/shared/paper.txt", params={"expected_publication_id": record["publication_id"]})
    assert result.status_code == 503, result.text
    assert path.read_bytes() == b"first"
    assert (await app.state.shared_publications_repo.publication(record["publication_id"]))["removed_at"] is None
    async with files.registry._sf() as session:
        assert await session.scalar(select(SpaceFileOperationRow).where(SpaceFileOperationRow.phase == "pending")) is None
