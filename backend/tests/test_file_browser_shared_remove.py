"""Selected Shared writes keep their original publication and operation identity."""

import os
from uuid import uuid4

import pytest
from test_compose_native_file_sources import native_request
from test_shared_file_attribution import replace_at_stage
from test_shared_router import _client, _own_file
from test_shared_router import paths as paths_fixture
from test_shared_router import repo as repo_fixture
from test_storage_feature_routes import feature_app as feature_app_fixture

from app.gateway.routers import file_browser
from deerflow.spaces.contract import Permission, PrincipalRef

feature_app = feature_app_fixture
paths = paths_fixture
repo = repo_fixture
ROUTE = "/api/file-browser/v1/shared"


async def published(feature_app):
    client, app, files, catalog, _, alice, _, _ = feature_app
    app.include_router(file_browser.router)
    native_request(app)
    owner = PrincipalRef("human", str(alice.id))
    personal = await files.registry.list(actor=owner)
    home = next(space for space in personal if space.custody.kind == "personal")
    company = next(space for space in personal if space.custody.kind == "company")
    async with files.registry.admitted(actor=owner, requests={home.id: (Permission.READ, home.generation)}) as (session, rows):
        volume = await files._volume(session, rows[home.id][0])
        (volume.data_path / "files").mkdir(exist_ok=True)
        (volume.data_path / "files" / "sample.txt").write_bytes(b"shared note")
    source = {"path": "/mnt/user-data/files/sample.txt"}
    result = await client.post("/api/shared/publish", json=source)
    assert result.status_code == 201, result.text
    path = next(vol.data_path / "shared" / "sample.txt" for vol in catalog.verified.values() if (vol.data_path / "shared" / "sample.txt").is_file())
    return result.json(), source, path, company


def remove_body(row):
    return {"path": row["path"], "expected_revision": row["revision"], "expected_publication_id": row["publication_id"], "operation_id": uuid4().hex}


@pytest.mark.asyncio
async def test_managed_remove_and_replay_leave_later_publication_alone(feature_app):
    client, app, files, _, _, alice, bob, current = feature_app
    receipt, source, path, company = await published(feature_app)
    duplicate = await client.post("/api/shared/publish", json=source)
    assert duplicate.status_code == 200 and not duplicate.json().get("revision")
    selected = (await client.get(ROUTE + "/selection", params={"path": path.name})).json()
    assert selected["can_remove"] and selected["publication_id"] == receipt["publication_id"]
    assert selected["revision"] == receipt["revision"]
    body = remove_body(selected)
    missing = dict(body)
    del missing["expected_publication_id"]
    assert (await client.post(ROUTE + "/remove", json=missing)).status_code == 422
    response = await client.post(ROUTE + "/remove", json=body)
    assert response.status_code == 204, response.text
    next_record = await client.post("/api/shared/publish", json=source)
    assert next_record.status_code == 201 and next_record.json()["publication_id"] != receipt["publication_id"]
    assert (await client.post(ROUTE + "/remove", json=body)).status_code == 204
    assert path.read_bytes() == b"shared note"
    assert (await client.post(ROUTE + "/remove", json={**body, "expected_revision": "a" * 64})).status_code == 409
    assert (await client.post(ROUTE + "/remove", json={**body, "operation_id": uuid4().hex})).status_code == 409
    await files.registry.host_grant(company.id, PrincipalRef("human", str(bob.id)), Permission.READ | Permission.EXPORT)
    current[0] = bob
    selected = (await client.get(ROUTE + "/selection", params={"path": path.name})).json()
    assert not selected["can_remove"] and (await client.post(ROUTE + "/remove", json=remove_body(selected))).status_code == 403
    assert (await client.post(ROUTE + "/remove", json=body)).status_code in {403, 409}
    current[0] = alice
    await files.registry.host_grant(company.id, PrincipalRef("human", str(alice.id)), Permission.READ)
    assert not (await client.get(ROUTE + "/selection", params={"path": path.name})).json()["can_remove"]
    assert (await client.post(ROUTE + "/remove", json=body)).status_code == 403


@pytest.mark.asyncio
async def test_managed_stale_selected_entry_and_unrecorded_file(feature_app, monkeypatch):
    client = feature_app[0]
    receipt, _, path, _ = await published(feature_app)
    stale = remove_body(receipt)
    replacement = path.with_name("updated.txt")
    replacement.write_bytes(path.read_bytes())
    os.replace(replacement, path)
    assert (await client.post(ROUTE + "/remove", json=stale)).status_code == 409
    fresh = (await client.get(ROUTE + "/selection", params={"path": path.name})).json()
    with monkeypatch.context() as changed:
        replace_at_stage(changed, path, path.read_bytes())
        assert (await client.post(ROUTE + "/remove", json=remove_body(fresh))).status_code == 409
    assert path.read_bytes() == b"shared note"
    path.write_bytes(b"other")
    unknown = (await client.get(ROUTE + "/selection", params={"path": path.name})).json()
    assert "publication_id" not in unknown and not unknown["can_remove"]
    assert (await client.post(ROUTE + "/remove", json=remove_body({**unknown, "publication_id": None}))).status_code == 409
    manual = path.with_name("manual.txt")
    manual.write_text("operator", encoding="utf-8")
    selected = (await client.get(ROUTE + "/selection", params={"path": manual.name})).json()
    assert selected["publication_id"] is None and selected["can_remove"]
    body = remove_body(selected)
    for _ in range(2):
        response = await client.post(ROUTE + "/remove", json=body)
        assert response.status_code == 204, response.text
    assert not manual.exists()


@pytest.mark.asyncio
async def test_managed_admission_changes_while_selecting_for_removal(feature_app, monkeypatch):
    from sqlalchemy import update

    from deerflow.persistence.spaces.model import SpaceGrantRow

    client, app, files, _, _, alice, _, _ = feature_app
    receipt, _, path, company = await published(feature_app)
    repo = app.state.shared_publications_repo
    lookup = repo.live_publication

    async def permission_changed(name):
        record = await lookup(name)
        async with files.registry._sf.begin() as session:
            await session.execute(update(SpaceGrantRow).where(SpaceGrantRow.space_id == company.id, SpaceGrantRow.subject_id == str(alice.id)).values(permissions=int(Permission.READ)))
        return record

    monkeypatch.setattr(repo, "live_publication", permission_changed)
    response = await client.post(ROUTE + "/remove", json=remove_body(receipt))
    assert response.status_code == 403, response.text
    assert path.read_bytes() == b"shared note"
    assert (await lookup(path.name))["removed_at"] is None


@pytest.mark.asyncio
async def test_concurrent_selected_removes_share_exactly_one_outcome(feature_app):
    import asyncio

    client = feature_app[0]
    receipt, _, path, _ = await published(feature_app)
    body = remove_body(receipt)
    results = await asyncio.gather(*(client.post(ROUTE + "/remove", json=body) for _ in range(2)))
    codes = [reply.status_code for reply in results]
    assert 204 in codes and all(code in (204, 409) for code in codes)
    assert not path.exists()
    path.write_bytes(b"new local file")
    replay = await client.post(ROUTE + "/remove", json=body)
    assert replay.status_code == 204 and path.read_bytes() == b"new local file"


@pytest.mark.anyio
async def test_ordinary_worker_withholds_selected_writes(paths, repo):
    client, viewer = _client(repo)
    client.app.include_router(file_browser.router)
    source = _own_file(paths, viewer, "paper.txt", b"ordinary copy")
    published = client.post("/api/shared/publish", json={"path": source})
    assert published.status_code == 201 and not published.json().get("revision")
    selected = client.get(ROUTE + "/selection", params={"path": "paper.txt"})
    assert selected.status_code == 200 and not selected.json()["can_remove"] and "publication_id" not in selected.json()
    operation = remove_body({**selected.json(), "publication_id": published.json()["publication_id"]})
    assert client.post(ROUTE + "/remove", json=operation).status_code == 501
    assert (paths.shared_dir() / "paper.txt").read_bytes() == b"ordinary copy"


@pytest.mark.asyncio
async def test_publication_replaced_before_response_does_not_issue_undo_for_replacement(feature_app, monkeypatch):
    from app.gateway.native_shared_publications import NativeSharedCopy

    original_close = NativeSharedCopy._close_stage

    def before_close(owner, stage):
        dest = owner.volume.data_path / "shared" / owner.published.path
        replacement = dest.with_name("local-save.tmp")
        replacement.write_bytes(b"shared note")
        os.replace(replacement, dest)
        return original_close(owner, stage)

    monkeypatch.setattr(NativeSharedCopy, "_close_stage", before_close)
    receipt, _, path, _ = await published(feature_app)
    assert not receipt.get("revision") and path.read_bytes() == b"shared note"
