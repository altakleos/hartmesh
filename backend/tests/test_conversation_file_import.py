"""Files presented by a finished answer can be attached to another owned conversation."""

import hashlib
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _router_auth_helpers import _make_stub_user, make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.deps import get_config
from app.gateway.routers import uploads
from deerflow.config.paths import Paths

OUTPUT = "/mnt/user-data/outputs/notes.txt"
BODY = {"source_thread_id": "earlier", "source_run_id": "answer", "path": OUTPUT}
URL = "/api/threads/later/uploads/from-conversation"


@pytest.fixture()
def site(tmp_path, monkeypatch):
    from app.gateway import conversation_file_import as imports
    from app.gateway.routers import conversation_files

    user = _make_stub_user()
    owner = str(user.id)
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True, signed_in=True)
    app.include_router(conversation_files.router)
    app.dependency_overrides[get_config] = lambda: SimpleNamespace(uploads=SimpleNamespace(auto_convert_documents=False))
    rows = {name: {"thread_id": name, "user_id": owner, "incarnation": name} for name in ("earlier", "later")}
    app.state.thread_store.get = AsyncMock(side_effect=lambda name, **kw: rows.get(name))
    record = {"thread_id": "earlier", "run_id": "answer", "status": "success", "user_id": owner}
    app.state.run_store = SimpleNamespace(get=AsyncMock(return_value=record))
    events = [{"content": {"presented_files": [OUTPUT, "/mnt/user-data/outputs/gone.txt"], "paths": ["/mnt/user-data/outputs/draft.txt"]}}]
    app.state.run_event_store = SimpleNamespace(list_events=AsyncMock(return_value=events))
    app.state.run_manager = SimpleNamespace(list_by_thread=AsyncMock(return_value=[SimpleNamespace(**record, created_at="2026-01-02T00:00:00Z", operation_kind="run")]))
    paths = Paths(base_dir=tmp_path)
    monkeypatch.setattr(imports, "get_paths", lambda: paths)
    destination = tmp_path / "uploads"
    destination.mkdir()
    monkeypatch.setattr(uploads, "ensure_uploads_dir", lambda *args, **kw: destination)
    monkeypatch.setattr(uploads, "get_sandbox_provider", lambda: None)
    monkeypatch.setattr(uploads, "_uses_thread_data_mounts", lambda provider: True)
    output = paths.sandbox_outputs_dir("earlier", user_id=owner)
    output.mkdir(parents=True)
    (output / "notes.txt").write_bytes(b"meeting notes")
    (output / "draft.txt").write_bytes(b"working draft")
    with TestClient(app) as client:
        yield SimpleNamespace(client=client, app=app, rows=rows, record=record, output=output, destination=destination, owner=owner, paths=paths)


def test_picker_lists_only_presented_paths_including_gone_files(site):
    answer = site.client.get("/api/threads/earlier/presented-files")
    assert answer.status_code == 200, answer.text
    files = answer.json()["files"]
    assert [(f["filename"], f["available"], f["size"]) for f in files] == [("notes.txt", True, 13), ("gone.txt", False, None)]
    assert files[0]["modified"] > 0
    assert files[1]["modified"] is None
    assert not answer.json()["has_more"]


def test_import_publishes_exact_bytes_and_reuses_a_collision_even_after_a_name_is_removed(site):
    (site.destination / "notes.txt").write_bytes(b"existing")
    answer = site.client.post(URL, json=BODY)
    assert answer.status_code == 200, answer.text
    info = answer.json()
    assert (info["filename"], info["size"], info["sha256"]) == ("notes_1.txt", 13, hashlib.sha256(b"meeting notes").hexdigest())
    assert info["virtual_path"] == "/mnt/user-data/uploads/notes_1.txt"
    assert "path" not in info
    assert (site.destination / "notes.txt").read_bytes() == b"existing"
    assert (site.destination / info["filename"]).read_bytes() == b"meeting notes"
    (site.destination / "notes.txt").unlink()
    assert site.client.post(URL, json=BODY).json()["filename"] == "notes_1.txt"
    assert sorted(p.name for p in site.destination.iterdir()) == ["notes_1.txt"]


def test_concurrent_repeats_share_the_same_original(site):
    with ThreadPoolExecutor(max_workers=3) as pool:
        responses = list(pool.map(lambda _: site.client.post(URL, json=BODY), range(3)))
    assert [(r.status_code, r.json().get("filename")) for r in responses] == [(200, "notes.txt")] * 3
    assert [p.name for p in site.destination.iterdir()] == ["notes.txt"]


@pytest.mark.parametrize("change", ["foreign-source", "unowned-source", "missing-source", "foreign-destination", "missing-destination", "wrong-run", "ongoing", "unpresented", "gone"])
def test_unavailable_or_unpresented_files_leave_destination_empty(site, change):
    body = dict(BODY)
    if change == "foreign-source":
        site.rows["earlier"]["user_id"] = "reader-two"
    if change == "unowned-source":
        site.rows["earlier"]["user_id"] = None
    if change == "missing-source":
        del site.rows["earlier"]
    if change == "foreign-destination":
        site.rows["later"]["user_id"] = "reader-two"
    if change == "missing-destination":
        del site.rows["later"]
    if change == "wrong-run":
        site.record["thread_id"] = "other-thread"
    if change == "ongoing":
        site.record["status"] = "running"
    if change == "unpresented":
        body["path"] = "/mnt/user-data/outputs/draft.txt"
    if change == "gone":
        body["path"] = "/mnt/user-data/outputs/gone.txt"
    answer = site.client.post(URL, json=body)
    assert answer.status_code in (404, 409), answer.text
    assert list(site.destination.iterdir()) == []
    if change in ("foreign-destination", "missing-destination"):
        site.app.state.run_event_store.list_events.assert_not_awaited()


def test_list_follows_empty_run_page(site):
    pending = SimpleNamespace(thread_id="earlier", run_id="unfinished", user_id=site.owner, created_at="2026-01-03T00:00:00+00:00", status="running", operation_kind="run")
    finished = SimpleNamespace(**site.record, created_at="2026-01-02T00:00:00Z", operation_kind="run")
    site.app.state.run_manager.list_by_thread.side_effect = [[pending, finished], [finished]]
    url = "/api/threads/earlier/presented-files"
    one = site.client.get(url, params={"limit": 1}).json()
    assert one == {"files": [], "has_more": True, "next_before_created_at": "2026-01-03T00:00:00Z", "next_before_run_id": "unfinished"}
    two = site.client.get(url, params={"limit": 1, "before_created_at": one["next_before_created_at"], "before_run_id": one["next_before_run_id"]})
    assert two.status_code == 200, two.text
    assert two.json()["files"][0]["filename"] == "notes.txt"
    assert site.client.get(url, params={"before_run_id": "unfinished"}).status_code == 422


def test_first_attachment_creates_draft_only_after_source_admission(site):
    del site.rows["later"]

    async def create(name, **kwargs):
        assert kwargs["user_id"] == site.owner
        row = dict(thread_id=name, user_id=site.owner, incarnation="new", created_at="2026-01-02T00:00:00Z", updated_at="2026-01-02T00:00:00Z")
        site.rows[name] = row
        return row

    site.app.state.thread_store.create = AsyncMock(side_effect=create)
    site.app.state.checkpointer = SimpleNamespace(aput=AsyncMock())
    draft = {**BODY, "create_destination": True}
    assert site.client.post(URL, json={**draft, "path": "/mnt/user-data/outputs/draft.txt"}).status_code == 404
    site.app.state.thread_store.create.assert_not_awaited()
    response = site.client.post(URL, json=draft)
    assert response.status_code == 200, response.text
    assert site.rows["later"]["user_id"] == site.owner
    assert (site.destination / "notes.txt").read_bytes() == b"meeting notes"


def test_ownerless_row_appearing_during_draft_creation_is_not_claimed(site):
    from sqlalchemy.exc import IntegrityError

    del site.rows["later"]

    async def collision(name, **kwargs):
        site.rows[name] = {"thread_id": name, "user_id": None, "incarnation": "other"}
        raise IntegrityError(None, None, Exception("duplicate"))

    site.app.state.thread_store.create = AsyncMock(side_effect=collision)
    site.app.state.thread_store.claim_unowned = AsyncMock()
    site.app.state.checkpointer = SimpleNamespace(aput=AsyncMock())
    response = site.client.post(URL, json={**BODY, "create_destination": True})
    assert response.status_code == 404, response.text
    assert site.rows["later"]["user_id"] is None
    site.app.state.thread_store.claim_unowned.assert_not_awaited()
    assert list(site.destination.iterdir()) == []


def test_source_replacement_while_reading_is_refused(site, monkeypatch):
    from app.gateway import conversation_file_import as imports

    original = imports._read_opened

    def read(file, maximum):
        result = original(file, maximum)
        site.rows["earlier"]["incarnation"] = "replacement"
        return result

    monkeypatch.setattr(imports, "_read_opened", read)
    assert site.client.post(URL, json=BODY).status_code == 404
    assert list(site.destination.iterdir()) == []


@pytest.mark.parametrize("state", ["live", "removed", "unknown", "record-replaced", "different-content", "different-size", "manifest-mismatch"])
def test_shared_source_must_match_live_publication_and_captured_bytes(site, monkeypatch, state):
    from fastapi import Request

    from app.gateway import conversation_file_import as imports
    from app.gateway.conversation_file_upload import import_into_chat

    content = b"published meeting notes"
    shared_dir = site.paths.shared_dir()
    shared_dir.mkdir(parents=True)
    (shared_dir / "minutes.txt").write_bytes(content)
    sha = hashlib.sha256(content).hexdigest()
    repo = SimpleNamespace(publication=AsyncMock(), live_publication=AsyncMock())
    row = {"publication_id": "minutes", "size": len(content), "sha256": sha, "path": "minutes.txt", "removed_at": None}
    if state == "removed":
        row["removed_at"] = "2026-01-02"
    if state == "different-size":
        row["size"] -= 1
    if state == "different-content":
        (shared_dir / "minutes.txt").write_bytes(b"altered" + content[7:])
    repo.publication.return_value = None if state == "unknown" else row
    repo.live_publication.return_value = {**row, "publication_id": "new"} if state == "record-replaced" else row
    site.app.state.shared_publications_repo = repo

    @site.app.post("/test-shared")
    async def use_publication(request: Request):
        source = imports.SharedPublicationSource(publication_id="minutes", sha256="0" * 64 if state == "manifest-mismatch" else sha)
        return await import_into_chat(request=request, thread_id="later", source=source, config=SimpleNamespace(uploads=SimpleNamespace(auto_convert_documents=False)))

    answer = site.client.post("/test-shared")
    if state == "live":
        assert answer.status_code == 200, answer.text
        assert answer.json()["sha256"] == sha
        assert (site.destination / "minutes.txt").read_bytes() == content
    else:
        assert answer.status_code == 409, answer.text
        assert answer.json()["detail"]["code"] == "no-longer-shared"
        assert list(site.destination.iterdir()) == []


def test_source_revoked_while_staging_cannot_publish_even_temporarily(site, monkeypatch):
    original = uploads._write_upload_chunk

    def revoke_after_staging(stage, chunk):
        original(stage, chunk)
        site.rows["earlier"]["user_id"] = "reader-two"

    monkeypatch.setattr(uploads, "_write_upload_chunk", revoke_after_staging)

    def must_not_publish(*args, **kwargs):
        pytest.fail("Publication began after the source owner changed")

    monkeypatch.setattr(uploads, "_commit_upload_temp_no_overwrite", must_not_publish)
    response = site.client.post(URL, json=BODY)
    assert response.status_code == 404, response.text
    assert list(site.destination.iterdir()) == []


def test_failed_finalization_preserves_attachment_replaced_by_another_writer(site, monkeypatch):
    from fastapi import HTTPException

    def replace_before_failure(paths):
        (current,) = paths
        replacement = current.with_suffix(".new")
        replacement.write_bytes(b"new destination content")
        replacement.replace(current)
        raise HTTPException(503, "Destination sync unavailable")

    monkeypatch.setattr(uploads, "_make_uploaded_paths_sandbox_readable", replace_before_failure)
    response = site.client.post(URL, json=BODY)
    assert response.status_code == 503, response.text
    assert (site.destination / "notes.txt").read_bytes() == b"new destination content"


def test_source_change_after_copy_is_finalized_preserves_destination_ownership(site, monkeypatch):
    from app.gateway.upload_ingestion import ThreadUploadIngestionService

    finalize = ThreadUploadIngestionService.finalize

    async def update_after_commit(session):
        await finalize(session)
        site.rows["earlier"]["incarnation"] = "replacement"

    monkeypatch.setattr(ThreadUploadIngestionService, "finalize", update_after_commit)
    response = site.client.post(URL, json=BODY)
    assert response.status_code == 200, response.text
    assert (site.destination / "notes.txt").read_bytes() == b"meeting notes"


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["publish", "finalize"])
async def test_import_cancellation_drains_disk_work_before_deciding_ownership(site, monkeypatch, stage):
    import asyncio
    import threading

    from httpx import ASGITransport, AsyncClient

    started, release = threading.Event(), threading.Event()
    name = "_commit_upload_temp_no_overwrite" if stage == "publish" else "_make_uploaded_paths_sandbox_readable"
    worker = getattr(uploads, name)

    def pause(*args, **kwargs):
        result = worker(*args, **kwargs)
        started.set()
        assert release.wait(10)
        return result

    monkeypatch.setattr(uploads, name, pause)
    async with AsyncClient(transport=ASGITransport(app=site.app), base_url="http://test") as client:
        task = asyncio.create_task(client.post(URL, json=BODY))
        try:
            assert await asyncio.to_thread(started.wait, 10)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    if stage == "publish":
        assert list(site.destination.iterdir()) == []
    else:
        assert (site.destination / "notes.txt").read_bytes() == b"meeting notes"


def test_destination_change_after_commit_withholds_receipt_and_retains_bytes(site, monkeypatch):
    from app.gateway.upload_ingestion import ThreadUploadIngestionService

    finalize = ThreadUploadIngestionService.finalize

    async def transfer_after_commit(session):
        await finalize(session)
        site.rows["later"]["user_id"] = "reader-two"

    monkeypatch.setattr(ThreadUploadIngestionService, "finalize", transfer_after_commit)
    response = site.client.post(URL, json=BODY)
    assert response.status_code == 404, response.text
    assert (site.destination / "notes.txt").read_bytes() == b"meeting notes"


def test_empty_timestamp_is_an_invalid_run_cursor(site):
    response = site.client.get("/api/threads/earlier/presented-files", params={"before_created_at": "", "before_run_id": "answer"})
    assert response.status_code == 422, response.text
    site.app.state.run_manager.list_by_thread.assert_not_awaited()


def test_external_upload_winning_a_suffix_preserves_retry_family(site, monkeypatch):
    (site.destination / "notes.txt").write_bytes(b"existing")
    original = uploads._commit_upload_temp_no_overwrite

    def concurrent_upload(staged, folder, name, **kwargs):
        if name == "notes_1.txt" and not (folder / name).exists():
            (folder / name).write_bytes(b"ordinary upload")
        return original(staged, folder, name, **kwargs)

    monkeypatch.setattr(uploads, "_commit_upload_temp_no_overwrite", concurrent_upload)
    first = site.client.post(URL, json=BODY)
    second = site.client.post(URL, json=BODY)
    assert first.status_code == second.status_code == 200
    assert first.json()["filename"] == second.json()["filename"] == "notes_2.txt"
    assert (site.destination / "notes_1.txt").read_bytes() == b"ordinary upload"


def test_retry_resynchronizes_verified_companion_to_nonmounted_sandbox(site, monkeypatch):
    from deerflow.uploads.companions import register_companion

    original = site.destination / "notes.txt"
    original.write_bytes(b"meeting notes")
    markdown = site.destination / "notes.md"
    markdown.write_bytes(b"converted meeting notes")
    register_companion(original, markdown)
    remote = {}
    sandbox = SimpleNamespace(update_file=lambda target, content: remote.update({target: content}))
    lease = SimpleNamespace(sandbox=sandbox, denied=False, release=AsyncMock())
    monkeypatch.setattr(uploads, "_uses_thread_data_mounts", lambda _: False)
    monkeypatch.setattr(uploads, "try_acquire_sandbox_for_request", AsyncMock(return_value=lease))
    answer = site.client.post(URL, json=BODY)
    assert answer.status_code == 200, answer.text
    assert answer.json()["markdown_file"] == "notes.md"
    assert remote == {"/mnt/user-data/uploads/notes.txt": b"meeting notes", "/mnt/user-data/uploads/notes.md": b"converted meeting notes"}


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["home_id", "home_generation", "generation"])
async def test_captured_agent_scope_changes_when_home_or_instance_changes(monkeypatch, field):
    from fastapi import HTTPException

    from app.gateway import conversation_file_import as imports

    instance = SimpleNamespace(id="assistant", home_id="home", generation=1)
    inspection = SimpleNamespace(instance=instance, thread_incarnation="incarnation", home_generation=2)
    monkeypatch.setattr(imports, "inspection_for_read", AsyncMock(return_value=inspection))
    scope = await imports.conversation_scope(None, "earlier")
    if field == "home_generation":
        inspection.home_generation += 1
    elif field == "home_id":
        instance.home_id = "different"
    else:
        instance.generation += 1
    with pytest.raises(HTTPException) as error:
        await imports.require_scope(None, "earlier", scope)
    assert error.value.status_code == 404


@pytest.mark.parametrize("missing", [False, True])
def test_run_store_outage_does_not_return_an_empty_page_or_cursor(site, missing):
    from deerflow.runtime.runs.manager import RunManager

    store = SimpleNamespace(list_by_thread=AsyncMock(side_effect=RuntimeError("temporary fixture outage")))
    site.app.state.run_manager = RunManager(store=None if missing else store)
    response = site.client.get("/api/threads/earlier/presented-files")
    assert response.status_code == 503, response.text
    assert set(response.json()) == {"detail"}
    site.app.state.run_event_store.list_events.assert_not_awaited()
