from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from test_shared_router import THREAD, _client, _own_file, _thread_output
from test_shared_router import paths as paths
from test_shared_router import repo as repo

from app.gateway import snapshot_publication
from app.gateway.routers import shared_conversations
from app.gateway.snapshot_manifest import MANIFEST_SOURCE, TRANSCRIPT_SOURCE
from deerflow.runtime import ThreadOperationKind


@pytest.fixture
def share(repo, paths, monkeypatch):
    client, user = _client(repo)
    client.app.include_router(shared_conversations.router)
    owner = str(user.id)
    record = {"thread_id": THREAD, "user_id": owner, "incarnation": "first", "display_name": "Meeting", "created_at": "2026-01-01T00:00:00+00:00"}
    client.app.state.thread_store.get = AsyncMock(side_effect=lambda thread_id, **kwargs: record if thread_id == THREAD and kwargs.get("user_id") in (None, owner) else None)

    client.app.state.run_store = SimpleNamespace(get=AsyncMock(return_value={"thread_id": THREAD, "user_id": owner, "status": "success"}))

    async def conversation(_request, _thread, _record, *, user_id, visible_runs):
        assert user_id == owner
        visible_runs.add("finished")
        return SimpleNamespace(title="Meeting", messages=[{"type": "human", "content": "First"}, {"type": "ai", "content": "Second"}])

    monkeypatch.setattr(snapshot_publication, "read_conversation", conversation)
    monkeypatch.setattr("app.gateway.conversation_file_import.get_paths", lambda: paths)
    monkeypatch.setattr("deerflow.files.folders.get_paths", lambda: paths)
    output = _thread_output(paths, user, "note.txt", b"blue")
    monkeypatch.setattr(snapshot_publication, "receipt_paths", AsyncMock(return_value=[output]))

    @asynccontextmanager
    async def reserve(thread_id, *, kind, user_id):
        assert (thread_id, kind, user_id) == (THREAD, ThreadOperationKind.branch, owner)
        yield

    monkeypatch.setattr(snapshot_publication, "get_run_manager", lambda req: SimpleNamespace(reserve_thread_operation=reserve))
    return client, user, record


@pytest.mark.anyio
async def test_snapshot_publish_group_read_duplicate_undo_and_retired_view(share, repo, paths):
    client, user, _record = share
    prefix = "/api/shared-conversations"
    with client:
        draft = client.get(prefix + "/draft", params={"thread_id": THREAD})
        assert draft.status_code == 200, draft.text
        approved = draft.json()
        assert approved["message_count"] == 2 and not approved["already_shared"] and approved["publication_id"] is None
        body = {"snapshot": approved["snapshot"], "thread_id": THREAD}
        stale = client.post(prefix, json={**body, "snapshot": "a" * 64})
        assert stale.status_code == 409 and stale.json()["detail"]["code"] == "snapshot-changed"
        put = client.post(prefix, json=body)
        assert put.status_code == 201, put.text
        receipt = put.json()
        pid = receipt["publication_id"]
        ids = receipt["created_publication_ids"]
        assert ids[-1] == pid and len(ids) == 3
        with_again = client.post(prefix, json=body)
        assert with_again.status_code == 200 and with_again.json()["already_shared"]
        assert with_again.json()["created_publication_ids"] == []
        detail = client.get(prefix + "/" + pid).json()
        assert detail["title"] == "Meeting" and detail["with"] is None and detail["can_remove"]
        assert detail["answerer"] == {"kind": "assistant"}
        assert [r["type"] for r in detail["messages"]] == ["human", "ai"]
        assert detail["files"][0]["available"] and detail["files"][0]["shared_path"].endswith("/files/note.txt")
        assert client.get(prefix).json()["conversations"][0]["publication_id"] == pid
        rows = client.get("/api/shared").json()["files"]
        assert len(rows) == 3 and all(row["snapshot_publication_id"] == pid for row in rows)
        add = client.post("/api/shared/publish", json={"path": _own_file(paths, user, "another.txt", b"ordinary"), "folder": rows[0]["path"].split("/files/")[0] if "/files/" in rows[0]["path"] else rows[0]["path"].rsplit("/", 1)[0]})
        assert add.status_code == 201
        forged = client.post(prefix + "/" + pid + "/undo", json={"publication_ids": [add.json()["publication_id"], pid], "undo_receipt": receipt["undo_receipt"]})
        assert forged.status_code == 409
        removed = client.post(prefix + "/" + pid + "/undo", json={"publication_ids": ids, "undo_receipt": receipt["undo_receipt"]})
        assert removed.status_code == 200, removed.text
        assert set(removed.json()["removed_publication_ids"]) == set(ids)
        retired = client.get(prefix + "/" + pid)
        assert retired.status_code == 410 and set(retired.json()["detail"]) == {"code", "publisher", "published_at"}
        remaining = client.get("/api/shared").json()["files"]
        assert len(remaining) == 1 and remaining[0]["publication_id"] == add.json()["publication_id"]
        assert client.portal.call(repo.publication, ids[1])["from_path"] == TRANSCRIPT_SOURCE
        assert client.portal.call(repo.publication, pid)["from_path"] == MANIFEST_SOURCE


@pytest.mark.anyio
async def test_nonowners_cannot_discover_or_publish_private_content(share):
    client, _user, row = share
    row["user_id"] = "another-owner"
    assert client.get("/api/shared-conversations/draft", params={"thread_id": THREAD}).status_code == 404
    assert client.post("/api/shared-conversations", json={"thread_id": THREAD, "snapshot": "0" * 64}).status_code == 404


@pytest.mark.anyio
async def test_unrecorded_file_refuses_before_first_write_but_draft_still_describes_the_source(share, paths):
    client, _, _ = share
    prefix = "/api/shared-conversations"
    before = client.get(prefix + "/draft", params={"thread_id": THREAD}).json()
    digest = before["snapshot"]
    occupied = paths.shared_dir() / f"Conversations/Meeting · {digest[:8]}/conversation.json"
    occupied.parent.mkdir(parents=True)
    occupied.write_bytes(b"existing file")
    after = client.get(prefix + "/draft", params={"thread_id": THREAD})
    assert after.status_code == 200 and after.json() == before
    result = client.post(prefix, json={"thread_id": THREAD, "snapshot": digest})
    assert result.status_code == 409 and result.json()["detail"]["code"] == "unrecorded-file"
    assert list(occupied.parent.iterdir()) == [occupied] and occupied.read_bytes() == b"existing file"
    assert client.get(prefix).json()["conversations"] == []


@pytest.mark.anyio
async def test_equal_file_from_another_publisher_is_kept_and_a_later_republication_is_not_a_member(share, paths, repo):
    client, _, _ = share
    prefix = "/api/shared-conversations"
    digest = client.get(prefix + "/draft", params={"thread_id": THREAD}).json()["snapshot"]
    folder = f"Conversations/Meeting · {digest[:8]}/files"
    from test_shared_router import _user

    other = _user("colleague")
    with _client(repo, other)[0] as colleague:
        body = {"path": _own_file(paths, other, "note.txt", b"blue"), "folder": folder}
        first = colleague.post("/api/shared/publish", json=body)
        assert first.status_code == 201
        created = client.post(prefix, json={"thread_id": THREAD, "snapshot": digest})
        assert created.status_code == 201, created.text
        pid = created.json()["publication_id"]
        detail = client.get(prefix + "/" + pid).json()
        assert detail["files"][0]["publication_id"] == first.json()["publication_id"]
        assert colleague.delete("/api/shared/" + first.json()["path"], params={"expected_publication_id": first.json()["publication_id"]}).status_code == 200
        second = colleague.post("/api/shared/publish", json=body)
        assert second.status_code == 201 and second.json()["publication_id"] != first.json()["publication_id"]
        assert not client.get(prefix + "/" + pid).json()["files"][0]["available"]
        rows = client.get("/api/shared").json()["files"]
        assert not next(r for r in rows if r["publication_id"] == second.json()["publication_id"]).get("snapshot_publication_id")
        assert client.delete(prefix + "/" + pid).status_code == 200
        assert colleague.get("/api/shared/" + second.json()["path"]).content == b"blue"


@pytest.mark.anyio
async def test_an_unfinished_run_cannot_add_a_file_to_the_draft(share):
    client, user, _ = share
    client.app.state.run_store.get.return_value = {"thread_id": THREAD, "user_id": str(user.id), "status": "running"}
    result = client.get("/api/shared-conversations/draft", params={"thread_id": THREAD})
    assert result.status_code == 200 and result.json()["files"] == []


@pytest.mark.anyio
@pytest.mark.parametrize(("version", "status"), [(1, 200), (2, 404)])
async def test_recognized_document_reads_version_one_agent_metadata_and_only_host_provenance(share, paths, repo, version, status):
    import json

    from app.gateway.snapshot_manifest import digest_of_bytes, freeze_manifest

    client, user, _ = share
    doc = json.loads(freeze_manifest("Handbook", [{"type": "human", "content": "Hello"}], []).document)
    doc.update({"version": version, "with": {"kind": "employee", "id": "editor", "name": "Editor"}})
    data = json.dumps(doc, sort_keys=True).encode()
    digest = digest_of_bytes(data)
    rel = f"Conversations/Handbook · {digest[:8]}/conversation.json"
    disk = paths.shared_dir() / rel
    disk.parent.mkdir(parents=True)
    disk.write_bytes(data)
    row = await repo.record_publication(path=rel, sha256=digest, size=len(data), published_by=str(user.id), from_thread_id=THREAD, from_path=MANIFEST_SOURCE)
    detail = client.get("/api/shared-conversations/" + row["publication_id"])
    assert detail.status_code == status
    if version == 1:
        assert getattr(client.app.state, "agent_instances", None) is None
        assert detail.json()["with"] == doc["with"]
        assert detail.json()["answerer"] == {"kind": "assistant", "reason": "unavailable"}
    else:
        assert detail.json()["detail"] == {"code": "snapshot-folder-available", "publication_id": row["publication_id"], "folder": rel.rsplit("/", 1)[0], "entries": ["conversation.json"], "truncated": False}
        assert not client.get("/api/shared-conversations").json()["conversations"]
        assert client.get("/api/shared/" + rel).content == data

    await repo.record_removal(row["publication_id"], removed_by=str(user.id))
    outsider = await repo.record_publication(path=rel, sha256=digest, size=len(data), published_by=str(user.id), from_thread_id=THREAD, from_path="/mnt/user-data/outputs/conversation.json")
    unrecognized = client.get("/api/shared-conversations/" + outsider["publication_id"])
    assert unrecognized.status_code == 404 and unrecognized.json()["detail"] == {"code": "snapshot-unavailable"}
    assert "folder" not in client.get("/api/shared-conversations/" + row["publication_id"]).json()["detail"]
    assert not client.get("/api/shared").json()["files"][0].get("snapshot_publication_id")


@pytest.mark.anyio
async def test_concurrent_colleague_file_is_retained_when_a_snapshot_is_removed(share, paths, repo):
    from test_shared_router import _user

    client, _, _ = share
    digest = client.get("/api/shared-conversations/draft", params={"thread_id": THREAD}).json()["snapshot"]
    other = _user("colleague")
    with _client(repo, other)[0] as colleague:
        first = colleague.post("/api/shared/publish", json={"folder": f"Conversations/Meeting · {digest[:8]}/files", "path": _own_file(paths, other, "note.txt", b"blue")}).json()
        created = client.post("/api/shared-conversations", json={"thread_id": THREAD, "snapshot": digest}).json()
        assert first["publication_id"] not in created["created_publication_ids"]
        removed = client.delete("/api/shared-conversations/" + created["publication_id"])
        assert removed.status_code == 200 and removed.json()["retained"][0]["path"] == first["path"]
        assert colleague.get("/api/shared/" + first["path"]).content == b"blue"


@pytest.mark.anyio
async def test_replacement_after_confirmation_never_reaches_a_shared_name(share, paths, monkeypatch):
    from app.gateway.routers import shared

    client, user, _ = share
    prefix = "/api/shared-conversations"
    digest = client.get(prefix + "/draft", params={"thread_id": THREAD}).json()["snapshot"]
    check = snapshot_publication.inspect_destination
    copy = shared.publish_file
    exposed = []

    async def replace_before_copy(**kwargs):
        result = await check(**kwargs)
        if kwargs.get("preflight"):
            _thread_output(paths, user, "note.txt", b"updated after confirmation")
        return result

    def observe(*args, **kwargs):
        saved = copy(*args, **kwargs)
        exposed.append((paths.shared_dir() / saved.path).read_bytes())
        return saved

    monkeypatch.setattr(snapshot_publication, "inspect_destination", replace_before_copy)
    monkeypatch.setattr(shared, "publish_file", observe)
    refused = client.post(prefix, json={"snapshot": digest, "thread_id": THREAD})
    assert refused.status_code == 409 and refused.json()["detail"]["code"] == "snapshot-file-changed"
    assert exposed == []
    assert client.get("/api/shared").json()["files"] == []


@pytest.mark.anyio
@pytest.mark.parametrize("missing", ["threads:read", "runs:read"])
async def test_ordinary_sharing_requires_reading_both_the_conversation_and_completed_runs(share, monkeypatch, missing):
    import _router_auth_helpers

    client, _, _ = share
    monkeypatch.setattr(_router_auth_helpers, "_STUB_PERMISSIONS", [p for p in _router_auth_helpers._STUB_PERMISSIONS if p != missing])
    prefix = "/api/shared-conversations"
    assert client.get(prefix + "/draft", params={"thread_id": THREAD}).status_code == 403
    assert client.post(prefix, json={"snapshot": "f" * 64, "thread_id": THREAD}).status_code == 403


@pytest.mark.anyio
async def test_draft_reports_a_disappeared_presented_output(share, paths):
    client, user, _ = share
    (paths.sandbox_outputs_dir(THREAD, user_id=str(user.id)) / "note.txt").unlink()
    response = client.get("/api/shared-conversations/draft", params={"thread_id": THREAD})
    assert response.status_code == 409 and response.json()["detail"]["code"] == "snapshot-file-changed"


@pytest.mark.anyio
async def test_snapshot_member_links_read_only_the_displayed_publication(share, paths, monkeypatch):
    import _router_auth_helpers

    client, user, _ = share
    prefix = "/api/shared-conversations"
    digest = client.get(prefix + "/draft", params={"thread_id": THREAD}).json()["snapshot"]
    publication = client.post(prefix, json={"thread_id": THREAD, "snapshot": digest}).json()["publication_id"]
    detail = client.get(f"{prefix}/{publication}").json()
    member = detail["files"][0]
    href = f"{prefix}/{publication}/members/{member['publication_id']}"
    transcript = client.get(f"{prefix}/{publication}/members/{detail['transcript']['publication_id']}")
    assert transcript.status_code == 200 and "Second" in transcript.text
    for download, disposition in ((False, "inline"), (True, "attachment")):
        opened = client.get(href, params={"download": download})
        assert opened.status_code == 200 and opened.content == b"blue"
        assert opened.headers["content-disposition"].startswith(disposition)
        assert "no-store" in opened.headers["cache-control"]
        ranged = client.get(href, params={"download": download}, headers={"Range": "bytes=1-2"})
        assert ranged.status_code == 206 and ranged.content == b"lu"

    with monkeypatch.context() as revoked:
        revoked.setattr(_router_auth_helpers, "_STUB_PERMISSIONS", [p for p in _router_auth_helpers._STUB_PERMISSIONS if p != "threads:read"])
        for download in (False, True):
            assert client.get(href, params={"download": download}).status_code == 403

    plain = "/api/shared/" + member["shared_path"]
    disk = paths.shared_dir() / member["shared_path"]
    from app.gateway.routers._file_http import DescriptorFileResponse

    read = DescriptorFileResponse._read

    def replace_before_send(response, offset, size):
        disk.write_bytes(b"grey")
        return read(response, offset, size)

    with monkeypatch.context() as changed:
        changed.setattr(DescriptorFileResponse, "_read", replace_before_send)
        assert client.get(href).content == b"blue"
    for download in (False, True):
        refused = client.get(href, params={"download": download})
        assert refused.status_code == 410 and "no longer available" in refused.text
    assert client.get(plain).content == b"grey"
    disk.write_bytes(b"blue")
    assert client.delete(plain, params={"expected_publication_id": member["publication_id"]}).status_code == 200
    assert client.get(href).status_code == 410
    new = client.post("/api/shared/publish", json={"folder": member["shared_path"].rsplit("/", 1)[0], "path": _own_file(paths, user, "note.txt", b"blue")}).json()
    assert new["path"] == member["shared_path"] and new["publication_id"] != member["publication_id"]
    for download in (False, True):
        assert client.get(href, params={"download": download}).status_code == 410
    assert client.get(plain).content == b"blue"


@pytest.mark.anyio
async def test_damaged_snapshot_offers_only_its_current_folder_under_shared_read(share, paths, repo, monkeypatch):
    import _router_auth_helpers

    client, _, _ = share
    prefix = "/api/shared-conversations"
    draft = client.get(prefix + "/draft", params={"thread_id": THREAD}).json()
    publication = client.post(prefix, json={"snapshot": draft["snapshot"], "thread_id": THREAD}).json()["publication_id"]
    url = f"{prefix}/{publication}"
    member = client.get(url).json()["files"][0]
    folder = member["shared_path"].rsplit("/files/", 1)[0]
    manifest = paths.shared_dir() / folder / "conversation.json"
    original = manifest.read_bytes()
    manifest.write_bytes(b"broken")
    disk = paths.shared_dir() / folder
    (disk / "other-publisher.txt").write_bytes(b"ordinary file")
    (disk / ".private").write_bytes(b"hidden")
    (disk / "unreadable-link").symlink_to(disk / ".private")
    response = client.get(url)
    assert response.status_code == 404 and response.json()["detail"] == {
        "code": "snapshot-folder-available",
        "publication_id": publication,
        "folder": folder,
        "entries": ["conversation.json", "files/", "other-publisher.txt", "transcript.md"],
        "truncated": False,
    }
    assert "no-store" in response.headers["cache-control"]
    assert client.get(url + "/members/" + member["publication_id"]).status_code == 410
    assert client.delete(url).status_code == 404
    for number in range(55):
        (disk / f"additional-{number}.txt").touch()
    limited = client.get(url).json()["detail"]
    assert limited["truncated"] and len(limited["entries"]) == 50
    with monkeypatch.context() as denied:
        denied.setattr(_router_auth_helpers, "_STUB_PERMISSIONS", [p for p in _router_auth_helpers._STUB_PERMISSIONS if p != "threads:read"])
        denied_response = client.get(url)
        assert denied_response.status_code == 403 and folder not in denied_response.text
    assert client.get(prefix + "/missing").json()["detail"] == {"code": "snapshot-unavailable"}
    with monkeypatch.context() as replaced:
        live = await repo.live_publication(folder + "/conversation.json")
        replaced.setattr(repo, "live_publication", AsyncMock(side_effect=[live, {**live, "publication_id": "replacement"}]))
        assert client.get(url).json()["detail"] == {"code": "snapshot-unavailable"}
    # An ordinary removal of the exact manifest must retire even a bookmarked recovery page.
    manifest.write_bytes(original)
    result = client.delete("/api/shared/" + folder + "/conversation.json", params={"expected_publication_id": publication})
    assert result.status_code == 200
    retired = client.get(url)
    assert retired.status_code == 410 and set(retired.json()["detail"]) == {"code", "publisher", "published_at"}


@pytest.mark.anyio
@pytest.mark.parametrize("cap", ["file-count", "path-limit", "source-path-limit", "path-depth", "manifest-limit", "transcript-limit", "message-count", "file-limit", "total-limit"])
async def test_draft_identifies_each_exceeded_limit(share, monkeypatch, cap):
    from app.gateway import snapshot_manifest

    client, _, _ = share
    if cap == "file-limit":
        monkeypatch.setattr(snapshot_publication, "FILE_LIMIT", 3)
    elif cap == "total-limit":
        monkeypatch.setattr(snapshot_publication, "TOTAL_LIMIT", 3)
    elif cap == "file-count":
        monkeypatch.setattr(snapshot_publication, "receipt_paths", AsyncMock(return_value=[f"/mnt/user-data/outputs/{i}" for i in range(65)]))
    elif cap in {"path-limit", "path-depth", "source-path-limit"}:
        relative = {"path-limit": ("n" * 100 + "/") * 10 + "one", "path-depth": "nested/" * 13 + "one", "source-path-limit": ("n" * 255 + "/") * 15 + "n" * 255}[cap]
        monkeypatch.setattr(snapshot_publication, "receipt_paths", AsyncMock(return_value=["/mnt/user-data/outputs/" + relative]))
        monkeypatch.setattr(snapshot_publication, "_read_output", AsyncMock(return_value=b"one"))
    elif cap == "message-count":
        monkeypatch.setattr(snapshot_publication, "read_conversation", AsyncMock(return_value=SimpleNamespace(title="One", messages=[{"type": "ai", "content": "Hi"}] * 10001)))
    else:
        monkeypatch.setattr(snapshot_manifest, "MANIFEST_LIMIT", 200 if cap == "manifest-limit" else 10)
    detail = client.get("/api/shared-conversations/draft", params={"thread_id": THREAD})
    assert detail.status_code == 413 and detail.json()["detail"]["code"] == "snapshot-" + cap
