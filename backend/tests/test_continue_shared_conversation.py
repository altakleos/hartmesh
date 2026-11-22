from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio
from _router_auth_helpers import _make_stub_user, make_authed_test_app
from fastapi.testclient import TestClient
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore

from app.gateway.deps import get_config
from deerflow.persistence.thread_meta.memory import MemoryThreadMetaStore
from deerflow.runtime.events.store.memory import MemoryRunEventStore


@pytest_asyncio.fixture
async def repo(tmp_path):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from deerflow.persistence.shared_publications.model import SharedPublicationRow
    from deerflow.persistence.shared_publications.sql import SharedPublicationRepository

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'published.db'}")
    try:
        async with engine.begin() as conn:
            await conn.run_sync(SharedPublicationRow.__table__.create)
        yield SharedPublicationRepository(async_sessionmaker(engine, expire_on_commit=False))
    finally:
        await engine.dispose()


@pytest.fixture()
def site(monkeypatch):
    from app.gateway.routers import continued_conversations as continued

    user = _make_stub_user()
    app = make_authed_test_app(user_factory=lambda: user, bind_current_user=True, signed_in=True)
    app.include_router(continued.router)
    app.state.checkpointer = InMemorySaver()
    app.state.thread_store = MemoryThreadMetaStore(InMemoryStore())
    app.state.run_event_store = MemoryRunEventStore()
    app.dependency_overrides[get_config] = lambda: SimpleNamespace()
    from app.gateway.conversation_snapshots import AssistantAnswerer

    snapshot = SimpleNamespace(
        answerer=AssistantAnswerer(), title="Shared notes", messages=[SimpleNamespace(type=kind, content=text) for kind, text in [("human", "A question"), ("ai", "One answer"), ("ai", "One more observation")]], files=[]
    )
    reader = AsyncMock(return_value=snapshot)
    monkeypatch.setattr(continued, "read_snapshot", reader)
    with TestClient(app) as client:
        yield SimpleNamespace(client=client, snapshot=snapshot, reader=reader, app=app, user=user)


def test_continue_uses_published_words_and_same_request_repairs_failed_feed(site, monkeypatch):
    url = "/api/shared-conversations/public-copy/continue"
    body = {"request_id": str(uuid4()), "answerer": {"kind": "assistant"}}
    store = site.app.state.run_event_store
    original = store.put_batch
    with monkeypatch.context() as patch:
        patch.setattr(store, "put_batch", AsyncMock(side_effect=OSError("test store unavailable")))
        assert site.client.post(url, json=body).status_code == 503
    response = site.client.post(url, json=body)
    assert response.status_code == 200, response.text
    conversation = response.json()["thread_id"]
    provenance = site.client.get(f"/api/threads/{conversation}/shared-history")
    assert provenance.status_code == 200, provenance.text
    proof = provenance.json()
    assert proof["publication_id"] == "public-copy" and len(set(proof["message_ids"])) == 3
    with monkeypatch.context() as patch:
        repeat_write = AsyncMock(wraps=original)
        patch.setattr(store, "put_batch", repeat_write)
        assert site.client.post(url, json=body).json() == response.json()
        repeat_write.assert_not_awaited()
    assert site.client.get(f"/api/threads/{conversation}/shared-history").json() == proof
    assert site.client.post(url, json={**body, "answerer": {"kind": "agent"}}).status_code in (409, 422)


def test_continue_repairs_only_missing_host_seed_rows(site, monkeypatch):
    url = "/api/shared-conversations/public-copy/continue"
    body = {"request_id": str(uuid4()), "answerer": {"kind": "assistant"}}
    store = site.app.state.run_event_store
    write = store.put_batch

    async def interrupted(rows):
        await write(rows[:1])
        raise OSError("test stop after first lawful seed row")

    with monkeypatch.context() as patch:
        patch.setattr(store, "put_batch", interrupted)
        assert site.client.post(url, json=body).status_code == 503
    with monkeypatch.context() as patch:
        completion = AsyncMock(wraps=write)
        patch.setattr(store, "put_batch", completion)
        retry = site.client.post(url, json=body)
        assert retry.status_code == 200, retry.text
        assert [r["content"]["content"] for r in completion.await_args.args[0]] == ["One answer", "One more observation"]
    assert len(site.client.get("/api/threads/" + retry.json()["thread_id"] + "/shared-history").json()["message_ids"]) == 3


@pytest.mark.asyncio
async def test_continue_imports_only_live_snapshot_files_and_keeps_copies_after_removal(site, tmp_path, monkeypatch, repo):
    from app.gateway import conversation_file_import, conversation_snapshots
    from app.gateway.routers import uploads
    from app.gateway.snapshot_manifest import MANIFEST_SOURCE, TRANSCRIPT_SOURCE, SnapshotMember, digest_of_bytes, freeze_manifest
    from deerflow.config.paths import Paths
    from deerflow.files import shared

    paths = Paths(tmp_path / "ordinary")
    for module in (conversation_file_import, conversation_snapshots, shared):
        monkeypatch.setattr(module, "get_paths", lambda: paths)
    output = tmp_path / "attachments"
    output.mkdir()
    (output / "notes.txt").write_text("My earlier note")
    monkeypatch.setattr(uploads, "ensure_uploads_dir", lambda *args, **kwargs: output)
    monkeypatch.setattr(uploads, "get_sandbox_provider", lambda: None)
    monkeypatch.setattr(uploads, "_uses_thread_data_mounts", lambda provider: True)
    site.app.state.shared_publications_repo = repo
    site.app.dependency_overrides[get_config] = lambda: SimpleNamespace(uploads=SimpleNamespace(auto_convert_documents=False))
    site.reader.side_effect = conversation_snapshots.read_snapshot
    data = b"Meeting notes"
    members = [SnapshotMember(path="files/notes.txt", size=len(data), sha256=digest_of_bytes(data)), SnapshotMember(path="files/retired.txt", size=1, sha256=digest_of_bytes(b"x"))]
    frozen = freeze_manifest("Published discussion", [{"type": "human", "content": "Read our notes"}], members)
    directory = paths.shared_dir() / frozen.folder
    (directory / "files").mkdir(parents=True)
    publisher = str(uuid4())
    for name, raw, source in [("files/notes.txt", data, "/mnt/user-data/outputs/notes.txt"), ("transcript.md", frozen.transcript, TRANSCRIPT_SOURCE), ("conversation.json", frozen.document, MANIFEST_SOURCE)]:
        (directory / name).write_bytes(raw)
        row = await repo.record_publication(path=frozen.folder + "/" + name, size=len(raw), sha256=digest_of_bytes(raw), published_by=publisher, from_path=source, from_thread_id="original")
    url = f"/api/shared-conversations/{row['publication_id']}/continue"
    body = {"request_id": str(uuid4()), "answerer": {"kind": "assistant"}}
    result = site.client.post(url, json=body)
    assert result.status_code == 200, result.text
    copied, missing = result.json()["files"]
    assert copied["path"] == "files/notes.txt" and copied["status"] == "copied"
    stored_name = copied["attachment"]["filename"]
    assert stored_name != "notes.txt" and (output / stored_name).read_bytes() == data
    assert (output / "notes.txt").read_text() == "My earlier note"
    assert missing == {"path": "files/retired.txt", "status": "not_copied", "reason": "no_longer_shared"}
    assert site.client.post(url, json=body).json() == result.json()
    assert len(list(output.iterdir())) == 2
    await repo.record_removal(publication_id=row["publication_id"], removed_by=publisher)
    assert site.client.post(url, json=body).status_code == 410
    assert (output / stored_name).read_bytes() == data
    thread_id = result.json()["thread_id"]
    assert site.client.get(f"/api/threads/{thread_id}/shared-history").json()["publication_id"] == row["publication_id"]


def test_empty_manifest_keeps_its_private_chat_title_on_retry(site):
    site.snapshot.messages.clear()
    params = {"request_id": str(uuid4()), "answerer": {"kind": "assistant"}}
    url = "/api/shared-conversations/empty-copy/continue"
    result = site.client.post(url, json=params)
    assert result.status_code == 200, result.text
    thread_id = result.json()["thread_id"]
    assert site.client.get(f"/api/threads/{thread_id}/shared-history").json() is None
    site.client.portal.call(partial(site.app.state.thread_store.update_display_name, thread_id, "Renamed", user_id=str(site.user.id)))
    assert site.client.post(url, json=params).json() == result.json()
    record = site.client.portal.call(partial(site.app.state.thread_store.get, thread_id, user_id=str(site.user.id)))
    assert record["display_name"] == "Renamed"


def test_snapshot_retired_before_admission_creates_no_thread(site, monkeypatch):
    from fastapi import HTTPException

    site.reader.side_effect = HTTPException(410, {"code": "snapshot_removed"})
    create = AsyncMock(wraps=site.app.state.thread_store.create)
    monkeypatch.setattr(site.app.state.thread_store, "create", create)
    response = site.client.post("/api/shared-conversations/removed/continue", json={"request_id": str(uuid4()), "answerer": {"kind": "assistant"}})
    assert response.status_code == 410
    create.assert_not_awaited()


def test_existing_personal_message_prevents_reseeding_after_interruption(site, monkeypatch):
    from app.gateway.routers import continued_conversations as continued

    params = {"request_id": str(uuid4()), "answerer": {"kind": "assistant"}}
    url = "/api/shared-conversations/public-copy/continue"
    with monkeypatch.context() as patch:
        patch.setattr(continued, "_seed", AsyncMock(side_effect=OSError("before seeding")))
        assert site.client.post(url, json=params).status_code == 503
    thread_id = continued._stable_id("continue-shared-conversation", str(site.user.id), "public-copy", params["request_id"])
    content = {"type": "human", "id": "own", "content": "My own text", "additional_kwargs": {"shared_history": {"publication_id": "public-copy"}}}
    store = site.app.state.run_event_store
    site.client.portal.call(partial(store.put_batch, [{"thread_id": thread_id, "run_id": "first-run", "event_type": "llm.human.input", "category": "message", "content": content}]))
    assert site.client.get(f"/api/threads/{thread_id}/shared-history").json() is None
    assert site.client.post(url, json=params).status_code == 409
    rows = site.client.portal.call(partial(store.list_messages, thread_id, user_id=None))
    assert [r["content"] for r in rows] == [content]


@pytest.mark.parametrize(("detail", "reason"), [({"code": "snapshot-member-unavailable"}, "no_longer_shared"), ({"code": "no-longer-shared"}, "no_longer_shared"), ("File changed during capture", "changed")])
def test_continue_reports_final_file_admission_result(site, monkeypatch, detail, reason):
    from fastapi import HTTPException

    from app.gateway.routers import continued_conversations as continued

    site.snapshot.files = [SimpleNamespace(path="files/report.txt", available=True, publication_id="member", sha256="1" * 64)]
    monkeypatch.setattr(continued, "import_into_chat", AsyncMock(side_effect=HTTPException(409, detail)))
    result = site.client.post("/api/shared-conversations/published/continue", json={"request_id": str(uuid4()), "answerer": {"kind": "assistant"}})
    assert result.status_code == 200
    assert result.json()["files"] == [{"path": "files/report.txt", "status": "not_copied", "reason": reason}]


def test_shared_history_requires_the_feed_read_permission(site, monkeypatch):
    import _router_auth_helpers

    result = site.client.post("/api/shared-conversations/published/continue", json={"request_id": str(uuid4()), "answerer": {"kind": "assistant"}})
    assert result.status_code == 200
    monkeypatch.setattr(_router_auth_helpers, "_STUB_PERMISSIONS", ["threads:read"])
    assert site.client.get("/api/threads/" + result.json()["thread_id"] + "/shared-history").status_code == 403
