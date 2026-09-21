"""The Shared API: publishing to the company, reading it, and taking something back."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.routers import files as files_router
from app.gateway.routers import shared as shared_router
from deerflow.config.paths import Paths
from deerflow.files import manager, shared
from deerflow.runtime.user_context import reset_current_user, set_current_user

THREAD = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Paths:
    paths = Paths(tmp_path / "state")
    for module in (manager, shared, files_router):
        monkeypatch.setattr(module, "get_paths", lambda: paths, raising=False)
    monkeypatch.setattr("app.gateway.path_utils.get_paths", lambda: paths)
    return paths


@pytest.fixture
async def repo(tmp_path: Path):
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine
    from deerflow.persistence.shared_publications import SharedPublicationRepository

    await init_engine("sqlite", url=f"sqlite+aiosqlite:///{tmp_path / 'shared.db'}", sqlite_dir=str(tmp_path))
    try:
        yield SharedPublicationRepository(get_session_factory())
    finally:
        await close_engine()


async def _all_records(repo) -> list[dict]:
    """Every publication and removal the table holds, oldest first."""
    from sqlalchemy import select

    from deerflow.persistence.shared_publications.model import SharedPublicationRow

    async with repo._sf() as session:
        rows = (await session.execute(select(SharedPublicationRow).order_by(SharedPublicationRow.published_at.asc()))).scalars().all()
        return [row.to_dict() for row in rows]


def _user(user_id: str, *, role: str = "user") -> User:
    return User(email=f"{user_id}@example.com", password_hash="x", system_role=role, id=uuid4())


def _client(repo, user: User | None = None, *, owner_check_passes: bool = True) -> tuple[TestClient, User]:
    stub = user or _user("someone")
    app = make_authed_test_app(user_factory=lambda: stub, owner_check_passes=owner_check_passes)

    @app.middleware("http")
    async def publish_current_user(request, call_next):
        token = set_current_user(stub)
        try:
            return await call_next(request)
        finally:
            reset_current_user(token)

    app.state.shared_publications_repo = repo
    app.include_router(shared_router.router)
    return TestClient(app), stub


def _own_file(paths: Paths, user: User, relative: str, content: bytes) -> str:
    target = paths.ensure_user_files_dir(str(user.id)) / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    return f"/mnt/user-data/files/{relative}"


def _thread_output(paths: Paths, user: User, name: str, content: bytes) -> str:
    directory = paths.sandbox_outputs_dir(THREAD, user_id=str(user.id))
    directory.mkdir(parents=True, exist_ok=True)
    (directory / name).write_bytes(content)
    return f"/mnt/user-data/outputs/{name}"


# ---------- publishing ----------


@pytest.mark.anyio
async def test_publishing_ones_own_file_makes_it_everyones(paths: Paths, repo) -> None:
    client, user = _client(repo)
    source = _own_file(paths, user, "Reports/august.pdf", b"%PDF august")

    with client:
        response = client.post("/api/shared/publish", json={"path": source, "folder": "Reports"})
        assert response.status_code == 201, response.text
        published = response.json()
        assert published["path"] == "Reports/august.pdf"
        # No users table is wired into this app, so nobody can be named; the
        # record still holds the id (see the publisher-name tests below).
        assert published["published_by"] is None
        assert published["published_at"] is not None
        assert published["from_thread_id"] is None
        assert published["url"] == "/api/shared/Reports/august.pdf"

    # Somebody else at the company sees it, with who put it there.
    colleague, _ = _client(repo, _user("colleague"))
    with colleague:
        listing = colleague.get("/api/shared").json()
        assert [entry["path"] for entry in listing["files"]] == ["Reports/august.pdf"]
        assert listing["files"][0]["can_remove"] is False, "a colleague may read it, not take it back"
        assert colleague.get("/api/shared/Reports/august.pdf").content == b"%PDF august"
    with client:
        assert client.get("/api/shared").json()["files"][0]["can_remove"] is True, "the publisher may"
    assert (paths.ensure_user_files_dir(str(user.id)) / "Reports" / "august.pdf").read_bytes() == b"%PDF august", "the source is untouched"


@pytest.mark.anyio
async def test_publishing_a_conversations_output_records_the_conversation(paths: Paths, repo) -> None:
    client, user = _client(repo)
    source = _thread_output(paths, user, "report.docx", b"docx")

    with client:
        response = client.post("/api/shared/publish", json={"path": source, "thread_id": THREAD})
        assert response.status_code == 201, response.text
        assert response.json()["from_thread_id"] == THREAD
    assert (paths.shared_dir() / "report.docx").read_bytes() == b"docx"


@pytest.mark.anyio
async def test_a_conversation_that_is_not_the_callers_cannot_be_published_from(paths: Paths, repo) -> None:
    client, user = _client(repo, owner_check_passes=False)
    source = _thread_output(paths, user, "report.docx", b"docx")

    with client:
        assert client.post("/api/shared/publish", json={"path": source, "thread_id": THREAD}).status_code == 404
    assert not (paths.shared_dir() / "report.docx").exists()


@pytest.mark.anyio
async def test_publishing_needs_a_thread_for_a_conversation_file_and_refuses_other_places(paths: Paths, repo) -> None:
    client, user = _client(repo)
    source = _thread_output(paths, user, "report.docx", b"docx")

    with client:
        assert client.post("/api/shared/publish", json={"path": source}).status_code == 400
        assert client.post("/api/shared/publish", json={"path": "/mnt/user-data/workspace/scratch.txt"}).status_code == 400
        assert client.post("/api/shared/publish", json={"path": "/mnt/user-data/files/missing.txt"}).status_code == 404
        # The source exists, so this is the folder being refused, not a miss.
        _own_file(paths, user, "x.txt", b"x")
        assert client.post("/api/shared/publish", json={"path": "/mnt/user-data/files/x.txt", "folder": "../out"}).status_code == 400


@pytest.mark.anyio
async def test_publishing_a_taken_name_keeps_both(paths: Paths, repo) -> None:
    client, user = _client(repo)
    first = _own_file(paths, user, "a/august.pdf", b"first")
    second = _own_file(paths, user, "b/august.pdf", b"second")

    with client:
        one = client.post("/api/shared/publish", json={"path": first, "folder": "Reports"}).json()
        two = client.post("/api/shared/publish", json={"path": second, "folder": "Reports"}).json()
        listing = client.get("/api/shared").json()

    assert one["path"] != two["path"]
    assert {entry["path"] for entry in listing["files"]} == {one["path"], two["path"]}
    assert (paths.shared_dir() / one["path"]).read_bytes() == b"first"
    assert (paths.shared_dir() / two["path"]).read_bytes() == b"second"


# ---------- sharing the same thing again ----------


@pytest.mark.anyio
async def test_sharing_the_same_file_again_finds_it_there_instead_of_copying(paths: Paths, repo) -> None:
    """A second click, a second tab, a second day: the bytes are already there, so nothing is copied."""
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"%PDF august")

    with client:
        first = client.post("/api/shared/publish", json={"path": source})
        assert first.status_code == 201, first.text
        again = client.post("/api/shared/publish", json={"path": source})
        assert again.status_code == 200, again.text
        assert again.json()["path"] == first.json()["path"] == "august.pdf"
        assert again.json()["can_remove"] is True
        listing = client.get("/api/shared").json()

    assert [entry["path"] for entry in listing["files"]] == ["august.pdf"]
    assert len(await _all_records(repo)) == 1, "no second record either"


@pytest.mark.anyio
async def test_the_same_bytes_under_another_name_in_the_same_folder_are_already_there(paths: Paths, repo) -> None:
    client, user = _client(repo)
    original = _own_file(paths, user, "august.pdf", b"%PDF august")
    renamed = _own_file(paths, user, "august-final.pdf", b"%PDF august")

    with client:
        client.post("/api/shared/publish", json={"path": original, "folder": "Reports"})
        again = client.post("/api/shared/publish", json={"path": renamed, "folder": "Reports"})
        assert again.status_code == 200, again.text
        assert again.json()["path"] == "Reports/august.pdf", "the answer names the copy that is there"
        listing = client.get("/api/shared").json()

    assert [entry["path"] for entry in listing["files"]] == ["Reports/august.pdf"]


@pytest.mark.anyio
async def test_the_same_bytes_in_another_folder_are_a_new_publication(paths: Paths, repo) -> None:
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"%PDF august")

    with client:
        client.post("/api/shared/publish", json={"path": source, "folder": "Reports"})
        elsewhere = client.post("/api/shared/publish", json={"path": source, "folder": "Archive"})
        assert elsewhere.status_code == 201, elsewhere.text
        assert elsewhere.json()["path"] == "Archive/august.pdf"


@pytest.mark.anyio
async def test_a_file_shared_then_changed_is_shared_again_beside_the_old_one(paths: Paths, repo) -> None:
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"%PDF august v1")

    with client:
        one = client.post("/api/shared/publish", json={"path": source}).json()
        _own_file(paths, user, "august.pdf", b"%PDF august v2")
        two = client.post("/api/shared/publish", json={"path": source})
        assert two.status_code == 201, two.text

    assert one["path"] == "august.pdf"
    assert two.json()["path"] == "august_1.pdf"
    assert (paths.shared_dir() / "august.pdf").read_bytes() == b"%PDF august v1"
    assert (paths.shared_dir() / "august_1.pdf").read_bytes() == b"%PDF august v2"


@pytest.mark.anyio
async def test_a_file_taken_out_of_shared_can_be_shared_again(paths: Paths, repo) -> None:
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"%PDF august")

    with client:
        client.post("/api/shared/publish", json={"path": source})
        assert client.delete("/api/shared/august.pdf").status_code == 200
        again = client.post("/api/shared/publish", json={"path": source})
        assert again.status_code == 201, again.text
        assert again.json()["path"] == "august.pdf"

    assert (paths.shared_dir() / "august.pdf").read_bytes() == b"%PDF august"


@pytest.mark.anyio
async def test_a_record_whose_file_is_gone_by_hand_does_not_stop_a_fresh_copy(paths: Paths, repo) -> None:
    """The record says it is there; the disk says otherwise. The disk wins and the person gets their file shared."""
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"%PDF august")

    with client:
        client.post("/api/shared/publish", json={"path": source})
        (paths.shared_dir() / "august.pdf").unlink()
        again = client.post("/api/shared/publish", json={"path": source})
        assert again.status_code == 201, again.text

    assert (paths.shared_dir() / "august.pdf").read_bytes() == b"%PDF august"


@pytest.mark.anyio
async def test_a_colleagues_identical_file_is_the_one_already_there(paths: Paths, repo) -> None:
    """Shared holds bytes for the company; who put them there first is the record's story, not a reason for a second copy."""
    client, user = _client(repo)
    colleague, other = _client(repo, _user("colleague"))
    mine = _own_file(paths, user, "price-list.xlsx", b"prices")
    theirs = _own_file(paths, other, "price-list.xlsx", b"prices")

    with client:
        first = client.post("/api/shared/publish", json={"path": mine}).json()
    with colleague:
        again = colleague.post("/api/shared/publish", json={"path": theirs})
        assert again.status_code == 200, again.text
        assert again.json()["path"] == first["path"]
        assert again.json()["can_remove"] is False, "it is still the first publisher's to take back"

    records = await _all_records(repo)
    assert len(records) == 1
    assert records[0]["published_by"] == str(user.id)


@pytest.mark.anyio
async def test_a_record_whose_file_was_replaced_by_hand_yields_one_fresh_copy_not_one_per_click(paths: Paths, repo) -> None:
    """The first record's file no longer holds the bytes; the fresh copy does, and every later share finds that one."""
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"%PDF august")

    with client:
        client.post("/api/shared/publish", json={"path": source})
        (paths.shared_dir() / "august.pdf").write_bytes(b"replaced by hand")
        fresh = client.post("/api/shared/publish", json={"path": source})
        assert fresh.status_code == 201, fresh.text
        assert fresh.json()["path"] == "august_1.pdf"
        again = client.post("/api/shared/publish", json={"path": source})
        assert again.status_code == 200, again.text
        assert again.json()["path"] == "august_1.pdf"
        listing = client.get("/api/shared").json()

    assert [entry["path"] for entry in listing["files"]] == ["august.pdf", "august_1.pdf"]
    assert len(await _all_records(repo)) == 2


@pytest.mark.anyio
async def test_a_conversations_output_shared_twice_lands_once(paths: Paths, repo) -> None:
    client, user = _client(repo)
    source = _thread_output(paths, user, "brief.pdf", b"%PDF brief")

    with client:
        first = client.post("/api/shared/publish", json={"path": source, "thread_id": THREAD, "folder": "Reports"})
        assert first.status_code == 201, first.text
        again = client.post("/api/shared/publish", json={"path": source, "thread_id": THREAD, "folder": "Reports"})
        assert again.status_code == 200, again.text
        assert again.json()["path"] == first.json()["path"]
        assert again.json()["from_thread_id"] == THREAD

    assert len(await _all_records(repo)) == 1


@pytest.mark.anyio
async def test_the_record_store_answers_every_live_publication_of_the_bytes_in_one_folder(repo) -> None:
    """Folder equality, not prefix; removed rows never; earliest first."""

    async def record(path: str) -> dict:
        return await repo.record_publication(path=path, size=1, sha256="x" * 64, published_by="p", from_thread_id=None, from_path=None)

    root = await record("a.pdf")
    reports_first = await record("Reports/a.pdf")
    removed = await record("Reports/b.pdf")
    await repo.record_removal(removed["publication_id"], removed_by="p")
    await record("Reports/Sub/a.pdf")
    reports_second = await record("Reports/a_1.pdf")
    await repo.record_publication(path="Reports/other.pdf", size=1, sha256="y" * 64, published_by="p", from_thread_id=None, from_path=None)

    assert [row["publication_id"] for row in await repo.live_publications_holding("x" * 64, folder="")] == [root["publication_id"]]
    assert [row["path"] for row in await repo.live_publications_holding("x" * 64, folder="Reports")] == ["Reports/a.pdf", "Reports/a_1.pdf"]
    assert [row["publication_id"] for row in await repo.live_publications_holding("x" * 64, folder="Reports")] == [reports_first["publication_id"], reports_second["publication_id"]]
    assert await repo.live_publications_holding("x" * 64, folder="Archive") == []
    assert await repo.live_publications_holding("y" * 64, folder="") == []


@pytest.mark.anyio
async def test_two_publishes_of_the_same_bytes_at_once_land_once(paths: Paths, repo, monkeypatch) -> None:
    """Two tabs, or a retried request: both digest at the same time, one copies, the other finds it."""
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor

    both_arrived = threading.Barrier(2, timeout=10)
    real_digest = shared_router.digest_of

    def slow_digest(source: Path) -> str:
        both_arrived.wait()
        time.sleep(0.05)
        return real_digest(source)

    monkeypatch.setattr(shared_router, "digest_of", slow_digest)
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"%PDF august")

    with client, ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(lambda _: client.post("/api/shared/publish", json={"path": source}), range(2)))
        listing = client.get("/api/shared").json()

    assert sorted(response.status_code for response in responses) == [200, 201], [response.text for response in responses]
    assert {response.json()["path"] for response in responses} == {"august.pdf"}
    assert [entry["path"] for entry in listing["files"]] == ["august.pdf"]
    assert len(await _all_records(repo)) == 1


@pytest.mark.anyio
async def test_a_record_store_that_cannot_be_read_shares_nothing(paths: Paths, repo, monkeypatch) -> None:
    async def _refuse(*_args, **_kwargs):
        raise RuntimeError("the record store is having a moment")

    monkeypatch.setattr(repo, "live_publications_holding", _refuse)
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"pdf")

    with client:
        response = client.post("/api/shared/publish", json={"path": source})
        assert response.status_code == 503, response.text

    assert list(paths.ensure_shared_dir().iterdir()) == []


@pytest.mark.anyio
async def test_a_source_swapped_for_a_link_after_the_preflight_is_refused_not_a_crash(paths: Paths, repo, monkeypatch) -> None:
    """The sandbox writes the person's files; what the digest opens is checked the way the copy checks it."""
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"%PDF august")
    real = paths.ensure_user_files_dir(str(user.id)) / "august.pdf"
    elsewhere = paths.ensure_user_files_dir(str(user.id)) / "elsewhere.pdf"
    elsewhere.write_bytes(b"other")
    real_source = shared_router._own_file_source

    def swap_then_resolve(user_id: str, virtual_path: str) -> Path:
        resolved = real_source(user_id, virtual_path)
        real.unlink()
        try:
            real.symlink_to(elsewhere)
        except OSError as exc:
            if getattr(exc, "winerror", None) == 1314:
                pytest.skip("Windows symlink privilege is not available")
            raise
        return resolved

    monkeypatch.setattr(shared_router, "_own_file_source", swap_then_resolve)
    with client:
        response = client.post("/api/shared/publish", json={"path": source})
        assert response.status_code == 400, response.text

    assert list(paths.ensure_shared_dir().iterdir()) == []


# ---------- reading ----------


@pytest.mark.anyio
async def test_reading_never_lets_the_browser_run_what_was_published(paths: Paths, repo) -> None:
    client, user = _client(repo)
    page = _own_file(paths, user, "page.html", b"<script>alert(1)</script>")
    with client:
        client.post("/api/shared/publish", json={"path": page})
        response = client.get("/api/shared/page.html")
    assert response.status_code == 200
    assert response.headers["content-disposition"].startswith("attachment")
    assert response.headers["x-content-type-options"] == "nosniff"


@pytest.mark.anyio
async def test_reading_a_missing_or_bad_path_says_so(paths: Paths, repo) -> None:
    client, _ = _client(repo)
    with client:
        assert client.get("/api/shared/Reports/never.pdf").status_code == 404
        assert client.get("/api/shared/..%2Fescape").status_code in (400, 404)


@pytest.mark.anyio
async def test_a_file_placed_by_hand_is_listed_without_a_publisher(paths: Paths, repo) -> None:
    client, _ = _client(repo)
    root = paths.ensure_shared_dir()
    (root / "Exports").mkdir()
    (root / "Exports" / "jobs.xlsx").write_bytes(b"xlsx")
    with client:
        listing = client.get("/api/shared").json()
    assert listing["files"][0]["path"] == "Exports/jobs.xlsx"
    assert listing["files"][0]["published_by"] is None
    assert listing["files"][0]["can_remove"] is False
    admin, _ = _client(repo, _user("boss", role="admin"))
    with admin:
        assert admin.get("/api/shared").json()["files"][0]["can_remove"] is True


# ---------- removing ----------


@pytest.mark.anyio
async def test_the_publisher_can_remove_and_the_record_says_who_did(paths: Paths, repo) -> None:
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"pdf")
    with client:
        client.post("/api/shared/publish", json={"path": source})
        response = client.delete("/api/shared/august.pdf")
        assert response.status_code == 200, response.text
        assert client.get("/api/shared").json()["files"] == []
    assert not (paths.shared_dir() / "august.pdf").exists()
    [record] = await _all_records(repo)
    assert record["published_by"] == str(user.id)
    assert record["removed_by"] == str(user.id)
    assert record["removed_at"] is not None


@pytest.mark.anyio
async def test_a_colleague_cannot_remove_but_an_admin_can(paths: Paths, repo) -> None:
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"pdf")
    with client:
        client.post("/api/shared/publish", json={"path": source})

    colleague, _ = _client(repo, _user("colleague"))
    with colleague:
        assert colleague.delete("/api/shared/august.pdf").status_code == 403
    assert (paths.shared_dir() / "august.pdf").exists(), "a refusal removes nothing"

    admin, admin_user = _client(repo, _user("boss", role="admin"))
    with admin:
        assert admin.delete("/api/shared/august.pdf").status_code == 200
    assert not (paths.shared_dir() / "august.pdf").exists()
    [record] = await _all_records(repo)
    assert record["removed_by"] == str(admin_user.id)


@pytest.mark.anyio
async def test_a_file_without_a_record_is_an_admins_to_remove(paths: Paths, repo) -> None:
    root = paths.ensure_shared_dir()
    (root / "orphan.txt").write_bytes(b"x")
    client, _ = _client(repo)
    with client:
        assert client.delete("/api/shared/orphan.txt").status_code == 403
    admin, _ = _client(repo, _user("boss", role="admin"))
    with admin:
        assert admin.delete("/api/shared/orphan.txt").status_code == 200
        assert admin.delete("/api/shared/orphan.txt").status_code == 404


@pytest.mark.anyio
async def test_without_a_record_store_the_routes_say_the_service_is_unavailable(paths: Paths) -> None:
    client, _ = _client(None)
    with client:
        assert client.get("/api/shared").status_code == 503


@pytest.mark.anyio
async def test_the_publisher_is_recognised_whatever_way_the_path_is_spelled(paths: Paths, repo) -> None:
    """An equivalent spelling of the same file finds the same record, so the publisher is not refused their own file."""
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"pdf")
    with client:
        client.post("/api/shared/publish", json={"path": source, "folder": "Reports"})
        response = client.delete("/api/shared//Reports//august.pdf")
        assert response.status_code == 200, response.text

    assert not (paths.shared_dir() / "Reports" / "august.pdf").exists()
    [record] = await _all_records(repo)
    assert record["removed_by"] == str(user.id), "the record is closed, not left live forever"


@pytest.mark.anyio
async def test_a_removal_closes_the_record_it_read_not_whatever_holds_the_name_now(paths: Paths, repo) -> None:
    """A colleague republishing the same name mid-removal keeps their own live record."""
    client, user = _client(repo)
    first = _own_file(paths, user, "august.pdf", b"first")
    with client:
        client.post("/api/shared/publish", json={"path": first})
    removed_id = (await repo.live_publication("august.pdf"))["publication_id"]

    # The record the remover read, then a second publication claiming the same
    # path before the removal is written.
    republished = await repo.record_publication(path="august.pdf", size=1, sha256="x", published_by="colleague", from_thread_id=None, from_path=None)
    await repo.record_removal(removed_id, removed_by=str(user.id))

    live = await repo.live_publication("august.pdf")
    assert live is not None, "the newer publication is still live"
    assert live["publication_id"] == republished["publication_id"]


@pytest.mark.anyio
async def test_the_listing_names_the_publisher_the_way_a_colleague_would_know_them(paths: Paths, repo, monkeypatch) -> None:
    """A stored user id means nothing to the person reading the page; their colleague's name does."""
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"pdf")

    class _Users:
        async def get_user_by_id(self, user_id: str):
            return user if user_id == str(user.id) else None

    monkeypatch.setattr(shared_router, "get_user_repo", lambda: _Users())

    with client:
        published = client.post("/api/shared/publish", json={"path": source}).json()
        [listed] = client.get("/api/shared").json()["files"]

    assert published["published_by"] == user.email
    assert listed["published_by"] == user.email


@pytest.mark.anyio
async def test_a_listing_still_renders_when_the_publisher_is_no_longer_an_account(paths: Paths, repo, monkeypatch) -> None:
    """Somebody leaving the company must not empty the page for everyone else."""
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"pdf")

    class _NoUsers:
        async def get_user_by_id(self, user_id: str):
            return None

    monkeypatch.setattr(shared_router, "get_user_repo", lambda: _NoUsers())

    with client:
        client.post("/api/shared/publish", json={"path": source})
        [listed] = client.get("/api/shared").json()["files"]

    assert listed["published_by"] is None
    assert listed["path"] == "august.pdf", "the file is still listed"


@pytest.mark.anyio
async def test_a_conversation_with_no_owner_is_not_everyones_to_publish(paths: Paths, repo) -> None:
    """A legacy thread nobody owns may be readable, but handing its outputs to the whole company is not."""
    from unittest.mock import AsyncMock

    client, user = _client(repo)
    source = _thread_output(paths, user, "report.docx", b"docx")
    thread_id = "11111111-1111-1111-1111-111111111111"
    # ``check_access`` still says yes — an unowned row admits every caller —
    # so only the owner-filtered read stands between them and publishing.
    client.app.state.thread_store.get = AsyncMock(return_value=None)

    with client:
        response = client.post("/api/shared/publish", json={"path": source, "thread_id": thread_id})

    assert response.status_code == 404, response.text
    assert list(paths.ensure_shared_dir().iterdir()) == []


@pytest.mark.anyio
async def test_a_publication_that_cannot_be_recorded_shares_nothing(paths: Paths, repo, monkeypatch) -> None:
    """A file in Shared that nobody can be shown as the publisher of is worse than a refusal."""

    async def _refuse(**_kwargs):
        raise RuntimeError("the record store is having a moment")

    monkeypatch.setattr(repo, "record_publication", _refuse)
    client, user = _client(repo)
    source = _own_file(paths, user, "august.pdf", b"pdf")

    with client:
        response = client.post("/api/shared/publish", json={"path": source})
        assert response.status_code == 503, response.text
        assert client.get("/api/shared").json()["files"] == []

    assert list(paths.ensure_shared_dir().iterdir()) == [], "the copy went back out"
    assert (paths.ensure_user_files_dir(str(user.id)) / "august.pdf").exists(), "the person's own file is untouched"
