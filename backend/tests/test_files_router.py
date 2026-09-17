"""The Files API: the person's own files, and keeping a conversation's file there."""

import threading
from pathlib import Path
from uuid import uuid4

import pytest
from _router_auth_helpers import make_authed_test_app
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.routers import files as files_router
from deerflow.config.paths import Paths
from deerflow.files import manager
from deerflow.runtime.user_context import reset_current_user, set_current_user

THREAD = "11111111-1111-1111-1111-111111111111"


@pytest.fixture
def paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Paths:
    paths = Paths(tmp_path)
    monkeypatch.setattr(manager, "get_paths", lambda: paths)
    monkeypatch.setattr(files_router, "get_paths", lambda: paths)
    # The keep route resolves the conversation's file through the shared
    # thread-path resolver, which reads the singleton.
    monkeypatch.setattr("app.gateway.path_utils.get_paths", lambda: paths)
    return paths


def _user(user_id: str) -> User:
    return User(email=f"{user_id}@example.com", password_hash="x", system_role="user", id=uuid4())


def _client(user: User | None = None, *, owner_check_passes: bool = True) -> tuple[TestClient, User]:
    stub = user or _user("someone")
    app = make_authed_test_app(user_factory=lambda: stub, owner_check_passes=owner_check_passes)

    # The production auth middleware also publishes the request's user through
    # the ``user_context`` contextvar, which is where every per-user route reads
    # its owner; the stub middleware only stamps ``request.state``.
    @app.middleware("http")
    async def publish_current_user(request, call_next):
        token = set_current_user(stub)
        try:
            return await call_next(request)
        finally:
            reset_current_user(token)

    app.include_router(files_router.router)
    return TestClient(app), stub


def _files_of(paths: Paths, user: User) -> Path:
    return paths.ensure_user_files_dir(str(user.id))


def _symlink_to_or_skip(link_path: Path, target_path: Path) -> None:
    try:
        link_path.symlink_to(target_path)
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege is not available")
        raise


# ---------- GET /api/files ----------


def test_list_files_is_the_callers_own(paths: Paths) -> None:
    client, user = _client()
    root = _files_of(paths, user)
    (root / "Reports").mkdir()
    (root / "Reports" / "august.pdf").write_bytes(b"pdf")
    (root / "notes.txt").write_bytes(b"hello")
    stranger = paths.ensure_user_files_dir("stranger")
    (stranger / "secret.txt").write_bytes(b"no")

    with client:
        response = client.get("/api/files")

    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert body["truncated"] is False
    assert [entry["path"] for entry in body["files"]] == ["Reports/august.pdf", "notes.txt"]
    august = body["files"][0]
    assert august == {
        "path": "Reports/august.pdf",
        "name": "august.pdf",
        "size": 3,
        "modified": pytest.approx((root / "Reports" / "august.pdf").stat().st_mtime),
        "virtual_path": "/mnt/user-data/files/Reports/august.pdf",
        "url": "/api/files/Reports/august.pdf",
    }


def test_list_files_offloads_the_walk_from_the_event_loop(paths: Paths, monkeypatch: pytest.MonkeyPatch) -> None:
    client, _user_ = _client()
    loop_thread = threading.get_ident()
    called_from: list[int] = []

    def record(user_id: str):
        called_from.append(threading.get_ident())
        return [], False

    monkeypatch.setattr(files_router, "list_user_files", record)

    with client:
        assert client.get("/api/files").status_code == 200

    assert called_from and called_from[0] != loop_thread


# ---------- GET /api/files/{path} ----------


def test_get_file_streams_the_callers_file(paths: Paths) -> None:
    client, user = _client()
    root = _files_of(paths, user)
    (root / "notes.txt").write_bytes(b"hello files")

    with client:
        inline = client.get("/api/files/notes.txt")
        download = client.get("/api/files/notes.txt", params={"download": "true"})

    assert inline.status_code == 200
    assert inline.content == b"hello files"
    assert inline.headers["content-type"].startswith("text/plain")
    assert inline.headers["content-disposition"].startswith("inline;")
    assert download.headers["content-disposition"].startswith("attachment;")


def test_get_file_forces_active_content_to_download(paths: Paths) -> None:
    client, user = _client()
    root = _files_of(paths, user)
    (root / "page.html").write_bytes(b"<script>alert(1)</script>")

    with client:
        response = client.get("/api/files/page.html")

    assert response.status_code == 200
    assert response.headers["content-disposition"].startswith("attachment;")


def test_get_file_answers_404_for_a_missing_file_and_400_for_a_bad_path(paths: Paths) -> None:
    client, user = _client()
    _files_of(paths, user)

    with client:
        missing = client.get("/api/files/nope.txt")
        folder = client.get("/api/files/Reports")
        hidden = client.get("/api/files/.env")

    assert missing.status_code == 404
    assert folder.status_code == 404
    assert hidden.status_code == 400


def test_get_file_refuses_a_symlink(paths: Paths, tmp_path: Path) -> None:
    client, user = _client()
    root = _files_of(paths, user)
    secret = tmp_path / "secret.txt"
    secret.write_bytes(b"secret")
    _symlink_to_or_skip(root / "link.txt", secret)

    with client:
        response = client.get("/api/files/link.txt")

    assert response.status_code == 400


# ---------- DELETE /api/files/{path} ----------


def test_delete_file_removes_the_callers_file(paths: Paths) -> None:
    client, user = _client()
    root = _files_of(paths, user)
    (root / "Reports").mkdir()
    (root / "Reports" / "august.pdf").write_bytes(b"pdf")

    with client:
        response = client.delete("/api/files/Reports/august.pdf")
        again = client.delete("/api/files/Reports/august.pdf")

    assert response.status_code == 200
    assert response.json() == {"success": True, "message": "Deleted Reports/august.pdf"}
    assert not (root / "Reports" / "august.pdf").exists()
    assert again.status_code == 404


def test_delete_file_refuses_a_folder(paths: Paths) -> None:
    client, user = _client()
    root = _files_of(paths, user)
    (root / "Reports").mkdir()

    with client:
        response = client.delete("/api/files/Reports")

    assert response.status_code == 400
    assert (root / "Reports").is_dir()


# ---------- POST /api/threads/{thread_id}/files ----------


def _thread_output(paths: Paths, user: User, name: str, content: bytes, *, kind: str = "outputs") -> Path:
    paths.ensure_thread_dirs(THREAD, user_id=str(user.id))
    directory = paths.sandbox_user_data_dir(THREAD, user_id=str(user.id)) / kind
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / name
    target.write_bytes(content)
    return target


def test_keep_copies_a_conversation_file_into_my_files(paths: Paths) -> None:
    client, user = _client()
    _thread_output(paths, user, "august.pdf", b"%PDF exact")

    with client:
        response = client.post(
            f"/api/threads/{THREAD}/files",
            json={"path": "/mnt/user-data/outputs/august.pdf", "folder": "Reports"},
        )

    assert response.status_code == 201, response.text
    body = response.json()
    assert body["path"] == "Reports/august.pdf"
    assert body["name"] == "august.pdf"
    assert body["size"] == len(b"%PDF exact")
    assert body["virtual_path"] == "/mnt/user-data/files/Reports/august.pdf"
    assert body["url"] == "/api/files/Reports/august.pdf"
    kept = paths.user_files_dir(str(user.id)) / "Reports" / "august.pdf"
    assert kept.read_bytes() == b"%PDF exact"


def test_keep_accepts_an_upload_and_keeps_both_on_a_taken_name(paths: Paths) -> None:
    client, user = _client()
    _thread_output(paths, user, "jobs.xlsx", b"second", kind="uploads")
    root = _files_of(paths, user)
    (root / "jobs.xlsx").write_bytes(b"first")

    with client:
        response = client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/uploads/jobs.xlsx"})

    assert response.status_code == 201, response.text
    assert response.json()["path"] == "jobs_1.xlsx"
    assert (root / "jobs.xlsx").read_bytes() == b"first"
    assert (root / "jobs_1.xlsx").read_bytes() == b"second"


def test_keep_takes_only_uploads_and_outputs(paths: Paths) -> None:
    client, user = _client()
    _thread_output(paths, user, "scratch.py", b"x", kind="workspace")

    with client:
        workspace = client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/workspace/scratch.py"})
        outside = client.post(f"/api/threads/{THREAD}/files", json={"path": "/etc/passwd"})
        own = client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/files/a.txt"})

    assert workspace.status_code == 400
    assert outside.status_code == 400
    assert own.status_code == 400


def test_keep_answers_404_for_a_missing_source(paths: Paths) -> None:
    client, user = _client()
    paths.ensure_thread_dirs(THREAD, user_id=str(user.id))

    with client:
        response = client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/nope.pdf"})

    assert response.status_code == 404


def test_keep_refuses_a_symlinked_source(paths: Paths, tmp_path: Path) -> None:
    client, user = _client()
    paths.ensure_thread_dirs(THREAD, user_id=str(user.id))
    secret = tmp_path / "secret.txt"
    secret.write_bytes(b"secret")
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(user.id))
    _symlink_to_or_skip(outputs / "link.txt", secret)

    with client:
        response = client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/link.txt"})

    # The thread resolver follows the link and finds it outside the thread.
    assert response.status_code == 403
    assert not (paths.user_files_dir(str(user.id)) / "link.txt").exists()


def test_keep_is_refused_on_a_thread_the_caller_does_not_own(paths: Paths) -> None:
    client, user = _client(owner_check_passes=False)
    _thread_output(paths, user, "august.pdf", b"pdf")

    with client:
        response = client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/august.pdf"})

    assert response.status_code == 404
    assert not (paths.user_files_dir(str(user.id)) / "august.pdf").exists()


def test_keep_refuses_a_bad_folder(paths: Paths) -> None:
    client, user = _client()
    _thread_output(paths, user, "august.pdf", b"pdf")

    with client:
        response = client.post(
            f"/api/threads/{THREAD}/files",
            json={"path": "/mnt/user-data/outputs/august.pdf", "folder": "../outside"},
        )

    assert response.status_code == 400
