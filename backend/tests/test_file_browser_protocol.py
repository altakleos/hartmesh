"""Versioned browser reads, bounded operations and mixed Gateway behavior."""

import os
from hashlib import sha256

import httpx
import pytest
from test_agent_instances import BOB, create, request_app
from test_agent_instances import instances as instances_fixture
from test_shared_router import _client, _own_file
from test_shared_router import paths as paths_fixture
from test_shared_router import repo as repo_fixture
from test_storage_feature_routes import THREAD
from test_storage_feature_routes import feature_app as feature_app_fixture

from app.gateway.routers import file_browser
from app.gateway.routers.features import AgentInstancesFeature, FileBrowserFeature, StorageSpacesFeature

instances = instances_fixture
paths = paths_fixture
repo = repo_fixture
feature_app = feature_app_fixture

PREFIX = "/api/file-browser/v1"


def test_incomplete_storage_reports_supported_reads_and_global_outage_withholds_them():
    agents = AgentInstancesFeature(enabled=True)
    for state in ("ready", "incomplete"):
        enabled = FileBrowserFeature.for_storage(StorageSpacesFeature(enabled=True, state=state), agents)
        assert enabled.operations.agent == ["select", "folder", "content", "text"]
    unavailable = FileBrowserFeature.for_storage(StorageSpacesFeature(state="unavailable"), agents)
    assert unavailable.storage_mode == "unavailable" and unavailable.operations.model_dump() == {"personal": [], "shared": [], "agent": []}


@pytest.mark.anyio
async def test_ordinary_selected_entries_follow_native_replacement(paths, repo):
    client, user = _client(repo)
    _own_file(paths, user, "items/item.txt", b"first")
    published = client.post("/api/shared/publish", json={"path": "/mnt/user-data/files/items/item.txt"})
    assert published.status_code == 201
    old = client.post(PREFIX + "/operations", json={"operation": "remove", "path": "item.txt"})
    assert old.status_code == 404
    client.app.include_router(file_browser.router)
    for location, root, path in (("personal", paths.user_files_dir(str(user.id)), "items/item.txt"), ("shared", paths.shared_dir(), "item.txt")):
        url = PREFIX + "/" + location + "/selection"
        first = client.get(url, params={"path": path})
        assert first.status_code == 200, first.text
        before = first.json()
        assert before["path"] == path and before["kind"] == "file" and before["size"] == 5
        replacement = root / "new.txt"
        replacement.write_bytes(b"later")
        os.replace(replacement, root / path)
        current = client.get(url, params={"path": path})
        assert current.status_code == 200 and current.json()["revision"] != before["revision"]
        operation = {"location": location, "path": path, "operation": "remove", "revision": before["revision"], "operation_id": "c" * 32}
        for _ in range(2):
            assert client.post(PREFIX + "/operations", json=operation).status_code == 404
        assert (root / path).read_bytes() == b"later"


@pytest.mark.asyncio
async def test_managed_selection_uses_only_admitted_feature_roots(feature_app):
    client, app, _, catalog, paths, alice, bob, current = feature_app
    app.include_router(file_browser.router)
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(alice.id))
    outputs.mkdir(parents=True)
    (outputs / "sample.txt").write_bytes(b"managed")
    saved = await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/sample.txt"})
    assert saved.status_code == 201, saved.text
    assert (await client.post("/api/shared/publish", json={"path": saved.json()["virtual_path"]})).status_code == 201
    for kind in ("personal", "shared"):
        response = await client.get(PREFIX + f"/{kind}/selection", params={"path": "sample.txt"})
        assert response.status_code == 200 and response.json()["size"] == 7, response.text
        area = "files" if kind == "personal" else "shared"
        item = next(volume.data_path / area / "sample.txt" for volume in catalog.verified.values() if (volume.data_path / area / "sample.txt").is_file())
        new = item.with_name("updated.txt")
        new.write_bytes(b"changed")
        os.replace(new, item)
        current_entry = await client.get(PREFIX + f"/{kind}/selection", params={"path": "sample.txt"})
        assert current_entry.json()["revision"] != response.json()["revision"]
        operation = {"location": kind, "path": "sample.txt", "operation": "remove", "revision": response.json()["revision"], "operation_id": "b" * 32}
        for _ in range(2):
            assert (await client.post(PREFIX + "/operations", json=operation)).status_code == 404
        assert item.read_bytes() == b"changed"
    current[0] = bob
    assert (await client.get(PREFIX + "/personal/selection", params={"path": "sample.txt"})).status_code == 404
    assert (await client.get(PREFIX + "/shared/selection", params={"path": "sample.txt"})).status_code == (await client.get("/api/shared/sample.txt")).status_code == 404
    assert not paths.user_files_dir(str(alice.id)).exists() and not paths.shared_dir().exists()


@pytest.mark.asyncio
async def test_agent_browse_and_open_admit_visible_entries_at_access(instances, monkeypatch):
    service, _, _, catalog = instances
    monkeypatch.setattr("app.gateway.routers.agent_instances.get_agents_api_config", lambda: type("Config", (), {"enabled": True})())
    agent = await create(service)
    app = request_app(service)
    app.include_router(file_browser.router)
    home = next(volume.data_path for volume in catalog.verified.values() if (volume.data_path / "uploads").is_dir())
    folder = home / "outputs" / "notes"
    folder.mkdir()
    (folder / "item.txt").write_bytes(b"report")
    # Native filenames may not be representable in a JSON response.
    (folder / os.fsdecode(b"legacy-\xff")).write_bytes(b"legacy")
    (folder / ".agent-skill-packages").mkdir()
    (folder / "reference").symlink_to("item.txt")
    path = PREFIX + "/agents/" + agent.id
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        result = await client.get(path + "/folder", params={"path": "outputs/notes"})
        assert result.status_code == 200, result.text
        rows = {entry["name"]: entry for entry in result.json()["files"]}
        assert set(rows) == {"item.txt", "reference"}
        bounded = await client.get(path + "/folder", params={"path": "outputs/notes", "limit": 2})
        assert bounded.json()["truncated"] is False and len(bounded.json()["files"]) == 2
        assert rows["reference"]["kind"] == "symlink" and not rows["reference"]["accessible"]
        assert (await client.get(path + "/content", params={"path": rows["reference"]["path"]})).status_code in (400, 404)
        assert (await client.get(path + "/folder", params={"path": "outputs/notes/.agent-skill-packages"})).status_code == 404
        text = await client.get(path + "/text", params={"path": "outputs/notes/item.txt"})
        assert text.status_code == 200, text.text
        assert text.json() == {"text": "report", "sha256": sha256(b"report").hexdigest(), "concurrency": "exclusive-host-window"}
        assert text.headers["cache-control"] == "private, no-store"
        assert (await client.get(path + "/text", params={"path": rows["reference"]["path"]})).status_code in (400, 404)
        for name, content, reason in (("binary.txt", b"bin\0", 415), ("legacy.txt", b"not UTF-8 \xff", 415), ("large.txt", b"x" * ((1 << 20) + 1), 400)):
            (folder / name).write_bytes(content)
            assert (await client.get(path + "/text", params={"path": "outputs/notes/" + name})).status_code == reason
        selected = await client.get(path + "/selection", params={"path": "outputs/notes/item.txt"})
        assert selected.status_code == 200 and selected.json()["size"] == 6
        # A native application may save over a name while the HTTP response is
        # being sent. The range still belongs to the already admitted inode.
        prepare = file_browser.AgentBrowserFileResponse._prepare_owned

        async def replace_after_open(response):
            await prepare(response)
            temporary = folder / "updated.txt"
            temporary.write_bytes(b"latest")
            os.replace(temporary, folder / "item.txt")

        monkeypatch.setattr(file_browser.AgentBrowserFileResponse, "_prepare_owned", replace_after_open)
        response = await client.get(path + "/content", params={"path": "outputs/notes/item.txt"}, headers={"Range": "bytes=0-2"})
        assert response.status_code == 206 and response.content == b"rep"
        updated = await client.get(path + "/selection", params={"path": "outputs/notes/item.txt"})
        assert updated.json()["revision"] != selected.json()["revision"]
        parent = await client.get(path + "/selection", params={"path": "outputs/notes"})
        assert parent.json()["kind"] == "directory"
        folder.rename(home / "outputs" / "previous")
        folder.mkdir()
        (folder / "kept.txt").write_bytes(b"new")
        for action in ("rename", "remove"):
            op = {"location": {"agent": agent.id}, "action": action, "path": "outputs/notes", "revision": parent.json()["revision"], "operation_id": "a" * 32}
            assert (await client.post(PREFIX + "/operations", json=op)).status_code == 404
        assert (folder / "kept.txt").read_bytes() == b"new"
    denied = request_app(service, actor=BOB)
    denied.include_router(file_browser.router)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=denied), base_url="http://test") as client:
        assert (await client.get(path + "/folder")).status_code == 404


@pytest.mark.asyncio
async def test_agent_open_rechecks_inspection_after_route_resolution(instances, monkeypatch):
    from deerflow.agent_instances.contract import AgentPermission
    from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow

    service, _, session_factory, catalog = instances
    monkeypatch.setattr("app.gateway.routers.agent_instances.get_agents_api_config", lambda: type("Config", (), {"enabled": True})())
    agent = await create(service)
    app = request_app(service)
    app.include_router(file_browser.router)
    home = next(volume.data_path for volume in catalog.verified.values() if (volume.data_path / "uploads").is_dir())
    (home / "outputs" / "team.txt").write_bytes(b"team copy")
    original = file_browser.AgentBrowserFileResponse._prepare_owned

    async def admission_changed(response):
        # The person retains the storage grant while their instance permission changes.
        async with session_factory.begin() as session:
            grant = await session.get(AgentInstanceGrantRow, (agent.id, "alice"))
            grant.permissions = int(AgentPermission.USE)
        await original(response)

    monkeypatch.setattr(file_browser.AgentBrowserFileResponse, "_prepare_owned", admission_changed)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        path = PREFIX + "/agents/" + agent.id
        assert (await client.get(path + "/selection", params={"path": "outputs/team.txt"})).status_code == 200
        refused = await client.get(path + "/content", params={"path": "outputs/team.txt"})
        assert refused.status_code == 403 and b"team copy" not in refused.content
        assert (await client.get(path + "/folder", params={"path": "outputs"})).status_code == 404
        assert (await client.get(path + "/text", params={"path": "outputs/team.txt"})).status_code == 404


def test_legacy_full_page_stops_before_inspecting_next_native_name(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from itertools import chain, repeat

    from deerflow.spaces.filesystem import ConfinedFilesystem

    data, control = tmp_path / "data", tmp_path / "control"
    data.mkdir()
    control.mkdir(mode=0o700)
    (data / "report.txt").touch()
    (data / os.fsdecode(b"legacy-\xff")).touch()
    scandir = os.scandir

    @contextmanager
    def ordered_directory(fd):
        with scandir(fd) as stream:
            valid, native = sorted(stream, key=lambda row: row.name != "report.txt")
            yield chain((valid,), repeat(native, 1001))

    with ConfinedFilesystem(data, control) as filesystem, monkeypatch.context() as scoped:
        scoped.setattr(os, "scandir", ordered_directory)
        ordinary, truncated = filesystem.list_directory(limit=1)
        assert [row.name for row in ordinary] == ["report.txt"] and truncated
        browser, truncated = filesystem.list_directory(limit=1, follow_symlinks=False)
        assert [row.name for row in browser] == ["report.txt"] and truncated


@pytest.mark.anyio
async def test_direct_folder_pages_show_only_witnessed_file_groups(paths, repo, monkeypatch):
    monkeypatch.setattr("deerflow.files.folders.get_paths", lambda: paths)
    client, user = _client(repo)
    client.app.include_router(file_browser.router)
    personal = paths.ensure_user_files_dir(str(user.id))
    for area, root in [("personal", personal), ("shared", paths.ensure_shared_dir())]:
        (root / "books" / "archive").mkdir(parents=True)
        (root / "books" / "archive" / "notes.txt").write_text("owned", encoding="utf-8")
        (root / "empty").mkdir()
        (root / "hidden-only").mkdir()
        (root / "hidden-only" / ".private").touch()
        (root / "plain.txt").touch()
        endpoint = f"{PREFIX}/{area}/folder"
        page = client.get(endpoint)
        assert page.status_code == 200, page.text
        assert page.headers["cache-control"] == "private, no-store"
        assert {row["path"]: row["kind"] for row in page.json()["files"]} == {"books": "directory", "plain.txt": "file"}
        assert page.json()["truncated"] is False
        assert client.get(endpoint, params={"path": "books"}).json()["files"][0]["path"] == "books/archive"
        item = client.get(endpoint, params={"path": "books/archive"}).json()["files"][0]
        assert item["name"] == "notes.txt" and item["size"] == 5 and item["url"].endswith("books/archive/notes.txt")
        assert client.get(endpoint, params={"limit": 1}).json()["truncated"] is True
        assert client.get(endpoint, params={"path": "empty"}).status_code == 404
        assert client.get(endpoint, params={"path": "missing"}).status_code == 404
        assert client.get(endpoint, params={"path": "plain.txt"}).status_code == 404
        if area == "shared":
            assert item["can_remove"] is False


@pytest.mark.asyncio
async def test_managed_folder_rows_retain_current_source_and_provenance(feature_app):
    client, app, _, _, paths, alice, bob, current = feature_app
    app.include_router(file_browser.router)
    outputs = paths.sandbox_outputs_dir(THREAD, user_id=str(alice.id))
    outputs.mkdir(parents=True)
    (outputs / "report.txt").write_bytes(b"draft")
    keep = await client.post(f"/api/threads/{THREAD}/files", json={"path": "/mnt/user-data/outputs/report.txt", "folder": "meeting"})
    assert keep.status_code == 201
    share = await client.post("/api/shared/publish", json={"path": keep.json()["virtual_path"], "folder": "company"})
    assert share.status_code == 201
    for area, group in [("personal", "meeting"), ("shared", "company")]:
        result = await client.get(f"{PREFIX}/{area}/folder", params={"path": group})
        assert result.status_code == 200, result.text
        row = result.json()["files"][0]
        assert row["path"] == group + "/report.txt" and row["kind"] == "file"
        if area == "shared":
            assert row["publication_id"] == share.json()["publication_id"] and row["can_remove"]
    current[0] = bob
    assert (await client.get(PREFIX + "/personal/folder", params={"path": "meeting"})).status_code != 200


def test_exhausted_folder_evidence_stays_partial_without_naming_an_unobserved_group(tmp_path, monkeypatch):
    from deerflow.config.paths import Paths, paths_scope
    from deerflow.files import folders

    local = Paths(tmp_path)
    root = local.ensure_shared_dir()
    hidden = root / "example"
    hidden.mkdir()
    (hidden / ".ignored").touch()
    (hidden / "ordinary.txt").touch()
    monkeypatch.setattr(folders, "MAX_FOLDER_SCAN", 1)
    with paths_scope(local):
        response = folders.read_folder(None, "", 500)
        assert response.files == response.folders == [] and response.truncated
        partial = folders.read_folder(None, "example", 500)
        assert partial.truncated


def test_ordinary_inaccessible_group_preserves_readable_siblings(tmp_path, monkeypatch):
    import errno

    from deerflow.config.paths import Paths, paths_scope
    from deerflow.files import folders

    local = Paths(tmp_path)
    root = local.ensure_shared_dir()
    (root / "blocked").mkdir()
    (root / "visible.txt").touch()
    native_open = os.open

    def refuse_group(path, flags, *args, **kwargs):
        if path == "blocked":
            raise PermissionError(errno.EACCES, "unreadable directory")
        return native_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(os, "open", refuse_group)
    with paths_scope(local):
        page = folders.read_folder(None, "", 500)
        assert [file.name for file in page.files] == ["visible.txt"] and not page.folders and not page.truncated
        with pytest.raises(PermissionError):
            folders.read_folder(None, "blocked", 500)


@pytest.mark.asyncio
async def test_managed_missing_collection_root_is_an_outage_for_nested_folder(feature_app):
    client, app, _, catalog, *_ = feature_app
    app.include_router(file_browser.router)
    assert (await client.get(PREFIX + "/shared/folder")).status_code == 200
    root = next(vol.data_path / "shared" for vol in catalog.verified.values() if (vol.data_path / "shared").is_dir())
    root.rename(root.with_name("held"))
    for path in ("", "meeting"):
        response = await client.get(PREFIX + "/shared/folder", params={"path": path})
        assert response.status_code == 503, response.text


@pytest.mark.parametrize("missing_shelf", (True, False))
def test_managed_directory_probe_preserves_backing_failures(tmp_path, missing_shelf):
    from types import SimpleNamespace

    from deerflow.config.paths import Paths, paths_scope
    from deerflow.features.paths import FeaturePaths
    from deerflow.files.folders import read_folder
    from deerflow.spaces.backings import BackingUnavailable
    from deerflow.spaces.filesystem import ConfinedFilesystem

    data = tmp_path / "data"
    control = tmp_path / "control"
    data.mkdir()
    control.mkdir(mode=0o700)
    (data / "shared" / "notes").mkdir(parents=True)
    (data / "shared" / "notes" / "one.txt").touch()
    volume = SimpleNamespace(filesystem=lambda: ConfinedFilesystem(data, control))
    paths = FeaturePaths(Paths(tmp_path), "reader", {"hm.shared": volume})
    with paths_scope(paths):
        assert read_folder(None, "notes", 500).files[0].name == "one.txt"
        if missing_shelf:
            (data / "shared").rename(data / "held")
        else:
            control.chmod(0o755)
        for path in ("", "notes"):
            with pytest.raises(BackingUnavailable):
                read_folder(None, path, 500)
