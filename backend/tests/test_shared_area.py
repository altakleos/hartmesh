"""The tenant's Shared area: publishing, reading, removing, and what a sandbox may do to it.

Shared is one directory for the whole company, mounted read-only into every
sandbox at ``/mnt/user-data/shared`` and written only by the Gateway's publish
route. Publishing copies the exact bytes of something the person already has,
records who published it and when, and never overwrites: a name already there
keeps both. Removing leaves the record, with who removed it and when.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from deerflow.config.paths import Paths


@pytest.fixture
def shared_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Paths:
    from deerflow.files import shared as shared_module
    from deerflow.files import store as store_module

    paths = Paths(tmp_path / "state")
    monkeypatch.setattr(shared_module, "get_paths", lambda: paths)
    monkeypatch.setattr(store_module, "get_paths", lambda: paths, raising=False)
    return paths


def _source(tmp_path: Path, name: str, content: bytes) -> Path:
    source = tmp_path / "source" / name
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(content)
    return source


# ── The directory ────────────────────────────────────────────────────────


def test_the_shared_directory_is_one_per_tenant_not_per_user(shared_paths: Paths) -> None:
    """Every person at the company reads the same directory; nothing is keyed by user."""
    assert shared_paths.shared_dir() == shared_paths.base_dir / "shared"
    assert shared_paths.ensure_shared_dir().is_dir()
    assert shared_paths.host_shared_dir().endswith("shared")


def test_the_sandbox_sees_shared_under_user_data(shared_paths: Paths) -> None:
    from deerflow.config.paths import SHARED_VIRTUAL_PREFIX

    assert SHARED_VIRTUAL_PREFIX == "/mnt/user-data/shared"


# ── Publishing ───────────────────────────────────────────────────────────


def test_publish_copies_the_exact_bytes_and_says_where_they_landed(shared_paths: Paths, tmp_path: Path) -> None:
    from deerflow.files.shared import publish_file

    content = b"%PDF-1.7 august review"
    published = publish_file(_source(tmp_path, "august.pdf", content), name="august.pdf", folder="Reports")

    assert published.path == "Reports/august.pdf"
    assert published.name == "august.pdf"
    assert published.size == len(content)
    assert published.sha256 == hashlib.sha256(content).hexdigest()
    assert published.virtual_path == "/mnt/user-data/shared/Reports/august.pdf"
    assert published.url == "/api/shared/Reports/august.pdf"
    assert (shared_paths.shared_dir() / "Reports" / "august.pdf").read_bytes() == content


def test_publishing_a_name_already_there_keeps_both(shared_paths: Paths, tmp_path: Path) -> None:
    """Nothing published is ever overwritten, by anyone."""
    from deerflow.files.shared import publish_file

    first = publish_file(_source(tmp_path, "august.pdf", b"first"), name="august.pdf", folder="Reports")
    second = publish_file(_source(tmp_path, "other.pdf", b"second"), name="august.pdf", folder="Reports")

    assert first.path == "Reports/august.pdf"
    assert second.path != first.path
    assert second.path.startswith("Reports/august")
    assert (shared_paths.shared_dir() / first.path).read_bytes() == b"first"
    assert (shared_paths.shared_dir() / second.path).read_bytes() == b"second"


def test_publish_refuses_a_link_and_anything_that_is_not_a_file(shared_paths: Paths, tmp_path: Path) -> None:
    from deerflow.files.shared import SharedFileError, publish_file

    directory = tmp_path / "source" / "a-folder"
    directory.mkdir(parents=True)
    with pytest.raises(SharedFileError):
        publish_file(directory, name="a-folder", folder=None)

    real = _source(tmp_path, "real.txt", b"real")
    link = tmp_path / "source" / "link.txt"
    try:
        link.symlink_to(real)
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege is not available")
        raise
    with pytest.raises(SharedFileError):
        publish_file(link, name="link.txt", folder=None)


def test_publish_refuses_a_folder_that_is_not_a_plain_path(shared_paths: Paths, tmp_path: Path) -> None:
    from deerflow.files.shared import SharedFileError, publish_file

    source = _source(tmp_path, "x.txt", b"x")
    for folder in ("../escape", "Reports/../..", ".hidden", "a/" * 20):
        with pytest.raises(SharedFileError):
            publish_file(source, name="x.txt", folder=folder)


# ── Reading ──────────────────────────────────────────────────────────────


def test_listing_shows_what_anyone_published_sorted_by_path(shared_paths: Paths, tmp_path: Path) -> None:
    from deerflow.files.shared import list_shared_files, publish_file

    publish_file(_source(tmp_path, "b.txt", b"b"), name="b.txt", folder="Reports")
    publish_file(_source(tmp_path, "a.txt", b"a"), name="a.txt", folder="Exports")
    publish_file(_source(tmp_path, "c.txt", b"c"), name="c.txt", folder=None)

    entries, truncated = list_shared_files()

    assert [entry.path for entry in entries] == ["Exports/a.txt", "Reports/b.txt", "c.txt"]
    assert truncated is False


def test_listing_an_empty_or_absent_shared_area_is_not_an_error(shared_paths: Paths) -> None:
    from deerflow.files.shared import list_shared_files

    assert list_shared_files() == ([], False)


def test_resolving_refuses_a_path_that_leaves_the_shared_area(shared_paths: Paths, tmp_path: Path) -> None:
    from deerflow.files.shared import SharedFileError, resolve_shared_file

    outside = tmp_path / "outside.txt"
    outside.write_bytes(b"not shared")
    shared_paths.ensure_shared_dir()
    link = shared_paths.shared_dir() / "escape.txt"
    try:
        link.symlink_to(outside)
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege is not available")
        raise

    with pytest.raises(SharedFileError):
        resolve_shared_file("../outside.txt")
    with pytest.raises(SharedFileError):
        resolve_shared_file("escape.txt")


# ── Removing ─────────────────────────────────────────────────────────────


def test_removing_takes_the_file_and_leaves_the_folder(shared_paths: Paths, tmp_path: Path) -> None:
    from deerflow.files.shared import publish_file, remove_shared_file

    published = publish_file(_source(tmp_path, "august.pdf", b"pdf"), name="august.pdf", folder="Reports")
    remove_shared_file(published.path)

    assert not (shared_paths.shared_dir() / published.path).exists()
    assert (shared_paths.shared_dir() / "Reports").is_dir()


def test_removing_something_that_is_not_there_says_so(shared_paths: Paths) -> None:
    from deerflow.files.shared import remove_shared_file

    shared_paths.ensure_shared_dir()
    with pytest.raises(FileNotFoundError):
        remove_shared_file("Reports/never.pdf")


# ── The mount ────────────────────────────────────────────────────────────


def test_every_aio_sandbox_mounts_shared_read_only(shared_paths: Paths, monkeypatch: pytest.MonkeyPatch) -> None:
    """The sandbox reads Shared and cannot write it: publication is the Gateway's alone."""
    import deerflow.community.aio_sandbox.aio_sandbox_provider as aio
    from deerflow.config.paths import SHARED_VIRTUAL_PREFIX

    monkeypatch.setattr(aio, "get_paths", lambda: shared_paths)

    mounts = aio.AioSandboxProvider._get_thread_mounts("11111111-1111-1111-1111-111111111111", user_id="owner-a")

    assert (shared_paths.host_shared_dir(), SHARED_VIRTUAL_PREFIX, True) in mounts
    assert shared_paths.shared_dir().is_dir(), "the mount source exists before the container binds it"
    # Nobody's private area is any less private for it: the person's files
    # are still theirs alone and read-write.
    assert (shared_paths.host_user_files_dir("owner-a"), "/mnt/user-data/files", False) in mounts


def test_every_local_sandbox_mounts_shared_read_only(shared_paths: Paths, monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    from deerflow.config.paths import SHARED_VIRTUAL_PREFIX
    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider

    config = SimpleNamespace(skills=SimpleNamespace(container_path="/mnt/skills"))
    monkeypatch.setattr("deerflow.config.get_app_config", lambda: config)
    monkeypatch.setattr("deerflow.config.paths.get_paths", lambda: shared_paths)
    monkeypatch.setattr("deerflow.sandbox.local.local_sandbox_provider.get_paths", lambda: shared_paths, raising=False)

    mappings = LocalSandboxProvider._build_thread_path_mappings("11111111-1111-1111-1111-111111111111", user_id="owner-a", accepted_skills_only=True)

    [shared] = [mapping for mapping in mappings if mapping.container_path == SHARED_VIRTUAL_PREFIX]
    assert shared.read_only is True
    assert Path(shared.local_path) == shared_paths.shared_dir()


# ── What the agent may do with it ────────────────────────────────────────


def _thread_data(paths: Paths) -> dict[str, str]:
    """The thread paths the local sandbox tools resolve against."""
    from deerflow.agents.middlewares.thread_data_middleware import ThreadDataMiddleware

    middleware = ThreadDataMiddleware(base_dir=paths.base_dir)
    return middleware._get_thread_paths("11111111-1111-1111-1111-111111111111", user_id="owner-a")


def test_a_local_agent_reads_a_shared_file_through_its_virtual_path(shared_paths: Paths, tmp_path: Path) -> None:
    """Asked about something a colleague shared, the agent can actually open it."""
    from deerflow.config.paths import SHARED_VIRTUAL_PREFIX
    from deerflow.files.shared import publish_file
    from deerflow.sandbox.tools import _resolve_and_validate_user_data_path

    published = publish_file(_source(tmp_path, "review.pdf", b"july"), name="review.pdf", folder="Reports")
    thread_data = _thread_data(shared_paths)

    resolved = _resolve_and_validate_user_data_path(f"{SHARED_VIRTUAL_PREFIX}/{published.path}", thread_data)

    assert Path(resolved) == shared_paths.shared_dir() / published.path
    assert Path(resolved).read_bytes() == b"july"


def test_a_local_agent_cannot_write_into_shared(shared_paths: Paths) -> None:
    """Publishing stays the person's decision through the Gateway, never the agent's write."""
    from deerflow.config.paths import SHARED_VIRTUAL_PREFIX
    from deerflow.sandbox.tools import validate_local_tool_path

    thread_data = _thread_data(shared_paths)
    target = f"{SHARED_VIRTUAL_PREFIX}/Reports/forged.pdf"

    validate_local_tool_path(target, thread_data, read_only=True)
    with pytest.raises(PermissionError, match="read-only|not allowed"):
        validate_local_tool_path(target, thread_data)
