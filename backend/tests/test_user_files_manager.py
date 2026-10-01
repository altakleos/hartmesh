"""The person's own files: kept across conversations, reached by relative path."""

import stat
from pathlib import Path

import pytest

from deerflow.config.paths import Paths
from deerflow.files import manager


@pytest.fixture
def paths(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Paths:
    paths = Paths(tmp_path)
    monkeypatch.setattr(manager, "get_paths", lambda: paths)
    return paths


def _symlink_to_or_skip(link_path: Path, target_path: Path) -> None:
    try:
        link_path.symlink_to(target_path)
    except OSError as exc:
        if getattr(exc, "winerror", None) == 1314:
            pytest.skip("Windows symlink privilege is not available")
        raise


# ---------- normalize_relative_path ----------


@pytest.mark.parametrize(
    ("given", "expected"),
    [
        ("august.pdf", "august.pdf"),
        ("Reports/august.pdf", "Reports/august.pdf"),
        ("/Reports/august.pdf", "Reports/august.pdf"),
        ("Reports//august.pdf", "Reports/august.pdf"),
        ("Reports/august.pdf/", "Reports/august.pdf"),
        ("Réunion août/rapport.pdf", "Réunion août/rapport.pdf"),
    ],
)
def test_normalize_relative_path_accepts_plain_paths(given: str, expected: str) -> None:
    assert manager.normalize_relative_path(given) == expected


@pytest.mark.parametrize(
    "given",
    [
        "",
        "/",
        ".",
        "..",
        "../secret",
        "Reports/../../secret",
        "Reports/./august.pdf",
        "Reports\\august.pdf",
        ".hidden/august.pdf",
        "Reports/.august.pdf",
        "a\x00b",
        "/".join(["d"] * (manager.MAX_PATH_DEPTH + 1)) + "/f",
    ],
)
def test_normalize_relative_path_refuses_what_cannot_name_a_file(given: str) -> None:
    with pytest.raises(manager.UserFileError):
        manager.normalize_relative_path(given)


def test_normalize_relative_path_may_name_a_folder() -> None:
    assert manager.normalize_relative_path("", allow_empty=True) == ""
    assert manager.normalize_relative_path("/", allow_empty=True) == ""
    assert manager.normalize_relative_path("Reports/2026", allow_empty=True) == "Reports/2026"


# ---------- list_user_files ----------


def test_list_user_files_walks_folders_and_skips_what_is_not_a_regular_file(paths: Paths) -> None:
    root = paths.ensure_user_files_dir("u1")
    (root / "Reports").mkdir()
    (root / "Reports" / "august.pdf").write_bytes(b"pdf")
    (root / "notes.txt").write_bytes(b"hello")
    (root / ".hidden").write_bytes(b"x")
    (root / ".scratch").mkdir()
    (root / ".scratch" / "tmp").write_bytes(b"x")
    (root / "empty").mkdir()
    _symlink_to_or_skip(root / "link.txt", root / "notes.txt")

    listed, truncated = manager.list_user_files("u1")

    assert truncated is False
    assert [entry.path for entry in listed] == ["Reports/august.pdf", "notes.txt"]
    august = listed[0]
    assert august.name == "august.pdf"
    assert august.size == 3
    assert august.virtual_path == "/mnt/user-data/files/Reports/august.pdf"
    assert august.url == "/api/files/Reports/august.pdf"
    assert august.modified == pytest.approx((root / "Reports" / "august.pdf").stat().st_mtime)


def test_list_user_files_of_a_person_with_no_files_is_empty(paths: Paths) -> None:
    assert manager.list_user_files("nobody") == ([], False)


def test_list_user_files_percent_encodes_the_url(paths: Paths) -> None:
    root = paths.ensure_user_files_dir("u1")
    (root / "Q3 report #2.pdf").write_bytes(b"pdf")

    listed, _ = manager.list_user_files("u1")

    assert listed[0].url == "/api/files/Q3%20report%20%232.pdf"


def test_list_user_files_skips_a_name_the_address_rules_refuse(paths: Paths) -> None:
    """The sandbox may write any name; the person is shown only what they can open and remove."""
    root = paths.ensure_user_files_dir("u1")
    (root / "fine.txt").write_bytes(b"x")
    (root / "back\\slash.txt").write_bytes(b"x")

    listed, _ = manager.list_user_files("u1")

    assert [entry.path for entry in listed] == ["fine.txt"]


def test_keep_file_refuses_a_folder_that_is_a_file(paths: Paths, tmp_path: Path) -> None:
    root = paths.ensure_user_files_dir("u1")
    (root / "Reports").write_bytes(b"not a folder")
    source = tmp_path / "august.pdf"
    source.write_bytes(b"pdf")

    with pytest.raises(manager.UserFileError):
        manager.keep_file("u1", source, name="august.pdf", folder="Reports")


# ---------- resolve_user_file ----------


def test_resolve_user_file_stays_inside_the_owner(paths: Paths) -> None:
    root = paths.ensure_user_files_dir("u1")
    (root / "Reports").mkdir()
    (root / "Reports" / "august.pdf").write_bytes(b"pdf")

    assert manager.resolve_user_file("u1", "Reports/august.pdf") == root / "Reports" / "august.pdf"


def test_resolve_user_file_refuses_symlinked_segments(paths: Paths, tmp_path: Path) -> None:
    root = paths.ensure_user_files_dir("u1")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_bytes(b"secret")
    _symlink_to_or_skip(root / "escape", outside)
    _symlink_to_or_skip(root / "escape.txt", outside / "secret.txt")

    with pytest.raises(manager.UserFileError):
        manager.resolve_user_file("u1", "escape/secret.txt")
    with pytest.raises(manager.UserFileError):
        manager.resolve_user_file("u1", "escape.txt")


# ---------- delete_user_file ----------


def test_delete_user_file_removes_only_a_regular_file(paths: Paths) -> None:
    root = paths.ensure_user_files_dir("u1")
    (root / "Reports").mkdir()
    (root / "Reports" / "august.pdf").write_bytes(b"pdf")

    manager.delete_user_file("u1", "Reports/august.pdf")

    assert not (root / "Reports" / "august.pdf").exists()
    assert (root / "Reports").is_dir()
    with pytest.raises(FileNotFoundError):
        manager.delete_user_file("u1", "Reports/august.pdf")
    with pytest.raises(manager.UserFileError):
        manager.delete_user_file("u1", "Reports")


# ---------- keep_file ----------


def test_keep_file_copies_the_exact_bytes_into_the_folder(paths: Paths, tmp_path: Path) -> None:
    source = tmp_path / "outputs" / "august.pdf"
    source.parent.mkdir()
    source.write_bytes(b"%PDF-1.7 exact bytes")

    kept = manager.keep_file("u1", source, name="august.pdf", folder="Reports")

    root = paths.user_files_dir("u1")
    assert kept.path == "Reports/august.pdf"
    assert kept.name == "august.pdf"
    assert kept.size == len(b"%PDF-1.7 exact bytes")
    assert (root / "Reports" / "august.pdf").read_bytes() == b"%PDF-1.7 exact bytes"
    # The sandbox rewrites the copy as its own uid, like an upload.
    assert stat.S_IMODE((root / "Reports" / "august.pdf").stat().st_mode) & 0o666 == 0o666
    assert stat.S_IMODE((root / "Reports").stat().st_mode) == 0o777
    assert stat.S_IMODE(root.stat().st_mode) == 0o777


def test_keep_file_keeps_both_on_a_name_that_exists(paths: Paths, tmp_path: Path) -> None:
    source = tmp_path / "august.pdf"
    source.write_bytes(b"second")
    root = paths.ensure_user_files_dir("u1")
    (root / "august.pdf").write_bytes(b"first")

    kept = manager.keep_file("u1", source, name="august.pdf")

    assert kept.path == "august_1.pdf"
    assert (root / "august.pdf").read_bytes() == b"first"
    assert (root / "august_1.pdf").read_bytes() == b"second"


def test_keep_file_normalizes_the_name_and_refuses_a_bad_folder(paths: Paths, tmp_path: Path) -> None:
    source = tmp_path / "august.pdf"
    source.write_bytes(b"x")

    kept = manager.keep_file("u1", source, name="/tmp/../august.pdf")
    assert kept.path == "august.pdf"

    with pytest.raises(manager.UserFileError):
        manager.keep_file("u1", source, name="august.pdf", folder="../outside")
    with pytest.raises(manager.UserFileError):
        manager.keep_file("u1", source, name="..")


# ---------- links planted by the sandbox ----------


def test_keep_file_refuses_a_folder_that_is_a_link(paths: Paths, tmp_path: Path) -> None:
    """The sandbox can plant a link inside the person's files; a keep must not write through it."""
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    root = paths.ensure_user_files_dir("u1")
    _symlink_to_or_skip(root / "pwn", outside)
    source = tmp_path / "planted.txt"
    source.write_bytes(b"planted")

    with pytest.raises(manager.UserFileError):
        manager.keep_file("u1", source, name="planted.txt", folder="pwn")
    with pytest.raises(manager.UserFileError):
        manager.keep_file("u1", source, name="planted.txt", folder="pwn/deeper")

    assert list(outside.iterdir()) == []
    assert stat.S_IMODE(outside.stat().st_mode) == 0o700


def test_keep_file_refuses_a_source_that_is_a_link(paths: Paths, tmp_path: Path) -> None:
    """The copy opens the source itself without following a link, whatever a preflight saw."""
    secret = tmp_path / "secret.txt"
    secret.write_bytes(b"secret")
    link = tmp_path / "out.pdf"
    _symlink_to_or_skip(link, secret)
    root = paths.ensure_user_files_dir("u1")

    with pytest.raises(manager.UserFileError):
        manager.keep_file("u1", link, name="out.pdf")

    assert list(root.iterdir()) == []


def test_keep_file_refuses_a_source_that_is_not_a_regular_file(paths: Paths, tmp_path: Path) -> None:
    with pytest.raises(manager.UserFileError):
        manager.keep_file("u1", tmp_path, name="dir")


def test_delete_user_file_refuses_a_link_and_a_linked_folder(paths: Paths, tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "victim.txt").write_bytes(b"x")
    root = paths.ensure_user_files_dir("u1")
    _symlink_to_or_skip(root / "escape", outside)
    _symlink_to_or_skip(root / "link.txt", outside / "victim.txt")

    with pytest.raises(manager.UserFileError):
        manager.delete_user_file("u1", "escape/victim.txt")
    with pytest.raises(manager.UserFileError):
        manager.delete_user_file("u1", "link.txt")

    assert (outside / "victim.txt").exists()
    assert (root / "link.txt").is_symlink()


def test_list_user_files_shows_only_what_can_be_addressed(paths: Paths) -> None:
    """A file deeper than the address rule allows is not listed rather than listed and unreachable."""
    root = paths.ensure_user_files_dir("u1")
    deep = root.joinpath(*["d"] * manager.MAX_PATH_DEPTH)
    deep.mkdir(parents=True)
    (deep / "unreachable.txt").write_bytes(b"x")
    at_limit = root.joinpath(*["d"] * (manager.MAX_PATH_DEPTH - 1)) / "reachable.txt"
    at_limit.write_bytes(b"x")

    listed, _ = manager.list_user_files("u1")

    assert [entry.path for entry in listed] == [at_limit.relative_to(root).as_posix()]
    manager.resolve_user_file("u1", listed[0].path)
