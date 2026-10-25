"""Opted-in inventory uses positive facts from the existing single scan."""

import os

import pytest

from deerflow.workspace_changes import get_changed_output_paths
from deerflow.workspace_changes.scanner import EXCLUDED_DIR_NAMES, scan_workspace_roots
from deerflow.workspace_changes.types import WorkspaceChangeLimits, WorkspaceRoot


def root(tmp_path):
    return WorkspaceRoot("outputs", tmp_path / "outputs", "/mnt/user-data/outputs")


def observations(snapshot):
    return [(item.name, item.virtual_prefix, item.state) for item in snapshot.root_observations]


def test_inventory_distinguishes_opened_empty_from_missing_without_another_open(tmp_path):
    outputs = root(tmp_path)
    missing = scan_workspace_roots([outputs], include_text=False, observe_roots=True)
    outputs.host_path.mkdir()
    calls = []

    def opener(item):
        calls.append(item)
        return os.open(item.host_path, os.O_RDONLY | os.O_DIRECTORY)

    empty = scan_workspace_roots([outputs], include_text=False, root_opener=opener, observe_roots=True)
    assert missing.files == empty.files == {}
    assert not missing.truncated and not empty.truncated
    assert observations(missing) == [("outputs", "/mnt/user-data/outputs", "missing")]
    assert observations(empty) == [("outputs", "/mnt/user-data/outputs", "opened")]
    assert calls == [outputs]


def test_inventory_records_unavailable_root_and_keeps_existing_truncation(tmp_path):
    def denied(_):
        raise PermissionError()

    snapshot = scan_workspace_roots([root(tmp_path)], root_opener=denied, include_text=False, observe_roots=True)
    assert snapshot.truncated
    assert observations(snapshot) == [("outputs", "/mnt/user-data/outputs", "unavailable")]


def test_inventory_scope_is_the_exact_frozen_exclusion_set(tmp_path):
    outputs = root(tmp_path)
    outputs.host_path.mkdir()
    custom = outputs.host_path / "feedback"
    custom.mkdir()
    (custom / "internal.txt").write_text("process feedback", encoding="utf-8")
    (outputs.host_path / "visible.txt").write_text("visible", encoding="utf-8")
    snapshot = scan_workspace_roots([outputs], include_text=False, observe_roots=True, extra_excluded_dir_names=frozenset({"feedback"}))
    assert snapshot.scan_excluded_dir_names == tuple(sorted(EXCLUDED_DIR_NAMES | {"feedback"}))
    assert list(snapshot.files) == ["/mnt/user-data/outputs/visible.txt"]


def test_inventory_root_opened_does_not_hide_incomplete_walk(tmp_path):
    outputs = root(tmp_path)
    outputs.host_path.mkdir()
    for name in ("a.txt", "b.txt"):
        (outputs.host_path / name).write_text(name, encoding="utf-8")
    snapshot = scan_workspace_roots([outputs], include_text=False, observe_roots=True, limits=WorkspaceChangeLimits(max_scanned_files=1))
    assert snapshot.truncated
    assert observations(snapshot) == [("outputs", "/mnt/user-data/outputs", "opened")]


def test_inventory_scan_retains_regular_change_semantics(tmp_path):
    outputs = root(tmp_path)
    outputs.host_path.mkdir()
    (outputs.host_path / "changed.txt").write_text("old", encoding="utf-8")
    (outputs.host_path / "deleted.txt").write_text("old", encoding="utf-8")
    before = scan_workspace_roots([outputs], include_text=False, observe_roots=True)
    (outputs.host_path / "changed.txt").write_text("new", encoding="utf-8")
    (outputs.host_path / "created.txt").write_text("new", encoding="utf-8")
    (outputs.host_path / "deleted.txt").unlink()
    (outputs.host_path / "link.txt").symlink_to("created.txt")
    after = scan_workspace_roots([outputs], include_text=False, observe_roots=True)
    assert get_changed_output_paths(before, after) == ["/mnt/user-data/outputs/changed.txt", "/mnt/user-data/outputs/created.txt"]


@pytest.mark.parametrize("observe", [False, True])
def test_legacy_scan_and_opted_in_scan_enumerate_the_same_files(tmp_path, observe):
    outputs = root(tmp_path)
    outputs.host_path.mkdir()
    (outputs.host_path / "file.txt").write_text("data", encoding="utf-8")
    snapshot = scan_workspace_roots([outputs], include_text=False, observe_roots=observe)
    assert list(snapshot.files) == ["/mnt/user-data/outputs/file.txt"]
    assert bool(snapshot.root_observations) is observe
