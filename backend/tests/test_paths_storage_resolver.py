"""Host-selected thread routing is pure, opt-in, and leaves legacy paths intact."""

import errno
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

import deerflow.config.paths as paths_module
from deerflow.config.paths import Paths


def test_resolver_preserves_thread_leaf_layout_and_other_user_fallback(tmp_path):
    paths = Paths(tmp_path / "legacy")
    root = tmp_path / "volume" / "data"
    paths.thread_root_resolver = lambda user: root if user == "alice" else None
    thread = root / "conversations" / "t1"
    assert paths.thread_dir("t1", user_id="alice") == thread
    assert paths.sandbox_user_data_dir("t1", user_id="alice") == thread / "user-data"
    for leaf in ("workspace", "uploads", "outputs"):
        method = getattr(paths, f"sandbox_{'work' if leaf == 'workspace' else leaf}_dir")
        assert method("t1", user_id="alice") == thread / "user-data" / leaf
    assert paths.acp_workspace_dir("t1", user_id="alice") == thread / "acp-workspace"
    assert paths.thread_skills_view_dir("t1", user_id="alice") == thread / "skills_view"
    assert paths.resolve_virtual_path("t1", "/mnt/user-data/outputs/a.txt", user_id="alice") == thread / "user-data" / "outputs" / "a.txt"
    assert paths.thread_dir("t1", user_id="bob") == tmp_path / "legacy" / "users" / "bob" / "threads" / "t1"
    # My Files and Shared require their own admitted adapters in the next slice.
    assert paths.user_files_dir("alice") == tmp_path / "legacy" / "users" / "alice" / "files"


def test_host_thread_sources_use_projected_root(tmp_path, monkeypatch):
    paths = Paths(tmp_path / "legacy")
    root = tmp_path / "volume" / "data"
    paths.thread_root_resolver = lambda _: root
    monkeypatch.setenv("DEER_FLOW_HOST_BASE_DIR", "/different/legacy-host")
    thread = root / "conversations" / "t1"
    assert paths.host_thread_dir("t1", user_id="alice") == str(thread)
    assert paths.host_sandbox_user_data_dir("t1", user_id="alice") == str(thread / "user-data")
    for leaf in ("workspace", "uploads", "outputs"):
        method = getattr(paths, f"host_sandbox_{'work' if leaf == 'workspace' else leaf}_dir")
        assert method("t1", user_id="alice") == str(thread / "user-data" / leaf)
    assert paths.host_acp_workspace_dir("t1", user_id="alice") == str(thread / "acp-workspace")
    assert paths.host_thread_skills_view_dir("t1", user_id="alice") == str(thread / "skills_view")


@pytest.mark.parametrize("method", ["thread_dir", "host_thread_dir", "ensure_thread_dirs"])
@pytest.mark.parametrize("user,thread", [("../alice", "t1"), ("alice", "../t1"), ("alice", "/t1")])
def test_invalid_ids_never_reach_resolver(tmp_path, method, user, thread):
    paths = Paths(tmp_path / "legacy")
    paths.thread_root_resolver = resolver = Mock(return_value=tmp_path / "data")
    with pytest.raises(ValueError):
        getattr(paths, method)(thread, user_id=user)
    resolver.assert_not_called()
    assert not (tmp_path / "legacy").exists()


@pytest.mark.parametrize("method", ["thread_dir", "host_thread_dir", "ensure_thread_dirs"])
@pytest.mark.parametrize("root", [Path("relative"), "/absolute/string", 5])
def test_invalid_resolved_root_never_creates_legacy_data(tmp_path, method, root):
    paths = Paths(tmp_path / "legacy")
    paths.thread_root_resolver = lambda _: root
    with pytest.raises((TypeError, ValueError)):
        getattr(paths, method)("t1", user_id="alice")
    assert not (tmp_path / "legacy").exists()


@pytest.mark.parametrize("method", ["thread_dir", "host_thread_dir", "ensure_thread_dirs"])
def test_resolver_failure_propagates_without_fallback(tmp_path, method):
    paths = Paths(tmp_path / "legacy")
    paths.thread_root_resolver = Mock(side_effect=RuntimeError("host preparation failed"))
    with pytest.raises(RuntimeError, match="host preparation failed"):
        getattr(paths, method)("t1", user_id="alice")
    assert not (tmp_path / "legacy").exists()


@pytest.mark.parametrize("install_resolver", [False, True])
def test_none_and_absent_resolver_preserve_windows_host_sources(tmp_path, monkeypatch, install_resolver):
    paths = Paths(tmp_path)
    if install_resolver:
        paths.thread_root_resolver = lambda _: None
    monkeypatch.setenv("DEER_FLOW_HOST_BASE_DIR", r"C:\Users\operator\.deer-flow")
    assert paths.host_thread_dir("t1", user_id="alice") == r"C:\Users\operator\.deer-flow\users\alice\threads\t1"
    assert paths.host_sandbox_work_dir("t1", user_id="alice") == r"C:\Users\operator\.deer-flow\users\alice\threads\t1\user-data\workspace"
    assert paths.thread_dir("t1", user_id="alice") == tmp_path / "users" / "alice" / "threads" / "t1"


def test_anonymous_thread_does_not_call_user_resolver(tmp_path):
    paths = Paths(tmp_path)
    paths.thread_root_resolver = resolver = Mock(side_effect=AssertionError("anonymous thread has no user root"))
    assert paths.thread_dir("t1") == tmp_path / "threads" / "t1"
    assert paths.host_thread_dir("t1") == str(tmp_path / "threads" / "t1")
    paths.ensure_thread_dirs("t1")
    resolver.assert_not_called()


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
def test_projected_ensure_resolves_once_and_only_changes_thread_nodes(tmp_path):
    paths = Paths(tmp_path / "legacy")
    root = tmp_path / "volume" / "data"
    conversations = root / "conversations"
    conversations.mkdir(parents=True)
    root.chmod(0o750)
    conversations.chmod(0o710)
    paths.thread_root_resolver = resolver = Mock(side_effect=[root, tmp_path / "another-volume"])
    paths.ensure_thread_dirs("t1", user_id="alice")
    resolver.assert_called_once_with("alice")
    thread = conversations / "t1"
    directories = [thread, thread / "user-data", thread / "acp-workspace"]
    directories += [thread / "user-data" / leaf for leaf in ("workspace", "uploads", "outputs")]
    assert all(directory.stat().st_mode & 0o777 == 0o770 for directory in directories)
    assert root.stat().st_mode & 0o777 == 0o750
    assert conversations.stat().st_mode & 0o777 == 0o710
    assert not (tmp_path / "legacy").exists()
    assert not (tmp_path / "another-volume").exists()


@pytest.mark.skipif(os.name == "nt", reason="POSIX permission bits")
@pytest.mark.parametrize("install_resolver", [False, True])
def test_absent_or_none_resolver_keeps_legacy_leaf_modes(tmp_path, install_resolver):
    paths = Paths(tmp_path)
    if install_resolver:
        paths.thread_root_resolver = lambda _: None
    paths.ensure_thread_dirs("t1", user_id="alice")
    thread = tmp_path / "users" / "alice" / "threads" / "t1"
    leaves = [thread / "user-data" / name for name in ("workspace", "uploads", "outputs")]
    leaves.append(thread / "acp-workspace")
    assert all(directory.stat().st_mode & 0o777 == 0o777 for directory in leaves)


def test_resolver_is_per_instance_and_routes_branch_source_and_target(tmp_path):
    first, second = Paths(tmp_path / "first"), Paths(tmp_path / "second")
    root = tmp_path / "volume" / "data"
    first.thread_root_resolver = lambda _: root
    assert first.thread_dir("source", user_id="alice") == root / "conversations" / "source"
    assert first.thread_dir("target", user_id="alice") == root / "conversations" / "target"
    assert second.thread_dir("source", user_id="alice") == tmp_path / "second" / "users" / "alice" / "threads" / "source"
    first.thread_root_resolver = None
    assert first.thread_dir("source", user_id="alice") == tmp_path / "first" / "users" / "alice" / "threads" / "source"


def test_projected_path_resolution_performs_no_filesystem_verification(tmp_path):
    paths = Paths(tmp_path / "legacy")
    root = tmp_path / "volume" / "data"
    paths.thread_root_resolver = lambda _: root
    with (
        patch.object(Path, "resolve", side_effect=AssertionError("routing must be pure")),
        patch.object(Path, "stat", side_effect=AssertionError("routing must be pure")),
        patch.object(Path, "exists", side_effect=AssertionError("routing must be pure")),
    ):
        local = paths.thread_dir("t1", user_id="alice")
        host = paths.host_thread_dir("t1", user_id="alice")
    assert local == root / "conversations" / "t1"
    assert host == str(root / "conversations" / "t1")


@pytest.mark.skipif(os.name == "nt", reason="POSIX descriptor confinement")
@pytest.mark.parametrize("parts", [("conversations",), ("conversations", "t1"), ("conversations", "t1", "user-data"), ("conversations", "t1", "user-data", "workspace")])
def test_projected_creation_rejects_symlink_ancestors_without_external_effects(tmp_path, parts):
    root, outside = tmp_path / "data", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    outside.chmod(0o710)
    link = root.joinpath(*parts)
    link.parent.mkdir(parents=True, exist_ok=True)
    link.symlink_to(outside, target_is_directory=True)
    paths = Paths(tmp_path / "legacy")
    paths.thread_root_resolver = lambda _: root
    with pytest.raises(OSError):
        paths.ensure_thread_dirs("t1", user_id="alice")
    assert outside.stat().st_mode & 0o777 == 0o710
    assert list(outside.iterdir()) == []
    assert not (tmp_path / "legacy").exists()


@pytest.mark.skipif(os.name == "nt", reason="POSIX descriptor confinement")
def test_projected_creation_rejects_directory_swapped_to_symlink_before_open(tmp_path):
    root, outside = tmp_path / "data", tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    outside.chmod(0o710)
    paths = Paths(tmp_path / "legacy")
    paths.thread_root_resolver = lambda _: root
    real_open = os.open

    def swap_before_open(path, *args, **kwargs):
        if path == "workspace":
            os.rmdir(path, dir_fd=kwargs["dir_fd"])
            os.symlink(outside, path, dir_fd=kwargs["dir_fd"])
        return real_open(path, *args, **kwargs)

    with patch.object(paths_module.os, "open", side_effect=swap_before_open):
        with pytest.raises(OSError):
            paths.ensure_thread_dirs("t1", user_id="alice")
    assert outside.stat().st_mode & 0o777 == 0o710
    assert list(outside.iterdir()) == []


@pytest.mark.skipif(os.name == "nt", reason="POSIX descriptor confinement")
def test_projected_creation_refuses_foreign_device_before_chmod_and_closes_descriptors(tmp_path):
    root = tmp_path / "data"
    root.mkdir()
    paths = Paths(tmp_path / "legacy")
    paths.thread_root_resolver = lambda _: root
    opened = []
    foreign = None
    real_open, real_stat, real_chmod = os.open, os.fstat, os.fchmod

    def track_open(path, *args, **kwargs):
        nonlocal foreign
        descriptor = real_open(path, *args, **kwargs)
        opened.append(descriptor)
        if path == "workspace":
            foreign = descriptor
        return descriptor

    def device_stat(descriptor):
        actual = real_stat(descriptor)
        return SimpleNamespace(st_dev=actual.st_dev + 1) if descriptor == foreign else actual

    with (
        patch.object(paths_module.os, "open", side_effect=track_open),
        patch.object(paths_module.os, "fstat", side_effect=device_stat),
        patch.object(paths_module.os, "fchmod", wraps=real_chmod) as chmod,
    ):
        with pytest.raises(OSError) as error:
            paths.ensure_thread_dirs("t1", user_id="alice")
    assert error.value.errno == errno.EXDEV
    assert foreign is not None
    assert all(call.args[0] != foreign for call in chmod.call_args_list)
    for descriptor in opened:
        with pytest.raises(OSError):
            real_stat(descriptor)
    assert not (tmp_path / "legacy").exists()
