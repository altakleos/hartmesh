"""Exercise snapshot verification and upstream merges in disposable Git trees."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/verify_frontend_isolation.py"
MARKER = ".github/upstream-frontend.json"


def git(root: Path, *args: str) -> str:
    result = subprocess.run(["git", "-C", str(root), *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def write(root: Path, path: str, text: str) -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def commit(root: Path) -> str:
    git(root, "add", "-A")
    git(root, "commit", "-qm", "fixture")
    return git(root, "rev-parse", "HEAD")


def pin(root: Path, upstream: str, seed: str | None = None) -> None:
    write(
        root,
        MARKER,
        json.dumps(
            {
                "schema_version": 1,
                "upstream_repository": "https://github.com/bytedance/deer-flow.git",
                "upstream_commit": upstream,
                "frontend_tree": git(root, "rev-parse", f"{upstream}:frontend"),
                "hartmesh_seed_commit": seed or upstream,
            }
        ),
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    git(tmp_path, "init", "-q", "-b", "main")
    git(tmp_path, "config", "user.email", "fixture@example.test")
    git(tmp_path, "config", "user.name", "Fixture")
    git(tmp_path, "config", "commit.gpgsign", "false")
    git(tmp_path, "config", "core.filemode", "true")
    write(tmp_path, "frontend/app.ts", "export const value = 'upstream';\n")
    write(tmp_path, "frontend/removed.ts", "export {};\n")
    upstream = commit(tmp_path)
    git(tmp_path, "branch", "upstream", upstream)
    shutil.copytree(tmp_path / "frontend", tmp_path / "frontend-hm")
    write(tmp_path, ".gitignore", ".env\nnode_modules/\n.next/\n")
    pin(tmp_path, upstream)
    commit(tmp_path)
    return tmp_path


def check(root: Path, *args: str):
    return subprocess.run([sys.executable, str(SCRIPT), "--repo", str(root), *args], capture_output=True, text=True)


def test_clean_snapshot_supports_revision_index_and_worktree(repo: Path):
    for args in [(), ("--revision", "HEAD"), ("--index",)]:
        result = check(repo, *args)
        assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("change", ["edit", "add", "delete", "mode"])
def test_local_drift_is_rejected_without_modifying_index_or_files(repo: Path, change: str):
    target = repo / "frontend/app.ts"
    if change == "edit":
        target.write_text("changed", encoding="utf-8")
    elif change == "add":
        write(repo, "frontend/extra.ts", "extra")
    elif change == "delete":
        target.unlink()
    else:
        if os.name == "nt":
            pytest.skip("executable-bit changes need a POSIX filesystem")
        target.chmod(0o755)
    before = git(repo, "status", "--porcelain")
    index = git(repo, "write-tree")
    result = check(repo)
    assert result.returncode == 1
    assert "frontend/" in result.stderr
    assert git(repo, "status", "--porcelain") == before
    assert git(repo, "write-tree") == index
    assert check(repo, "--revision", "HEAD").returncode == 0
    commit(repo)
    assert check(repo, "--revision", "HEAD").returncode == 1


def test_staged_drift_cannot_hide_behind_a_clean_worktree(repo: Path):
    original = (repo / "frontend/app.ts").read_text(encoding="utf-8")
    write(repo, "frontend/app.ts", "staged drift")
    git(repo, "add", "frontend/app.ts")
    write(repo, "frontend/app.ts", original)
    assert check(repo, "--index").returncode == 1
    assert check(repo).returncode == 1


def test_ignored_settings_and_outputs_do_not_count_as_snapshot_drift(repo: Path):
    write(repo, "frontend/.env", "PRIVATE=not-printed")
    write(repo, "frontend/node_modules/generated.js", "local")
    result = check(repo)
    assert result.returncode == 0, result.stderr
    assert "PRIVATE" not in result.stdout + result.stderr


@pytest.mark.parametrize("field,value", [("frontend_tree", "0" * 40), ("upstream_commit", "0" * 40), ("schema_version", 2)])
def test_invalid_marker_is_rejected(repo: Path, field: str, value):
    marker = json.loads((repo / MARKER).read_text(encoding="utf-8"))
    marker[field] = value
    write(repo, MARKER, json.dumps(marker))
    result = check(repo)
    assert result.returncode == 1
    assert result.stderr


def test_revision_check_uses_its_own_marker(repo: Path):
    write(repo, MARKER, "invalid worktree marker")
    assert check(repo, "--revision", "HEAD").returncode == 0
    assert check(repo).returncode == 1


def test_non_ancestor_pin_is_rejected(repo: Path):
    git(repo, "switch", "-q", "upstream")
    write(repo, "upstream-only", "new")
    unmerged = commit(repo)
    git(repo, "switch", "-q", "main")
    pin(repo, unmerged)
    result = check(repo)
    assert result.returncode == 1
    assert "ancestor" in result.stderr


def test_missing_history_is_an_error(repo: Path, tmp_path: Path):
    clone = tmp_path / "shallow"
    subprocess.run(["git", "clone", "--quiet", "--depth=1", repo.as_uri(), str(clone)], check=True)
    result = check(clone, "--revision", "HEAD")
    assert result.returncode == 1
    assert "history" in result.stderr.lower()


@pytest.mark.parametrize(
    "path,content",
    [
        ("src/app.ts", "import '../frontend/app';"),
        ("package.json", '{"dependencies":{"upstream":"file:../frontend"}}'),
        ("tsconfig.json", '{"compilerOptions":{"paths":{"upstream/*":["../frontend/src/*"]}}}'),
        ("next.config.js", "const assets = '../frontend/public';"),
        ("Dockerfile", "COPY frontend ./frontend\n"),
    ],
)
def test_hartmesh_must_not_source_the_upstream_app(repo: Path, path: str, content: str):
    write(repo, f"frontend-hm/{path}", content)
    result = check(repo)
    assert result.returncode == 1
    assert f"frontend-hm/{path}" in result.stderr


def test_hartmesh_symlink_cannot_point_to_upstream(repo: Path):
    if os.name == "nt":
        pytest.skip("symlink creation needs privileges on Windows")
    (repo / "frontend-hm/linked").symlink_to("../frontend")
    assert check(repo).returncode == 1


def test_upstream_edit_add_delete_merge_leaves_hartmesh_untouched(repo: Path):
    write(repo, "frontend-hm/app.ts", "Hartmesh independent changes")
    commit(repo)
    expected = git(repo, "rev-parse", "HEAD:frontend-hm")
    git(repo, "switch", "-q", "upstream")
    write(repo, "frontend/app.ts", "new upstream UI")
    write(repo, "frontend/new.ts", "new upstream file")
    (repo / "frontend/removed.ts").unlink()
    upstream = commit(repo)
    git(repo, "switch", "-q", "main")
    git(repo, "merge", "--no-edit", "upstream")
    assert git(repo, "rev-parse", "HEAD:frontend-hm") == expected
    assert check(repo).returncode == 1  # The sync must advance its marker.
    pin(repo, upstream)
    commit(repo)
    result = check(repo, "--revision", "HEAD")
    assert result.returncode == 0, result.stderr
