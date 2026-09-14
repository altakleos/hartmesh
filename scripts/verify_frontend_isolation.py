#!/usr/bin/env python3
"""Verify upstream tree equality and reject direct cross-application inputs.

Default: check both the index and worktree without changing the real index.
CI: --revision HEAD checks only committed material, including that marker.
The source scan is a guard for literal paths, not a JavaScript interpreter;
the independent build without frontend/ exercises computed build inputs.
"""

from __future__ import annotations

import argparse
import json
import os
import posixpath
import re
import subprocess
import sys
import tempfile
from pathlib import Path

MARKER = ".github/upstream-frontend.json"
FIELDS = {"schema_version", "upstream_repository", "upstream_commit", "frontend_tree", "hartmesh_seed_commit"}
SOURCE_SUFFIXES = {".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".json", ".yaml", ".yml", ".css", ".html"}
LITERAL = re.compile(r"[\"'`]([^\"'`\r\n]+)[\"'`]")
UPSTREAM_PATH = re.compile(r"^(?:(?:file|link):)?(?:\.?\.?/)*frontend(?:/|$)")


class VerificationError(Exception):
    pass


def git(root: Path, *args: str, env: dict[str, str] | None = None, data: bytes | None = None) -> bytes:
    result = subprocess.run(["git", "-C", str(root), *args], env=env, input=data, capture_output=True)
    if result.returncode:
        # Never include blob contents or arbitrary command output in errors.
        raise VerificationError(f"Git {args[0]} failed. Ensure the snapshot and its history are available (CI: fetch-depth: 0).")
    return result.stdout


def object_id(root: Path, expression: str) -> str:
    return git(root, "rev-parse", "--verify", "--end-of-options", expression).decode().strip()


def verify_sources(root: Path, tree: str) -> None:
    entries = git(root, "ls-tree", "-rz", tree, "--", "frontend-hm").split(b"\0")
    selected: list[tuple[str, str, str]] = []
    for entry in filter(None, entries):
        metadata, raw_path = entry.split(b"\t", 1)
        mode, kind, oid = metadata.decode().split()
        path = raw_path.decode("utf-8")
        if kind != "blob":
            raise VerificationError(f"{path}: application inputs must not be Git submodules")
        if mode == "120000" or Path(path).suffix in SOURCE_SUFFIXES or Path(path).name in {"Dockerfile", "Makefile"}:
            selected.append((path, mode, oid))
    if not selected:
        raise VerificationError("frontend-hm/: Hartmesh application source is missing")
    payload = git(root, "cat-file", "--batch", data="".join(f"{oid}\n" for _, _, oid in selected).encode())
    offset = 0
    for path, mode, _ in selected:
        end = payload.index(b"\n", offset)
        size = int(payload[offset:end].split()[2])
        content = payload[end + 1 : end + 1 + size].decode("utf-8")
        offset = end + size + 2
        if mode == "120000":
            resolved = posixpath.normpath(posixpath.join(posixpath.dirname(path), content))
            if not resolved.startswith("frontend-hm/"):
                raise VerificationError(f"{path}: symlink escapes the Hartmesh application")
        elif any(UPSTREAM_PATH.match(value.replace("\\", "/")) for value in LITERAL.findall(content)):
            raise VerificationError(f"{path}: literal input references upstream frontend/; copy or port it into frontend-hm/")
        elif Path(path).name == "Dockerfile" and re.search(r"(?im)^\s*(?:COPY|ADD)\s+(?:--\S+\s+)*frontend(?:/|\s)", content):
            raise VerificationError(f"{path}: image build sources upstream frontend/")


def verify_tree(root: Path, tree: str, revision: str) -> None:
    try:
        marker = json.loads(git(root, "show", f"{tree}:{MARKER}"))
    except (ValueError, VerificationError) as exc:
        raise VerificationError(f"{MARKER}: missing or invalid marker in the checked tree") from exc
    if not isinstance(marker, dict) or set(marker) != FIELDS or type(marker["schema_version"]) is not int or marker["schema_version"] != 1:
        raise VerificationError(f"{MARKER}: expected schema version 1 and its exact fields")
    if marker["upstream_repository"] != "https://github.com/bytedance/deer-flow.git":
        raise VerificationError(f"{MARKER}: unexpected upstream repository")
    for field in ("upstream_commit", "frontend_tree", "hartmesh_seed_commit"):
        if not isinstance(marker[field], str) or not re.fullmatch(r"[0-9a-f]{40}", marker[field]):
            raise VerificationError(f"{MARKER}: {field} must be a full Git object ID")
    upstream = marker["upstream_commit"]
    object_id(root, f"{upstream}^{{commit}}")
    try:
        git(root, "merge-base", "--is-ancestor", upstream, revision)
    except VerificationError as exc:
        raise VerificationError(f"{MARKER}: upstream commit is not an available ancestor of {revision}; check the merged history") from exc
    expected = object_id(root, f"{upstream}:frontend")
    if expected != marker["frontend_tree"]:
        raise VerificationError(f"{MARKER}: frontend_tree does not match the pinned upstream commit")
    actual = object_id(root, f"{tree}:frontend")
    if actual != expected:
        paths = git(root, "diff", "--name-only", "--no-renames", upstream, tree, "--", "frontend").decode().strip()
        raise VerificationError(f"Upstream frontend/ differs from {upstream}:\n{paths}")
    verify_sources(root, tree)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parent.parent)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--revision", help="Verify a committed revision, using its own marker")
    mode.add_argument("--index", action="store_true", help="Verify only staged material")
    args = parser.parse_args(argv)
    try:
        root = args.repo.resolve()
        revision = object_id(root, f"{args.revision or 'HEAD'}^{{commit}}")
        if args.revision:
            verify_tree(root, revision, revision)
        else:
            index_tree = git(root, "write-tree").decode().strip()
            verify_tree(root, index_tree, revision)
            if not args.index:
                with tempfile.TemporaryDirectory(prefix="frontend-isolation-") as temp:
                    env = {**os.environ, "GIT_INDEX_FILE": str(Path(temp) / "index")}
                    git(root, "read-tree", index_tree, env=env)
                    git(root, "add", "-A", "--", "frontend", "frontend-hm", MARKER, env=env)
                    tree = git(root, "write-tree", env=env).decode().strip()
                    verify_tree(root, tree, revision)
    except (VerificationError, OSError, UnicodeError, ValueError) as exc:
        print(f"Frontend isolation failed: {exc}", file=sys.stderr)
        return 1
    print("Frontend isolation OK: upstream snapshot matches; Hartmesh source has no direct upstream inputs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
