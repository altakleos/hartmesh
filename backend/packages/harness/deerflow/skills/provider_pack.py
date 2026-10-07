"""Operator-only snapshots into the existing stable read-only pack inventory."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import zipfile
from pathlib import Path

from deerflow.config.paths import Paths, get_paths
from deerflow.skills.export import build_skill_export, export_manifest
from deerflow.skills.installer import safe_extract_skill_archive
from deerflow.skills.parser import parse_skill_file
from deerflow.skills.permissions import make_skill_tree_sandbox_readable
from deerflow.skills.private_variants import _SourceStorage
from deerflow.skills.projection import _projection_lock
from deerflow.skills.security_static_scanner import enforce_static_scan
from deerflow.skills.storage.local_skill_storage import LocalSkillStorage
from deerflow.skills.storage.skill_storage import walk_skill_directories
from deerflow.skills.types import SKILL_MD_FILE, SkillCategory


def _exchange_directories(staged: Path, published: Path) -> None:
    """Exchange complete Linux trees in one syscall, keeping a live baseline.

    Unsupported kernels/filesystems fail before either directory is retired.
    The caller owns staging and removes the old tree after successful exchange.
    """
    import ctypes

    library = ctypes.CDLL(None, use_errno=True)
    exchange = getattr(library, "renameat2", None)
    if exchange is None:
        raise OSError("Atomic provider pack replacement is unsupported on this host.")
    exchange.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    exchange.restype = ctypes.c_int
    if exchange(-100, os.fsencode(staged), -100, os.fsencode(published), 2) != 0:
        raise OSError(ctypes.get_errno(), "Atomic provider pack replacement failed; the existing pack is unchanged.")


def install_provider_skill_pack(source: str | Path, name: str, *, home: Path | None = None, replace: bool = False) -> dict:
    """Explicit OS operator action; no customer endpoint or code installation."""
    name = LocalSkillStorage.validate_skill_name(name)
    paths = Paths(base_dir=Path(home).absolute()) if home is not None else get_paths()
    root = paths.integration_skills_dir() / "provider"
    target = root / name
    source = Path(source).absolute()
    if source.is_symlink() or not source.is_dir():
        raise ValueError("Provider source must be a regular directory.")
    if not all(not ancestor.is_symlink() for ancestor in (root, *root.parents)):
        raise ValueError("Linked provider pack state roots are unsupported.")
    root.mkdir(parents=True, exist_ok=True)
    capture_storage = LocalSkillStorage(host_path=str(paths.base_dir))
    selected = []
    for current, directories, files in walk_skill_directories(source):
        directories[:] = sorted(n for n in directories if not n.startswith("."))
        if SKILL_MD_FILE not in files:
            continue
        directories.clear()
        skill = parse_skill_file(Path(current) / SKILL_MD_FILE, category=SkillCategory.INTEGRATION, relative_path=Path(current).relative_to(source))
        if skill is None:
            raise ValueError("Provider pack contains invalid skill metadata.")
        LocalSkillStorage.validate_skill_name(skill.name)
        if any(other.name == skill.name for other in selected):
            raise ValueError("Provider pack has duplicate skill names.")
        selected.append(skill)
        if len(selected) > 64:
            raise ValueError("Provider pack exceeds 64 skills.")
    if not selected:
        raise ValueError("Provider pack contains no skills.")
    with tempfile.TemporaryDirectory(prefix=".provider-pack-", dir=root) as temporary:
        staged = Path(temporary) / "pack"
        staged.mkdir()
        revisions = {}
        total = 0
        for skill in selected:
            capture = _SourceStorage(capture_storage, skill)
            manifest = export_manifest(capture, skill.name)
            if not manifest["can_export"]:
                raise ValueError("Provider package contains unsupported files or links.")
            total += manifest["total_bytes"]
            if total > 100 * 1024 * 1024:
                raise ValueError("Provider pack exceeds its byte limit.")
            archive = build_skill_export(capture, skill.name, manifest["revision"])
            try:
                with zipfile.ZipFile(archive.file) as file:
                    safe_extract_skill_archive(file, staged)
            finally:
                archive.close()
            enforce_static_scan(staged / skill.name, skill_name=skill.name)
            make_skill_tree_sandbox_readable(staged / skill.name)
            revisions[skill.name] = manifest["revision"]
        revision = hashlib.sha256(json.dumps(revisions, sort_keys=True).encode("utf-8")).hexdigest()
        (staged / ".provider-pack.json").write_text(json.dumps({"version": 1, "name": name, "revision": revision, "skills": revisions}, indent=2) + "\n", encoding="utf-8")
        with _projection_lock(root):
            if target.is_symlink():
                raise ValueError("Linked provider pack targets are unsupported.")
            if target.exists() and not replace:
                raise FileExistsError("Provider pack exists; use explicit replacement to upgrade it.")
            for current, directories, files in walk_skill_directories(root):
                path = Path(current)
                directories[:] = sorted(n for n in directories if not n.startswith("."))
                if path == target:
                    directories.clear()
                    continue
                if SKILL_MD_FILE not in files:
                    continue
                directories.clear()
                other = parse_skill_file(path / SKILL_MD_FILE, category=SkillCategory.INTEGRATION, relative_path=path.relative_to(root))
                if other is not None and other.name in revisions:
                    raise ValueError("Skill name already belongs to another provider pack.")
            if target.exists():
                _exchange_directories(staged, target)
            else:
                staged.rename(target)
    return {"pack": name, "revision": revision, "skills": sorted(revisions)}
