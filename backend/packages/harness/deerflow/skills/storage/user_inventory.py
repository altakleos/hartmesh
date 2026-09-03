"""Protected authoritative index for user-scoped skill stores.

The index retains raw subject IDs only in server-owned storage so a bootstrap
adapter can reopen the correct bucket. Public revision/audit projections use
opaque user refs and never expose this file's contents.
"""

from __future__ import annotations

import json
from pathlib import Path

from deerflow.config.extensions_config import (
    atomic_write_extensions_config,
    extensions_config_file_lock,
)
from deerflow.config.paths import Paths, get_paths

_INDEX_VERSION = 1
_MAX_SUBJECTS = 10_000


def _index_path(paths: Paths) -> Path:
    return paths.base_dir / "runtime" / "user-skill-store-index.v1.json"


def _load(path: Path) -> tuple[str, ...]:
    if not path.exists():
        return ()
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or set(raw) != {"version", "subjects"}:
        raise ValueError("user skill-store inventory is malformed")
    if raw.get("version") != _INDEX_VERSION or not isinstance(raw.get("subjects"), list):
        raise ValueError("user skill-store inventory is malformed")
    subjects = raw["subjects"]
    if len(subjects) > _MAX_SUBJECTS or any(not isinstance(subject, str) or not subject or len(subject.encode("utf-8")) > 512 for subject in subjects) or subjects != sorted(set(subjects)):
        raise ValueError("user skill-store inventory is malformed")
    return tuple(subjects)


def register_user_skill_subject(
    raw_subject_id: str,
    *,
    paths: Paths | None = None,
) -> None:
    """Idempotently register a subject under a cross-process file lock."""

    if not isinstance(raw_subject_id, str) or not raw_subject_id or len(raw_subject_id.encode("utf-8")) > 512:
        raise ValueError("user skill-store subject ID is invalid")
    resolved_paths = paths or get_paths()
    path = _index_path(resolved_paths)
    with extensions_config_file_lock(path):
        subjects = set(_load(path))
        if raw_subject_id in subjects:
            return
        if len(subjects) >= _MAX_SUBJECTS:
            raise ValueError("user skill-store inventory limit exceeded")
        subjects.add(raw_subject_id)
        atomic_write_extensions_config(
            path,
            {"version": _INDEX_VERSION, "subjects": sorted(subjects)},
        )
        path.chmod(0o600)


def list_registered_user_skill_subjects(
    *,
    paths: Paths | None = None,
) -> tuple[str, ...]:
    """Read one validated snapshot of the protected subject index."""

    resolved_paths = paths or get_paths()
    path = _index_path(resolved_paths)
    with extensions_config_file_lock(path):
        return _load(path)


__all__ = [
    "list_registered_user_skill_subjects",
    "register_user_skill_subject",
]
