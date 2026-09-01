"""Deterministic publisher-side package check used by this template's tests.

HartMesh's source lock and installed artifact manifest remain authoritative.
This small stdlib-only helper lets a copied template prove its reviewed source
set is deterministic before handing it to ``deerflow extensions install``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from pathlib import Path


def _digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def build_template_artifact_manifest(
    root: Path,
    files: Sequence[str],
) -> dict[str, object]:
    entries: list[dict[str, object]] = []
    for relative in sorted(set(files)):
        path = Path(relative)
        if path.is_absolute() or not relative or any(part in {"", ".", ".."} for part in path.parts):
            raise ValueError("template artifact path is invalid")
        source = root / path
        if source.is_symlink() or not source.is_file():
            raise ValueError("template artifact input must be a regular file")
        content = source.read_bytes()
        entries.append(
            {
                "path": path.as_posix(),
                "size": len(content),
                "content_digest": f"sha256:{hashlib.sha256(content).hexdigest()}",
            }
        )
    core: dict[str, object] = {"version": 1, "files": entries}
    return {**core, "digest": _digest(core)}


def verify_template_artifact_manifest(
    root: Path,
    manifest: dict[str, object],
) -> None:
    if set(manifest) != {"version", "files", "digest"} or manifest.get("version") != 1:
        raise ValueError("template artifact manifest is invalid")
    entries = manifest.get("files")
    if not isinstance(entries, list) or any(
        not isinstance(item, dict) or not isinstance(item.get("path"), str) for item in entries
    ):
        raise ValueError("template artifact manifest is invalid")
    rebuilt = build_template_artifact_manifest(
        root,
        [item["path"] for item in entries],
    )
    if rebuilt != manifest:
        raise ValueError("template artifact digest mismatch")
