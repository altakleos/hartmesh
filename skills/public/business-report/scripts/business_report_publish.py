"""Publish complete report directories; never replace a published companion.

The lock coordinates this script's writers. Storage is not made read-only:
the manifest detects outside edits before another command adopts a bundle.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import shutil
import tempfile
import uuid
from pathlib import Path

from business_report_common import REPORT_SUFFIX, InputError

STATE_DIR = Path(".cache/business-report")
BUNDLE_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,79}$")


def write_json(path: Path, value) -> None:
    """Same-directory atomic replacement, durable before publishing a pointer."""
    fd, temporary = tempfile.mkstemp(prefix=".write-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        sync_directory(path.parent)
    finally:
        Path(temporary).unlink(missing_ok=True)


def sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@contextlib.contextmanager
def publication_lock(root: Path, base: str):
    require_publication_root(root)
    try:
        import fcntl
    except ImportError as error:
        raise InputError("Report publication requires filesystem locking on this platform.") from error
    state = root / STATE_DIR
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (state / f"{base}.lock").open("a+b") as stream:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def require_publication_root(root: Path) -> None:
    resolved = root.resolve()
    if any(parent.parent.name == "drafts" and (parent / "renders.json").exists() for parent in [resolved, *resolved.parents]):
        raise InputError("Choose a report root outside existing published bundles; use the original --out directory.")


def bundle_manifest(path: Path, *, verify: bool = True) -> dict:
    try:
        manifest = json.loads((path.parent / "renders.json").read_text(encoding="utf-8"))
        base = path.name.removesuffix(REPORT_SUFFIX)
        if manifest["version"] != 2 or manifest["base"] != base or manifest["bundle"] != path.parent.name:
            raise ValueError("identity")
        if not BUNDLE_ID.fullmatch(path.parent.name) or not isinstance(manifest["draft"], int) or manifest["draft"] < 1:
            raise ValueError("draft identity")
        members = manifest["sha256"]
        if not isinstance(members, dict) or path.name not in members or "checks.json" not in members:
            raise ValueError("members")
        for name, expected in members.items():
            member = Path(name)
            if member.is_absolute() or not member.parts or any(part in (".", "..") for part in member.parts) or member.as_posix() != name:
                raise ValueError("member path")
            source = path.parent / member
            if path.parent.is_symlink() or any((path.parent / Path(*member.parts[:index])).is_symlink() for index in range(1, len(member.parts) + 1)):
                raise ValueError("symlink")
            if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected) or (verify and hashlib.sha256(source.read_bytes()).hexdigest() != expected):
                raise ValueError("content")
        files = manifest["files"]
        if not isinstance(files, list) or any(not isinstance(name, str) or name not in members or name not in {f"{base}.{kind}" for kind in ("pdf", "docx", "xlsx", "html")} for name in files):
            raise ValueError("renders")
        if verify:
            document = json.loads(path.read_text(encoding="utf-8"))
            if document["meta"]["draft"] != manifest["draft"] or any(chart["png"] not in members for chart in document["charts"]) or json.loads((path.parent / "checks.json").read_text(encoding="utf-8")) != document["checks"]:
                raise ValueError("report companions")
        return manifest
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise InputError("This report bundle is incomplete or was edited outside the report script. Rebuild it before changing or rendering it.") from error


def publication_root(path: Path) -> Path:
    if path.parent.parent.name == "drafts":
        bundle_manifest(path)
        return path.parent.parent.parent
    return path.parent


def current_report(root: Path, base: str, *, verify: bool = True) -> Path | None:
    pointer = root / STATE_DIR / f"{base}.current.json"
    if not pointer.exists():
        legacy = root / f"{base}{REPORT_SUFFIX}"
        return legacy if legacy.is_file() else None
    try:
        value = json.loads(pointer.read_text(encoding="utf-8"))
        relative = Path(value["report"])
        if len(relative.parts) != 3 or relative.parts[0] != "drafts" or not BUNDLE_ID.fullmatch(relative.parts[1]) or relative.parts[2] != f"{base}{REPORT_SUFFIX}" or relative.as_posix() != value["report"]:
            raise ValueError("pointer")
        path = root / relative
        bundle_manifest(path, verify=verify)
        return path
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise InputError("Could not verify the current report draft. Restore its complete bundle before continuing.") from error


def require_current(root: Path, base: str, source: Path) -> None:
    current = current_report(root, base)
    if current is not None and current.resolve() != source.resolve():
        raise InputError(f"This is an earlier report draft. Use the current report: {current}")


def current_identity(root: Path, base: str, *, verify: bool = True):
    current = current_report(root, base, verify=verify)
    return (str(current.resolve()), hashlib.sha256(current.read_bytes()).hexdigest()) if current else None


def publish_bundle(root: Path, base: str, report: dict, targets: list[str], tenant_dir, render, prepare, *, bundle_id: str | None, expected_current, verify_current: bool = True, source: Path | None = None, carry: list[str] = ()) -> Path:
    """Generate privately, then lock only for the conditional publication.

    A crash after rename but before pointer replacement leaves a complete,
    unadopted directory. It never exposes half a draft or changes older bytes.
    """
    identity = bundle_id or f"draft-{report['meta']['draft']}-{uuid.uuid4().hex}"
    require_publication_root(root)
    if not BUNDLE_ID.fullmatch(identity):
        raise InputError("--bundle-id must be 1 to 80 lowercase letters, digits or hyphens, beginning with a letter or digit.")
    destination = root / "drafts" / identity
    if destination.exists() or destination.is_symlink():
        raise InputError(f"Report bundle {identity} already exists. Choose a new --bundle-id; published drafts are retained.")
    state = root / STATE_DIR
    state.mkdir(parents=True, exist_ok=True, mode=0o700)
    stage = Path(tempfile.mkdtemp(prefix="staging-", dir=state))
    path = stage / f"{base}{REPORT_SUFFIX}"
    try:
        prepare(stage)
        write_json(path, report)
        write_json(stage / "checks.json", report["checks"])
        if source and carry:
            manifest = bundle_manifest(source)
            for name in carry:
                content = (source.parent / name).read_bytes()
                if hashlib.sha256(content).hexdigest() != manifest["sha256"][name]:
                    raise InputError("A report companion changed while copying. Retry from a verified draft.")
                (stage / name).write_bytes(content)
        names = list(carry)
        for target in targets:
            generated = render(report, path, target, None, tenant_dir)
            names.append(generated.name)
        members = {member.relative_to(stage).as_posix(): hashlib.sha256(member.read_bytes()).hexdigest() for member in sorted(stage.rglob("*")) if member.is_file()}
        write_json(stage / "renders.json", {"version": 2, "base": base, "bundle": identity, "draft": report["meta"]["draft"], "files": sorted(set(names)), "sha256": members})
        # Flush every member and child directory before the one visibility boundary.
        for member in stage.rglob("*"):
            if member.is_file():
                member.chmod(0o644)
                with member.open("rb") as stream:
                    os.fsync(stream.fileno())
        for directory in sorted((member for member in stage.rglob("*") if member.is_dir()), reverse=True):
            directory.chmod(0o755)
            sync_directory(directory)
        stage.chmod(0o755)
        sync_directory(stage)
        with publication_lock(root, base):
            if current_identity(root, base, verify=verify_current) != expected_current:
                raise InputError("The current report changed while this draft was prepared. Retry from the current draft.")
            if destination.exists() or destination.is_symlink():
                raise InputError(f"Report bundle {identity} already exists. Choose a new --bundle-id.")
            destination.parent.mkdir(exist_ok=True)
            sync_directory(root)
            os.rename(stage, destination)
            sync_directory(destination.parent)
            path = destination / path.name
            try:
                write_json(state / f"{base}.current.json", {"version": 1, "report": path.relative_to(root).as_posix()})
            except OSError as error:
                raise InputError(f"Complete report bundle created at {path}, but current-draft state could not be durably updated. Retry with a new --bundle-id.") from error
        return path
    finally:
        if stage.exists():
            shutil.rmtree(stage)
