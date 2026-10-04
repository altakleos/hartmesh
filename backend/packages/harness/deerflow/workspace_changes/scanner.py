from __future__ import annotations

import fnmatch
import hashlib
import os
import stat
from codecs import BOM_UTF16_BE, BOM_UTF16_LE, getincrementaldecoder
from dataclasses import dataclass
from pathlib import Path

from deerflow.constants import BROWSER_FRAMES_DIRNAME, MCP_INTERNAL_DIRNAME, TOOL_RESULTS_DIRNAME
from deerflow.files.store import SafeFileAccessUnavailable, StoreError, open_directory_source

from .types import (
    DiffUnavailableReason,
    FileSnapshot,
    WorkspaceChangeLimits,
    WorkspaceRoot,
    WorkspaceSnapshot,
)

EXCLUDED_DIR_NAMES = {
    ".git",
    ".hg",
    ".svn",
    ".cache",
    # Stdio MCP subprocess temp/debug files live below this server-owned
    # namespace. They remain addressable when a tool returns their path, but
    # they are not user-authored workspace deliverables or workspace changes.
    MCP_INTERNAL_DIRNAME,
    ".next",
    ".venv",
    # Transient per-step browser screenshots: live progress feedback surfaced in
    # the browser panel + inline thumbnails, not workspace deliverables. Shared
    # constant with the browser tools so the name cannot drift out of sync.
    BROWSER_FRAMES_DIRNAME,
    # Externalized oversized tool outputs (the tool-output budget middleware's
    # default storage_subdir): process feedback the model reads back via
    # read_file, not workspace deliverables — same intent as the browser frames
    # exclusion above. Without this, a run that externalizes any tool output
    # would trip run delivery verification (produced output never presented)
    # and fail as an error. Custom storage_subdir values are passed through
    # ``extra_excluded_dir_names`` instead.
    TOOL_RESULTS_DIRNAME,
    "__pycache__",
    "build",
    "dist",
    "node_modules",
}

BINARY_EXTENSIONS = {
    ".7z",
    ".avif",
    ".bmp",
    ".class",
    ".db",
    ".dll",
    ".dmg",
    ".doc",
    ".docx",
    ".exe",
    ".gif",
    ".gz",
    ".ico",
    ".jar",
    ".jpeg",
    ".jpg",
    ".mov",
    ".mp3",
    ".mp4",
    ".o",
    ".pdf",
    ".png",
    ".pyc",
    ".so",
    ".tar",
    ".webp",
    ".xls",
    ".xlsx",
    ".zip",
}

SENSITIVE_PATH_PATTERNS = (
    ".env",
    ".env.*",
    "*api_key*",
    "*apikey*",
    "*.key",
    "*.pem",
    "*credential*",
    "*password*",
    "*private_key*",
    "*secret*",
    "*token*",
)

SAMPLE_BYTES = 4096
_UTF16_BOMS = (BOM_UTF16_LE, BOM_UTF16_BE)


def is_sensitive_workspace_path(path: str) -> bool:
    normalized = path.lower()
    parts = [part.lower() for part in Path(path).parts]
    basename = parts[-1] if parts else normalized
    for pattern in SENSITIVE_PATH_PATTERNS:
        if fnmatch.fnmatch(basename, pattern) or fnmatch.fnmatch(normalized, pattern):
            return True
        if any(fnmatch.fnmatch(part, pattern) for part in parts):
            return True
    return False


@dataclass
class _TextBudget:
    remaining: int

    def decode(self, raw: bytes) -> tuple[str | None, DiffUnavailableReason | None]:
        # Reserve before decoding: UTF-16 can expand to three UTF-8 bytes per
        # code unit. A surrogate pair needs four, within this conservative cap.
        upper_bound = ((len(raw) - 2) // 2) * 3 if raw.startswith(_UTF16_BOMS) else len(raw)
        if upper_bound > self.remaining:
            return None, "truncated"
        self.remaining -= upper_bound
        decoded = _decode_text_bytes(raw)
        if decoded is None:
            return None, "binary"
        self.remaining += upper_bound - len(decoded.encode("utf-8"))
        return decoded, None


def scan_workspace_roots(
    roots: list[WorkspaceRoot],
    *,
    limits: WorkspaceChangeLimits | None = None,
    include_text: bool = True,
    text_paths: set[str] | None = None,
    text_cache_dir: Path | None = None,
    extra_excluded_dir_names: frozenset[str] | None = None,
) -> WorkspaceSnapshot:
    if not callable(getattr(os, "fwalk", None)):
        raise SafeFileAccessUnavailable("Safe workspace capture requires descriptor-relative directory walking")
    resolved_limits = limits or WorkspaceChangeLimits()
    cache_dir = Path(text_cache_dir) if text_cache_dir is not None else None
    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)
    # Operator-customized tool_output.storage_subdir values arrive here; the
    # default name is already part of EXCLUDED_DIR_NAMES, so merging is safe.
    # Only single-segment directory names are meaningful: os.walk yields
    # one-segment dirnames, so a nested value like "cache/tool-results" would
    # never match. ToolOutputConfig enforces the single-segment contract, so a
    # multi-segment value is a caller error, not a silent no-op.
    excluded_dir_names = EXCLUDED_DIR_NAMES | extra_excluded_dir_names if extra_excluded_dir_names else EXCLUDED_DIR_NAMES
    files: dict[str, FileSnapshot] = {}
    scanned = 0
    directories = 0
    truncated = False
    text_budget = _TextBudget(max(0, resolved_limits.max_total_text_bytes))

    def scan_error(_error: OSError) -> None:
        nonlocal truncated
        truncated = True

    for root in roots:
        try:
            root_fd = open_directory_source(root.host_path)
        except FileNotFoundError:
            continue
        except SafeFileAccessUnavailable:
            raise
        except (OSError, StoreError):
            truncated = True
            continue
        try:
            # fwalk pins each directory beneath the admitted root and refuses
            # symlink descent. File opens/stat/readlink use its directory fd.
            for dirpath, dirnames, filenames, directory_fd in os.fwalk(".", dir_fd=root_fd, follow_symlinks=False, onerror=scan_error):
                directories += 1
                if directories > max(1, resolved_limits.max_scanned_files):
                    return WorkspaceSnapshot(files=files, truncated=True, text_cache_dir=str(cache_dir) if cache_dir is not None else None)
                dirnames[:] = [dirname for dirname in dirnames if dirname not in excluded_dir_names]
                for filename in sorted(filenames):
                    if scanned >= resolved_limits.max_scanned_files:
                        return WorkspaceSnapshot(files=files, truncated=True, text_cache_dir=str(cache_dir) if cache_dir is not None else None)
                    scanned += 1
                    host_file = root.host_path / dirpath / filename
                    try:
                        metadata = os.stat(filename, dir_fd=directory_fd, follow_symlinks=False)
                    except OSError:
                        truncated = True
                        continue
                    if stat.S_ISLNK(metadata.st_mode):
                        snapshot = _snapshot_symlink(root, host_file, directory_fd=directory_fd, metadata=metadata)
                    elif stat.S_ISREG(metadata.st_mode):
                        snapshot = _snapshot_file(
                            root,
                            host_file,
                            directory_fd=directory_fd,
                            limits=resolved_limits,
                            include_text=include_text,
                            text_paths=text_paths,
                            text_cache_dir=cache_dir,
                            text_budget=text_budget,
                        )
                    else:
                        continue
                    if snapshot is not None:
                        files[snapshot.path] = snapshot
                    else:
                        truncated = True
        finally:
            os.close(root_fd)

    return WorkspaceSnapshot(
        files=files,
        truncated=truncated,
        text_cache_dir=str(cache_dir) if cache_dir is not None else None,
    )


def _snapshot_file(
    root: WorkspaceRoot,
    host_file: Path,
    *,
    directory_fd: int,
    limits: WorkspaceChangeLimits,
    include_text: bool,
    text_paths: set[str] | None,
    text_cache_dir: Path | None,
    text_budget: _TextBudget,
) -> FileSnapshot | None:
    try:
        fd = os.open(host_file.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    except OSError:
        return None
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            return None
        size, mtime_ns = metadata.st_size, metadata.st_mtime_ns
        relative = host_file.relative_to(root.host_path).as_posix()
        virtual_path = f"{root.virtual_prefix}/{relative}"
        sensitive = is_sensitive_workspace_path(virtual_path)
        sha256 = None
        text = text_path = None
        binary = False
        reason: DiffUnavailableReason | None = None
        if sensitive:
            reason = "sensitive"
        else:
            sample = _read_sample(fd)
            raw = None
            if size <= limits.max_file_bytes_for_diff:
                os.lseek(fd, 0, os.SEEK_SET)
                with os.fdopen(fd, "rb", closefd=False) as source:
                    raw = source.read(limits.max_file_bytes_for_diff + 1)
                if len(raw) != size:
                    return None
                sha256 = hashlib.sha256(raw).hexdigest()
                sample = raw[:SAMPLE_BYTES]
            binary = host_file.suffix.lower() in BINARY_EXTENSIONS or _looks_binary(sample)
            should_include_text = include_text and (text_paths is None or virtual_path in text_paths)
            if binary:
                reason = "binary"
            elif raw is None:
                reason = "large"
            elif should_include_text:
                decoded, reason = text_budget.decode(raw)
                if reason == "binary":
                    binary = True
                elif decoded is not None and text_cache_dir is not None:
                    text_path = str(_cache_text_file(decoded, virtual_path, text_cache_dir))
                elif decoded is not None:
                    text = decoded
        after = os.fstat(fd)
        if (after.st_size, after.st_mtime_ns) != (size, mtime_ns):
            return None
        return FileSnapshot(
            path=virtual_path,
            root=root.name,
            size=size,
            mtime_ns=mtime_ns,
            sha256=sha256,
            binary=binary,
            sensitive=sensitive,
            text=text,
            text_path=text_path,
            content_unavailable_reason=reason,
        )
    except OSError:
        return None
    finally:
        os.close(fd)


def _normalize_symlink_target(target: str) -> str:
    """Strip the Windows extended-length prefix from a symlink target.

    ``os.readlink`` on Windows reports absolute targets in extended-length
    form (``\\\\?\\C:\\...`` or ``\\\\?\\UNC\\server\\share``). Recorded targets
    are surfaced in workspace-change events and compared against ordinary
    paths, so keep the plain spelling.

    On POSIX this is a provable identity: the strip only applies on Windows
    hosts. ``readlink(2)`` returns the literal string the link was created
    with, and backslash is a valid filename byte on Linux — a target string
    that merely starts with ``\\\\?\\`` there must be recorded verbatim.

    On Windows, only extended *drive-letter* paths are stripped. Other
    ``\\\\?\\`` namespace forms (volume-GUID paths, device paths) are kept
    verbatim: stripping them would leave a relative-looking remainder that
    no longer names the target's namespace.
    """
    if os.name != "nt":
        return target
    if target.startswith("\\\\?\\UNC\\"):
        return "\\\\" + target[len("\\\\?\\UNC\\") :]
    if target.startswith("\\\\?\\") and len(target) >= 7 and target[4].isascii() and target[4].isalpha() and target[5] == ":" and target[6] in "\\/":
        return target[4:]
    return target


def _snapshot_symlink(root: WorkspaceRoot, host_file: Path, *, directory_fd: int, metadata: os.stat_result) -> FileSnapshot | None:
    # Deliberately never follows the link (no read_bytes()/open() on the target):
    # the target may point anywhere on the host, including outside the scanned
    # root, so stat'ing or reading through it here would risk exposing arbitrary
    # host file content/metadata as if it belonged to the workspace.
    try:
        size = metadata.st_size
        mtime_ns = metadata.st_mtime_ns
        relative = host_file.relative_to(root.host_path).as_posix()
        virtual_path = f"{root.virtual_prefix}/{relative}"
        sensitive = is_sensitive_workspace_path(virtual_path)
    except OSError:
        return None

    try:
        target = os.readlink(host_file.name, dir_fd=directory_fd)
    except OSError:
        target = None
    else:
        target = _normalize_symlink_target(target)

    return FileSnapshot(
        path=virtual_path,
        root=root.name,
        size=size,
        mtime_ns=mtime_ns,
        sha256=None,
        binary=False,
        sensitive=sensitive,
        text=None,
        content_unavailable_reason="symlink",
        symlink=True,
        symlink_target=target,
    )


def _cache_text_file(text: str, virtual_path: str, cache_dir: Path) -> Path:
    cache_name = hashlib.sha256(virtual_path.encode("utf-8")).hexdigest()
    target = cache_dir / cache_name
    target.write_text(text, encoding="utf-8")
    return target


def _read_sample(fd: int) -> bytes:
    os.lseek(fd, 0, os.SEEK_SET)
    return os.read(fd, SAMPLE_BYTES)


def _decode_text_bytes(data: bytes) -> str | None:
    for encoding in ("utf-8-sig", "utf-8"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue

    if data.startswith(_UTF16_BOMS):
        try:
            return data.decode("utf-16")
        except UnicodeDecodeError:
            return None

    return None


def _sample_decodes_as_text(sample: bytes, encoding: str) -> bool:
    try:
        decoder = getincrementaldecoder(encoding)()
        decoder.decode(sample, final=False)
    except UnicodeDecodeError:
        return False
    return True


def _looks_binary(sample: bytes) -> bool:
    if sample.startswith(_UTF16_BOMS) and _sample_decodes_as_text(sample, "utf-16"):
        return False
    if b"\x00" in sample:
        return True
    if _sample_decodes_as_text(sample, "utf-8"):
        return False
    return True
