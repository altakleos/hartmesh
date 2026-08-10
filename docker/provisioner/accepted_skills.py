"""Verify accepted skill snapshots and guard their Kubernetes data plane.

This module intentionally uses only the Python standard library.  The
provisioner image runs it in two narrowly scoped modes:

* ``materialize`` copies one content-addressed RWX snapshot into a private
  per-Pod ``emptyDir`` and verifies the existing Hartmesh snapshot digest.
* ``gate`` exposes the sandbox HTTP API only to a caller holding the
  per-attempt capability mounted from a Kubernetes Secret.

Neither mode discovers live skills or interprets caller configuration.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import http.server
import json
import os
import posixpath
import re
import shutil
import stat
import tempfile
import threading
import urllib.error
import urllib.request
from pathlib import Path

MAX_SKILLS = 64
MAX_FILES_PER_SKILL = 256
MAX_TOTAL_FILES = 2_048
MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_SKILL_BYTES = 8 * 1024 * 1024
MAX_TOTAL_BYTES = 32 * 1024 * 1024
MAX_RELATIVE_PATH_BYTES = 512
MAX_EVIDENCE_BYTES = 64 * 1024
MAX_RECEIPT_BYTES = 4 * 1024
MAX_GATE_BODY_BYTES = 110 * 1024 * 1024
MAX_TREE_ENTRIES_PER_SKILL = 1_024
MAX_GATE_CONCURRENCY = 32
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_PROFILE = "rwx_verified_copy_v2"
_CATEGORIES = frozenset({"public", "custom", "integrations", "legacy"})
_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
}


class AcceptedSkillMaterializationError(RuntimeError):
    """A bounded fail-closed verifier error."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _fail(code: str) -> None:
    raise AcceptedSkillMaterializationError(code)


def _bounded_text(value: object, *, field: str, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum or any(ord(character) < 32 for character in value):
        _fail(f"skill_snapshot_{field}_invalid")
    return value


def _bounded_relative(value: object) -> str:
    text = _bounded_text(value, field="path", maximum=MAX_RELATIVE_PATH_BYTES)
    path = Path(text)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts) or path.as_posix() != text:
        _fail("skill_snapshot_path_invalid")
    return text


def _strict_projection(raw: object) -> dict[str, object]:
    if not isinstance(raw, dict) or set(raw) != {
        "name",
        "category",
        "relative_path",
        "manifest_digest",
        "content_digest",
        "file_count",
        "total_bytes",
    }:
        _fail("skill_snapshot_evidence_invalid")
    projection = {
        "name": _bounded_text(raw["name"], field="name", maximum=128),
        "category": _bounded_text(raw["category"], field="category", maximum=32),
        "relative_path": _bounded_relative(raw["relative_path"]),
        "manifest_digest": raw["manifest_digest"],
        "content_digest": raw["content_digest"],
        "file_count": raw["file_count"],
        "total_bytes": raw["total_bytes"],
    }
    if projection["category"] not in _CATEGORIES:
        _fail("skill_snapshot_category_invalid")
    for key in ("manifest_digest", "content_digest"):
        if not isinstance(projection[key], str) or _DIGEST.fullmatch(projection[key]) is None:
            _fail("skill_snapshot_evidence_invalid")
    file_count = projection["file_count"]
    total_bytes = projection["total_bytes"]
    if type(file_count) is not int or not (1 <= file_count <= MAX_FILES_PER_SKILL):
        _fail("skill_snapshot_evidence_invalid")
    if type(total_bytes) is not int or not (1 <= total_bytes <= MAX_SKILL_BYTES):
        _fail("skill_snapshot_evidence_invalid")
    return projection


def validate_evidence(raw: object) -> dict[str, object]:
    """Return one strict bounded evidence record."""

    if not isinstance(raw, dict) or set(raw) != {
        "snapshot_id",
        "content_digest",
        "projections",
        "file_count",
        "total_bytes",
    }:
        _fail("skill_snapshot_evidence_invalid")
    snapshot_id = raw["snapshot_id"]
    content_digest = raw["content_digest"]
    if not isinstance(snapshot_id, str) or _DIGEST.fullmatch(snapshot_id) is None or content_digest != snapshot_id:
        _fail("skill_snapshot_evidence_invalid")
    projections_raw = raw["projections"]
    if not isinstance(projections_raw, list) or not (1 <= len(projections_raw) <= MAX_SKILLS):
        _fail("skill_snapshot_evidence_invalid")
    projections = [_strict_projection(item) for item in projections_raw]
    identities = [(item["category"], item["relative_path"], item["name"]) for item in projections]
    if identities != sorted(identities) or len(set(identities)) != len(identities):
        _fail("skill_snapshot_evidence_invalid")
    file_count = raw["file_count"]
    total_bytes = raw["total_bytes"]
    if type(file_count) is not int or file_count != sum(int(item["file_count"]) for item in projections) or file_count > MAX_TOTAL_FILES:
        _fail("skill_snapshot_evidence_invalid")
    if type(total_bytes) is not int or total_bytes != sum(int(item["total_bytes"]) for item in projections) or total_bytes > MAX_TOTAL_BYTES:
        _fail("skill_snapshot_evidence_invalid")
    return {
        "snapshot_id": snapshot_id,
        "content_digest": content_digest,
        "projections": projections,
        "file_count": file_count,
        "total_bytes": total_bytes,
    }


def _same_file(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev,
        left.st_ino,
        left.st_mode,
        left.st_size,
        left.st_mtime_ns,
        left.st_ctime_ns,
    ) == (
        right.st_dev,
        right.st_ino,
        right.st_mode,
        right.st_size,
        right.st_mtime_ns,
        right.st_ctime_ns,
    )


def _read_regular_at(
    directory_fd: int,
    name: str,
    before: os.stat_result,
) -> tuple[bytes, bool]:
    if stat.S_ISLNK(before.st_mode):
        _fail("skill_snapshot_symlink")
    if not stat.S_ISREG(before.st_mode):
        _fail("skill_snapshot_special_file")
    if before.st_size > MAX_FILE_BYTES:
        _fail("skill_snapshot_file_too_large")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=directory_fd)
        with os.fdopen(descriptor, "rb", closefd=True) as stream:
            opened = os.fstat(stream.fileno())
            if not _same_file(before, opened):
                _fail("skill_snapshot_changed")
            data = stream.read(MAX_FILE_BYTES + 1)
            after_read = os.fstat(stream.fileno())
        after_path = os.stat(
            name,
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
    except AcceptedSkillMaterializationError:
        raise
    except OSError as exc:
        raise AcceptedSkillMaterializationError("skill_snapshot_file_unreadable") from exc
    if len(data) > MAX_FILE_BYTES:
        _fail("skill_snapshot_file_too_large")
    if not _same_file(opened, after_read) or not _same_file(before, after_path):
        _fail("skill_snapshot_changed")
    return data, bool(before.st_mode & 0o111)


def _walk_skill(
    snapshot_root: Path,
    skill_relative_path: str,
) -> list[tuple[str, bytes, bool]]:
    """Read one skill tree without following any projection-path symlink."""

    try:
        root_stat = snapshot_root.lstat()
    except OSError as exc:
        raise AcceptedSkillMaterializationError("skill_snapshot_unavailable") from exc
    if stat.S_ISLNK(root_stat.st_mode):
        _fail("skill_snapshot_symlink")
    if not stat.S_ISDIR(root_stat.st_mode):
        _fail("skill_snapshot_tree_invalid")
    directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        root_fd = os.open(snapshot_root, directory_flags)
    except OSError as exc:
        raise AcceptedSkillMaterializationError("skill_snapshot_tree_unreadable") from exc
    chain: list[tuple[int, str, os.stat_result, int]] = []
    current_fd = root_fd
    try:
        for part in Path(_bounded_relative(skill_relative_path)).parts:
            metadata = os.stat(
                part,
                dir_fd=current_fd,
                follow_symlinks=False,
            )
            if stat.S_ISLNK(metadata.st_mode):
                _fail("skill_snapshot_symlink")
            if not stat.S_ISDIR(metadata.st_mode):
                _fail("skill_snapshot_tree_invalid")
            child_fd = os.open(
                part,
                directory_flags,
                dir_fd=current_fd,
            )
            if not _same_file(metadata, os.fstat(child_fd)):
                os.close(child_fd)
                _fail("skill_snapshot_changed")
            chain.append((current_fd, part, metadata, child_fd))
            current_fd = child_fd
    except AcceptedSkillMaterializationError:
        for _parent_fd, _part, _metadata, child_fd in reversed(chain):
            os.close(child_fd)
        os.close(root_fd)
        raise
    except OSError as exc:
        for _parent_fd, _part, _metadata, child_fd in reversed(chain):
            os.close(child_fd)
        os.close(root_fd)
        raise AcceptedSkillMaterializationError(
            "skill_snapshot_tree_unreadable",
        ) from exc
    files: list[tuple[str, bytes, bool]] = []
    seen: set[str] = set()
    total_bytes = 0
    total_entries = 0

    def visit(directory_fd: int, relative_root: str) -> None:
        nonlocal total_bytes, total_entries
        try:
            with os.scandir(directory_fd) as iterator:
                entries = sorted(iterator, key=lambda item: item.name)
        except OSError as exc:
            raise AcceptedSkillMaterializationError(
                "skill_snapshot_tree_unreadable",
            ) from exc
        for entry in entries:
            total_entries += 1
            if total_entries > MAX_TREE_ENTRIES_PER_SKILL:
                _fail("skill_snapshot_too_many_entries")
            relative = f"{relative_root}/{entry.name}" if relative_root else entry.name
            normalized = _bounded_relative(relative)
            if normalized in seen:
                _fail("skill_snapshot_duplicate_path")
            seen.add(normalized)
            try:
                metadata = os.stat(
                    entry.name,
                    dir_fd=directory_fd,
                    follow_symlinks=False,
                )
            except OSError as exc:
                raise AcceptedSkillMaterializationError(
                    "skill_snapshot_file_unreadable",
                ) from exc
            if stat.S_ISLNK(metadata.st_mode):
                _fail("skill_snapshot_symlink")
            if stat.S_ISDIR(metadata.st_mode):
                try:
                    child_fd = os.open(
                        entry.name,
                        directory_flags,
                        dir_fd=directory_fd,
                    )
                except OSError as exc:
                    raise AcceptedSkillMaterializationError(
                        "skill_snapshot_tree_unreadable",
                    ) from exc
                try:
                    if not _same_file(metadata, os.fstat(child_fd)):
                        _fail("skill_snapshot_changed")
                    visit(child_fd, normalized)
                    after = os.stat(
                        entry.name,
                        dir_fd=directory_fd,
                        follow_symlinks=False,
                    )
                    if not _same_file(metadata, after):
                        _fail("skill_snapshot_changed")
                finally:
                    os.close(child_fd)
            elif stat.S_ISREG(metadata.st_mode):
                data, executable = _read_regular_at(
                    directory_fd,
                    entry.name,
                    metadata,
                )
                total_bytes += len(data)
                if total_bytes > MAX_SKILL_BYTES:
                    _fail("skill_snapshot_skill_too_large")
                files.append((normalized, data, executable))
                if len(files) > MAX_FILES_PER_SKILL:
                    _fail("skill_snapshot_too_many_files")
            else:
                _fail("skill_snapshot_special_file")

    try:
        if not _same_file(root_stat, os.fstat(root_fd)):
            _fail("skill_snapshot_changed")
        visit(current_fd, "")
        for parent_fd, part, metadata, child_fd in chain:
            after = os.stat(
                part,
                dir_fd=parent_fd,
                follow_symlinks=False,
            )
            if not _same_file(metadata, after) or not _same_file(
                metadata,
                os.fstat(child_fd),
            ):
                _fail("skill_snapshot_changed")
        if not _same_file(
            root_stat,
            snapshot_root.stat(follow_symlinks=False),
        ):
            _fail("skill_snapshot_changed")
    finally:
        for _parent_fd, _part, _metadata, child_fd in reversed(chain):
            os.close(child_fd)
        os.close(root_fd)
    files.sort(key=lambda item: item[0])
    if not any(relative == "SKILL.md" for relative, _data, _executable in files):
        _fail("skill_snapshot_manifest_missing")
    return files


def _tree_digest(category: str, relative_path: str, files: list[tuple[str, bytes, bool]]) -> str:
    digest = hashlib.sha256()
    for file_relative, data, executable in files:
        header = json.dumps(
            [category, relative_path, file_relative, "executable" if executable else "regular"],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        digest.update(len(header).to_bytes(4, "big"))
        digest.update(header)
        digest.update(len(data).to_bytes(8, "big"))
        digest.update(data)
    return digest.hexdigest()


def _capture(root: Path, evidence: dict[str, object]) -> tuple[list[tuple[dict[str, object], list[tuple[str, bytes, bool]]]], str, int, int]:
    captured: list[tuple[dict[str, object], list[tuple[str, bytes, bool]]]] = []
    rebuilt: list[dict[str, object]] = []
    total_files = 0
    total_bytes = 0
    for projection in evidence["projections"]:
        assert isinstance(projection, dict)
        category = str(projection["category"])
        relative_path = str(projection["relative_path"])
        skill_relative = posixpath.join(category, relative_path)
        files = _walk_skill(root, skill_relative)
        skill_bytes = sum(len(data) for _path, data, _executable in files)
        manifest = next(data for path, data, _executable in files if path == "SKILL.md")
        rebuilt_projection = {
            "name": projection["name"],
            "category": category,
            "relative_path": relative_path,
            "manifest_digest": hashlib.sha256(manifest).hexdigest(),
            "content_digest": _tree_digest(category, relative_path, files),
            "file_count": len(files),
            "total_bytes": skill_bytes,
        }
        if rebuilt_projection != projection:
            _fail("skill_snapshot_drift")
        captured.append((projection, files))
        rebuilt.append(rebuilt_projection)
        total_files += len(files)
        total_bytes += skill_bytes
        if total_files > MAX_TOTAL_FILES:
            _fail("skill_snapshot_too_many_files")
        if total_bytes > MAX_TOTAL_BYTES:
            _fail("skill_snapshot_too_large")
    payload = json.dumps(
        {"version": 1, "skills": rebuilt},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    if digest != evidence["snapshot_id"] or total_files != evidence["file_count"] or total_bytes != evidence["total_bytes"]:
        _fail("skill_snapshot_drift")
    return captured, digest, total_files, total_bytes


def _write_private(path: Path, data: bytes, executable: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o700 if executable else 0o600)
    with os.fdopen(descriptor, "wb", closefd=True) as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _make_read_only(root: Path) -> None:
    for current, directories, files in os.walk(root, topdown=False, followlinks=False):
        current_path = Path(current)
        for name in files:
            file_path = current_path / name
            executable = bool(file_path.stat().st_mode & 0o111)
            file_path.chmod(0o555 if executable else 0o444)
        for name in directories:
            (current_path / name).chmod(0o555)
        current_path.chmod(0o555)


def materialize_verified_snapshot(*, source: Path, destination: Path, evidence: object) -> dict[str, object]:
    """Atomically publish a verified private copy beneath ``destination``."""

    normalized = validate_evidence(evidence)
    captured, digest, file_count, total_bytes = _capture(source, normalized)
    destination.mkdir(parents=True, exist_ok=True, mode=0o700)
    final = destination / str(normalized["snapshot_id"])
    if final.exists() or final.is_symlink():
        _fail("skill_snapshot_destination_exists")
    stage = Path(tempfile.mkdtemp(prefix=".building-", dir=destination))
    try:
        for projection, files in captured:
            for relative, data, executable in files:
                _write_private(
                    stage / str(projection["category"]) / Path(str(projection["relative_path"])) / relative,
                    data,
                    executable,
                )
        confirmed, confirmed_digest, confirmed_files, confirmed_bytes = _capture(source, normalized)
        staged, staged_digest, staged_files, staged_bytes = _capture(stage, normalized)
        if confirmed != captured or staged != captured or (confirmed_digest, confirmed_files, confirmed_bytes) != (digest, file_count, total_bytes) or (staged_digest, staged_files, staged_bytes) != (digest, file_count, total_bytes):
            _fail("skill_snapshot_changed")
        _make_read_only(stage)
        os.replace(stage, final)
        return {
            "version": 2,
            "profile": _PROFILE,
            "snapshot_id": normalized["snapshot_id"],
            "content_digest": digest,
            "file_count": file_count,
            "total_bytes": total_bytes,
        }
    except Exception:
        if stage.exists():
            shutil.rmtree(stage, ignore_errors=True)
        raise


def write_materialization_receipt(
    path: Path,
    receipt: dict[str, object],
) -> None:
    """Write the verifier-authored receipt once for the gate sidecar."""

    payload = json.dumps(
        receipt,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(payload) > MAX_RECEIPT_BYTES:
        _fail("skill_snapshot_receipt_too_large")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o400,
    )
    with os.fdopen(descriptor, "wb", closefd=True) as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _read_json(path: Path) -> object:
    try:
        if path.stat().st_size > MAX_EVIDENCE_BYTES:
            _fail("skill_snapshot_evidence_too_large")
        return json.loads(path.read_text(encoding="utf-8"))
    except AcceptedSkillMaterializationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise AcceptedSkillMaterializationError("skill_snapshot_evidence_invalid") from exc


class _GateHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "HartmeshAcceptedSkillGate/2"

    def log_message(self, format: str, *args: object) -> None:
        return None

    def _handle(self) -> None:
        expected = self.server.attempt_capability  # type: ignore[attr-defined]
        authorization = self.headers.get("Authorization", "")
        supplied = authorization.removeprefix("Bearer ") if authorization.startswith("Bearer ") else ""
        if not supplied or not hmac.compare_digest(supplied, expected):
            self.send_error(403, "Forbidden")
            return
        if self.path == "/__hartmesh/accepted-material/v2":
            if self.command != "GET":
                self.send_error(405, "Method Not Allowed")
                return
            receipt_file = self.server.receipt_file  # type: ignore[attr-defined]
            if receipt_file is None:
                self.send_error(404, "Not Found")
                return
            try:
                payload = receipt_file.read_bytes()
            except OSError:
                self.send_error(503, "Service Unavailable")
                return
            if not payload or len(payload) > MAX_RECEIPT_BYTES:
                self.send_error(503, "Service Unavailable")
                return
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        length_text = self.headers.get("Content-Length", "0")
        try:
            length = int(length_text)
        except ValueError:
            self.send_error(400, "Bad Request")
            return
        if length < 0 or length > MAX_GATE_BODY_BYTES:
            self.send_error(413, "Payload Too Large")
            return
        body = self.rfile.read(length) if length else None
        upstream = self.server.upstream.rstrip("/") + self.path  # type: ignore[attr-defined]
        headers = {key: value for key, value in self.headers.items() if key.lower() not in _HOP_HEADERS and key.lower() not in {"authorization", "host", "content-length"}}
        request = urllib.request.Request(
            upstream,
            data=body,
            headers=headers,
            method=self.command,
        )
        try:
            response = self.server.opener.open(request, timeout=620)  # type: ignore[attr-defined]
        except urllib.error.HTTPError as error:
            response = error
        except (OSError, urllib.error.URLError):
            self.send_error(502, "Bad Gateway")
            return
        with response:
            payload = response.read(MAX_GATE_BODY_BYTES + 1)
            if len(payload) > MAX_GATE_BODY_BYTES:
                self.send_error(502, "Bad Gateway")
                return
            self.send_response(response.status)
            for key, value in response.headers.items():
                if key.lower() not in _HOP_HEADERS and key.lower() != "content-length":
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    do_DELETE = _handle
    do_GET = _handle
    do_HEAD = _handle
    do_PATCH = _handle
    do_POST = _handle
    do_PUT = _handle


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        del req, fp, code, msg, headers, newurl
        return None


class _BoundedThreadingHTTPServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._capacity = threading.BoundedSemaphore(MAX_GATE_CONCURRENCY)

    def process_request(self, request, client_address) -> None:
        if not self._capacity.acquire(blocking=False):
            self.shutdown_request(request)
            return
        super().process_request(request, client_address)

    def process_request_thread(self, request, client_address) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._capacity.release()


def serve_gate(
    *,
    listen_host: str,
    listen_port: int,
    upstream: str,
    capability_file: Path,
    receipt_file: Path | None = None,
) -> None:
    capability = _bounded_text(capability_file.read_text(encoding="utf-8").strip(), field="capability", maximum=128)
    server = _BoundedThreadingHTTPServer((listen_host, listen_port), _GateHandler)
    server.attempt_capability = capability  # type: ignore[attr-defined]
    server.upstream = upstream  # type: ignore[attr-defined]
    server.receipt_file = receipt_file  # type: ignore[attr-defined]
    server.opener = urllib.request.build_opener(_NoRedirect())  # type: ignore[attr-defined]
    server.serve_forever()


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    materialize = subparsers.add_parser("materialize")
    materialize.add_argument("--source", type=Path, required=True)
    materialize.add_argument("--destination", type=Path, required=True)
    materialize.add_argument("--evidence-file", type=Path, required=True)
    materialize.add_argument("--receipt-file", type=Path, required=True)
    gate = subparsers.add_parser("gate")
    gate.add_argument("--listen-host", default="0.0.0.0")
    gate.add_argument("--listen-port", type=int, default=8081)
    gate.add_argument("--upstream", default="http://127.0.0.1:8080")
    gate.add_argument("--capability-file", type=Path, required=True)
    gate.add_argument("--receipt-file", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "materialize":
        receipt = materialize_verified_snapshot(
            source=args.source,
            destination=args.destination,
            evidence=_read_json(args.evidence_file),
        )
        write_materialization_receipt(args.receipt_file, receipt)
        return 0
    serve_gate(
        listen_host=args.listen_host,
        listen_port=args.listen_port,
        upstream=args.upstream,
        capability_file=args.capability_file,
        receipt_file=args.receipt_file,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
