"""Bounded report-card projection; canonical report bytes remain untouched."""

import hashlib
import json
import os

from fastapi import HTTPException
from fastapi.responses import Response

from app.gateway.path_utils import normalize_outputs_virtual_path
from deerflow.config.paths import get_paths
from deerflow.files.store import StoreError, open_regular_source
from deerflow.runtime.user_context import get_effective_user_id

MAX_REPORT_BYTES = 16 * 1024 * 1024
MAX_PROJECTION_BYTES = 1024 * 1024
_CARD_FIELDS = ("version", "meta", "kpis", "sections", "charts", "checks", "notes")
_META_FIELDS = ("title", "period", "draft", "brand", "company", "currency", "inputs")


def _reject_nonfinite(value: str) -> None:
    raise ValueError("Non-finite JSON number")


def report_projection(thread_id: str, path: str, *, user_id: str | None) -> Response:
    if not path.endswith(".report.json"):
        raise HTTPException(415, "Not a report artifact")
    # Other artifact namespaces retain their ordinary source preview.
    try:
        virtual = normalize_outputs_virtual_path(path)
    except HTTPException:
        raise HTTPException(415, "Report projection requires an outputs artifact") from None
    if os.open not in os.supports_dir_fd or not hasattr(os, "O_NOFOLLOW"):
        raise HTTPException(501, "Report projection requires safe descriptor-relative reads")
    root = get_paths().sandbox_outputs_dir(thread_id, user_id=user_id or get_effective_user_id())
    actual = root / virtual.removeprefix("/mnt/user-data/outputs/")
    try:
        with os.fdopen(open_regular_source(actual), "rb") as source:
            if os.fstat(source.fileno()).st_size > MAX_REPORT_BYTES:
                raise HTTPException(413, "Report is too large to preview")
            raw = source.read(MAX_REPORT_BYTES + 1)
    except FileNotFoundError:
        raise HTTPException(404, "Report artifact not found") from None
    except (StoreError, PermissionError):
        raise HTTPException(403, "Report artifact is not accessible") from None
    if len(raw) > MAX_REPORT_BYTES:
        raise HTTPException(413, "Report is too large to preview")
    try:
        report = json.loads(raw.decode("utf-8"), parse_constant=_reject_nonfinite)
        if not isinstance(report, dict) or not isinstance(report.get("meta"), dict):
            raise ValueError("Not a report object")
        projection = {key: report[key] for key in _CARD_FIELDS if key in report}
        projection["meta"] = {key: report["meta"][key] for key in _META_FIELDS if key in report["meta"]}
        content = json.dumps(projection, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")
    except (ValueError, RecursionError):
        raise HTTPException(422, "Report cannot be projected") from None
    if len(content) > MAX_PROJECTION_BYTES:
        raise HTTPException(413, "Report card is too large to preview")
    return Response(
        content=content,
        media_type="application/json",
        headers={
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "X-Artifact-Projection": "business-report-v1",
            "X-Artifact-Source-Bytes": str(len(raw)),
            "ETag": f'"{hashlib.sha256(raw).hexdigest()}"',
        },
    )
