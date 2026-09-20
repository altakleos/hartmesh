"""What the two file routers do identically: who is asking, and how a path becomes a file.

``/api/files`` (the person's own) and ``/api/shared`` (the company's) differ
in who may write and who may remove. They do not differ in how a caller is
identified, how a virtual path is turned into a host path, or how that file
is handed back to the browser — so those live here once, and both routers
import them. The filesystem layer underneath is shared the same way, in
:mod:`deerflow.files.store`.
"""

from __future__ import annotations

import mimetypes
import os
import stat
from collections.abc import Callable
from pathlib import Path

from fastapi import HTTPException, Request

from app.gateway.internal_auth import get_trusted_internal_owner_user_id
from app.gateway.routers.artifacts import ACTIVE_CONTENT_MIME_TYPES, is_text_file_by_content
from deerflow.config.paths import make_safe_user_id
from deerflow.files.store import StoreError
from deerflow.runtime.user_context import get_effective_user_id

__all__ = ["acting_user_id", "existing_regular_file", "response_plan"]


def acting_user_id(request: Request) -> str:
    """Who is acting: the trusted internal owner, else the caller."""
    raw_owner = get_trusted_internal_owner_user_id(request)
    if raw_owner:
        return make_safe_user_id(raw_owner)
    return get_effective_user_id()


def existing_regular_file(resolve: Callable[[], Path], *, label: str, non_regular_is_missing: bool = True) -> Path:
    """Worker-thread body: the host path *resolve* names, or the HTTP reason it is not a file.

    A link is always ``400`` — the sandbox can plant one, and following it is
    how a file area leaks. What a directory means depends on the question:
    asking to *read* one is a miss (``404``), while offering one as the
    source of a publish is a bad request (``400``), which
    *non_regular_is_missing* selects.
    """
    try:
        actual = resolve()
    except StoreError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    try:
        metadata = os.lstat(actual)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"File not found: {label}") from None
    if stat.S_ISLNK(metadata.st_mode):
        raise HTTPException(status_code=400, detail=f"Not a file: {label}")
    if not stat.S_ISREG(metadata.st_mode):
        if non_regular_is_missing:
            raise HTTPException(status_code=404, detail=f"File not found: {label}")
        raise HTTPException(status_code=400, detail=f"Not a file: {label}")
    return actual


def response_plan(actual: Path, download: bool) -> tuple[bool, str | None]:
    """Worker-thread body: whether to force a download, and the media type."""
    mime_type, _ = mimetypes.guess_type(actual)
    if download or mime_type in ACTIVE_CONTENT_MIME_TYPES:
        return True, mime_type
    if mime_type is None and is_text_file_by_content(actual):
        mime_type = "text/plain"
    return False, mime_type
