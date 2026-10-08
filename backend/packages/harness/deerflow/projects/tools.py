"""Project shelf tools: bounded live reads through the pinned project identity.

Two read-only tools (Phase-2 spec §7.3; the shelf is user-curated — there is
no agent-initiated shelf write). Both take ``project_id`` from the run's
admission-pinned context (``PROJECT_CONTEXT_KEY``) and ``user_id`` from
:func:`resolve_runtime_user_id`, then query **live** shelf rows: the pinned
snapshot fixes *which* project, never *what* the shelf currently holds. A
missing pin or a missing session factory is a tool error, never an empty
success; a document trashed after this run's index was rendered fails with a
"no longer on the shelf" error rather than serving stale content (§11).

Registration is conditional on the pinned key (§10.11) — see
``agents/lead_agent/agent.py``; subagents never receive these tools. Every
file read and conversion is offloaded via
:func:`deerflow.utils.file_io.run_file_io`.
"""

from __future__ import annotations

import json
import logging
from typing import Annotated, Any

from langchain.tools import tool

from deerflow.agents.middlewares.input_sanitization_middleware import neutralize_untrusted_tags
from deerflow.config.paths import Paths, get_paths
from deerflow.projects.context import pinned_project_snapshot
from deerflow.projects.documents import auto_convert_documents_enabled, document_char_count, read_document_text_window, read_text_serving_path
from deerflow.runtime.user_context import resolve_runtime_user_id
from deerflow.tools.types import Runtime
from deerflow.utils.file_io import run_file_io

logger = logging.getLogger(__name__)

_LIST_DEFAULT_LIMIT = 50
_LIST_MAX_LIMIT = 200
_READ_DEFAULT_LIMIT = 8000
_READ_MAX_LIMIT = 20000

_BINARY_DECLINE_MESSAGE = "This document is binary and cannot be read as text. To let the agent process it, attach it to a thread (attach-to-thread) instead."
_CONVERSION_DISABLED_MESSAGE = "This document is a convertible office/PDF file, but automatic document conversion is disabled (uploads.auto_convert_documents). Enable conversion, or attach the file to a thread (attach-to-thread) instead."
_NOT_ON_SHELF_MESSAGE = "This document is no longer on the shelf (it was trashed or removed after this run started). Call list_project_documents for the current shelf."
_CONTENT_MISSING_MESSAGE = "This document's content is missing from storage (content_missing); only its shelf row remains. Move it to trash from the project page."
_NO_PROJECT_CONTEXT_MESSAGE = "No project is pinned for this run; project document tools are only available in project member threads."
_NO_STORE_MESSAGE = "Project document store is unavailable."


def _error(message: str) -> str:
    return json.dumps({"error": message}, ensure_ascii=False)


def _resolve_pin_and_repo(runtime: Runtime) -> tuple[str, str, Any] | str:
    """Resolve ``(project_id, user_id, repository)`` or a JSON error string.

    Fail closed (§7.3): no pinned project context or no session factory is a
    tool error with no data, never an empty success.
    """
    snapshot = pinned_project_snapshot(runtime)
    project_id = str(snapshot.get("project_id") or "") if snapshot is not None else ""
    if not project_id:
        return _error(_NO_PROJECT_CONTEXT_MESSAGE)
    from deerflow.persistence import get_session_factory
    from deerflow.persistence.projects import ProjectDocumentRepository

    paths = get_paths()
    resource_repo = getattr(paths, "project_document_repository", None)
    if resource_repo is not None:
        user_id = resolve_runtime_user_id(runtime)
        if user_id != paths._feature_user:
            return _error("Project resource identity does not match the host caller")
        return project_id, user_id, resource_repo
    session_factory = get_session_factory()
    if session_factory is None:
        return _error(_NO_STORE_MESSAGE)
    user_id = resolve_runtime_user_id(runtime)
    return project_id, user_id, ProjectDocumentRepository(session_factory)


def _resolve_auto_convert() -> bool:
    """Worker-thread config read: the app config may cold-load from disk."""
    try:
        from deerflow.config.app_config import get_app_config

        return auto_convert_documents_enabled(get_app_config())
    except Exception:
        return False


def _clamp(value: int, *, default: int, lower: int, upper: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(lower, min(parsed, upper))


def _shelf_entry_json(row: dict) -> dict[str, Any]:
    return {
        "id": row["id"],
        "name": neutralize_untrusted_tags(str(row.get("name") or "")),
        "size_bytes": int(row.get("size_bytes") or 0),
        "updated_at": str(row.get("updated_at") or ""),
    }


async def _list_project_documents_impl(runtime: Runtime, *, offset: int, limit: int) -> str:
    resolved = _resolve_pin_and_repo(runtime)
    if isinstance(resolved, str):
        return resolved
    project_id, user_id, repo = resolved
    limit = _clamp(limit, default=_LIST_DEFAULT_LIMIT, lower=1, upper=_LIST_MAX_LIMIT)
    offset = max(_clamp(offset, default=0, lower=0, upper=1 << 62), 0)
    rows = await repo.list_active(project_id, limit=limit, offset=offset, user_id=user_id)
    total = await repo.count_active(project_id, user_id=user_id)
    next_offset: int | None = offset + len(rows) if offset + len(rows) < total else None
    return json.dumps(
        {
            "total": total,
            "offset": offset,
            "next_offset": next_offset,
            "documents": [_shelf_entry_json(row) for row in rows],
        },
        ensure_ascii=False,
    )


async def _read_project_document_impl(runtime: Runtime, *, document_id: str, offset: int, limit: int, paths: Paths | None = None) -> str:
    resolved = _resolve_pin_and_repo(runtime)
    if isinstance(resolved, str):
        return resolved
    project_id, user_id, repo = resolved
    row = await repo.get(document_id, user_id=user_id)
    # Fail closed on every mismatch: trashed/purged after the index rendered,
    # foreign, or belonging to a different project than the pinned one — one
    # stale-entry error, never cross-project reads, never stale content (§11).
    if row is None or row.get("project_id") != project_id:
        return _error(_NOT_ON_SHELF_MESSAGE)
    paths = paths or get_paths()
    auto_convert = await run_file_io(_resolve_auto_convert)
    serving_path, reason = await read_text_serving_path(repo, paths, user_id=user_id, row=row, auto_convert=auto_convert)
    if serving_path is None:
        if reason == "content_missing":
            return _error(_CONTENT_MISSING_MESSAGE)
        if reason == "conversion_disabled":
            return _error(_CONVERSION_DISABLED_MESSAGE)
        return _error(_BINARY_DECLINE_MESSAGE)
    offset = _clamp(offset, default=0, lower=0, upper=1 << 62)
    limit = _clamp(limit, default=_READ_DEFAULT_LIMIT, lower=1, upper=_READ_MAX_LIMIT)
    # The character count is content-identity cached (immutable rows, §6.2);
    # the windowed read decodes only what the page needs — and past-end pages
    # read nothing at all.
    total_chars = await document_char_count(document_id=row["id"], sha256=row["sha256"], path=serving_path)
    content = await read_document_text_window(serving_path, offset=offset, limit=limit) if offset < total_chars else ""
    return json.dumps(
        {
            "name": neutralize_untrusted_tags(str(row.get("name") or "")),
            "total_chars": total_chars,
            "offset": offset,
            "returned_chars": len(content),
            "truncated": offset + len(content) < total_chars,
            "content": content,
        },
        ensure_ascii=False,
    )


@tool
async def list_project_documents(
    runtime: Runtime,
    offset: Annotated[int, "Number of shelf entries to skip for pagination (default 0). Use next_offset from a previous call to walk the shelf."] = 0,
    limit: Annotated[int, "Maximum entries to return (default 50, max 200)."] = _LIST_DEFAULT_LIMIT,
) -> str:
    """List documents on the current project's shelf (metadata only, no content).

    Returns JSON: {"total", "offset", "next_offset", "documents": [{"id",
    "name", "size_bytes", "updated_at"}, ...]} in the shelf's own order
    (recently updated first). Use this when the <documents> index says the
    shelf has more entries than it shows, or the user asks what documents the
    project holds. Each entry's stable "id" is what read_project_document
    takes. Live data: reflects the shelf as of this call.
    """
    return await _with_resource_feature(runtime, lambda: _list_project_documents_impl(runtime, offset=offset, limit=limit), write=False)


@tool
async def read_project_document(
    runtime: Runtime,
    document_id: Annotated[str, "Stable document ID from the <documents> index or list_project_documents."],
    offset: Annotated[int, "Character offset into the document's text (default 0)."] = 0,
    limit: Annotated[int, "Maximum characters to return (default 8000, max 20000)."] = _READ_DEFAULT_LIMIT,
) -> str:
    """Read a bounded text slice of one project shelf document.

    Returns JSON: {"name", "total_chars", "offset", "returned_chars",
    "truncated", "content"}. Long documents are served in slices: advance
    "offset" by the returned character count while "truncated" is true.
    Office/PDF documents are served as converted markdown when conversion is
    enabled. Binary documents cannot be read — attach them to a thread
    instead. A document trashed after this run started reports that it is no
    longer on the shelf.
    """
    return await _with_resource_feature(runtime, lambda: _read_project_document_impl(runtime, document_id=document_id, offset=offset, limit=limit), write=True)


async def _with_resource_feature(runtime, callback, *, write):
    from deerflow.config.app_config import get_app_config
    from deerflow.extensions import resolve_run_extensions
    from deerflow.features.execution import run_project_feature
    from deerflow.runtime.context_keys import STORAGE_PROVIDER_CONTEXT_KEY
    from deerflow.spaces.facade import HostStorageProvider

    context = getattr(runtime, "context", None)
    provider = context.get(STORAGE_PROVIDER_CONTEXT_KEY) if isinstance(context, dict) else None
    try:
        config = await run_file_io(get_app_config)
        enabled = getattr(getattr(config, "storage_spaces", None), "enabled", False) is True
    except FileNotFoundError:
        enabled = False
    if not enabled and not (isinstance(provider, HostStorageProvider) and provider.capabilities.available):
        return await callback()
    if not isinstance(provider, HostStorageProvider):
        return _error("Project resources require a current host storage binding")
    try:
        await provider.current()
        return await run_project_feature(resolve_run_extensions(context), callback, write=write, operation="read-document" if write else "list-documents")
    except Exception:
        return _error("The Projects feature or current resource access is unavailable")


def get_project_document_tools() -> list:
    """The two shelf tools the lead agent gains in project runs (§7.3)."""
    return [list_project_documents, read_project_document]
