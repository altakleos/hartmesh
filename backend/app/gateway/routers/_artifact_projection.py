"""Generic, installed preview dispatch over one authorized source snapshot."""

import hashlib
import inspect
import logging
import os

from fastapi import HTTPException
from fastapi.responses import Response

from app.gateway.path_utils import normalize_outputs_virtual_path
from deerflow.config.paths import get_paths
from deerflow.extensions.plugin_tools import plugin_settings
from deerflow.files.store import SafeFileAccessUnavailable, StoreError, open_regular_source
from deerflow.runtime.user_context import get_effective_user_id

logger = logging.getLogger(__name__)


def installed_projection(extensions, params):
    """Return an installed declaration, or None for an ordinary source read."""
    plugins = getattr(extensions, "plugins", ())
    key = params.get("preview")
    if isinstance(key, str):
        for source, plugin in plugins:
            for declaration in plugin.artifacts:
                if key == f"{plugin.namespace}/{declaration.id}":
                    return source, plugin, declaration
        raise HTTPException(501, "Artifact preview capability unavailable")
    for source, plugin in plugins:
        for declaration in plugin.artifacts:
            for alias in declaration.compat_queries:
                value = params.get(alias)
                if value is None:
                    continue
                if value.lower() in {"true", "1", "yes", "on"}:
                    return source, plugin, declaration
                if value.lower() not in {"false", "0", "no", "off"}:
                    raise HTTPException(422, "Invalid artifact preview flag")
    return None


def project_artifact(installed, thread_id: str, path: str, *, user_id: str | None) -> Response:
    source_name, plugin, declaration = installed
    if plugin_settings(source_name, plugin)["enabled"] is not True or declaration.project is None:
        raise HTTPException(501, "Artifact preview capability unavailable")
    if not any(path.lower().endswith(suffix) for suffix in declaration.suffixes):
        raise HTTPException(415, "Artifact preview does not match this file")
    try:
        virtual = normalize_outputs_virtual_path(path)
    except HTTPException:
        raise HTTPException(415, "Artifact preview requires an outputs source") from None
    root = get_paths().sandbox_outputs_dir(thread_id, user_id=user_id or get_effective_user_id())
    actual = root / virtual.removeprefix("/mnt/user-data/outputs/")
    try:
        with os.fdopen(open_regular_source(actual), "rb") as source:
            if os.fstat(source.fileno()).st_size > declaration.source_max_bytes:
                raise HTTPException(413, "Artifact source exceeds its preview budget")
            raw = source.read(declaration.source_max_bytes + 1)
    except SafeFileAccessUnavailable:
        raise HTTPException(501, "Artifact preview requires safe source reads") from None
    except FileNotFoundError:
        raise HTTPException(404, "Artifact source not found") from None
    except (StoreError, PermissionError):
        raise HTTPException(403, "Artifact source not accessible") from None
    if len(raw) > declaration.source_max_bytes:
        raise HTTPException(413, "Artifact source exceeds its preview budget")
    try:
        content = declaration.project(raw)
        if inspect.iscoroutine(content):
            content.close()
        if not isinstance(content, bytes):
            raise ValueError("Preview callbacks must return bytes")
    except Exception:
        logger.warning("Artifact preview failed: %s/%s", plugin.namespace, declaration.id)
        raise HTTPException(422, "Artifact preview unavailable") from None
    if len(content) > declaration.preview_max_bytes:
        raise HTTPException(413, "Artifact preview exceeds its response budget")
    return Response(
        content,
        media_type="application/json",
        headers={
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "X-Artifact-Projection": declaration.projection_marker,
            "X-Artifact-Source-Bytes": str(len(raw)),
            "ETag": f'"{hashlib.sha256(raw).hexdigest()}"',
        },
    )
