"""Generic, installed preview dispatch over one authorized source snapshot."""

import hashlib
import os
from contextlib import asynccontextmanager

from fastapi import HTTPException, Request
from fastapi.responses import Response

from app.gateway.auth_disabled import AUTH_SOURCE_AUTH_DISABLED, AUTH_SOURCE_INTERNAL, AUTH_SOURCE_PAT, AUTH_SOURCE_SESSION, is_auth_disabled
from app.gateway.authz import Permissions, require_permission, resolve_route_permissions_for_request
from app.gateway.deps import get_thread_store, resolve_session_cookie
from app.gateway.internal_auth import get_trusted_internal_owner_user_id
from app.gateway.path_utils import normalize_outputs_virtual_path
from deerflow.config.paths import get_paths
from deerflow.extensions.plugin_tools import plugin_settings
from deerflow.extensions.projection_host import ProjectionError, ProjectionHost
from deerflow.files.store import SafeFileAccessUnavailable, StoreError, open_regular_source
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.utils.file_io import await_drained, run_file_io


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


@asynccontextmanager
async def projection_lifespan(app):
    host = ProjectionHost()
    app.state.projection_host = host
    try:
        host.start(
            declaration.project
            for source, plugin in getattr(getattr(app.state, "extensions", None), "plugins", ())
            if plugin_settings(source, plugin)["enabled"] is True
            for declaration in plugin.artifacts
            if declaration.project is not None
        )
        yield
    finally:
        await host.close()
        app.state.projection_host = None


async def _thread_generation(request, thread_id):
    # An untracked legacy thread remains readable while it stays untracked.
    # A tracked thread keeps its owner and its server-assigned generation.
    record = await get_thread_store(request).get(thread_id, user_id=None)
    if record is None:
        return None
    owner = record.get("user_id")
    if owner is not None and str(owner) not in (str(request.state.auth.user.id), get_trusted_internal_owner_user_id(request)):
        raise HTTPException(404, "Thread not found")
    return owner, record.get("incarnation") or record.get("created_at")


@require_permission("threads", "read", owner_check=True)
async def _check_delivery(request: Request, thread_id: str, chat_access, generation):
    """Recheck the current conversation binding, owner, and managed Space routing."""
    from app.gateway.agent_conversations import reject_legacy_binding

    # A queued request can outlive a role change, logout credential revocation,
    # or withdrawal of the thread owner's access. Preserve the original identity.
    credential = getattr(request.state, "auth_source", None)
    user = request.state.auth.user
    if credential == AUTH_SOURCE_SESSION:
        user = await resolve_session_cookie(request)
    elif credential == AUTH_SOURCE_PAT:
        from app.gateway.auth.pat import authenticate_pat

        user, scopes = await authenticate_pat(request.app, request.headers.get("authorization"))
        if Permissions.THREADS_READ not in scopes:
            raise HTTPException(403, "Permission denied: threads:read")
    elif credential == AUTH_SOURCE_INTERNAL:
        from app.gateway.auth.mode import owner_is_refused
        from app.gateway.internal_auth import INTERNAL_AUTH_HEADER_NAME, is_valid_internal_auth_token

        owner = get_trusted_internal_owner_user_id(request)
        if not is_valid_internal_auth_token(request.headers.get(INTERNAL_AUTH_HEADER_NAME)) or (owner and await owner_is_refused(owner)):
            raise HTTPException(401, "Internal caller unavailable")
    elif credential == AUTH_SOURCE_AUTH_DISABLED and not is_auth_disabled():
        raise HTTPException(401, "Authentication required")

    if user.id != request.state.auth.user.id:
        raise HTTPException(401, "Account changed")
    if credential is not None and Permissions.THREADS_READ not in await resolve_route_permissions_for_request(request, user):
        raise HTTPException(403, "Permission denied: threads:read")
    await reject_legacy_binding(request, thread_id)
    if chat_access is not None:
        await chat_access.refresh(request)
    if await _thread_generation(request, thread_id) != generation:
        raise HTTPException(404, "Thread not found")


def _fingerprint(source, budget):
    digest = hashlib.sha256()
    size = 0
    while block := source.read(min(65536, budget + 1 - size)):
        size += len(block)
        if size > budget:
            raise HTTPException(413, "Artifact source exceeds its preview budget")
        digest.update(block)
    return digest.hexdigest(), size


async def project_artifact(installed, thread_id: str, path: str, *, request: Request, user_id: str | None, chat_access) -> Response:
    source_name, plugin, declaration = installed
    if plugin_settings(source_name, plugin)["enabled"] is not True or declaration.project is None:
        raise HTTPException(501, "Artifact preview capability unavailable")
    if not any(path.lower().endswith(suffix) for suffix in declaration.suffixes):
        raise HTTPException(415, "Artifact preview does not match this file")
    try:
        virtual = normalize_outputs_virtual_path(path)
    except HTTPException:
        raise HTTPException(415, "Artifact preview requires an outputs source") from None

    budget = declaration.source_max_bytes

    async def read(reader):
        def bounded(source):
            if os.fstat(source.fileno()).st_size > budget:
                raise HTTPException(413, "Artifact source exceeds its preview budget")
            return reader(source)

        if chat_access is not None:
            return await chat_access.read_with(request, path, bounded)

        def read_legacy():
            root = get_paths().sandbox_outputs_dir(thread_id, user_id=user_id or get_effective_user_id())
            actual = root / virtual.removeprefix("/mnt/user-data/outputs/")
            try:
                with os.fdopen(open_regular_source(actual), "rb") as source:
                    return bounded(source)
            except SafeFileAccessUnavailable:
                raise HTTPException(501, "Artifact preview requires safe source reads") from None
            except FileNotFoundError:
                raise HTTPException(404, "Artifact source not found") from None
            except (StoreError, PermissionError):
                raise HTTPException(403, "Artifact source not accessible") from None

        return await await_drained(run_file_io(read_legacy))

    host = getattr(request.app.state, "projection_host", None)
    if host is None:
        raise HTTPException(501, "Artifact preview worker unavailable")
    generation = await _thread_generation(request, thread_id)
    try:
        content, digest, size = await host.project(
            key=(plugin.namespace, declaration.id, declaration.projection_marker),
            projector=declaration.project,
            requester_id=str(request.state.auth.user.id),
            source_max_bytes=budget,
            preview_max_bytes=declaration.preview_max_bytes,
            fingerprint=lambda: read(lambda source: _fingerprint(source, budget)),
            read_source=lambda: read(lambda source: source.read(budget + 1)),
        )
    except ProjectionError as exc:
        if exc.status == 503:
            raise HTTPException(503, {"code": "preview-busy", "message": "Previews are busy. Try again in a moment."}, headers={"Retry-After": str(exc.retry_after or 1)}) from None
        raise HTTPException(exc.status, "Artifact preview unavailable") from None
    await _check_delivery(request=request, thread_id=thread_id, chat_access=chat_access, generation=generation)
    return Response(
        content,
        media_type="application/json",
        headers={
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "X-Artifact-Projection": declaration.projection_marker,
            "X-Artifact-Source-Bytes": str(size),
            "ETag": f'"{digest}"',
        },
    )
