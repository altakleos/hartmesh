"""Authenticated discovery and execution for deployment-installed full-stack plugins."""

import asyncio
import hashlib
import json
import logging
from dataclasses import asdict
from types import MappingProxyType

from deerflow_extension_api.auth import resolve_principal
from deerflow_extension_api.storage import StorageConflict, StorageIdentityRequired, StorageOperationPending, StorageUnavailable
from fastapi import APIRouter, HTTPException, Request, Response

from deerflow.extensions.browser_assets import LoadedBrowserAssets, valid_asset_path
from deerflow.extensions.plugin_tools import plugin_settings

router = APIRouter(prefix="/api/plugins", tags=["plugins"])
logger = logging.getLogger(__name__)


def _principal(request):
    principal = resolve_principal(request)
    if principal is None:
        raise HTTPException(401, "Authentication required.")
    return principal


@router.get("")
async def list_plugins(request: Request, response: Response):
    principal = _principal(request)
    response.headers["Cache-Control"] = "private, no-store"
    entries = []
    for source, plugin in request.app.state.extensions.plugins:
        settings = plugin_settings(source, plugin)
        provider = getattr(request.app.state, "extension_storage", None)
        if plugin.storage_api_version == 1 and provider is not None:
            provider = provider.for_plugin(source, plugin, lambda: request.app.state.extensions)
        module = plugin.frontend
        if isinstance(module, LoadedBrowserAssets):
            revision = module.revision
            entry = f"/api/plugins/{plugin.namespace}/assets/{revision}/{module.entry}"
            transport = "assets-v1"
        else:
            revision = hashlib.sha256(module.code.encode()).hexdigest() if module else None
            entry = f"/api/plugins/modules/{module.module}/{revision}.mjs" if module else None
            transport = "inline-v1" if module else None
        public = ("enabled", *module.public_fields) if module else ("enabled",)
        management_allowed = False
        if settings["enabled"] is True and any(action.purpose == "management" for action in plugin.backend):
            from app.gateway.app import _resolve_extension_plugin_management_async

            management_allowed = await _resolve_extension_plugin_management_async(request, plugin.namespace, "write") is True
        entries.append(
            {
                "namespace": plugin.namespace,
                "title": plugin.title,
                "description": plugin.description,
                "viewer_id": principal.user_id,
                "module": module.module if module else None,
                "entry": entry,
                "transport": transport,
                "settings": {key: settings[key] for key in public},
                "backend_actions": [action.name for action in plugin.backend if action.purpose != "management" or management_allowed],
                "artifact_presentations": [
                    {"id": item.id, "suffixes": list(item.suffixes), "source_max_bytes": item.source_max_bytes, "preview_max_bytes": item.preview_max_bytes, "projection_marker": item.projection_marker} for item in plugin.artifacts
                ],
                "storage_api_version": plugin.storage_api_version,
                "actor_kinds": list(plugin.actor_kinds),
                "storage_capabilities": asdict(provider.capabilities) if plugin.storage_api_version == 1 and provider is not None else None,
            }
        )
    return entries


@router.get("/modules/{module}/{revision}.mjs")
async def plugin_module(request: Request, module: str, revision: str):
    _principal(request)
    for _, plugin in request.app.state.extensions.plugins:
        if plugin.frontend and not isinstance(plugin.frontend, LoadedBrowserAssets) and plugin.frontend.module == module:
            code = plugin.frontend.code.encode()
            if hashlib.sha256(code).hexdigest() == revision:
                return Response(code, media_type="text/javascript", headers={"Cache-Control": "private, no-store", "X-Content-Type-Options": "nosniff"})
    raise HTTPException(404, "Plugin module unavailable; reload the page.")


@router.get("/{namespace}/assets/{revision}/{path:path}")
async def plugin_asset(request: Request, namespace: str, revision: str, path: str):
    _principal(request)
    if valid_asset_path(path):
        for _, plugin in request.app.state.extensions.plugins:
            module = plugin.frontend
            if plugin.namespace == namespace and isinstance(module, LoadedBrowserAssets) and module.revision == revision:
                asset = module.files.get(path)
                if asset is not None:
                    return Response(
                        asset.content,
                        media_type=asset.media_type,
                        headers={
                            "Cache-Control": "private, max-age=31536000, immutable",
                            "Vary": "Cookie, Authorization",
                            "X-Content-Type-Options": "nosniff",
                            # Assets can also be opened as documents (notably SVG).
                            "Content-Security-Policy": "sandbox",
                        },
                    )
    raise HTTPException(404, "Plugin asset unavailable; reload the page.", headers={"Cache-Control": "private, no-store"})


@router.post("/{namespace}/actions/{action_name}")
async def invoke_plugin_action(request: Request, namespace: str, action_name: str):
    """Invoke an installed action with the authenticated viewer and deployment settings."""
    from deerflow_extension_api.auth import resolve_principal
    from deerflow_extension_api.plugins import ActionContext

    from deerflow.extensions.plugin_tools import plugin_settings

    principal = resolve_principal(request)
    if principal is None:
        raise HTTPException(401, "Authentication required.")
    if request.headers.get("x-deerflow-plugin-viewer") not in (None, principal.user_id):
        raise HTTPException(409, "Account changed; reload this plugin view.")
    found = next(((source, plugin) for source, plugin in request.app.state.extensions.plugins if plugin.namespace == namespace), None)
    if found is None:
        raise HTTPException(404, "Plugin is not installed.")
    source, plugin = found
    action = next((item for item in plugin.backend if item.name == action_name), None)
    if action is None:
        raise HTTPException(404, "Plugin action is not installed.")
    if action.purpose == "management":
        from app.gateway.app import _resolve_extension_plugin_management_async

        if await _resolve_extension_plugin_management_async(request, namespace, "write") is not True:
            raise HTTPException(403, "This customization requires provider enablement and administrator permission.")
    try:
        settings = await asyncio.to_thread(plugin_settings, source, plugin)
    except (ValueError, OSError) as exc:
        raise HTTPException(503, "Plugin settings unavailable.") from exc
    if settings["enabled"] is not True:
        raise HTTPException(403, "Plugin disabled by administrator.")
    # Authorize after the action is resolved (so the target is a host-validated
    # declared name and an unknown action stays a 404) and before the body is
    # streamed (so a denied caller cannot consume the input budget or reach the
    # handler). No provider decision happens when authorization is disabled.
    from app.gateway.authz import authorize_plugin_action_for_request

    await authorize_plugin_action_for_request(request, namespace=namespace, action_name=action_name)
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > 256 * 1024:
            raise HTTPException(413, "Plugin action input exceeds 256 KiB.")
    try:
        payload = json.loads(body)
        if not isinstance(payload, dict):
            raise ValueError("Expected an object")
    except (ValueError, UnicodeDecodeError) as exc:
        raise HTTPException(422, "Plugin action requires a JSON object.") from exc
    try:
        async with asyncio.timeout(30):
            if plugin.api_version == 4:
                from deerflow_extension_api.storage import StorageActor

                from app.gateway.routers.spaces import _ERRORS, _actor, _http_error
                from deerflow.spaces.facade import HostStorageProvider, storage_actor_scope

                actor = await _actor(request)
                if actor.kind not in plugin.actor_kinds:
                    raise HTTPException(403, "This plugin does not support the host actor.")
                with storage_actor_scope(actor):
                    storage = resource = None
                    if plugin.storage_api_version == 1:
                        provider = getattr(request.app.state, "extension_storage", None)
                        if not isinstance(provider, HostStorageProvider):
                            raise HTTPException(501, "Host resource storage is unavailable.")
                        try:
                            storage = await provider.for_plugin(source, plugin, lambda: request.app.state.extensions).current()
                            requested_resource = request.headers.get("x-deerflow-resource")
                            if requested_resource is not None:
                                resource = await storage.get(space_id=requested_resource)
                        except _ERRORS as exc:
                            raise _http_error(exc) from None
                        except NotImplementedError as exc:
                            raise HTTPException(501, "Host resource storage is unavailable.") from exc
                    return await action.handler(MappingProxyType(payload), ActionContext(principal, MappingProxyType(settings), actor=StorageActor(actor.kind, actor.subject_id), storage=storage, resource=resource))
            return await action.handler(MappingProxyType(payload), ActionContext(principal, MappingProxyType(settings)))
    except HTTPException:
        raise
    except StorageIdentityRequired as exc:
        raise HTTPException(401, "A current host storage identity is required.") from exc
    except StorageOperationPending as exc:
        raise HTTPException(409, "Resource operation outcome is pending; inspect recovery before another mutation.") from exc
    except StorageUnavailable as exc:
        raise HTTPException(503, "Resource backing is currently unavailable.") from exc
    except StorageConflict as exc:
        raise HTTPException(409, "Resource state conflicts with this request; inspect recovery if its outcome is pending.") from exc
    except PermissionError as exc:
        raise HTTPException(403, "Current storage authority is required.") from exc
    except NotImplementedError as exc:
        raise HTTPException(501, "Required host capability is unavailable.") from exc
    except TimeoutError as exc:
        raise HTTPException(504, "Plugin action timed out.") from exc
    except ValueError as exc:
        raise HTTPException(422, "Invalid plugin action input.") from exc
    except Exception as exc:
        logger.warning("Plugin action failed: %s/%s (%s)", namespace, action_name, type(exc).__name__)
        raise HTTPException(502, "Plugin action failed.") from exc
