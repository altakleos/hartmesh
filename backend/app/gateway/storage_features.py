"""Authorized compatibility dispatch into the existing first-party plugin services."""

import asyncio
import inspect
import uuid
from functools import wraps
from pathlib import Path
from typing import get_type_hints

from fastapi import HTTPException, Request
from starlette.responses import FileResponse

from app.gateway.routers.spaces import _ERRORS, SpaceFileResponse, _actor, _http_error
from app.gateway.storage_spaces import supports_storage_credentials
from deerflow.config.paths import USER_FILES_VIRTUAL_PREFIX, get_paths, paths_scope
from deerflow.extensions.plugin_tools import plugin_settings
from deerflow.features.paths import FeaturePaths
from deerflow.features.plugins import FeatureService, FeatureServices
from deerflow.spaces.contract import Permission, SpaceDenied
from deerflow.spaces.facade import storage_actor_scope
from deerflow.spaces.service import SpaceFiles
from deerflow.spaces.workflows import WorkflowRejected, _effect


def _installed(request, namespace):
    extensions = getattr(request.app.state, "extensions", None)
    state = extensions.app_store.get(FeatureServices) if extensions else None
    service = state.services.get(namespace) if state else None
    if not isinstance(service, FeatureService) or not service.healthy:
        raise HTTPException(503, "The required storage feature is unavailable")
    entry = next(((source, plugin) for source, plugin in extensions.plugins if plugin is service.contribution), None)
    if entry is None:
        raise HTTPException(503, "The required storage feature is not installed")
    return service, entry


def current_feature_service(request, namespace):
    state = getattr(getattr(request, "app", None), "state", None)
    if not isinstance(getattr(state, "storage_spaces", None), SpaceFiles):
        return None
    return _installed(request, namespace)[0]


def storage_feature(namespace, *, write=False):
    """HTTP parsing/legacy permission guards run before this feature boundary."""

    def decorate(function):
        signature = inspect.signature(function)
        hints = get_type_hints(function)

        @wraps(function)
        async def dispatch(*args, **kwargs):
            bound = signature.bind(*args, **kwargs)
            request = bound.arguments.get("request")
            if not isinstance(request, Request):
                raise HTTPException(401, "A current host request is required")
            files = getattr(request.app.state, "storage_spaces", None)
            if not isinstance(files, SpaceFiles):
                if getattr(request.app.state, "storage_spaces_enabled", False):
                    raise HTTPException(503, "Qualified storage is unavailable")
                return await function(*args, **kwargs)
            if not supports_storage_credentials(request):
                raise HTTPException(403, "This credential has no storage resource scopes")
            service, entry = _installed(request, namespace)

            async def enabled():
                current_service, current_entry = _installed(request, namespace)
                return current_service is service and current_entry == entry and (await asyncio.to_thread(plugin_settings, *entry))["enabled"] is True

            if not await enabled():
                raise HTTPException(503, "The required storage feature is disabled")
            actor = await _actor(request)
            with storage_actor_scope(actor):
                try:
                    space = await service.resource()
                    permission = Permission.OPERATE if write and service.mediated else Permission.WRITE if write else Permission.READ
                    requests = {space.id: (permission, space.generation)}
                    associations = {namespace: space.id}
                    body = bound.arguments.get("body")
                    if namespace == "hm.shared" and write and str(getattr(body, "path", "")).startswith(USER_FILES_VIRTUAL_PREFIX + "/"):
                        home_service, home_entry = _installed(request, "hm.my-files")
                        if (await asyncio.to_thread(plugin_settings, *home_entry))["enabled"] is not True:
                            raise HTTPException(503, "My Files is disabled")
                        home = await home_service.resource()
                        requests[home.id] = (Permission.READ | Permission.EXPORT | Permission.ADMIN, home.generation)
                        associations["hm.my-files"] = home.id

                    async def controller(_session, rows):
                        if not await enabled():
                            raise SpaceDenied("The required installed feature is disabled or unavailable")
                        if service.mediated:
                            row, _grant = rows[space.id]
                            if (row.mode, row.feature_namespace, row.feature_controller, row.feature_metadata_version) != ("mediated", namespace, "publication", 1):
                                raise SpaceDenied("The installed publication controller is incompatible with this resource")

                    async def invoke(volumes):
                        paths = FeaturePaths(get_paths(), actor.subject_id, {name: volumes[identifier] for name, identifier in associations.items()})
                        with paths_scope(paths):
                            try:
                                result = await function(*args, **kwargs)
                            except HTTPException as exc:
                                effect = _effect.get()
                                if write and exc.status_code < 500 and effect is not None and effect[0] is False:
                                    raise WorkflowRejected(exc) from None
                                raise
                            if isinstance(result, FileResponse):
                                relative = Path(result.path).relative_to(volumes[space.id].data_path).as_posix()
                                download = bound.arguments.get("download", False) is True or result.headers.get("content-disposition", "").startswith("attachment")
                                document = getattr(result, "_project_document", None)
                                if namespace == "hm.projects" and isinstance(document, dict):
                                    from app.gateway.project_file_response import ProjectSpaceFileResponse

                                    original = Path(result._project_original).relative_to(volumes[space.id].data_path).as_posix()
                                    return ProjectSpaceFileResponse(files, actor, space.id, relative, download, original_path=original, document=document, repository=service.behavior.documents, controller=controller)
                                return SpaceFileResponse(files, actor, space.id, relative, download)
                            return result

                    if write:
                        return await files.run_workflow(
                            actor=actor,
                            requests=requests,
                            destination_id=space.id,
                            operation_id=uuid.uuid4().hex,
                            request={
                                "action": "feature-workflow",
                                "namespace": namespace,
                                "operation": function.__name__,
                                "parameters": {name: value for name, value in bound.arguments.items() if isinstance(value, str) and len(value) <= 4096},
                                "source": {name: ("/" + getattr(body, name).lstrip("/") if name == "path" else getattr(body, name)) for name in ("path", "thread_id", "folder") if getattr(body, name, None) is not None},
                            },
                            controller_admission=controller,
                            call=invoke,
                        )
                    return await files.run_read_workflow(actor=actor, requests=requests, controller_admission=controller, call=invoke)
                except _ERRORS as exc:
                    raise _http_error(exc) from None

        return_type = hints.get("return", signature.return_annotation)
        dispatch.__signature__ = signature.replace(
            parameters=[parameter.replace(annotation=hints.get(name, parameter.annotation)) for name, parameter in signature.parameters.items()], return_annotation=None if return_type is type(None) else return_type
        )
        return dispatch

    return decorate
