"""Authorized compatibility dispatch into the existing first-party plugin services."""

import asyncio
import inspect
import uuid
from functools import wraps
from pathlib import Path
from typing import get_type_hints

from fastapi import HTTPException, Request
from starlette.responses import FileResponse

from app.gateway.native_file_sources import feature_input_scope, prepare_feature_source
from app.gateway.routers.spaces import _ERRORS, SpaceFileResponse, _actor, _http_error
from app.gateway.storage_spaces import STORAGE_REASONS, supports_storage_credentials
from deerflow.config.paths import USER_FILES_VIRTUAL_PREFIX, get_paths, paths_scope
from deerflow.extensions.plugin_tools import plugin_settings
from deerflow.features.paths import FeaturePaths
from deerflow.features.plugins import FeatureService, FeatureServices
from deerflow.features.resources import MissingFeatureResource, StorageFeatureLinkRow
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.contract import Permission, SpaceDenied
from deerflow.spaces.facade import storage_actor_scope
from deerflow.spaces.filesystem import SpaceFull
from deerflow.spaces.service import SpaceFiles
from deerflow.spaces.workflows import WorkflowRejected, _effect


def _unavailable(reason):
    return HTTPException(503, {"code": "storage-unavailable", "reason": reason, "message": "Your files are not available right now. Chats run without them."})


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
        hints = get_type_hints(function, include_extras=True)

        @wraps(function)
        async def dispatch(*args, **kwargs):
            bound = signature.bind(*args, **kwargs)
            request = bound.arguments.get("request")
            if not isinstance(request, Request):
                raise HTTPException(401, "A current host request is required")
            files = getattr(request.app.state, "storage_spaces", None)
            if not isinstance(files, SpaceFiles):
                if getattr(request.app.state, "storage_spaces_enabled", False):
                    reason = getattr(getattr(request.app.state, "agent_execution", None), "storage_reason", None)
                    raise _unavailable(reason if reason in STORAGE_REASONS else "admission-failed")
                return await function(*args, **kwargs)
            reason = getattr(getattr(request.app.state, "agent_execution", None), "storage_reason", None)
            if reason is not None:
                raise _unavailable(reason if reason in STORAGE_REASONS else "admission-failed")
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
                    async with prepare_feature_source(request, actor, function.__name__, bound.arguments) as native:
                        space = await service.resource()
                        permission = Permission.READ | Permission.EXPORT if write and service.host_written else Permission.WRITE if write else Permission.READ
                        requests = {space.id: (permission, space.generation)}
                        associations = {namespace: space.id}
                        operation_id = uuid.uuid4().hex
                        if native is not None:
                            sources, source = native
                            for identifier, (source_permissions, generation) in sources.requests.items():
                                permission, expected_generation = requests.get(identifier, (Permission(0), generation))
                                if expected_generation != generation:
                                    raise SpaceDenied("The source and destination generations differ")
                                requests[identifier] = (permission | source_permissions, generation)
                        body = bound.arguments.get("body")
                        if namespace == "hm.shared" and write and str(getattr(body, "path", "")).startswith(USER_FILES_VIRTUAL_PREFIX + "/"):
                            home_service, home_entry = _installed(request, "hm.my-files")
                            if (await asyncio.to_thread(plugin_settings, *home_entry))["enabled"] is not True:
                                raise HTTPException(503, "My Files is disabled")
                            home = await home_service.resource()
                            requests[home.id] = (Permission.READ | Permission.EXPORT, home.generation)
                            associations["hm.my-files"] = home.id

                        async def controller(_session, rows):
                            if native is not None:
                                await sources.guard(_session, rows)
                            if not await enabled():
                                raise SpaceDenied("The required installed feature is disabled or unavailable")
                            if service.host_written:
                                link = await _session.get(StorageFeatureLinkRow, ("hm.company-space", "company"))
                                row, _grant = rows[space.id]
                                if (
                                    namespace != "hm.shared"
                                    or link is None
                                    or link.space_id != space.id
                                    or (row.mode, row.custody_kind, row.feature_namespace, row.feature_controller, row.feature_metadata_version) != ("native", "company", None, None, None)
                                ):
                                    raise SpaceDenied("The host publication requires the assigned company Space")

                        async def invoke(volumes):
                            paths = FeaturePaths(get_paths(), actor.subject_id, {name: volumes[identifier] for name, identifier in associations.items()})
                            with paths_scope(paths):
                                try:
                                    if native is None:
                                        result = await function(*args, **kwargs)
                                    else:

                                        async def admission(session=None):
                                            if session is None:
                                                async with files.registry.admitted(actor=actor, requests=requests, operation_id=(space.id, operation_id)) as (owned_session, rows):
                                                    if service.host_written:
                                                        await files._host_write_admission(owned_session, rows, space.id, operation_id)
                                                    await controller(owned_session, rows)
                                            else:
                                                rows = await files.registry.admit_in_session(session, actor=actor, requests=requests, operation_id=(space.id, operation_id))
                                                if service.host_written:
                                                    await files._host_write_admission(session, rows, space.id, operation_id)
                                                await controller(session, rows)

                                        async def copy_async(source, *, name, folder, user_id=None):
                                            from deerflow.files.manager import UserFile
                                            from deerflow.files.shared import SharedFile

                                            stored = await sources.copy_to(
                                                source,
                                                volumes[space.id],
                                                root_relative="shared" if service.host_written else "files",
                                                name=name,
                                                folder=folder,
                                                admission=admission,
                                                folder_mode=0o755 if service.host_written else 0o777,
                                                file_mode=0o644 if service.host_written else 0o666,
                                            )
                                            return SharedFile.of(stored) if service.host_written else UserFile.of(stored)

                                        stage_async = commit_async = None
                                        shared_owner = None
                                        if service.host_written and function.__name__ == "publish":
                                            from app.gateway.native_shared_publications import NativeSharedCopy

                                            shared_owner = NativeSharedCopy(sources, volumes[space.id], admission)
                                            copy_async = shared_owner.copy
                                        if function.__name__ == "promote_thread_file_to_shelf":
                                            from app.gateway.native_project_documents import stage_native_document
                                            from deerflow.projects.documents import add_staged_document

                                            async def stage_async(**values):
                                                return await stage_native_document(sources, volumes[space.id], admission=admission, **values)

                                            async def commit_async(**values):
                                                staged = values["staged"]
                                                try:
                                                    await staged.placement.prepare()
                                                    return await add_staged_document(service.behavior.documents, paths, user_id=actor.subject_id, **values)
                                                finally:
                                                    from deerflow.utils.file_io import await_drained, run_file_io

                                                    await await_drained(run_file_io(staged.placement.close))

                                        with feature_input_scope(
                                            sources,
                                            source,
                                            copy_async,
                                            stage_async=stage_async,
                                            commit_async=commit_async,
                                            holding_async=shared_owner.holding if shared_owner is not None else None,
                                            rollback_async=shared_owner.rollback if shared_owner is not None else None,
                                        ):
                                            try:
                                                result = await function(*args, **kwargs)
                                            finally:
                                                if shared_owner is not None:
                                                    await shared_owner.close()
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
                                operation_id=operation_id,
                                request={
                                    "action": "feature-workflow",
                                    "namespace": namespace,
                                    "operation": function.__name__,
                                    "parameters": {name: value for name, value in bound.arguments.items() if isinstance(value, str) and len(value) <= 4096},
                                    "source": {name: ("/" + getattr(body, name).lstrip("/") if name == "path" else getattr(body, name)) for name in ("path", "thread_id", "folder") if getattr(body, name, None) is not None},
                                },
                                controller_admission=controller,
                                call=invoke,
                                host_write=service.host_written,
                            )
                        return await files.run_read_workflow(actor=actor, requests=requests, controller_admission=controller, call=invoke)
                except BackingUnavailable:
                    raise _unavailable("space-unverified") from None
                except MissingFeatureResource as exc:
                    raise HTTPException(503, {"code": "storage-unavailable", "reason": "space-not-assigned", "message": str(exc)}) from None
                except SpaceFull:
                    from deerflow.spaces.chat_paths import ChatFilesFull

                    raise ChatFilesFull() from None
                except _ERRORS as exc:
                    raise _http_error(exc) from None

        return_type = hints.get("return", signature.return_annotation)
        dispatch.__signature__ = signature.replace(
            parameters=[parameter.replace(annotation=hints.get(name, parameter.annotation)) for name, parameter in signature.parameters.items()], return_annotation=None if return_type is type(None) else return_type
        )
        return dispatch

    return decorate
