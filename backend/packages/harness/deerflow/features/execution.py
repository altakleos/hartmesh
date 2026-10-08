"""Run-bound first-party behavior over the same installed plugin snapshot."""

import asyncio
import uuid

from deerflow_extension_api.storage import StorageUnsupported

from deerflow.config.paths import get_paths, paths_scope
from deerflow.extensions.plugin_tools import plugin_settings
from deerflow.features.paths import FeaturePaths
from deerflow.features.plugins import FeatureServices
from deerflow.spaces.contract import Permission, SpaceDenied
from deerflow.spaces.facade import _current_actor


async def run_project_feature(loaded, callback, *, write=False, operation="context"):
    state = loaded.app_store.get(FeatureServices) if loaded else None
    service = state.services.get("hm.projects") if state else None
    if service is None or not service.healthy:
        raise StorageUnsupported("The Projects feature is unavailable")
    entry = next(((source, plugin) for source, plugin in loaded.plugins if plugin is service.contribution), None)
    if entry is None or (await asyncio.to_thread(plugin_settings, *entry))["enabled"] is not True:
        raise StorageUnsupported("The Projects feature is disabled or not installed")
    actor = _current_actor()
    files = service.resources().files
    space = await service.resource(provision=False)
    requests = {space.id: (Permission.READ | (Permission.WRITE if write else Permission(0)), space.generation)}

    async def guard(_session, _rows):
        if not service.healthy or (await asyncio.to_thread(plugin_settings, *entry))["enabled"] is not True:
            raise SpaceDenied("The Projects feature is unavailable")

    async def invoke(volumes):
        paths = FeaturePaths(get_paths(), actor.subject_id, {"hm.projects": volumes[space.id]})
        paths.project_document_repository = service.behavior.documents
        with paths_scope(paths):
            return await callback()

    if write:
        return await files.run_workflow(
            actor=actor, requests=requests, destination_id=space.id, operation_id=uuid.uuid4().hex, request={"action": "feature-workflow", "namespace": "hm.projects", "operation": operation}, controller_admission=guard, call=invoke
        )
    return await files.run_read_workflow(actor=actor, requests=requests, controller_admission=guard, call=invoke)
