"""Browser tools select a host publisher, never caller-supplied native paths."""

from deerflow_extension_api.storage import StorageUnsupported

from deerflow.agent_instances.runtime import current_environment
from deerflow.config.app_config import get_app_config
from deerflow.config.paths import peek_scoped_paths
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.spaces.chat_bridge import get_chat_storage_host
from deerflow.spaces.chat_paths import ChatStoragePaths
from deerflow.utils.file_io import run_file_io


async def native_screenshot_writer(thread_id):
    if current_environment() is not None:
        raise StorageUnsupported("Browser captures are unavailable for AI employee execution; use Home Space resources")
    host = get_chat_storage_host()
    if host is None:
        scoped = peek_scoped_paths()
        config = await run_file_io(get_app_config)
        if isinstance(scoped, ChatStoragePaths) or getattr(getattr(config, "storage_spaces", None), "enabled", False) is True:
            raise StorageUnsupported("Native screenshot publication requires its owning Gateway host")
        return None
    user_id = get_effective_user_id()
    prepared = await host.prepare_async(user_id, thread_id)
    if prepared is None or prepared.mounts.user is None:
        raise StorageUnsupported("Your files are unavailable; browser screenshots cannot be saved")
    reference = prepared.paths.routing_signature()[:2]

    async def write(name, content, *, frames):
        return await host.publish_async(user_id, thread_id, name=name, content=content, frames=frames, expected_reference=reference)

    return write
