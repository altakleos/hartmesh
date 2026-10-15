"""Observed output metadata from the exact admitted Home, never container paths."""

import asyncio
import os
import stat

from deerflow_extension_api.storage import StorageAccessDenied, StorageConflict, StorageUnavailable

from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.runtime import InstanceSandboxProvider, current_environment
from deerflow.spaces.contract import Permission
from deerflow.spaces.filesystem import _openat2, _path
from deerflow.utils.file_io import await_drained, run_file_io


def _metadata(filesystem, relative, directory="outputs"):
    outputs = filesystem._open(directory, os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        descriptor = _openat2(outputs, _path(relative), os.O_PATH, follow_symlinks=directory != "uploads")
        try:
            return os.fstat(descriptor)
        finally:
            os.close(descriptor)
    finally:
        os.close(outputs)


def _observed_metadata(execution, relative, directory):
    """Run in a tool worker, outside the authority loop and file-I/O pool.

    SQL remains on its owning loop; backing verification and descriptor reads
    use the separate file-I/O pool, so callers must not occupy that pool.
    """
    environment = current_environment()
    if environment is None or environment.execution is not execution or not isinstance(environment.provider, InstanceSandboxProvider) or environment.provider.execution is not execution:
        raise ValueError(f"The exact prepared {'Space' if directory == 'uploads' else 'Home'} is unavailable")

    async def read():
        files = execution.authority.instances.files
        async with files.registry.admitted(actor=execution.instance.principal, requests={execution.instance.home_id: (Permission.READ, execution.home_generation)}) as (session, rows):
            volume = await files._volume(session, rows[execution.instance.home_id][0])
            return await run_file_io(files._io, volume, lambda fs: _metadata(fs, relative, directory))

    try:
        # Reject inspection/expired authority and blocking our owner loop.
        # No ambient provider or requester-directory fallback is possible.
        execution.validate_sync()
        return asyncio.run_coroutine_threadsafe(await_drained(read()), execution.owner_loop).result()
    except (AgentDenied, AgentConflict, StorageAccessDenied, StorageConflict, StorageUnavailable) as exc:
        raise ValueError(f"The admitted {'Space' if directory == 'uploads' else 'Home'} metadata is unavailable") from exc


def output_metadata(execution, relative):
    return _observed_metadata(execution, relative, "outputs")


def upload_metadata(execution, filename):
    """Observe a regular upload only; never open its content or a link."""
    from deerflow.spaces.filesystem import UnsafeSpacePath

    if not isinstance(filename, str) or "/" in filename or "\\" in filename:
        return None
    try:
        result = _observed_metadata(execution, _path(filename), "uploads")
    except (FileNotFoundError, UnsafeSpacePath):
        return None
    return result if stat.S_ISREG(result.st_mode) else None
