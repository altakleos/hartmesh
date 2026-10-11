"""Fresh, descriptor-confined evidence for ordinary native-storage chats."""

import asyncio
import errno
import os

from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.chat_bridge import get_chat_storage_host
from deerflow.spaces.filesystem import UnsafeSpacePath
from deerflow.utils.file_io import await_drained
from deerflow.workspace_changes.scanner import scan_workspace_roots
from deerflow.workspace_changes.types import WorkspaceRoot, WorkspaceSnapshot


async def capture_chat_snapshot(paths, thread_id, *, user_id, limits=None, include_text=True, outputs_only=False, text_paths=None, extra_excluded_dir_names=None):
    paths.mounts(thread_id, user_id=user_id)  # Validate owner/thread even without files.
    if not paths.has_personal_files:
        return WorkspaceSnapshot()
    host = get_chat_storage_host()
    if host is None:
        raise BackingUnavailable("Native file evidence requires its owning Gateway")

    async def prepare():
        current = await host.prepare_async(user_id, thread_id)
        if current is None or current.mounts.user is None or current.paths.routing_signature()[:2] != paths.routing_signature()[:2]:
            raise BackingUnavailable("Native file evidence no longer has its admitted User Space")
        return current

    admitted = await prepare()
    volume = admitted.mounts.user
    roots = [WorkspaceRoot("outputs", admitted.paths.sandbox_outputs_dir(thread_id, user_id=user_id), "/mnt/user-data/outputs")]
    if not outputs_only:
        roots.insert(0, WorkspaceRoot("workspace", admitted.paths.sandbox_work_dir(thread_id, user_id=user_id), "/mnt/user-data/workspace"))

    def scan():
        with volume.filesystem() as filesystem:

            def relative_path(root, child="."):
                base = root.host_path.relative_to(volume.data_path).as_posix()
                return base if child == "." else base + "/" + child.removeprefix("./")

            def open_confined(relative, flags):
                try:
                    return filesystem._open(relative, flags, follow_symlinks=False)
                except UnsafeSpacePath:
                    raise OSError(errno.EXDEV, "Native evidence requires its confined mount") from None

            def open_root(root):
                return open_confined(relative_path(root), os.O_RDONLY | os.O_DIRECTORY)

            def validate_directory(root, child, walked_fd):
                admitted_fd = open_confined(relative_path(root, child), os.O_PATH | os.O_DIRECTORY)
                try:
                    admitted, walked = os.fstat(admitted_fd), os.fstat(walked_fd)
                    if (admitted.st_dev, admitted.st_ino) != (walked.st_dev, walked.st_ino):
                        raise OSError(errno.ESTALE, "Native evidence directory changed during enumeration")
                finally:
                    os.close(admitted_fd)

            def open_file(root, child):
                try:
                    return filesystem.open_regular(relative_path(root, child), follow_symlinks=False)
                except UnsafeSpacePath:
                    raise OSError(errno.EXDEV, "Native evidence file crossed its confined mount") from None

            # Native text evidence is bounded in memory. It creates no legacy
            # text-cache directory containing a copy of conversation data.
            return scan_workspace_roots(
                roots,
                limits=limits,
                include_text=include_text,
                text_paths=text_paths,
                extra_excluded_dir_names=extra_excluded_dir_names,
                root_opener=open_root,
                directory_validator=validate_directory,
                file_opener=open_file,
            )

    result = await await_drained(asyncio.to_thread(scan))
    await prepare()  # Do not publish evidence admitted before a slow scan revoked.
    return result
