"""Narrow host cleanup of one already-authorized ordinary conversation.

This capability removes only that conversation. It neither enables unsupported
storage credentials nor installs a general User Space path/resource scope.
"""

import hashlib
import os
import stat

from fastapi import HTTPException

from app.gateway import chat_storage
from app.gateway.chat_files import files_unavailable
from deerflow.config.paths import _validate_thread_id, _validate_user_id, get_paths
from deerflow.spaces import filesystem as filesystem_module
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.chat_paths import ChatStoragePaths
from deerflow.spaces.contract import InvalidPrincipal, PrincipalRef, SpaceConflict, SpaceDenied, SpaceNotFound
from deerflow.spaces.filesystem import FilesystemUnavailable, UnsafeSpacePath
from deerflow.spaces.service import SpaceFiles
from deerflow.utils.file_io import await_drained, run_file_io


def purge_private_tree(filesystem, name):
    """Remove a host-private ticket without traversing any nested mount."""

    def remove(relative, parent, leaf):
        descriptor = filesystem_module._openat2(filesystem._control_fd, relative, os.O_PATH | os.O_NOFOLLOW, follow_symlinks=False)
        try:
            expected = os.fstat(descriptor)
            if stat.S_ISDIR(expected.st_mode):
                directory = filesystem_module._openat2(filesystem._control_fd, relative, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
                try:
                    observed = os.fstat(directory)
                    if (observed.st_dev, observed.st_ino) != (expected.st_dev, expected.st_ino):
                        raise FilesystemUnavailable("Private conversation directory changed during cleanup")
                    with os.scandir(directory) as entries:
                        for entry in entries:
                            remove(relative + "/" + entry.name, directory, entry.name)
                finally:
                    os.close(directory)
            current = os.stat(leaf, dir_fd=parent, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != (expected.st_dev, expected.st_ino):
                raise FilesystemUnavailable("Private conversation entry changed during cleanup")
            if stat.S_ISDIR(expected.st_mode):
                os.rmdir(leaf, dir_fd=parent)
            else:
                os.unlink(leaf, dir_fd=parent)
        finally:
            os.close(descriptor)

    remove(name, filesystem._control_fd, name)


class ConversationCleanup:
    def __init__(self, files, actor, thread_id, volume, identity):
        self._files, self._actor, self.thread_id = files, actor, thread_id
        self._volume, self._identity = volume, identity
        self.paths = ChatStoragePaths(get_paths(), user_id=actor.subject_id)

    async def refresh(self):
        volume, _, identity = await chat_storage._verified_role(self._files, self._actor, "hm.user-space", "human:" + self._actor.subject_id)
        if identity != self._identity:
            raise BackingUnavailable("Conversation cleanup's exact User role changed")
        if identity is not None:
            await chat_storage._admit_verified(self._files, self._actor, identity)
        return volume

    async def remove_files(self):
        volume = await self.refresh()  # Fresh role/root after slow container teardown.
        if volume is None:
            return False

        def remove():
            with volume.filesystem() as filesystem:
                parent = filesystem._open("conversations", os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
                expected = None
                try:
                    space_id = self._identity[0][0] if self._identity is not None else volume.spec.slot_id
                    private = "chat-delete-" + hashlib.sha256((space_id + "\0" + self._actor.subject_id + "\0" + self.thread_id).encode("utf-8")).hexdigest()
                    try:
                        pending = os.stat(private, dir_fd=filesystem._control_fd, follow_symlinks=False)
                    except FileNotFoundError:
                        pending = None
                    try:
                        expected = filesystem._open("conversations/" + self.thread_id, os.O_PATH | os.O_DIRECTORY, follow_symlinks=False)
                    except FileNotFoundError:
                        if pending is None:
                            return False
                        if not stat.S_ISDIR(pending.st_mode) or pending.st_dev != volume.device:
                            raise FilesystemUnavailable("Conversation cleanup ticket is not on its qualified volume")
                        purge_private_tree(filesystem, private)
                        return True
                    if pending is not None:
                        raise FilesystemUnavailable("Conversation cleanup has both a public directory and a pending private ticket")
                    identity = os.fstat(expected)
                    filesystem._rename_no_replace(parent, self.thread_id, filesystem._control_fd, private)
                    moved = os.stat(private, dir_fd=filesystem._control_fd, follow_symlinks=False)
                    if (moved.st_dev, moved.st_ino) != (identity.st_dev, identity.st_ino):
                        try:
                            filesystem._rename_no_replace(filesystem._control_fd, private, parent, self.thread_id)
                        except Exception:
                            raise FilesystemUnavailable("Conversation cleanup requires reconciliation; changed directory was preserved") from None
                        raise FilesystemUnavailable("Conversation directory changed before cleanup")
                    purge_private_tree(filesystem, private)
                    return True
                finally:
                    if expected is not None:
                        os.close(expected)
                    os.close(parent)

        return await await_drained(run_file_io(remove))


async def prepare_conversation_cleanup(request, thread_id):
    """Called only under exact thread ownership and its durable delete fence."""
    app = request.app
    if getattr(app.state, "storage_spaces_enabled", False) is not True:
        return None
    from app.gateway.deps import get_current_user_from_request
    from app.gateway.internal_auth import get_trusted_internal_owner_user_id

    current = await get_current_user_from_request(request)
    owner = get_trusted_internal_owner_user_id(request) or str(current.id)
    _validate_user_id(owner)
    _validate_thread_id(thread_id)
    actor = PrincipalRef("human", owner)
    files = getattr(app.state, "storage_spaces", None)
    if not isinstance(files, SpaceFiles):
        raise files_unavailable("admission-failed")
    try:
        await files.registry._actor(actor)
        state = await chat_storage.storage_state(app)
        if state.state == "unavailable":
            raise files_unavailable(state.reason or "admission-failed")
        volume, _, identity = await chat_storage._verified_role(files, actor, "hm.user-space", "human:" + owner)
        return ConversationCleanup(files, actor, thread_id, volume, identity)
    except InvalidPrincipal:
        raise HTTPException(401, "Conversation owner is unavailable") from None
    except (BackingUnavailable, FilesystemUnavailable, UnsafeSpacePath, SpaceConflict, SpaceDenied, SpaceNotFound, OSError, ValueError):
        raise files_unavailable("space-unverified") from None
