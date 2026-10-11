"""Private, quota-counted copies for ordinary conversation branching."""

import errno
import os
import stat
import uuid
from contextlib import asynccontextmanager

from app.gateway.chat_cleanup import purge_private_tree
from app.gateway.chat_files import ChatFileAccess
from deerflow.config.paths import _validate_thread_id
from deerflow.spaces import filesystem as filesystem_module
from deerflow.spaces.chat_gate import conversation_gate
from deerflow.spaces.chat_paths import ChatFilesFull
from deerflow.spaces.filesystem import FilesystemUnavailable
from deerflow.utils.file_io import await_drained, run_file_io


def _revision(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


class NativeBranchClone:
    mode = "current_thread_best_effort"

    def __init__(self, prepared, target_thread_id):
        _validate_thread_id(target_thread_id)
        self.access = ChatFileAccess(prepared)
        self.target = target_thread_id
        self.filesystem = None
        self.private = None
        self.root_identity = None
        self.published = False
        self.committed = False

    def _capacity(self, size=0):
        free = os.fstatvfs(self.filesystem._control_fd)
        if free.f_bavail * free.f_frsize < size + (1 << 20) or free.f_favail < 2:
            raise ChatFilesFull()

    async def stage(self, request, source_thread_id):
        self.access = await self.access.refresh(request)

        def copy():
            prepared = self.access.prepared
            prepared.paths.check_thread_capacity(self.target, user_id=prepared.mounts.user_id)
            self.filesystem = prepared.mounts.user.filesystem()
            self._capacity()
            name = "chat-branch-" + uuid.uuid4().hex
            os.mkdir(name, 0o700, dir_fd=self.filesystem._control_fd)
            self.private = name
            root = filesystem_module._openat2(self.filesystem._control_fd, name, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
            try:
                info = os.fstat(root)
                self.root_identity = info.st_dev, info.st_ino
                os.fchmod(root, 0o770)
                os.mkdir("user-data", 0o770, dir_fd=root)
                os.mkdir("acp-workspace", 0o770, dir_fd=root)
                acp = os.open("acp-workspace", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=root)
                try:
                    os.fchmod(acp, 0o770)
                finally:
                    os.close(acp)
                user_data = os.open("user-data", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=root)
                try:
                    os.fchmod(user_data, 0o770)
                    for leaf in ("workspace", "uploads", "outputs"):
                        self._capacity()
                        os.mkdir(leaf, 0o770, dir_fd=user_data)
                        target = os.open(leaf, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=user_data)
                        try:
                            os.fchmod(target, 0o770)
                            self._copy_directory(f"conversations/{source_thread_id}/user-data/{leaf}", target, missing_ok=True)
                        finally:
                            os.close(target)
                    os.fsync(user_data)
                finally:
                    os.close(user_data)
                os.fsync(root)
            finally:
                os.close(root)

        try:
            await await_drained(run_file_io(copy))
        except OSError as exc:
            if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                raise ChatFilesFull() from None
            raise
        await self.access.refresh(request)

    def _copy_directory(self, relative, target, *, missing_ok=False, expected=None):
        try:
            source = self.filesystem._open(relative, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
        except FileNotFoundError:
            if missing_ok:
                return
            raise
        try:
            before = os.fstat(source)
            os.fchmod(target, 0o770)
            if expected is not None and _revision(before) != _revision(expected):
                raise FilesystemUnavailable("Source directory changed before branching")
            with os.scandir(source) as entries:
                for entry in entries:
                    if entry.name.startswith(".upload-") and entry.name.endswith(".part"):
                        continue
                    child = relative + "/" + entry.name
                    descriptor = self.filesystem._open(child, os.O_PATH | os.O_NOFOLLOW, follow_symlinks=False)
                    try:
                        info = os.fstat(descriptor)
                        if stat.S_ISLNK(info.st_mode):
                            continue
                        self._capacity(info.st_size if stat.S_ISREG(info.st_mode) else 0)
                        if stat.S_ISDIR(info.st_mode):
                            os.mkdir(entry.name, 0o770, dir_fd=target)
                            directory = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=target)
                            try:
                                self._copy_directory(child, directory, expected=info)
                            finally:
                                os.close(directory)
                        elif stat.S_ISREG(info.st_mode):
                            self._copy_file(child, info, target, entry.name)
                    finally:
                        os.close(descriptor)
            if _revision(os.fstat(source)) != _revision(before):
                raise FilesystemUnavailable("Source directory changed during branching")
            current = self.filesystem._open(relative, os.O_PATH | os.O_DIRECTORY, follow_symlinks=False)
            try:
                if _revision(os.fstat(current)) != _revision(before):
                    raise FilesystemUnavailable("Source directory changed during branching")
            finally:
                os.close(current)
            os.fsync(target)
        finally:
            os.close(source)

    def _copy_file(self, relative, expected, target, name):
        source = self.filesystem.open_regular(relative, follow_symlinks=False)
        destination = None
        try:
            if _revision(os.fstat(source)) != _revision(expected):
                raise FilesystemUnavailable("Source file changed during branching")
            destination = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o660, dir_fd=target)
            remaining = expected.st_size
            while remaining:
                chunk = os.read(source, min(65536, remaining))
                if not chunk:
                    raise FilesystemUnavailable("Source file shortened during branching")
                self._capacity(len(chunk))
                offset = 0
                while offset < len(chunk):
                    written = os.write(destination, chunk[offset:])
                    if written <= 0:
                        raise OSError(errno.EIO, "Branch copy made no progress")
                    offset += written
                remaining -= len(chunk)
            current = self.filesystem._open(relative, os.O_PATH | os.O_NOFOLLOW, follow_symlinks=False)
            try:
                if _revision(os.fstat(source)) != _revision(expected) or _revision(os.fstat(current)) != _revision(expected):
                    raise FilesystemUnavailable("Source file changed during branching")
            finally:
                os.close(current)
            os.fchmod(destination, stat.S_IMODE(expected.st_mode) & 0o770)
            os.fsync(destination)
        finally:
            os.close(source)
            if destination is not None:
                os.close(destination)

    async def publish(self, request):
        self.access = await self.access.refresh(request)

        def publish():
            parent = self.filesystem._open("conversations", os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
            try:
                self.filesystem._rename_no_replace(self.filesystem._control_fd, self.private, parent, self.target)
                self.published = True
                os.fsync(parent)
                os.fsync(self.filesystem._control_fd)
            finally:
                os.close(parent)

        try:
            await await_drained(run_file_io(publish))
        except OSError as exc:
            if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                # Report full only after the owned public/private tree is gone.
                await self.close()
                raise ChatFilesFull() from None
            raise

    async def close(self):
        def close():
            if self.filesystem is None:
                return
            try:
                if self.published and not self.committed:
                    parent = self.filesystem._open("conversations", os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
                    try:
                        self.filesystem._rename_no_replace(parent, self.target, self.filesystem._control_fd, self.private)
                        moved = os.stat(self.private, dir_fd=self.filesystem._control_fd, follow_symlinks=False)
                        if (moved.st_dev, moved.st_ino) != self.root_identity:
                            self.filesystem._rename_no_replace(self.filesystem._control_fd, self.private, parent, self.target)
                            raise FilesystemUnavailable("Branch rollback preserved a replaced conversation directory")
                        self.published = False
                    finally:
                        os.close(parent)
                if self.private is not None and not self.published:
                    purge_private_tree(self.filesystem, self.private)
                    self.private = None
            finally:
                self.filesystem.close()
                self.filesystem = None

        await await_drained(run_file_io(close))


async def _rollback_target(request, target, owner):
    from app.gateway.deps import get_checkpointer, get_run_event_store, get_thread_store
    from app.gateway.routers.threads import _event_delete_owner_kwargs

    delete_events = get_run_event_store(request).delete_by_thread
    actions = [lambda: get_checkpointer(request).adelete_thread(target), lambda: delete_events(target, **_event_delete_owner_kwargs(delete_events, owner)), lambda: get_thread_store(request).delete(target, user_id=owner)]
    first_error = None
    for action in actions:
        try:
            await action()
        except BaseException as exc:
            if first_error is None:
                first_error = exc
    if first_error is not None:
        raise first_error


@asynccontextmanager
async def native_branch_scope(request, source, target, *, latest, ordinary):
    from app.gateway.chat_storage import prepare_chat_request

    prepared = await prepare_chat_request(request, source) if ordinary else None
    if prepared is None:
        yield None
        return
    from app.gateway.deps import get_run_manager
    from deerflow.runtime import ThreadOperationKind

    owner = prepared.mounts.user_id
    clone = None
    mutations_started = False
    mode = "skipped_historical_turn" if not latest else "no_personal_files"
    async with get_run_manager(request).reserve_thread_operation(target, kind=ThreadOperationKind.branch, user_id=owner):
        async with conversation_gate(prepared.paths, target, user_id=owner):
            try:
                if latest and prepared.mounts.user is not None:
                    async with conversation_gate(prepared.paths, source, user_id=owner):
                        clone = NativeBranchClone(prepared, target)
                        await clone.stage(request, source)
                        await clone.publish(request)
                    mode = clone.mode
                mutations_started = True
                yield mode
                if clone is not None:
                    await clone.access.refresh(request)
                    clone.committed = True
            except BaseException:
                if mutations_started:
                    await await_drained(_rollback_target(request, target, owner))
                raise
            finally:
                if clone is not None:
                    await clone.close()
