"""Stable owner/thread fence shared by creation and native host file effects."""

import asyncio
import os
import stat
from contextlib import asynccontextmanager

from deerflow.spaces.filesystem import FilesystemUnavailable
from deerflow.utils.file_io import await_drained, run_file_io


def conversation_lock_path(paths, thread_id, *, user_id):
    root = paths.operational_thread_dir(thread_id, user_id=user_id)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    return root / "conversation.lock"


@asynccontextmanager
async def conversation_gate(paths, thread_id, *, user_id):
    descriptor = None

    def open_owned():
        nonlocal descriptor
        path = conversation_lock_path(paths, thread_id, user_id=user_id)
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or metadata.st_uid != os.getuid() or metadata.st_mode & 0o022:
            raise FilesystemUnavailable("The conversation fence is not a private host-owned file")

    def try_lock():
        import fcntl

        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            return False

    def close_owned():
        nonlocal descriptor
        if descriptor is not None:
            owned, descriptor = descriptor, None
            os.close(owned)  # Closing releases only this open description's lock.

    try:
        await await_drained(run_file_io(open_owned))
        while not await await_drained(run_file_io(try_lock)):
            await asyncio.sleep(0.02)
        yield
    finally:
        await await_drained(run_file_io(close_owned))
