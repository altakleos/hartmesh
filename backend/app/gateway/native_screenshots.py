"""Owned browser bytes published through the current human User volume."""

import errno
import os

from fastapi import HTTPException

from app.gateway.chat_files import QualifiedUploadStage, files_unavailable
from app.gateway.native_file_sources import NativeFileSources
from deerflow.config.paths import _validate_thread_id, get_paths
from deerflow.constants import BROWSER_FRAMES_DIRNAME
from deerflow.spaces.chat_gate import conversation_gate
from deerflow.spaces.chat_paths import ChatFilesFull, ChatStoragePaths
from deerflow.spaces.contract import Permission, PrincipalRef, SpaceDenied
from deerflow.uploads.manager import claim_unique_filename, normalize_filename
from deerflow.utils.file_io import await_drained, run_file_io


async def publish_screenshot(app, user_id, thread_id, *, name, content, frames, expected_reference=None):
    """Run only on the trusted host loop; no SQL or browser work in file I/O."""
    _validate_thread_id(thread_id)
    safe_name = normalize_filename(name)
    if safe_name != name or safe_name.startswith("."):
        raise ValueError("Select a plain screenshot filename")
    if type(frames) is not bool or not isinstance(content, bytes):
        raise ValueError("Screenshot publication requires owned bytes and an explicit destination")
    if len(content) > 64 * 1024 * 1024:
        raise HTTPException(413, "Screenshot exceeds the 64 MiB publication limit")
    authority = getattr(app.state, "agent_conversations", None)
    if authority is not None and await authority.binding(thread_id) is not None:
        raise HTTPException(501, "Use the AI employee's Home Space for instance resources")
    actor = PrincipalRef("human", user_id)
    sources = await NativeFileSources.prepare_host(app, actor, required=Permission.READ | Permission.WRITE)
    paths = ChatStoragePaths(get_paths(), user_id=user_id, user=sources.user, user_reference=sources.snapshot[0][:2])
    if expected_reference is not None and paths.routing_signature()[:2] != expected_reference:
        raise SpaceDenied("Screenshot storage changed during capture")
    stage = None
    relative = f"conversations/{thread_id}/user-data/outputs"

    def stage_bytes():
        nonlocal stage
        stage = QualifiedUploadStage(sources.user, relative, expected_bytes=len(content))
        if frames:
            filesystem = stage.filesystem
            stage.check_capacity(len(content), minimum_inodes=3)
            try:
                child = filesystem._open(relative + "/" + BROWSER_FRAMES_DIRNAME, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
            except FileNotFoundError:
                try:
                    os.mkdir(BROWSER_FRAMES_DIRNAME, 0o755, dir_fd=stage.parent_fd)
                except FileExistsError:
                    pass
                child = filesystem._open(relative + "/" + BROWSER_FRAMES_DIRNAME, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
            os.close(stage.parent_fd)
            stage.parent_fd, stage.parent_relative = child, relative + "/" + BROWSER_FRAMES_DIRNAME
        stage.check_capacity(len(content))
        stage.write(content)

    def publish():
        # Reopen current roots before using the old pinned publication parent.
        with sources.user.filesystem() as current:
            for current_fd, held_fd in ((current._data_fd, stage.filesystem._data_fd), (current._control_fd, stage.filesystem._control_fd)):
                actual, held = os.fstat(current_fd), os.fstat(held_fd)
                if (actual.st_dev, actual.st_ino) != (held.st_dev, held.st_ino):
                    raise files_unavailable("space-unverified")
        seen = set()
        while True:
            candidate = claim_unique_filename(safe_name, seen)
            try:
                stage.publish(candidate, file_mode=0o644)
                return candidate
            except FileExistsError:
                continue

    try:
        async with conversation_gate(paths, thread_id, user_id=user_id):
            try:
                await sources.refresh()
                await await_drained(run_file_io(paths.ensure_thread_dirs, thread_id, user_id=user_id))
                await sources.refresh()
                await await_drained(run_file_io(stage_bytes))
                await sources.refresh()
                final_name = await await_drained(run_file_io(publish))
                await sources.refresh()
                return final_name
            except BaseException:
                if stage is not None and stage.published_name is not None:
                    await await_drained(run_file_io(stage.rollback_published))
                raise
    except OSError as exc:
        if exc.errno in (errno.ENOSPC, errno.EDQUOT):
            raise ChatFilesFull() from None
        raise
    finally:
        if stage is not None:
            await await_drained(run_file_io(stage.close))
