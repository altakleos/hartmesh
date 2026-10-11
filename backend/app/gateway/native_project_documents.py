"""Private native shelf staging with publication under borrowed project SQL."""

import errno
import hashlib
import os

from fastapi import HTTPException

from app.gateway.chat_files import QualifiedUploadStage
from app.gateway.native_directory_changes import NativeDirectoryChanges
from deerflow.files.store import normalize_relative_path
from deerflow.projects.documents import ShelfUploadTooLargeError, StagedDocument
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.chat_paths import ChatFilesFull
from deerflow.spaces.filesystem import SpaceFull
from deerflow.spaces.workflows import mark_effect
from deerflow.utils.file_io import await_drained, run_file_io


class NativeDocumentPlacement:
    def __init__(self, sources, volume, stage, admission):
        self.sources, self.volume, self.stage, self.admission = sources, volume, stage, admission
        self.directories = NativeDirectoryChanges(stage.filesystem)

    def verify(self):
        current_source = self.sources.files.catalog.verify(self.sources.user.spec.slot_id, previous=self.sources.user)
        self.sources.files._check_incarnation(self.sources.snapshot[1], current_source)
        current = self.sources.files.catalog.verify(self.volume.spec.slot_id, previous=self.volume)
        with current.filesystem() as filesystem:
            self._same_roots(filesystem)

    def _same_roots(self, current):
        for current_fd, held_fd in ((current._data_fd, self.stage.filesystem._data_fd), (current._control_fd, self.stage.filesystem._control_fd)):
            actual, held = os.fstat(current_fd), os.fstat(held_fd)
            if (actual.st_dev, actual.st_ino) != (held.st_dev, held.st_ino):
                raise BackingUnavailable("The shelf incarnation changed while staging")

    async def prepare(self):
        await await_drained(run_file_io(self.verify))
        await self.admission()

    async def place(self, session, *, relpath, name):
        await self.admission(session)

        def publish():
            with self.volume.filesystem() as current:
                self._same_roots(current)
            filesystem = self.stage.filesystem
            parent = "projects"
            for segment in normalize_relative_path(relpath + "/original").split("/"):
                path = parent + "/" + segment
                self.directories.ensure(parent, segment, mode=0o755, check_capacity=self.stage.check_capacity)
                parent = path
            final_parent = filesystem._open(parent, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
            os.close(self.stage.parent_fd)
            self.stage.parent_fd, self.stage.parent_relative = final_parent, parent
            mark_effect()
            self.stage.publish(name, file_mode=0o644)

        try:
            await await_drained(run_file_io(publish))
        except ChatFilesFull:
            raise SpaceFull("The shelf has no room; no document was published") from None
        except OSError as exc:
            if self.stage.published_name is None and exc.errno in (errno.ENOSPC, errno.EDQUOT):
                raise SpaceFull("The shelf has no room; no document was published") from exc
            raise

    def discard_private(self):
        if self.stage.stage_name is not None:
            os.unlink(self.stage.stage_name, dir_fd=self.stage.filesystem._control_fd)
            self.stage.stage_name = None

    def rollback(self):
        if self.stage.rollback_published():
            self.directories.rollback()

    def close(self):
        try:
            self.directories.close()
        finally:
            self.stage.close()


async def stage_native_document(sources, volume, *, admission, project_id, chunks, max_bytes):
    project = normalize_relative_path(project_id)
    if "/" in project:
        raise ValueError("Select one project identifier")
    stage = None
    iterator = chunks.__aiter__() if hasattr(chunks, "__aiter__") else None

    def create():
        nonlocal stage
        stage = QualifiedUploadStage(volume, "projects")

    def write(chunk):
        stage.check_capacity(len(chunk))
        stage.write(chunk)

    try:
        digest = hashlib.sha256()
        size = 0
        try:
            await admission()
            await await_drained(run_file_io(create))
            async for chunk in iterator:
                size += len(chunk)
                if size > max_bytes:
                    raise ShelfUploadTooLargeError(f"File exceeds the {max_bytes}-byte limit")
                digest.update(chunk)
                await await_drained(run_file_io(write, chunk))
        finally:
            if iterator is not None and hasattr(iterator, "aclose"):
                await await_drained(iterator.aclose())
        if not size:
            raise HTTPException(400, "Empty file")
        placement = NativeDocumentPlacement(sources, volume, stage, admission)
        return StagedDocument(stage.private_path, digest.hexdigest(), size, placement)
    except BaseException as exc:
        if stage is not None:
            await await_drained(run_file_io(stage.close))
        if isinstance(exc, ChatFilesFull) or isinstance(exc, OSError) and exc.errno in (errno.ENOSPC, errno.EDQUOT):
            raise SpaceFull("The shelf has no room; no document was published") from exc
        raise
