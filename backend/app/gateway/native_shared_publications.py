"""Keep native Company reads and rollback bound to the owned publication."""

import os

from deerflow.features.publications import PublicationUnavailable
from deerflow.features.qualified_publications import shared_file_holding
from deerflow.files.shared import SharedFile
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.workflows import record_fact
from deerflow.utils.file_io import await_drained, run_file_io


class NativeSharedCopy:
    def __init__(self, sources, volume, admission):
        self.sources, self.volume, self.admission = sources, volume, admission
        self.roots = volume.root_inode, volume.control_inode
        self.stage = None
        self.directories = None
        self.published = None

    def verify(self):
        current = self.sources.files.catalog.verify(self.volume.spec.slot_id, previous=self.volume)
        if (current.root_inode, current.control_inode) != self.roots:
            raise BackingUnavailable("Company publication storage changed")
        return current

    async def holding(self, path, digest):
        await self.admission()
        current = await await_drained(run_file_io(self.verify))
        held = await await_drained(run_file_io(shared_file_holding, current, path, digest))
        await self.admission()
        return held

    def retain(self, stage, directories):
        self.stage, self.directories = stage, directories

    async def copy(self, source, *, name, folder):
        await await_drained(run_file_io(self.verify))
        stored = await self.sources.copy_to(source, self.volume, root_relative="shared", name=name, folder=folder, admission=self.admission, folder_mode=0o755, file_mode=0o644, retain_stage=self.retain)
        self.published = SharedFile.of(stored)

        def remember_identity():
            metadata = os.fstat(self.stage.fd)
            record_fact("publication_identity", {"device": metadata.st_dev, "inode": metadata.st_ino})

        await await_drained(run_file_io(remember_identity))
        return self.published

    async def rollback(self, published):
        if self.stage is None or published is not self.published:
            raise PublicationUnavailable("The owned publication is unavailable; preserve bytes for recovery")
        owned_removed = await await_drained(run_file_io(self.stage.rollback_published))
        if not owned_removed:
            raise PublicationUnavailable("The public file was replaced; its bytes were preserved for recovery")
        if self.directories is not None:
            await await_drained(run_file_io(self.directories.rollback))

    async def close(self):
        try:
            if self.directories is not None:
                directories, self.directories = self.directories, None
                await await_drained(run_file_io(directories.close))
        finally:
            if self.stage is not None:
                stage, self.stage = self.stage, None
                await await_drained(run_file_io(stage.close))
