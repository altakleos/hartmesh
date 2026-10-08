"""Immutable shelf provenance checked on the descriptors actually streamed."""

import hashlib
import os

from app.gateway.routers.spaces import SpaceFileResponse
from deerflow.spaces.contract import SpaceConflict


class ProjectSpaceFileResponse(SpaceFileResponse):
    def __init__(self, service, actor, space_id, path, download, *, original_path, document, repository, controller):
        super().__init__(service, actor, space_id, path, download)
        self._original_path = original_path
        self._expected_sha256, self._expected_size = document["sha256"], document["size_bytes"]
        self._document, self._repository, self._controller = dict(document), repository, controller

    async def _admit_source(self, session, rows):
        await self._controller(session, rows)
        current = await self._repository.get(self._document["id"], user_id=self._actor.subject_id)
        if current is None or any(current.get(key) != self._document.get(key) for key in ("project_id", "stored_relpath", "sha256", "size_bytes", "name")):
            raise SpaceConflict("content_missing")

    def _open_descriptor(self):
        original = self._filesystem.open_regular(self._original_path)
        snapshot = None
        try:
            if os.fstat(original).st_size != self._expected_size:
                raise SpaceConflict("content_missing")
            # Anonymous same-filesystem disk snapshot: no user-visible name,
            # bounded memory, and normal resource byte/inode limits apply.
            snapshot = os.open(".", os.O_TMPFILE | os.O_RDWR | os.O_CLOEXEC, 0o600, dir_fd=self._filesystem._control_fd)
            digest = hashlib.sha256()
            count = 0
            while chunk := os.read(original, 65536):
                count += len(chunk)
                if count > self._expected_size:
                    raise SpaceConflict("content_missing")
                digest.update(chunk)
                if str(self.path) == self._original_path:
                    self._write_all(snapshot, chunk)
            if count != self._expected_size or digest.hexdigest() != self._expected_sha256:
                raise SpaceConflict("content_missing")
            if str(self.path) != self._original_path:
                derived = self._filesystem.open_regular(str(self.path))
                try:
                    while chunk := os.read(derived, 65536):
                        self._write_all(snapshot, chunk)
                finally:
                    os.close(derived)
            os.lseek(snapshot, 0, os.SEEK_SET)
            owned, snapshot = snapshot, None
            return owned
        finally:
            os.close(original)
            if snapshot is not None:
                os.close(snapshot)

    @staticmethod
    def _write_all(fd, content):
        remaining = memoryview(content)
        while remaining:
            count = os.write(fd, remaining)
            if count == 0:
                raise OSError("Project read snapshot could not advance")
            remaining = remaining[count:]
