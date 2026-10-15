"""Host-qualified read-only projections, separate from actor-admitted views."""

import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import PurePosixPath

from deerflow.spaces.filesystem import FilesystemUnavailable


def _readonly_revision(backing_handle, slot_id, filesystem_uuid, source, root_inode, control_inode, identity):
    """Stable digest of an explicitly captured projection's original facts."""
    facts = (backing_handle, slot_id, filesystem_uuid, source, root_inode, control_inode, identity)
    return hashlib.sha256(json.dumps(facts, separators=(",", ":")).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ReadOnlyRoot:
    volume: object
    relative: str
    destination: str
    identity: tuple[int, int]

    @classmethod
    def capture(cls, volume, relative, destination):
        parts, target = PurePosixPath(relative), PurePosixPath(destination)
        if not relative or parts.is_absolute() or str(parts) != relative or ".." in parts.parts or not target.is_absolute() or str(target) != destination:
            raise ValueError("Read-only projections require canonical confined directories")
        if not re.fullmatch(r"[0-9a-f]{32}", volume.spec.slot_id):
            raise ValueError("Read-only projections require qualified physical slots")
        root = cls(volume, relative, destination, (0, 0))
        return cls(volume, relative, destination, root._identity())

    @property
    def source(self):
        return str(self.volume.data_path / self.relative)

    @property
    def slot_id(self):
        return self.volume.spec.slot_id

    @property
    def label(self):
        return "hartmesh.storage.readonly." + hashlib.sha256(self.destination.encode("utf-8")).hexdigest()[:16]

    @property
    def revision(self):
        return _readonly_revision(getattr(self.volume, "backing_handle", None), self.slot_id, self.volume.spec.filesystem_uuid, self.source, self.volume.root_inode, self.volume.control_inode, self.identity)

    def _identity(self):
        with self.volume.filesystem() as filesystem:
            parts = self.relative.split("/")
            for index in range(1, len(parts) + 1):
                fd = filesystem._open("/".join(parts[:index]), os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW, follow_symlinks=False)
                try:
                    info = os.fstat(fd)
                    if info.st_dev != self.volume.device:
                        raise FilesystemUnavailable("A read-only projection crosses the qualified filesystem")
                    identity = (info.st_dev, info.st_ino)
                finally:
                    os.close(fd)
        return identity

    def validate(self):
        if self._identity() != self.identity:
            raise FilesystemUnavailable("The qualified read-only directory was replaced")
