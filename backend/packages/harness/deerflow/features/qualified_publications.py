"""Read immutable publication candidates through a qualified native volume."""

import os

from deerflow.files.shared import SharedFile
from deerflow.files.store import DescriptorSource, StoreError, digest_and_stat, normalize_relative_path
from deerflow.spaces.filesystem import UnsafeSpacePath


def shared_file_holding(volume, path, sha256, *, expected_identity=None):
    relative = normalize_relative_path(path)
    try:
        with volume.filesystem() as filesystem:
            descriptor = filesystem.open_regular("shared/" + relative, follow_symlinks=False)
            try:
                metadata = os.fstat(descriptor)
                if expected_identity is not None and {"device": metadata.st_dev, "inode": metadata.st_ino} != expected_identity:
                    return None
                digest, metadata = digest_and_stat(DescriptorSource(descriptor, relative.rsplit("/", 1)[-1]))
            finally:
                os.close(descriptor)
    except (FileNotFoundError, UnsafeSpacePath, StoreError):
        return None
    if digest != sha256:
        return None
    return SharedFile(relative, relative.rsplit("/", 1)[-1], metadata.st_size, metadata.st_mtime, sha256)
