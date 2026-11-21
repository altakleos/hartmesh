"""Read publication evidence from the verified Company filesystem."""

import os
import re

from deerflow.files.shared import SharedFile
from deerflow.files.store import MAX_LISTED_FILES, DescriptorSource, StoreError, digest_and_stat, normalize_relative_path
from deerflow.spaces.filesystem import UnsafeSpacePath
from deerflow.uploads.manager import numbered_upload_name


def _revision(metadata):
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns


def shared_file_holding(volume, path, sha256, *, expected_identity=None, settling=False):
    relative = normalize_relative_path(path)
    name = relative.rsplit("/", 1)[-1]
    with volume.filesystem() as filesystem:
        try:
            descriptor = filesystem.open_regular("shared/" + relative, follow_symlinks=False)
        except FileNotFoundError:
            return None
        except UnsafeSpacePath:
            if settling:
                raise
            return None
        try:
            metadata = os.fstat(descriptor)
            if expected_identity is not None and metadata.st_ino != expected_identity["inode"]:
                return None
            digest, metadata = digest_and_stat(DescriptorSource(descriptor, name))
            if settling:
                current = filesystem._open("shared/" + relative, os.O_PATH | os.O_NOFOLLOW, follow_symlinks=False)
                try:
                    if _revision(os.fstat(current)) != _revision(metadata):
                        raise StoreError("Shared publication changed while checking its identity")
                finally:
                    os.close(current)
        except StoreError:
            if settling:
                raise
            return None
        finally:
            os.close(descriptor)
    if sha256 is not None and digest != sha256:
        return None
    return SharedFile(relative, name, metadata.st_size, metadata.st_mtime, digest)


def is_publication_name(requested, actual):
    folded = actual.casefold()
    if folded == requested.casefold():
        return True
    return any(numbered_upload_name(requested, int(counter)).casefold() == folded for counter in re.findall(r"_([1-9][0-9]*)", actual))


def publication_names(volume, folder, requested):
    """Enumerate only this folder, using the same sibling rule as the copy."""
    root = "shared/" + folder if folder else "shared"
    with volume.filesystem() as filesystem:
        try:
            directory = filesystem._open(root, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
        except FileNotFoundError:
            return []
        try:
            names = []
            with os.scandir(directory) as entries:
                for index, entry in enumerate(entries):
                    if index >= MAX_LISTED_FILES:
                        raise StoreError("Shared folder exceeds the bounded settlement scan")
                    if is_publication_name(requested, entry.name) and entry.is_file(follow_symlinks=False):
                        names.append(folder + "/" + entry.name if folder else entry.name)
            return sorted(names)
        finally:
            os.close(directory)
