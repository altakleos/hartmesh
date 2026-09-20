"""The company's Shared area: what anyone at the tenant published, readable by everyone.

One directory for the tenant, ``{base_dir}/shared`` (``Paths.shared_dir``),
mounted at ``/mnt/user-data/shared`` in every sandbox of every person and
written only through here, by the Gateway's publish route -- read-only to
the sandbox wherever the provider can enforce it (see ``Paths.shared_dir``).
Publishing copies the exact bytes of something the person already has; a
name already there keeps both, nothing is ever overwritten; removing takes
the file and leaves the folder. Who published or removed what, and when, is
the publication record's business (``SharedPublicationRepository``), not the
directory's: the directory holds bytes, the record holds the story.

Publication does not go through the sandbox, but the Gateway walks the tree the same way
it walks the person's files (:mod:`deerflow.files.store`): a mount is
read-only for the container, not for a bug on the host.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from deerflow.config.paths import SHARED_VIRTUAL_PREFIX, get_paths
from deerflow.files.store import StoredFile, StoreError, copy_into, delete_under, list_under, resolve_under

__all__ = [
    "SharedFile",
    "SharedFileError",
    "list_shared_files",
    "publish_file",
    "remove_shared_file",
    "resolve_shared_file",
]

#: A path that cannot name a file in the Shared area.
SharedFileError = StoreError
#: Only the Gateway writes here; the sandbox uid (and everyone else) reads.
_FOLDER_MODE = 0o755
_FILE_MODE = 0o644


@dataclass(frozen=True)
class SharedFile:
    """One published file, addressed by its path relative to the Shared root."""

    path: str
    name: str
    size: int
    modified: float
    #: Known for a file this process just published; a listing does not
    #: read every file to say it, the publication record carries it.
    sha256: str | None = None

    @classmethod
    def of(cls, stored: StoredFile) -> SharedFile:
        return cls(path=stored.path, name=stored.name, size=stored.size, modified=stored.modified, sha256=stored.sha256)

    @property
    def virtual_path(self) -> str:
        """Where every sandbox sees this file."""
        return f"{SHARED_VIRTUAL_PREFIX}/{self.path}"

    @property
    def url(self) -> str:
        """Where the browser fetches this file (the Gateway's shared route)."""
        return f"/api/shared/{quote(self.path, safe='/')}"


def _shared_root() -> Path:
    return get_paths().shared_dir()


def list_shared_files() -> tuple[list[SharedFile], bool]:
    """Every published file, sorted by path; and whether the listing stopped."""
    entries, truncated = list_under(_shared_root())
    return [SharedFile.of(entry) for entry in entries], truncated


def resolve_shared_file(path: str) -> Path:
    """The host path *path* names in Shared, refusing any symlinked segment on the way."""
    return resolve_under(_shared_root(), path)


def publish_file(source: Path, *, name: str, folder: str | None = None) -> SharedFile:
    """Copy *source*'s exact bytes into Shared as *name* under *folder*; never overwrite.

    Returns the file with the digest of what was written, for the record.
    """
    root = get_paths().ensure_shared_dir()
    return SharedFile.of(copy_into(root, source, name=name, folder=folder, folder_mode=_FOLDER_MODE, file_mode=_FILE_MODE))


def remove_shared_file(path: str) -> None:
    """Remove one published file. Folders stay; a missing file raises ``FileNotFoundError``."""
    delete_under(_shared_root(), path)
