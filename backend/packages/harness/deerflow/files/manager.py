"""The person's own files, kept across conversations.

Pure filesystem logic with no HTTP dependencies. The directory is
``{base_dir}/users/{user_id}/files`` (``Paths.user_files_dir``), mounted
read-write at ``/mnt/user-data/files`` in every sandbox of that user, so a
file kept in one conversation is on the disk of the next. The Gateway's files
router and the sandbox both write here; the person reaches it through the
Files page. Uploads and outputs stay per conversation until the person keeps
one ("Save to My files"), which copies the exact bytes.

The walking, listing, copying and removing are :mod:`deerflow.files.store`'s,
shared with the company's Shared area; what is the person's alone is here.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from deerflow.config.paths import USER_FILES_VIRTUAL_PREFIX, get_paths
from deerflow.files.store import (
    MAX_LISTED_FILES,
    MAX_PATH_DEPTH,
    StoredFile,
    StoreError,
    copy_into,
    delete_under,
    list_under,
    normalize_relative_path,
    resolve_under,
)

__all__ = [
    "MAX_LISTED_FILES",
    "MAX_PATH_DEPTH",
    "UserFile",
    "UserFileError",
    "delete_user_file",
    "keep_file",
    "list_user_files",
    "normalize_relative_path",
    "resolve_user_file",
]

#: A path that cannot name one of the person's files.
UserFileError = StoreError
#: The sandbox writes here as its own uid, so the person's folders and files
#: are open to it.
_FOLDER_MODE = 0o777
_FILE_MODE = 0o666


@dataclass(frozen=True)
class UserFile:
    """One file, addressed by its path relative to the person's files root."""

    path: str
    name: str
    size: int
    modified: float

    @classmethod
    def of(cls, stored: StoredFile) -> UserFile:
        return cls(path=stored.path, name=stored.name, size=stored.size, modified=stored.modified)

    @property
    def virtual_path(self) -> str:
        """Where the sandbox sees this file."""
        return f"{USER_FILES_VIRTUAL_PREFIX}/{self.path}"

    @property
    def url(self) -> str:
        """Where the browser fetches this file (the Gateway's files route)."""
        return f"/api/files/{quote(self.path, safe='/')}"


def _files_root(user_id: str) -> Path:
    return get_paths().user_files_dir(user_id)


def list_user_files(user_id: str) -> tuple[list[UserFile], bool]:
    """Every regular file under the person's root, sorted by path; and whether the listing stopped."""
    entries, truncated = list_under(_files_root(user_id))
    return [UserFile.of(entry) for entry in entries], truncated


def resolve_user_file(user_id: str, path: str) -> Path:
    """The host path *path* names, refusing any symlinked segment on the way.

    The file itself need not exist; the caller decides what a missing file
    means. Raises ``UserFileError`` for a path that cannot name a file here.
    """
    return resolve_under(_files_root(user_id), path)


def delete_user_file(user_id: str, path: str) -> None:
    """Remove one regular file. Folders stay; a missing file raises ``FileNotFoundError``."""
    delete_under(_files_root(user_id), path)


def keep_file(user_id: str, source: Path, *, name: str, folder: str | None = None) -> UserFile:
    """Copy *source*'s exact bytes into the person's files as *name* under *folder*.

    A name that already exists is kept beside the new one with the next free
    ``_N`` suffix; nothing is overwritten.
    """
    root = get_paths().ensure_user_files_dir(user_id)
    return UserFile.of(copy_into(root, source, name=name, folder=folder, folder_mode=_FOLDER_MODE, file_mode=_FILE_MODE))
