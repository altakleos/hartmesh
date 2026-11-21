"""Associate Shared records with observed file contents under the mutation lock."""

import os
from collections import OrderedDict
from dataclasses import dataclass
from threading import Lock

from deerflow.files.store import MAX_LISTED_FILES, StoreError, digest_open_file, open_regular_source
from deerflow.spaces.contract import SpaceConflict

# A normal refresh needs metadata only for unchanged files, even across viewers.
# The cache contains digests, never ownership or authorization decisions.
_digests = OrderedDict()
_cache_lock = Lock()


@dataclass(frozen=True)
class SharedFileProof:
    device: int
    inode: int
    size: int
    digest: str

    def matches(self, record):
        return record is not None and self.size == record["size"] and self.digest == record["sha256"]


def _revision(metadata):
    return metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns


def _proof(fd, *, use_cache):
    metadata = os.fstat(fd)
    revision = _revision(metadata)
    digest = None
    if use_cache:
        with _cache_lock:
            digest = _digests.get(revision)
            if digest is not None:
                _digests.move_to_end(revision)
    if digest is None:
        digest, metadata = digest_open_file(fd)
        if use_cache:
            revision = _revision(metadata)
            with _cache_lock:
                _digests[revision] = digest
                _digests.move_to_end(revision)
                if len(_digests) > MAX_LISTED_FILES:
                    _digests.popitem(last=False)
    return SharedFileProof(metadata.st_dev, metadata.st_ino, metadata.st_size, digest)


def observed_file(path, *, use_cache=False):
    fd = open_regular_source(path)
    try:
        return _proof(fd, use_cache=use_cache)
    finally:
        os.close(fd)


def visible_records(state, entries, records):
    visible = {}
    for entry in entries:
        record = records.get(entry.path)
        if record is None or record["size"] != entry.size:
            continue
        try:
            proof = observed_file(state.root / entry.path, use_cache=True)
        except (OSError, StoreError):
            # A file that vanished or changed during a listing has no proven owner.
            continue
        if proof.matches(record):
            visible[entry.path] = record
    return visible


def _stage_matches(pending, observed):
    fd = pending.open_payload()
    try:
        return _proof(fd, use_cache=False) == observed
    finally:
        os.close(fd)


class SharedPublicationChanged(SpaceConflict):
    """The selected entry changed and its staged replacement was restored."""


def stage_observed(state, path, publication_id, observed):
    from deerflow.spaces.workflows import _effect

    # A successful restore cancels only this stage's effect. Preserve earlier
    # workflow effects and conservatively propagate every uncertain outcome.
    parent = _effect.get()
    staging = [False]
    token = _effect.set(staging)
    try:
        pending = state.stage(path, publication_id)
        try:
            if _stage_matches(pending, observed):
                return pending
            pending.restore()
            staging[0] = False
            raise SharedPublicationChanged("This Shared publication changed; refresh before removing it")
        except BaseException:
            pending.close()
            raise
    finally:
        _effect.reset(token)
        if parent is not None and staging[0]:
            parent[0] = True
