"""Evidence for a direct mkdir while the caller holds exclusive file-writer ownership."""

import errno
import os
import stat
import sys

from deerflow.spaces.filesystem import FilesystemUnavailable, SpaceFull

_STAGE = ".mkdir-stage"
_FS_IOC_GETVERSION = 0x80087601  # _IOR('v', 1, long) on the supported 64-bit Linux ABIs


def _open_directory(parent, name):
    return os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)


def _identity(directory):
    # FS_IOC_GETVERSION is optional. It identifies inode reuse on a prepared
    # ext4 volume. Live creation still has its pinned FD on other filesystems.
    import fcntl

    try:
        generation = int.from_bytes(fcntl.ioctl(directory, _FS_IOC_GETVERSION, bytes(8))[:4], sys.byteorder)
    except OSError:
        generation = None
    return {"inode": os.fstat(directory).st_ino, "inode_generation": generation}


def prepare(filesystem):
    # There is at most one empty stage per Space, even across a downgrade.
    try:
        os.mkdir(_STAGE, 0o700, dir_fd=filesystem._control_fd)
    except FileExistsError:
        pass
    except OSError as exc:
        if exc.errno in (errno.ENOSPC, errno.EDQUOT):
            raise SpaceFull("The filesystem has no room; nothing was published") from exc
        raise
    directory = _open_directory(filesystem._control_fd, _STAGE)
    try:
        st = os.fstat(directory)
        if st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o700 or os.listdir(directory):
            raise FilesystemUnavailable("Unconfirmed private directory stage")
        os.fsync(filesystem._control_fd)
        return _identity(directory)
    finally:
        os.close(directory)


def _flush(filesystem, path, parent, name, directory):
    created = os.fstat(directory)
    if not os.path.samestat(created, os.stat(name, dir_fd=parent, follow_symlinks=False)):
        raise FilesystemUnavailable("Directory changed before its creation could be confirmed")
    os.fsync(directory)
    os.fsync(parent)
    os.fsync(filesystem._control_fd)
    named_parent, _ = filesystem._parent(path)
    try:
        if not os.path.samestat(os.fstat(parent), os.fstat(named_parent)) or not os.path.samestat(created, os.stat(name, dir_fd=named_parent, follow_symlinks=False)):
            raise FilesystemUnavailable("Directory changed before its creation could be confirmed")
    finally:
        os.close(named_parent)


def publish(filesystem, path, identity):
    parent, name = filesystem._parent(path)
    try:
        directory = _open_directory(filesystem._control_fd, _STAGE)
        try:
            # The private stage and its parent have remained under the same
            # host writer lock since prepare. Restart uses the stronger proof.
            if os.fstat(directory).st_ino != identity["inode"]:
                raise FilesystemUnavailable("Private directory stage changed before publication")
            filesystem._rename_no_replace(filesystem._control_fd, _STAGE, parent, name)
            filesystem.published_action = "mkdir"
            owner = os.fstat(filesystem._data_fd)
            try:
                os.fchown(directory, owner.st_uid, owner.st_gid)
            except OSError:
                # mkdir's result describes creation of this directory.
                pass
            _flush(filesystem, path, parent, name, directory)
        finally:
            os.close(directory)
    finally:
        os.close(parent)


def valid_identity(identity):
    if not isinstance(identity, dict) or set(identity) != {"inode", "inode_generation"}:
        return False
    inode, generation = identity["inode"], identity["inode_generation"]
    return type(inode) is int and 0 < inode < 2**64 and (generation is None or type(generation) is int and 0 <= generation < 2**32)


def settle(filesystem, path, identity):
    if not valid_identity(identity):
        raise ValueError("Invalid directory creation evidence")
    generation = identity["inode_generation"]
    try:
        staged = _open_directory(filesystem._control_fd, _STAGE)
    except FileNotFoundError:
        pass
    else:
        try:
            actual = _identity(staged)
            if actual["inode"] != identity["inode"] or (generation is not None and actual["inode_generation"] != generation):
                raise FilesystemUnavailable("Private directory stage changed before settlement")
        finally:
            os.close(staged)
        # The no-replace rename never published this stage, even if a
        # different directory occupies the requested name.
        return "failed", {"resolution": "host-settled"}
    parent, name = filesystem._parent(path)
    try:
        try:
            directory = _open_directory(parent, name)
        except FileNotFoundError:
            return "failed", {"resolution": "host-settled"}
        try:
            if generation is None or _identity(directory) != identity:
                raise FilesystemUnavailable("Directory creation cannot be confirmed from its name")
            _flush(filesystem, path, parent, name, directory)
        finally:
            os.close(directory)
    finally:
        os.close(parent)
    return "complete", {"resolution": "host-settled", "value": None}
