"""Own newly published empty directories; never erase another writer's data."""

import os
from uuid import uuid4

from deerflow.spaces.filesystem import FileConflict, FilesystemUnavailable, SpaceFull
from deerflow.spaces.workflows import _effect, mark_effect


class NativeDirectoryChanges:
    def __init__(self, filesystem):
        self.filesystem = filesystem
        self.created = []
        self.effect = _effect.get()
        self.previous_effect = self.effect[0] if self.effect is not None else None

    def ensure(self, parent, name, *, mode, check_capacity):
        relative = parent + "/" + name
        try:
            descriptor = self.filesystem._open(relative, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            os.close(descriptor)
            return
        check_capacity(0, minimum_inodes=3)
        directory = self.filesystem._open(parent, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
        private = "native-directory-" + uuid4().hex
        owned = None
        private_exists = False
        try:
            os.mkdir(private, 0o700, dir_fd=self.filesystem._control_fd)
            private_exists = True
            owned = os.open(private, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=self.filesystem._control_fd)
            os.fchmod(owned, mode)
            current = self.filesystem._open(parent, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
            try:
                actual, held = os.fstat(current), os.fstat(directory)
                if (actual.st_dev, actual.st_ino) != (held.st_dev, held.st_ino):
                    raise FilesystemUnavailable("The destination directory changed")
            finally:
                os.close(current)
            mark_effect()
            try:
                self.filesystem._rename_no_replace(self.filesystem._control_fd, private, directory, name)
            except SpaceFull:
                raise
            except FileConflict:
                current = self.filesystem._open(relative, os.O_RDONLY | os.O_DIRECTORY, follow_symlinks=False)
                os.close(current)
                return
            private_exists = False
            self.created.append((directory, name, owned))
            directory = owned = None
        finally:
            try:
                if private_exists:
                    os.rmdir(private, dir_fd=self.filesystem._control_fd)
            finally:
                try:
                    if owned is not None:
                        os.close(owned)
                finally:
                    if directory is not None:
                        os.close(directory)

    def _restore(self, private, parent, name):
        try:
            self.filesystem._rename_no_replace(self.filesystem._control_fd, private, parent, name)
        except BaseException:
            raise FilesystemUnavailable("Directory cleanup requires reconciliation; competing data was preserved in control") from None

    def rollback(self):
        for parent, name, owned in reversed(self.created):
            private = "native-directory-rollback-" + uuid4().hex
            try:
                self.filesystem._rename_no_replace(parent, name, self.filesystem._control_fd, private)
            except FileNotFoundError:
                raise FilesystemUnavailable("An owned directory moved; preserve data for reconciliation") from None
            actual = os.stat(private, dir_fd=self.filesystem._control_fd, follow_symlinks=False)
            expected = os.fstat(owned)
            if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
                self._restore(private, parent, name)
                raise FilesystemUnavailable("Directory replacement was preserved; cleanup requires reconciliation")
            try:
                os.rmdir(private, dir_fd=self.filesystem._control_fd)
            except OSError:
                self._restore(private, parent, name)
                raise FilesystemUnavailable("An owned directory contains new data; cleanup requires reconciliation") from None
        if self.effect is not None:
            self.effect[0] = self.previous_effect
        self.close()

    def close(self):
        created, self.created = self.created, []
        for parent, _name, owned in created:
            os.close(owned)
            os.close(parent)
