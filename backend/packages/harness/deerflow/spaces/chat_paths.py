"""Pure host-projected conversation paths and Docker bind facts.

Neither a projection nor a stored root is an authorization capability. The host
must refresh current authority before each effect and each retained-client use.
"""

import errno
import os
from dataclasses import dataclass
from types import MappingProxyType

from deerflow.agent_instances.contract import Refusal
from deerflow.config.paths import Paths, _join_host_path, _validate_thread_id, _validate_user_id
from deerflow.spaces.backings import VerifiedVolume
from deerflow.spaces.provision import SPACE_MIN_FREE_BYTES, SPACE_MIN_FREE_INODES


class ChatFilesFull(Refusal):
    status = 507
    reason = "files-full"

    def __init__(self):
        super().__init__("Your files are full. Delete files or old conversations to free room.")


@dataclass(frozen=True)
class ThreadMounts:
    user_id: str
    thread_id: str
    user: VerifiedVolume | None = None
    company: VerifiedVolume | None = None

    def __post_init__(self):
        _validate_user_id(self.user_id)
        _validate_thread_id(self.thread_id)
        if self.user is None and self.company is not None:
            raise ValueError("Shared cannot be mounted without the personal conversation Space")

    @property
    def binds(self):
        if self.user is None:
            return ()
        root = self.user.data_path
        thread = root / "conversations" / self.thread_id
        binds = [(str(thread / "user-data" / leaf), "/mnt/user-data/" + leaf, False) for leaf in ("workspace", "uploads", "outputs")]
        binds.append((str(root / "files"), "/mnt/user-data/files", False))
        if self.company is not None:
            binds.append((str(self.company.data_path / "shared"), "/mnt/user-data/shared", True))
        binds.append((str(thread / "acp-workspace"), "/mnt/acp-workspace", True))
        return tuple(binds)

    @property
    def labels(self):
        return MappingProxyType({"hartmesh.storage.space." + volume.spec.slot_id: "1" for volume in (self.user, self.company) if volume is not None})

    def validate_sources(self):
        """Check every bind source below its pinned qualified volume.

        Docker resolves source names itself, so these checks are repeated just
        before creation and retained-client inspection. Every component must
        be a real directory on the verified device, including MyFiles/Shared.
        """
        from deerflow.spaces.filesystem import UnsafeSpacePath

        sources = []
        if self.user is not None:
            prefix = "conversations/" + self.thread_id + "/"
            sources.extend((self.user, prefix + "user-data/" + leaf) for leaf in ("workspace", "uploads", "outputs"))
            sources.extend(((self.user, "files"), (self.user, prefix + "acp-workspace")))
        if self.company is not None:
            sources.append((self.company, "shared"))
        for volume, relative in sources:
            with volume.filesystem() as filesystem:
                parts = relative.split("/")
                for index in range(1, len(parts) + 1):
                    descriptor = filesystem._open("/".join(parts[:index]), os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW, follow_symlinks=False)
                    try:
                        if os.fstat(descriptor).st_dev != volume.device:
                            raise UnsafeSpacePath("A chat mount crosses the qualified filesystem")
                    finally:
                        os.close(descriptor)


class ChatStoragePaths(Paths):
    resource_shelf = True

    @property
    def has_personal_files(self):
        return self._chat_user is not None

    @property
    def has_shared_files(self):
        return self._chat_company is not None

    def routing_signature(self):
        """Stable routing facts, excluding changing free-space measurements."""

        def identity(volume):
            if volume is None:
                return None
            return (volume.spec.slot_id, volume.spec.filesystem_uuid, str(volume.data_path), volume.root_inode, volume.control_inode, *(getattr(volume, name, None) for name in ("device", "mount_id", "image_device", "image_inode")))

        return self._chat_owner, (self._chat_user_reference, identity(self._chat_user)), (self._chat_company_reference, identity(self._chat_company))

    def __init__(self, base, *, user_id, user=None, company=None, user_reference=None, company_reference=None, protected_roots=()):
        _validate_user_id(user_id)
        if user is None and company is not None:
            raise ValueError("Shared requires an admitted personal Space")
        self.__dict__.update(base.__dict__)
        self._chat_owner = user_id
        self._chat_user = user
        self._chat_company = company
        self._chat_user_reference = user_reference if user is not None else None
        self._chat_company_reference = company_reference if company is not None else None
        self.protected_roots = tuple(protected_roots)
        self.thread_root_resolver = self._chat_root

    def _owner(self, user_id):
        if user_id is None:
            raise PermissionError("Chat storage requires its admitted owner")
        _validate_user_id(user_id)
        if user_id != self._chat_owner:
            raise PermissionError("Chat paths cannot select another owner")

    def _chat_root(self, user_id):
        self._owner(user_id)
        if self._chat_user is None:
            raise PermissionError("This conversation has no admitted personal files")
        return self._chat_user.data_path

    def _thread_location(self, thread_id, user_id):
        _validate_thread_id(thread_id)
        self._chat_root(user_id)
        return super()._thread_location(thread_id, user_id)

    def host_thread_dir(self, thread_id, *, user_id=None):
        _validate_thread_id(thread_id)
        self._chat_root(user_id)
        return super().host_thread_dir(thread_id, user_id=user_id)

    @staticmethod
    def _thread_capacity(filesystem, root_descriptor, thread_id):
        try:
            existing = filesystem._open("conversations/" + thread_id, os.O_PATH | os.O_DIRECTORY)
        except FileNotFoundError:
            free = os.fstatvfs(root_descriptor)
            if free.f_bavail * free.f_frsize < SPACE_MIN_FREE_BYTES or free.f_favail < SPACE_MIN_FREE_INODES:
                raise ChatFilesFull()
        else:
            os.close(existing)

    def check_thread_capacity(self, thread_id, *, user_id=None):
        self._owner(user_id)
        _validate_thread_id(thread_id)
        if self._chat_user is None:
            return
        with self._chat_user.filesystem() as filesystem:
            root_descriptor = filesystem._open("", os.O_RDONLY | os.O_DIRECTORY, allow_root=True)
            try:
                self._thread_capacity(filesystem, root_descriptor, thread_id)
            finally:
                os.close(root_descriptor)

    def check_file_capacity(self, expected_bytes, *, user_id=None):
        self._owner(user_id)
        self._chat_root(user_id)
        if type(expected_bytes) is not int or expected_bytes < 0:
            raise ValueError("File capacity requires a nonnegative byte count")
        with self._chat_user.filesystem() as filesystem:
            descriptor = filesystem._open("", os.O_RDONLY | os.O_DIRECTORY, allow_root=True)
            try:
                free = os.fstatvfs(descriptor)
                if free.f_bavail * free.f_frsize < expected_bytes + (1 << 20) or free.f_favail < 2:
                    raise ChatFilesFull()
            finally:
                os.close(descriptor)

    def ensure_thread_dirs(self, thread_id, *, user_id=None):
        _validate_thread_id(thread_id)
        self._chat_root(user_id)
        with self._chat_user.filesystem() as filesystem:
            root_descriptor = filesystem._open("", os.O_RDONLY | os.O_DIRECTORY, allow_root=True)
            try:
                self._thread_capacity(filesystem, root_descriptor, thread_id)
                self._ensure_native_thread_dirs(filesystem, thread_id)
            except OSError as exc:
                if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                    raise ChatFilesFull() from None
                raise
            finally:
                os.close(root_descriptor)

    @staticmethod
    def _ensure_native_thread_dirs(filesystem, thread_id):
        """Qualify every component before creating children or changing modes."""

        def directory(relative, *, thread_node=True):
            flags = os.O_RDONLY | os.O_DIRECTORY
            try:
                descriptor = filesystem._open(relative, flags, follow_symlinks=False)
            except FileNotFoundError:
                parent, _, name = relative.rpartition("/")
                parent_descriptor = filesystem._open(parent, flags, allow_root=not parent, follow_symlinks=False)
                try:
                    try:
                        os.mkdir(name, mode=0o770 if thread_node else 0o777, dir_fd=parent_descriptor)
                    except FileExistsError:
                        pass
                finally:
                    os.close(parent_descriptor)
                descriptor = filesystem._open(relative, flags, follow_symlinks=False)
            try:
                if thread_node:
                    os.fchmod(descriptor, 0o770)
            finally:
                os.close(descriptor)

        directory("conversations", thread_node=False)
        thread = "conversations/" + thread_id
        directory(thread)
        directory(thread + "/user-data")
        for leaf in ("workspace", "uploads", "outputs"):
            directory(thread + "/user-data/" + leaf)
        directory(thread + "/acp-workspace")

    def user_files_dir(self, user_id):
        return self._chat_root(user_id) / "files"

    def ensure_user_files_dir(self, user_id):
        return self.user_files_dir(user_id)

    def host_user_files_dir(self, user_id):
        return str(self.user_files_dir(user_id))

    def user_projects_dir(self, user_id):
        return self._chat_root(user_id) / "projects"

    def shared_dir(self):
        if self._chat_company is None:
            raise PermissionError("This conversation has no admitted Shared files")
        return self._chat_company.data_path / "shared"

    def ensure_shared_dir(self):
        return self.shared_dir()

    def host_shared_dir(self):
        return str(self.shared_dir())

    def operational_thread_dir(self, thread_id, *, user_id=None):
        self._owner(user_id)
        _validate_thread_id(thread_id)
        return self.base_dir / "chat-runtime" / user_id / thread_id

    def thread_skills_view_dir(self, thread_id, *, user_id=None):
        return self.operational_thread_dir(thread_id, user_id=user_id) / "skills_view"

    def host_operational_thread_dir(self, thread_id, *, user_id=None):
        self._owner(user_id)
        _validate_thread_id(thread_id)
        return _join_host_path(self._host_base_dir_str(), "chat-runtime", user_id, thread_id)

    def host_thread_skills_view_dir(self, thread_id, *, user_id=None):
        return _join_host_path(self.host_operational_thread_dir(thread_id, user_id=user_id), "skills_view")

    def mounts(self, thread_id, *, user_id=None):
        self._owner(user_id)
        return ThreadMounts(user_id, thread_id, self._chat_user, self._chat_company)

    def thread_data(self, thread_id, *, user_id=None):
        self._owner(user_id)
        _validate_thread_id(thread_id)
        if self._chat_user is None:
            return {}
        result = {leaf + "_path": str(getattr(self, "sandbox_" + ("work" if leaf == "workspace" else leaf) + "_dir")(thread_id, user_id=user_id)) for leaf in ("workspace", "uploads", "outputs")}
        result["files_path"] = str(self.user_files_dir(user_id))
        if self._chat_company is not None:
            result["shared_path"] = str(self.shared_dir())
        return result
