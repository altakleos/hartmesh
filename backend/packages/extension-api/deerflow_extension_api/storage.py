"""Transport-neutral resource capabilities; identity values confer no authority.

Only the host binds a ResourceStorage handle to the current authenticated actor.
Caller payloads, filesystem paths and user/thread aliases cannot select it.
"""

import re
from dataclasses import dataclass
from typing import Literal, Protocol


class StorageAccessDenied(PermissionError):
    """Current mandatory resource grants do not admit this operation."""


class StorageIdentityRequired(StorageAccessDenied):
    """The host has no current validated identity for this call."""


class StorageConflict(RuntimeError):
    """Captured generation/revision or resource state conflicts with the request."""


class StorageOperationPending(StorageConflict):
    """A durable outcome is uncertain; inspect recovery instead of replaying it."""


class StorageUnavailable(RuntimeError):
    """The backing cannot currently establish the promised storage facts."""


class StorageUnsupported(NotImplementedError):
    """This host/controller does not implement the requested capability."""


@dataclass(frozen=True)
class StorageActor:
    kind: Literal["human", "nonhuman"]
    subject_id: str

    def __post_init__(self):
        if self.kind not in ("human", "nonhuman") or not isinstance(self.subject_id, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,127}", self.subject_id):
            raise ValueError("Invalid typed storage actor")


@dataclass(frozen=True)
class StorageCapabilities:
    api_version: int = 1
    available: bool = False
    actor_kinds: tuple[str, ...] = ()
    files: bool = False
    provision: bool = False
    grants: bool = False
    mediated_mutations: bool = False
    native_attachments: bool = False
    quiesced_recovery: bool = False

    def __post_init__(self):
        object.__setattr__(self, "actor_kinds", tuple(self.actor_kinds))
        if type(self.api_version) is not int or self.api_version != 1 or any(kind not in ("human", "nonhuman") for kind in self.actor_kinds) or len(set(self.actor_kinds)) != len(self.actor_kinds):
            raise ValueError("Unsupported storage capability contract")
        if any(type(getattr(self, field)) is not bool for field in ("available", "files", "provision", "grants", "mediated_mutations", "native_attachments", "quiesced_recovery")):
            raise ValueError("Storage capabilities require explicit boolean support")


@dataclass(frozen=True)
class StorageResource:
    id: str
    name: str
    custody: Literal["personal", "company"]
    custodian: StorageActor | None
    mode: Literal["native", "mediated"]
    generation: int
    status: Literal["active", "archived", "deleted"]
    permissions: int


@dataclass(frozen=True)
class ResourceReference:
    space_id: str
    path: str = ""
    revision: str | None = None

    def __post_init__(self):
        if not isinstance(self.space_id, str) or not re.fullmatch(r"[0-9a-f]{32}", self.space_id):
            raise ValueError("Invalid stable resource reference")
        if not isinstance(self.path, str) or self.path.startswith("/") or "\x00" in self.path or (self.path != "" and any(part in ("", ".", "..") for part in self.path.split("/"))):
            raise ValueError("References require a resource-relative path")
        if self.revision is not None and (not isinstance(self.revision, str) or not re.fullmatch(r"[0-9a-f]{64}", self.revision)):
            raise ValueError("References use an optional captured SHA-256")


@dataclass(frozen=True)
class StorageController:
    name: str
    metadata_version: int = 1

    def __post_init__(self):
        if not isinstance(self.name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,127}", self.name) or type(self.metadata_version) is not int or not 1 <= self.metadata_version <= 2**31 - 1:
            raise ValueError("Invalid installed storage controller declaration")


class ResourceStorage(Protocol):
    actor: StorageActor
    capabilities: StorageCapabilities

    async def list(self, *, limit: int = 100, offset: int = 0) -> list[StorageResource]:
        raise StorageUnsupported("Resource discovery is unsupported")

    async def get(self, *, space_id: str) -> StorageResource:
        raise StorageUnsupported("Resource access is unsupported")

    async def provision(self, *, name: str, custody: Literal["personal", "company"], mode: Literal["native", "mediated"] = "native") -> StorageResource:
        raise StorageUnsupported("Resource provisioning is unsupported")

    async def read(self, *, space_id: str, path: str, max_bytes: int) -> bytes:
        raise StorageUnsupported("Resource reads are unsupported")

    async def export_file(self, *, space_id: str, path: str, max_bytes: int) -> bytes:
        raise StorageUnsupported("Resource export is unsupported")

    async def quota(self, *, space_id: str) -> dict:
        raise StorageUnsupported("Resource limits are unsupported")

    async def list_directory(self, *, space_id: str, path: str = "", limit: int = 200):
        raise StorageUnsupported("Resource directory listing is unsupported")

    async def write(self, *, space_id: str, generation: int, operation_id: str, path: str, content: bytes, expected_sha256: str | None = None, create: bool = False) -> str:
        raise StorageUnsupported("Resource writes are unsupported")

    async def mkdir(self, *, space_id: str, generation: int, operation_id: str, path: str) -> None:
        raise StorageUnsupported("Resource directory creation is unsupported")

    async def rename(self, *, space_id: str, generation: int, operation_id: str, path: str, destination: str) -> None:
        raise StorageUnsupported("Resource rename is unsupported")

    async def remove(self, *, space_id: str, generation: int, operation_id: str, path: str) -> None:
        raise StorageUnsupported("Resource removal is unsupported")

    async def grant(self, *, space_id: str, generation: int, subject: StorageActor, permissions: int, acknowledge_existing_data: bool = False) -> StorageResource:
        raise StorageUnsupported("Resource grant changes are unsupported")

    async def copy(self, *, source: ResourceReference, destination: ResourceReference, source_generation: int, destination_generation: int, operation_id: str, acknowledge_disclosure: bool = False) -> str:
        raise StorageUnsupported("Resource transfer is unsupported")

    async def backup(self, *, space_id: str, generation: int, operation_id: str):
        raise StorageUnsupported("Quiesced resource backup is unsupported")

    async def restore(self, *, space_id: str, generation: int, operation_id: str, backup_id: str) -> StorageResource:
        raise StorageUnsupported("Resource restore is unsupported")

    async def archive(self, *, space_id: str, generation: int, operation_id: str) -> StorageResource:
        raise StorageUnsupported("Resource archive is unsupported")

    async def delete(self, *, space_id: str, generation: int, operation_id: str) -> StorageResource:
        raise StorageUnsupported("Resource deletion is unsupported")

    async def recovery_status(self, *, space_id: str) -> dict:
        raise StorageUnsupported("Resource recovery facts are unsupported")

    async def mediated_write(self, *, space_id: str, generation: int, operation_id: str, path: str, content: bytes, expected_sha256: str | None = None, create: bool = False) -> str:
        raise StorageUnsupported("Required bound resource controller is unavailable")

    async def mediated_remove(self, *, space_id: str, generation: int, operation_id: str, path: str) -> None:
        raise StorageUnsupported("Required bound resource controller is unavailable")

    async def mediated_mkdir(self, *, space_id: str, generation: int, operation_id: str, path: str) -> None:
        raise StorageUnsupported("Required bound resource controller is unavailable")

    async def mediated_rename(self, *, space_id: str, generation: int, operation_id: str, path: str, destination: str) -> None:
        raise StorageUnsupported("Required bound resource controller is unavailable")

    async def mediated_copy(self, *, source: ResourceReference, destination: ResourceReference, source_generation: int, destination_generation: int, operation_id: str, acknowledge_disclosure: bool = False) -> str:
        raise StorageUnsupported("Required bound resource controller is unavailable")


class StorageProvider(Protocol):
    capabilities: StorageCapabilities

    async def current(self) -> ResourceStorage:
        raise StorageUnsupported("No host resource capability is installed")
