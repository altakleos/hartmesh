"""Storage authority comes from host identities and persisted grants, never paths."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import IntFlag, StrEnum
from typing import Literal

from deerflow_extension_api.storage import StorageAccessDenied, StorageConflict, StorageIdentityRequired

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:@+-]{0,127}\Z")


class InvalidPrincipal(StorageIdentityRequired):
    """The trusted identity adapter did not validate this subject."""


class SpaceDenied(StorageAccessDenied):
    """A known actor lacks mandatory resource authority."""


class SpaceNotFound(SpaceDenied):
    """Missing, inaccessible and deleted resources have the same read outcome."""


class SpaceConflict(StorageConflict):
    """An operation uses a stale generation or violates a resource invariant."""


@dataclass(frozen=True)
class PrincipalRef:
    kind: Literal["human", "nonhuman"]
    subject_id: str

    def __post_init__(self) -> None:
        if self.kind not in ("human", "nonhuman") or not isinstance(self.subject_id, str) or not _IDENTIFIER.fullmatch(self.subject_id):
            raise ValueError("Invalid storage principal reference")


@dataclass(frozen=True)
class ResolvedPrincipal:
    """A host adapter's current identity projection, not client-provided metadata."""

    reference: PrincipalRef
    can_provision_company: bool = False


@dataclass(frozen=True)
class Custody:
    kind: Literal["personal", "company"]
    principal: PrincipalRef | None = None

    def __post_init__(self) -> None:
        if self.kind not in ("personal", "company") or (self.kind == "personal" and not isinstance(self.principal, PrincipalRef)) or (self.kind == "company" and self.principal is not None):
            raise ValueError("Personal custody requires a principal; company custody has no personal custodian")

    @classmethod
    def personal(cls, principal: PrincipalRef) -> Custody:
        return cls("personal", principal)

    @classmethod
    def company(cls) -> Custody:
        return cls("company")


class MutationMode(StrEnum):
    NATIVE = "native"
    MEDIATED = "mediated"


class Permission(IntFlag):
    READ = 1
    WRITE = 2
    OPERATE = 4
    ADMIN = 8
    EXPORT = 16


@dataclass(frozen=True)
class FeatureBinding:
    """Opaque installed-controller identity. Its presence alone executes nothing."""

    namespace: str
    controller: str
    metadata_version: int

    def __post_init__(self) -> None:
        if not isinstance(self.namespace, str) or not _IDENTIFIER.fullmatch(self.namespace) or not isinstance(self.controller, str) or not _IDENTIFIER.fullmatch(self.controller):
            raise ValueError("Invalid storage feature binding")
        if type(self.metadata_version) is not int or not 1 <= self.metadata_version <= 2**31 - 1:
            raise ValueError("Invalid feature metadata version")


@dataclass(frozen=True)
class Space:
    id: str
    backing_handle: str
    name: str
    custody: Custody
    mode: MutationMode
    generation: int
    status: Literal["active", "archived", "deleted"]
    permissions: Permission
    feature: FeatureBinding | None = None

    def __post_init__(self) -> None:
        for value in (self.id, self.backing_handle):
            if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
                raise ValueError("Invalid persisted space identity or backing handle")
        validate_name(self.name)
        if not isinstance(self.custody, Custody) or not isinstance(self.mode, MutationMode) or self.status not in ("active", "archived", "deleted"):
            raise ValueError("Invalid persisted space definition")
        if type(self.generation) is not int or not 1 <= self.generation <= 2**31 - 1:
            raise ValueError("Invalid persisted space generation")
        if (self.feature is not None and not isinstance(self.feature, FeatureBinding)) or (self.mode == MutationMode.MEDIATED and self.feature is None):
            raise ValueError("Invalid persisted feature binding")
        validate_permissions(self.permissions, self.mode)


def validate_permissions(value: Permission, mode: MutationMode) -> Permission:
    if not isinstance(value, Permission) or int(value) < 0 or int(value) & ~31:
        raise ValueError("Invalid space permissions")
    if mode == MutationMode.MEDIATED and value & Permission.WRITE:
        raise ValueError("Mediated storage cannot grant native write")
    if mode == MutationMode.NATIVE and value & Permission.OPERATE:
        raise ValueError("Native storage has no mediated operation")
    if value & Permission.WRITE and not value & Permission.READ:
        raise ValueError("Native write authority requires read authority")
    return value


def validate_name(name: str) -> str:
    if not isinstance(name, str) or not name.strip() or len(name) > 128 or any(ord(c) < 32 for c in name):
        raise ValueError("A space label must contain 1–128 characters without control characters")
    return name


def validate_generation(value: int) -> int:
    if type(value) is not int or not 1 <= value <= 2**31 - 1:
        raise ValueError("A positive bounded resource generation is required")
    return value
