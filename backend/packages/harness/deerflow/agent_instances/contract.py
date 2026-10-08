"""Instance authority and adopted definitions are independent of chat identity."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from enum import IntFlag

from deerflow.config.agents_config import AgentConfig
from deerflow.spaces.contract import PrincipalRef, validate_generation, validate_name


class AgentDenied(PermissionError):
    """Unknown, inaccessible or unauthorized persistent instance."""


class AgentConflict(RuntimeError):
    """Stale instance generation or reused creation identity with another intent."""


class AgentPermission(IntFlag):
    USE = 1
    INSPECT = 2
    MANAGE = 4


ALL_AGENT_PERMISSIONS = AgentPermission.USE | AgentPermission.INSPECT | AgentPermission.MANAGE


@dataclass(frozen=True)
class DefinitionSnapshot:
    owner_id: str
    config_json: str
    soul: str
    revision: str

    @classmethod
    def capture(cls, *, owner_id: str, config: dict, soul: str) -> DefinitionSnapshot:
        PrincipalRef("human", owner_id)
        parsed = AgentConfig.model_validate(config)
        if not re.fullmatch(r"[A-Za-z0-9-]{1,128}", parsed.name) or not isinstance(soul, str):
            raise ValueError("Invalid custom definition")
        # Keep the captured document exact. Adding an optional AgentConfig
        # default in a future binary must not rewrite a published revision.
        normalized = dict(config)
        config_json = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        document = json.dumps({"owner_id": owner_id, "config": normalized, "soul": soul}, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        if len(document) > 256 * 1024:
            raise ValueError("An adopted definition exceeds 256 KiB")
        return cls(owner_id, config_json, soul, hashlib.sha256(document).hexdigest())

    @property
    def config(self) -> dict:
        return json.loads(self.config_json)

    def validated(self) -> DefinitionSnapshot:
        result = self.capture(owner_id=self.owner_id, config=self.config, soul=self.soul)
        if result != self:
            raise ValueError("Adopted definition does not match its recorded revision")
        return result


@dataclass(frozen=True)
class InstanceIdentity:
    id: str
    principal: PrincipalRef
    name: str
    custody: str
    owner_id: str | None
    creator_id: str
    supervisor: PrincipalRef
    definition_revision: str
    home_id: str | None
    status: str
    generation: int

    def __post_init__(self) -> None:
        if not isinstance(self.id, str) or not re.fullmatch(r"[0-9a-f]{32}", self.id) or self.principal != PrincipalRef("nonhuman", "agent:" + self.id):
            raise ValueError("Invalid persistent agent principal")
        validate_name(self.name)
        validate_generation(self.generation)
        PrincipalRef("human", self.creator_id)
        if self.supervisor.kind != "human":
            raise ValueError("An agent supervisor must be human")
        if self.custody not in {"personal", "company"} or (self.custody == "personal") != (self.owner_id is not None):
            raise ValueError("Invalid agent custody")
        if self.owner_id is not None:
            PrincipalRef("human", self.owner_id)
        if not isinstance(self.definition_revision, str) or not re.fullmatch(r"[0-9a-f]{64}", self.definition_revision):
            raise ValueError("Invalid adopted definition revision")
        if self.home_id is not None and not re.fullmatch(r"[0-9a-f]{32}", self.home_id):
            raise ValueError("Invalid persistent home reference")
        if self.status not in {"provisioning", "active", "suspended", "archived", "deleted"} or (self.status != "provisioning" and self.home_id is None):
            raise ValueError("Invalid persisted agent lifecycle")

    @classmethod
    def from_row(cls, row) -> InstanceIdentity:
        return cls(
            row.id,
            PrincipalRef("nonhuman", row.principal_id),
            row.name,
            row.custody,
            row.owner_id,
            row.creator_id,
            PrincipalRef("human", row.supervisor_id),
            row.definition_revision,
            row.home_id,
            row.status,
            row.generation,
        )


@dataclass(frozen=True)
class AgentInstance(InstanceIdentity):
    permissions: AgentPermission

    def __post_init__(self) -> None:
        super().__post_init__()
        if not isinstance(self.permissions, AgentPermission) or not 1 <= int(self.permissions) <= 7:
            raise ValueError("Invalid agent access")
