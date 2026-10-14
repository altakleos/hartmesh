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


class CompanyProvisioningDenied(AgentDenied):
    """A current human lacks host authority to provision company AI employees."""


class AgentConflict(RuntimeError):
    """Stale instance generation or reused creation identity with another intent."""


class Refusal(Exception):
    """A host refusal with a stable code, status and fallback sentence."""

    status = 409
    reason = ""

    def __init__(self, detail: str, *, status: int | None = None, reason: str | None = None, **extra):
        super().__init__(detail)
        self.detail = detail
        if status is not None:
            self.status = status
        if reason is not None:
            self.reason = reason
        self.extra = extra


class ExecutionUnavailable(Refusal):
    reason = "execution-unavailable"

    def __init__(self, execution_reason: str | None):
        super().__init__("AI assistants and AI employees cannot run on this deployment right now.", execution_reason=execution_reason)


class ResidentUnavailable(Refusal):
    """The AI agent's workspace cannot admit this run now."""

    template = ""

    def __init__(self, name: str, **extra):
        super().__init__(self.template.format(name=name), **extra)


class ResidentBusy(ResidentUnavailable):
    reason = "busy"
    template = "Too much is running at once, so {name} could not start. Try again in a minute."


class ResidentWorkLimit(ResidentUnavailable):
    reason = "work-limit"
    template = "As many assignments are running as this deployment runs at once. Start this one when one of them finishes."


class ResidentRunLimit(ResidentUnavailable):
    reason = "run-limit"
    template = "{name} is already working in two conversations. Try again when one of them finishes."


class ResidentRestarting(ResidentUnavailable):
    reason = "restarting"
    template = "{name}'s workspace restarts once another conversation finishes. Try again in a few minutes."


class ResidentStartingElsewhere(ResidentUnavailable):
    reason = "starting-elsewhere"
    template = "Another conversation is starting {name}. Try again in a moment."


class ResidentStartFailed(ResidentUnavailable):
    reason = "start-failed"
    template = "{name}'s workspace did not start. Try again."


class ResidentFilesFull(ResidentUnavailable):
    reason = "files-full"
    template = "{name}'s files are full, so {name} cannot start. Delete files or ask for more room."


class ResidentFilesBusy(ResidentUnavailable):
    reason = "storage-busy"
    template = "{name}'s files are being changed right now and cannot be used. Try again in a few minutes."


RESIDENT_INTERRUPTED = "ResidentInterrupted"


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
