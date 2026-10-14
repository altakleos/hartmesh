"""Private immutable teardown correlation; these facts grant no authority."""

import re
from dataclasses import asdict, dataclass
from datetime import datetime

from deerflow.spaces.docker import StorageHostUnqualified

_FIELDS = frozenset({"schema_version", "record_kind", "id", "daemon_id", "owner_id", "thread_id", "sandbox_id", "slots", "target_slot", "container_id", "name", "created_at", "network_mode", "policy_digest", "resources"})
_RESOURCE_FIELDS = frozenset({"kind", "role", "identity", "name", "created_at"})
_ROLES = {"network-proxy": "container", "network": "network", "egress-network": "network"}


def _text(value, limit=256):
    return isinstance(value, str) and 0 < len(value) <= limit and not any(ord(char) < 32 for char in value)


def _hex(value, size):
    return isinstance(value, str) and re.fullmatch("[0-9a-f]{" + str(size) + "}", value) is not None


def _name(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}", value) is not None


def _timestamp(value):
    if not _text(value, 128) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})", value):
        return False
    try:
        return datetime.fromisoformat(value).tzinfo is not None
    except ValueError:
        return False


@dataclass(frozen=True)
class NativeTeardownResource:
    kind: str
    role: str
    identity: str
    name: str
    created_at: str


@dataclass(frozen=True)
class NativeTeardownRecord:
    id: str
    daemon_id: str
    owner_id: str
    thread_id: str
    sandbox_id: str
    slots: tuple[str, ...]
    target_slot: str | None
    container_id: str
    name: str
    created_at: str
    network_mode: str
    policy_digest: str | None
    resources: tuple[NativeTeardownResource, ...]

    def document(self):
        return {"schema_version": 2, "record_kind": "teardown", **asdict(self), "slots": list(self.slots), "resources": [asdict(resource) for resource in self.resources]}

    @classmethod
    def parse(cls, document, identifier):
        if not isinstance(document, dict) or set(document) != _FIELDS or type(document["schema_version"]) is not int or document["schema_version"] != 2 or document["record_kind"] != "teardown":
            raise StorageHostUnqualified("Native teardown evidence schema is unknown")
        slots, raw = document["slots"], document["resources"]
        if (
            document["id"] != identifier
            or not _hex(identifier, 32)
            or not _text(document["daemon_id"], 128)
            or any(not _text(document[key]) for key in ("owner_id", "thread_id", "sandbox_id"))
            or not isinstance(slots, list)
            or not 1 <= len(slots) <= 128
            or any(not _hex(slot, 32) for slot in slots)
            or slots != sorted(set(slots))
            or (document["target_slot"] is not None and document["target_slot"] not in slots)
            or not _hex(document["container_id"], 64)
            or not _name(document["name"])
            or not _timestamp(document["created_at"])
            or document["network_mode"] not in ("open", "blocked", "allowlist")
            or (document["policy_digest"] is not None and not _hex(document["policy_digest"], 64))
            or not isinstance(raw, list)
            or len(raw) > 3
        ):
            raise StorageHostUnqualified("Native teardown correlation is malformed")
        resources = []
        for entry in raw:
            if (
                not isinstance(entry, dict)
                or set(entry) != _RESOURCE_FIELDS
                or not isinstance(entry["role"], str)
                or entry["role"] not in _ROLES
                or entry["kind"] != _ROLES[entry["role"]]
                or not _hex(entry["identity"], 64)
                or not _name(entry["name"])
                or not _timestamp(entry["created_at"])
            ):
                raise StorageHostUnqualified("Native teardown member is malformed")
            resources.append(NativeTeardownResource(**entry))
        if document["network_mode"] == "open":
            complete = not resources
        else:
            complete = len(resources) == 3 and {resource.role for resource in resources} == set(_ROLES) and _hex(document["policy_digest"], 64)
        identities = [document["container_id"], *(resource.identity for resource in resources)]
        names = [("container", document["name"]), *((resource.kind, resource.name) for resource in resources)]
        if not complete or len(set(identities)) != len(identities) or len(set(names)) != len(names):
            raise StorageHostUnqualified("Native teardown set is incomplete or ambiguous")
        return cls(**{key: tuple(value) if key == "slots" else tuple(resources) if key == "resources" else value for key, value in document.items() if key not in ("schema_version", "record_kind")})
