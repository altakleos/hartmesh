"""Persisted execution scope. Only a missing scope denotes a personal batch."""

from dataclasses import dataclass


@dataclass(frozen=True)
class AgentBatchScope:
    instance_id: str
    system_role: str
    oauth_provider: str | None
    oauth_id: str | None
    token_version: int


def is_ordinary_batch_spec(spec: object) -> bool:
    return isinstance(spec, dict) and "scope" not in spec


def known_agent_batch_scope(spec: object) -> AgentBatchScope | None:
    """Recognize a session principal without turning unknown data into authority."""
    if not isinstance(spec, dict) or spec.keys() & {"user_role", "oauth_provider", "oauth_id", "is_internal", "channel_user_id", "authz_attributes"}:
        return None
    scope = spec.get("scope")
    if not isinstance(scope, dict) or type(scope.get("version")) is not int or scope["version"] != 1 or scope.get("kind") != "agent_conversation":
        return None
    instance_id = scope.get("instance_id")
    principal = scope.get("principal")
    if not isinstance(instance_id, str) or not instance_id.strip() or not isinstance(principal, dict):
        return None
    if principal.keys() != {"source", "system_role", "oauth_provider", "oauth_id", "token_version"} or principal["source"] != "session":
        return None
    role, version = principal["system_role"], principal["token_version"]
    if role not in ("user", "admin") or type(version) is not int or version < 0:
        return None
    provider, identity = principal["oauth_provider"], principal["oauth_id"]
    if any(value is not None and (not isinstance(value, str) or not value) for value in (provider, identity)):
        return None
    return AgentBatchScope(instance_id, role, provider, identity, version)
