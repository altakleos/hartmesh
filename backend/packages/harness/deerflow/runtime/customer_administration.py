"""Host-captured customer administration policy, independent of hot config."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import ValidationError

from deerflow.config.customer_administration_config import ApprovedLocalMcpDefinition, CustomerAdministrationConfig
from deerflow.config.extensions_config import ExtensionsConfig, McpServerConfig, normalize_mcp_transport_alias

CUSTOMER_ADMINISTRATION_CONTEXT_KEY = "__customer_administration_policy"
CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY = "__customer_management_actor"
CustomerManagementPermission = Literal["plugin_management", "local_skill_management", "local_mcp_management"]


class CustomerManagementDenied(PermissionError):
    """A customer operation lacks active delegation or provider approval."""


@dataclass(frozen=True)
class CustomerAdministrationPolicy:
    plugin_management: bool = False
    local_skill_management: bool = False
    local_mcp_management: bool = False
    approved_local_launches: frozenset[str] = frozenset()


DENIED_CUSTOMER_ADMINISTRATION = CustomerAdministrationPolicy()


@dataclass(frozen=True)
class CustomerManagementActor:
    """Server-attributed owner authority; account role alone is insufficient."""

    owner_id: str | None = None
    administrator: bool = False


def customer_management_actor_role(actor: Any) -> str:
    return "admin" if isinstance(actor, CustomerManagementActor) and actor.owner_id and actor.administrator is True else "user"


_hosting_policy: ContextVar[CustomerAdministrationPolicy | None] = ContextVar("deerflow_customer_administration_policy", default=None)
_hosting_actor: ContextVar[CustomerManagementActor | None] = ContextVar("deerflow_customer_management_actor", default=None)
_hosting_role: ContextVar[str | None] = ContextVar("deerflow_customer_management_role", default=None)


def _launch_fingerprint(definition: Mapping[str, Any], *, resolve_environment: bool = False) -> str:
    raw = dict(definition)
    if resolve_environment:
        raw = ExtensionsConfig.resolve_env_variables(raw)
    raw = dict(normalize_mcp_transport_alias(raw))
    if raw.get("transport") is not None:
        if "type" in raw and raw["type"] != raw["transport"]:
            raise ValueError("Conflicting MCP transport declarations.")
        raw["type"] = raw.pop("transport")
    raw["type"] = raw.get("type") or "stdio"
    # Unknown server extras must not silently become launch controls later.
    # Installation metadata is never a source approval.
    known = set(McpServerConfig.model_fields) | {"capability", "personal_public_network"}
    if set(raw) - known:
        raise ValueError("Unsupported local MCP definition fields.")
    launch = {key: raw[key] for key in ApprovedLocalMcpDefinition.model_fields if key in raw}
    normalized = ApprovedLocalMcpDefinition.model_validate(launch)
    serialized = json.dumps(normalized.model_dump(), sort_keys=True, ensure_ascii=True, allow_nan=False)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def effective_mcp_transport(definition: Mapping[str, Any], *, resolve_environment: bool = False) -> str:
    """Match configuration alias promotion and the runtime's empty default."""
    raw = dict(definition)
    if resolve_environment:
        raw = ExtensionsConfig.resolve_env_variables(raw)
    return normalize_mcp_transport_alias(raw).get("type") or "stdio"


def capture_customer_administration_policy(app_config: Any) -> CustomerAdministrationPolicy:
    """Capture once at host initialization; no caller/config lookup at admission."""
    configured = getattr(app_config, "customer_administration", None)
    if not isinstance(configured, CustomerAdministrationConfig):
        return DENIED_CUSTOMER_ADMINISTRATION
    definitions = getattr(app_config, "approved_local_mcp_definitions", [])
    approvals = frozenset(_launch_fingerprint(item.model_dump()) for item in definitions)
    return CustomerAdministrationPolicy(
        plugin_management=configured.plugin_management is True,
        local_skill_management=configured.local_skill_management is True,
        local_mcp_management=configured.local_mcp_management is True,
        approved_local_launches=approvals,
    )


def require_customer_management(policy: Any, permission: CustomerManagementPermission) -> None:
    if permission not in CustomerAdministrationConfig.model_fields:
        raise ValueError("Unknown customer management permission.")
    if not isinstance(policy, CustomerAdministrationPolicy) or getattr(policy, permission) is not True:
        raise CustomerManagementDenied("This customization requires provider enablement.")


def require_approved_local_mcp_definition(policy: Any, definition: Mapping[str, Any], *, resolve_environment: bool = False) -> None:
    """Check the final merged candidate inside its existing mutation lock."""
    require_customer_management(policy, "local_mcp_management")
    try:
        fingerprint = _launch_fingerprint(definition, resolve_environment=resolve_environment)
    except (ValueError, TypeError, ValidationError):
        raise CustomerManagementDenied("This local MCP launch requires provider approval.") from None
    if fingerprint not in policy.approved_local_launches:
        raise CustomerManagementDenied("This local MCP launch requires provider approval.")


def require_mcp_management_transition(policy: Any, previous: Mapping[str, Any] | None, candidate: Mapping[str, Any] | None, *, resolve_environment: bool = False) -> None:
    """Admit the locked effective change without widening remote preferences.

    Unchanged local launches need no new source admission when a remote sibling
    or display metadata changes. Removal and disabling require delegation;
    creation, launch reconfiguration and activation also require exact approval.
    """
    was_local = previous is not None and effective_mcp_transport(previous, resolve_environment=resolve_environment) == "stdio"
    is_local = candidate is not None and effective_mcp_transport(candidate, resolve_environment=resolve_environment) == "stdio"
    if not was_local and not is_local:
        return

    def effective(definition):
        if definition is None:
            return None
        raw = dict(definition)
        if resolve_environment:
            raw = ExtensionsConfig.resolve_env_variables(raw)
        try:
            return McpServerConfig.model_validate(raw)
        except (ValueError, TypeError, ValidationError):
            raise CustomerManagementDenied("Invalid MCP configuration.") from None

    previous_config = effective(previous)
    candidate_config = effective(candidate)
    # Preserve complete existing definitions, including inert legacy extras.
    # This is equality, never an approval for a new or modified extra.
    if previous_config is not None and candidate_config is not None and previous_config == candidate_config:
        return
    same_launch = was_local and is_local and previous_config.model_dump(exclude={"enabled"}) == candidate_config.model_dump(exclude={"enabled"})
    if was_local and is_local:
        try:
            same_launch = same_launch or _launch_fingerprint(previous, resolve_environment=resolve_environment) == _launch_fingerprint(candidate, resolve_environment=resolve_environment)
        except (ValueError, TypeError, ValidationError):
            pass
    was_enabled = previous_config is not None and previous_config.enabled
    is_enabled = candidate_config is not None and candidate_config.enabled
    if same_launch and was_enabled == is_enabled:
        return
    require_customer_management(policy, "local_mcp_management")
    if is_local and (not same_launch or (is_enabled and not was_enabled)):
        require_approved_local_mcp_definition(policy, candidate, resolve_environment=resolve_environment)


def resolve_customer_administration_policy(context: Any) -> CustomerAdministrationPolicy:
    if isinstance(context, Mapping):
        policy = context.get(CUSTOMER_ADMINISTRATION_CONTEXT_KEY)
        if isinstance(policy, CustomerAdministrationPolicy):
            return policy
    return DENIED_CUSTOMER_ADMINISTRATION


def resolve_customer_management_actor(context: Any) -> CustomerManagementActor:
    if isinstance(context, Mapping):
        actor = context.get(CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY)
        if isinstance(actor, CustomerManagementActor):
            return actor
    return CustomerManagementActor()


def current_customer_administration_policy() -> CustomerAdministrationPolicy:
    """Hosting/assembly scope only; absence never activates configuration."""
    return _hosting_policy.get() or DENIED_CUSTOMER_ADMINISTRATION


def bound_customer_administration_policy() -> CustomerAdministrationPolicy | None:
    """Distinguish an existing denied host from a standalone initializer."""
    return _hosting_policy.get()


def current_customer_management_actor() -> CustomerManagementActor:
    """Preserve the attributed owner across nested execution boundaries."""
    actor = _hosting_actor.get()
    if actor is not None:
        return actor
    from deerflow.runtime.user_context import get_current_user

    user = get_current_user()
    if user is None:
        return CustomerManagementActor()
    role = _hosting_role.get()
    administrator = role == "admin" if role is not None else getattr(user, "system_role", None) == "admin"
    return CustomerManagementActor(owner_id=str(user.id), administrator=administrator)


@contextmanager
def bind_customer_management_actor(actor: CustomerManagementActor | None) -> Iterator[None]:
    token = _hosting_actor.set(actor if isinstance(actor, CustomerManagementActor) else CustomerManagementActor())
    try:
        yield
    finally:
        _hosting_actor.reset(token)


def customer_management_actor_is_admin(context: Any = None) -> bool:
    role = _hosting_role.get()
    if context is not None:
        actor = resolve_customer_management_actor(context)
        return isinstance(context, Mapping) and bool(actor.owner_id) and actor.owner_id == context.get("user_id") and actor.administrator is True and (role is None or role == "admin")
    actor = current_customer_management_actor()
    return bool(actor.owner_id) and actor.administrator is True and (role is None or role == "admin")


def private_skill_management_available(app_config: Any) -> bool:
    """Assembly discovery uses the same floor, owner and supported storage."""
    if not current_customer_administration_policy().local_skill_management or not customer_management_actor_is_admin():
        return False
    from deerflow.config.paths import make_safe_user_id
    from deerflow.skills.storage import get_or_new_user_skill_storage
    from deerflow.skills.storage.user_scoped_skill_storage import UserScopedSkillStorage

    actor = current_customer_management_actor()
    try:
        storage = get_or_new_user_skill_storage(actor.owner_id, app_config=app_config)
        if not isinstance(storage, UserScopedSkillStorage) or storage.user_id != make_safe_user_id(actor.owner_id):
            return False
        storage._require_private_writable_path(storage.get_user_custom_root())
        return True
    except Exception:
        return False


@contextmanager
def bind_customer_management_actor_role(role: str | None) -> Iterator[None]:
    token = _hosting_role.set(role if role in {"admin", "user"} else "user")
    try:
        yield
    finally:
        _hosting_role.reset(token)


@contextmanager
def bind_customer_administration_policy(policy: CustomerAdministrationPolicy) -> Iterator[None]:
    if not isinstance(policy, CustomerAdministrationPolicy):
        raise TypeError("A host-captured customer policy is required.")
    token = _hosting_policy.set(policy)
    try:
        yield
    finally:
        _hosting_policy.reset(token)
