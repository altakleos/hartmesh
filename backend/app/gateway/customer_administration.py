"""Request admission uses the hosting runtime's captured provider policy."""

from contextlib import asynccontextmanager

from fastapi import HTTPException, Request

from deerflow.runtime.customer_administration import (
    DENIED_CUSTOMER_ADMINISTRATION,
    CustomerAdministrationPolicy,
    CustomerManagementActor,
    CustomerManagementDenied,
    CustomerManagementPermission,
    bind_customer_administration_policy,
    require_customer_management,
)


def request_customer_administration_policy(request: Request) -> CustomerAdministrationPolicy:
    scope = getattr(request, "scope", None)
    application = scope.get("app") if isinstance(scope, dict) else getattr(request, "app", None)
    state = getattr(application, "state", None)
    policy = getattr(state, "customer_administration_policy", None)
    return policy if isinstance(policy, CustomerAdministrationPolicy) else DENIED_CUSTOMER_ADMINISTRATION


def require_customer_management_for_request(request: Request, permission: CustomerManagementPermission) -> CustomerAdministrationPolicy:
    policy = request_customer_administration_policy(request)
    try:
        require_customer_management(policy, permission)
    except CustomerManagementDenied as exc:
        raise HTTPException(403, str(exc)) from None
    return policy


def require_provider_operation() -> None:
    """Customer/internal credentials do not confer deployment authority."""
    raise HTTPException(403, "This deployment operation requires the provider's operator tools.")


def plugin_management_admitted(request: Request) -> bool:
    """Independent floor and current caller authority for trusted contributions."""
    from deerflow_extension_api.auth import resolve_principal

    try:
        require_customer_management_for_request(request, "plugin_management")
    except HTTPException:
        return False
    principal = resolve_principal(request)
    return principal is not None and principal.is_admin is True and principal.is_internal is False


async def resolve_customer_management_actor(request: Request, *, internal_owner_user=None) -> CustomerManagementActor:
    """Carry request authority into workers without restoring PAT privileges."""
    from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
    from app.gateway.deps import is_admin_user

    state = getattr(request, "state", None)
    if getattr(state, "auth_source", None) == AUTH_SOURCE_INTERNAL:
        owner = internal_owner_user
        administrator = owner is not None and getattr(owner, "system_role", None) == "admin" and getattr(state, "customer_management_notification", False) is not True
    else:
        owner = getattr(state, "user", None)
        if getattr(owner, "id", None) is None:
            return CustomerManagementActor()
        administrator = await is_admin_user(request)
    owner_id = getattr(owner, "id", None)
    if owner_id is None:
        return CustomerManagementActor()
    return CustomerManagementActor(owner_id=str(owner_id), administrator=administrator)


@asynccontextmanager
async def customer_administration_host_scope(app):
    policy = getattr(app.state, "customer_administration_policy", None)
    if not isinstance(policy, CustomerAdministrationPolicy):
        policy = DENIED_CUSTOMER_ADMINISTRATION
    with bind_customer_administration_policy(policy):
        yield
