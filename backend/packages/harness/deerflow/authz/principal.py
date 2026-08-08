"""Principal builder — the single sanctioned way to construct a Principal.

Both Layer 1 (tool assembly) and Layer 2 (GuardrailAuthorizationAdapter) must
use this builder so identity semantics stay consistent. It is a pure function:
no global config reads, no caching, no input mutation.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from deerflow_extension_api import InvocationIdentityV1

from deerflow.authz.provider import Principal
from deerflow.runtime.accepted_invocation import INVOCATION_IDENTITY_CONTEXT_KEY


def normalize_authz_attributes(raw: Any) -> dict[str, Any]:
    """Validate and copy ``authz_attributes`` into a fresh dict.

    This is the single normalization point shared by the Principal builder and
    all propagation sites (middleware, executor, task_tool). Keeping it in one
    place ensures every in-process consumption boundary raises ``TypeError``
    for non-Mapping values rather than silently coercing.

    Raises:
        TypeError: If *raw* is not ``None`` and not a ``Mapping``.
    """
    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        return dict(raw)
    raise TypeError(f"authz_attributes must be a Mapping, got {type(raw).__name__}")


def build_principal_from_context(
    context: Mapping[str, Any],
    *,
    default_role: str,
) -> Principal:
    """Build a :class:`Principal` from a runtime context mapping.

    Args:
        context: The runtime context (``config["context"]`` or a dict assembled
            from a :class:`~deerflow.guardrails.provider.GuardrailRequest`).
        default_role: Role used when ``user_role`` is ``None`` or empty string.
            Unknown but non-empty roles are **not** replaced — only missing ones.

    Raises:
        TypeError: If ``authz_attributes`` is present but not a ``Mapping``.
    """
    resolved_role = context.get("user_role")
    if resolved_role is None or resolved_role == "":
        resolved_role = default_role

    identity = context.get(INVOCATION_IDENTITY_CONTEXT_KEY)
    if identity is not None and not isinstance(identity, InvocationIdentityV1):
        raise TypeError("accepted invocation identity must be InvocationIdentityV1")
    if identity is not None:
        return Principal.from_identity(
            identity,
            channel_user_id=context.get("channel_user_id"),
        )

    return Principal(
        user_id=context.get("user_id"),
        role=resolved_role,
        oauth_provider=context.get("oauth_provider"),
        oauth_id=context.get("oauth_id"),
        channel_user_id=context.get("channel_user_id"),
        is_internal=context.get("is_internal") is True,
        attributes=normalize_authz_attributes(context.get("authz_attributes")),
    )
