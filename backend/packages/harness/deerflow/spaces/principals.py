"""Trusted host adapters validate storage actors and membership targets alike."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Protocol

from deerflow.runtime.user_context import get_current_user
from deerflow.spaces.contract import InvalidPrincipal, PrincipalRef, ResolvedPrincipal


class PrincipalResolver(Protocol):
    async def resolve(self, reference: PrincipalRef) -> ResolvedPrincipal: ...


Lookup = Callable[[PrincipalRef], Awaitable[ResolvedPrincipal | None]]


class HostPrincipalResolver:
    """Only trusted host code supplies identity lookups; there is no public registrar.

    Lookups must recheck existence, disabled/retired status and provisioning
    authority. Nonhuman support is explicitly unavailable without its adapter.
    No identity is projected through another person's user/thread ID.
    """

    def __init__(self, *, human: Lookup, nonhuman: Lookup | None = None) -> None:
        self._human = human
        self._nonhuman = nonhuman

    async def resolve(self, reference: PrincipalRef) -> ResolvedPrincipal:
        if not isinstance(reference, PrincipalRef):
            raise InvalidPrincipal("Storage requires a host-validated principal")
        lookup = self._human if reference.kind == "human" else self._nonhuman
        if lookup is None:
            raise InvalidPrincipal("No host adapter supports this principal kind")
        result = await lookup(reference)
        if not isinstance(result, ResolvedPrincipal) or result.reference != reference or type(result.can_provision_company) is not bool:
            raise InvalidPrincipal("Unknown, retired or incorrectly resolved storage principal")
        return result


def current_human_principal() -> PrincipalRef:
    """Project authenticated host context; absence never becomes the default user.

    The registry still resolves this reference against the current host directory
    before accessing storage. This helper alone is not an authorization grant.
    """
    user = get_current_user()
    if user is None:
        raise InvalidPrincipal("Storage requires authenticated host context")
    return PrincipalRef("human", str(user.id))
