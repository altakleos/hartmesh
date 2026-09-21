"""Admission by claim, and the role that follows it.

The identity provider is the membership authority: for each deployment it
says who belongs and whether each person administers it, as one claim in
the token. Configuration names that claim and the values that admit
(``access_claim``, ``access_values``) and may map values to the product's
two roles (``access_roles``). Everything here reads those three fields and
the identity's claims; nothing keeps a member list.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException, status

from app.gateway.auth.oidc import OIDCIdentity
from deerflow.config.auth_config import OIDCProviderConfig

logger = logging.getLogger(__name__)

# The two things a refused person sees, and the codes the login page maps them by.
NO_ACCESS_CODE = "sso_no_access"
NO_ACCESS_MESSAGE = "You have no access to this workspace. Ask your administrator."
ACCESS_OFF_CODE = "sso_access_off"
ACCESS_OFF_MESSAGE = "Your access to this workspace has been turned off. Ask your administrator."

# Where a claim is read from, in order: the ID token, then userinfo when
# the ID token does not carry it at all.
CLAIM_SOURCES = ("id_token", "userinfo")


class AccessRefused(HTTPException):
    """A sign-in the deployment will not admit; carries the code the login page shows."""

    def __init__(self, *, code: str, message: str) -> None:
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, detail=message)
        self.redirect_code = code


@dataclass(frozen=True)
class ClaimReading:
    """What one claim said, or why it could not be read."""

    source: str | None
    values: frozenset[str] | None
    problem: str | None  # "missing" | "empty" | "wrong type"


def read_claim(identity: OIDCIdentity, name: str) -> ClaimReading:
    """Read ``name`` literally (colons and dots are characters, not a path).

    Accepted shapes: a list of strings, a single string, or an object whose
    keys are the values. The ID token is consulted first; userinfo only
    when the ID token does not carry the name at all, so a claim the ID
    token carries in a wrong shape is refused rather than repaired from
    userinfo.
    """
    sources = {"id_token": identity.id_token_claims, "userinfo": identity.userinfo_claims}
    for source in CLAIM_SOURCES:
        claims = sources[source]
        if name in claims:
            return _reading(source, claims[name])
    return ClaimReading(source=None, values=None, problem="missing")


def _reading(source: str, raw: Any) -> ClaimReading:
    if isinstance(raw, str):
        values = [raw]
    elif isinstance(raw, list) and all(isinstance(item, str) for item in raw):
        values = raw
    elif isinstance(raw, dict) and all(isinstance(key, str) for key in raw):
        values = list(raw)
    else:
        return ClaimReading(source=source, values=None, problem="wrong type")
    cleaned = frozenset(value.strip() for value in values if value.strip())
    if not cleaned:
        return ClaimReading(source=source, values=None, problem="empty")
    return ClaimReading(source=source, values=cleaned, problem=None)


def admitted_values(provider_config: OIDCProviderConfig, identity: OIDCIdentity) -> frozenset[str] | None:
    """The admitting values the token carries, or ``None`` when no claim is configured.

    Raises :class:`AccessRefused` when the claim is configured and the token
    does not admit. The log line names the issuer and the subject, never a
    token, and the person's message never names the claim.
    """
    if not provider_config.access_claim:
        return None
    reading = read_claim(identity, provider_config.access_claim)
    if reading.problem is not None:
        reason = f"claim {reading.problem}" + (f" in {reading.source}" if reading.source else "")
    else:
        admitted = frozenset(reading.values or ()) & frozenset(provider_config.access_values)
        if admitted:
            return admitted
        reason = f"no admitting value in {reading.source}"
    logger.warning("OIDC sign-in refused: no access for subject %s at issuer %s (%s)", identity.subject, provider_config.issuer, reason)
    raise AccessRefused(code=NO_ACCESS_CODE, message=NO_ACCESS_MESSAGE)


def role_for(provider_config: OIDCProviderConfig, admitted: frozenset[str] | None, email: str) -> str:
    """The role the sign-in carries: from the mapping when set, else from the email list.

    With the mapping, ``admin`` wins when the token carries several mapped
    values. Without it, the email list decides, as it always did.
    """
    if provider_config.access_roles:
        roles = {provider_config.access_roles[value] for value in (admitted or ()) if value in provider_config.access_roles}
        return "admin" if "admin" in roles else "user"
    lowered = email.lower()
    return "admin" if any(candidate.lower() == lowered for candidate in provider_config.admin_emails) else "user"


def refuse_turned_off(provider_config: OIDCProviderConfig, identity: OIDCIdentity) -> AccessRefused:
    """The refusal a person whose access the deployer turned off gets at sign-in."""
    logger.warning("OIDC sign-in refused: access turned off for subject %s at issuer %s", identity.subject, provider_config.issuer)
    return AccessRefused(code=ACCESS_OFF_CODE, message=ACCESS_OFF_MESSAGE)
