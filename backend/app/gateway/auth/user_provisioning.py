"""User provisioning for OIDC logins.

Handles the logic of finding existing users, auto-creating new ones, and
enforcing email domain restrictions. A pre-existing local account is never
auto-linked to an OIDC identity: an email collision blocks the SSO login with
a 409 instead, so an SSO login can never seize a local password account.

Two checks come before anything else at every sign-in, first or not: an
identity the deployer turned off is refused, and, where an admission claim
is configured, a token that does not carry an admitting value is refused,
with no account created and none returned.

An address the account record will not hold is refused in its own right
(``sso_email_unusable``), never as a collision with an account that does not
exist: the record's own refusal and the repository's uniqueness refusal are
both ``ValueError``, and only the second one means an account is in the way.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from fastapi import HTTPException, status
from pydantic import ValidationError

from app.gateway.auth.access import admitted_values, refuse_turned_off, role_for
from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.models import User
from app.gateway.auth.oidc import OIDCIdentity
from deerflow.config.auth_config import OIDCProviderConfig

logger = logging.getLogger(__name__)

# The refusal for an address no account can hold, and the code the login page
# maps it by. Its own code on purpose: an address the record refuses is not a
# conflict with an account, so the person must not be sent looking for one.
EMAIL_UNUSABLE_CODE = "sso_email_unusable"
EMAIL_UNUSABLE_MESSAGE = "Your organization's sign-in did not provide a usable email address. Ask your administrator to correct it."


# The refusal for an address another account in this deployment holds. Its
# own code too: the person is not in conflict with a password account of
# their own, and telling them to sign in with a password would be false.
EMAIL_TAKEN_CODE = "sso_email_taken"
# One sentence, identical here and in both locale maps. It has to be
# distinguishable by ear from the account-exists refusal on a support
# call, because the remedies are opposite: that one is a stale account of
# the person's own to clear, this one is another person's live account,
# and clearing it would delete someone.
EMAIL_TAKEN_MESSAGE = "That email address belongs to another person's account here. Ask your administrator to release it."


class EmailUnusable(HTTPException):
    """The provider asserted an address the account record will not hold, or none at all."""

    def __init__(self) -> None:
        super().__init__(status_code=status.HTTP_403_FORBIDDEN, detail=EMAIL_UNUSABLE_MESSAGE)
        self.redirect_code = EMAIL_UNUSABLE_CODE


class EmailTaken(HTTPException):
    """Another provider account already holds the address this sign-in carries."""

    def __init__(self) -> None:
        super().__init__(status_code=status.HTTP_409_CONFLICT, detail=EMAIL_TAKEN_MESSAGE)
        self.redirect_code = EMAIL_TAKEN_CODE


def asserted_email(identity: OIDCIdentity) -> str | None:
    """The address the token carries, canonical, or ``None`` when it carries none usable.

    ``OIDCIdentity.email`` is a plain ``str`` field on a dataclass, so a
    provider that emits a list (a directory attribute mapped as multivalued)
    or any other type reaches here as that type. Reading it as an address
    would raise inside the sign-in and answer 500; there is no address here,
    which is a refusal the person can be told about.
    """
    email = getattr(identity, "email", None)
    if not isinstance(email, str) or not email.strip():
        return None
    return email.strip().lower()


def holds_as_an_account(address: str) -> bool:
    """Whether the account record would accept *address* as an account's email."""
    try:
        User(email=address)
    except ValidationError:
        return False
    return True


async def get_or_provision_oidc_user(
    provider_id: str,
    provider_config: OIDCProviderConfig,
    identity: OIDCIdentity,
    local_provider: LocalAuthProvider,
) -> dict:
    """Resolve an OIDC identity to a DeerFlow user.

    Flow:
    0. Refuse an identity the deployer turned off, and (with an admission
       claim configured) a token without an admitting value
    1. Look up existing user by (provider, subject); re-read its role when
       the role mapping is set; stamp the sign-in
    2. If not found, require a verified email and an email at all
    3. Enforce the domain restriction
    4. Block if a local account already owns the email (never auto-link)
    5. Auto-create if enabled, refusing an address no account can hold

    Returns a dict with ``user`` (the User model instance) and ``created`` (bool).
    """
    # 0. Before any account is created or returned.
    if await local_provider.is_identity_disabled(provider_config.issuer, identity.subject):
        raise refuse_turned_off(provider_config, identity)
    admitted = admitted_values(provider_config, identity)
    now = datetime.now(UTC)

    # 1. Existing OAuth link, pinned to the issuer that created it. The lookup
    # key is (provider name, subject); the issuer is what stops a provider
    # name pointed at a new issuer from handing this account to whoever holds
    # the same subject there. A row linked before the issuer was recorded
    # carries None and adopts the configured issuer now: nothing else could
    # ever know which issuer it belonged to.
    existing = await local_provider.get_user_by_oauth(provider_id, identity.subject)
    if existing:
        recorded = getattr(existing, "oauth_issuer", None)
        if recorded is None:
            existing.oauth_issuer = provider_config.issuer
        elif _issuer_key(recorded) != _issuer_key(provider_config.issuer):
            logger.warning("OIDC sign-in refused: the subject under provider %s is linked to issuer %s, and the provider is configured for %s", provider_id, recorded, provider_config.issuer)
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Your account is linked to a different identity provider. Contact your administrator.",
            )
        # The address follows the sign-in: the subject is who the person is,
        # the address is something the provider says about them, and a company
        # that corrects it there must not have to correct it here too.
        held = existing.email
        follows = await _address_that_follows(provider_id, provider_config, identity, existing, local_provider)
        existing.last_sign_in_at = now

        def settle(address: str | None, *, regrade: bool) -> None:
            """Put the account on *address* (or back on the one it held) and read its role from there."""
            existing.email = address if address is not None else held
            if not (provider_config.access_roles or regrade):
                # Without the claim mapping the administrators' list decides,
                # and it is keyed by address. An address that did not change
                # is not an occasion to re-grade a role.
                return
            # With the mapping, the role follows the claim at every sign-in;
            # without it, the list is read from the address this sign-in
            # settled on -- or a person whose address changed would carry a
            # role their old one earned.
            role = role_for(provider_config, admitted, existing.email)
            if role != existing.system_role:
                read_from = "the claim" if provider_config.access_roles else "the address it now holds"
                logger.info("OIDC sign-in: role of subject %s at issuer %s is now %s (was %s), read from %s", identity.subject, provider_config.issuer, role, existing.system_role, read_from)
                existing.system_role = role  # type: ignore[assignment]

        settle(follows, regrade=follows is not None)
        # A targeted write: never the whole row, so a token_version read a
        # moment ago cannot land back over an end-sessions that ran meanwhile.
        followed = await local_provider.record_sign_in(existing, email=follows)
        if follows is not None and not followed:
            # Another account took the address between the holder check and
            # this write. The contract is the same as finding it there in the
            # first place: the person still signs in as the account they are,
            # keeping the address it had -- and the role read from that one.
            logger.warning(
                "OIDC sign-in: the address now asserted for subject %s of provider %s at issuer %s was taken between the check and the write; the account keeps the one it has",
                identity.subject,
                provider_id,
                provider_config.issuer,
            )
            # Back to the address the account holds, and the role that one earns.
            settle(None, regrade=True)
            await local_provider.record_sign_in(existing)
        return {"user": existing, "created": False}

    # 2. Verified email requirement
    if provider_config.require_verified_email and not identity.email_verified:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=("Your email could not be verified by the identity provider. Please contact your administrator."),
        )

    email = asserted_email(identity)
    if email is None:
        # No address at all is the degenerate form of an address no account
        # can hold, and reads the same way to the person. It used to fall to
        # the status map and tell them SSO was not allowed for their account,
        # which said their account was the problem when they have none.
        logger.warning("OIDC sign-in refused: no usable email address was asserted for subject %s at issuer %s", identity.subject, provider_config.issuer)
        raise EmailUnusable()

    # 3. Domain restriction
    if not domain_is_allowed(provider_config, email):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Your email domain is not allowed. Please use an approved email address.",
        )

    # 4. Block if an account already owns this email. We never auto-link an
    # SSO identity onto a pre-existing account, since that would let an SSO
    # login take over an account that happens to share the email. Which
    # account holds it decides what the person is told: a password account of
    # their own is theirs to sign in with, and a provider account of another
    # subject is not, so telling them to use a password there would be false.
    holder = await local_provider.get_user_by_email(email)

    # The discriminator is the door the holder has, because that is what the
    # two messages differ about: one sends the person to a password they can
    # use, the other must not. A row with an identity but no password is a
    # provider account; one with neither is not a door either.
    if holder is not None and not holder.password_hash:
        logger.warning(
            "OIDC sign-in refused: subject %s of provider %s at issuer %s carries an address held by %s",
            identity.subject,
            provider_id,
            provider_config.issuer,
            describe_holder(holder),
        )
        raise EmailTaken()

    if holder is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=("An account with this email already exists. Contact your administrator to link it to your SSO account."),
        )

    # 5. Auto-create
    if not provider_config.auto_create_users:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Automatic account creation is disabled. Contact your administrator.",
        )

    role = role_for(provider_config, admitted, email)
    try:
        user = await local_provider.create_oauth_user(
            email=email,
            oauth_provider=provider_id,
            oauth_id=identity.subject,
            system_role=role,
            oauth_issuer=provider_config.issuer,
            last_sign_in_at=now,
        )
    except ValidationError as exc:
        # The record refused the address itself. This arm comes first because
        # pydantic's ValidationError *is* a ValueError: without it the race
        # handler below reports an address no account can hold as a collision
        # with an account that does not exist. The model stays the only
        # validator -- nothing here restates what an address may be.
        if not _only_the_address_was_refused(exc):
            raise
        logger.warning("OIDC sign-in refused: the address asserted for subject %s at issuer %s is not one an account can hold", identity.subject, provider_config.issuer)
        raise EmailUnusable() from None
    except ValueError:
        # Lost a race: a concurrent callback (double-click, replayed code) already
        # inserted a row that collides on the unique index. Re-resolve instead of
        # bubbling a raw 500. If the winner created this same identity, return it;
        # otherwise the email now belongs to a different account → 409.
        existing = await local_provider.get_user_by_oauth(provider_id, identity.subject)
        if existing:
            return {"user": existing, "created": False}
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=("An account with this email already exists. Contact your administrator to link it to your SSO account."),
        ) from None
    logger.info("Auto-created OIDC user %s (provider=%s, role=%s)", email, provider_id, role)
    return {"user": user, "created": True}


async def _address_that_follows(
    provider_id: str,
    provider_config: OIDCProviderConfig,
    identity: OIDCIdentity,
    existing: User,
    local_provider: LocalAuthProvider,
) -> str | None:
    """The address this sign-in should write to *existing*, or ``None`` to leave it.

    Five reasons to leave it, each of which still signs the person in,
    because their subject is who they are and an attribute of theirs is not
    grounds to turn them away:

    - the token carries no usable address;
    - it carries one the deployment will not take unverified, under the same
      rule that governs account creation;
    - it carries one from a domain the deployment does not allow -- the same
      rule, and the same list, that governs account creation. Without this an
      address a *new* subject may not have, an existing one could acquire,
      and since the role is read from the address this sign-in settles on,
      acquiring an ``admin_emails`` address would be a promotion;
    - it carries one no account record can hold -- ``record_sign_in`` is a
      targeted UPDATE, not a model write, so storing such an address would
      leave a row that raises on every later read;
    - another account already holds it, which is the deployer's to resolve
      and never this sign-in's to take.
    """
    asserted = asserted_email(identity)
    if asserted is None or asserted == existing.email.lower():
        return None
    if provider_config.require_verified_email and not identity.email_verified:
        logger.info("OIDC sign-in: the address for subject %s at issuer %s changed but is not verified; the account keeps the one it has", identity.subject, provider_config.issuer)
        return None
    if not domain_is_allowed(provider_config, asserted):
        logger.warning("OIDC sign-in: the address now asserted for subject %s at issuer %s is from a domain this deployment does not allow; the account keeps the one it has", identity.subject, provider_config.issuer)
        return None
    if not holds_as_an_account(asserted):
        logger.warning("OIDC sign-in: the address now asserted for subject %s at issuer %s is not one an account can hold; the account keeps the one it has", identity.subject, provider_config.issuer)
        return None
    holder = await local_provider.get_user_by_email(asserted)
    if holder is not None and str(holder.id) != str(existing.id):
        logger.warning(
            "OIDC sign-in: the address now asserted for subject %s of provider %s at issuer %s is held by %s; the account keeps the one it has",
            identity.subject,
            provider_id,
            provider_config.issuer,
            describe_holder(holder),
        )
        return None
    return asserted


def domain_is_allowed(provider_config: OIDCProviderConfig, email: str) -> bool:
    """The deployment's domain rule, read in one place.

    Creation and the follow have to apply the same list: an address a new
    subject may not have must not be one an existing subject can acquire.
    Two copies of the comparison would be free to drift, and the drift would
    not show up as a failure anywhere -- only as an account holding an
    address the deployment had said no to.
    """
    if not provider_config.allowed_email_domains:
        return True
    return email.rsplit("@", 1)[-1] in {allowed.lower().lstrip("@") for allowed in provider_config.allowed_email_domains}


def describe_holder(holder: User) -> str:
    """Name the account that holds an address, in terms the deployer addresses accounts by.

    The provider name is part of it: two configured providers may point at
    one issuer, and then ``(issuer, subject)`` alone would not say which
    account. A holder with no provider identity has no such name at all --
    saying "subject None at issuer None" would be worse than saying what it
    is.
    """
    if holder.oauth_provider and holder.oauth_id:
        return f"the account of subject {holder.oauth_id} of provider {holder.oauth_provider} at issuer {holder.oauth_issuer}"
    if holder.password_hash:
        return "a local password account"
    return f"the account {holder.id}, which has neither a provider identity nor a password"


def _only_the_address_was_refused(exc: ValidationError) -> bool:
    """True when the record's complaint is about the address and nothing else.

    The address is the one field here a provider's assertion supplies; a
    complaint about any other is a fault in this code, and propagating it says
    so instead of refusing the person for something they did not send.
    """
    complaints = exc.errors()
    return bool(complaints) and all(complaint.get("loc") == ("email",) for complaint in complaints)


def _issuer_key(issuer: str) -> str:
    """Two spellings of one issuer: discovery already treats a trailing slash as the same address."""
    return issuer.strip().rstrip("/")


def _resolve_role(email: str, admin_emails: list[str]) -> str:
    """Return ``admin`` if the email is in the admin list, otherwise ``user``."""
    email_lower = email.lower()
    return "admin" if any(e.lower() == email_lower for e in admin_emails) else "user"
