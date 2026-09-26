"""ORM models for what the deployer holds against an identity: turned off, or limited in role.

Both are keyed by the identity provider's ``(issuer, subject)`` rather than
by a user row, because a person can be turned off or demoted before their
first sign-in, when no row exists, and the fact must still hold when one
would be created -- and because one identity can hold an account under each
provider configured at its issuer, and the fact is the person's. An account
is disabled exactly when its identity is in ``disabled_identities``, and its
role is at most the one in ``role_limits``: every read derives both.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base

#: The roles an account holds, lowest first.
ROLES = ("user", "admin")

#: The roles a limit may hold an identity at: every role below the highest.
#: Today that is ``user`` alone.
LIMIT_ROLES = ROLES[:-1]


def issuer_key(issuer: str) -> str:
    """One spelling per issuer: discovery already treats a trailing slash as the same address."""
    return issuer.strip().rstrip("/")


def identity_lock_key(issuer: str, subject: str) -> str:
    """What serialises every write of one identity's role on PostgreSQL (a transaction-scoped advisory lock)."""
    return f"hartmesh.identity-role\n{issuer_key(issuer)}\n{subject}"


def limited_role(role: str, limit: str | None) -> str:
    """The lower of ``role`` and ``limit``; ``role`` itself when no limit holds.

    Fails closed: a limit it does not know holds the lowest role, and a role
    it does not know under a limit reads as the limit.
    """
    if limit is None:
        return role
    ceiling = limit if limit in ROLES else ROLES[0]
    if role not in ROLES:
        return ceiling
    return role if ROLES.index(role) <= ROLES.index(ceiling) else ceiling


class DisabledIdentityRow(Base):
    __tablename__ = "disabled_identities"

    # Stored as ``issuer_key`` spells it, so a lookup never misses on a slash.
    issuer: Mapped[str] = mapped_column(String(512), primary_key=True)
    subject: Mapped[str] = mapped_column(String(255), primary_key=True)
    disabled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RoleLimitRow(Base):
    """The highest role an identity may hold here, whatever its sign-in's claim says."""

    __tablename__ = "role_limits"

    issuer: Mapped[str] = mapped_column(String(512), primary_key=True)
    subject: Mapped[str] = mapped_column(String(255), primary_key=True)
    role: Mapped[str] = mapped_column(String(16), nullable=False)
    limited_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
