"""ORM model for the identities the deployer turned off.

Keyed by the identity provider's ``(issuer, subject)`` rather than by a user
row, because a person can be turned off before their first sign-in, when no
row exists, and the refusal must still hold when one would be created. An
account is disabled exactly when its identity is here: the user row keeps
no copy of the fact, it derives it at every read.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


def issuer_key(issuer: str) -> str:
    """One spelling per issuer: discovery already treats a trailing slash as the same address."""
    return issuer.strip().rstrip("/")


class DisabledIdentityRow(Base):
    __tablename__ = "disabled_identities"

    # Stored as ``issuer_key`` spells it, so a lookup never misses on a slash.
    issuer: Mapped[str] = mapped_column(String(512), primary_key=True)
    subject: Mapped[str] = mapped_column(String(255), primary_key=True)
    disabled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
