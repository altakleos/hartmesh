"""ORM models for provider keys an administrator set in the product.

``provider_keys`` holds one row per catalog variable (``OPENAI_API_KEY``),
the key wrapped under the deployment's wrapping key, which lives in the
environment and never on this database's disk. ``provider_key_events`` is
the record of every add, replace and remove: who and when, never the value.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import DateTime, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from deerflow.persistence.base import Base


class ProviderKeyRow(Base):
    __tablename__ = "provider_keys"

    # The catalog fragment's variable: the one name a release promises not to change.
    variable: Mapped[str] = mapped_column(String(64), primary_key=True)
    ciphertext: Mapped[str] = mapped_column(Text, nullable=False)
    changed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC))
    changed_by: Mapped[str] = mapped_column(String(320), nullable=False)

    def __repr__(self) -> str:
        # The base repr prints every column; the ciphertext is not the key,
        # but nothing gains from it reaching a log line either.
        return f"ProviderKeyRow(variable={self.variable!r}, changed_at={self.changed_at!r}, changed_by={self.changed_by!r})"


class ProviderKeyEventRow(Base):
    __tablename__ = "provider_key_events"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    variable: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    # added | replaced | removed
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    actor_id: Mapped[str] = mapped_column(String(64), nullable=False)
    # The address the administrator held when they acted, so the record still
    # reads after the account is renamed or removed.
    actor_email: Mapped[str | None] = mapped_column(String(320))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True, default=lambda: datetime.now(UTC))
