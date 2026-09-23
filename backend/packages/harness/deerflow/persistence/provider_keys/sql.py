"""SQLAlchemy-backed store for provider keys set in the product.

It stores what it is handed -- ciphertext -- and never sees a key. A change
and its record are written in one transaction, so a key never changes
without a record of who changed it.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from deerflow.persistence.provider_keys.model import ProviderKeyEventRow, ProviderKeyRow
from deerflow.utils.time import coerce_iso

EVENT_LIMIT_MAX = 200


def _aware(value: datetime) -> datetime:
    # SQLite drops tzinfo on read; every stored time is UTC.
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


@dataclass(frozen=True)
class StoredProviderKey:
    variable: str
    ciphertext: str
    changed_at: datetime
    changed_by: str

    def __repr__(self) -> str:
        return f"StoredProviderKey(variable={self.variable!r}, changed_at={self.changed_at!r}, changed_by={self.changed_by!r})"


class ProviderKeyRepository:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory

    async def stored(self) -> dict[str, StoredProviderKey]:
        """Every stored key by variable."""
        async with self._sf() as session:
            rows = (await session.execute(select(ProviderKeyRow).order_by(ProviderKeyRow.variable))).scalars().all()
            return {row.variable: StoredProviderKey(row.variable, row.ciphertext, _aware(row.changed_at), row.changed_by) for row in rows}

    async def put(self, variable: str, ciphertext: str, *, actor_id: str, actor_email: str | None) -> str:
        """Store *ciphertext* for *variable*; ``"added"`` or ``"replaced"``."""
        now = datetime.now(UTC)
        async with self._sf() as session:
            row = await session.get(ProviderKeyRow, variable)
            action = "added" if row is None else "replaced"
            if row is None:
                session.add(ProviderKeyRow(variable=variable, ciphertext=ciphertext, changed_at=now, changed_by=actor_email or actor_id))
            else:
                row.ciphertext = ciphertext
                row.changed_at = now
                row.changed_by = actor_email or actor_id
            session.add(ProviderKeyEventRow(event_id=str(uuid.uuid4()), variable=variable, action=action, actor_id=actor_id, actor_email=actor_email, occurred_at=now))
            await session.commit()
        return action

    async def remove(self, variable: str, *, actor_id: str, actor_email: str | None) -> bool:
        """Delete the stored key; False, and no record, when there was none."""
        async with self._sf() as session:
            result = await session.execute(delete(ProviderKeyRow).where(ProviderKeyRow.variable == variable))
            if not result.rowcount:
                await session.rollback()
                return False
            session.add(ProviderKeyEventRow(event_id=str(uuid.uuid4()), variable=variable, action="removed", actor_id=actor_id, actor_email=actor_email, occurred_at=datetime.now(UTC)))
            await session.commit()
        return True

    async def rewrap(self, variable: str, *, expected: str, ciphertext: str) -> bool:
        """Replace *expected* with *ciphertext* -- the same key under a new wrapping key.

        Compare-and-swap, so a key an administrator replaced since it was read
        is never overwritten. Not a change of key: no record, and the
        last-changed facts stay.
        """
        async with self._sf() as session:
            result = await session.execute(update(ProviderKeyRow).where(ProviderKeyRow.variable == variable, ProviderKeyRow.ciphertext == expected).values(ciphertext=ciphertext))
            await session.commit()
            return bool(result.rowcount)

    async def events(self, *, limit: int = 50) -> list[dict]:
        """The most recent changes, newest first."""
        if type(limit) is not int or not 1 <= limit <= EVENT_LIMIT_MAX:
            raise ValueError(f"limit must be between 1 and {EVENT_LIMIT_MAX}")
        async with self._sf() as session:
            rows = (await session.execute(select(ProviderKeyEventRow).order_by(ProviderKeyEventRow.occurred_at.desc(), ProviderKeyEventRow.event_id).limit(limit))).scalars().all()
            return [
                {
                    "event_id": row.event_id,
                    "variable": row.variable,
                    "action": row.action,
                    "actor_id": row.actor_id,
                    "actor_email": row.actor_email,
                    "occurred_at": coerce_iso(row.occurred_at),
                }
                for row in rows
            ]
