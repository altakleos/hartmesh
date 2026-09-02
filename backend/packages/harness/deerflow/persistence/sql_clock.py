"""Dialect-aware database wall clocks for post-lock lease decisions."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func


def database_wall_clock_expression(dialect_name: str) -> Any:
    """Return a fractional statement-time clock for the selected dialect."""

    if dialect_name == "postgresql":
        # PostgreSQL ``now()`` is fixed at transaction start. Callers use this
        # after acquiring their row lock, so observe the actual wall clock.
        return func.clock_timestamp()
    if dialect_name == "sqlite":
        # CURRENT_TIMESTAMP truncates to whole seconds in SQLite, which can
        # extend a stale lease by nearly one second at a security boundary.
        # Match SQLAlchemy's SQLite ``DateTime`` storage representation so
        # column comparisons are chronological rather than comparing a space
        # in stored values with ``T`` in the expression text.
        return func.strftime("%Y-%m-%d %H:%M:%f", "now")
    return func.current_timestamp()


def coerce_database_wall_clock(value: object) -> datetime:
    """Normalize a database clock result to an aware UTC datetime."""

    if isinstance(value, datetime):
        result = value
    elif isinstance(value, str):
        result = datetime.fromisoformat(value)
    else:
        raise TypeError("database clock is unavailable")
    if result.tzinfo is None:
        result = result.replace(tzinfo=UTC)
    return result.astimezone(UTC)


__all__ = ["coerce_database_wall_clock", "database_wall_clock_expression"]
