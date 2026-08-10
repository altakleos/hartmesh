"""Shared PostgreSQL qualification helpers."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_SUPPORTED_POSTGRES_SCHEMES = frozenset({"postgres", "postgresql", "postgresql+asyncpg"})
_ASYNC_DRIVER_QUERY_OPTIONS = frozenset({"sslmode", "channel_binding"})


def postgres_async_url(url: str) -> str:
    """Return an asyncpg URL while preserving non-driver query parameters."""

    parts = urlsplit(url)
    if parts.scheme not in _SUPPORTED_POSTGRES_SCHEMES:
        raise ValueError("PostgreSQL qualification URL must use postgres, postgresql, or postgresql+asyncpg")
    query = urlencode([(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key not in _ASYNC_DRIVER_QUERY_OPTIONS])
    return urlunsplit(parts._replace(scheme="postgresql+asyncpg", query=query))
