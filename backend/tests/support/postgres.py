"""Adapt libpq test URIs to SQLAlchemy's asyncpg connection options.

asyncpg rejects libpq-only query options such as ``sslmode`` and
``channel_binding``; drop them the way HartMesh's existing live-PostgreSQL
tests do (``tests/test_scheduled_task_postgres.py``).
"""

from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def asyncpg_test_url(uri: str) -> str:
    parts = urlsplit(uri)
    query = urlencode([(key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True) if key not in {"sslmode", "channel_binding"}])
    return urlunsplit(parts._replace(query=query))
