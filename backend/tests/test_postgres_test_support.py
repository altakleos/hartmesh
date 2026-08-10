"""Contracts for the shared PostgreSQL qualification URL adapter."""

from __future__ import annotations

import pytest
from support.postgres import postgres_async_url


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            "postgres://user:p%40ss@db.example/test?sslmode=require&application_name=&application_name=qualification&channel_binding=require&keep=%2F",
            "postgresql+asyncpg://user:p%40ss@db.example/test?application_name=&application_name=qualification&keep=%2F",
        ),
        (
            "postgresql://user@localhost/test?blank=&repeated=one&repeated=two",
            "postgresql+asyncpg://user@localhost/test?blank=&repeated=one&repeated=two",
        ),
        (
            "postgresql+asyncpg://user@localhost/test?sslmode=disable&unrelated=value",
            "postgresql+asyncpg://user@localhost/test?unrelated=value",
        ),
    ],
)
def test_postgres_async_url_preserves_non_driver_query_semantics(source: str, expected: str) -> None:
    assert postgres_async_url(source) == expected
