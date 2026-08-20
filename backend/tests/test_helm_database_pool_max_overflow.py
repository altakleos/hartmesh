"""Helm contract for the application database pool overflow cap."""

from __future__ import annotations

import pytest
from support.helm import deployment_env


@pytest.mark.parametrize(
    ("extra_args", "expected"),
    [
        ((), None),
        (("--set", "database.poolMaxOverflow=2"), "2"),
    ],
    ids=["unset", "configured"],
)
def test_database_pool_max_overflow_renders_conditionally(
    extra_args: tuple[str, ...],
    expected: str | None,
) -> None:
    env = deployment_env("gateway", *extra_args)

    if expected is None:
        assert "DATABASE_POOL_MAX_OVERFLOW" not in env
    else:
        assert env["DATABASE_POOL_MAX_OVERFLOW"] == expected
