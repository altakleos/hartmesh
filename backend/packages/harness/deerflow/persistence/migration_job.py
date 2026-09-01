"""Helm migration Job entry point using the normal advisory-locked bootstrap."""

from __future__ import annotations

import asyncio
import json

from deerflow.config.app_config import get_app_config
from deerflow.persistence.bootstrap import verify_schema_head
from deerflow.persistence.engine import (
    close_engine,
    get_engine,
    init_engine_from_config,
)


async def run() -> str:
    config = get_app_config()
    try:
        await init_engine_from_config(config.database, migration_mode="upgrade")
        engine = get_engine()
        if engine is None:
            raise RuntimeError("migration_job_requires_durable_database")
        return await verify_schema_head(engine)
    finally:
        await close_engine()


def main() -> int:
    head = asyncio.run(run())
    print(
        json.dumps(
            {"status": "migrated", "migration_head": head},
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by the Helm Job
    raise SystemExit(main())
