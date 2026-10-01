"""Where each catalog provider's key comes from, from inside the deployment.

The deployment-side reading of what ``GET /api/provider-keys`` shows an
administrator, for a deployer with a shell in the container and no browser
session, in the manner of ``accounts`` and ``add_user``. Nothing reachable
over HTTP answers it for them, and it never prints a key.

Usage:
    python -m app.gateway.provider_keys.status

Prints one JSON document on stdout. ``0``: the document carries
``providers``, one entry per provider in the release's catalog, each with
``source`` -- ``product`` (a key an administrator set in the product is in
use), ``environment`` (the deployment's own key is in use) or ``none`` --
plus ``product_key`` (``absent``, ``set`` or ``unreadable``: stored, but not
opened by this deployment's wrapping key, which leaves the provider with no
key at all), ``wrapped_with`` (``current`` or ``previous``, for a rotation),
``changed_at`` and ``changed_by``; ``refusal`` says why an administrator
cannot set keys now, or is null. ``1``: the document carries ``error``
(``not_available``, ``usage`` or ``failed``) and ``message``. A caller that
runs this through a remote runner may not see its exit status, so the
document is the answer.

It reads the environment this process was started with, which is the one
the Gateway started with, and the database; it needs no running Gateway.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, NoReturn

COMMAND = "provider-keys-status"
USAGE = "usage: python -m app.gateway.provider_keys.status"


class CommandError(Exception):
    """A refusal the document reports with ``code``; the exit status is 1."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


async def _run() -> dict[str, Any]:
    from app.gateway.provider_keys.service import ProviderKeyService
    from deerflow.config import get_app_config
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config
    from deerflow.persistence.provider_keys import ProviderKeyRepository

    config = get_app_config()
    if config.database.backend == "memory":
        raise CommandError("failed", "the memory database backend keeps no keys between processes; this command needs config.database on sqlite or postgres")
    await init_engine_from_config(config.database)
    try:
        session_factory = get_session_factory()
        if session_factory is None:
            raise CommandError("failed", "persistence engine not available (check config.database)")
        service = ProviderKeyService.from_environ(ProviderKeyRepository(session_factory))
        if service is None:
            raise CommandError("not_available", "this deployment does not manage provider keys in the product (HARTMESH_PROFILE_DIR is not set)")
        document = await service.status()
    finally:
        await close_engine()
    return {"command": COMMAND, **document}


class _Parser(argparse.ArgumentParser):
    """A command line it cannot read is a refusal like any other: one document, never usage text."""

    def error(self, message: str) -> NoReturn:  # noqa: ARG002 - deliberately not echoed
        raise CommandError("usage", USAGE)


def _refusal(code: str, message: str) -> int:
    print(json.dumps({"command": COMMAND, "error": code, "message": message}, sort_keys=True), flush=True)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(prog="python -m app.gateway.provider_keys.status", add_help=False, description="Where each catalog provider's key comes from: product, environment or none.")
    try:
        parser.parse_args(argv)
        document = asyncio.run(_run())
    except CommandError as exc:
        return _refusal(exc.code, exc.message)
    except Exception as exc:  # noqa: BLE001 - the document is the answer, whatever failed
        return _refusal("failed", type(exc).__name__)
    print(json.dumps(document, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
