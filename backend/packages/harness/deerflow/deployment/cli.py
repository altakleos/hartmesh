"""Explicit operator workflows for deployment-owned state."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections.abc import Sequence


def build_parser() -> argparse.ArgumentParser:
    """Build the bounded operator-only deployment command parser."""

    parser = argparse.ArgumentParser(
        prog="deerflow deployment",
        description="Manage server-owned deployment identity state.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    bind = commands.add_parser(
        "bind-tenant",
        help="bind a migrated nonempty legacy schema to one tenant",
    )
    bind.add_argument("--tenant-id", required=True)
    bind.add_argument(
        "--expected-nonempty-schema",
        action="store_true",
        required=True,
        help="acknowledge that the stopped legacy schema already contains data",
    )
    bind.add_argument(
        "--dry-run",
        action="store_true",
        help="inspect binding eligibility without writing the identity row or row anchors",
    )
    bind.add_argument(
        "--legacy-stream-bridge-prefix",
        help="record the exact stopped deployment stream-bridge prefix",
    )
    bind.add_argument(
        "--legacy-checkpoint-cache-prefix",
        help="record the exact stopped deployment checkpoint-cache prefix",
    )
    bind.add_argument(
        "--legacy-sandbox-ownership-prefix",
        help="record the exact stopped deployment sandbox ownership/capacity prefix",
    )
    return parser


async def _bind_tenant(
    *,
    tenant_id: str,
    dry_run: bool,
    legacy_stream_bridge_prefix: str | None,
    legacy_checkpoint_cache_prefix: str | None,
    legacy_sandbox_ownership_prefix: str | None,
) -> dict[str, object]:
    from deerflow.config import get_app_config
    from deerflow.persistence.engine import (
        close_engine,
        get_session_factory,
        init_engine_from_config,
    )
    from deerflow.persistence.tenant_binding import ensure_schema_tenant_binding
    from deerflow.runtime.tenant_identity import (
        LegacyRedisPrefixRecordV1,
        TenantIdentityV1,
    )

    identity = TenantIdentityV1.from_canonical_id(tenant_id)
    legacy_prefixes = LegacyRedisPrefixRecordV1(
        stream_bridge=legacy_stream_bridge_prefix,
        checkpoint_cache=legacy_checkpoint_cache_prefix,
        sandbox_ownership=legacy_sandbox_ownership_prefix,
    )
    config = get_app_config()
    if config.database.backend == "memory":
        raise ValueError("deployment bind-tenant requires database.backend sqlite or postgres")

    try:
        migration_needs_binding = False
        try:
            await init_engine_from_config(config.database)
        except RuntimeError as exc:
            if str(exc) != "credential_tenant_binding_required":
                raise
            # Migration 0033 deliberately refuses to infer tenant anchors for
            # populated PAT rows. Its engine/session remain usable at the
            # prior revision (Postgres rolls the DDL transaction back; SQLite
            # may retain the idempotent nullable columns), allowing this
            # explicit operator flow to install the authoritative singleton.
            migration_needs_binding = True
        session_factory = get_session_factory()
        if session_factory is None:
            raise RuntimeError("persistence session factory was not initialized")
        result = await ensure_schema_tenant_binding(
            session_factory,
            identity,
            allow_nonempty_legacy=True,
            require_nonempty_legacy=True,
            dry_run=dry_run,
            legacy_redis_prefixes=legacy_prefixes,
        )
        if migration_needs_binding and not dry_run:
            # Re-open through the ordinary bootstrap after the binding commit;
            # 0033 can now backfill PAT rows from the singleton and enforce its
            # required tenant columns. Closing first avoids leaking the engine
            # retained by the failed bootstrap attempt.
            await close_engine()
            await init_engine_from_config(config.database)
        recorded_components = [
            name
            for name, value in (
                ("stream_bridge", result.legacy_redis_prefixes.stream_bridge),
                (
                    "checkpoint_cache",
                    result.legacy_redis_prefixes.checkpoint_cache,
                ),
                (
                    "sandbox_ownership",
                    result.legacy_redis_prefixes.sandbox_ownership,
                ),
            )
            if value is not None
        ]
        return {
            "action": result.action.value,
            "dry_run": dry_run,
            "identity_version": result.tenant.version,
            "tenant_ref": result.tenant.public_ref,
            "tenant_digest": result.tenant.digest,
            "legacy_redis_prefixes_recorded": recorded_components,
        }
    finally:
        await close_engine()


def main(argv: Sequence[str] | None = None) -> int:
    """Run an explicit deployment administration command."""

    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "bind-tenant":
            result = asyncio.run(
                _bind_tenant(
                    tenant_id=args.tenant_id,
                    dry_run=args.dry_run,
                    legacy_stream_bridge_prefix=args.legacy_stream_bridge_prefix,
                    legacy_checkpoint_cache_prefix=args.legacy_checkpoint_cache_prefix,
                    legacy_sandbox_ownership_prefix=args.legacy_sandbox_ownership_prefix,
                )
            )
            print(json.dumps(result, sort_keys=True, separators=(",", ":")))
            return 0
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"deployment command failed: {exc}", file=sys.stderr)
        return 1
    raise AssertionError(f"unhandled deployment command: {args.command}")


__all__ = ["build_parser", "main"]
