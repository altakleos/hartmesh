#!/usr/bin/env python3
"""Dry-run-only bounded inventory of tenant-derived Redis namespaces."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence


def _bounded_positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1 or parsed > 10_000:
        raise argparse.ArgumentTypeError("must be between 1 and 10000")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument(
        "--redis-url",
        default=None,
        help="Redis URL (defaults to REDIS_URL; never emitted in output)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        required=True,
        help="required safety acknowledgement; this command never mutates Redis",
    )
    parser.add_argument("--scan-count", type=_bounded_positive_int, default=500)
    parser.add_argument("--max-scan-iterations", type=_bounded_positive_int, default=100)
    return parser


def inventory(
    client: object,
    *,
    tenant_id: str,
    scan_count: int,
    max_scan_iterations: int,
) -> dict[str, object]:
    from deerflow.runtime.tenant_identity import (
        RedisTenantComponent,
        TenantIdentityV1,
        TenantSubsystem,
        redis_component_match_pattern,
    )

    identity = TenantIdentityV1.from_canonical_id(tenant_id)
    namespace = identity.namespace(TenantSubsystem.REDIS)
    families: list[dict[str, object]] = []
    for component in RedisTenantComponent:
        cursor = 0
        count = 0
        iterations = 0
        pattern = redis_component_match_pattern(namespace, component)
        truncated = False
        while True:
            cursor, keys = client.scan(
                cursor=cursor,
                match=pattern,
                count=scan_count,
            )
            count += len(keys)
            iterations += 1
            if cursor == 0:
                break
            if iterations >= max_scan_iterations:
                truncated = True
                break
        families.append(
            {
                "component": component.value,
                "key_count": count,
                "scan_truncated": truncated,
            }
        )
    return {
        "dry_run": True,
        "identity_version": identity.version,
        "tenant_ref": identity.public_ref,
        "tenant_digest": identity.digest,
        "prefix_schema_version": 1,
        "key_families": families,
        # The covered factories currently use Redis keys/streams only. Keep
        # this explicit so ACL reviews do not infer uninspected channels.
        "pubsub_channel_patterns": 0,
    }


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    redis_url = args.redis_url or os.environ.get("REDIS_URL")
    if not redis_url:
        print(
            "tenant namespace check failed: --redis-url or REDIS_URL is required",
            file=sys.stderr,
        )
        return 2
    try:
        from redis import Redis

        client = Redis.from_url(redis_url, decode_responses=False)
        try:
            result = inventory(
                client,
                tenant_id=args.tenant_id,
                scan_count=args.scan_count,
                max_scan_iterations=args.max_scan_iterations,
            )
        finally:
            client.close()
    except Exception:  # noqa: BLE001 - connection details must stay redacted
        print(
            "tenant namespace check failed; connection details are redacted",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
