#!/usr/bin/env python3
"""Plan the tenant-scoped Honcho workspace migration without provider writes.

Honcho does not expose a workspace-copy operation in DeerFlow's supported
client surface. This utility therefore emits a bounded, pseudonymous mapping
for an operator to execute with provider tooling; it never reads messages,
accepts credentials, writes to Honcho, or enables a legacy dual-read path.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from deerflow.agents.memory.backends.honcho.config import (
    HONCHO_ID_MAX_LENGTH,
    HonchoConfig,
    HonchoIdentityResolver,
)
from deerflow.agents.memory.honcho_tenant import project_honcho_backend_config
from deerflow.runtime.tenant_identity import TenantIdentityV1

MAX_OUTPUT_MAPPINGS = 100
MAX_INVENTORY_ENTRIES = 10_000
MAX_INVENTORY_BYTES = 1_048_576
MAX_RAW_USER_BYTES = 512

_HONCHO_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$", re.ASCII)


class HonchoMigrationError(ValueError):
    """Stable, content-free migration planning failure."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


def _validate_page(*, offset: object, limit: object) -> tuple[int, int]:
    if type(offset) is not int or offset < 0:
        raise HonchoMigrationError(
            "honcho_migration_input_invalid",
            "offset must be a non-negative integer",
        )
    if type(limit) is not int or not 1 <= limit <= MAX_OUTPUT_MAPPINGS:
        raise HonchoMigrationError(
            "honcho_migration_input_invalid",
            f"limit must be an integer from 1 through {MAX_OUTPUT_MAPPINGS}",
        )
    return offset, limit


def _validate_inventory(inventory: object) -> dict[str, str]:
    if not isinstance(inventory, Mapping) or len(inventory) > MAX_INVENTORY_ENTRIES:
        raise HonchoMigrationError(
            "honcho_migration_input_invalid",
            f"inventory must be a JSON object with at most {MAX_INVENTORY_ENTRIES} entries",
        )

    validated: dict[str, str] = {}
    for user_id, workspace in inventory.items():
        if not isinstance(user_id, str) or not user_id or len(user_id.encode("utf-8")) > MAX_RAW_USER_BYTES:
            raise HonchoMigrationError(
                "honcho_migration_input_invalid",
                f"each user identifier must be a non-empty string of at most {MAX_RAW_USER_BYTES} bytes",
            )
        if not isinstance(workspace, str) or not workspace or len(workspace) > HONCHO_ID_MAX_LENGTH or _HONCHO_ID_RE.fullmatch(workspace) is None:
            raise HonchoMigrationError(
                "honcho_migration_input_invalid",
                "each source workspace must satisfy Honcho's identifier grammar and length bound",
            )
        validated[user_id] = workspace
    return validated


def _user_ref(user_id: str) -> str:
    return f"user-{hashlib.sha256(user_id.encode('utf-8')).hexdigest()[:16]}"


def _canonical_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def run_honcho_workspace_migration(
    inventory: Mapping[str, str],
    *,
    tenant_id: str,
    dry_run: bool,
    offset: int = 0,
    limit: int = MAX_OUTPUT_MAPPINGS,
) -> dict[str, Any]:
    """Return one bounded migration-plan page or refuse unsupported writes."""

    if dry_run is not True:
        raise HonchoMigrationError(
            "honcho_provider_copy_required",
            "use Honcho provider tooling for the exact emitted mapping; local writes and dual-read fallback are intentionally unsupported",
        )
    offset, limit = _validate_page(offset=offset, limit=limit)
    validated = _validate_inventory(inventory)
    try:
        tenant_identity = TenantIdentityV1.from_canonical_id(tenant_id)
    except (TypeError, ValueError):
        raise HonchoMigrationError(
            "honcho_migration_input_invalid",
            "tenant_id must satisfy the deployment tenant identity contract",
        ) from None

    projected = project_honcho_backend_config(
        {},
        tenant_identity=tenant_identity,
        deployment_profile="durable_production",
    )
    config = HonchoConfig.from_backend_config(projected)
    resolver = HonchoIdentityResolver(config)

    mappings = [
        {
            "user_ref": _user_ref(user_id),
            "source_workspace": source_workspace,
            "target_workspace": resolver.derived_workspace(user_id),
        }
        for user_id, source_workspace in validated.items()
    ]
    mappings.sort(key=lambda item: item["user_ref"])
    if len({item["user_ref"] for item in mappings}) != len(mappings) or len({item["target_workspace"] for item in mappings}) != len(mappings):
        raise HonchoMigrationError(
            "honcho_identity_collision",
            "the inventory produced a pseudonymous identity collision",
        )

    page = mappings[offset : offset + limit]
    return {
        "version": 1,
        "mode": "dry_run",
        "provider_copy_supported": False,
        "write_count": 0,
        "tenant_public_ref": tenant_identity.public_ref,
        "mapping_digest": _canonical_digest(mappings),
        "total_count": len(mappings),
        "offset": offset,
        "limit": limit,
        "emitted_count": len(page),
        "has_more": offset + len(page) < len(mappings),
        "mappings": page,
        "instruction": "Stop Gateway writes, copy these exact workspace mappings with Honcho provider tooling, validate provider counts, then deploy tenant-derived configuration. DeerFlow will not dual-read legacy workspaces.",
    }


def _read_inventory(path: Path) -> Mapping[str, str]:
    try:
        if path.stat().st_size > MAX_INVENTORY_BYTES:
            raise HonchoMigrationError(
                "honcho_migration_input_invalid",
                f"inventory file exceeds {MAX_INVENTORY_BYTES} bytes",
            )
        value = json.loads(path.read_text(encoding="utf-8"))
    except HonchoMigrationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise HonchoMigrationError(
            "honcho_migration_input_invalid",
            "inventory must be a readable UTF-8 JSON object",
        ) from None
    if not isinstance(value, Mapping):
        raise HonchoMigrationError(
            "honcho_migration_input_invalid",
            "inventory must be a JSON object",
        )
    return value  # type: ignore[return-value]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True, help="JSON object mapping raw user IDs to exact legacy workspace IDs")
    parser.add_argument("--tenant-id", required=True, help="canonical deployment tenant ID (never emitted)")
    parser.add_argument("--dry-run", action="store_true", help="required; this utility has no provider write path")
    parser.add_argument("--offset", type=int, default=0, help="sorted mapping offset")
    parser.add_argument("--limit", type=int, default=MAX_OUTPUT_MAPPINGS, help=f"mapping rows to emit (maximum {MAX_OUTPUT_MAPPINGS})")
    args = parser.parse_args(argv)

    try:
        inventory = _read_inventory(args.inventory)
        report = run_honcho_workspace_migration(
            inventory,
            tenant_id=args.tenant_id,
            dry_run=args.dry_run,
            offset=args.offset,
            limit=args.limit,
        )
    except HonchoMigrationError as exc:
        parser.exit(2, f"{exc}\n")
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
