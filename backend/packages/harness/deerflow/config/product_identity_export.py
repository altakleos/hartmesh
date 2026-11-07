"""Operator-only export of the admitted public identity for offline readers.

Run using the Gateway's configuration/environment, before admitting its matching
report runtimes. This is data export, not activation or authorization.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys

from deerflow.config.product_identity import identity_for


def offline_product_identity(config, *, include_default: bool = False) -> dict:
    identity = identity_for(config)
    if identity["mode"] != "configured" and not include_default:
        raise ValueError("An offline projection requires a configured public product identity")
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    return {"schema_version": 1, "identity": identity, "sha256": hashlib.sha256(canonical).hexdigest()}


def main() -> int:
    from deerflow.config import get_app_config

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-default", action="store_true", help="Export explicit default identity binding without inventing provider attribution")
    args = parser.parse_args()
    try:
        record = offline_product_identity(get_app_config(), include_default=args.include_default)
    except (OSError, ValueError):
        sys.stderr.write("Public product identity export refused; verify the Gateway configuration and payload.\n")
        return 1
    sys.stdout.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
