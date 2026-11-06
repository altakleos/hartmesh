"""Operator-only export of the admitted public identity for offline readers.

Run using the Gateway's configuration/environment, before admitting its matching
report runtimes. This is data export, not activation or authorization.
"""

from __future__ import annotations

import hashlib
import json
import sys

from deerflow.config.product_identity import identity_for


def offline_product_identity(config) -> dict:
    identity = identity_for(config)
    if identity["mode"] != "configured":
        raise ValueError("An offline projection requires a configured public product identity")
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    return {"schema_version": 1, "identity": identity, "sha256": hashlib.sha256(canonical).hexdigest()}


def main() -> int:
    from deerflow.config import get_app_config

    try:
        record = offline_product_identity(get_app_config())
    except (OSError, ValueError):
        sys.stderr.write("Public product identity export refused; verify the Gateway configuration and payload.\n")
        return 1
    sys.stdout.write(json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
