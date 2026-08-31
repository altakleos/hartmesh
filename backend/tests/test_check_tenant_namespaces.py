from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parents[2] / "scripts/check_tenant_namespaces.py"
_SPEC = importlib.util.spec_from_file_location(
    "check_tenant_namespaces_script",
    _SCRIPT,
)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
build_parser = _MODULE.build_parser
inventory = _MODULE.inventory


class _FakeRedis:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str, int]] = []

    def scan(self, *, cursor: int, match: str, count: int):
        self.calls.append((cursor, match, count))
        return 0, [b"secret-key-name"]


def test_command_requires_explicit_dry_run_flag() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--tenant-id", "tenant-a"])


def test_inventory_is_bounded_and_never_returns_key_values() -> None:
    fake = _FakeRedis()

    result = inventory(
        fake,
        tenant_id="tenant-a",
        scan_count=25,
        max_scan_iterations=3,
    )

    assert result["dry_run"] is True
    assert len(fake.calls) == 4
    assert all(call[2] == 25 for call in fake.calls)
    assert "secret-key-name" not in str(result)
    assert [family["key_count"] for family in result["key_families"]] == [
        1,
        1,
        1,
        1,
    ]
