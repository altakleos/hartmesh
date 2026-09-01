"""Safe planning contract for the operator-driven Honcho migration."""

from __future__ import annotations

import pytest

from scripts.plan_honcho_workspace_migration import (
    HonchoMigrationError,
    run_honcho_workspace_migration,
)


def test_dry_run_emits_only_a_bounded_pseudonymous_mapping() -> None:
    inventory = {
        "alice@example.internal": "legacy-workspace-a",
        "bob@example.internal": "legacy-workspace-b",
    }

    report = run_honcho_workspace_migration(
        inventory,
        tenant_id="customer-production",
        dry_run=True,
        offset=0,
        limit=1,
    )

    assert report["mode"] == "dry_run"
    assert report["provider_copy_supported"] is False
    assert report["total_count"] == 2
    assert report["emitted_count"] == 1
    assert report["has_more"] is True
    assert report["write_count"] == 0
    assert len(report["mappings"]) == 1
    assert set(report["mappings"][0]) == {
        "user_ref",
        "source_workspace",
        "source_user_peer",
        "source_assistant_peer",
        "target_workspace",
        "target_user_peer",
        "target_assistant_peer",
    }
    assert report["mappings"][0]["source_user_peer"] != report["mappings"][0]["target_user_peer"]
    assert report["mappings"][0]["source_assistant_peer"] != report["mappings"][0]["target_assistant_peer"]
    rendered = repr(report)
    assert "alice@example.internal" not in rendered
    assert "bob@example.internal" not in rendered
    assert "customer-production" not in rendered
    assert "api_key" not in rendered
    assert "content" not in rendered


def test_non_dry_run_stops_with_provider_tooling_instruction() -> None:
    with pytest.raises(
        HonchoMigrationError,
        match="honcho_provider_copy_required.*provider tooling.*dual-read",
    ):
        run_honcho_workspace_migration(
            {"alice": "legacy-workspace-a"},
            tenant_id="customer-production",
            dry_run=False,
        )


def test_dry_run_preserves_exact_legacy_peer_overrides_for_provider_copy() -> None:
    report = run_honcho_workspace_migration(
        {
            "alice@example.internal": {
                "workspace": "legacy-workspace-a",
                "user_peer": "legacy-user-peer-a",
                "assistant_peer": "legacy-assistant",
            }
        },
        tenant_id="customer-production",
        dry_run=True,
    )

    mapping = report["mappings"][0]
    assert mapping["source_workspace"] == "legacy-workspace-a"
    assert mapping["source_user_peer"] == "legacy-user-peer-a"
    assert mapping["source_assistant_peer"] == "legacy-assistant"
    assert mapping["target_user_peer"] != mapping["source_user_peer"]
    assert "peer associations" in report["instruction"]


@pytest.mark.parametrize("limit", [0, 101, True])
def test_output_limit_is_strictly_bounded(limit: object) -> None:
    with pytest.raises(HonchoMigrationError, match="honcho_migration_input_invalid"):
        run_honcho_workspace_migration(
            {"alice": "legacy-workspace-a"},
            tenant_id="customer-production",
            dry_run=True,
            limit=limit,  # type: ignore[arg-type]
        )
