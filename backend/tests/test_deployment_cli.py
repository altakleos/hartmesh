from __future__ import annotations

import json

import pytest

from deerflow.deployment import cli


def test_bind_tenant_requires_explicit_nonempty_schema_acknowledgement() -> None:
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(["bind-tenant", "--tenant-id", "tenant-a", "--dry-run"])


def test_bind_tenant_emits_only_safe_identity_projection(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    async def fake_bind_tenant(
        *,
        tenant_id: str,
        dry_run: bool,
        legacy_stream_bridge_prefix: str | None,
        legacy_checkpoint_cache_prefix: str | None,
        legacy_sandbox_ownership_prefix: str | None,
    ):
        assert tenant_id == "customer-readable-name"
        assert dry_run is True
        assert legacy_stream_bridge_prefix == "legacy:stream"
        assert legacy_checkpoint_cache_prefix is None
        assert legacy_sandbox_ownership_prefix is None
        return {
            "action": "would_bind_nonempty_schema",
            "dry_run": True,
            "identity_version": 1,
            "tenant_ref": "tenant-d25d6d3e435cafee",
            "tenant_digest": "d25d6d3e435cafee9cbb0925350695cf31a9f2316658a580babf91f06bf1a6d9",
        }

    monkeypatch.setattr(cli, "_bind_tenant", fake_bind_tenant)

    assert (
        cli.main(
            [
                "bind-tenant",
                "--tenant-id",
                "customer-readable-name",
                "--expected-nonempty-schema",
                "--dry-run",
                "--legacy-stream-bridge-prefix",
                "legacy:stream",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["tenant_ref"] == "tenant-d25d6d3e435cafee"
    assert "customer-readable-name" not in str(payload)


def test_top_level_cli_dispatches_deployment_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from deerflow.tui import cli as top_level

    monkeypatch.setattr(cli, "main", lambda argv: 17)

    assert top_level.main(["deployment", "bind-tenant"]) == 17
