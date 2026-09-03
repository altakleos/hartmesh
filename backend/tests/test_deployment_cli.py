from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from deerflow.deployment import cli
from deerflow.runtime.tenant_identity import TenantIdentityV1


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


@pytest.mark.asyncio
async def test_bind_tenant_recovers_the_credential_migration_precondition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The new CLI can bind a populated schema stopped by migration 0033."""

    config = SimpleNamespace(database=SimpleNamespace(backend="sqlite"))
    monkeypatch.setattr(
        "deerflow.config.get_app_config",
        lambda: config,
    )
    initialize = AsyncMock(
        side_effect=[
            RuntimeError("credential_tenant_binding_required"),
            None,
        ]
    )
    close = AsyncMock()
    session_factory = object()
    monkeypatch.setattr(
        "deerflow.persistence.engine.init_engine_from_config",
        initialize,
    )
    monkeypatch.setattr(
        "deerflow.persistence.engine.get_session_factory",
        lambda: session_factory,
    )
    monkeypatch.setattr(
        "deerflow.persistence.engine.close_engine",
        close,
    )
    tenant = TenantIdentityV1.from_canonical_id("tenant-a").to_persisted_reference()
    bind = AsyncMock(
        return_value=SimpleNamespace(
            action=SimpleNamespace(value="bound_nonempty_schema"),
            tenant=tenant,
            legacy_redis_prefixes=SimpleNamespace(
                stream_bridge=None,
                checkpoint_cache=None,
                sandbox_ownership=None,
            ),
        )
    )
    monkeypatch.setattr(
        "deerflow.persistence.tenant_binding.ensure_schema_tenant_binding",
        bind,
    )

    result = await cli._bind_tenant(
        tenant_id="tenant-a",
        dry_run=False,
        legacy_stream_bridge_prefix=None,
        legacy_checkpoint_cache_prefix=None,
        legacy_sandbox_ownership_prefix=None,
    )

    assert result["action"] == "bound_nonempty_schema"
    assert initialize.await_count == 2
    assert close.await_count == 2
    bind.assert_awaited_once()
