"""GitHub ingress authentication has one host-owned classification."""

from __future__ import annotations

import hashlib
import hmac

import pytest

from app.gateway.github.webhook_auth import (
    GitHubWebhookAuth,
    GitHubWebhookAuthMode,
    resolve_github_webhook_auth,
)


@pytest.mark.parametrize("truthy", ["1", "true", "YES", " on "])
def test_local_development_explicitly_classifies_unverified_webhooks(
    monkeypatch: pytest.MonkeyPatch,
    truthy: str,
) -> None:
    monkeypatch.delenv("GITHUB_WEBHOOK_SECRET", raising=False)
    monkeypatch.setenv("DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS", truthy)

    resolved = resolve_github_webhook_auth(deployment_profile="local_development")

    assert resolved.mode is GitHubWebhookAuthMode.unverified_development
    assert resolved.secret is None


def test_nonblank_secret_is_verified_and_wins_over_development_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "  rotate-me  ")
    monkeypatch.setenv("DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS", "1")

    resolved = resolve_github_webhook_auth(deployment_profile="durable_production")

    assert resolved.mode is GitHubWebhookAuthMode.hmac_sha256_verified
    assert resolved.secret == "rotate-me"
    assert "rotate-me" not in repr(resolved)


@pytest.mark.parametrize("dev_value", [None, "", "0", "false", "off", "sometimes"])
def test_missing_secret_without_exact_local_opt_in_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
    dev_value: str | None,
) -> None:
    monkeypatch.setenv("GITHUB_WEBHOOK_SECRET", "   ")
    if dev_value is None:
        monkeypatch.delenv("DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS", raising=False)
    else:
        monkeypatch.setenv("DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS", dev_value)

    resolved = resolve_github_webhook_auth(deployment_profile="local_development")

    assert resolved.mode is GitHubWebhookAuthMode.disabled
    assert resolved.secret is None


def test_durable_profile_never_falls_through_to_unverified_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GITHUB_WEBHOOK_SECRET", raising=False)
    monkeypatch.setenv("DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS", "1")

    resolved = resolve_github_webhook_auth(deployment_profile="durable_production")

    assert resolved.mode is GitHubWebhookAuthMode.disabled
    assert resolved.secret is None


def test_verified_mode_preserves_whitespace_tolerant_digest_compatibility() -> None:
    authentication = GitHubWebhookAuth(
        mode=GitHubWebhookAuthMode.hmac_sha256_verified,
        secret="secret",
    )
    body = b"payload"
    digest = hmac.new(b"secret", body, hashlib.sha256).hexdigest()

    assert authentication.verify(body, f"sha256= {digest} ") is True
