"""Host-owned GitHub webhook authentication classification.

The deployment report and request router consume this Module so neither can
infer durable ingress from storage while ignoring source authentication.
Secret material remains process-local and is excluded from representation.
"""

from __future__ import annotations

import hashlib
import hmac
import os
from dataclasses import dataclass, field
from enum import StrEnum

GITHUB_WEBHOOK_SECRET_ENV = "GITHUB_WEBHOOK_SECRET"
ALLOW_UNVERIFIED_GITHUB_WEBHOOKS_ENV = "DEER_FLOW_ALLOW_UNVERIFIED_GITHUB_WEBHOOKS"

_LOCAL_DEVELOPMENT_PROFILE = "local_development"
_TRUTHY = frozenset({"1", "true", "yes", "on"})


class GitHubWebhookAuthMode(StrEnum):
    """The current server-owned trust mode for GitHub webhook requests."""

    disabled = "disabled"
    unverified_development = "unverified_development"
    hmac_sha256_verified = "hmac_sha256_verified"


@dataclass(frozen=True, slots=True)
class GitHubWebhookAuth:
    """One immutable environment snapshot used to authenticate a request."""

    mode: GitHubWebhookAuthMode
    secret: str | None = field(default=None, repr=False, compare=False)

    def __post_init__(self) -> None:
        has_secret = self.secret is not None
        if has_secret is not (self.mode is GitHubWebhookAuthMode.hmac_sha256_verified):
            raise ValueError("GitHub webhook secret must exist only in verified mode")

    @property
    def route_enabled(self) -> bool:
        """Whether this startup profile may mount the webhook route."""

        return self.mode is not GitHubWebhookAuthMode.disabled

    def verify(self, body: bytes, signature_header: str | None) -> bool:
        """Verify one signature without exposing retained secret material."""

        if self.secret is None or not signature_header:
            return False
        prefix = "sha256="
        if not signature_header.startswith(prefix):
            return False
        supplied = signature_header[len(prefix) :].strip()
        if len(supplied) != 64:
            return False
        try:
            int(supplied, 16)
        except ValueError:
            return False
        expected = hmac.new(
            self.secret.encode("utf-8"),
            body,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(expected, supplied.lower())


def resolve_github_webhook_auth(
    *,
    deployment_profile: object = _LOCAL_DEVELOPMENT_PROFILE,
) -> GitHubWebhookAuth:
    """Resolve current authentication with verified-secret precedence.

    Unverified delivery is an explicit local-development convenience. A durable
    production process never falls through to it when a secret is absent or is
    removed during rotation.
    """

    raw_secret = os.environ.get(GITHUB_WEBHOOK_SECRET_ENV)
    secret = raw_secret.strip() if raw_secret is not None else ""
    if secret:
        return GitHubWebhookAuth(
            mode=GitHubWebhookAuthMode.hmac_sha256_verified,
            secret=secret,
        )

    raw_profile = getattr(deployment_profile, "value", deployment_profile)
    allow_unverified = isinstance(raw_profile, str) and raw_profile == _LOCAL_DEVELOPMENT_PROFILE and os.environ.get(ALLOW_UNVERIFIED_GITHUB_WEBHOOKS_ENV, "").strip().lower() in _TRUTHY
    if allow_unverified:
        return GitHubWebhookAuth(mode=GitHubWebhookAuthMode.unverified_development)
    return GitHubWebhookAuth(mode=GitHubWebhookAuthMode.disabled)


__all__ = [
    "ALLOW_UNVERIFIED_GITHUB_WEBHOOKS_ENV",
    "GITHUB_WEBHOOK_SECRET_ENV",
    "GitHubWebhookAuth",
    "GitHubWebhookAuthMode",
    "resolve_github_webhook_auth",
]
