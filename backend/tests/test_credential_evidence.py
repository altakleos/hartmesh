"""Safe, canonical authentication evidence at the accepted-run boundary."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from deerflow_extension_api import (
    ActingServiceV1,
    CredentialEvidenceV1,
    EffectiveSubjectV1,
    InvocationIdentityV1,
    ResolvedAgentRevisionReferenceV1,
    ResolvedProfileRevisionReferenceV1,
    SealedOriginV1,
    TenantReferenceV1,
    TrustedRunContextV1,
    VerifiedActorContextV1,
    canonicalize_authority_v1,
    effective_authority_digest_v1,
)

_ALLOWED = frozenset(
    {
        "runs:cancel",
        "runs:create",
        "runs:read",
        "threads:read",
    }
)


def _tenant() -> TenantReferenceV1:
    return TenantReferenceV1(
        version=1,
        public_ref="tenant-aaaaaaaaaaaaaaaa",
        digest="a" * 64,
    )


def _identity() -> InvocationIdentityV1:
    return InvocationIdentityV1(
        effective_subject=EffectiveSubjectV1(
            kind="human",
            subject_id="user-1",
            role="member",
        ),
        acting_service=ActingServiceV1(service_id="channel:slack"),
    )


def test_authority_is_alias_normalized_sorted_deduplicated_and_digest_stable() -> None:
    aliases = {"run:read": "runs:read", "thread:read": "threads:read"}

    first = canonicalize_authority_v1(
        ["threads:read", "run:read", "runs:read"],
        aliases=aliases,
        allowed=_ALLOWED,
    )
    second = canonicalize_authority_v1(
        ["thread:read", "runs:read"],
        aliases=aliases,
        allowed=_ALLOWED,
    )

    assert first == second == ("runs:read", "threads:read")
    assert effective_authority_digest_v1(first) == effective_authority_digest_v1(second)
    assert effective_authority_digest_v1(("run:read", "thread:read")) == effective_authority_digest_v1(second)


def test_authority_rejects_unknown_or_malformed_identifiers() -> None:
    with pytest.raises(ValueError, match="unknown authority"):
        canonicalize_authority_v1(["admin:everything"], allowed=_ALLOWED)
    with pytest.raises(ValueError, match="authority identifier"):
        canonicalize_authority_v1(["runs:read\nforged"])


def test_credential_evidence_round_trip_is_immutable_bounded_and_secret_free() -> None:
    issued = datetime(2026, 9, 2, 10, 30, tzinfo=UTC)
    authority = ("runs:create", "runs:read")
    credential = CredentialEvidenceV1(
        method="personal_access_token",
        credential_ref="018f2d70-0fca-4f88-b0c3-a0f83ebf2c89",
        effective_authority_digest=effective_authority_digest_v1(authority),
        authority_categories=("runs",),
        issued_at=issued,
        expires_at=issued + timedelta(days=30),
    )

    payload = credential.to_json()
    recovered = CredentialEvidenceV1.from_json(payload)

    assert recovered == credential
    assert recovered.digest == credential.digest
    assert payload["version"] == 1
    assert "runs:create" not in repr(payload)
    assert "dfp_" not in repr(payload)
    assert "token_digest" not in repr(payload)
    assert "effective_authority_digest" in payload

    with pytest.raises(ValueError, match="credential_ref"):
        replace(credential, credential_ref="dfp_secret-looking-reference")


def test_pat_evidence_requires_uuid4_public_reference() -> None:
    digest = effective_authority_digest_v1(("runs:read",))
    with pytest.raises(ValueError, match="UUID4"):
        CredentialEvidenceV1(
            method="personal_access_token",
            credential_ref="not-random",
            effective_authority_digest=digest,
        )
    with pytest.raises(ValueError, match="credential_ref"):
        CredentialEvidenceV1(
            method="personal_access_token",
            credential_ref=None,
            effective_authority_digest=digest,
        )


def test_verified_actor_composes_existing_identity_and_tenant_without_duplication() -> None:
    credential = CredentialEvidenceV1(
        method="channel",
        credential_ref=None,
        effective_authority_digest=effective_authority_digest_v1(("runs:create",)),
        authority_categories=("runs",),
    )
    actor = VerifiedActorContextV1(
        identity=_identity(),
        credential=credential,
        tenant=_tenant(),
    )

    recovered = VerifiedActorContextV1.from_json(actor.to_json())

    assert recovered == actor
    assert recovered.identity.acting_service is not None
    assert recovered.identity.acting_service.service_id == "channel:slack"
    assert recovered.credential.method == "channel"
    assert recovered.tenant == _tenant()
    assert len(actor.digest) == 64


def test_pat_actor_cannot_be_relabelled_as_an_acting_service() -> None:
    credential = CredentialEvidenceV1(
        method="personal_access_token",
        credential_ref="018f2d70-0fca-4f88-b0c3-a0f83ebf2c89",
        effective_authority_digest=effective_authority_digest_v1(("runs:create",)),
        authority_categories=("runs",),
    )

    with pytest.raises(ValueError, match="PAT is a user credential"):
        VerifiedActorContextV1(
            identity=_identity(),
            credential=credential,
            tenant=_tenant(),
        )
    with pytest.raises(ValueError, match="PAT is a user credential"):
        _trusted_context(credential)


def _trusted_context(credential: CredentialEvidenceV1 | None) -> TrustedRunContextV1:
    return TrustedRunContextV1(
        identity=_identity(),
        credential=credential,
        tenant=_tenant(),
        origin=SealedOriginV1(source_kind="native_channel", digest="b" * 64),
        thread_id="thread-1",
        external_key_reference=None,
        agent_revision=ResolvedAgentRevisionReferenceV1(
            agent_id="lead-agent",
            digest="c" * 64,
        ),
        profile_revision=ResolvedProfileRevisionReferenceV1(
            profile_id="default",
            digest="d" * 64,
        ),
        extension_generation=1,
        extension_manifest_digest="e" * 64,
    )


def test_trusted_run_context_v4_binds_credential_and_reads_legacy_contexts() -> None:
    credential = CredentialEvidenceV1(
        method="channel",
        credential_ref=None,
        effective_authority_digest=effective_authority_digest_v1(("runs:create",)),
        authority_categories=("runs",),
    )
    trusted = _trusted_context(credential)

    persisted = trusted.to_persisted_json()
    recovered = TrustedRunContextV1.from_persisted_json(persisted)

    assert persisted["version"] == 4
    assert persisted["credential"] == credential.to_json()
    assert recovered.credential == credential
    assert recovered.verified_actor == VerifiedActorContextV1(
        identity=trusted.identity,
        credential=credential,
        tenant=_tenant(),
    )
    assert recovered.digest == trusted.digest

    different = replace(
        trusted,
        credential=replace(
            credential,
            effective_authority_digest=effective_authority_digest_v1(
                ("runs:create", "runs:cancel"),
            ),
        ),
    )
    assert different.digest != trusted.digest
    assert different.execution_digest != trusted.execution_digest

    legacy = _trusted_context(None)
    legacy_payload = legacy.to_persisted_json()
    assert legacy_payload["version"] == 2
    assert "credential" not in legacy_payload
    assert TrustedRunContextV1.from_persisted_json(legacy_payload).credential is None
