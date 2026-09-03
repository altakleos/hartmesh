"""Gateway authentication adapters preserve actor/source separation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from deerflow_extension_api import CredentialEvidenceV1, effective_authority_digest_v1

from app.gateway.auth_disabled import (
    AUTH_SOURCE_AUTH_DISABLED,
    AUTH_SOURCE_INTERNAL,
    AUTH_SOURCE_PAT,
    AUTH_SOURCE_SESSION,
)
from app.gateway.credential_evidence import (
    CredentialEvidenceError,
    build_boundary_credential_evidence,
    credential_evidence_for_admission,
)
from app.runtime.invocation import (
    InternalLaunchIntent,
    InternalNativeChannelFacts,
    InternalSourceKind,
)


def test_boundary_adapters_distinguish_session_pat_internal_and_development() -> None:
    now = datetime(2026, 9, 2, tzinfo=UTC)
    session = build_boundary_credential_evidence(
        auth_source=AUTH_SOURCE_SESSION,
        permissions=["runs:create"],
        session_payload=SimpleNamespace(iat=now, exp=now + timedelta(hours=1)),
    )
    pat = build_boundary_credential_evidence(
        auth_source=AUTH_SOURCE_PAT,
        permissions=["runs:create"],
        pat_record={
            "id": "018f2d70-0fca-4f88-b0c3-a0f83ebf2c89",
            "created_at": now,
            "expires_at": None,
        },
    )
    internal = build_boundary_credential_evidence(
        auth_source=AUTH_SOURCE_INTERNAL,
        permissions=["runs:create"],
    )
    development = build_boundary_credential_evidence(
        auth_source=AUTH_SOURCE_AUTH_DISABLED,
        permissions=["runs:create"],
    )

    assert [item.method for item in (session, pat, internal, development)] == [
        "session",
        "personal_access_token",
        "internal_service",
        "development_bypass",
    ]
    assert pat.credential_ref == "018f2d70-0fca-4f88-b0c3-a0f83ebf2c89"
    assert session.credential_ref is None
    assert internal.credential_ref is None


def test_channel_admission_changes_method_not_authority_or_principal_claims() -> None:
    evidence = CredentialEvidenceV1(
        method="internal_service",
        credential_ref=None,
        effective_authority_digest=effective_authority_digest_v1(("runs:create",)),
        authority_categories=("runs",),
    )
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_source=AUTH_SOURCE_INTERNAL,
            auth=SimpleNamespace(permissions=["runs:create"]),
            credential_evidence=evidence,
        )
    )
    intent = InternalLaunchIntent(
        thread_id="thread-1",
        source_kind=InternalSourceKind.native_channel,
        owner_user_id="owner-1",
        native_channel=InternalNativeChannelFacts(
            provider="slack",
            connection_id="connection-1",
            workspace_id="workspace-1",
            chat_id="chat-1",
            topic_id=None,
            provider_message_id="message-1",
            channel_user_id="platform-user",
            resolved_assistant_id="lead_agent",
            resolved_agent_name=None,
        ),
    )

    channel = credential_evidence_for_admission(request, intent)

    assert channel.method == "channel"
    assert channel.effective_authority_digest == evidence.effective_authority_digest
    assert channel.credential_ref is None


def test_admission_rejects_pat_without_server_stamped_evidence() -> None:
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_source=AUTH_SOURCE_PAT,
            auth=SimpleNamespace(permissions=["runs:create"]),
        )
    )
    with pytest.raises(
        CredentialEvidenceError,
        match="credential_evidence_unavailable",
    ):
        credential_evidence_for_admission(
            request,
            InternalLaunchIntent(thread_id="thread-1"),
        )


def test_admission_rejects_authority_changed_after_evidence_projection() -> None:
    evidence = build_boundary_credential_evidence(
        auth_source=AUTH_SOURCE_SESSION,
        permissions=["runs:read"],
    )
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_source=AUTH_SOURCE_SESSION,
            auth=SimpleNamespace(permissions=["runs:create"]),
            credential_evidence=evidence,
        )
    )
    with pytest.raises(CredentialEvidenceError, match="authority_digest_mismatch"):
        credential_evidence_for_admission(
            request,
            InternalLaunchIntent(thread_id="thread-1"),
        )


def test_admission_rejects_coarse_authority_categories_changed_after_projection() -> None:
    evidence = build_boundary_credential_evidence(
        auth_source=AUTH_SOURCE_SESSION,
        permissions=["runs:create"],
    )
    forged = CredentialEvidenceV1(
        method=evidence.method,
        credential_ref=evidence.credential_ref,
        effective_authority_digest=evidence.effective_authority_digest,
        authority_categories=("threads",),
        issued_at=evidence.issued_at,
        expires_at=evidence.expires_at,
    )
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_source=AUTH_SOURCE_SESSION,
            auth=SimpleNamespace(permissions=["runs:create"]),
            credential_evidence=forged,
        )
    )

    with pytest.raises(CredentialEvidenceError, match="authority_digest_mismatch"):
        credential_evidence_for_admission(
            request,
            InternalLaunchIntent(thread_id="thread-1"),
        )


def test_boundary_adapter_rejects_unknown_authority_with_typed_error() -> None:
    with pytest.raises(
        CredentialEvidenceError,
        match="credential_evidence_unavailable",
    ):
        build_boundary_credential_evidence(
            auth_source=AUTH_SOURCE_SESSION,
            permissions=["deployment:admin"],
        )


def test_admission_does_not_invent_evidence_for_unsupported_source() -> None:
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_source="anonymous",
            auth=SimpleNamespace(permissions=["runs:create"]),
        )
    )

    with pytest.raises(
        CredentialEvidenceError,
        match="credential_evidence_unavailable",
    ):
        credential_evidence_for_admission(
            request,
            InternalLaunchIntent(thread_id="thread-1"),
        )


def test_admission_rejects_forged_evidence_for_unsupported_source() -> None:
    evidence = build_boundary_credential_evidence(
        auth_source=AUTH_SOURCE_SESSION,
        permissions=["runs:create"],
    )
    request = SimpleNamespace(
        state=SimpleNamespace(
            auth_source="anonymous",
            auth=SimpleNamespace(permissions=["runs:create"]),
            credential_evidence=evidence,
        )
    )

    with pytest.raises(
        CredentialEvidenceError,
        match="credential_evidence_unavailable",
    ):
        credential_evidence_for_admission(
            request,
            InternalLaunchIntent(thread_id="thread-1"),
        )
