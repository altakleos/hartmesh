"""Deployment truth stays separate from the portable runtime contract."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from _router_auth_helpers import make_authed_test_app
from deerflow_runtime_api import RuntimeCapabilities, record_from_dict
from fastapi.testclient import TestClient

from app.gateway.auth.models import User
from app.gateway.routers import runtime_api
from app.runtime.deployment import (
    DeploymentProfile,
    DeploymentProvenance,
    DeploymentQualification,
    GatewayDeploymentReporter,
    IngressDeliveryGuarantee,
    PersistenceTier,
    describe_native_ingress,
    describe_persistence,
    validate_deployment_profile,
)
from app.runtime.readiness import RuntimeReadinessSnapshot
from deerflow.extensions.capabilities import (
    CapabilityHealthSnapshot,
    build_capability_manifest,
)
from deerflow.extensions.registry import ExtensionRegistry


def _admin_user() -> User:
    return User(
        id=UUID("00000000-0000-0000-0000-000000000001"),
        email="runtime-admin@example.com",
        password_hash="x",
        system_role="admin",
    )


class _CapabilitiesAdapter:
    def capabilities(self) -> RuntimeCapabilities:
        return RuntimeCapabilities()


class _HealthMonitor:
    async def health(self):
        checked_at = datetime(2026, 8, 7, tzinfo=UTC)
        return (
            CapabilityHealthSnapshot(
                contribution_id="safe-policy",
                capability_id="authorization_provider:safe-policy",
                status="healthy",
                diagnostic_code="healthy",
                checked_at=checked_at,
                expires_at=checked_at + timedelta(seconds=10),
            ),
        )


class _Readiness:
    last_snapshot = RuntimeReadinessSnapshot(
        status="not_ready",
        reason_codes=("lifecycle_event_bounds_invalid",),
        checked_at=datetime(2026, 8, 7, tzinfo=UTC),
        correlation_id="a" * 32,
    )


def _reporter(*, backend: str, profile: DeploymentProfile):
    return GatewayDeploymentReporter(
        profile=profile,
        database_backend=backend,
        atomic_lifecycle=True,
        manifest=build_capability_manifest(ExtensionRegistry().build(generation=7)),
        health_monitor=_HealthMonitor(),
        readiness_supplier=lambda: _Readiness.last_snapshot,
        provenance=DeploymentProvenance(
            image_reference="registry.example/hartmesh/gateway:2026.08",
            image_digest="sha256:" + ("a" * 64),
            source_revision="b" * 40,
        ),
    )


def test_production_shaped_capabilities_remain_exactly_portable() -> None:
    app = make_authed_test_app(user_factory=_admin_user)
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: _CapabilitiesAdapter()
    app.state.capability_manifest = build_capability_manifest(ExtensionRegistry().build(generation=7))
    app.state.capability_health_monitor = _HealthMonitor()
    app.state.deployment_reporter = _reporter(
        backend="postgres",
        profile=DeploymentProfile.durable_production,
    )

    with TestClient(app) as client:
        payload = client.get("/api/runtime/v1/capabilities").json()

    assert record_from_dict(payload) == _CapabilitiesAdapter().capabilities()


def test_capabilities_adapter_exception_is_bounded_and_correlated_over_http(caplog) -> None:
    class ExplodingCapabilities:
        def capabilities(self):
            raise RuntimeError("database password=never-return-this")

    app = make_authed_test_app(user_factory=_admin_user)
    app.include_router(runtime_api.router)
    app.dependency_overrides[runtime_api.get_runtime_api] = lambda: ExplodingCapabilities()

    with caplog.at_level(logging.ERROR, logger="app.runtime.api"):
        with TestClient(app) as client:
            response = client.get("/api/runtime/v1/capabilities")

    assert response.status_code == 503
    payload = response.json()
    assert payload["code"] == "indeterminate"
    assert "never-return-this" not in str(payload)
    correlation_id = payload["details"]["correlation_id"]
    matching = [record for record in caplog.records if getattr(record, "correlation_id", None) == correlation_id]
    assert len(matching) == 1
    assert matching[0].runtime_operation == "capabilities"


def test_admin_deployment_report_is_versioned_truthful_and_redacted() -> None:
    app = make_authed_test_app(user_factory=_admin_user)
    app.include_router(runtime_api.router)
    app.state.deployment_reporter = _reporter(
        backend="postgres",
        profile=DeploymentProfile.durable_production,
    )

    with TestClient(app) as client:
        response = client.get("/api/runtime/v1/deployment")

    assert response.status_code == 200
    payload = response.json()
    assert payload["api_version"] == "deerflow.deployment/v1"
    assert payload["kind"] == "runtime.deployment.report"
    assert payload["profile"] == "durable_production"
    assert payload["extension_manifest"]["extension_generation"] == 7
    assert payload["persistence"] == {
        "version": 1,
        "tier": "shared_durable",
        "atomic_lifecycle": True,
        "restart_durable": True,
        "pod_loss_durable": True,
    }
    assert payload["qualification"] == {
        "version": 1,
        "status": "unqualified",
        "trust": "none_declared",
        "evidence": [],
    }
    assert payload["admission_readiness"] == {
        "version": 1,
        "status": "not_ready",
        "reason_codes": ["lifecycle_event_bounds_invalid"],
        "checked_at": "2026-08-07T00:00:00Z",
        "correlation_id": "a" * 32,
    }
    assert payload["provenance"]["image_digest"] == "sha256:" + ("a" * 64)
    serialized = str(payload).lower()
    assert "secret" not in serialized
    assert "exception" not in serialized

    with pytest.raises(ValueError, match="credential"):
        DeploymentProvenance(image_reference="registry.example/user:secret@private/image:latest")


def test_deployment_qualification_reads_bounded_trusted_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DEER_FLOW_QUALIFICATION_EVIDENCE",
        '[{"qualificationId":"durable-contract-2026-08","artifactDigest":"sha256:' + ("c" * 64) + '","completedAt":"2026-08-08T12:00:00Z"}]',
    )

    qualification = DeploymentQualification.from_environment()

    assert qualification.to_dict() == {
        "version": 1,
        "status": "qualified",
        "trust": "operator_asserted",
        "evidence": [
            {
                "qualification_id": "durable-contract-2026-08",
                "scope": "legacy_unspecified",
                "status": "passed",
                "artifact_digest": "sha256:" + ("c" * 64),
                "completed_at": "2026-08-08T12:00:00Z",
            }
        ],
    }


def test_kubernetes_qualification_evidence_declares_bounded_scope_and_pass_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "DEER_FLOW_QUALIFICATION_EVIDENCE",
        '[{"qualificationId":"pod-recovery-20260808","scope":"durable_one_replica_pod_recovery","status":"passed","artifactDigest":"sha256:' + ("d" * 64) + '","completedAt":"2026-08-08T12:00:00Z"}]',
    )

    qualification = DeploymentQualification.from_environment().to_dict()

    assert qualification["status"] == "qualified"
    assert qualification["trust"] == "operator_asserted"
    assert qualification["evidence"][0]["scope"] == "durable_one_replica_pod_recovery"
    assert qualification["evidence"][0]["status"] == "passed"


@pytest.mark.parametrize(
    "completed_at",
    [
        "2026-W32-6T12:00:00Z",
        "2026-08-08 12:00:00+00:00",
        "2026-08-08T12:00:00",
    ],
)
def test_deployment_qualification_rejects_non_rfc3339_timestamps(
    monkeypatch: pytest.MonkeyPatch,
    completed_at: str,
) -> None:
    monkeypatch.setenv(
        "DEER_FLOW_QUALIFICATION_EVIDENCE",
        '[{"qualificationId":"durable-contract-2026-08","artifactDigest":"sha256:' + ("c" * 64) + '","completedAt":"' + completed_at + '"}]',
    )

    with pytest.raises(ValueError, match="qualification evidence values are invalid"):
        DeploymentQualification.from_environment()


def test_deployment_report_requires_an_authenticated_administrator() -> None:
    app = make_authed_test_app()
    app.include_router(runtime_api.router)
    app.state.deployment_reporter = _reporter(
        backend="sqlite",
        profile=DeploymentProfile.local_development,
    )

    with TestClient(app) as client:
        response = client.get("/api/runtime/v1/deployment")

    assert response.status_code == 403
    assert response.json() == {
        "api_version": "deerflow.runtime/v1",
        "kind": "runtime.error",
        "code": "denied",
    }


def test_persistence_tiers_do_not_confuse_atomicity_with_restart_durability() -> None:
    memory = describe_persistence("memory", atomic_lifecycle=True)
    sqlite = describe_persistence("sqlite", atomic_lifecycle=True)
    postgres = describe_persistence("postgres", atomic_lifecycle=True)

    assert memory.tier is PersistenceTier.process_local
    assert memory.atomic_lifecycle is True
    assert memory.restart_durable is False
    assert memory.pod_loss_durable is False
    assert sqlite.tier is PersistenceTier.node_durable
    assert sqlite.restart_durable is True
    assert sqlite.pod_loss_durable is False
    assert postgres.tier is PersistenceTier.shared_durable
    assert postgres.restart_durable is True
    assert postgres.pod_loss_durable is True


def test_process_local_storage_cannot_enter_the_durable_production_profile() -> None:
    config = SimpleNamespace(
        deployment=SimpleNamespace(profile="durable_production"),
        database=SimpleNamespace(backend="memory"),
    )

    try:
        validate_deployment_profile(config)
    except ValueError as exc:
        assert "process-local" in str(exc)
    else:
        raise AssertionError("process-local state must fail the durable production profile")

    reporter = _reporter(
        backend="memory",
        profile=DeploymentProfile.durable_production,
    )
    assert reporter.persistence_ready is False


def test_local_development_profile_explicitly_allows_process_local_storage() -> None:
    config = SimpleNamespace(
        deployment=SimpleNamespace(profile="local_development"),
        database=SimpleNamespace(backend="memory"),
    )

    validate_deployment_profile(config)


def test_native_ingress_report_distinguishes_durable_and_best_effort() -> None:
    def config(*, database: str, receipt_backend: str):
        return SimpleNamespace(
            database=SimpleNamespace(backend=database),
            dedupe_storage=SimpleNamespace(backend=receipt_backend),
            model_extra={"channels": {"github": {"enabled": True}}},
        )

    local = describe_native_ingress(config(database="sqlite", receipt_backend="memory"))
    durable = describe_native_ingress(config(database="postgres", receipt_backend="auto"))

    assert local.sources == (("github", IngressDeliveryGuarantee.best_effort),)
    assert durable.sources == (("github", IngressDeliveryGuarantee.durable),)
    assert local.to_dict()["sources"] == {"github": "best_effort"}
    assert durable.to_dict()["sources"] == {"github": "durable"}


def test_durable_profile_rejects_enabled_source_without_postgres_receipts() -> None:
    config = SimpleNamespace(
        deployment=SimpleNamespace(profile="durable_production"),
        database=SimpleNamespace(backend="sqlite"),
        dedupe_storage=SimpleNamespace(backend="memory"),
        model_extra={"channels": {"github": {"enabled": True}}},
    )

    with pytest.raises(ValueError, match="PostgreSQL inbound receipt storage"):
        validate_deployment_profile(config)
