"""Safe, immutable Capability Host manifest behavior."""

from __future__ import annotations

import asyncio
import subprocess
import sys
from pathlib import Path

from deerflow_extension_api import (
    ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
    CapabilityHealthResult,
    OriginContributorFactory,
)

from deerflow.extensions.capabilities import (
    build_capability_manifest,
    capability_manifest_to_dict,
)
from deerflow.extensions.loader import ExtensionSpec, load_extensions
from deerflow.extensions.registry import ExtensionRegistry
from deerflow.runtime.accepted_invocation import (
    AcceptedInvocation,
    InvocationOrigin,
    PrincipalProjection,
    ResolvedAgentRevision,
    canonical_digest,
)


def test_health_contract_imports_without_host_packages() -> None:
    package_root = Path(__file__).parents[1] / "packages" / "extension-api"
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            (
                f"import sys; sys.path.insert(0, {str(package_root)!r}); "
                "from deerflow_extension_api import CapabilityHealthResult; "
                "assert CapabilityHealthResult(status='healthy').status == 'healthy'; "
                "assert 'deerflow' not in sys.modules; assert 'app' not in sys.modules"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_empty_manifest_is_deterministic_and_bound_to_extension_generation() -> None:
    extensions = ExtensionRegistry().build(generation=7)

    first = build_capability_manifest(extensions)
    second = build_capability_manifest(extensions)
    next_generation = build_capability_manifest(ExtensionRegistry().build(generation=8))

    assert first == second
    assert first.extension_generation == 7
    assert first.extension_api_version == "0.12.0"
    assert first.digest == second.digest
    assert first.digest != next_generation.digest
    assert len(first.digest) == 64
    assert first.plugins == ()
    assert first.capabilities == ()


def test_authoritative_descriptor_health_probe_is_optional_and_typed() -> None:
    async def probe() -> CapabilityHealthResult:
        return CapabilityHealthResult(
            status="unhealthy",
            diagnostic_code="dependency_unavailable",
        )

    defaulted = OriginContributorFactory(
        contribution_id="audit_origin",
        capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
        factory=lambda: object(),
        kind="origin_contributor",
    )
    explicit = OriginContributorFactory(
        contribution_id="policy_origin",
        capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
        factory=lambda: object(),
        kind="origin_contributor",
        health_probe=probe,
    )

    assert defaulted.health_probe is None
    assert asyncio.run(explicit.health_probe()) == CapabilityHealthResult(
        status="unhealthy",
        diagnostic_code="dependency_unavailable",
    )


def test_manifest_uses_loader_owned_provenance_and_safe_initialization_codes(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "deerflow.extensions.loader._distribution_provenance",
        lambda _install: ("example-policy", "3.2.1"),
    )
    loaded, diagnostics = load_extensions(
        [
            ExtensionSpec(
                use="extension_test_fixtures.demo_extensions:install_authorization_provider",
                required=True,
            )
        ]
    )

    manifest = build_capability_manifest(
        loaded,
        authorization_required=True,
        initialized_capability_ids={
            "authorization_provider:fixture.authorization",
        },
    )

    assert diagnostics == []
    assert [(item.package_name, item.package_version) for item in manifest.plugins] == [("example-policy", "3.2.1")]
    assert manifest.plugins[0].load_required is True
    assert len(manifest.capabilities) == 1
    capability = manifest.capabilities[0]
    assert capability.contribution_id == "fixture.authorization"
    assert capability.capability_id == "authorization_provider:fixture.authorization"
    assert capability.capability_type == "authorization_provider"
    assert capability.capability_api_version == "1.0"
    assert capability.operator_required is True
    assert capability.initialization_status == "initialized"
    assert capability.diagnostic_code == "initialized"
    assert "demo_extensions" not in repr(manifest)


def test_loaded_plugin_without_authoritative_registration_remains_visible(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "deerflow.extensions.loader._distribution_provenance",
        lambda _install: ("example-observer", "1.0.0"),
    )
    loaded, diagnostics = load_extensions(
        [
            ExtensionSpec(
                use="extension_test_fixtures.demo_extensions:install_ok",
                required=True,
            )
        ]
    )

    manifest = build_capability_manifest(
        loaded,
        required_capabilities=("origin_contributor:missing_origin",),
    )

    assert diagnostics == []
    assert manifest.plugins[0].package_name == "example-observer"
    assert manifest.plugins[0].load_required is True
    assert manifest.capabilities[0].capability_id == ("origin_contributor:missing_origin")
    assert manifest.capabilities[0].initialization_status == "missing"


def test_manifest_serialization_excludes_source_config_and_high_cardinality_data() -> None:
    registry = ExtensionRegistry()
    with registry.attributed_to(
        "private.module:install?token=secret",
        package_name="example-policy",
        package_version="1.2.3",
    ):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="safe_origin",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=lambda: object(),
                kind="origin_contributor",
            )
        )
    manifest = build_capability_manifest(
        registry.build(generation=9),
        initialized_capability_ids=("origin_contributor:safe_origin",),
    )

    payload = capability_manifest_to_dict(manifest)
    rendered = repr(payload)

    assert payload["manifest_digest"] == manifest.digest
    assert payload["extension_generation"] == 9
    assert "example-policy" in rendered
    assert "private.module" not in rendered
    assert "token=secret" not in rendered
    assert "user_id" not in rendered
    assert "thread_id" not in rendered


def test_accepted_invocation_pins_rollout_generation_and_manifest_digest() -> None:
    first = build_capability_manifest(ExtensionRegistry().build(generation=11))
    replacement = build_capability_manifest(ExtensionRegistry().build(generation=12))

    accepted = AcceptedInvocation.seal(
        principal=PrincipalProjection(user_id="owner-1", role="member"),
        origin=InvocationOrigin(source_kind="http"),
        thread_id="thread-1",
        context_references={},
        agent_revision=ResolvedAgentRevision(
            agent_id="default",
            digest="a" * 64,
            storage_source="file",
            storage_version="v1",
        ),
        normalized_input={"messages": []},
        execution_options={"multitask_strategy": "reject"},
        extension_generation=first.extension_generation,
        extension_manifest_digest=first.digest,
        contributor_execution_digest=canonical_digest({"version": 1, "execution": []}),
    )
    persisted = accepted.to_persisted()

    assert accepted.extension_generation == 11
    assert accepted.extension_manifest_digest == first.digest
    assert persisted["extension_generation"] == 11
    assert persisted["decision_evidence_json"]["capability_manifest"] == {
        "version": 1,
        "generation": 11,
        "digest": first.digest,
    }
    assert accepted.extension_manifest_digest != replacement.digest
