"""Public invocation-constraint contracts and Capability Host ownership."""

from __future__ import annotations

import dataclasses
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from deerflow_extension_api import (
    INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
    INVOCATION_CONSTRAINTS_KIND,
    ConstraintIndeterminate,
    ConstraintProjectionRequestV1,
    ConstraintProjectionV1,
    ConstraintRejected,
    InvocationConstraintsProvider,
    InvocationConstraintsProviderFactory,
)

from deerflow.extensions.constraints import (
    ConstraintStartupError,
    InvocationConstraintsHost,
)
from deerflow.extensions.registry import ExtensionRegistry

_DIGEST_A = "a" * 64
_DIGEST_B = "b" * 64


class _StaticProvider:
    async def project(self, request: ConstraintProjectionRequestV1):
        now = datetime(2026, 8, 7, 12, tzinfo=UTC)
        return ConstraintProjectionV1(
            request_digest=request.request_digest,
            agent_revision_digest=request.agent_revision_digest,
            projection_revision="policy-7",
            issued_at=now,
            valid_until=now + timedelta(minutes=5),
            evidence_id="evidence-7",
            evidence_digest="c" * 64,
            max_total_subagents=3,
        )


def _projection(**overrides) -> ConstraintProjectionV1:
    now = datetime(2026, 8, 7, 12, tzinfo=UTC)
    values = {
        "request_digest": _DIGEST_A,
        "agent_revision_digest": _DIGEST_B,
        "projection_revision": "policy-7",
        "issued_at": now,
        "valid_until": now + timedelta(minutes=5),
        "evidence_id": "evidence-7",
        "evidence_digest": "c" * 64,
        "max_total_subagents": 3,
    }
    values.update(overrides)
    return ConstraintProjectionV1(**values)


def test_contract_imports_without_the_harness() -> None:
    package_root = Path(__file__).parents[1] / "packages" / "extension-api"
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            (
                f"import sys; sys.path.insert(0, {str(package_root)!r}); "
                "from deerflow_extension_api import ("
                "ConstraintProjectionV1, ConstraintProjectionRequestV2, "
                "ConstraintProjectionV2, InvocationConstraintsProvider, "
                "InvocationConstraintsProviderV2); "
                "assert 'deerflow' not in sys.modules"
            ),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_projection_has_exact_v1_fields_and_union_is_structural() -> None:
    expected = {
        "request_digest",
        "agent_revision_digest",
        "projection_revision",
        "issued_at",
        "valid_until",
        "evidence_id",
        "evidence_digest",
        "max_total_subagents",
    }
    assert {field.name for field in dataclasses.fields(ConstraintProjectionV1)} == expected
    assert dataclasses.is_dataclass(ConstraintRejected)
    assert dataclasses.is_dataclass(ConstraintIndeterminate)
    assert isinstance(_StaticProvider(), InvocationConstraintsProvider)


def test_factory_is_typed_versioned_and_singular() -> None:
    provider = _StaticProvider()
    factory = InvocationConstraintsProviderFactory(
        contribution_id="static-constraints",
        capability_api_version=INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
        factory=lambda: provider,
        kind=INVOCATION_CONSTRAINTS_KIND,
    )
    assert factory.factory() is provider


@pytest.mark.parametrize(
    "overrides",
    [
        {"request_digest": "not-a-digest"},
        {"agent_revision_digest": "A" * 64},
        {"evidence_digest": "z" * 64},
        {"issued_at": datetime(2026, 8, 7, 12)},
        {"valid_until": datetime(2026, 8, 7, 12)},
        {"valid_until": datetime(2026, 8, 7, 12, tzinfo=UTC)},
        {"max_total_subagents": 0},
        {"max_total_subagents": -1},
        {"max_total_subagents": True},
    ],
)
def test_projection_rejects_malformed_or_impossible_values(overrides: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        _projection(**overrides)


def test_projection_rejects_unknown_token_or_runtime_fields() -> None:
    with pytest.raises(TypeError):
        _projection(max_total_tokens=10)


def test_request_is_strict_and_binds_the_two_digests() -> None:
    request = ConstraintProjectionRequestV1(
        request_digest=_DIGEST_A,
        agent_revision_digest=_DIGEST_B,
    )
    assert request.request_digest == _DIGEST_A
    assert request.agent_revision_digest == _DIGEST_B
    with pytest.raises(TypeError):
        ConstraintProjectionRequestV1(
            request_digest=_DIGEST_A,
            agent_revision_digest=_DIGEST_B,
            token_budget=10,
        )


def _registry(provider_factory=lambda: _StaticProvider()) -> ExtensionRegistry:
    registry = ExtensionRegistry()
    with registry.attributed_to(
        "constraints_demo:install",
        package_name="constraints-demo",
        package_version="1.2.3",
    ):
        registry.invocation_constraints(
            InvocationConstraintsProviderFactory(
                contribution_id="static-constraints",
                capability_api_version=INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
                factory=provider_factory,
                kind=INVOCATION_CONSTRAINTS_KIND,
            )
        )
    return registry


def test_registry_stamps_provenance_rejects_duplicates_and_rolls_back() -> None:
    registry = _registry()
    registered = registry.build().invocation_constraints_provider_factory
    assert registered is not None
    assert registered.source == "constraints_demo:install"
    assert registered.package_name == "constraints-demo"
    assert registered.package_version == "1.2.3"

    with registry.attributed_to("duplicate:install"):
        with pytest.raises(ValueError, match="already registered"):
            registry.invocation_constraints(
                InvocationConstraintsProviderFactory(
                    contribution_id="duplicate",
                    capability_api_version=INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
                    factory=_StaticProvider,
                    kind=INVOCATION_CONSTRAINTS_KIND,
                )
            )

    empty = ExtensionRegistry()
    mark = empty.mark()
    with empty.attributed_to("partial:install"):
        empty.invocation_constraints(
            InvocationConstraintsProviderFactory(
                contribution_id="partial",
                capability_api_version=INVOCATION_CONSTRAINTS_CAPABILITY_API_VERSION,
                factory=_StaticProvider,
                kind=INVOCATION_CONSTRAINTS_KIND,
            )
        )
    empty.rollback_to(mark)
    assert empty.build().invocation_constraints_provider_factory is None


def test_required_missing_or_failed_provider_fails_startup() -> None:
    with pytest.raises(ConstraintStartupError, match="invocation_constraints.v1"):
        InvocationConstraintsHost(
            ExtensionRegistry().build(),
            required_capabilities=("invocation_constraints.v1",),
        )

    def broken_factory():
        raise RuntimeError("must-not-leak")

    with pytest.raises(ConstraintStartupError, match="RuntimeError"):
        InvocationConstraintsHost(
            _registry(broken_factory).build(),
            required_capabilities=("invocation_constraints.v1",),
        )


@pytest.mark.asyncio
async def test_optional_absence_and_initialization_failure_have_no_projection() -> None:
    request = ConstraintProjectionRequestV1(_DIGEST_A, _DIGEST_B)
    absent = InvocationConstraintsHost(ExtensionRegistry().build())
    assert await absent.project(request, host_max_total_subagents=6) is None

    def broken_factory():
        raise RuntimeError("secret-like-message")

    broken = InvocationConstraintsHost(_registry(broken_factory).build())
    assert await broken.project(request, host_max_total_subagents=6) is None
    assert broken.startup_diagnostics == ("RuntimeError",)


@pytest.mark.asyncio
async def test_host_uses_injected_clock_and_narrows_without_widening() -> None:
    now = datetime(2026, 8, 7, 12, tzinfo=UTC)
    host = InvocationConstraintsHost(_registry().build(), clock=lambda: now)
    result = await host.project(
        ConstraintProjectionRequestV1(_DIGEST_A, _DIGEST_B),
        host_max_total_subagents=2,
    )
    assert isinstance(result, ConstraintProjectionV1)
    assert result.max_total_subagents == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "projection",
    [
        _projection(request_digest="d" * 64),
        _projection(agent_revision_digest="e" * 64),
        _projection(issued_at=datetime(2026, 8, 7, 12, 0, 31, tzinfo=UTC)),
        _projection(
            issued_at=datetime(2026, 8, 7, 11, 59, tzinfo=UTC),
            valid_until=datetime(2026, 8, 7, 12, tzinfo=UTC),
        ),
    ],
)
async def test_host_maps_mismatch_future_skew_and_expiry_to_indeterminate(
    projection: ConstraintProjectionV1,
) -> None:
    class _Provider:
        async def project(self, _request):
            return projection

    host = InvocationConstraintsHost(
        _registry(_Provider).build(),
        clock=lambda: datetime(2026, 8, 7, 12, tzinfo=UTC),
    )
    result = await host.project(
        ConstraintProjectionRequestV1(_DIGEST_A, _DIGEST_B),
        host_max_total_subagents=6,
    )
    assert isinstance(result, ConstraintIndeterminate)


@pytest.mark.asyncio
async def test_host_preserves_rejection_and_bounds_timeout() -> None:
    class _Reject:
        async def project(self, _request):
            return ConstraintRejected()

    rejected = await InvocationConstraintsHost(_registry(_Reject).build()).project(
        ConstraintProjectionRequestV1(_DIGEST_A, _DIGEST_B),
        host_max_total_subagents=6,
    )
    assert isinstance(rejected, ConstraintRejected)

    class _Slow:
        async def project(self, _request):
            await __import__("asyncio").Event().wait()

    timed_out = await InvocationConstraintsHost(
        _registry(_Slow).build(),
        timeout_seconds=0.01,
    ).project(
        ConstraintProjectionRequestV1(_DIGEST_A, _DIGEST_B),
        host_max_total_subagents=6,
    )
    assert isinstance(timed_out, ConstraintIndeterminate)


@pytest.mark.asyncio
async def test_host_rejects_a_ceiling_the_active_runtime_cannot_enforce() -> None:
    result = await InvocationConstraintsHost(_registry().build()).project(
        ConstraintProjectionRequestV1(_DIGEST_A, _DIGEST_B),
        host_max_total_subagents=None,
        runtime_enforceable=False,
    )
    assert isinstance(result, ConstraintIndeterminate)
