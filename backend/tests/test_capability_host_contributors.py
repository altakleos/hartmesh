"""Capability Host contracts and deterministic invocation-context composition."""

from __future__ import annotations

import asyncio

import pytest
from deerflow_extension_api import (
    ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
    ORIGIN_CONTRIBUTOR_KIND,
    RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION,
    RUN_CONTEXT_CONTRIBUTOR_KIND,
    OriginContributionRequestV1,
    OriginContributionV1,
    OriginContributor,
    OriginContributorFactory,
    RunContextContributionRequestV1,
    RunContextContributionV1,
    RunContextContributor,
    RunContextContributorFactory,
    SafeContextReferenceV1,
)

from deerflow.extensions.contributors import (
    ContributorHost,
    ContributorIndeterminateError,
    RequiredCapabilityError,
)
from deerflow.extensions.registry import ExtensionRegistry


class _Origin:
    async def contribute(self, request: OriginContributionRequestV1) -> OriginContributionV1:
        await asyncio.sleep(0)
        return OriginContributionV1(
            namespace="demo",
            references=(
                SafeContextReferenceV1(
                    key="delivery_id",
                    value=request.source_kind,
                    storage_class="persistable",
                    purpose="correlation",
                ),
            ),
        )


class _Context:
    async def contribute(self, request: RunContextContributionRequestV1) -> RunContextContributionV1:
        await asyncio.sleep(0)
        return RunContextContributionV1(namespace="demo", references=())


def test_contributor_contracts_round_trip_and_are_structural() -> None:
    origin = _Origin()
    context = _Context()
    assert isinstance(origin, OriginContributor)
    assert isinstance(context, RunContextContributor)

    origin_factory = OriginContributorFactory(
        contribution_id="demo-origin",
        capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
        factory=lambda: origin,
        kind=ORIGIN_CONTRIBUTOR_KIND,
    )
    context_factory = RunContextContributorFactory(
        contribution_id="demo-context",
        capability_api_version=RUN_CONTEXT_CONTRIBUTOR_CAPABILITY_API_VERSION,
        factory=lambda: context,
        kind=RUN_CONTEXT_CONTRIBUTOR_KIND,
    )

    assert origin_factory.factory() is origin
    assert context_factory.factory() is context


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("bad key", "ok"),
        ("x" * 65, "ok"),
        ("ok", {"nested": "map"}),
        ("ok", "x" * 1025),
        ("ok", float("nan")),
    ],
)
def test_safe_reference_rejects_unsafe_or_oversized_values(key: str, value: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        SafeContextReferenceV1(
            key=key,
            value=value,
            storage_class="persistable",
            purpose="execution",
        )


def test_contribution_rejects_duplicate_keys_and_oversized_inventory() -> None:
    ref = SafeContextReferenceV1(
        key="same",
        value=True,
        storage_class="persistable",
        purpose="execution",
    )
    with pytest.raises(ValueError, match="duplicate"):
        OriginContributionV1(namespace="demo", references=(ref, ref))

    with pytest.raises(ValueError, match="32"):
        RunContextContributionV1(
            namespace="demo",
            references=tuple(
                SafeContextReferenceV1(
                    key=f"k_{index}",
                    value=index,
                    storage_class="persistable",
                    purpose="execution",
                )
                for index in range(33)
            ),
        )


def test_registry_stamps_provenance_and_rejects_duplicate_ids() -> None:
    registry = ExtensionRegistry()
    factory = OriginContributorFactory(
        contribution_id="demo-origin",
        capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
        factory=_Origin,
        kind=ORIGIN_CONTRIBUTOR_KIND,
    )
    with registry.attributed_to(
        "demo:install",
        package_name="deerflow-demo",
        package_version="1.2.3",
    ):
        registry.origin_contributor(factory)
        with pytest.raises(ValueError, match="duplicate"):
            registry.origin_contributor(factory)

    registered = registry.build().origin_contributor_factories[0]
    assert registered.source == "demo:install"
    assert registered.package_name == "deerflow-demo"
    assert registered.package_version == "1.2.3"


@pytest.mark.asyncio
async def test_host_composes_concurrently_in_contribution_id_order_and_redacts_runtime_only() -> None:
    release = asyncio.Event()

    class _Ordered:
        def __init__(self, value: str) -> None:
            self.value = value

        async def contribute(self, request: OriginContributionRequestV1) -> OriginContributionV1:
            await release.wait()
            return OriginContributionV1(
                namespace=self.value,
                references=(
                    SafeContextReferenceV1(
                        key="persisted",
                        value=self.value,
                        storage_class="persistable",
                        purpose="execution",
                    ),
                    SafeContextReferenceV1(
                        key="ephemeral",
                        value=f"runtime-{self.value}",
                        storage_class="runtime_only",
                        purpose="execution",
                    ),
                    SafeContextReferenceV1(
                        key="trace",
                        value="ignored-by-execution-digest",
                        storage_class="persistable",
                        purpose="correlation",
                    ),
                ),
            )

    registry = ExtensionRegistry()
    with registry.attributed_to("z:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="z",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=lambda: _Ordered("z"),
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )
    with registry.attributed_to("a:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="a",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=lambda: _Ordered("a"),
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )

    host = ContributorHost(registry.build())
    task = asyncio.create_task(host.contribute_origin(OriginContributionRequestV1(source_kind="http")))
    await asyncio.sleep(0)
    release.set()
    composed = await task

    assert [item.namespace for item in composed.persistable] == ["a", "a", "z", "z"]
    assert all(item.reference.key != "ephemeral" for item in composed.persistable)
    assert len(composed.execution_digest) == 64


def test_missing_required_capability_fails_startup() -> None:
    with pytest.raises(RequiredCapabilityError, match="origin_contributor:missing"):
        ContributorHost(
            ExtensionRegistry().build(),
            required_capabilities=("origin_contributor:missing",),
        )


def test_factories_are_initialized_once_at_startup() -> None:
    calls = 0

    def factory():
        nonlocal calls
        calls += 1
        return _Origin()

    registry = ExtensionRegistry()
    with registry.attributed_to("demo:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="demo",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=factory,
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )
    ContributorHost(registry.build())
    assert calls == 1


def test_positional_rollback_removes_partial_contributor_registration() -> None:
    registry = ExtensionRegistry()
    mark = registry.mark()
    with registry.attributed_to("broken:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="partial",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_Origin,
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )
    registry.rollback_to(mark)
    assert registry.build().origin_contributor_factories == ()


@pytest.mark.asyncio
async def test_required_runtime_failure_is_indeterminate_and_optional_failure_is_omitted(caplog) -> None:
    class _Broken:
        async def contribute(self, _request):
            raise RuntimeError("credential-like-value-must-not-leak")

    registry = ExtensionRegistry()
    with registry.attributed_to("demo:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="broken",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_Broken,
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )

    optional = await ContributorHost(registry.build()).contribute_origin(OriginContributionRequestV1(source_kind="http"))
    assert optional.persistable == ()
    assert len(optional.diagnostics) == 1
    assert len(optional.diagnostics[0].message.encode("utf-8")) <= 512

    required = ContributorHost(
        registry.build(),
        required_capabilities=("origin_contributor:broken",),
    )
    with caplog.at_level("ERROR", logger="deerflow.extensions.contributors"):
        with pytest.raises(ContributorIndeterminateError, match="origin_contributor:broken"):
            await required.contribute_origin(OriginContributionRequestV1(source_kind="http"))
    assert "diagnostic_code=contribution_failed" in caplog.text
    assert "contribution_id=broken" in caplog.text
    assert "error_class=RuntimeError" in caplog.text
    assert "correlation_id=" in caplog.text
    assert "credential-like-value-must-not-leak" not in caplog.text


@pytest.mark.asyncio
async def test_required_timeout_fails_closed(monkeypatch) -> None:
    import deerflow.extensions.contributors as host_module

    class _Slow:
        async def contribute(self, _request):
            await asyncio.Event().wait()

    monkeypatch.setattr(host_module, "_TIMEOUT_SECONDS", 0.01)
    registry = ExtensionRegistry()
    with registry.attributed_to("slow:install"):
        registry.origin_contributor(
            OriginContributorFactory(
                contribution_id="slow",
                capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                factory=_Slow,
                kind=ORIGIN_CONTRIBUTOR_KIND,
            )
        )
    host = ContributorHost(
        registry.build(),
        required_capabilities=("origin_contributor:slow",),
    )
    with pytest.raises(ContributorIndeterminateError, match="TimeoutError"):
        await host.contribute_origin(OriginContributionRequestV1(source_kind="http"))


@pytest.mark.asyncio
async def test_correlation_values_do_not_change_execution_digest_but_secret_handles_do() -> None:
    class _Purpose:
        def __init__(self, correlation: str, handle: str) -> None:
            self.correlation = correlation
            self.handle = handle

        async def contribute(self, _request):
            return OriginContributionV1(
                namespace="purpose",
                references=(
                    SafeContextReferenceV1(
                        key="correlation",
                        value=self.correlation,
                        storage_class="persistable",
                        purpose="correlation",
                    ),
                    SafeContextReferenceV1(
                        key="handle",
                        value=self.handle,
                        storage_class="runtime_only",
                        purpose="secret_handle",
                    ),
                ),
            )

    async def digest(correlation: str, handle: str) -> str:
        registry = ExtensionRegistry()
        with registry.attributed_to("purpose:install"):
            registry.origin_contributor(
                OriginContributorFactory(
                    contribution_id="purpose",
                    capability_api_version=ORIGIN_CONTRIBUTOR_CAPABILITY_API_VERSION,
                    factory=lambda: _Purpose(correlation, handle),
                    kind=ORIGIN_CONTRIBUTOR_KIND,
                )
            )
        result = await ContributorHost(registry.build()).contribute_origin(OriginContributionRequestV1(source_kind="http"))
        assert all(item.reference.key != "handle" for item in result.persistable)
        return result.execution_digest

    assert await digest("trace-a", "secret-id") == await digest("trace-b", "secret-id")
    assert await digest("trace-a", "secret-id") != await digest("trace-a", "other-secret-id")


def test_secret_handle_requires_one_stable_string_identifier() -> None:
    with pytest.raises(TypeError, match="stable string identifier"):
        SafeContextReferenceV1(
            key="credential",
            value=["handle-a", "handle-b"],
            storage_class="runtime_only",
            purpose="secret_handle",
        )


def test_canonical_contribution_size_is_bounded() -> None:
    with pytest.raises(ValueError, match="8 KiB"):
        OriginContributionV1(
            namespace="large",
            references=tuple(
                SafeContextReferenceV1(
                    key=f"key_{index}",
                    value="x" * 1000,
                    storage_class="persistable",
                    purpose="execution",
                )
                for index in range(9)
            ),
        )
