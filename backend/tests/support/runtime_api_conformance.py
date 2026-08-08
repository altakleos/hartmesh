"""Transport-neutral conformance probes for runtime API adapters."""

from __future__ import annotations

from deerflow_runtime_api import (
    CancelInvocationRequest,
    ContextInvocationsQuery,
    DurableInvocationPort,
    InvocationControlReceipt,
    InvocationEnsureReceipt,
    InvocationEnsureRequest,
    InvocationObservation,
    InvocationQuery,
    RuntimeCapabilities,
    RuntimeFailure,
    record_from_dict,
)


async def assert_runtime_adapter_conformance(
    adapter: DurableInvocationPort,
    *,
    ensure: InvocationEnsureRequest,
    invocation_query: InvocationQuery,
    context_query: ContextInvocationsQuery,
    control: CancelInvocationRequest,
) -> None:
    """Exercise and strictly serialize every v1 adapter operation."""

    results = (
        adapter.capabilities(),
        await adapter.ensure(ensure),
        await adapter.observe(invocation_query),
        await adapter.observe(context_query),
        await adapter.control(control),
    )
    expected_types = (
        RuntimeCapabilities,
        (InvocationEnsureReceipt, RuntimeFailure),
        (InvocationObservation, RuntimeFailure),
        (InvocationObservation, RuntimeFailure),
        (InvocationControlReceipt, RuntimeFailure),
    )
    for result, expected in zip(results, expected_types, strict=True):
        assert isinstance(result, expected)
        assert record_from_dict(result.to_dict()) == result


__all__ = ["assert_runtime_adapter_conformance"]
