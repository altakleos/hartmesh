"""Deep provider-neutral service for evidence-bearing retrieval."""

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import AsyncIterator, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

from deerflow.retrieval.contracts import (
    AcceptedRetrievalRequest,
    ProviderRetrievalItem,
    ProviderRetrievalRequest,
    ProviderRetrievalResponse,
    RetrievalCandidate,
    RetrievalEvidenceError,
    RetrievalObservationDraftV1,
    RetrievalProviderError,
    _canonical_bytes,
    _domain_digest,
    normalize_web_source_reference,
)


@runtime_checkable
class RetrievalProvider(Protocol):
    async def search(
        self,
        request: ProviderRetrievalRequest,
    ) -> ProviderRetrievalResponse: ...


@runtime_checkable
class EvidenceBearingRetriever(Protocol):
    async def retrieve(
        self,
        request: AcceptedRetrievalRequest,
        provider: RetrievalProvider,
    ) -> RetrievalCandidate: ...


@runtime_checkable
class RetrievalConcurrencyLimiter(Protocol):
    def limit(
        self,
        tenant_digest: str,
        provider_id: str,
    ) -> AbstractAsyncContextManager[None]: ...


class TenantProviderConcurrencyLimiter:
    """Bound provider calls per tenant without exporting either key as a label."""

    def __init__(
        self,
        *,
        max_concurrency: int = 4,
        max_keys_per_loop: int = 4_096,
    ) -> None:
        if type(max_concurrency) is not int or not 1 <= max_concurrency <= 64:
            raise ValueError("max_concurrency must be between 1 and 64")
        if type(max_keys_per_loop) is not int or not 1 <= max_keys_per_loop <= 65_536:
            raise ValueError("max_keys_per_loop must be between 1 and 65536")
        self._max_concurrency = max_concurrency
        self._max_keys_per_loop = max_keys_per_loop
        self._lock = threading.Lock()
        self._semaphores: weakref.WeakKeyDictionary[
            asyncio.AbstractEventLoop,
            dict[tuple[str, str], asyncio.Semaphore],
        ] = weakref.WeakKeyDictionary()

    def _semaphore(
        self,
        tenant_digest: str,
        provider_id: str,
    ) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        key = (tenant_digest, provider_id)
        with self._lock:
            loop_semaphores = self._semaphores.setdefault(loop, {})
            semaphore = loop_semaphores.get(key)
            if semaphore is not None:
                return semaphore
            if len(loop_semaphores) >= self._max_keys_per_loop:
                raise RetrievalProviderError("provider_unavailable")
            semaphore = asyncio.Semaphore(self._max_concurrency)
            loop_semaphores[key] = semaphore
            return semaphore

    @asynccontextmanager
    async def limit(
        self,
        tenant_digest: str,
        provider_id: str,
    ) -> AsyncIterator[None]:
        semaphore = self._semaphore(tenant_digest, provider_id)
        await semaphore.acquire()
        try:
            yield
        finally:
            semaphore.release()


_DEFAULT_CONCURRENCY_LIMITER = TenantProviderConcurrencyLimiter()


async def run_blocking_provider_call[**P, R](
    function: Callable[P, R],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> R:
    """Offload blocking provider I/O without orphaning its live-call permit.

    ``asyncio.to_thread`` cannot stop its worker when the awaiting task is
    cancelled. Shield the worker and finish joining it before propagating
    cancellation, so the service's surrounding tenant/provider semaphore is
    not released while network I/O is still running. Provider clients still
    own the finite socket timeout that bounds this join.
    """

    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await task
        except Exception:
            pass
        raise


def _internal_source_reference(
    item: ProviderRetrievalItem,
    request: AcceptedRetrievalRequest,
) -> str:
    selector = item.collection_selector
    document = item.document_selector
    if selector is None or document is None:
        raise RetrievalEvidenceError("retrieval_internal_source_invalid")
    constraints = request.effective_constraints
    try:
        position = constraints.collections.index(selector)
        public_ref = constraints.collection_public_refs[position]
    except (ValueError, IndexError) as exc:
        raise RetrievalEvidenceError("retrieval_internal_source_denied") from exc
    opaque_document = _domain_digest(
        "retrieval-document-reference",
        {
            "tenant": request.tenant.digest,
            "collection_public_ref": public_ref,
            "document_selector": document,
        },
    )
    return f"ragflow-doc:{public_ref}:{opaque_document}"


class EvidenceBearingRetrievalService:
    """Enforce policy, invoke one adapter, and create a safe draft.

    This service never computes a result digest. The returned candidate may be
    sanitized or budgeted again; only the outer receipt middleware can finalize
    the draft against the exact model-visible result.
    """

    def __init__(
        self,
        *,
        concurrency_limiter: RetrievalConcurrencyLimiter | None = None,
    ) -> None:
        limiter = concurrency_limiter or _DEFAULT_CONCURRENCY_LIMITER
        if not isinstance(limiter, RetrievalConcurrencyLimiter):
            raise TypeError("concurrency_limiter must implement RetrievalConcurrencyLimiter")
        self._concurrency_limiter = limiter

    async def retrieve(
        self,
        request: AcceptedRetrievalRequest,
        provider: RetrievalProvider,
    ) -> RetrievalCandidate:
        if not isinstance(request, AcceptedRetrievalRequest):
            raise TypeError("request must be AcceptedRetrievalRequest")
        if not isinstance(provider, RetrievalProvider):
            raise TypeError("provider must implement RetrievalProvider")
        constraints = request.effective_constraints
        provider_request = ProviderRetrievalRequest(
            query=request.query,
            credential=request.credential,
            endpoint=request.requested_constraints.endpoint,
            constraints=constraints,
        )
        try:
            async with asyncio.timeout(constraints.timeout_ms / 1_000):
                async with self._concurrency_limiter.limit(
                    request.tenant.digest,
                    request.provider_id,
                ):
                    response = await provider.search(provider_request)
            if not isinstance(response, ProviderRetrievalResponse):
                raise RetrievalEvidenceError("retrieval_provider_response_invalid")
            if response.content_type not in {
                "application/json",
                "application/vnd.deerflow.retrieval+json",
            }:
                raise RetrievalEvidenceError("retrieval_provider_content_type_invalid")
            if response.partial and not constraints.accept_partial:
                raise RetrievalEvidenceError("retrieval_partial_response_denied")
            if len(response.items) > constraints.max_results:
                raise RetrievalEvidenceError("retrieval_response_count_exceeded")
            result_count = len(response.items) if response.result_count is None else response.result_count
            if result_count != len(response.items):
                raise RetrievalEvidenceError("retrieval_provider_result_count_mismatch")
            if result_count > constraints.max_results:
                raise RetrievalEvidenceError("retrieval_response_count_exceeded")
            encoded_result = _canonical_bytes(response.candidate_result)
            if len(encoded_result) > constraints.max_aggregate_bytes:
                raise RetrievalEvidenceError("retrieval_response_aggregate_exceeded")

            source_references: list[str] = []
            aggregate_item_bytes = 0
            for item in response.items:
                item_bytes = len(_canonical_bytes(item.content))
                if item_bytes > constraints.max_item_bytes:
                    raise RetrievalEvidenceError("retrieval_response_item_exceeded")
                aggregate_item_bytes += item_bytes
                if aggregate_item_bytes > constraints.max_aggregate_bytes:
                    raise RetrievalEvidenceError("retrieval_response_aggregate_exceeded")
                if item.source_locator is not None:
                    source = normalize_web_source_reference(
                        item.source_locator,
                        allowed_schemes=constraints.source_schemes,
                        allowed_domains=constraints.domains,
                        denied_domains=request.policy.web_domain_denylist,
                    )
                else:
                    if "ragflow-doc" not in constraints.source_schemes:
                        raise RetrievalEvidenceError("retrieval_source_scheme_denied")
                    source = _internal_source_reference(item, request)
                if source not in source_references:
                    source_references.append(source)
            if len(source_references) > 64:
                raise RetrievalEvidenceError("retrieval_source_count_exceeded")
        except TimeoutError:
            draft = self._draft(
                request,
                provider_status="timeout",
                safe_reason="timeout",
            )
            self._publish(draft)
            raise RetrievalProviderError("timeout") from None
        except RetrievalProviderError as exc:
            draft = self._draft(
                request,
                provider_status=exc.status,
                safe_reason=exc.status,
            )
            self._publish(draft)
            raise
        except RetrievalEvidenceError as exc:
            oversized = "exceeded" in exc.code or "too_large" in exc.code
            status = "oversized_response" if oversized else "unsafe_response"
            draft = self._draft(
                request,
                provider_status=status,
                safe_reason=status,
            )
            self._publish(draft)
            raise RetrievalProviderError(status) from None
        except Exception:
            draft = self._draft(
                request,
                provider_status="provider_unavailable",
                safe_reason="provider_unavailable",
            )
            self._publish(draft)
            raise RetrievalProviderError("provider_unavailable") from None

        status = "partial" if response.partial else ("empty" if result_count == 0 else "success")
        draft = self._draft(
            request,
            provider_status=status,
            safe_reason=None,
            result_count=result_count,
            source_references=tuple(source_references),
            truncated=response.truncated,
            partial=response.partial,
            safe_provider_request_ref=response.safe_request_ref,
        )
        self._publish(draft)
        return RetrievalCandidate(result=response.candidate_result, draft=draft)

    @staticmethod
    def _draft(
        request: AcceptedRetrievalRequest,
        *,
        provider_status: str,
        safe_reason: str | None,
        result_count: int = 0,
        source_references: tuple[str, ...] = (),
        truncated: bool = False,
        partial: bool = False,
        safe_provider_request_ref: str | None = None,
    ) -> RetrievalObservationDraftV1:
        constraints = request.effective_constraints
        return RetrievalObservationDraftV1(
            tenant_ref=request.tenant.public_ref,
            tenant_digest=request.tenant.digest,
            run_id=request.receipt.context.run_id,
            receipt_id=request.receipt.receipt_id,
            attempt=request.receipt.context.attempt,
            provider_id=request.provider_id,
            tool_kind=request.tool_kind,
            adapter_capability_version=request.adapter_capability_version,
            policy_digest=constraints.policy_digest,
            safe_constraints=constraints.to_safe_projection(),
            started_at=request.receipt.occurred_at,
            provider_finished_at=datetime.now(UTC),
            provider_status=provider_status,  # type: ignore[arg-type]
            safe_reason=safe_reason,
            result_count=result_count,
            source_count=len(source_references),
            source_references=source_references,
            truncated=truncated,
            partial=partial,
            safe_provider_request_ref=safe_provider_request_ref,
            tool_plane_base_revision_digest=request.tool_plane_base_revision_digest,
            tool_plane_user_overlay_digest=request.tool_plane_user_overlay_digest,
            tool_plane_projection_digest=request.tool_plane_projection_digest,
            tool_plane_effective_digest=request.tool_plane_effective_digest,
            accepted_execution_evidence_ref=request.accepted_execution_evidence_ref,
            accepted_sandbox_operation_ref=request.accepted_sandbox_operation_ref,
            mcp_evidence_ref=request.mcp_evidence_ref,
        )

    @staticmethod
    def _publish(draft: RetrievalObservationDraftV1) -> None:
        from deerflow.retrieval.context import (
            publish_retrieval_observation_draft_if_active,
        )

        publish_retrieval_observation_draft_if_active(draft)
