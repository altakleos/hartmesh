"""Read-only Agent tool for operator-scoped RAGFlow knowledge retrieval."""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from langchain_core.tools import StructuredTool
from pydantic import AnyHttpUrl, BaseModel, ConfigDict, Field, SecretStr, ValidationError, field_validator

from deerflow.config import get_app_config
from deerflow.retrieval import (
    RETRIEVAL_TOOL_METADATA_KEY,
    EvidenceBearingRetrievalService,
    ProviderRetrievalItem,
    ProviderRetrievalResponse,
    ResolvedRetrievalCredentialV1,
    RetrievalPolicyV1,
    RetrievalProviderError,
    RetrievalRequestConstraintsV1,
    RetrievalToolDeclarationV1,
    accepted_retrieval_app_config_from_active,
    accepted_retrieval_request_from_active,
    get_active_retrieval_handoff,
)

from .client import RAGFlowAPIError, RAGFlowClient, RAGFlowConnectionError, RAGFlowProtocolError
from .formatting import format_retrieval_result

logger = logging.getLogger(__name__)

_warned: set[str] = set()
_RAGFLOW_UUID_PATTERN = re.compile(r"(?<![0-9A-Fa-f])(?:[0-9A-Fa-f]{32}|[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12})(?![0-9A-Fa-f])")
_MAX_PARALLEL_RETRIEVAL_GROUPS = 4
_NO_RELEVANT_CONTENT = "No relevant content found."


@dataclass(frozen=True, slots=True)
class _ResolvedDataset:
    dataset_id: str
    name: str
    embedding_model: str
    chunk_count: int | None


class _RAGFlowRetrievalSettings(BaseModel):
    """Validated provider settings stored on the knowledge_search tool entry."""

    model_config = ConfigDict(validate_default=True)

    datasets: list[str] | None = Field(default=None, max_length=100)
    base_url: AnyHttpUrl = Field(default="http://localhost:9380")
    api_key: SecretStr | None = Field(default=None)
    timeout: float = Field(default=30, gt=0, le=600)
    page_size: int = Field(default=8, ge=1, le=100)
    similarity_threshold: float = Field(default=0.2, ge=0, le=1)
    vector_similarity_weight: float = Field(default=0.3, ge=0, le=1)
    top_k: int = Field(default=256, ge=1, le=1024)
    max_chars_per_chunk: int = Field(default=800, ge=1, le=100_000)
    max_total_chars: int = Field(default=8000, ge=1, le=1_000_000)

    @field_validator("datasets")
    @classmethod
    def _normalize_dataset_ids(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        if not value:
            raise ValueError("datasets must not be empty when configured; omit it to search all accessible datasets")
        normalized: list[str] = []
        seen: set[str] = set()
        for dataset_id in value:
            clean_id = dataset_id.strip()
            if not clean_id or len(clean_id) > 256:
                raise ValueError("dataset IDs must contain between 1 and 256 characters")
            if clean_id not in seen:
                normalized.append(clean_id)
                seen.add(clean_id)
        return normalized

    @field_validator("base_url")
    @classmethod
    def _reject_url_userinfo(cls, value: AnyHttpUrl) -> AnyHttpUrl:
        if value.username is not None or value.password is not None:
            raise ValueError("base_url must not contain username or password information")
        return value


def _api_key(settings: _RAGFlowRetrievalSettings) -> str | None:
    value = settings.api_key
    if isinstance(value, SecretStr):
        value = value.get_secret_value()
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _redact_api_key(value: object, api_key: str | None) -> str:
    text = str(value)
    if api_key:
        text = text.replace(api_key, "[REDACTED]")
    return text


def _redact_error(value: object, api_key: str | None) -> str:
    """Redact provider credentials and opaque dataset IDs on error paths."""
    return _RAGFLOW_UUID_PATTERN.sub("[DATASET_ID]", _redact_api_key(value, api_key))


def _settings_from_extra(extra: Mapping[str, object]) -> _RAGFlowRetrievalSettings:
    return _RAGFlowRetrievalSettings.model_validate(dict(extra))


def _settings_or_error(
    *,
    app_config: object | None = None,
) -> tuple[_RAGFlowRetrievalSettings | None, str | None]:
    config_source = get_app_config() if app_config is None else app_config
    tool_config = config_source.get_tool_config("knowledge_search")
    if tool_config is None:
        return None, "Error: knowledge_search is not configured; add its RAGFlow settings to the tools list in config.yaml."
    try:
        settings = _settings_from_extra(tool_config.model_extra or {})
    except ValidationError:
        logger.warning("RAGFlow knowledge_search tool configuration is invalid")
        return None, "Error: Invalid RAGFlow settings for knowledge_search; check config.yaml."
    if not _api_key(settings):
        if "api_key" not in _warned:
            _warned.add("api_key")
            logger.warning("RAGFlow API key is not configured; set knowledge_search.api_key in config.yaml, preferably via $RAGFLOW_API_KEY.")
        return None, "Error: RAGFlow API key is not configured; set knowledge_search.api_key in config.yaml (prefer $RAGFLOW_API_KEY)."
    return settings, None


def _build_client(
    settings: _RAGFlowRetrievalSettings,
    *,
    max_response_bytes: int = 8 * 1024 * 1024,
) -> RAGFlowClient:
    api_key = _api_key(settings)
    if api_key is None:  # Guarded by _settings_or_error; keeps this helper total.
        raise ValueError("RAGFlow API key is missing")
    return RAGFlowClient(
        base_url=str(settings.base_url).rstrip("/"),
        api_key=api_key,
        timeout=settings.timeout,
        max_response_bytes=max_response_bytes,
    )


def _tool_error(exc: Exception, settings: _RAGFlowRetrievalSettings) -> str:
    key = _api_key(settings)
    safe_detail = _redact_error(exc, key)
    base_url = _redact_error(str(settings.base_url).rstrip("/"), key)

    if isinstance(exc, RAGFlowAPIError):
        logger.warning("RAGFlow API rejected a read-only tool request")
        return f"Error: {safe_detail}"
    if isinstance(exc, RAGFlowConnectionError):
        logger.warning("RAGFlow connection failed for %s (%s)", base_url, type(exc).__name__)
        return f"Error: Unable to connect to RAGFlow ({base_url}): {safe_detail}"
    if isinstance(exc, RAGFlowProtocolError):
        logger.warning("RAGFlow returned an invalid response for a read-only tool request (%s)", type(exc).__name__)
        return f"Error: RAGFlow request failed: {safe_detail}"

    logger.warning("Unexpected RAGFlow read-only tool failure (%s)", type(exc).__name__)
    return "Error: An unexpected RAGFlow retrieval error occurred; try again later."


def _resolved_dataset(dataset: Mapping[str, object], *, expected_id: str | None = None) -> _ResolvedDataset | None:
    dataset_id = dataset.get("id")
    if not isinstance(dataset_id, str) or not dataset_id.strip():
        return None
    clean_id = dataset_id.strip()
    if expected_id is not None and clean_id != expected_id:
        return None

    name = dataset.get("name")
    raw_chunk_count = dataset.get("chunk_count")
    chunk_count = raw_chunk_count if isinstance(raw_chunk_count, int) and not isinstance(raw_chunk_count, bool) and raw_chunk_count >= 0 else None
    embedding_model = dataset.get("embedding_model")
    if not isinstance(embedding_model, str) or not embedding_model.strip():
        if chunk_count == 0:
            warning_key = f"empty_embedding:{clean_id}"
            if warning_key not in _warned:
                _warned.add(warning_key)
                logger.warning("Skipping empty RAGFlow dataset without embedding model metadata")
            embedding_model = ""
        else:
            raise RAGFlowProtocolError("RAGFlow returned a searchable dataset without embedding model metadata.")
    return _ResolvedDataset(
        dataset_id=clean_id,
        name=str(name).strip() if name else "Unknown dataset",
        embedding_model=embedding_model.strip(),
        chunk_count=chunk_count,
    )


def _current_dataset(datasets: list[dict], bound_id: str) -> _ResolvedDataset | None:
    for dataset in datasets:
        resolved = _resolved_dataset(dataset, expected_id=bound_id)
        if resolved is not None:
            return resolved
    return None


def _ordinal(value: int) -> str:
    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value}{suffix}"


def _missing_dataset_error(position: int) -> str:
    return f"Error: The {_ordinal(position)} entry of knowledge_search.datasets was not found or is inaccessible; check config.yaml."


def _log_missing_dataset(*, position: int, dataset_id: str, code: object = None) -> None:
    del dataset_id
    logger.warning(
        "Configured RAGFlow dataset binding could not be resolved (position=%d, code_category=%s)",
        position,
        "provider_error" if code is not None else "not_found",
    )


async def _resolve_datasets(
    client: RAGFlowClient,
    settings: _RAGFlowRetrievalSettings,
) -> tuple[list[_ResolvedDataset] | None, str | None]:
    if settings.datasets is None:
        datasets = await client.list_datasets()
        resolved_by_id: dict[str, _ResolvedDataset] = {}
        for dataset in datasets:
            resolved = _resolved_dataset(dataset)
            if resolved is None:
                continue
            resolved_by_id.setdefault(resolved.dataset_id, resolved)

        if not resolved_by_id:
            return (
                None,
                "Error: No accessible RAGFlow datasets were found; configure knowledge_search.datasets or add a dataset in RAGFlow.",
            )
        return list(resolved_by_id.values()), None

    resolved_datasets: list[_ResolvedDataset] = []
    for position, bound_id in enumerate(settings.datasets, start=1):
        datasets = await client.list_datasets(dataset_id=bound_id)
        resolved = _current_dataset(datasets, bound_id)
        if resolved is None:
            _log_missing_dataset(position=position, dataset_id=bound_id)
            return None, _missing_dataset_error(position)
        resolved_datasets.append(resolved)

    return resolved_datasets, None


def _group_searchable_datasets(datasets: list[_ResolvedDataset]) -> list[tuple[str, list[str]]]:
    groups: dict[str, list[str]] = {}
    for dataset in datasets:
        if dataset.chunk_count == 0:
            continue
        groups.setdefault(dataset.embedding_model, []).append(dataset.dataset_id)
    return sorted(groups.items())


def _result_chunks(result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    chunks = result.get("chunks")
    if not isinstance(chunks, list):
        return []
    return [chunk for chunk in chunks if isinstance(chunk, Mapping)]


def _result_document_aggregates(result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    aggregates = result.get("doc_aggs")
    if isinstance(aggregates, list):
        return [aggregate for aggregate in aggregates if isinstance(aggregate, Mapping)]
    if isinstance(aggregates, Mapping):
        return [aggregate for aggregate in aggregates.values() if isinstance(aggregate, Mapping)]
    return []


def _merge_group_results(results: list[dict[str, Any]], *, page_size: int) -> dict[str, Any]:
    chunk_groups = [_result_chunks(result) for result in results]
    merged_chunks: list[Mapping[str, Any]] = []
    max_group_size = max((len(chunks) for chunks in chunk_groups), default=0)
    hide_cross_group_scores = len(results) > 1
    # Similarity scores from different embedding spaces are not globally
    # calibrated. Preserve each provider-ranked list and interleave equal rank
    # positions instead of comparing raw scores across models.
    for rank in range(max_group_size):
        for chunks in chunk_groups:
            if rank < len(chunks):
                chunk = chunks[rank]
                if hide_cross_group_scores and "similarity" in chunk:
                    chunk = {key: value for key, value in chunk.items() if key != "similarity"}
                merged_chunks.append(chunk)
                if len(merged_chunks) >= page_size:
                    break
        if len(merged_chunks) >= page_size:
            break

    selected_document_ids: list[str] = []
    for chunk in merged_chunks:
        document_id = chunk.get("document_id")
        if document_id is not None:
            clean_id = str(document_id)
            if clean_id not in selected_document_ids:
                selected_document_ids.append(clean_id)

    aggregates_by_document_id: dict[str, Mapping[str, Any]] = {}
    for result in results:
        for aggregate in _result_document_aggregates(result):
            document_id = aggregate.get("doc_id")
            if document_id is not None:
                aggregates_by_document_id.setdefault(str(document_id), aggregate)

    total = 0
    for result in results:
        value = result.get("total")
        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            total += value
    return {
        "chunks": merged_chunks,
        "doc_aggs": [aggregates_by_document_id[document_id] for document_id in selected_document_ids if document_id in aggregates_by_document_id],
        "total": total,
    }


async def _retrieve_dataset_groups(
    client: RAGFlowClient,
    settings: _RAGFlowRetrievalSettings,
    query: str,
    groups: list[tuple[str, list[str]]],
) -> dict[str, Any]:
    semaphore = asyncio.Semaphore(_MAX_PARALLEL_RETRIEVAL_GROUPS)

    async def retrieve_group(dataset_ids: list[str]) -> dict[str, Any]:
        async with semaphore:
            return await client.retrieve(
                query,
                dataset_ids=dataset_ids,
                page_size=settings.page_size,
                similarity_threshold=settings.similarity_threshold,
                vector_similarity_weight=settings.vector_similarity_weight,
                top_k=settings.top_k,
            )

    results = await asyncio.gather(*(retrieve_group(dataset_ids) for _, dataset_ids in groups), return_exceptions=True)
    successful_results: list[dict[str, Any]] = []
    for result in results:
        if isinstance(result, BaseException):
            raise result
        successful_results.append(result)

    return _merge_group_results(successful_results, page_size=settings.page_size)


async def knowledge_search(query: str) -> str:
    """Search the configured RAGFlow scope, defaulting to every accessible dataset."""
    if get_active_retrieval_handoff() is not None:
        return await _knowledge_search_with_evidence(query)
    query = query.strip()
    if not query:
        return "Error: query must not be empty."

    settings, error = _settings_or_error()
    if settings is None:
        return error or "Error: Invalid RAGFlow settings for knowledge_search; check config.yaml."

    client = _build_client(settings)
    try:
        datasets, resolution_error = await _resolve_datasets(client, settings)
        if resolution_error is not None:
            return resolution_error
        if not datasets:  # Defensive; both resolution paths return a non-empty scope.
            return "Error: No RAGFlow datasets could be resolved; check knowledge_search in config.yaml."

        groups = _group_searchable_datasets(datasets)
        if not groups:
            return _NO_RELEVANT_CONTENT

        result = await _retrieve_dataset_groups(client, settings, query, groups)
        names_by_id = {dataset.dataset_id: dataset.name for dataset in datasets}
        formatted = format_retrieval_result(
            result,
            dataset_names_by_id=names_by_id,
            max_chars_per_chunk=settings.max_chars_per_chunk,
            max_total_chars=settings.max_total_chars,
        )
        # API-key redaction remains mandatory on success. UUID redaction is
        # deliberately error-only so valid checksums and trace IDs survive.
        return _redact_api_key(formatted, _api_key(settings))
    except Exception as exc:
        return _tool_error(exc, settings)


class _RAGFlowRetrievalProvider:
    def __init__(
        self,
        *,
        settings: _RAGFlowRetrievalSettings,
        client: RAGFlowClient,
    ) -> None:
        self._settings = settings
        self._client = client

    async def search(self, request) -> ProviderRetrievalResponse:
        if not request.credential.available:
            raise RetrievalProviderError("configuration_error")
        try:
            datasets, resolution_error = await _resolve_datasets(
                self._client,
                self._settings,
            )
            if resolution_error is not None or not datasets:
                raise RetrievalProviderError("configuration_error")
            groups = _group_searchable_datasets(datasets)
            if not groups:
                return ProviderRetrievalResponse(
                    candidate_result=_NO_RELEVANT_CONTENT,
                    items=(),
                    result_count=0,
                )
            result = await _retrieve_dataset_groups(
                self._client,
                self._settings,
                request.query,
                groups,
            )
            names_by_id = {dataset.dataset_id: dataset.name for dataset in datasets}
            formatted = format_retrieval_result(
                result,
                dataset_names_by_id=names_by_id,
                max_chars_per_chunk=self._settings.max_chars_per_chunk,
                max_total_chars=self._settings.max_total_chars,
            )
            formatted = _redact_api_key(
                formatted,
                _api_key(self._settings),
            )
            chunks = _result_chunks(result)
            items = tuple(
                ProviderRetrievalItem(
                    source_locator=None,
                    collection_selector=str(chunk["dataset_id"]),
                    document_selector=str(chunk["document_id"]),
                    content=chunk.get("content", ""),
                )
                for chunk in chunks[: request.constraints.max_results]
                if chunk.get("dataset_id") is not None and chunk.get("document_id") is not None
            )
            return ProviderRetrievalResponse(
                candidate_result=formatted,
                items=items,
                result_count=min(
                    len(chunks),
                    request.constraints.max_results,
                ),
                truncated=(len(chunks) > request.constraints.max_results or "response truncated" in formatted),
            )
        except RetrievalProviderError:
            raise
        except RAGFlowConnectionError as exc:
            if "timed out" in str(exc).lower():
                raise RetrievalProviderError("timeout") from None
            raise RetrievalProviderError("provider_unavailable") from None
        except RAGFlowAPIError as exc:
            code = str(exc.code or "").lower()
            if code == "429" or "rate" in code or "limit" in code:
                raise RetrievalProviderError("rate_limited") from None
            if code in {"401", "403"} or any(marker in code for marker in ("auth", "token", "key")):
                raise RetrievalProviderError("authentication_failed") from None
            raise RetrievalProviderError("provider_unavailable") from None
        except RAGFlowProtocolError as exc:
            raise RetrievalProviderError(exc.provider_status) from None
        except Exception:
            raise RetrievalProviderError("provider_unavailable") from None


async def _knowledge_search_with_evidence(query: str) -> str:
    query = query.strip()
    settings, _error = _settings_or_error(
        app_config=accepted_retrieval_app_config_from_active(),
    )
    if settings is None:
        # No provider call occurs. The outer middleware records the bounded
        # configuration failure without copying this user-facing detail.
        raise RetrievalProviderError("configuration_error")
    if settings.datasets is None:
        # Durable evidence cannot prove a tenant collection boundary from a
        # mutable provider-wide enumeration. Direct/local calls retain the
        # legacy credential-scoped behavior; evidence-bearing runs fail closed.
        raise RetrievalProviderError("configuration_error")
    handoff = get_active_retrieval_handoff()
    if handoff is None:
        raise RetrievalProviderError("configuration_error")
    receipt = handoff.receipt
    tenant = getattr(getattr(receipt, "context", None), "tenant", None)
    if tenant is None:
        raise RetrievalProviderError("configuration_error")
    collection_refs = tuple(f"{tenant.public_ref}-ragflow-{position}" for position, _dataset_id in enumerate(settings.datasets, start=1))
    timeout_ms = max(1, int(settings.timeout * 1_000))
    policy = RetrievalPolicyV1(
        allowed_providers=("ragflow",),
        allowed_endpoint_origins=(str(settings.base_url),),
        allowed_collections=tuple(settings.datasets),
        collection_public_refs=collection_refs,
        max_results=settings.page_size,
        max_item_bytes=max(4 * settings.max_chars_per_chunk, 4_096),
        max_aggregate_bytes=max(4 * settings.max_total_chars, 16_384),
        timeout_ms=timeout_ms,
        allow_redirects=False,
        source_schemes=("ragflow-doc",),
    )
    accepted = accepted_retrieval_request_from_active(
        query=query,
        credential=ResolvedRetrievalCredentialV1(
            provider_id="ragflow",
            selector_ref="ragflow-knowledge-search",
            secret=_api_key(settings),
        ),
        policy=policy,
        requested_constraints=RetrievalRequestConstraintsV1(
            provider_id="ragflow",
            endpoint=str(settings.base_url),
            collections=tuple(settings.datasets),
            max_results=settings.page_size,
            max_item_bytes=max(4 * settings.max_chars_per_chunk, 4_096),
            max_aggregate_bytes=max(
                4 * settings.max_total_chars,
                16_384,
            ),
            timeout_ms=timeout_ms,
            source_schemes=("ragflow-doc",),
        ),
    )
    candidate = await EvidenceBearingRetrievalService().retrieve(
        accepted,
        _RAGFlowRetrievalProvider(
            settings=settings,
            client=_build_client(
                settings,
                max_response_bytes=accepted.effective_constraints.max_aggregate_bytes,
            ),
        ),
    )
    if not isinstance(candidate.result, str):
        raise RetrievalProviderError("unsafe_response")
    return candidate.result


def _tool_description() -> str:
    return "Search the operator-approved RAGFlow datasets and return compact, citation-numbered source chunks. Durable runs require an explicit operator-configured dataset allowlist. Dataset IDs are never shown to the model."


async def _knowledge_search_entrypoint(query: str) -> str:
    """Search the configured RAGFlow datasets, or every accessible dataset by default.

    Args:
        query: Specific question or search terms to retrieve from the configured private documents.
    """
    return await knowledge_search(query)


knowledge_search_tool = StructuredTool.from_function(
    coroutine=_knowledge_search_entrypoint,
    name="knowledge_search",
    description=_tool_description(),
    parse_docstring=True,
    metadata={
        RETRIEVAL_TOOL_METADATA_KEY: RetrievalToolDeclarationV1(
            provider_id="ragflow",
            tool_kind="knowledge_search",
            adapter_capability_version="ragflow-http-v1",
        ).to_metadata()
    },
)
