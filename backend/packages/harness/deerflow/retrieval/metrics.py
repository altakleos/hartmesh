"""Low-cardinality operational metrics for committed retrieval observations."""

from __future__ import annotations

import threading
from collections import Counter
from dataclasses import dataclass

from deerflow.retrieval.contracts import RetrievalObservationV1

_KNOWN_PROVIDER_CATEGORIES = frozenset(
    {
        "duckduckgo",
        "ragflow",
        "serply",
        "tencent_wsa",
    }
)
_KNOWN_PROVIDER_STATUSES = frozenset(
    {
        "success",
        "empty",
        "partial",
        "policy_denied",
        "provider_unavailable",
        "timeout",
        "rate_limited",
        "authentication_failed",
        "configuration_error",
        "unsafe_response",
        "oversized_response",
        "internal_error",
        "cancelled",
    }
)


def _provider_category(provider_id: str) -> str:
    if provider_id in _KNOWN_PROVIDER_CATEGORIES:
        return provider_id
    if provider_id == "mcp" or provider_id.startswith("mcp:"):
        return "mcp"
    return "other"


@dataclass(frozen=True, slots=True)
class RetrievalMetricPoint:
    provider_category: str
    status: str
    count: int
    total_duration_ms: int


class RetrievalMetricsRegistry:
    """Thread-safe counters whose only labels are closed provider/status values."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: Counter[tuple[str, str]] = Counter()
        self._durations: Counter[tuple[str, str]] = Counter()

    def observe(self, observation: RetrievalObservationV1) -> None:
        if not isinstance(observation, RetrievalObservationV1):
            raise TypeError("observation must be RetrievalObservationV1")
        provider = _provider_category(observation.draft.provider_id)
        status = observation.draft.provider_status
        if status not in _KNOWN_PROVIDER_STATUSES:
            status = "internal_error"
        duration_ms = observation.draft.to_event_projection()["duration_ms"]
        if type(duration_ms) is not int:
            raise TypeError("retrieval duration must be an integer")
        key = (provider, status)
        with self._lock:
            self._counts[key] += 1
            self._durations[key] += duration_ms

    def snapshot(self) -> tuple[RetrievalMetricPoint, ...]:
        with self._lock:
            return tuple(
                RetrievalMetricPoint(
                    provider_category=provider,
                    status=status,
                    count=count,
                    total_duration_ms=self._durations[(provider, status)],
                )
                for (provider, status), count in sorted(self._counts.items())
            )


RETRIEVAL_METRICS = RetrievalMetricsRegistry()


def record_retrieval_observation_metric(
    observation: RetrievalObservationV1,
) -> None:
    RETRIEVAL_METRICS.observe(observation)


__all__ = [
    "RETRIEVAL_METRICS",
    "RetrievalMetricPoint",
    "RetrievalMetricsRegistry",
    "record_retrieval_observation_metric",
]
