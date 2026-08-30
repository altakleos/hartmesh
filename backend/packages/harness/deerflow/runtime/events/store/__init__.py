from deerflow.runtime.events.store.base import RunEventStore
from deerflow.runtime.events.store.memory import MemoryRunEventStore


def make_run_event_store(config=None, *, run_store: object | None = None) -> RunEventStore:
    """Create a RunEventStore based on run_events.backend configuration."""
    if config is None or config.backend == "memory":
        return MemoryRunEventStore(run_store=run_store)
    if config.backend == "db":
        from deerflow.persistence.engine import get_session_factory

        sf = get_session_factory()
        if sf is None:
            # database.backend=memory but run_events.backend=db -> fallback
            return MemoryRunEventStore(run_store=run_store)
        from deerflow.runtime.events.store.db import DbRunEventStore

        return DbRunEventStore(sf, max_trace_content=config.max_trace_content)
    if config.backend == "jsonl":
        from deerflow.runtime.events.store.jsonl import JsonlRunEventStore

        return JsonlRunEventStore(run_store=run_store)
    raise ValueError(f"Unknown run_events backend: {config.backend!r}")


__all__ = ["MemoryRunEventStore", "RunEventStore", "make_run_event_store"]
