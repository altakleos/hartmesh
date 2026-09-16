"""The durable half of a run's artifact-delivery verdict.

A run that produced output files and presented none of them is failed by the
worker's delivery fence. Live clients hear that as one advisory ``custom``
frame; this module is what everyone else reads, and it is the same verdict from
the same bytes — the terminal ``run.delivery`` receipt, which carries the full
produced/presented sets the bounded frame truncates.

Written for hartmesh-tenancy/DF14. DF13 gave the browser the live notice and
stopped there: the frame is page-local state, so a reload left the run record
correctly saying ``error`` with ``stop_reason=artifact_delivery_incomplete``
while the correction under the turn was gone and the files it offered went with
it. The receipt outlives the page, so the notice can too.
"""

from __future__ import annotations

from typing import Any

#: Stamped on the run record by the delivery fence, and the only thing a client
#: needs in order to know whether asking for the detail below is worthwhile.
DELIVERY_INCOMPLETE_STOP_REASON = "artifact_delivery_incomplete"

#: Enough to act on, bounded so one run cannot push an unbounded list through
#: every subscriber's replay buffer or one HTTP response. ``undelivered_count``
#: stays exact.
MAX_DISCLOSED_UNDELIVERED_PATHS = 20

#: The receipt this reads. Terminal, written once per run.
DELIVERY_EVENT_TYPE = "run.delivery"

#: The sentence the fence commits as the run's terminal error and publishes on
#: the advisory frame. Owned here so the frame, the record and this projection
#: say one thing; the projection emits it rather than echoing ``record.error``,
#: which keeps an unbounded run error off a ``runs:read`` surface that
#: ``RunResponse`` deliberately omits it from.
DELIVERY_INCOMPLETE_ERROR = "Artifact delivery incomplete: no produced output artifact was presented"


def _path_list(value: Any) -> list[str]:
    """The stored field as a list of paths, or nothing.

    This is a projection over bytes written by an earlier process, so it reads
    them the way ``_presented_files_from_delivery`` reads the same receipt:
    types checked, not assumed. A string here would otherwise iterate into
    characters, and an unhashable element would raise out of the route.
    """
    if not isinstance(value, list):
        return []
    return [entry for entry in value if isinstance(entry, str) and entry]


def undelivered_paths(content: dict[str, Any]) -> list[str]:
    """Produced outputs this run never presented, in scan order.

    The fence only fires when nothing matched, so at today's call sites this
    subtracts an empty set; the subtraction keeps the helper honest if the
    satisfaction rule ever narrows below "any match satisfies".
    """
    matched = set(_path_list(content.get("matched_paths")))
    return [path for path in _path_list(content.get("produced_paths")) if path not in matched]


def unavailable_delivery_response() -> dict[str, Any]:
    """The answer for every run that delivered what it produced.

    Which is almost all of them, so this is the shape the client sees on the
    ordinary path and must render as "nothing to correct" rather than as an
    error or an unknown.
    """
    return {"available": False, "version": 1}


async def get_run_delivery_response(
    event_store: Any,
    thread_id: str,
    run_id: str,
    *,
    stop_reason: str | None,
) -> dict[str, Any]:
    """Project one run's durable delivery verdict for a client that rejoined.

    ``stop_reason`` is the authority on *whether* this run failed delivery: it
    is committed with the terminal status in the same write, whereas the receipt
    is best-effort and a run can be fenced without one. So a missing or
    unreadable receipt yields "nothing to show" rather than a notice with no
    files under it — a correction that names no file is worse than the silence
    it replaces, exactly as the live frame's parser already decides.
    """
    if stop_reason != DELIVERY_INCOMPLETE_STOP_REASON:
        return unavailable_delivery_response()

    events = await event_store.list_events(
        thread_id,
        run_id,
        event_types=[DELIVERY_EVENT_TYPE],
        limit=2,
    )
    if len(events) != 1:
        return unavailable_delivery_response()
    content = events[0].get("content")
    if not isinstance(content, dict):
        return unavailable_delivery_response()

    paths = undelivered_paths(content)
    if not paths:
        return unavailable_delivery_response()

    return {
        "available": True,
        "version": 1,
        "run_id": run_id,
        # The same sentence the live frame carried, from the same constant, so
        # the notice cannot say one thing live and another after a reload.
        "message": DELIVERY_INCOMPLETE_ERROR,
        "undelivered_paths": paths[:MAX_DISCLOSED_UNDELIVERED_PATHS],
        "undelivered_count": len(paths),
    }
