"""Committed host recovery metadata, separate from ordinary file completion."""

from datetime import UTC, datetime

HOST_RESOLUTIONS = frozenset(("host-settled", "host-reconciled", "host-discarded"))


def host_settlement_time(result):
    if not isinstance(result, dict) or "host_settled_at" not in result:
        return None
    if result.get("resolution") not in HOST_RESOLUTIONS or not isinstance(result["host_settled_at"], str):
        raise ValueError("Host settlement metadata is unconfirmed")
    try:
        moment = datetime.fromisoformat(result["host_settled_at"])
    except ValueError:
        raise ValueError("Host settlement metadata is unconfirmed") from None
    if moment.tzinfo is None or moment.utcoffset() != UTC.utcoffset(moment):
        raise ValueError("Host settlement metadata is unconfirmed")
    return moment


def host_settlement_result(result, *, at=None):
    """Call inside the terminal SQL transaction; retries retain the first time."""
    if not isinstance(result, dict) or result.get("resolution") not in HOST_RESOLUTIONS:
        raise ValueError("Host settlement requires a confirmed host resolution")
    if host_settlement_time(result) is not None:
        return dict(result)
    moment = datetime.now(UTC) if at is None else at
    if not isinstance(moment, datetime) or moment.tzinfo is None or moment.utcoffset() != UTC.utcoffset(moment):
        raise ValueError("Host settlement requires a UTC completion time")
    return {**result, "host_settled_at": moment.isoformat()}
