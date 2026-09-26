"""What this Gateway process carries out for a person outside any run, and how a refusal reaches it.

- ``subagent_batches``: the items of a durable batch this process is
  executing. The account command cancels the batch rows; an executing item
  reads that only at its next lease renewal (``lease_seconds / 3``), so the
  batch service is added as a source (``OwnerHoldings.add_source``) and the
  refusal watch stops a refused owner's items at its look.
- ``mcp_tasks``: the account command records a cancellation request, and a
  Gateway's task loop carries it out at the remote server. A process that
  runs no task loop names ``mcp_tasks`` when it registers, so the command
  reports a task that nothing here will cancel as not reached, rather than
  wait on it.
"""

from __future__ import annotations

from typing import Any

from deerflow.runtime.owner_holdings import OwnerHoldings


def add_durable_work_sources(holdings: OwnerHoldings, *, batches: Any | None) -> None:
    """Add the batch service, when this process has one, as the source of ``subagent_batches``."""
    if batches is not None:
        holdings.add_source("subagent_batches", batches.end_for_owners)


def unreached_durable_work(*, runs_task_loop: bool) -> tuple[str, ...]:
    """The durable work this process has no way to end: MCP tasks, when it runs no task loop."""
    return () if runs_task_loop else ("mcp_tasks",)


__all__ = ["add_durable_work_sources", "unreached_durable_work"]
