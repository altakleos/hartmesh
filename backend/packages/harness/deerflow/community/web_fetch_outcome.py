"""What a ``web_fetch`` provider tells the model and the runtime when it has no page.

Shared by every first-party fetch provider so a refusal reads the same and
is typed the same whichever path produced it. Two readers:

* the model reads one bounded sentence that names the address (when the
  refusal is the address's) and says what to do next;
* the runtime reads ``deerflow_tool_meta`` stamped by the tool itself, with
  ``error_scope`` set from what the provider saw on the transport -- the one
  fact the keyword classifier in ``tool_result_meta`` cannot recover from the
  sentence. ``origin`` is that page refusing; ``provider`` is the fetch path
  refusing every page this turn, which ``ProviderRefusalMiddleware`` acts on
  by withdrawing the tool for the rest of the run.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

from langchain_core.messages import ToolMessage
from langgraph.types import Command

from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY

__all__ = ["FetchRefusal", "describe_refusal", "refusal_meta", "stamped_result", "success_meta"]

#: A refused address is the model's own text, echoed clean and clipped.
MAX_ECHOED_URL = 160
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")


@dataclass(frozen=True, slots=True)
class FetchRefusal:
    """Why no page came back, in the tool metadata's own vocabulary.

    ``scope`` is the one fact the caller must not guess: ``origin`` means this
    address refused and another may not; ``provider`` means the path refused
    and no address will do better this turn.
    """

    scope: Literal["origin", "provider"]
    error_type: str
    reason: str
    status_code: int | None = None


def _echo(url: str) -> str:
    clean = _CONTROL_CHARS.sub("?", url)
    return clean if len(clean) <= MAX_ECHOED_URL else clean[: MAX_ECHOED_URL - 1] + "…"


def describe_refusal(url: str, refusal: FetchRefusal) -> str:
    """The sentence the model reads. Bounded; says what to do next."""
    if refusal.scope == "provider":
        return f"Error: web_fetch is unavailable for the rest of this turn: {refusal.reason}. Do not call it again; answer from the search results you already have and say that sources could not be fetched."
    return f"Error: could not fetch {_echo(url)}: {refusal.reason}. Use another source from the search results instead of retrying this address."


def refusal_meta(refusal: FetchRefusal) -> dict[str, Any]:
    """The typed outcome, from the transport rather than from the sentence."""
    provider = refusal.scope == "provider"
    return {
        "status": "error",
        "error_type": refusal.error_type,
        "recoverable_by_model": not provider,
        "recommended_next_action": "stop" if provider else "try_alternative",
        "source": "tool_return",
        "error_scope": refusal.scope,
    }


def success_meta() -> dict[str, Any]:
    return {
        "status": "success",
        "error_type": None,
        "recoverable_by_model": True,
        "recommended_next_action": "continue",
        "source": "tool_return",
        "error_scope": "origin",
    }


def stamped_result(text: str, meta: dict[str, Any], tool_call_id: str, *, name: str = "web_fetch") -> str | Command:
    """The tool result carrying its own stamp, or the bare text when no call can carry one."""
    if not tool_call_id:
        return text
    status = "error" if meta.get("status") == "error" else "success"
    return Command(update={"messages": [ToolMessage(text, tool_call_id=tool_call_id, name=name, status=status, additional_kwargs={TOOL_META_KEY: meta})]})
