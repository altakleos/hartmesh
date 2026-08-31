"""Neutral Guardrail->observer authorization outcome contract.

GuardrailMiddleware writes an AuthorizationOutcome into the per-run runtime
context; an observer pops it to record which policy actually decided a given
tool call. Neither side imports the other -- both depend only on this
contract. The context key is ``__``-prefixed so Gateway build_run_config
strips any caller-supplied forgery, matching ``__run_journal`` /
``__active_skill_secrets``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Literal

AUTHORIZATION_OUTCOME_CONTEXT_KEY = "__authorization_outcome"

#: A run with no observer never pops entries (``pop_authorization_outcome`` has
#: no production caller yet), so an authorization-enabled deployment would
#: otherwise grow this store for the life of the run, one entry per tool call.
#: Capping it bounds that growth to a fixed footprint; the oldest entries are
#: evicted first since a stale decision is the least likely to still be wanted.
_MAX_TRACKED_OUTCOMES = 500
_MAX_OUTCOMES_PER_CALL = 16


@dataclass(frozen=True)
class AuthorizationOutcome:
    decision: Literal["allowed", "denied"]
    policy_id: str
    policy_version: str
    reason_codes: tuple[str, ...] = ()
    kind: Literal["authorization", "guardrail"] = "guardrail"

    @property
    def decision_ref(self) -> str:
        """Return a stable bounded reference without provider payload text."""

        encoded = json.dumps(
            {
                "version": 1,
                "kind": self.kind,
                "decision": self.decision,
                "policy_id": self.policy_id,
                "policy_version": self.policy_version,
                "reason_codes": list(self.reason_codes),
            },
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return "pd_" + hashlib.sha256(encoded).hexdigest()


def put_authorization_outcome(context: object, tool_call_id: object, outcome: AuthorizationOutcome) -> None:
    if not isinstance(context, dict) or not tool_call_id:
        return
    store = context.get(AUTHORIZATION_OUTCOME_CONTEXT_KEY)
    if not isinstance(store, dict):
        store = {}
        context[AUTHORIZATION_OUTCOME_CONTEXT_KEY] = store
    existing = store.get(tool_call_id)
    outcomes = existing if isinstance(existing, tuple) and all(isinstance(item, AuthorizationOutcome) for item in existing) else ()
    store[tool_call_id] = (*outcomes, outcome)[-_MAX_OUTCOMES_PER_CALL:]
    while len(store) > _MAX_TRACKED_OUTCOMES:
        store.pop(next(iter(store)))


def pop_authorization_outcome(context: object, tool_call_id: object) -> AuthorizationOutcome | None:
    if not isinstance(context, dict) or not tool_call_id:
        return None
    store = context.get(AUTHORIZATION_OUTCOME_CONTEXT_KEY)
    if not isinstance(store, dict):
        return None
    value = store.pop(tool_call_id, None)
    if isinstance(value, AuthorizationOutcome):
        return value
    if isinstance(value, tuple) and value and all(isinstance(item, AuthorizationOutcome) for item in value):
        return value[-1]
    return None


def pop_policy_outcomes(context: object, tool_call_id: object) -> tuple[AuthorizationOutcome, ...]:
    """Consume every ordered authorization/guardrail decision for one call."""

    if not isinstance(context, dict) or not tool_call_id:
        return ()
    store = context.get(AUTHORIZATION_OUTCOME_CONTEXT_KEY)
    if not isinstance(store, dict):
        return ()
    value = store.pop(tool_call_id, None)
    if isinstance(value, AuthorizationOutcome):
        return (value,)
    if isinstance(value, tuple) and all(isinstance(item, AuthorizationOutcome) for item in value):
        return value
    return ()


def peek_policy_outcomes(
    context: object,
    tool_call_id: object,
) -> tuple[AuthorizationOutcome, ...]:
    """Read ordered decisions for one call without exposing storage shape."""

    if not isinstance(context, dict) or not tool_call_id:
        return ()
    store = context.get(AUTHORIZATION_OUTCOME_CONTEXT_KEY)
    if not isinstance(store, dict):
        return ()
    value = store.get(tool_call_id)
    if isinstance(value, AuthorizationOutcome):
        return (value,)
    if isinstance(value, tuple) and all(isinstance(item, AuthorizationOutcome) for item in value):
        return value
    return ()
