"""GuardrailProvider protocol and data structures for pre-tool-call authorization."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from deerflow_extension_api import InvocationIdentityV1, SealedOriginV1


@dataclass
class GuardrailRequest:
    """Context passed to the provider for each tool call."""

    tool_name: str
    tool_input: dict[str, Any]
    agent_id: str | None = None
    thread_id: str | None = None
    is_subagent: bool = False
    timestamp: str = ""
    user_id: str | None = None
    user_role: str | None = None
    oauth_provider: str | None = None
    oauth_id: str | None = None
    run_id: str | None = None
    tool_call_id: str | None = None
    # Authorization identity fields (populated by GuardrailMiddleware from
    # runtime context). Default values ensure backward compatibility for
    # providers that don't read them.
    channel_user_id: str | None = None
    is_internal: bool = False
    authz_attributes: dict[str, Any] = field(default_factory=dict)
    identity: InvocationIdentityV1 | None = None
    origin: SealedOriginV1 | None = None


@dataclass
class GuardrailReason:
    """Structured reason for an allow/deny decision (OAP reason object)."""

    code: str
    message: str = ""


@dataclass
class GuardrailDecision:
    """Provider's allow/deny verdict (aligned with OAP Decision object)."""

    allow: bool
    reasons: list[GuardrailReason] = field(default_factory=list)
    policy_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    provider_receipt: object | None = field(default=None, repr=False, compare=False)


_PROVIDER_RECEIPT: ContextVar[object | None] = ContextVar(
    "deerflow_guardrail_provider_receipt",
    default=None,
)


@contextmanager
def bind_guardrail_provider_receipt(receipt: object | None) -> Iterator[None]:
    """Make one allowed provider result available only during its tool call."""

    if receipt is None:
        # An independent inner guardrail must not mask the authorization
        # adapter's outer receipt. With no outer receipt this still exposes
        # the default ``None`` and required operational preparation fails shut.
        yield
        return
    token = _PROVIDER_RECEIPT.set(receipt)
    try:
        yield
    finally:
        _PROVIDER_RECEIPT.reset(token)


def current_guardrail_provider_receipt() -> object | None:
    """Return the receipt bound to the current tool-call execution context."""

    return _PROVIDER_RECEIPT.get()


@runtime_checkable
class GuardrailProvider(Protocol):
    """Contract for pluggable tool-call authorization.

    Any class with these methods works - no base class required.
    Providers are loaded by class path via resolve_variable(),
    the same mechanism DeerFlow uses for models, tools, and sandbox.
    """

    name: str

    def evaluate(self, request: GuardrailRequest) -> GuardrailDecision:
        """Evaluate whether a tool call should proceed."""
        ...

    async def aevaluate(self, request: GuardrailRequest) -> GuardrailDecision:
        """Async variant."""
        ...
