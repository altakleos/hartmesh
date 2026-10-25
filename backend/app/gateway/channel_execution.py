"""Pure wire types for trusted, owner-scoped channel execution.

A receipt describes server admission. It does not establish final-output
completeness or authorize a second creating request after uncertainty.
"""

from collections.abc import Mapping
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

CHANNEL_EXECUTION_REQUEST_HEADER = "X-DeerFlow-Require-Channel-Execution"
CHANNEL_EXECUTION_RESPONSE_HEADER = "X-DeerFlow-Channel-Execution-Version"
CHANNEL_EXECUTION_VERSION = 1
CHANNEL_EXECUTION_BUSY_CODE = "thread_busy"
CHANNEL_TURN_ID_KEY = "channel_turn_id"

_Identity = Annotated[str, Field(strict=True, min_length=1, max_length=128)]
_ThreadIdentity = Annotated[str, Field(strict=True, min_length=1, max_length=64)]
_PublicationIdentity = Annotated[str, Field(strict=True, min_length=32, max_length=32, pattern=r"^[0-9a-f]{32}$")]


class ChannelExecutionRequest(BaseModel):
    """An explicit version rejects older endpoints before model admission."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    version: Literal[1]
    publication_id: _PublicationIdentity

    @field_validator("version", mode="before")
    @classmethod
    def literal_integer_version(cls, value):
        # Literal[1] alone also accepts True and numerically equal values.
        if type(value) is not int or value != CHANNEL_EXECUTION_VERSION:
            raise ValueError("Unsupported channel execution version")
        return value


class ChannelExecutionReceipt(ChannelExecutionRequest):
    """Exact admitted owner, original thread and resolved model target."""

    owner_user_id: _Identity
    thread_id: _ThreadIdentity
    assistant_id: _Identity
    agent_name: _Identity | None


class ChannelExecutionProofError(ValueError):
    """Malformed stored proof must not become a legacy nonmatch."""

    def __init__(self):
        super().__init__("Channel execution proof is unavailable")


def receipt_from_run_kwargs(kwargs: object) -> ChannelExecutionReceipt | None:
    """Return None only for the absent key in a real kwargs mapping."""
    if not isinstance(kwargs, Mapping):
        raise ChannelExecutionProofError()
    try:
        if "channel_execution" not in kwargs:
            return None
        value = kwargs["channel_execution"]
        return ChannelExecutionReceipt.model_validate(dict(value) if isinstance(value, Mapping) else value)
    except Exception:
        raise ChannelExecutionProofError() from None
