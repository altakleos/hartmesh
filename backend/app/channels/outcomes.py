"""Process-local evidence of an acknowledged refusal to create a run."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from app.channels.execution import ChannelExecutionBusyError, ChannelExecutionContext, ChannelExecutionProtocolError, ChannelExecutionScope
from app.channels.message_bus import OutboundMessage

_ISSUER = object()


@dataclass(frozen=True, init=False)
class CreationRefusal:
    """Opaque host evidence; provider data and model text cannot reconstruct it."""

    _scope: ChannelExecutionScope
    _issuer: object

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


def capture_creation_refusal(context: ChannelExecutionContext, error: ChannelExecutionBusyError) -> CreationRefusal:
    if type(context) is not ChannelExecutionContext or type(error) is not ChannelExecutionBusyError:
        raise ChannelExecutionProtocolError()
    if context.admission.current("run_read") is not True:
        raise ChannelExecutionProtocolError()
    result = object.__new__(CreationRefusal)
    object.__setattr__(result, "_scope", context.scope)
    object.__setattr__(result, "_issuer", _ISSUER)
    return result


def validate_creation_refusal(value: object, scope: ChannelExecutionScope) -> Literal["thread_busy"]:
    if type(value) is not CreationRefusal or getattr(value, "_issuer", None) is not _ISSUER or getattr(value, "_scope", None) != scope:
        raise ChannelExecutionProtocolError()
    return "thread_busy"


@dataclass
class CreationRefusedMessage(OutboundMessage):
    """Host-only bus subtype retaining the exact refused publication scope."""

    creation_refusal: CreationRefusal | None = None

    def __post_init__(self) -> None:
        proof = self.creation_refusal
        if type(proof) is not CreationRefusal:
            raise ChannelExecutionProtocolError()
        scope = proof._scope
        validate_creation_refusal(proof, scope)
        if (self.chat_id, self.thread_id, self.owner_user_id, self.connection_id) != (scope.chat_id, scope.thread_id, scope.owner_user_id, scope.connection_id):
            raise ChannelExecutionProtocolError()
