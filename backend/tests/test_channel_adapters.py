"""Native-channel adapter trust facts used by accepted invocations."""

from __future__ import annotations

from dataclasses import replace

import pytest

from app.gateway.services import _GatewayLaunchNormalizer
from app.runtime.invocation import (
    InternalLaunchIntent,
    InternalNativeChannelFacts,
    InternalSourceKind,
)


def _intent() -> InternalLaunchIntent:
    return InternalLaunchIntent(
        thread_id="thread-1",
        assistant_id="lead_agent",
        context={
            "channel_name": "slack",
            "channel_user_id": "platform-user",
            "agent_name": None,
        },
        source_kind=InternalSourceKind.native_channel,
        owner_user_id="owner-1",
        native_channel=InternalNativeChannelFacts(
            provider="slack",
            connection_id="connection-1",
            workspace_id="workspace-1",
            chat_id="chat-1",
            topic_id=None,
            provider_message_id=None,
            channel_user_id="platform-user",
            resolved_assistant_id="lead_agent",
            resolved_agent_name=None,
        ),
    )


def test_provider_without_stable_event_id_keeps_null_at_runtime_boundary() -> None:
    facts = _GatewayLaunchNormalizer._validate_native_channel_facts(_intent())
    assert facts.provider_message_id is None


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("provider", "discord", "provider"),
        ("channel_user_id", "forged-sender", "sender"),
        ("resolved_assistant_id", "forged-assistant", "assistant"),
        ("resolved_agent_name", "forged-agent", "agent"),
    ],
)
def test_contradictory_authenticated_channel_facts_are_rejected(
    field: str,
    value: str,
    message: str,
) -> None:
    intent = _intent()
    assert intent.native_channel is not None
    forged = replace(
        intent,
        native_channel=replace(intent.native_channel, **{field: value}),
    )
    with pytest.raises(ValueError, match=message):
        _GatewayLaunchNormalizer._validate_native_channel_facts(forged)
