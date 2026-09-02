from __future__ import annotations

import pytest

from deerflow.runtime import (
    ExecutionRecoveryPayloadV1,
    project_execution_recovery_config,
)


def _payload(**overrides):
    values = {
        "input_kind": "graph",
        "input_value": {
            "messages": [
                {
                    "role": "user",
                    "content": "inspect upload",
                    "attachments": [{"name": "note.txt", "url": "data:text/plain;base64,b2s="}],
                }
            ]
        },
        "config": {
            "recursion_limit": 100,
            "configurable": {
                "thread_id": "thread-recovery",
                "checkpoint_ns": "",
            },
            "context": {
                "mode": "chat",
                "disable_clarification": True,
            },
        },
        "stream_modes": ("values", "messages-tuple"),
        "stream_subgraphs": True,
        "interrupt_before": ("tools",),
        "interrupt_after": "*",
    }
    values.update(overrides)
    return ExecutionRecoveryPayloadV1(**values)


def test_recovery_payload_round_trips_exact_graph_input_and_options() -> None:
    payload = _payload()

    restored = ExecutionRecoveryPayloadV1.from_persisted(payload.to_persisted())

    assert restored == payload
    assert restored.input_value["messages"][0]["attachments"][0]["url"].endswith("b2s=")
    assert restored.stream_modes == ("values", "messages-tuple")
    assert restored.interrupt_after == "*"


def test_recovery_payload_preserves_command_resume_separately() -> None:
    payload = _payload(
        input_kind="command_resume",
        input_value={"approved": True, "answer": "continue"},
    )

    restored = ExecutionRecoveryPayloadV1.from_persisted(payload.to_persisted())

    assert restored.input_kind == "command_resume"
    assert restored.input_value == {"approved": True, "answer": "continue"}


@pytest.mark.parametrize(
    "config",
    [
        {"context": {"secrets": {"API_KEY": "raw"}}},
        {"metadata": {"auth_token": "raw"}},
        {"context": {"access_token": "raw"}},
        {"context": {"nested": {"password": "raw"}}},
        {"context": {"nested": {"clientSecret": "raw"}}},
        {"context": {"nested": {"private-key": "raw"}}},
        {"context": {"apiKey": "raw"}},
        {"context": {"bearer": "raw"}},
        {"context": {"authorization": "raw"}},
        {"context": {"headers": {"x-custom-auth": "raw"}}},
        {"context": {"base_url": "https://user:pass@example.test/api"}},
        {"context": {"callback_url": "https://example.test/cb?access_token=raw"}},
        {"context": {"object_url": "https://example.test/file?X-Amz-Signature=raw"}},
        {"context": {"return_url": "https://example.test/#token=raw"}},
    ],
)
def test_recovery_payload_rejects_secret_bearing_config(config) -> None:
    with pytest.raises(ValueError, match="recovery_payload_secret_config"):
        _payload(config=config)


def test_recovery_payload_rejects_unclassified_public_url() -> None:
    with pytest.raises(
        ValueError,
        match="recovery_payload_config_context_keys_invalid",
    ):
        _payload(
            config={
                "recursion_limit": 100,
                "configurable": {"thread_id": "thread-recovery"},
                "context": {
                    "base_url": "https://example.test/api?region=us-east-1",
                },
            }
        )


@pytest.mark.parametrize(
    "config",
    [
        {"callbacks": []},
        {"max_concurrency": 4},
        {"session": "sk_live_unclassified_secret"},
        {"configurable": {"thread_id": "thread-recovery", "jwt": "raw"}},
        {
            "configurable": {"thread_id": "thread-recovery"},
            "context": {"session": "sk_live_unclassified_secret"},
        },
        {
            "configurable": {"thread_id": "thread-recovery"},
            "context": {"connection_string": "opaque"},
        },
    ],
)
def test_recovery_payload_rejects_every_unclassified_config_key(config) -> None:
    with pytest.raises(ValueError, match="recovery_payload_config_.*invalid"):
        _payload(config=config)


def test_recovery_config_projection_is_minimal_and_semantic() -> None:
    projected = project_execution_recovery_config(
        {
            "recursion_limit": 250,
            "run_name": "trace-only",
            "metadata": {"correlation": "trace-only"},
            "configurable": {
                "thread_id": "thread-recovery",
                "checkpoint_ns": "",
                "checkpoint_id": "checkpoint-1",
                "checkpoint_map": {"": "checkpoint-1"},
                "model_name": "basic",
                "thinking_enabled": True,
                "mode": "chat",
            },
            "context": {
                "thread_id": "thread-recovery",
                "user_id": "server-user",
                "user_role": "member",
                "oauth_provider": "local",
                "oauth_id": "subject",
                "is_internal": False,
                "agent_name": "lead-agent",
                "mode": "chat",
                "disable_clarification": True,
            },
        }
    )

    assert projected == {
        "recursion_limit": 250,
        "configurable": {
            "thread_id": "thread-recovery",
            "checkpoint_ns": "",
            "checkpoint_id": "checkpoint-1",
            "checkpoint_map": {"": "checkpoint-1"},
        },
        "context": {
            "mode": "chat",
            "disable_clarification": True,
        },
    }


def test_recovery_payload_rejects_unknown_or_malformed_persistence() -> None:
    persisted = _payload().to_persisted()
    persisted["forged"] = True

    with pytest.raises(ValueError, match="recovery_payload_fields_invalid"):
        ExecutionRecoveryPayloadV1.from_persisted(persisted)


def test_recovery_payload_rejects_oversize_values() -> None:
    with pytest.raises(ValueError, match="recovery_payload_too_large"):
        _payload(input_value={"messages": [{"content": "x" * 300_000}]})
