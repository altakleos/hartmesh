import base64
import json
import re

import pytest
from pydantic import ValidationError

from app.mcp_tasks.replay_commitment import (
    MCP_TASK_REPLAY_KEYRING_CONFIRMATION_VERSION,
    McpTaskReplayCommitmentError,
    McpTaskReplayKeyring,
)
from deerflow.config.app_config import AppConfig
from deerflow.config.mcp_tasks_config import McpTasksConfig
from deerflow.config.reload_boundary import STARTUP_ONLY_FIELDS, STARTUP_ONLY_PREFIX


def _encoded_key(character: str) -> str:
    return base64.urlsafe_b64encode(character.encode("ascii") * 32).decode("ascii").rstrip("=")


def _replay_keyring(
    *,
    active_key_id: str,
    keys: dict[str, str],
) -> McpTaskReplayKeyring:
    keyring = McpTaskReplayKeyring.from_environment(
        required=True,
        environ={
            "MCP_TASK_REPLAY_HMAC_KEYS": json.dumps(keys),
            "MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID": active_key_id,
        },
    )
    assert keyring is not None
    return keyring


def test_mcp_task_runtime_is_disabled_by_default_and_bounded():
    config = McpTasksConfig()
    assert config.enabled is False
    assert config.poll_interval_seconds == 5
    assert config.lease_seconds == 120
    assert config.max_concurrent_polls == 8
    assert config.max_poll_backoff_seconds == 300
    assert config.input_required_poll_interval_seconds == 60
    assert config.tracking_degraded_after_errors == 3
    assert config.max_result_bytes == 65_536
    assert config.result_preview_max_chars == 2_000

    with pytest.raises(ValidationError):
        McpTasksConfig(poll_interval_seconds=0)
    with pytest.raises(ValidationError):
        McpTasksConfig(max_concurrent_polls=0)
    with pytest.raises(ValidationError):
        McpTasksConfig(max_result_bytes=10)


def test_mcp_task_runtime_is_registered_as_startup_only():
    assert "mcp_tasks" in STARTUP_ONLY_FIELDS
    field = AppConfig.model_fields["mcp_tasks"]
    assert (field.description or "").startswith(STARTUP_ONLY_PREFIX)


def test_enabled_mcp_runtime_requires_dedicated_replay_keyring() -> None:
    assert McpTaskReplayKeyring.from_environment(required=False, environ={}) is None

    with pytest.raises(
        McpTaskReplayCommitmentError,
        match="mcp_task_request_commitment_unavailable",
    ):
        McpTaskReplayKeyring.from_environment(required=True, environ={})


def test_replay_keyring_is_bounded_versioned_and_rotation_safe() -> None:
    keyring = McpTaskReplayKeyring.from_environment(
        required=True,
        environ={
            "MCP_TASK_REPLAY_HMAC_KEYS": ('{"old-v1":"b2xkb2xkb2xkb2xkb2xkb2xkb2xkb2xkb2xkb2xkb2Q","new-v2":"bmV3bmV3bmV3bmV3bmV3bmV3bmV3bmV3bmV3bmV3bmU"}'),
            "MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID": "new-v2",
        },
    )

    assert keyring is not None
    active = keyring.commit({"topic": "alpha"})
    old = keyring.commit({"topic": "alpha"}, key_id="old-v1")
    changed = keyring.commit({"topic": "bravo"}, key_id="old-v1")
    assert active.key_id == "new-v2"
    assert old.key_id == "old-v1"
    assert old.digest != changed.digest
    assert "b2xkb2" not in repr(keyring)

    with pytest.raises(
        McpTaskReplayCommitmentError,
        match="mcp_task_request_commitment_configuration_invalid",
    ):
        McpTaskReplayKeyring.from_environment(
            required=True,
            environ={
                "MCP_TASK_REPLAY_HMAC_KEYS": '{"weak":"dG9vLXNob3J0"}',
                "MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID": "weak",
            },
        )

    with pytest.raises(
        McpTaskReplayCommitmentError,
        match="mcp_task_request_commitment_configuration_invalid",
    ):
        McpTaskReplayKeyring.from_environment(
            required=True,
            environ={
                "MCP_TASK_REPLAY_HMAC_KEYS": '{"padded":"a2tra2tra2tra2tra2tra2tra2tra2tra2tra2tra2s="}',
                "MCP_TASK_REPLAY_HMAC_ACTIVE_KEY_ID": "padded",
            },
        )


def test_replay_keyring_confirmation_is_versioned_order_independent_and_redacted() -> None:
    left = _replay_keyring(
        active_key_id="new-v2",
        keys={
            "old-v1": _encoded_key("o"),
            "new-v2": _encoded_key("n"),
        },
    )
    right = _replay_keyring(
        active_key_id="new-v2",
        keys={
            "new-v2": _encoded_key("n"),
            "old-v1": _encoded_key("o"),
        },
    )

    confirmation = left.confirmation()

    assert confirmation == right.confirmation()
    assert confirmation.version == MCP_TASK_REPLAY_KEYRING_CONFIRMATION_VERSION
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", confirmation.digest)
    assert _encoded_key("o") not in repr(confirmation)
    assert _encoded_key("n") not in repr(confirmation)


def test_replay_keyring_confirmation_detects_same_id_with_different_secret_bytes() -> None:
    left = _replay_keyring(
        active_key_id="v1",
        keys={"v1": _encoded_key("a")},
    )
    right = _replay_keyring(
        active_key_id="v1",
        keys={"v1": _encoded_key("b")},
    )

    assert left.confirmation() != right.confirmation()


def test_replay_keyring_confirmation_detects_active_key_switch() -> None:
    keys = {
        "old-v1": _encoded_key("o"),
        "new-v2": _encoded_key("n"),
    }

    old_active = _replay_keyring(active_key_id="old-v1", keys=keys)
    new_active = _replay_keyring(active_key_id="new-v2", keys=keys)

    assert old_active.confirmation() != new_active.confirmation()


def test_replay_keyring_confirmation_covers_every_retained_rotation_key() -> None:
    old_only = _replay_keyring(
        active_key_id="old-v1",
        keys={"old-v1": _encoded_key("o")},
    )
    additive_rotation = _replay_keyring(
        active_key_id="old-v1",
        keys={
            "old-v1": _encoded_key("o"),
            "new-v2": _encoded_key("n"),
        },
    )

    assert old_only.confirmation() != additive_rotation.confirmation()
