"""Operator policy is strict, independent and separate from customer state."""

import pytest
from pydantic import ValidationError

from deerflow.config.app_config import AppConfig


def config(**values):
    return AppConfig.model_validate({"models": [], "sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "tools": [], "tool_groups": [], **values})


def test_missing_policy_denies_all_customer_management():
    assert config().customer_administration.model_dump() == {
        "plugin_management": False,
        "local_skill_management": False,
        "local_mcp_management": False,
    }
    assert config().approved_local_mcp_definitions == []


@pytest.mark.parametrize("field", ["plugin_management", "local_skill_management", "local_mcp_management"])
def test_each_permission_is_independent(field):
    policy = config(customer_administration={field: True}).customer_administration
    assert policy.model_dump() == {name: name == field for name in ("plugin_management", "local_skill_management", "local_mcp_management")}


@pytest.mark.parametrize("value", ["true", "false", 0, 1, None, [], {}])
@pytest.mark.parametrize("field", ["plugin_management", "local_skill_management", "local_mcp_management"])
def test_permissions_refuse_non_boolean_values(field, value):
    with pytest.raises(ValidationError):
        config(customer_administration={field: value})


@pytest.mark.parametrize("policy", [None, [], "enabled", {"plugin_management": True, "unknown_permission": True}])
def test_malformed_or_unknown_policy_is_not_activated(policy):
    with pytest.raises(ValidationError):
        config(customer_administration=policy)


def test_approval_is_an_operator_owned_exact_launch_definition():
    launch = {"type": "stdio", "command": "npx", "args": ["-y", "example-server@1.0.0"], "env": {"MODE": "read"}, "cwd": "/opt/provider"}
    parsed = config(approved_local_mcp_definitions=[launch]).approved_local_mcp_definitions
    assert parsed[0].model_dump() == launch
    assert not config(approved_local_mcp_definitions=[launch]).customer_administration.local_mcp_management


@pytest.mark.parametrize("launch", [{"command": ""}, {"type": "http", "command": "npx"}, {"command": "npx", "approved": True}, {"command": "npx", "args": [1]}, {"command": "npx", "env": {"MODE": 1}}])
def test_incomplete_or_self_approved_definitions_fail_validation(launch):
    with pytest.raises(ValidationError):
        config(approved_local_mcp_definitions=[launch])
