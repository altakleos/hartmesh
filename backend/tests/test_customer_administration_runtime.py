"""Immutable host policy cannot be inferred from caller metadata or roles."""

from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest

from deerflow.config.app_config import AppConfig

LAUNCH = {"type": "stdio", "command": "npx", "args": ["-y", "example-server@1.0.0"], "env": {"MODE": "read"}, "cwd": "/opt/provider"}


def configured(**flags):
    return AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "customer_administration": flags, "approved_local_mcp_definitions": [LAUNCH]})


def test_policy_is_detached_from_hot_configuration():
    from deerflow.runtime.customer_administration import capture_customer_administration_policy, require_approved_local_mcp_definition, require_customer_management

    config = configured(local_mcp_management=True)
    policy = capture_customer_administration_policy(config)
    config.customer_administration.local_mcp_management = False
    config.approved_local_mcp_definitions[0].env["MODE"] = "changed"
    require_customer_management(policy, "local_mcp_management")
    require_approved_local_mcp_definition(policy, LAUNCH)
    with pytest.raises(FrozenInstanceError):
        policy.local_mcp_management = False


@pytest.mark.parametrize("policy", [None, {"plugin_management": True}, SimpleNamespace(plugin_management=True)])
def test_missing_or_forged_policy_denies_even_with_admin_metadata(policy):
    from deerflow.runtime.customer_administration import CustomerManagementDenied, require_customer_management

    with pytest.raises(CustomerManagementDenied):
        require_customer_management(policy, "plugin_management")


@pytest.mark.parametrize("permission", ["plugin_management", "local_skill_management", "local_mcp_management"])
def test_default_snapshot_denies_each_operation(permission):
    from deerflow.runtime.customer_administration import CustomerManagementDenied, capture_customer_administration_policy, require_customer_management

    with pytest.raises(CustomerManagementDenied):
        require_customer_management(capture_customer_administration_policy(configured()), permission)


@pytest.mark.parametrize(
    "delta", [{"command": "uvx"}, {"args": ["example-server@1.0.0", "-y"]}, {"env": {"MODE": "write"}}, {"env": {"MODE": "read", "EXTRA": "value"}}, {"cwd": "/other"}, {"type": "http"}, {"unknown_launch_option": "value"}]
)
def test_approval_binds_every_launch_component_and_refuses_unknown_knobs(delta):
    from deerflow.runtime.customer_administration import CustomerManagementDenied, capture_customer_administration_policy, require_approved_local_mcp_definition

    with pytest.raises(CustomerManagementDenied):
        require_approved_local_mcp_definition(capture_customer_administration_policy(configured(local_mcp_management=True)), {**LAUNCH, **delta})


def test_client_markers_do_not_approve_a_different_definition():
    from deerflow.runtime.customer_administration import CustomerManagementDenied, capture_customer_administration_policy, require_approved_local_mcp_definition

    candidate = {**LAUNCH, "args": ["different-server@1.0.0"], "capability": {"approved": True, "id": "provider"}}
    with pytest.raises(CustomerManagementDenied):
        require_approved_local_mcp_definition(capture_customer_administration_policy(configured(local_mcp_management=True)), candidate)


def test_transport_alias_and_non_launch_metadata_do_not_change_exact_approval():
    from deerflow.runtime.customer_administration import capture_customer_administration_policy, require_approved_local_mcp_definition

    candidate = {**LAUNCH, "transport": "stdio", "enabled": False, "description": "Owner label", "capability": {"id": "owner-installation"}}
    require_approved_local_mcp_definition(capture_customer_administration_policy(configured(local_mcp_management=True)), candidate)


def test_host_scopes_are_nested_and_missing_scope_denies():
    from deerflow.runtime.customer_administration import bind_customer_administration_policy, capture_customer_administration_policy, current_customer_administration_policy

    enabled = capture_customer_administration_policy(configured(local_skill_management=True))
    disabled = capture_customer_administration_policy(configured())
    assert not current_customer_administration_policy().local_skill_management
    with bind_customer_administration_policy(enabled):
        assert current_customer_administration_policy() is enabled
        with bind_customer_administration_policy(disabled):
            assert current_customer_administration_policy() is disabled
        assert current_customer_administration_policy() is enabled
    assert not current_customer_administration_policy().local_skill_management


def test_worker_discards_caller_policy_and_installs_only_host_snapshot():
    from deerflow.runtime.customer_administration import CUSTOMER_ADMINISTRATION_CONTEXT_KEY, capture_customer_administration_policy
    from deerflow.runtime.runs.worker import _build_runtime_context, _install_runtime_context

    spoofed = capture_customer_administration_policy(configured(local_skill_management=True))
    host = capture_customer_administration_policy(configured())
    caller = {CUSTOMER_ADMINISTRATION_CONTEXT_KEY: spoofed, "ordinary": "data"}
    empty = _build_runtime_context("thread", "run", caller)
    assert CUSTOMER_ADMINISTRATION_CONTEXT_KEY not in empty
    runtime = _build_runtime_context("thread", "run", caller, customer_administration_policy=host)
    assert runtime[CUSTOMER_ADMINISTRATION_CONTEXT_KEY] is host
    config = {"context": dict(caller), "configurable": dict(caller)}
    _install_runtime_context(config, runtime)
    assert config["context"][CUSTOMER_ADMINISTRATION_CONTEXT_KEY] is host
    assert CUSTOMER_ADMINISTRATION_CONTEXT_KEY not in config["configurable"]
    assert config["context"]["ordinary"] == "data"


def test_embedded_client_inherits_active_host_instead_of_hot_config(monkeypatch):
    from deerflow.client import DeerFlowClient
    from deerflow.runtime.customer_administration import bind_customer_administration_policy, capture_customer_administration_policy

    host_config = configured()
    host_policy = capture_customer_administration_policy(host_config)
    hot_config = configured(local_skill_management=True)
    monkeypatch.setattr("deerflow.client.get_app_config", lambda: hot_config)
    with bind_customer_administration_policy(host_policy):
        hosted = DeerFlowClient()
    assert hosted._customer_administration_policy is host_policy
    assert not hosted._customer_administration_policy.local_skill_management
    standalone = DeerFlowClient()
    assert standalone._customer_administration_policy.local_skill_management
    hot_config.customer_administration.local_skill_management = False
    assert standalone._customer_administration_policy.local_skill_management


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source,role,owner_role,expected",
    [("session", "admin", None, True), ("pat", "admin", None, False), ("session", "user", None, False), ("internal", "internal", None, False), ("internal", "internal", "admin", True), ("internal", "internal", "user", False)],
)
async def test_run_management_actor_preserves_request_authority(source, role, owner_role, expected):
    from types import SimpleNamespace

    from app.gateway.customer_administration import resolve_customer_management_actor

    request = SimpleNamespace(state=SimpleNamespace(auth_source=source, user=SimpleNamespace(id="caller", system_role=role)))
    owner = SimpleNamespace(id="owner", system_role=owner_role) if owner_role else None
    actor = await resolve_customer_management_actor(request, internal_owner_user=owner)
    assert actor.administrator is expected
    assert actor.owner_id == ("owner" if owner else None if source == "internal" else "caller")


@pytest.mark.asyncio
@pytest.mark.parametrize("administrator", [False, True])
async def test_worker_management_binding_uses_host_actor_not_account_role(monkeypatch, administrator):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from deerflow.runtime.customer_administration import CustomerManagementActor, customer_management_actor_is_admin
    from deerflow.runtime.runs import worker

    observed = []

    async def execute(*args, **kwargs):
        observed.append(customer_management_actor_is_admin())

    monkeypatch.setattr(worker, "_run_agent", execute)
    ctx = worker.RunContext(checkpointer=None, customer_management_actor=CustomerManagementActor(owner_id="owner", administrator=administrator))
    await worker.run_agent(Mock(), None, SimpleNamespace(run_id="fixture-run", status="success"), ctx=ctx, config={"context": {"user_role": "admin"}})
    assert observed == [administrator]


def test_runtime_management_actor_carrier_is_server_owned():
    from deerflow.runtime.customer_administration import CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY, CustomerManagementActor
    from deerflow.runtime.runs.worker import _build_runtime_context, _install_runtime_context

    spoofed = CustomerManagementActor(owner_id="forged", administrator=True)
    host = CustomerManagementActor(owner_id="owner", administrator=False)
    caller = {CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY: spoofed}
    assert CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY not in _build_runtime_context("thread", "run", caller)
    runtime = _build_runtime_context("thread", "run", caller, customer_management_actor=host)
    config = {"context": dict(caller), "configurable": dict(caller)}
    _install_runtime_context(config, runtime)
    assert config["context"][CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY] is host
    assert CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY not in config["configurable"]


@pytest.mark.parametrize(
    "previous,candidate",
    [
        (None, {"type": "http", "url": "https://example.com/mcp"}),
        ({"type": "http", "url": "https://example.com/mcp"}, None),
        ({"type": "http", "url": "https://example.com/mcp"}, {"type": "http", "url": "https://example.com/other"}),
        (LAUNCH, {**LAUNCH, "description": "Customer label"}),
    ],
)
def test_remote_preferences_and_unchanged_local_launch_need_no_new_delegation(previous, candidate):
    from deerflow.runtime.customer_administration import DENIED_CUSTOMER_ADMINISTRATION, require_mcp_management_transition

    require_mcp_management_transition(DENIED_CUSTOMER_ADMINISTRATION, previous, candidate)


@pytest.mark.parametrize(
    "previous,candidate",
    [(None, LAUNCH), (LAUNCH, None), (LAUNCH, {"type": "http", "url": "https://example.com/mcp"}), (LAUNCH, {**LAUNCH, "enabled": False}), ({**LAUNCH, "enabled": False}, LAUNCH), (LAUNCH, {**LAUNCH, "args": ["other-server@1.0.0"]})],
)
def test_local_launch_changes_always_need_active_delegation(previous, candidate):
    from deerflow.runtime.customer_administration import DENIED_CUSTOMER_ADMINISTRATION, CustomerManagementDenied, require_mcp_management_transition

    with pytest.raises(CustomerManagementDenied):
        require_mcp_management_transition(DENIED_CUSTOMER_ADMINISTRATION, previous, candidate)


def test_unchanged_operator_extras_do_not_reapprove_a_local_sibling():
    from deerflow.runtime.customer_administration import DENIED_CUSTOMER_ADMINISTRATION, CustomerManagementDenied, require_mcp_management_transition

    existing = {**LAUNCH, "operator_note": "Existing deployment metadata"}
    require_mcp_management_transition(DENIED_CUSTOMER_ADMINISTRATION, existing, dict(existing))
    with pytest.raises(CustomerManagementDenied):
        require_mcp_management_transition(DENIED_CUSTOMER_ADMINISTRATION, existing, {**existing, "operator_note": "Changed"})


def test_embedded_stream_authority_is_scoped_to_each_step_and_close():
    from deerflow.client import _stream_with_customer_administration_scope
    from deerflow.runtime.customer_administration import (
        CUSTOMER_ADMINISTRATION_CONTEXT_KEY,
        CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY,
        CustomerManagementActor,
        capture_customer_administration_policy,
        current_customer_administration_policy,
        customer_management_actor_is_admin,
    )

    policy = capture_customer_administration_policy(configured(local_skill_management=True))
    observed = []

    def items():
        try:
            observed.append((current_customer_administration_policy(), customer_management_actor_is_admin()))
            yield "item"
        finally:
            observed.append((current_customer_administration_policy(), customer_management_actor_is_admin()))

    stream = _stream_with_customer_administration_scope(items(), {CUSTOMER_ADMINISTRATION_CONTEXT_KEY: policy, CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY: CustomerManagementActor(owner_id="owner", administrator=True)})
    assert next(stream) == "item"
    assert not current_customer_administration_policy().local_skill_management
    assert not customer_management_actor_is_admin()
    stream.close()
    assert observed == [(policy, True), (policy, True)]
    assert not current_customer_administration_policy().local_skill_management


def test_typed_owner_authority_cannot_override_bound_token_restrictions():
    from deerflow.runtime.customer_administration import CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY, CustomerManagementActor, bind_customer_management_actor_role, customer_management_actor_is_admin

    context = {"user_id": "owner", CUSTOMER_MANAGEMENT_ACTOR_CONTEXT_KEY: CustomerManagementActor(owner_id="owner", administrator=True)}
    assert customer_management_actor_is_admin(context)
    with bind_customer_management_actor_role("user"):
        assert not customer_management_actor_is_admin(context)
    context["user_id"] = "another-owner"
    assert not customer_management_actor_is_admin(context)


def test_delegated_disable_preserves_unchanged_legacy_extras_without_new_launch_approval():
    from deerflow.runtime.customer_administration import CustomerAdministrationPolicy, CustomerManagementDenied, require_mcp_management_transition

    policy = CustomerAdministrationPolicy(local_mcp_management=True)
    existing = {**LAUNCH, "enabled": True, "operator_note": "Existing metadata"}
    require_mcp_management_transition(policy, existing, {**existing, "enabled": False})
    with pytest.raises(CustomerManagementDenied):
        require_mcp_management_transition(policy, existing, {**existing, "enabled": False, "operator_note": "Changed metadata"})


@pytest.mark.asyncio
async def test_concurrent_host_scopes_do_not_share_policy_or_role():
    import asyncio

    from deerflow.runtime.customer_administration import (
        bind_customer_administration_policy,
        bind_customer_management_actor_role,
        capture_customer_administration_policy,
        current_customer_administration_policy,
        customer_management_actor_is_admin,
    )

    ready = asyncio.Event()
    released = asyncio.Event()
    enabled = capture_customer_administration_policy(configured(local_skill_management=True))
    disabled = capture_customer_administration_policy(configured())

    async def first():
        with bind_customer_administration_policy(enabled), bind_customer_management_actor_role("admin"):
            ready.set()
            await released.wait()
            return current_customer_administration_policy().local_skill_management, customer_management_actor_is_admin()

    async def second():
        await ready.wait()
        with bind_customer_administration_policy(disabled), bind_customer_management_actor_role("user"):
            released.set()
            return current_customer_administration_policy().local_skill_management, customer_management_actor_is_admin()

    assert await asyncio.gather(first(), second()) == [(True, True), (False, False)]


def test_typed_host_actor_survives_missing_current_user_and_never_changes_owner():
    from deerflow.runtime.customer_administration import CustomerManagementActor, bind_customer_management_actor, current_customer_management_actor
    from deerflow.runtime.user_context import reset_current_user, set_current_user

    token = set_current_user(None)
    actor = CustomerManagementActor(owner_id="trusted-owner", administrator=True)
    try:
        with bind_customer_management_actor(actor):
            assert current_customer_management_actor() is actor
        assert current_customer_management_actor() == CustomerManagementActor()
    finally:
        reset_current_user(token)


@pytest.mark.asyncio
async def test_background_notification_does_not_restore_owner_admin_authority():
    from types import SimpleNamespace

    from app.gateway.auth_disabled import AUTH_SOURCE_INTERNAL
    from app.gateway.customer_administration import resolve_customer_management_actor

    owner = SimpleNamespace(id="owner", system_role="admin")
    request = SimpleNamespace(state=SimpleNamespace(auth_source=AUTH_SOURCE_INTERNAL, customer_management_notification=True))
    actor = await resolve_customer_management_actor(request, internal_owner_user=owner)
    assert actor.owner_id == "owner"
    assert actor.administrator is False


@pytest.mark.asyncio
@pytest.mark.parametrize("delegated", [False, True])
@pytest.mark.parametrize("owner_role", ["admin", "user"])
async def test_scheduled_launcher_retains_verified_owner_and_requires_provider_floor(delegated, owner_role, monkeypatch):
    from types import SimpleNamespace

    from app.gateway import services
    from app.gateway.customer_administration import request_customer_administration_policy, resolve_customer_management_actor
    from deerflow.runtime.customer_administration import CustomerAdministrationPolicy, CustomerManagementDenied, require_customer_management

    owner = SimpleNamespace(id="owner", system_role=owner_role)
    observed = []

    async def start(body, thread_id, request, **kwargs):
        actor = await resolve_customer_management_actor(request, internal_owner_user=owner)
        assert actor.owner_id == "owner"
        admitted = actor.administrator
        try:
            require_customer_management(request_customer_administration_policy(request), "local_skill_management")
        except CustomerManagementDenied:
            admitted = False
        observed.append(admitted)
        return SimpleNamespace(run_id="scheduled-run", thread_id=thread_id)

    monkeypatch.setattr(services, "start_run", start)
    monkeypatch.setattr(services, "_resolve_scheduler_recursion_limit", lambda: 100)
    app = SimpleNamespace(state=SimpleNamespace(customer_administration_policy=CustomerAdministrationPolicy(local_skill_management=delegated)))
    await services.launch_scheduled_thread_run(app=app, owner_user_id="owner", thread_id="thread", assistant_id=None, prompt="Scheduled work", metadata={"user_role": "admin"})
    assert observed == [delegated and owner_role == "admin"]
