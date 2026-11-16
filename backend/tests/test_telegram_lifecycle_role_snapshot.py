"""The actual global lifecycle notice repeats its role facts at final SQL."""

import asyncio
from contextvars import ContextVar
from datetime import UTC, datetime

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import render_compose_config
from _telegram_work_worker_support import installed_worker
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from deerflow.config.app_config import peek_loaded_app_config
from deerflow.config.authorization_config import AuthorizationConfig
from deerflow.persistence.agent_instances.model import AgentInstanceRow


@pytest.mark.asyncio
async def test_actual_notice_rechecks_role_limit_after_last_require(capture_environment, monkeypatch, tmp_path):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        users = SQLiteUserRepository(e.e.sf)
        owner = await users.get_user_by_id(e.link.owner_user_id)
        issuer, subject = "https://identity.example", "notice-owner"
        owner.oauth_provider, owner.oauth_id, owner.oauth_issuer = "fixture", subject, issuer
        await users.update_user(owner)
        await users.record_sign_in(str(owner.id), system_role="admin", oauth_issuer=issuer, last_sign_in_at=datetime.now(UTC))
        assert (await users.get_user_by_id(str(owner.id))).system_role == "admin"
        policy = {"admin": {"routes": {"allow": "*"}}, "user": {"routes": {"allow": "*", "deny": ["agents:read"]}}}
        monkeypatch.setattr(peek_loaded_app_config(), "authorization", AuthorizationConfig.model_validate({"enabled": True, "provider": {"use": "deerflow.authz.rbac:RbacAuthorizationProvider", "config": {"roles": policy}}}))
        async with e.e.sf.begin() as writer:
            (await writer.get(AgentInstanceRow, e.instance.id)).status = "suspended"
        protecting = ContextVar("actual-notice-protection", default=False)
        actual_protect, actual_policy = e.host.authority.protect_notice, e.host.authority.instance_send_policy
        changed, returned = [], []

        async def protect(context):
            token = protecting.set(True)
            try:
                result = await actual_protect(context)
                returned.append(result)
                return result
            finally:
                protecting.reset(token)

        async def policy_check(link, **kwargs):
            if protecting.get() and kwargs.get("expected_lifecycle") == "agent.paused" and not changed:

                async def downgrade():
                    if not changed:
                        changed.append(True)
                        await users.limit_role(issuer, subject, "user")

                kwargs["before_snapshot"] = downgrade
            return await actual_policy(link, **kwargs)

        monkeypatch.setattr(e.host.authority, "protect_notice", protect)
        monkeypatch.setattr(e.host.authority, "instance_send_policy", policy_check)
        before = len(e.normal.calls)
        await e.host.notice_instance_unavailable(e.link)
        assert changed == [True]
        assert not returned, "final lifecycle protection accepted the earlier role after its actual limit committed"
        assert not [call for call in e.normal.calls[before:] if call.endpoint == "sendMessage"]
