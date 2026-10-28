"""Saved Telegram responses reach the existing policy only at the next turn.

Remote request proofs are explicit seeds. SQL/card/enqueue, middleware and the
disk policy database are real; the provider bridge performs no network I/O.
"""

import sqlite3
from pathlib import Path

import pytest
from _telegram_repository_test_support import db as db
from langchain_core.messages import HumanMessage
from langgraph.runtime import Runtime
from sqlalchemy import select
from test_telegram_human_input_plans import parts
from test_telegram_publication_authority import attempt_environment as attempt_environment
from test_telegram_publication_coordinator import coordinator_environment as coordinator_environment
from test_telegram_submission_publication import submission_environment as submission_environment

from deerflow.community.aio_sandbox import network_proxy
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow
from deerflow.sandbox.exceptions import SandboxRuntimeError
from deerflow.sandbox.middleware import SandboxMiddleware
from deerflow.sandbox.sandbox_provider import SandboxProvider, reset_sandbox_provider, set_sandbox_provider


class PolicyBridge(SandboxProvider):
    """Only the provider boundary is doubled; decisions use the real policy DB."""

    def __init__(self):
        self.calls = []

    def acquire(self, thread_id=None, *, user_id=None):
        raise AssertionError("This policy test must never acquire a sandbox")

    def get(self, sandbox_id):
        raise AssertionError("This policy test must never open a sandbox")

    def release(self, sandbox_id):
        raise AssertionError("This policy test must never release a sandbox")

    def sandbox_network_mode(self):
        return "allowlist"

    def decide_network_policy_request(self, sandbox_id, request_id, decision):
        self.calls.append((sandbox_id, request_id, decision))
        return sandbox_id == "request-sandbox" and network_proxy.decide(request_id, decision, ttl=300)


@pytest.mark.asyncio
@pytest.mark.parametrize("submission_environment", ["network"], indirect=True)
@pytest.mark.parametrize("decision", ["allow_temporary", "deny"])
async def test_saved_network_answer_is_applied_only_at_the_matching_next_turn(submission_environment, decision, tmp_path, monkeypatch):
    e = submission_environment
    response = {**e.response, "option_id": decision, "value": "Allow for five minutes" if decision == "allow_temporary" else "Keep blocked"}
    parent, callback_id, message_id = e.card, e.callback_id, 1901
    if decision == "deny":
        payload = {**e.network_body, "option_id": decision}
        keyboard = [[{"label": "label.keep_blocked", "kind": "network", "target_kind": "thread", "target_id": e.thread_id, "payload": payload}]]
        parent = await e.outbox.plan_human_input_step(e.parent, e.qualified, step_id="deny-next-turn", parts=parts(e.parent), keyboard=keyboard, callbacks=e.callbacks, guard=e.guard)
        async with e.db.sessions() as reader:
            row = await reader.scalar(select(TelegramCallbackRow).where(TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == parent))
            callback_id, group = row.id, row.group_id
        message_id = 1902
        assert await e.callbacks.adopt(e.link, group, message_id, chat_id=e.link.chat_id, message_thread_id=None, outbox_id=parent, presentation_version=0, check_current=lambda: True)

    policy = Path(tmp_path) / "network-policy.sqlite3"
    monkeypatch.setattr(network_proxy, "POLICY_DB", policy)
    monkeypatch.setenv("DEERFLOW_NETWORK_MODE", "allowlist")
    monkeypatch.setenv("DEERFLOW_ALLOW_DOMAINS_JSON", "[]")
    # Deterministic event ID matches the explicitly seeded current request.
    # The pending event and all subsequent decisions use production policy SQL.
    with monkeypatch.context() as identity:
        identity.setattr(network_proxy.uuid, "uuid4", lambda: response["request_id"])
        assert network_proxy.record_denial("example.test", 443, "CONNECT") == response["request_id"]
    other_request = network_proxy.record_denial("unrelated.test", 443, "CONNECT")
    provider = PolicyBridge()
    set_sandbox_provider(provider)
    try:
        stage = await e.inbox.stage_submission(
            e.link, source_outbox_id=parent, response=response, guard=e.guard, callback_id=callback_id, update_id=3902, chat_id=e.link.chat_id, message_id=message_id, message_thread_id=None, callbacks=e.callbacks
        )
        record = await e.inbox.reserve_callback_publication(e.link, stage.callback_id, thread_id=e.thread_id, endpoint_id=e.http.endpoint_id, guard=e.guard)
        await e.coordinator.dispatch_record(record, e.link)
        sent = e.bus.get_inbound_nowait()
        e.bus.inbound_task_done()
        assert sent.metadata["human_message_kwargs"]["human_input_response"] == response
        assert provider.calls == [] and not network_proxy.policy_allows("example.test", 443)
        actual_human = HumanMessage(content=sent.text, additional_kwargs=sent.metadata["human_message_kwargs"])
        middleware = SandboxMiddleware(owns_agent_skill_projection=False)

        def runtime():
            return Runtime(context={"thread_id": e.thread_id, "user_id": e.link.owner_user_id, "interaction_mode": "interactive"})

        # An older saved answer cannot be applied by a newer ordinary turn.
        await middleware.abefore_agent({"sandbox": {"sandbox_id": "request-sandbox"}, "messages": [actual_human, HumanMessage(content="Another question")]}, runtime())
        assert provider.calls == []
        with pytest.raises(SandboxRuntimeError, match="stale or does not belong"):
            await middleware.abefore_agent({"sandbox": {"sandbox_id": "another-sandbox"}, "messages": [actual_human]}, runtime())
        assert not network_proxy.policy_allows("example.test", 443)
        with sqlite3.connect(policy) as reader:
            assert reader.execute("SELECT COUNT(*) FROM events WHERE decision IS NOT NULL").fetchone() == (0,)

        current = runtime()
        state = {"sandbox": {"sandbox_id": "request-sandbox"}, "messages": [actual_human]}
        await middleware.abefore_agent(state, current)
        assert provider.calls == [("another-sandbox", response["request_id"], decision), ("request-sandbox", response["request_id"], decision)]
        assert network_proxy.policy_allows("example.test", 443) is (decision == "allow_temporary")
        assert not network_proxy.policy_allows("unrelated.test", 443)
        with sqlite3.connect(policy) as reader:
            assert dict(reader.execute("SELECT request_id, decision FROM events")) == {response["request_id"]: decision, other_request: None}
        # Re-entering the same run cannot apply the saved decision twice.
        await middleware.abefore_agent(state, current)
        assert len(provider.calls) == 2
    finally:
        reset_sandbox_provider()
