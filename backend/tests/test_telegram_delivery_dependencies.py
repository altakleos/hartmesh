"""Declared delivery dependencies cannot cross actual bound-chat ownership."""

import asyncio
from uuid import uuid4

import pytest
from _config_singleton_guard import restore_config_singletons  # noqa: F401 -- autouse fixture
from _run_output_capture_test_support import capture_environment as capture_environment
from _run_output_capture_test_support import output_store as output_store
from _telegram_compose_turn_support import render_compose_config
from _telegram_work_worker_support import installed_worker
from sqlalchemy import select
from test_telegram_supervisor import _supervisor_logging as _supervisor_logging

from app.telegram_bots.producer import _item
from deerflow.persistence.telegram_bots.model import TelegramOutboxRow


@pytest.mark.asyncio
async def test_dependency_scope_before_claim(capture_environment, monkeypatch, tmp_path):
    await asyncio.to_thread(render_compose_config, tmp_path, monkeypatch, keys={"OPENAI_API_KEY": "sk-synthetic"})
    async with installed_worker(capture_environment, monkeypatch, tmp_path) as e:
        parent_id = await e.host.outbox.plan_product(e.link, source_id="dependency-parent", parts=[{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}}])
        key = "notice:dependency-child"
        assert await e.host.outbox.add(e.link, dedupe_key=key, class_="reply", payload={"after_delivery_id": parent_id, "parts": [{"kind": "text", "source_ref": {"key": "chat.restarted", "values": {}}}]})
        async with e.e.sf() as reader:
            child = await reader.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.dedupe_key == key))
            child_id, original = child.id, _item(child)
            parent = await reader.get(TelegramOutboxRow, parent_id)
            scope = {name: getattr(parent, name) for name in ("binding_generation", "message_thread_id")}
        assert (await e.host.sender._eligibility(e.link, original, None)).decision == "wait"
        for case in ("malformed", "self", "missing", "binding_generation", "message_thread_id"):
            async with e.e.sf.begin() as writer:
                child = await writer.get(TelegramOutboxRow, child_id)
                parent = await writer.get(TelegramOutboxRow, parent_id)
                for name, value in scope.items():
                    setattr(parent, name, value)
                if case in {"malformed", "self", "missing"}:
                    dependency = "invalid" if case == "malformed" else child_id if case == "self" else uuid4().hex
                    child.payload = {**child.payload, "after_delivery_id": dependency}
                else:
                    child.payload = {**child.payload, "after_delivery_id": parent_id}
                    value = getattr(parent, case)
                    setattr(parent, case, (value or 0) + 1)
                snapshot = _item(child)
            decision = await e.host.sender._eligibility(e.link, snapshot, None)
            assert (decision.decision, decision.reason) == ("skip", "source-gone"), case
            async with e.e.sf() as reader:
                actual = await reader.get(TelegramOutboxRow, child_id)
                assert actual.state == "pending" and actual.claim_owner is None and actual.attempts == 0
