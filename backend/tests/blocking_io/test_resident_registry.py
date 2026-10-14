"""Shared capacity eviction never performs physical lifecycle work on-loop."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from _sandbox_provider_fakes import _make_provider

from deerflow.agent_instances.residents import ResidentRegistry


@pytest.mark.asyncio
async def test_chat_capacity_evicts_resident_with_offloop_containment(tmp_path, monkeypatch):
    provider, _backend = await asyncio.to_thread(_make_provider, tmp_path, monkeypatch, replicas=1)
    attachments = SimpleNamespace(contain_host=AsyncMock())
    residents = ResidentRegistry(attachments=attachments, provider=provider, run_manager=SimpleNamespace(task_runs=lambda _key: {}), settle=AsyncMock(), owner_of=AsyncMock(return_value=None), replicas=1, idle_timeout=60)
    provider.external_slots, provider.evict_external = residents.external_slots, residents.evict_external
    agent = SimpleNamespace(id="agent", home_id="home", name="AI employee")
    residents.claimed(agent, "attachment")

    async def contain(attachment_id, reason):
        await asyncio.to_thread(time.sleep, 0.02)
        await residents.contained(attachment_id, [agent.home_id], reason)
        return True

    attachments.contain_host.side_effect = contain
    try:
        await provider._admit_create_async("chat")
        assert provider._starting == {"chat"} and not residents.slots
        attachments.contain_host.assert_awaited_once_with("attachment", "idle")
    finally:
        await residents.close()
        provider.reset()
