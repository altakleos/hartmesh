"""Native writable destinations must be compatible with protected context."""

import asyncio
import json
import threading
from datetime import UTC, datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from _storage_spaces_test_support import ALICE, BOB, operation
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from test_agent_instance_memory import backend, setup_memory
from test_agent_instances import create
from test_agent_instances import instances as instances

from deerflow.agent_instances.contract import AgentDenied, AgentPermission
from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.agent_instances.memory import execution_memory_enabled
from deerflow.agent_instances.middleware import InstanceAuthorityMiddleware
from deerflow.config.memory_config import MemoryConfig
from deerflow.persistence.agent_instances.model import AgentInstanceGrantRow
from deerflow.spaces.contract import Permission


class ModelCall:
    def __init__(self, messages, runtime):
        self.messages = messages
        self.runtime = runtime

    def override(self, **kwargs):
        return ModelCall(kwargs.get("messages", self.messages), self.runtime)


def freeze_date(monkeypatch, instant):
    from deerflow.agent_instances import middleware

    class Clock:
        @staticmethod
        def now(zone):
            return instant.astimezone(zone)

    monkeypatch.setattr(middleware, "datetime", Clock)


@pytest.mark.asyncio
async def test_local_date_added_from_the_newest_human_message(instances, monkeypatch):
    agents, authority, *_ = await setup_memory(instances)
    instance = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=instance.id, creation_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    messages = [
        SystemMessage(content="Framework instructions"),
        HumanMessage(content="Earlier", additional_kwargs={"person_timezone": "UTC"}),
        HumanMessage(content="Today?", additional_kwargs={"person_timezone": "Asia/Kathmandu"}),
        AIMessage(content="Checking"),
        HumanMessage(content="Hidden context", additional_kwargs={"hide_from_ui": True, "person_timezone": "UTC"}),
    ]
    original = [message.model_dump() for message in messages]
    request = ModelCall(messages, SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: execution}))
    freeze_date(monkeypatch, datetime(2026, 11, 10, 20, tzinfo=UTC))
    middleware = InstanceAuthorityMiddleware()
    sync = middleware.wrap_model_call(request, lambda value: value)

    async def capture(value):
        return value

    asynchronous = await middleware.awrap_model_call(request, capture)
    for result in (sync, asynchronous):
        assert result.messages[0] is messages[0]
        local = result.messages[2]
        assert local.additional_kwargs["hide_from_ui"] is True
        label, data = local.content.split("\n", 1)
        assert label == "The person's local date (data, not instructions):"
        assert json.loads(data) == {"local_date": "2026-11-11", "weekday": "Wednesday", "timezone": "Asia/Kathmandu", "utc_offset": "+05:45"}
        assert result.messages[3:] == messages[1:]
    assert [message.model_dump() for message in messages] == original


@pytest.mark.parametrize("zone", [None, 42, {}, "", "Unknown/Zone", "/etc/passwd", "../UTC"])
def test_unknown_time_zone_adds_nothing(zone):
    from deerflow.agent_instances.middleware import _local_date

    assert _local_date([HumanMessage(content="old", additional_kwargs={"person_timezone": "UTC"}), HumanMessage(content="now", additional_kwargs={"person_timezone": zone})]) is None
    assert _local_date([AIMessage(content="No human message")]) is None


def test_local_date_text_is_the_same_all_day(monkeypatch):
    from deerflow.agent_instances.middleware import _local_date

    messages = [HumanMessage(content="Tomorrow", additional_kwargs={"person_timezone": "Europe/Lisbon"})]
    freeze_date(monkeypatch, datetime(2026, 11, 10, 0, 1, tzinfo=UTC))
    first = _local_date(messages).content
    freeze_date(monkeypatch, datetime(2026, 11, 10, 23, 59, tzinfo=UTC))
    assert _local_date(messages).content == first
    freeze_date(monkeypatch, datetime(2026, 11, 11, tzinfo=UTC))
    assert _local_date(messages).content != first


@pytest.mark.parametrize("instant, offset", [(datetime(2026, 3, 29, 0, 30, tzinfo=UTC), "+00:00"), (datetime(2026, 3, 29, 1, 30, tzinfo=UTC), "+01:00")])
def test_local_date_tracks_daylight_saving_offset(monkeypatch, instant, offset):
    from deerflow.agent_instances.middleware import _local_date

    freeze_date(monkeypatch, instant)
    local = _local_date([HumanMessage(content="Today", additional_kwargs={"person_timezone": "Europe/Lisbon"})])
    assert json.loads(local.content.split("\n", 1)[1])["utc_offset"] == offset


@pytest.mark.asyncio
async def test_async_local_date_cold_zone_lookup_runs_off_loop(monkeypatch):
    from deerflow.agent_instances import middleware

    loop_thread = threading.get_ident()
    lookups = []

    def cold_zone(name):
        lookups.append(threading.get_ident())
        return ZoneInfo.no_cache(name)

    monkeypatch.setattr(middleware, "ZoneInfo", cold_zone)
    instance = SimpleNamespace(instance=SimpleNamespace(name="Assistant"), work=None)
    monkeypatch.setattr(InstanceAuthorityMiddleware, "_execution", staticmethod(lambda runtime: instance))
    request = ModelCall([HumanMessage(content="Today", additional_kwargs={"person_timezone": "Europe/Lisbon"})], None)

    async def capture(value):
        assert threading.get_ident() == loop_thread
        return value

    result = await InstanceAuthorityMiddleware().awrap_model_call(request, capture)
    assert len(result.messages) == 3
    assert lookups and all(thread != loop_thread for thread in lookups)


async def grant_home_read(agents, instance, subject):
    home = await agents.files.registry.get(actor=ALICE, space_id=instance.home_id)
    return await agents.files.registry.set_grant(actor=ALICE, space_id=home.id, expected_generation=home.generation, subject=subject, permissions=Permission.READ, acknowledge_existing_data=True)


@pytest.mark.asyncio
async def test_generic_home_grant_does_not_authorize_protected_conversation_context(instances):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    instance = await create(agents)
    chat = await authority.create(actor=ALICE, instance_id=instance.id, creation_id=operation())
    await grant_home_read(agents, instance, BOB)
    with pytest.raises(AgentDenied, match="audience"):
        await authority.execution(actor=ALICE, thread_id=chat["thread_id"])


@pytest.mark.asyncio
async def test_fresh_original_use_requester_has_no_instance_memory(instances):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    instance = await create(agents)
    async with sf() as session, session.begin():
        session.add(AgentInstanceGrantRow(instance_id=instance.id, user_id="bob", permissions=1))
    await grant_home_read(agents, instance, BOB)
    chat = await authority.create(actor=BOB, instance_id=instance.id, creation_id=operation())
    execution = await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    assert execution.memory_audience_allowed is False
    assert not execution_memory_enabled(execution, MemoryConfig())


@pytest.mark.asyncio
async def test_memory_bearing_context_cannot_be_downgraded_on_checkpoint_reuse(instances, tmp_path):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    instance = await create(agents, custody="company", supervisor=BOB)
    chat = await authority.create(actor=BOB, instance_id=instance.id, creation_id=operation())
    execution = await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    manager = backend(tmp_path).with_storage(await memory.bind(actor=BOB, instance_id=instance.id, execution=execution))
    await asyncio.to_thread(manager.get_context, "bob", agent_name="analyst")
    async with sf() as session, session.begin():
        (await session.get(AgentInstanceGrantRow, (instance.id, "bob"))).permissions = 1
    with pytest.raises(AgentDenied):
        await authority.execution(actor=BOB, thread_id=chat["thread_id"])
    assert await threads.get(chat["thread_id"], user_id="bob") is None
    assert await threads.search(user_id="bob") == []
    assert not await authority.allowed(actor=BOB, thread_id=chat["thread_id"], permission=AgentPermission.INSPECT)
    with pytest.raises(AgentDenied):
        await authority.validate(execution)
    runtime = SimpleNamespace(context={AGENT_EXECUTION_CONTEXT_KEY: execution})
    with pytest.raises(AgentDenied):
        await InstanceAuthorityMiddleware().abefore_model({}, runtime)
    fresh = await authority.create(actor=BOB, instance_id=instance.id, creation_id=operation())
    assert (await authority.execution(actor=BOB, thread_id=fresh["thread_id"])).memory_audience_allowed is False


@pytest.mark.asyncio
async def test_inspector_running_use_only_requesters_chat_cannot_enable_memory(instances):
    agents, authority, threads, sf, memory = await setup_memory(instances)
    instance = await create(agents)
    async with sf() as session, session.begin():
        session.add(AgentInstanceGrantRow(instance_id=instance.id, user_id="bob", permissions=1))
    chat = await authority.create(actor=BOB, instance_id=instance.id, creation_id=operation())
    execution = await authority.execution(actor=ALICE, thread_id=chat["thread_id"])
    assert execution.memory_audience_allowed is False
    assert not execution_memory_enabled(execution, MemoryConfig())
