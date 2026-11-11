"""Bounded discovery and presentation; command/delivery activation is separate."""

from types import SimpleNamespace

import httpx
import pytest


def entry(number, *, custody="personal", owner="person", permissions=3, **changes):
    return {"id": str(number), "name": f"Agent {number}", "custody": custody, "owner_id": owner, "permissions": permissions, "status": "active", **changes}


def body(entries, *, per_user=100, mine=0):
    return {"instances": entries, "ai_assistants": {"per_user": per_user, "mine": mine}}


def presentation(monkeypatch, payload, *, custody="personal", administrator=False, details=None, bots=()):
    from app.telegram_bots import agent_lists

    monkeypatch.setattr(agent_lists, "page_address", lambda key, link, **kw: "https://product.example/" + (kw["target"][1] if "target" in kw else key))
    link = SimpleNamespace(agent_kind="lead_agent", agent_ref="lead_agent", owner_user_id="person")
    snapshot = agent_lists.select_agents(payload, owner_user_id="person", custody=custody)
    return agent_lists.present_list(snapshot, link=link, bots=bots, details=details or {}, locale="en", product="Product", administrator=administrator)


def test_own_assistants_precede_colleagues_and_filter_unusable_entries(monkeypatch):
    result = presentation(
        monkeypatch, body([entry("shared", owner="colleague"), entry("hidden", permissions=2), entry("pending", status="provisioning"), entry("deleted", status="deleted"), entry("employee", custody="company"), entry("mine")])
    )
    assert result.text == "Your AI assistants:\nAgent mine\nAI assistants colleagues gave you access to:\nAgent shared"
    assert [row[0]["label_values"] for row in result.keyboard[:-1]] == [{"name": "Agent mine"}, {"name": "Agent shared"}]
    assert result.keyboard[-1][0]["label"] == "label.add_assistant"


@pytest.mark.parametrize("count", [21, 30, 500])
def test_twenty_lines_and_more_marker_have_only_twenty_agent_buttons(monkeypatch, count):
    result = presentation(monkeypatch, body([entry(i, custody="company") for i in range(count)]), custody="company")
    assert len(result.keyboard) == 20
    assert result.text.count("\nAgent ") == 20
    assert result.text.endswith("More are in Product on the web.")


def test_full_raw_page_warns_even_when_only_one_entry_qualifies(monkeypatch):
    result = presentation(monkeypatch, body([entry(0)] + [entry(i, permissions=2) for i in range(1, 500)]))
    assert "More are in Product" in result.text
    assert len(result.keyboard) == 2


@pytest.mark.parametrize("per_user,mine,offered", [(0, 0, False), (1, 1, False), (2, 3, False), (2, 1, True)])
def test_add_assistant_respects_quota_without_hiding_existing_agents(monkeypatch, per_user, mine, offered):
    result = presentation(monkeypatch, body([entry(1)], per_user=per_user, mine=mine))
    assert "Agent 1" in result.text
    assert (result.keyboard[-1][0]["label"] == "label.add_assistant") is offered
    empty = presentation(monkeypatch, body([], per_user=per_user, mine=mine))
    assert empty.text == "You have no AI assistants yet."
    assert bool(empty.keyboard) is offered


@pytest.mark.parametrize("administrator", [False, True])
def test_employee_inventory_does_not_grant_chat_and_only_admin_can_add(monkeypatch, administrator):
    result = presentation(monkeypatch, body([{"id": "hidden", "name": "Hidden", "supervisor_name": "Boss", "status": "active"}]), custody="company", administrator=administrator)
    assert result.text == "No AI employees you can chat with yet." + ("" if administrator else " Administrators add AI employees.")
    assert bool(result.keyboard) is administrator
    if administrator:
        assert result.keyboard[-1][0]["label"] == "label.add_employee"


def test_shared_only_and_own_only_groups(monkeypatch):
    shared = presentation(monkeypatch, body([entry(1, owner="colleague")]))
    assert shared.text.startswith("You have no AI assistants yet.\nAI assistants colleagues gave you access to:")
    own = presentation(monkeypatch, body([entry(1)]))
    assert "colleagues" not in own.text


def test_inspect_controls_role_and_work_and_only_owners_bot_is_opened(monkeypatch):
    from app.telegram_bots.agent_lists import AgentListDetails

    bots = [SimpleNamespace(owner_user_id="person", agent_kind="instance", agent_ref="1", bot_username="AgentOneBot"), SimpleNamespace(owner_user_id="other", agent_kind="instance", agent_ref="2", bot_username="NotYourBot")]
    result = presentation(
        monkeypatch, body([entry(1, name="<Maya>"), entry(2, permissions=1), entry(3)]), details={"1": AgentListDetails("Reporting analyst", 2, 3), "2": AgentListDetails("Private role", 7, 9), "3": AgentListDetails(None, 0, 0)}, bots=bots
    )
    assert "&lt;Maya&gt;, Reporting analyst: working on 2, 3 waiting for you" in result.text
    assert "Agent 2\n" in result.text and "Private role" not in result.text
    assert "Agent 3: nothing in progress" in result.text
    assert result.keyboard[0][0]["url"] == "https://t.me/AgentOneBot"
    assert result.keyboard[1][0]["url"] == "https://product.example/2"
    assert result.keyboard[0][0]["label_values"] == {"name": "<Maya>"}


@pytest.mark.parametrize("payload", [{}, {"instances": "bad"}, body([entry(1), entry(1)]), body([entry(1, permissions=True)]), body([entry(i) for i in range(501)]), body([], mine=-1)])
def test_malformed_gateway_discovery_is_refused(payload):
    from app.channels.execution import ChannelExecutionProtocolError
    from app.telegram_bots.agent_lists import select_agents

    with pytest.raises(ChannelExecutionProtocolError):
        select_agents(payload, owner_user_id="person", custody="personal")


@pytest.mark.asyncio
@pytest.mark.parametrize("custody", ["personal", "company"])
async def test_list_read_uses_gateway_owner_maximum_page_and_retains_response(custody):
    from app.telegram_bots.gateway_client import GatewayClientOwner

    calls, checks = [], []

    async def guard():
        checks.append(True)

    async def respond(request):
        calls.append(request)
        return httpx.Response(200, json=body([]))

    client = GatewayClientOwner(gateway_url="https://gateway.example/root", langgraph_url="https://runtime.example/other/api", transport=httpx.MockTransport(respond))
    try:
        assert await client.get_agent_instances("person", custody=custody, before_dispatch=guard) == body([])
        request = calls[0]
        assert request.url.host == "gateway.example"
        assert request.url.path == "/root/api/agent-instances"
        assert dict(request.url.params) == {"custody": custody, "limit": "500"}
        assert request.headers["X-DeerFlow-Owner-User-Id"] == "person"
        assert len(checks) >= 3
        assert not client._responses
    finally:
        await client.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("when", ["leaf", "response"])
async def test_lost_read_guard_refuses_and_closes_without_retaining_private_result(when):
    from app.channels.execution import ChannelExecutionProtocolError
    from app.telegram_bots.gateway_client import GatewayClientOwner

    calls, responses, checks = [], [], []

    async def guard():
        checks.append(True)
        if when == "leaf" and len(checks) == 3 or when == "response" and responses:
            raise RuntimeError("lost-owner")

    async def respond(request):
        calls.append(request)
        response = httpx.Response(200, json=body([entry(1)]))
        responses.append(response)
        return response

    client = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api", transport=httpx.MockTransport(respond))
    try:
        with pytest.raises(ChannelExecutionProtocolError):
            await client.get_agent_instances("person", custody="personal", before_dispatch=guard)
        assert len(calls) == (0 if when == "leaf" else 1)
        assert not client._responses
        assert all(response.is_closed for response in responses)
    finally:
        await client.stop()


@pytest.mark.asyncio
async def test_cancellation_during_list_decode_drains_before_client_close(monkeypatch):
    import asyncio
    import threading

    from app.telegram_bots import gateway_client

    entered, release = threading.Event(), threading.Event()
    original = gateway_client._decode

    def held_decode(value):
        entered.set()
        assert release.wait(10)
        return original(value)

    async def guard():
        pass

    monkeypatch.setattr(gateway_client, "_decode", held_decode)
    client = gateway_client.GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api", transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body([]))))
    request = asyncio.create_task(client.get_agent_instances("person", custody="personal", before_dispatch=guard))
    stop = None
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        request.cancel()
        request.cancel()
        stop = asyncio.create_task(client.stop())
        await asyncio.sleep(0)
        assert not request.done() and not client.closed
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await request
        await stop
        assert client.closed and not client._responses
    finally:
        release.set()
        await asyncio.gather(request, *([stop] if stop else []), return_exceptions=True)
        await client.stop()


def test_own_agents_take_precedence_within_twenty_line_cap(monkeypatch):
    result = presentation(monkeypatch, body([entry(i, owner="colleague") for i in range(25)] + [entry("mine")]))
    assert result.text.startswith("Your AI assistants:\nAgent mine\nAI assistants colleagues gave you access to:")
    assert result.keyboard[0][0]["label_values"] == {"name": "Agent mine"}
    assert len(result.keyboard) == 21  # Twenty agents and Add.


def test_chinese_omits_punctuation_for_unreadable_role():
    from app.telegram_bots.render import render_plain

    assert render_plain("agents.line_plain", "zh-CN", name="小林", role=None) == "小林"
    assert render_plain("agents.line_idle", "zh-CN", name="小林", role=None) == "小林：没有正在处理的事"


@pytest.mark.asyncio
@pytest.mark.parametrize("route", ["instances", "features"])
async def test_final_cleanup_cannot_return_a_read_after_its_guard_retires(monkeypatch, route):
    import asyncio

    from app.channels.execution import ChannelExecutionProtocolError
    from app.telegram_bots.gateway_client import GatewayClientOwner

    entered, release = asyncio.Event(), asyncio.Event()
    live = True

    async def guard():
        if not live:
            raise RuntimeError("retired")

    client = GatewayClientOwner(gateway_url="https://gateway.example", langgraph_url="https://runtime.example/api", transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body([]))))
    original_close = client._close_response

    async def held_close(response):
        entered.set()
        await release.wait()
        await original_close(response)

    monkeypatch.setattr(client, "_close_response", held_close)
    read = client.get_agent_instances("person", custody="personal", before_dispatch=guard) if route == "instances" else client.get_features("person", before_dispatch=guard)
    task = asyncio.create_task(read)
    try:
        await asyncio.wait_for(entered.wait(), 3)
        live = False
        release.set()
        with pytest.raises(ChannelExecutionProtocolError):
            await task
        assert not client._responses
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await client.stop()


def test_existing_instance_name_contract_allows_128_characters(monkeypatch):
    from deerflow.spaces.contract import validate_name

    name = validate_name("M" * 128)
    result = presentation(monkeypatch, body([entry(1, name=name)]))
    assert name in result.text
    assert result.keyboard[0][0]["label_values"] == {"name": name}


def test_list_parts_preserve_both_languages_and_bound_long_names(monkeypatch):
    from app.telegram_bots.agent_lists import AgentListDetails, list_parts
    from app.telegram_bots.render import render
    from app.telegram_bots.split import utf16_units, visible_text

    details = {str(i): AgentListDetails("角色" * 100, 3, 4) for i in range(20)}
    result = presentation(monkeypatch, body([entry(i, name="名" * 128) for i in range(20)]), details=details)
    parts = list_parts(result)
    assert len(parts) > 1
    for locale in ("en", "zh-CN"):
        texts = [render(part["source_ref"]["key"], locale, **part["source_ref"]["values"]) for part in parts]
        assert all(utf16_units(visible_text(text)) <= 4096 for text in texts)
        assert "\n".join(texts) == render("agents.presentation", locale, lines=result.lines)
    assert "\n".join(render(part["source_ref"]["key"], "en", **part["source_ref"]["values"]) for part in parts) == result.text
