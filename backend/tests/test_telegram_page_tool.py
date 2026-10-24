"""Web handovers use stored targets, exact routes and current reader permissions."""

from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from app.telegram_bots import pages
from deerflow.agent_instances.contract import AgentPermission
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow

BASE = "https://workspace.example.test"
ALL = AgentPermission.USE | AgentPermission.INSPECT | AgentPermission.MANAGE


def link(kind="lead_agent", ref="lead_agent"):
    return TelegramBotLinkRow(agent_kind=kind, agent_ref=ref)


@pytest.fixture(autouse=True)
def configured(monkeypatch):
    app_config = SimpleNamespace(model_extra={"telegram_bots": {"public_base_url": BASE}})
    monkeypatch.setattr(pages, "get_app_config", lambda: app_config)
    return app_config


@pytest.mark.parametrize(
    "key,kind,ref,kwargs,expected",
    [
        ("web.memory", "lead_agent", "lead_agent", {}, "/workspace?settings=memory"),
        ("web.conversations", "lead_agent", "lead_agent", {}, "/workspace/chats"),
        ("web.conversations", "custom_agent", "writer", {}, "/workspace/chats"),
        ("web.conversation", "lead_agent", "lead_agent", {"thread_id": "chat-1"}, "/workspace/chats/chat-1"),
        ("web.conversation", "custom_agent", "writer", {"thread_id": "chat-1"}, "/workspace/agents/writer/chats/chat-1"),
        ("web.conversation", "instance", "agent-1", {"thread_id": "chat-1"}, "/workspace/chats/chat-1"),
        ("web.telegram", "lead_agent", "lead_agent", {}, "/workspace?settings=channels&telegram=chat"),
        ("web.telegram", "custom_agent", "writer", {}, "/workspace?settings=channels&telegram=custom_agent.writer"),
        ("web.telegram", "instance", "agent-1", {}, "/workspace?settings=channels&telegram=agent.agent-1"),
        ("web.timezone", "lead_agent", "lead_agent", {}, "/workspace?settings=channels"),
        ("web.scheduled", "custom_agent", "writer", {}, "/workspace/scheduled-tasks"),
        ("web.account", "lead_agent", "lead_agent", {}, "/workspace?settings=account"),
        ("web.addAssistant", "lead_agent", "lead_agent", {}, "/workspace/assistants"),
        ("web.addEmployee", "lead_agent", "lead_agent", {"reader_is_administrator": True}, "/workspace/employees"),
        ("web.request", "instance", "agent-1", {"request_id": "request-1"}, "/workspace/attention?request=request-1"),
        ("policy.off", "instance", "agent-1", {}, "/workspace/instances?instance=agent-1"),
        ("web.fileTooLarge", "instance", "agent-1", {"permissions": ALL}, "/workspace/instances?instance=agent-1&tab=files"),
        ("web.fileTooLarge", "instance", "agent-1", {"permissions": AgentPermission.USE, "thread_id": "chat-1"}, "/workspace/chats/chat-1"),
        ("web.fileTooLarge", "lead_agent", "lead_agent", {"thread_id": "chat-1"}, "/workspace/chats/chat-1"),
        ("web.resultTooLarge", "custom_agent", "writer", {"thread_id": "chat-1"}, "/workspace/agents/writer/chats/chat-1"),
    ],
)
def test_page_button_built_from_public_base_url(key, kind, ref, kwargs, expected):
    assert pages.page_address(key, link(kind, ref), **kwargs) == BASE + expected


@pytest.mark.parametrize("key,tab", [("web.access", "access"), ("web.files", "files"), ("web.memory", "memory"), ("web.conversations", "conversations"), ("web.work", "work"), ("web.agentSettings", "settings")])
def test_instance_tabs_use_the_links_own_instance(key, tab):
    result = pages.page_address(key, link("instance", "agent-1"), permissions=ALL, ceilings=frozenset({"memory:read"}), target=("instance", "another-agent"))
    assert result == BASE + f"/workspace/instances?instance=agent-1&tab={tab}"


def test_one_assignment_has_no_tab_query():
    assert pages.page_address("web.work", link("instance", "agent-1"), work_id="work-1", permissions=ALL) == BASE + "/workspace/instances?instance=agent-1&work=work-1"


@pytest.mark.parametrize(
    "base",
    [None, "", "relative/path", "javascript:alert(1)", "https://user:password@workspace.example.test", "https://workspace.example.test?redirect=elsewhere", "https://workspace.example.test#fragment", "https://workspace.example.test\n"],
)
def test_no_button_without_public_base_url(configured, base):
    configured.model_extra["telegram_bots"]["public_base_url"] = base
    assert pages.page_address("web.account", link()) is None


def test_missing_section_has_no_button(configured):
    configured.model_extra.clear()
    assert pages.page_address("web.account", link()) is None


def test_public_base_is_read_again_after_runtime_change(configured):
    assert pages.page_address("web.account", link()).startswith(BASE)
    configured.model_extra["telegram_bots"]["public_base_url"] = "https://new.example.test/hartmesh/"
    assert pages.page_address("web.account", link()) == "https://new.example.test/hartmesh/workspace?settings=account"


@pytest.mark.parametrize("kind,ref", [("lead_agent", "lead_agent"), ("custom_agent", "writer")])
@pytest.mark.parametrize("key", ["web.access", "web.files", "web.work", "web.agentSettings", "policy.off"])
def test_key_not_for_this_bot_kind_shows_no_button(kind, ref, key):
    assert pages.page_address(key, link(kind, ref), permissions=ALL) is None


def test_custom_agent_has_no_memory_page():
    assert pages.page_address("web.memory", link("custom_agent", "writer"), permissions=ALL, ceilings=frozenset({"memory:read"})) is None


@pytest.mark.parametrize(
    "key,required", [("web.access", AgentPermission.MANAGE), ("web.agentSettings", AgentPermission.MANAGE), ("web.files", AgentPermission.INSPECT), ("web.work", AgentPermission.INSPECT), ("web.memory", AgentPermission.INSPECT)]
)
def test_instance_restricted_page_requires_current_grant(key, required):
    agent = link("instance", "agent-1")
    for permissions in (None, AgentPermission.USE, ALL & ~required):
        assert pages.page_address(key, agent, permissions=permissions, ceilings=frozenset({"memory:read"})) is None
    assert pages.page_address(key, agent, permissions=required, ceilings=frozenset({"memory:read"})) is not None


@pytest.mark.parametrize("ceilings", [None, frozenset(), frozenset({"agents:read"})])
def test_memory_requires_inspect_and_memory_ceiling(ceilings):
    assert pages.page_address("web.memory", link("instance", "agent-1"), permissions=ALL, ceilings=ceilings) is None


@pytest.mark.parametrize("kind,ref", [("lead_agent", "lead_agent"), ("custom_agent", "writer"), ("instance", "agent-1")])
def test_only_administrator_has_add_employee_handover(kind, ref):
    assert pages.page_address("web.addEmployee", link(kind, ref)) is None
    assert pages.page_address("web.addEmployee", link(kind, ref), reader_is_administrator=True) == BASE + "/workspace/employees"


def test_telegram_override_uses_the_named_bots_target():
    assert pages.page_address("web.telegram", link(), target=("instance", "other-agent")) == BASE + "/workspace?settings=channels&telegram=agent.other-agent"
    assert pages.page_address("web.telegram", link(), target=("custom_agent", "other-writer")) == BASE + "/workspace?settings=channels&telegram=custom_agent.other-writer"


def test_path_and_query_values_are_encoded_without_changing_route():
    hostile = "two/../names?new=1#part & 中文"
    result = pages.page_address("web.conversation", link("custom_agent", hostile), thread_id=hostile)
    assert result == BASE + "/workspace/agents/two%2F..%2Fnames%3Fnew%3D1%23part%20%26%20%E4%B8%AD%E6%96%87/chats/two%2F..%2Fnames%3Fnew%3D1%23part%20%26%20%E4%B8%AD%E6%96%87"
    query = urlsplit(pages.page_address("web.work", link("instance", hostile), work_id=hostile, permissions=ALL))
    assert query.path == "/workspace/instances"
    assert parse_qs(query.query) == {"instance": [hostile], "work": [hostile]}
    assert query.fragment == ""


@pytest.mark.parametrize("key,kwargs", [("web.conversation", {}), ("web.request", {}), ("web.fileTooLarge", {}), ("web.resultTooLarge", {}), ("web.list", {}), ("https://elsewhere.test", {}), ("unknown", {})])
def test_missing_required_identity_or_unknown_key_has_no_address(key, kwargs):
    assert pages.page_address(key, link(), **kwargs) is None


@pytest.mark.parametrize("target", [("unknown", "id"), ("instance", ""), ("custom_agent", ""), ("lead_agent", "unexpected"), ("lead_agent", "")])
def test_invalid_target_override_has_no_address(target):
    assert pages.page_address("web.telegram", link(), target=target) is None


def test_unknown_link_kind_has_no_address():
    assert pages.page_address("web.account", link("unsupported", "id")) is None


@pytest.mark.parametrize("key,kwargs", [("web.addAssistant", {}), ("web.addEmployee", {"reader_is_administrator": True})])
@pytest.mark.parametrize("kind,ref", [("lead_agent", "lead_agent"), ("custom_agent", "writer"), ("instance", "agent-1")])
def test_shared_creation_pages_exist_for_every_bot_kind(key, kwargs, kind, ref):
    assert pages.page_address(key, link(kind, ref), **kwargs) is not None


def test_file_handover_does_not_offer_uninspectable_files():
    assert pages.page_address("web.fileTooLarge", link("instance", "agent-1"), permissions=AgentPermission.MANAGE, thread_id="chat-1") == BASE + "/workspace/chats/chat-1"


@pytest.mark.parametrize("thread_id,agent_ref", [(".", "writer"), ("..", "writer"), ("chat-1", "."), ("chat-1", "..")])
def test_path_dot_segments_are_refused(thread_id, agent_ref):
    assert pages.page_address("web.conversation", link("custom_agent", agent_ref), thread_id=thread_id) is None


@pytest.mark.parametrize(
    "key,label_key,label,kwargs,path",
    [
        ("web.memory", "label.open_memory", "Open Memory", {}, "/workspace?settings=memory"),
        ("web.conversations", "label.open_conversations", "Open conversations", {}, "/workspace/chats"),
        ("web.conversation", "label.open_conversation", "Open conversation", {"thread_id": "chat-1"}, "/workspace/chats/chat-1"),
        ("web.addAssistant", "label.add_assistant", "Add AI assistant", {}, "/workspace/assistants"),
        ("web.addEmployee", "label.add_employee", "Add AI employee", {"reader_is_administrator": True}, "/workspace/employees"),
        ("web.telegram", "label.open_telegram", "Open Telegram settings", {}, "/workspace?settings=channels&telegram=chat"),
        ("web.timezone", "label.open_telegram", "Open Telegram settings", {}, "/workspace?settings=channels"),
        ("web.scheduled", "label.open_scheduled", "Open scheduled tasks", {}, "/workspace/scheduled-tasks"),
        ("web.account", "label.open_product", "Open HartMesh", {}, "/workspace?settings=account"),
        ("web.fileTooLarge", "label.open_conversation", "Open conversation", {"thread_id": "chat-1"}, "/workspace/chats/chat-1"),
        ("web.resultTooLarge", "label.open_conversation", "Open conversation", {"thread_id": "chat-1"}, "/workspace/chats/chat-1"),
    ],
)
def test_link_button_labels_and_addresses_match_the_hand_over_table(key, label_key, label, kwargs, path):
    from app.telegram_bots.render import render_plain

    assert render_plain(label_key, "en-US", product="HartMesh") == label
    assert pages.page_address(key, link(), **kwargs) == BASE + path
