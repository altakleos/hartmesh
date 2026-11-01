"""Bounded page offers; the receiving client resolves addresses and access."""

from typing import Literal

from langchain.tools import ToolRuntime, tool
from langchain_core.messages import ToolMessage

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY, AgentExecution

WebPageKey = Literal["access", "files", "memory", "conversations", "work", "agentSettings", "addAssistant", "addEmployee", "telegram", "scheduled", "account", "timezone"]


@tool
def show_web_page(page: WebPageKey, runtime: ToolRuntime[dict | None]) -> ToolMessage:
    """Offer the person a link that opens one page of the web app.

    Use it when the person asks for something that is done on a page: who has
    access, the files, what is remembered, earlier conversations, assignment
    history, settings, adding an AI assistant or an AI employee, Telegram bots,
    scheduled tasks, the account. Answer what you can answer yourself. Name the
    page; never write an address.
    """
    context = runtime.context or {}
    is_instance = isinstance(context.get(AGENT_EXECUTION_CONTEXT_KEY), AgentExecution)
    if not is_instance and (page in {"access", "files", "work", "agentSettings"} or (page == "memory" and context.get("agent_name"))):
        return ToolMessage(name="show_web_page", tool_call_id=runtime.tool_call_id, content="That page does not exist in this chat.")
    return ToolMessage(
        name="show_web_page",
        tool_call_id=runtime.tool_call_id,
        content="A link to this page is shown with your reply where this chat can show one. Say in one sentence what the page is for. Do not write an address.",
        artifact={"web_page": {"version": 1, "kind": "web_page", "key": page}},
    )
