"""Artifact-only chat offers; clients own presentation and explicit consent."""

from datetime import datetime
from hashlib import sha256
from typing import Annotated, Literal

from langchain.tools import ToolRuntime, tool
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.graph import END
from langgraph.types import Command
from pydantic import StringConstraints, field_validator

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY, AgentExecution
from deerflow.agent_instances.work_contract import Statement, StrictModel
from deerflow.utils.messages import is_real_user_message

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


def chat_tools(execution: AgentExecution) -> list:
    """Tools for a normal conversation with an AI assistant or AI employee."""
    return [show_web_page]


class ProposeAssignment(StrictModel):
    objective: Statement
    success_criteria: Statement
    due_text: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=120)] | None = None
    due_at: datetime | None = None

    @field_validator("due_at")
    @classmethod
    def aware_due_date(cls, value):
        if value is not None and value.utcoffset() is None:
            raise ValueError("An explicit time zone is required")
        return value


@tool
def propose_assignment(proposal: ProposeAssignment, runtime: ToolRuntime[dict | None]) -> Command | ToolMessage:
    """Show the person a card that proposes a tracked assignment.

    Call it when the person hands over a piece of work with a result to review
    or a deadline. Do not call it for a question, a quick look-up, or when the
    person says to do it now in this conversation. The card is only a proposal:
    nothing is saved or started until the person confirms it. After calling it,
    write nothing more. If the person then says to do it here, do it here and
    do not propose again. due_at: only when the person's words give a day. Work
    it out from the person's local date given in this conversation, with that
    zone's offset; a day without a time is 18:00. Otherwise leave it empty and
    put the person's words in due_text.
    """
    marks = proposal_marks(runtime.state.get("messages", []))
    response = marks.get("assignment_card_response")
    identity = runtime.tool_call_id
    if isinstance(response, dict) and response.get("choice") == "answer_here":
        return ToolMessage(name="propose_assignment", tool_call_id=identity, content="The person chose to have this done here. Do it now. Do not propose an assignment for it.")
    cards = marks.get("shows_cards")
    if not isinstance(cards, list) or "assignment_card" not in cards:
        return ToolMessage(name="propose_assignment", tool_call_id=identity, content="This conversation cannot show an assignment card. Do the request here.")
    draft = proposal.model_dump(mode="json")
    lines = [proposal.objective, proposal.success_criteria]
    if proposal.due_text or proposal.due_at:
        lines.append(proposal.due_text or proposal.due_at.isoformat())
    lines.append("Nothing is saved or started until the person confirms it.")
    return Command(
        update={
            "messages": [
                ToolMessage(
                    id=proposal_message_id(identity, proposal_issuer(runtime.state.get("messages", []), identity)),
                    name="propose_assignment",
                    tool_call_id=identity,
                    content="\n\n".join(lines),
                    artifact={"assignment_card": {"version": 1, "kind": "assignment_card", "tool_call_id": identity, "draft": draft}},
                )
            ]
        },
        goto=END,
    )


def proposal_marks(messages):
    latest = next((message for message in reversed(messages) if is_real_user_message(message)), None)
    return latest.additional_kwargs if latest is not None else {}


def supports_assignment_card(messages):
    marks = proposal_marks(messages)
    response, cards = marks.get("assignment_card_response"), marks.get("shows_cards")
    return isinstance(cards, list) and "assignment_card" in cards and not (isinstance(response, dict) and response.get("choice") == "answer_here")


def proposal_issuer(messages, identity):
    for message in reversed(messages):
        if is_real_user_message(message):
            break
        if isinstance(message, AIMessage) and any(call.get("name") == "propose_assignment" and call.get("id") == identity for call in message.tool_calls):
            return message.id
    return None


def proposal_message_id(identity, issuer):
    scope = ":" + sha256(issuer.encode("utf-8")).hexdigest()[:12] if issuer else ""
    return f"assignment-card:{identity}{scope}"
