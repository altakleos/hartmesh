"""Bounded ordinary-chat discovery and presentation; snapshots confer no authority."""

import re
from dataclasses import dataclass

from app.channels.execution import ChannelExecutionProtocolError
from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.pages import page_address
from app.telegram_bots.render import render, render_plain
from deerflow.spaces.contract import validate_name

AGENT_LIST_LINES = 20
AGENT_LIST_FETCH_LIMIT = 500


@dataclass(frozen=True)
class AgentListEntry:
    id: str
    name: str
    own: bool
    permissions: int


@dataclass(frozen=True)
class AgentList:
    owner_user_id: str
    custody: str
    entries: tuple[AgentListEntry, ...]
    more: bool
    can_add: bool


@dataclass(frozen=True)
class AgentListDetails:
    role: str | None
    working: int
    waiting: int

    def __post_init__(self):
        if self.role is not None and (type(self.role) is not str or len(self.role) > 256) or any(type(n) is not int or n < 0 for n in (self.working, self.waiting)):
            raise ChannelExecutionProtocolError()


@dataclass(frozen=True)
class AgentListPresentation:
    text: str
    keyboard: list[list[dict]]


def select_agents(body, *, owner_user_id: str, custody: str) -> AgentList:
    """Filter Use before the display cap; retain no unneeded discovery fields."""
    if custody not in {"personal", "company"} or type(owner_user_id) is not str or not owner_user_id or type(body) is not dict:
        raise ChannelExecutionProtocolError()
    rows = body.get("instances")
    counts = body.get("ai_assistants")
    if type(rows) is not list or len(rows) > AGENT_LIST_FETCH_LIMIT or type(counts) is not dict or any(type(counts.get(k)) is not int or counts[k] < 0 for k in ("per_user", "mine")):
        raise ChannelExecutionProtocolError()
    entries, seen = [], set()
    for row in rows:
        if type(row) is not dict or type(row.get("id")) is not str or not row["id"] or row["id"] in seen:
            raise ChannelExecutionProtocolError()
        seen.add(row["id"])
        permissions = row.get("permissions")
        if permissions is None:
            # Administrator inventory alone does not confer permission to chat.
            continue
        if type(permissions) is not int or not 1 <= permissions <= 7:
            raise ChannelExecutionProtocolError()
        if not permissions & 1 or row.get("status") in {"provisioning", "deleted"} or row.get("custody") != custody:
            continue
        try:
            validate_name(row.get("name"))
        except ValueError:
            raise ChannelExecutionProtocolError() from None
        entries.append(AgentListEntry(row["id"], row["name"], row.get("owner_id") == owner_user_id, permissions))
    if custody == "personal":
        entries.sort(key=lambda entry: not entry.own)
    return AgentList(owner_user_id, custody, tuple(entries[:AGENT_LIST_LINES]), len(entries) > AGENT_LIST_LINES or len(rows) == AGENT_LIST_FETCH_LIMIT, counts["per_user"] > counts["mine"])


def present_list(snapshot: AgentList, *, link, bots, details: dict[str, AgentListDetails], locale: str, product: str, administrator: bool) -> AgentListPresentation:
    """Render a qualified read; the sender must requalify every retained target."""
    if link.agent_kind != "lead_agent" or link.agent_ref != "lead_agent" or snapshot.owner_user_id != link.owner_user_id:
        raise ChannelExecutionProtocolError()
    own, shared, lines, keyboard = [], [], [], []
    owned_bots = {bot.agent_ref: bot for bot in bots if bot.owner_user_id == snapshot.owner_user_id and bot.agent_kind == "instance"}
    for entry in snapshot.entries:
        info = details.get(entry.id) if entry.permissions & 2 else None
        key = "agents.line_plain" if info is None else "agents.line_work" if info.working or info.waiting else "agents.line_idle"
        line = render_plain(key, locale, name=entry.name, role=info.role if info else None, working=info.working if info else 0, waiting=info.waiting if info else 0)
        (own if entry.own else shared).append(line)
        lines.append(line)
        bot = owned_bots.get(entry.id)
        username = getattr(bot, "bot_username", None)
        if type(username) is str and re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
            address, label = "https://t.me/" + username, "label.open_agent"
        else:
            address = page_address("web.telegram", link, target=("instance", entry.id))
            label = "label.connect_agent"
        if address is None:
            raise ChannelExecutionProtocolError()
        keyboard.append([{"label": label, "label_values": {"name": entry.name}, "url": address}])
    if snapshot.custody == "personal":
        text = render("assistants.list", locale, lines="\n".join(own)) if own else render("assistants.none", locale)
        if shared:
            text += "\n" + render("assistants.colleagues", locale, lines="\n".join(shared))
        add = "Assistant" if snapshot.can_add else None
    else:
        # Preserve the HTTP list's order for company custody.
        text = render("employees.list", locale, lines="\n".join(lines)) if lines else render("employees.none", locale, reader_is_administrator=administrator)
        add = "Employee" if administrator else None
    if snapshot.more:
        text += "\n" + render("agents.more", locale, product=product)
    if add:
        address = page_address("web.add" + add, link, reader_is_administrator=administrator)
        if address is None:
            raise ChannelExecutionProtocolError()
        keyboard.append([{"label": "label.add_assistant" if add == "Assistant" else "label.add_employee", "url": address}])
    return AgentListPresentation(text, canonical_keyboard(keyboard))
