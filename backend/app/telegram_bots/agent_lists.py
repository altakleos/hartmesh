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
    lines: tuple[dict, ...]


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
        line = {"key": key, "values": {"name": entry.name, "role": info.role if info else None, "working": info.working if info else 0, "waiting": info.waiting if info else 0}}
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
        display = [{"key": "assistants.list", "values": {"lines": ""}}, *own] if own else [{"key": "assistants.none", "values": {}}]
        if shared:
            display.extend([{"key": "assistants.colleagues", "values": {"lines": ""}}, *shared])
        add = "Assistant" if snapshot.can_add else None
    else:
        # Preserve the HTTP list's order for company custody.
        display = [{"key": "employees.list", "values": {"lines": ""}}, *lines] if lines else [{"key": "employees.none", "values": {"reader_is_administrator": administrator}}]
        add = "Employee" if administrator else None
    if snapshot.more:
        display.append({"key": "agents.more", "values": {"product": product}})
    if add:
        address = page_address("web.add" + add, link, reader_is_administrator=administrator)
        if address is None:
            raise ChannelExecutionProtocolError()
        keyboard.append([{"label": "label.add_assistant" if add == "Assistant" else "label.add_employee", "url": address}])
    return AgentListPresentation(render_lines(display, locale), canonical_keyboard(keyboard), tuple(display))


_LINE_KEYS = frozenset({"assistants.list", "assistants.none", "assistants.colleagues", "employees.list", "employees.none", "agents.line_plain", "agents.line_work", "agents.line_idle", "agents.more"})


def render_lines(lines, locale, *, html=True):
    """Render saved semantic lines in the recipient's current language."""
    if not isinstance(lines, (list, tuple)) or not 1 <= len(lines) <= AGENT_LIST_LINES + 3:
        raise ChannelExecutionProtocolError()
    result = []
    for line in lines:
        if type(line) is not dict or set(line) != {"key", "values"} or line["key"] not in _LINE_KEYS or type(line["values"]) is not dict:
            raise ChannelExecutionProtocolError()
        result.append((render if html else render_plain)(line["key"], locale, **line["values"]).rstrip("\n"))
    return "\n".join(result)


def list_parts(presentation: AgentListPresentation) -> list[dict]:
    """Keep line boundaries and fit both supported locales before publication."""
    from app.telegram_bots.split import utf16_units

    chunks, current = [], []
    for line in presentation.lines:
        candidate = [*current, line]
        if any(utf16_units(render_lines(candidate, locale, html=False)) > 4096 for locale in ("en", "zh-CN")):
            if not current:
                raise ChannelExecutionProtocolError()
            chunks.append(current)
            current = [line]
        else:
            current = candidate
    if current:
        chunks.append(current)
    return [{"kind": "text", "source_ref": {"key": "agents.presentation", "values": {"lines": chunk}}} for chunk in chunks]
