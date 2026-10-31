"""Build navigation from configured origins and stored Telegram bot targets."""

from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import quote, urlencode, urlsplit, urlunsplit

from app.telegram_bots.config import load_config
from deerflow.agent_instances.contract import AgentPermission
from deerflow.config.app_config import get_app_config

if TYPE_CHECKING:
    from app.telegram_bots.links import BotLink

_KINDS = {"lead_agent", "custom_agent", "instance"}
_TABS = {"web.access": "access", "web.files": "files", "web.memory": "memory", "web.conversations": "conversations", "web.work": "work", "web.agentSettings": "settings"}
_MANAGE = {"web.access", "web.agentSettings"}
WEB_PAGE_BUTTONS_MAX = 3
_PAGE_BUTTONS = {
    "files": ("web.files", "label.open_files"),
    "memory": ("web.memory", "label.open_memory"),
    "conversations": ("web.conversations", "label.open_conversations"),
    "addAssistant": ("web.addAssistant", "label.add_assistant"),
    "addEmployee": ("web.addEmployee", "label.add_employee"),
    "telegram": ("web.telegram", "label.open_telegram"),
    "timezone": ("web.timezone", "label.open_telegram"),
    "scheduled": ("web.scheduled", "label.open_scheduled"),
    "account": ("web.account", "label.open_product"),
}
_INSPECT = {"web.files", "web.work", "web.memory"}


def _base_url() -> str | None:
    base = load_config(get_app_config()).public_base_url
    if not base or any(ord(character) <= 32 or ord(character) == 127 for character in base):
        return None
    try:
        parsed = urlsplit(base)
        if parsed.scheme not in {"https", "http"} or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
            return None
        _ = parsed.port
    except ValueError:
        return None
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))


def _target(kind: str, ref: str) -> str | None:
    if kind == "lead_agent":
        return "chat" if ref == "lead_agent" else None
    if kind in {"custom_agent", "instance"} and ref:
        return ("agent" if kind == "instance" else "custom_agent") + "." + ref
    return None


def page_address(
    key: str,
    link: BotLink,
    *,
    thread_id: str | None = None,
    work_id: str | None = None,
    request_id: str | None = None,
    target: tuple[str, str] | None = None,
    permissions: AgentPermission | None = None,
    ceilings: frozenset[str] | None = None,
    reader_is_administrator: bool = False,
) -> str | None:
    """Return a handover URL, never a sign-in or authorization capability.

    Instance tabs with restricted visibility require a current grant snapshot.
    Actual page access still belongs to the web app; this helper only omits
    navigation that the supplied reader cannot use.
    """
    base = _base_url()
    if base is None or link.agent_kind not in _KINDS or _target(link.agent_kind, link.agent_ref) is None:
        return None

    def workspace(path: str = "", **query: str) -> str:
        return base + "/workspace" + path + ("?" + urlencode(query) if query else "")

    def conversation() -> str | None:
        if not thread_id or thread_id in {".", ".."} or (link.agent_kind == "custom_agent" and link.agent_ref in {".", ".."}):
            return None
        path = "/chats/" + quote(thread_id, safe="")
        if link.agent_kind == "custom_agent":
            path = "/agents/" + quote(link.agent_ref, safe="") + path
        return workspace(path)

    if key == "web.conversation" or key == "web.resultTooLarge":
        return conversation()
    if key == "web.fileTooLarge":
        if link.agent_kind == "instance" and permissions is not None and permissions & (AgentPermission.MANAGE | AgentPermission.INSPECT) == AgentPermission.MANAGE | AgentPermission.INSPECT:
            return workspace("/instances", instance=link.agent_ref, tab="files")
        return conversation()
    if key == "web.addAssistant":
        return workspace("/assistants")
    if key == "web.addEmployee":
        return workspace("/employees") if reader_is_administrator else None
    if key == "web.telegram":
        selected = _target(*(target if target is not None else (link.agent_kind, link.agent_ref)))
        return workspace(settings="channels", telegram=selected) if selected is not None else None
    if key == "web.timezone":
        return workspace(settings="channels")
    if key == "web.scheduled":
        return workspace("/scheduled-tasks")
    if key == "web.account":
        return workspace(settings="account")
    if key == "web.request":
        return workspace("/attention", request=request_id) if request_id else None

    if link.agent_kind != "instance":
        if key == "web.memory" and link.agent_kind == "lead_agent":
            return workspace(settings="memory")
        return workspace("/chats") if key == "web.conversations" else None

    if key == "policy.off":
        return workspace("/instances", instance=link.agent_ref)
    if key not in _TABS:
        return None
    required = AgentPermission.MANAGE if key in _MANAGE else AgentPermission.INSPECT if key in _INSPECT else AgentPermission(0)
    if required and (permissions is None or permissions & required != required):
        return None
    if key == "web.memory" and (ceilings is None or "memory:read" not in ceilings):
        return None
    if key == "web.work" and work_id:
        return workspace("/instances", instance=link.agent_ref, work=work_id)
    return workspace("/instances", instance=link.agent_ref, tab=_TABS[key])


def page_buttons(keys: tuple[str, ...], link: BotLink, *, reader_is_administrator: bool = False) -> list[tuple[str, str]]:
    """(label, address) for a turn's first offers. The model only ever names a key."""
    buttons = []
    for key in keys[:WEB_PAGE_BUTTONS_MAX]:
        page, label = _PAGE_BUTTONS.get(key, (None, None))
        address = page_address(page, link, reader_is_administrator=reader_is_administrator) if page is not None else None
        if address is not None:
            buttons.append((label, address))
    return buttons
