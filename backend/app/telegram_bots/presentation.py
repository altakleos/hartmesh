"""Current instance facts for private greetings, help and navigation."""

from app.telegram_bots.authority import AuthorityUnavailable, ceilings, get_local_provider, read_by_others
from app.telegram_bots.roles import role_of
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained


async def instance_values(authority, link, *, product: str) -> dict:
    view = await authority.instance_view(link)
    role = await role_of(authority.app, PrincipalRef("human", link.owner_user_id), view.id)
    colleague = view.custody == "personal" and view.owner_id != link.owner_user_id
    owner = None
    if colleague:
        person = await await_drained(get_local_provider().get_user(view.owner_id))
        if person is None:
            raise AuthorityUnavailable
        owner = person.email
    return {
        "product": product,
        "name": view.name,
        "agent": view.name,
        "custody": view.custody,
        "owner": owner,
        "role": role["name"] if role else None,
        "role_description": role["description"] if role else None,
        "read_by_others": colleague or await read_by_others(link.owner_user_id, view, session_factory=authority.links.session_factory),
        "protected": await authority.instance_policy(link) == "protected",
    }


async def page_permissions(authority, link) -> dict:
    if link.agent_kind != "instance":
        return {}
    permissions = await ceilings(link.owner_user_id)
    view = await authority.instance_view(link)
    return {"permissions": view.permissions, "ceilings": permissions}


def connections_enabled():
    from deerflow.config import get_app_config

    policy = getattr(get_app_config(use_runtime_override=False), "agent_connections", None)
    return policy is not None and policy.enabled


def visible_connections(instance_id):
    from deerflow.agent_instances.connections import list_connections

    return list_connections(instance_id, manager=False)


CONNECTION_DISCLOSURE_KEYS = {"hello.employee", "hello.assistant", "new.done", "new.done_instance", "privacy.full"}


async def connection_disclosure(services, link, key, locale, renderer):
    """Resolve only current usable names at delivery. Queued values contain no roster."""
    from app.telegram_bots.split import utf16_units
    from deerflow.agent_instances.contract import AgentConflict
    from deerflow.utils.file_io import run_file_io

    if link.agent_kind != "instance" or key not in CONNECTION_DISCLOSURE_KEYS:
        return ""
    if not await run_file_io(connections_enabled):
        return ""
    authority = services.authority
    if await authority.instance_send_policy(link, require_use=True) not in {"allowed", "protected"}:
        return ""
    view = await authority.instance_view(link)
    try:
        current = await run_file_io(visible_connections, link.agent_ref)
    except (OSError, ValueError, AgentConflict):
        return renderer("privacy.connections_unavailable", locale)
    names = [item["name"] for item in current]
    if not names:
        return ""
    # Reserve room for the fixed text and keep the roster readable on phones.
    listed = []
    width = 0
    for name in names:
        cost = utf16_units(name) + (2 if listed else 0)
        if width + cost > 320:
            break
        listed.append(name)
        width += cost
    others = len(names) - len(listed)
    if others:
        listed.append(renderer("privacy.connections_more", locale, count=others))
    key = "privacy.connections" if key == "privacy.full" else "hello.connections"
    return renderer(key, locale, name=view.name, names=", ".join(listed))
