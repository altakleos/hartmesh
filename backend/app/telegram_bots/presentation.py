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
