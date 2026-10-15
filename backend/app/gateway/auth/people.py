"""Session directory data; account identifiers never confer authority."""

from typing import Annotated

from fastapi import HTTPException
from pydantic import AfterValidator

from app.gateway.auth.mode import account_refusal, local_registration_open
from app.gateway.deps import get_user_repo
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import run_file_io


def _identifier(value: str) -> str:
    PrincipalRef("human", value)
    return value


UserIdentifier = Annotated[str, AfterValidator(_identifier)]


async def lookup_people(*, query: str | None, ids: list[str] | None, limit: int) -> list[dict]:
    if (query is None) == (ids is None):
        raise HTTPException(400, "Give either query or ids")
    if ids is not None and len(ids) > 50:
        raise HTTPException(400, "At most 50 ids")
    exact = await run_file_io(local_registration_open)
    if query is not None and not exact and len(query) > 64:
        raise HTTPException(400, "Search text is at most 64 characters")
    repository = get_user_repo()
    if repository is None:
        return []
    people, seen, offset = [], set(), 0
    while True:
        users = await repository.lookup_users(query=query, ids=ids, exact=exact, offset=offset)
        active = await run_file_io(lambda: [account_refusal(user) is None for user in users])
        for user, live in zip(users, active, strict=True):
            identity = str(user.id)
            if identity in seen or (ids is None and not live):
                continue
            people.append({"id": identity, "email": user.email, "name": None, "active": live})
            seen.add(identity)
            if ids is None and len(people) >= (1 if exact else limit):
                return people
        if ids is not None or len(users) < 50:
            return people
        offset += len(users)
