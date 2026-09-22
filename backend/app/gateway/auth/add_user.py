"""Add a local-password account for someone, from inside the deployment.

The deployment-side surface of ``app.gateway.auth.local_accounts``, for a
deployer with a shell in the container and no browser session, in the manner
of ``reset_admin`` and ``accounts``. The same operation as
``POST /api/v1/auth/users``: role ``user``, a one-time password that opens
nothing but the account's setup, refused in sign-on-only mode and for an
address any account already holds.

Usage:
    python -m app.gateway.auth.add_user --email who@example.com

Prints one JSON document on stdout. ``0``: the account was added, and the
document carries ``one_time_password`` -- the only copy there will ever be;
hand it to the person, who chooses their own password at first sign-in.
``1``: nothing was added, and the document carries ``error`` (a stable code:
``sign_on_required``, ``email_already_exists``, ``email_invalid``, ``usage``
or ``failed``) and ``message``. A caller that runs this through a remote
runner may not see its exit status, so the document is the answer. Nothing
is logged: the password leaves this process on stdout only.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from typing import Any, NoReturn

from pydantic import EmailStr, TypeAdapter, ValidationError

from app.gateway.auth.local_accounts import AddAccountRefused, add_local_account, refuse_unless_local_passwords

COMMAND = "add-user"


class CommandError(Exception):
    """A refusal the document reports with ``code``; the exit status is 1."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _email(raw: str) -> str:
    """The address as ``/register`` would accept it (``EmailStr``), or refuse."""
    try:
        return str(TypeAdapter(EmailStr).validate_python(raw.strip()))
    except ValidationError:
        # Never echoed: whatever was typed here is the deployer's, and this
        # document may be logged by the agent that runs the command.
        raise CommandError("email_invalid", "--email must be one email address") from None


async def _run(email: str) -> dict[str, Any]:
    from app.gateway.auth.local_provider import LocalAuthProvider
    from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
    from deerflow.config import get_app_config
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config

    config = get_app_config()
    # Said before the database is opened: in sign-on-only mode there is
    # nothing here to add.
    refuse_unless_local_passwords()
    if config.database.backend == "memory":
        raise CommandError("failed", "the memory database backend keeps no accounts between processes; this command needs config.database on sqlite or postgres")
    await init_engine_from_config(config.database)
    try:
        session_factory = get_session_factory()
        if session_factory is None:
            raise CommandError("failed", "persistence engine not available (check config.database)")
        added = await add_local_account(LocalAuthProvider(SQLiteUserRepository(session_factory)), email)
    finally:
        await close_engine()
    return {
        "command": COMMAND,
        "id": str(added.user.id),
        "email": added.user.email,
        "system_role": added.user.system_role,
        "needs_setup": added.user.needs_setup,
        "one_time_password": added.one_time_password,
    }


USAGE = "usage: python -m app.gateway.auth.add_user --email ADDRESS"


class _Parser(argparse.ArgumentParser):
    """A command line it cannot read is a refusal like any other: one document, never usage text.

    ``-h`` included (``add_help=False``): help text on stdout would be a
    second shape for the caller to parse.

    The message is fixed rather than argparse's own, which quotes whatever was
    typed -- a password pasted into the wrong place included.
    """

    def error(self, message: str) -> NoReturn:  # noqa: ARG002 - deliberately not echoed
        raise CommandError("usage", USAGE)


def _refusal(code: str, message: str) -> int:
    print(json.dumps({"command": COMMAND, "error": code, "message": message}, sort_keys=True), flush=True)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(prog="python -m app.gateway.auth.add_user", add_help=False, description="Add a local-password account (role user) whose first sign-in chooses its password. Local-password mode only.")
    parser.add_argument("--email", required=True, help="the person's email address")
    try:
        args = parser.parse_args(argv)
        document = asyncio.run(_run(_email(args.email)))
    except CommandError as exc:
        return _refusal(exc.code, exc.message)
    except AddAccountRefused as exc:
        return _refusal(str(exc.code), exc.message)
    except Exception as exc:  # noqa: BLE001 - the document is the answer, whatever failed
        return _refusal("failed", type(exc).__name__)
    print(json.dumps(document, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
