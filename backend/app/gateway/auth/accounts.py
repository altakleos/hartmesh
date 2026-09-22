"""Turn one account off or on, end its sessions, release its address, or list every account.

An operator command run inside the deployment, in the manner of
``reset_admin``; nothing reachable over HTTP does any of this. Accounts are
addressed by the identity provider's issuer and subject, which exist before
an account does; an email is accepted only when it resolves to exactly one
provider account. The schema's uniqueness is ``(provider, subject)``, so a
deployment with two providers configured at one issuer can have two accounts
for one subject: that pair is then refused, naming both, rather than acting
on whichever row came back first.

Usage:
    python -m app.gateway.auth.accounts list
    python -m app.gateway.auth.accounts disable --issuer URL --subject SUB
    python -m app.gateway.auth.accounts disable --email who@example.com
    python -m app.gateway.auth.accounts enable --issuer URL --subject SUB
    python -m app.gateway.auth.accounts end-sessions --issuer URL --subject SUB
    python -m app.gateway.auth.accounts release-email --issuer URL --subject SUB

Every form is idempotent and prints one JSON document on stdout, with the
verdict and what was done, and exits non-zero on failure (the document then
carries ``error``). The consumer runs this through a guest agent whose own
exit status does not carry the command's, so the document is the answer.

What ``disable`` does, and where the fact lives: one row in
``disabled_identities`` keyed by ``(issuer, subject)``. Every read of the
account derives ``disabled_at`` from it, and every path that acts for an
account refuses: the session cookie and personal access tokens at their next
request (``app.gateway.auth.mode.require_live_account``, ``authenticate_pat``),
the browser WebSocket, the LangGraph auth hook, an internal caller's owner
header (``owner_is_refused``), and every process-internal launch for the
owner -- a due scheduled task, a channel message, an MCP task notification
(``services._principal_projection_for_intent``). Sign-in is refused before
an account is created or returned (``user_provisioning``). On top of the
derived refusal the command ends the account's sessions (``token_version``)
and revokes its personal access tokens, so ``enable`` cannot revive them.
A run already executing is not interrupted: the Gateway reads the fact at
the next credential resolution or launch, and the command is a database
write. Its stream ends when the run does.

What ``release-email`` is for: ``users.email`` is unique, so one address
belongs to one account for good. That is right while the account is
someone's and wrong once it is nobody's. A company that deletes a person and
invites them again, or gives a departed person's address to someone new,
sends a *new* subject carrying an address an old account still holds; every
sign-in of theirs is refused and no deployer command could change it. This
one gives up a turned-off account's address, recording what it held
(``users.email_released_from``, migration 0041), and leaves everything else
about the account alone.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from typing import Any

from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from deerflow.persistence.user.access import issuer_key

COMMANDS = ("list", "disable", "enable", "end-sessions", "release-email")

# Where a released address goes. ``.example`` is reserved by RFC 2606: it can
# never be registered, so no person can ever hold an address there and nothing
# can be delivered to one. It is also an address the account record accepts,
# which the special-use domains are not -- ``@released.invalid`` is refused by
# the model's email validator, and a row it could not read back would raise on
# every later read of that account.
RELEASED_EMAIL_DOMAIN = "released.example"


def released_email_for(user_id: str) -> str:
    """The address a released account holds. Derived from its id, so releasing twice is the same address."""
    return f"released-{user_id}@{RELEASED_EMAIL_DOMAIN}"


ACTIVE_SCHEDULE_STATUSES = frozenset({"enabled", "running"})


class CommandError(Exception):
    """A refusal the document reports; the exit status is 1."""


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _account_document(user: User) -> dict[str, Any]:
    return {
        "id": str(user.id),
        "email": user.email,
        "role": user.system_role,
        "provider": user.oauth_provider,
        "issuer": issuer_key(user.oauth_issuer) if user.oauth_issuer else None,
        "subject": user.oauth_id,
        "disabled": user.disabled_at is not None,
        "disabled_at": _iso(user.disabled_at),
        "last_sign_in_at": _iso(user.last_sign_in_at),
        # Released is a column, not the shape of the address: nothing here
        # parses an email to decide what an account is.
        "released": user.email_released_from is not None,
        "released_from": user.email_released_from,
    }


class AccountsCommand:
    """The five forms over one users repository, a token store and a schedule store."""

    def __init__(self, users: SQLiteUserRepository, *, tokens: Any | None, schedules: Any | None) -> None:
        self._users = users
        self._tokens = tokens
        self._schedules = schedules

    async def run(self, command: str, *, issuer: str | None = None, subject: str | None = None, email: str | None = None) -> dict[str, Any]:
        if command == "list":
            return await self.list()
        identity, account = await self._resolve(issuer=issuer, subject=subject, email=email)
        if command == "disable":
            return await self.disable(identity, account)
        if command == "enable":
            return await self.enable(identity, account)
        if command == "end-sessions":
            return await self.end_sessions(identity, account)
        if command == "release-email":
            return await self.release_email(identity, account)
        raise CommandError(f"unknown command {command!r}; one of {', '.join(COMMANDS)}")

    # ── Selecting the account ────────────────────────────────────────────

    async def _resolve(self, *, issuer: str | None, subject: str | None, email: str | None) -> tuple[tuple[str, str], User | None]:
        by_identity = issuer is not None or subject is not None
        if by_identity and email is not None:
            raise CommandError("address the account by --issuer and --subject, or by --email, not both")
        if by_identity:
            if not (issuer or "").strip() or not (subject or "").strip():
                raise CommandError("--issuer and --subject go together; give both")
            key = (issuer_key(issuer or ""), (subject or "").strip())
            # Uniqueness is (provider, subject); two configured providers may
            # point at one issuer, and then this pair names two accounts.
            # Acting on whichever came back first would be a wrong-target
            # write, so say so and let the deployer address one by its email.
            candidates = await self._users.list_users_by_identity(*key)
            if len(candidates) > 1:
                named = ", ".join(sorted(f"{account.oauth_provider} ({account.email})" for account in candidates))
                raise CommandError(f"{len(candidates)} accounts have subject {key[1]!r} at this issuer, one per configured provider: {named}; address one of them by --email")
            return key, (candidates[0] if candidates else None)
        if email is None or not email.strip():
            raise CommandError("address the account by --issuer and --subject, or by --email")
        account = await self._users.get_user_by_email(email.strip())
        if account is None:
            raise CommandError(f"no account has the email {email.strip()!r}; a person with no account is addressed by --issuer and --subject")
        if not account.oauth_provider or not account.oauth_id:
            raise CommandError(f"the account with email {email.strip()!r} has no identity-provider identity (a local-password account); this command addresses provider accounts")
        if not account.oauth_issuer:
            raise CommandError(f"the account with email {email.strip()!r} was linked before its issuer was recorded and has not signed in since; address it by --issuer (the provider's configured issuer) and --subject {account.oauth_id!r}")
        return (issuer_key(account.oauth_issuer), account.oauth_id), account

    # ── The forms ────────────────────────────────────────────────────────

    async def list(self) -> dict[str, Any]:
        accounts = [_account_document(user) for user in await self._users.list_users()]
        with_account = {(entry["issuer"], entry["subject"]) for entry in accounts if entry["issuer"]}
        without = [{"issuer": issuer, "subject": subject, "disabled_at": _iso(disabled_at)} for issuer, subject, disabled_at in await self._users.list_disabled_identities() if (issuer, subject) not in with_account]
        return {"command": "list", "accounts": accounts, "disabled_without_account": without}

    async def disable(self, identity: tuple[str, str], account: User | None) -> dict[str, Any]:
        issuer, subject = identity
        recorded = await self._users.disable_identity(issuer, subject)
        # Looked up again only when there was nothing to address: a first
        # sign-in racing the command may have created the account in between,
        # and its session must be ended like any other. Re-resolving
        # unconditionally would throw away the account the deployer actually
        # named -- with --email, the one whose address they typed.
        if account is None:
            account = await self._users.get_user_by_identity(issuer, subject)
        document: dict[str, Any] = {
            "command": "disable",
            "identity": {"issuer": issuer, "subject": subject},
            "verdict": "disabled" if recorded else "already_disabled",
            "account": None,
            "sessions_ended": False,
            "tokens_revoked": 0,
            "schedules_held": 0,
        }
        # The refusal is keyed by (issuer, subject): it is the person at the
        # provider who is turned off, so it covers every account that identity
        # has here. Only one of them is the account addressed, and only that
        # one's sessions and tokens are ended -- say so rather than let the
        # deployer discover it.
        siblings = [other for other in await self._users.list_users_by_identity(issuer, subject) if account is None or str(other.id) != str(account.id)]
        if siblings:
            document["identity_also_covers"] = [{"provider": other.oauth_provider, "email": other.email, "id": str(other.id)} for other in siblings]
        if account is None:
            document["note"] = "no account existed for this identity; the refusal is recorded and a sign-in that would create one is refused"
            return document
        # The derived refusal already stops every path; ending the sessions
        # and revoking the tokens is what keeps them dead after an enable.
        document["sessions_ended"] = await self._users.end_sessions(str(account.id))
        document["tokens_revoked"] = await self._revoke_tokens(account)
        document["schedules_held"] = await self._count_schedules(account)
        document["account"] = _account_document(await self._users.get_user_by_id(str(account.id)) or account)
        document["note"] = "sessions are refused at their next request; a run already executing finishes and its stream ends with it; no new run starts for this account"
        return document

    async def enable(self, identity: tuple[str, str], account: User | None) -> dict[str, Any]:
        issuer, subject = identity
        withdrawn = await self._users.enable_identity(issuer, subject)
        refreshed = await self._users.get_user_by_id(str(account.id)) if account is not None else None
        return {
            "command": "enable",
            "identity": {"issuer": issuer, "subject": subject},
            "verdict": "enabled" if withdrawn else "already_enabled",
            "account": _account_document(refreshed) if refreshed is not None else None,
            "note": "the person may sign in again; sessions ended and tokens revoked by disable stay ended and revoked",
        }

    async def end_sessions(self, identity: tuple[str, str], account: User | None) -> dict[str, Any]:
        issuer, subject = identity
        if account is None:
            raise CommandError(f"no account exists for subject {subject!r} at issuer {issuer!r}; there are no sessions to end")
        await self._users.end_sessions(str(account.id))
        return {
            "command": "end-sessions",
            "identity": {"issuer": issuer, "subject": subject},
            "verdict": "sessions_ended",
            "account": _account_document(account),
            "sessions_ended": True,
            "tokens_revoked": 0,
            "note": "every open session is refused at its next request; personal access tokens are untouched; the next sign-in re-reads the claim",
        }

    async def release_email(self, identity: tuple[str, str], account: User | None) -> dict[str, Any]:
        """Give up the address of an account that is turned off, so a person may hold it again.

        Only an account nobody can use any more: while a person can still
        sign in, their address is theirs. The account keeps its subject, its
        issuer, its role, its turned-off state and everything it holds.
        """
        issuer, subject = identity
        if account is None:
            raise CommandError(f"no account exists for subject {subject!r} at issuer {issuer!r}; there is no address to release")
        if account.disabled_at is None:
            raise CommandError(f"the account of subject {subject!r} at issuer {issuer!r} is not turned off; releasing an address is for an account nobody can use any more, so turn it off first with `disable`")
        released = await self._users.release_email(str(account.id), replacement=released_email_for(str(account.id)))
        refreshed = await self._users.get_user_by_id(str(account.id)) or account
        if released is None:
            return {
                "command": "release-email",
                "identity": {"issuer": issuer, "subject": subject},
                "verdict": "already_released",
                "released": refreshed.email_released_from,
                "account": _account_document(refreshed),
                "note": "this account gave up its address already; the address it held is in `released`",
            }
        return {
            "command": "release-email",
            "identity": {"issuer": issuer, "subject": subject},
            "verdict": "released",
            "released": released,
            "account": _account_document(refreshed),
            "note": "a sign-in carrying that address may now create a new account for its own subject; this account keeps its subject, its role, its turned-off state and everything it holds",
        }

    # ── What disable reaches beyond the row ──────────────────────────────

    async def _revoke_tokens(self, account: User) -> int:
        if self._tokens is None:
            return 0
        revoked = 0
        for record in await self._tokens.list_for_user(str(account.id)):
            if record.get("revoked_at") is None and await self._tokens.revoke(str(record["id"]), str(account.id)):
                revoked += 1
        return revoked

    async def _count_schedules(self, account: User) -> int:
        if self._schedules is None:
            return 0
        tasks = await self._schedules.list_by_user(str(account.id))
        return sum(1 for task in tasks if task.get("status") in ACTIVE_SCHEDULE_STATUSES)


# ── Running it inside the deployment ────────────────────────────────────


async def _run(command: str, *, issuer: str | None, subject: str | None, email: str | None) -> dict[str, Any]:
    from deerflow.config import get_app_config
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    config = get_app_config()
    if config.database.backend == "memory":
        raise CommandError("the memory database backend keeps no accounts between processes; this command needs config.database on sqlite or postgres")
    # The same tenant the Gateway resolves at construction: the token store
    # filters rows by it.
    tenant = TenantIdentityV1.resolve(deployment_config=config.deployment, environ=os.environ).to_persisted_reference()
    await init_engine_from_config(config.database)
    try:
        session_factory = get_session_factory()
        if session_factory is None:
            raise CommandError("persistence engine not available (check config.database)")
        command_runner = AccountsCommand(
            SQLiteUserRepository(session_factory),
            tokens=PersonalAccessTokenRepository(session_factory, tenant=tenant),
            schedules=ScheduledTaskRepository(session_factory),
        )
        return await command_runner.run(command, issuer=issuer, subject=subject, email=email)
    finally:
        await close_engine()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m app.gateway.auth.accounts", description="Turn one account off or on, end its sessions, release its address, or list every account. Not a network route.")
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument("--issuer", help="the identity provider's issuer URL, as configured")
    parser.add_argument("--subject", help="the person's subject at that issuer")
    parser.add_argument("--email", help="convenience: the account's email, when it resolves to exactly one provider account")
    args = parser.parse_args(argv)
    try:
        document = asyncio.run(_run(args.command, issuer=args.issuer, subject=args.subject, email=args.email))
    except CommandError as exc:
        print(json.dumps({"command": args.command, "error": str(exc)}, sort_keys=True), flush=True)
        return 1
    except Exception as exc:  # noqa: BLE001 - the document is the answer the consumer reads, whatever failed
        print(json.dumps({"command": args.command, "error": f"{type(exc).__name__}: {exc}"}, sort_keys=True), flush=True)
        return 1
    print(json.dumps(document, sort_keys=True), flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
