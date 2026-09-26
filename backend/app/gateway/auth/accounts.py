"""Turn one account off or on, end its sessions, limit its role, release its address, or list every account.

An operator command run inside the deployment, in the manner of
``reset_admin``; nothing reachable over HTTP does any of this. Accounts are
addressed by the identity provider's issuer and subject, which exist before
an account does; an email is accepted only when it resolves to exactly one
provider account. The schema's uniqueness is ``(provider, subject)``, so a
deployment with two providers configured at one issuer can have two accounts
for one subject: that pair is then refused, naming both, rather than acting
on whichever row came back first -- except by the role-limit verbs, whose
limit is the person's and so reaches both.

Usage:
    python -m app.gateway.auth.accounts list
    python -m app.gateway.auth.accounts disable --issuer URL --subject SUB
    python -m app.gateway.auth.accounts disable --email who@example.com
    python -m app.gateway.auth.accounts enable --issuer URL --subject SUB
    python -m app.gateway.auth.accounts end-sessions --issuer URL --subject SUB
    python -m app.gateway.auth.accounts end-sessions --email who@example.com --end-running-work
    python -m app.gateway.auth.accounts release-email --issuer URL --subject SUB
    python -m app.gateway.auth.accounts limit-role --issuer URL --subject SUB [--role user] [--end-running-work]
    python -m app.gateway.auth.accounts lift-role-limit --issuer URL --subject SUB

Every form is idempotent and prints one JSON document on stdout, with the
verdict and what was done. Three exit statuses: ``0`` means done; ``1`` means
the command refused and changed nothing (the document then carries ``error``);
``2`` means it did what was asked but could not confirm all of it: a run it
cancelled had not stopped (named under ``runs_unconfirmed``), or a surface
had not been confirmed stopped (named under ``surfaces_unconfirmed`` -- a
run's ``running_work``, or the connections a Gateway process had not yet
recorded closing).
A malformed command line is a refusal like any other: a document and ``1``,
never argparse's usage text and the ``2`` that would read as "unconfirmed".
A caller that runs this through a remote runner may not see its exit status,
so the document is the answer.

What ``disable`` does, and where the fact lives: one row in
``disabled_identities`` keyed by ``(issuer, subject)``. Every read of the
account derives ``disabled_at`` from it, and every path that acts for an
account refuses: the session cookie and personal access tokens at their next
request (``app.gateway.auth.mode.require_live_account``, ``authenticate_pat``),
the browser WebSocket, the LangGraph auth hook, an internal caller's owner
header (``owner_is_refused``), and every process-internal launch for the
owner -- a due scheduled task, a channel message, an MCP task notification
(``services._principal_projection_for_intent``). Sign-in is refused before
an account is created or returned (``user_provisioning``), and a run that
was admitted just before the refusal committed is refused as it starts
(``services._owner_refusal``). On top of the derived refusal the command ends
the sessions (``token_version``) and revokes the personal access tokens of
every account the identity covers, so ``enable`` cannot revive them.

A connection that authenticated once never reads the refusal again: an SSE
stream, a streaming download, the browser WebSocket. Every Gateway process
holds each one under its owner (``app.gateway.owner_connections``) and
closes what a refused owner holds (``app.gateway.refusal_watch``); the
command asks every live process to look (``refusal_checks``), waits -- while
the runs unwind, within the same ``--wait-seconds`` -- for each to record
that it did and what it ended, and reports ``websockets``, ``sse_streams``
and ``downloads`` from that record. A process beats even when it cannot
look, so one that is alive and has not recorded its look when the wait runs
out leaves those surfaces unconfirmed; only one that has not beaten for 90 s
is gone, holding nothing.

It also ends the account's running work. The refusal stops the next request
and the next launch, but a run already executing was the one path left: a
tool call can run for minutes, keep writing files and calling out through
the sandbox's network, and deliver its result into a thread after the
person was removed. Every non-terminal run the account owns is cancelled
through the same durable request a person's own cancel makes, which the
owning worker applies -- including a run owned by another worker. The
command then waits, bounded, for each to reach a terminal status and
reports what it saw: ``runs_found``, ``runs_cancelled``, the ids under
``runs_finished_first`` of any that completed on their own before the
cancellation reached them, and the ids under ``runs_unconfirmed`` of any
that did not stop -- which is a failure carrying ``returncode`` 2, never a
silent success. ``--wait-seconds`` moves the bound. What it cancels is
every account the refusal covers, not only the one named: one identity can
hold an account under each configured provider, and both are refused. After
the wait it looks once more, for a run that a request authenticated just
before the refusal inserted meanwhile.

``end-sessions`` does the same only when asked, with
``--end-running-work``: demoting an administrator is not removing them, and
their run keeps going unless the deployer says otherwise.

What ``release-email`` is for: ``users.email`` is unique, so one address
belongs to one account for good. That is right while the account is
someone's and wrong once it is nobody's. A company that deletes a person and
invites them again, or gives a departed person's address to someone new,
sends a *new* subject carrying an address an old account still holds; every
sign-in of theirs is refused and no deployer command could change it. This
one gives up a turned-off account's address, recording what it held
(``users.email_released_from``, migration 0041), and leaves everything else
about the account alone.

What ``limit-role`` is for: demoting an administrator at the provider reaches
nothing here until they sign in again, and the provider can be restored from
a backup whose claim says ``admin`` -- so a demotion the next sign-in could
override would hand the role back between the deployer's passes. The limit
is one row in ``role_limits`` keyed by ``(issuer, subject)`` (migration
0043), valid before an account exists and covering every account the
identity holds. Every read of an account derives its role from it, the way
the turned-off state is derived, so every path that reads the stored role --
a session's next request and what it may see of other people's runs, a run a
personal access token starts, a scheduled or channel launch -- takes the
limited role at once. The stored column is kept at or below it too: the
limit lowers it in its own transaction, a sign-in stores the lower of its
claim and the limit in the statement that stores the role, a first sign-in
applies it again inside the insert's transaction, and ``lift-role-limit``
leaves the column where the limit held it, so lifting changes nothing until
a sign-in reads the role again. On PostgreSQL, whose statements read other
tables as of their own start, all of these take one transaction lock per
identity, so none of them can act on a limit it read before another
committed. The limit ends the sessions of every covered account, as
``end-sessions`` does but in the transaction that records the limit, so a
command interrupted after it leaves none open; and it leaves a run already
executing alone unless
``--end-running-work`` is given, which cancels only runs admitted with a
role above the limit: such a run keeps the role it started with, which
matters only where something reads a run's role -- ``authorization.enabled``,
or ``guardrails`` with a provider -- and the document names which of those
this deployment has (``role_read_by``). A re-run with nothing to lower
changes nothing (``changed`` is false) and signs nobody out, and work the
person starts afterwards is not above the limit, so a deployer may re-apply
its record every pass, with or without the flag. With
local passwords on it refuses to limit the last administrator, because a
deployment with none offers first-boot setup to whoever reaches it first.

The ``disable`` and role-limit documents say when each surface stopped:
``started_at`` (UTC), ``elapsed_ms`` for the whole command, and under
``surfaces`` one entry per surface with its ``action``, its ``count``,
``stopped_after_ms`` on a monotonic clock from the command's start, and
``stopped_at``, that offset added to ``started_at``. A surface refused or
limited at its next use reports the commit. A surface a Gateway process
ends reports when every live process had confirmed (``confirmed_by:
gateway_record``), with how many each could not confirm ended
(``not_ended``). What a process keeps for a person between requests
(``app.gateway.retained_state``: sandboxes, pooled MCP sessions, browsers,
queued memory updates) is confirmed by a second check once the runs are
over, because a run that is ending parks its sandbox as it goes; a surface
some process has no way to end at all is ``not_reached``. ``running_work``
reports when the runs' sandboxes were confirmed stopped (``confirmed_by:
sandbox_gone``), or, where they were not, only when the run rows went
terminal (``run_status``). A surface the command could not confirm is named
under ``surfaces_unconfirmed`` and makes the exit status 2;
``runs_unconfirmed`` keeps naming only runs.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from app.gateway.auth.models import User
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.gateway.retained_state import RETAINED_SURFACES
from deerflow.persistence.user.access import LIMIT_ROLES, issuer_key, limited_role
from deerflow.runtime.owner_holdings import ANY_OWNER

logger = logging.getLogger(__name__)

COMMANDS = ("list", "disable", "enable", "end-sessions", "release-email", "limit-role", "lift-role-limit")

#: The forms that take ``--end-running-work``; ``disable`` always ends the work.
ENDS_WORK_ON_REQUEST = ("end-sessions", "limit-role")

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

#: A run has stopped once its row reads one of these.
TERMINAL_RUN_STATUSES = frozenset({"success", "error", "timeout", "interrupted"})

#: How long to wait for the runs this command cancelled to reach a terminal
#: status. The owning worker applies a durable cancellation on its next
#: observation -- every five seconds without a lease heartbeat, every
#: ``lease_seconds / 3`` with one -- and then has to unwind the graph and the
#: tool call around it, which on a remote sandbox includes bounded abort
#: requests of its own. Waiting costs nothing when the runs stop (the wait ends
#: as soon as they do), and reporting a run unconfirmed that was merely slow
#: sends the deployer looking for a fault that is not there, so the default is
#: generous; ``--wait-seconds`` moves it.
DEFAULT_RUN_WAIT_SECONDS = 120.0

#: How often the command re-reads the rows it is waiting on.
RUN_WAIT_POLL_SECONDS = 0.5

#: Exit status when the command did what was asked but could not confirm that
#: every run it cancelled had stopped. Distinct from 1, which means the command
#: refused and changed nothing: an offboarding script has to be able to tell
#: "turned off, one run still unwinding" from "did not run".
EXIT_UNCONFIRMED_RUNS = 2


class CommandError(Exception):
    """A refusal the document reports; the exit status is 1."""


#: What a surface entry says was done to it.
ACTION_LOWERED = "lowered"
ACTION_ENDED = "ended"
ACTION_LIMITED_AT_NEXT_USE = "limited_at_next_use"
ACTION_ALREADY_LIMITED = "already_limited"
ACTION_LEFT_ALONE = "left_alone"
ACTION_LIFTED = "lifted"
ACTION_REFUSED_AT_NEXT_USE = "refused_at_next_use"
ACTION_REVOKED = "revoked"
#: A surface some live Gateway process has no way to end (``gateway_processes.unreached``).
ACTION_NOT_REACHED = "not_reached"

#: What a ``running_work`` stop time was confirmed from: the run rows reached
#: a terminal status. The cancellation also attempts to kill the sandbox
#: command in flight, which this process cannot observe.
CONFIRMED_BY_RUN_STATUS = "run_status"

#: A ``running_work`` stop time that is also when every live process
#: confirmed it had stopped the sandboxes the runs used, and with them the
#: command in flight, its children and anything the runs left running.
CONFIRMED_BY_SANDBOX_GONE = "sandbox_gone"

#: What a connection surface's stop time was confirmed from: every live
#: Gateway process recorded that it had looked for refused owners after the
#: refusal committed, and what it ended (``app.gateway.refusal_watch``).
CONFIRMED_BY_GATEWAY_RECORD = "gateway_record"

#: The connections a Gateway process holds open for an account after it
#: authenticated once (``app.gateway.owner_connections``).
CONNECTION_SURFACES = ("websockets", "sse_streams", "downloads")

#: Surfaces whose state outlives the process that kept it: a sandbox's
#: container keeps running after its Gateway is gone, so with no live process
#: to confirm them they are unconfirmed, never "nothing held".
OUTLIVE_THEIR_PROCESS = ("sandboxes",)

#: How often the command re-reads the processes' record while it waits.
SWEEP_WAIT_POLL_SECONDS = 0.2


def _sealed_role_above(row: dict[str, Any], limit: str) -> bool:
    """Whether a run was admitted with a role above ``limit``; a run whose role is not recorded counts as above."""
    projection = row.get("principal_projection_json")
    sealed = projection.get("role") if isinstance(projection, dict) else None
    if not isinstance(sealed, str):
        return True
    return limited_role(sealed, limit) != sealed


class SurfaceClock:
    """When each surface stopped, measured from the command's start.

    One anchor: ``started_at`` is read once, and every entry's wall time is
    that plus its monotonic offset, so a wall-clock step during the command
    cannot make two of its own times disagree. The caller anchors them on its
    own clock with ``elapsed_ms``.
    """

    def __init__(self) -> None:
        self._start = time.monotonic()
        self.started_at = datetime.now(UTC)
        self.surfaces: dict[str, dict[str, Any]] = {}
        self.unconfirmed: list[str] = []

    def now_ms(self) -> int:
        return int((time.monotonic() - self._start) * 1000)

    def stopped(self, surface: str, action: str, count: int, *, at_ms: int | None = None, **facts: Any) -> None:
        """Record that ``surface`` stopped ``at_ms`` (now when omitted)."""
        offset = self.now_ms() if at_ms is None else at_ms
        self.surfaces[surface] = {"action": action, "count": count, "stopped_after_ms": offset, "stopped_at": (self.started_at + timedelta(milliseconds=offset)).isoformat(), **facts}

    def not_stopped(self, surface: str, action: str, count: int, *, unconfirmed: bool, **facts: Any) -> None:
        """Record a surface with no stop time: left running on purpose, or not confirmed."""
        self.surfaces[surface] = {"action": action, "count": count, "stopped_after_ms": None, "stopped_at": None, **facts}
        if unconfirmed:
            self.unconfirmed.append(surface)

    def document(self) -> dict[str, Any]:
        return {"started_at": self.started_at.isoformat(), "elapsed_ms": self.now_ms(), "surfaces": self.surfaces, "surfaces_unconfirmed": sorted(self.unconfirmed)}


def run_role_readers(config: Any) -> tuple[str, ...]:
    """What in this deployment reads a run's role while it executes.

    ``authorization`` (``authorization.enabled``) drives the sandbox and tool
    authorization and fixes the tool set when a run starts; ``guardrails``
    (enabled, with a provider) passes the role to every tool-call decision.
    With neither -- the compose profile has neither -- a run's role is carried
    and read by nothing, so a run started before a demotion can do nothing its
    owner's new role could not.
    """
    readers: list[str] = []
    if getattr(getattr(config, "authorization", None), "enabled", False):
        readers.append("authorization")
    guardrails = getattr(config, "guardrails", None)
    if getattr(guardrails, "enabled", False) and getattr(guardrails, "provider", None) is not None:
        readers.append("guardrails")
    return tuple(readers)


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
        "role_limit": user.role_limit,
    }


class AccountsCommand:
    """Every form over one users repository, a token store, a schedule store and the run store."""

    def __init__(
        self,
        users: SQLiteUserRepository,
        *,
        tokens: Any | None,
        schedules: Any | None,
        runs: Any | None = None,
        wait_seconds: float = DEFAULT_RUN_WAIT_SECONDS,
        role_readers: tuple[str, ...] = (),
        setup_opens_without_admin: bool = False,
        sweeps: Any | None = None,
        live_window_seconds: float | None = None,
    ) -> None:
        from app.gateway.refusal_watch import LIVE_WINDOW_SECONDS

        self._users = users
        self._tokens = tokens
        self._schedules = schedules
        self._runs = runs
        self._sweeps = sweeps
        self._deadline: float | None = None
        self._live_window = LIVE_WINDOW_SECONDS if live_window_seconds is None else live_window_seconds
        self._wait_seconds = wait_seconds
        self._role_readers = role_readers
        self._setup_opens_without_admin = setup_opens_without_admin

    async def run(
        self,
        command: str,
        *,
        issuer: str | None = None,
        subject: str | None = None,
        email: str | None = None,
        end_running_work: bool = False,
        role: str = LIMIT_ROLES[0],
    ) -> dict[str, Any]:
        # One bound for the whole command: the run wait, the second look for
        # late runs and the connection wait all end by it, so a caller can
        # size its runner's timeout from --wait-seconds alone.
        self._deadline = time.monotonic() + max(0.0, self._wait_seconds)
        if command == "list":
            return await self.list()
        if command in ("limit-role", "lift-role-limit"):
            # Started before anything is looked up, so ``elapsed_ms`` is the
            # whole command.
            clock = SurfaceClock()
            # The limit is the person's, so it reaches every account the
            # identity holds: two accounts for one subject are not a guess
            # to refuse here, they are both what is meant.
            identity, _ = await self._resolve(issuer=issuer, subject=subject, email=email, one_account=False)
            if command == "limit-role":
                return await self.limit_role(identity, role=role, end_running_work=end_running_work, clock=clock)
            return await self.lift_role_limit(identity, clock=clock)
        clock = SurfaceClock()
        identity, account = await self._resolve(issuer=issuer, subject=subject, email=email)
        if command == "disable":
            return await self.disable(identity, account, clock=clock)
        if command == "enable":
            return await self.enable(identity, account)
        if command == "end-sessions":
            return await self.end_sessions(identity, account, end_running_work=end_running_work)
        if command == "release-email":
            return await self.release_email(identity, account)
        raise CommandError(f"unknown command {command!r}; one of {', '.join(COMMANDS)}")

    # ── Selecting the account ────────────────────────────────────────────

    async def _resolve(self, *, issuer: str | None, subject: str | None, email: str | None, one_account: bool = True) -> tuple[tuple[str, str], User | None]:
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
            if len(candidates) > 1 and one_account:
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
        # An account linked before its issuer was recorded matches on its
        # subject alone (every read fails closed that way), so an identity it
        # would match is not one "without an account".
        issuerless = {entry["subject"] for entry in accounts if entry["subject"] and not entry["issuer"]}

        def has_account(issuer: str, subject: str) -> bool:
            return (issuer, subject) in with_account or subject in issuerless

        without = [{"issuer": issuer, "subject": subject, "disabled_at": _iso(disabled_at)} for issuer, subject, disabled_at in await self._users.list_disabled_identities() if not has_account(issuer, subject)]
        limits_without = [{"issuer": issuer, "subject": subject, "role": role, "limited_at": _iso(limited_at)} for issuer, subject, role, limited_at in await self._users.list_role_limits() if not has_account(issuer, subject)]
        return {"command": "list", "accounts": accounts, "disabled_without_account": without, "role_limits_without_account": limits_without}

    async def disable(self, identity: tuple[str, str], account: User | None, *, clock: SurfaceClock | None = None) -> dict[str, Any]:
        clock = clock or SurfaceClock()
        issuer, subject = identity
        recorded = await self._users.disable_identity(issuer, subject)
        committed = clock.now_ms()
        # Asked once the refusal has committed, so a Gateway process that acts
        # on this check reads the refusal too; asked before the slow part, so
        # the processes look while the runs unwind.
        check = await self._sweeps.request_check() if self._sweeps is not None else None
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
            "runs_found": 0,
            "runs_cancelled": 0,
            "runs_finished_first": [],
            "runs_unconfirmed": [],
            "returncode": 0,
        }
        # The refusal is keyed by (issuer, subject): it is the person at the
        # provider who is turned off, so it covers every account that identity
        # has here, and everything below reaches each of them. The document
        # names the ones besides the account addressed.
        siblings = [other for other in await self._users.list_users_by_identity(issuer, subject) if account is None or str(other.id) != str(account.id)]
        if siblings:
            document["identity_also_covers"] = [{"provider": other.oauth_provider, "email": other.email, "id": str(other.id)} for other in siblings]
        accounts = ([account] if account is not None else []) + siblings
        clock.stopped("sign_in", ACTION_REFUSED_AT_NEXT_USE, len(accounts), at_ms=committed)
        if account is None:
            # Nothing to end, and every surface named all the same, so a
            # caller reads one shape whatever the identity held.
            clock.stopped("sessions", ACTION_ENDED, 0, at_ms=committed)
            clock.stopped("personal_access_tokens", ACTION_REVOKED, 0, at_ms=committed)
            clock.stopped("internal_launches", ACTION_REFUSED_AT_NEXT_USE, 0, at_ms=committed)
            clock.stopped("running_work", ACTION_ENDED, 0, at_ms=committed, confirmed_by=CONFIRMED_BY_RUN_STATUS)
            await self._confirm_processes(check, check, [], clock, CONNECTION_SURFACES + RETAINED_SURFACES)
            document.update(clock.document())
            document["surfaces_not_reached"] = []
            if document["surfaces_unconfirmed"]:
                document["returncode"] = EXIT_UNCONFIRMED_RUNS
            document["note"] = "no account existed for this identity; the refusal is recorded and a sign-in that would create one is refused" + self._surfaces_note(clock)
            return document
        # The derived refusal already stops every session and token from the
        # commit; ending the sessions and revoking the tokens is what keeps
        # them dead after an enable.
        ended = [await self._users.end_sessions(str(covered.id)) for covered in accounts]
        document["sessions_ended"] = any(ended)
        document["tokens_revoked"] = sum([await self._revoke_tokens(covered) for covered in accounts])
        document["schedules_held"] = await self._count_schedules(account)
        clock.stopped("sessions", ACTION_ENDED, len(accounts), at_ms=committed)
        clock.stopped("personal_access_tokens", ACTION_REVOKED, document["tokens_revoked"], at_ms=committed)
        clock.stopped("internal_launches", ACTION_REFUSED_AT_NEXT_USE, sum([await self._count_schedules(covered) for covered in accounts]), at_ms=committed)
        # The connections are confirmed while the runs unwind, not after: their
        # stop times are the processes' own, not the run wait's.
        runs, _ = await asyncio.gather(self._end_running_work(accounts, relook=True), self._confirm_processes(check, check, accounts, clock, CONNECTION_SURFACES))
        document.update(runs)
        runs_ended_at = clock.now_ms()
        # A run that is ending parks its sandbox, and may leave a pooled
        # session or a queued memory update, after the first look: ask again
        # now that the runs are over, and count what either look ended.
        retained_check = await self._sweeps.request_check() if self._sweeps is not None else None
        await self._confirm_processes(retained_check, check, accounts, clock, RETAINED_SURFACES)
        sandboxes = clock.surfaces.get("sandboxes", {})
        if document["runs_unconfirmed"]:
            clock.not_stopped("running_work", ACTION_ENDED, document["runs_found"], unconfirmed=True, confirmed_by=CONFIRMED_BY_RUN_STATUS)
        elif sandboxes.get("stopped_after_ms") is not None:
            clock.stopped("running_work", ACTION_ENDED, document["runs_found"], at_ms=sandboxes["stopped_after_ms"], confirmed_by=CONFIRMED_BY_SANDBOX_GONE)
        else:
            clock.stopped("running_work", ACTION_ENDED, document["runs_found"], at_ms=runs_ended_at, confirmed_by=CONFIRMED_BY_RUN_STATUS)
        document["account"] = _account_document(await self._users.get_user_by_id(str(account.id)) or account)
        document.update(clock.document())
        document["surfaces_not_reached"] = sorted(name for name, entry in clock.surfaces.items() if entry.get("action") == ACTION_NOT_REACHED)
        if document["surfaces_unconfirmed"]:
            document["returncode"] = EXIT_UNCONFIRMED_RUNS
        document["note"] = self._run_note(
            document,
            done="sessions are refused at their next request; every run this identity had executing was cancelled and its stream ended with it; no new run starts",
            undone="sessions are refused at their next request and no new run starts, but the runs named in `runs_unconfirmed` had not reached a terminal status when the wait ran out; re-run this command to see whether they have since",
        )
        document["note"] += self._surfaces_note(clock)
        return document

    @staticmethod
    def _surfaces_note(clock: SurfaceClock) -> str:
        """What the unconfirmed process surfaces mean, saying only what the processes' record showed."""
        entries = {name: clock.surfaces[name] for name in clock.unconfirmed if name != "running_work"}
        note = ""
        if any(entry.get("processes_unconfirmed") for entry in entries.values()):
            note += (
                "; a Gateway process named under the surfaces in `surfaces_unconfirmed` had not recorded that it looked when the wait ran out, "
                "so what it holds for this identity is not confirmed ended; re-run this command to see whether it has since"
            )
        not_ended = [name for name, entry in entries.items() if entry.get("not_ended")]
        if not_ended:
            note += (
                f"; what {', '.join(not_ended)} counts under `not_ended` could not be confirmed ended -- it would not stop, or a Gateway keeps one whose owner it does not know, "
                "such as a sandbox it took over after a restart, which its idle timeout ends; re-run this command to see whether it has since"
            )
        if any(name in OUTLIVE_THEIR_PROCESS and entry.get("processes") == 0 and not entry.get("processes_unconfirmed") and entry.get("action") != ACTION_NOT_REACHED for name, entry in entries.items()):
            note += "; no Gateway process is running to confirm the sandboxes stopped, and a sandbox outlives its Gateway; re-run this command once the Gateway is up"
        not_reached = [name for name, entry in entries.items() if entry.get("action") == ACTION_NOT_REACHED]
        if not_reached:
            note += (
                f"; {', '.join(not_reached)} is `not_reached` (listed under `surfaces_not_reached`): the processes named under it have no way to end it in this deployment "
                "(a sandbox provider that cannot stop an owner's sandboxes), so what a run left running there is not confirmed ended, and re-running this command will not change that"
            )
        return note

    async def _confirm_processes(self, check: int | None, since_check: int | None, accounts: list[User], clock: SurfaceClock, surfaces: tuple[str, ...]) -> None:
        """Wait until every live Gateway process has acted on ``check``, then report what they ended of ``surfaces`` since ``since_check``.

        This process cannot see into a Gateway's memory; each Gateway looks
        for refused owners among what it holds and keeps, and records it. A
        process that stopped beating holds nothing any more and is not waited
        on; one that is beating but has not acted when the wait runs out is
        named, and the surfaces are unconfirmed rather than assumed ended. So
        is a surface a process tried to end and could not (``not_ended``),
        and one it has no way to end at all (``not_reached``).
        """
        if self._sweeps is None or check is None or since_check is None:
            return
        deadline = self._deadline if self._deadline is not None else time.monotonic() + max(0.0, self._wait_seconds)
        while True:
            live = await self._sweeps.live_processes(window_seconds=self._live_window)
            waiting = sorted(process.process_id for process in live if process.checked_through < check)
            if not waiting or time.monotonic() >= deadline:
                break
            await asyncio.sleep(SWEEP_WAIT_POLL_SECONDS)
        confirmed_at = clock.now_ms()
        counts = dict.fromkeys(surfaces, 0)
        not_ended = dict.fromkeys(surfaces, 0)
        # ``ANY_OWNER`` rows are what a process could not attribute -- a
        # subsystem that failed outright, or a sandbox it adopted without
        # learning whose -- and so may be this identity's.
        owners = [str(covered.id) for covered in accounts] + ([ANY_OWNER] if accounts else [])
        for ending in await self._sweeps.endings_for(owners, since_check=since_check):
            if ending.surface in counts:
                counts[ending.surface] += ending.count
                # Each look tries again what an earlier one could not end, so
                # what is still there is what the looks on ``check`` could not.
                if ending.check_id >= check:
                    not_ended[ending.surface] += ending.failed
        for surface in surfaces:
            # Nothing can be kept for an identity with no account, reachable or not.
            unreached = sorted(process.process_id for process in live if surface in process.unreached) if accounts else []
            # ``processes``: the live processes that confirmed; ``processes_unconfirmed``:
            # the live ones that had not when the wait ran out, whose count is not in ``count``.
            facts = {
                "confirmed_by": CONFIRMED_BY_GATEWAY_RECORD,
                "processes": len(live) - len(waiting),
                "processes_unconfirmed": waiting,
                "processes_unreached": unreached,
                "not_ended": not_ended[surface],
            }
            unvouched = surface in OUTLIVE_THEIR_PROCESS and not live and bool(accounts)
            if unreached:
                clock.not_stopped(surface, ACTION_NOT_REACHED, counts[surface], unconfirmed=True, **facts)
            elif waiting or not_ended[surface] or unvouched:
                clock.not_stopped(surface, ACTION_ENDED, counts[surface], unconfirmed=True, **facts)
            else:
                clock.stopped(surface, ACTION_ENDED, counts[surface], at_ms=confirmed_at, **facts)

    async def enable(self, identity: tuple[str, str], account: User | None) -> dict[str, Any]:
        issuer, subject = identity
        withdrawn = await self._users.enable_identity(issuer, subject)
        if self._sweeps is not None:
            # So each Gateway takes the person off its refused list now,
            # rather than cut what they open for up to its periodic look.
            await self._sweeps.request_check()
        refreshed = await self._users.get_user_by_id(str(account.id)) if account is not None else None
        return {
            "command": "enable",
            "identity": {"issuer": issuer, "subject": subject},
            "verdict": "enabled" if withdrawn else "already_enabled",
            "account": _account_document(refreshed) if refreshed is not None else None,
            "note": "the person may sign in again; sessions ended and tokens revoked by disable stay ended and revoked",
        }

    async def end_sessions(self, identity: tuple[str, str], account: User | None, *, end_running_work: bool = False) -> dict[str, Any]:
        """End an account's sessions, and its running work only when asked.

        The default leaves a run alone on purpose. This form is what a demoted
        administrator gets, and demoting someone is not removing them: their
        work is still theirs to finish, at the role the next sign-in reads.
        ``--end-running-work`` is for the deployer who means the other thing.
        """
        issuer, subject = identity
        if account is None:
            raise CommandError(f"no account exists for subject {subject!r} at issuer {issuer!r}; there are no sessions to end")
        await self._users.end_sessions(str(account.id))
        document: dict[str, Any] = {
            "command": "end-sessions",
            "identity": {"issuer": issuer, "subject": subject},
            "verdict": "sessions_ended",
            "account": _account_document(account),
            "sessions_ended": True,
            "tokens_revoked": 0,
            "note": "every open session is refused at its next request; personal access tokens are untouched; a run already executing keeps going (pass --end-running-work to cancel it); the next sign-in re-reads the claim",
        }
        if not end_running_work:
            # The keys are present either way, zeroed, so one script can read
            # this document without knowing which form produced it.
            document.update({"runs_found": 0, "runs_cancelled": 0, "runs_finished_first": [], "runs_unconfirmed": [], "returncode": 0})
            return document
        document.update(await self._end_running_work([account]))
        document["note"] = self._run_note(
            document,
            done="every open session is refused at its next request; every run this account had executing was cancelled; personal access tokens are untouched and the next sign-in re-reads the claim",
            undone="every open session is refused at its next request, but the runs named in `runs_unconfirmed` had not reached a terminal status when the wait ran out; re-run this command to see whether they have since",
        )
        return document

    async def limit_role(self, identity: tuple[str, str], *, role: str, end_running_work: bool = False, clock: SurfaceClock | None = None) -> dict[str, Any]:
        """Hold the identity at ``role`` on every account it has here, now and at every later sign-in."""
        clock = clock or SurfaceClock()
        if role not in LIMIT_ROLES:
            raise CommandError(f"a role limit holds a person below administrator; --role must be one of: {', '.join(LIMIT_ROLES)}")
        issuer, subject = identity
        if self._setup_opens_without_admin:
            # In local mode, a deployment with no administrator offers
            # first-boot setup to whoever reaches it first. Limiting the last
            # administrator would open that door to the internet.
            covered = await self._users.list_users_by_identity(issuer, subject)
            held_here = sum(account.system_role == "admin" for account in covered)
            if held_here and await self._users.count_admin_users() <= held_here:
                raise CommandError("this would leave the deployment with no administrator, and with local passwords on that reopens first-boot setup to whoever reaches it first; make someone else an administrator first")
        recorded, lowered, ended = await self._users.limit_role(issuer, subject, role)
        committed = clock.now_ms()
        # Read after the write: a first sign-in racing the command may have
        # created an account in between, and it is covered like any other.
        accounts = await self._users.list_users_by_identity(issuer, subject)
        changed = recorded or lowered > 0
        at_next_use = ACTION_LIMITED_AT_NEXT_USE if changed else ACTION_ALREADY_LIMITED
        clock.stopped("stored_role", ACTION_LOWERED if changed else ACTION_ALREADY_LIMITED, lowered, at_ms=committed)
        clock.stopped("sign_in", at_next_use, len(accounts), at_ms=committed)
        clock.stopped("personal_access_tokens", at_next_use, await self._count_live_tokens(accounts), at_ms=committed)
        clock.stopped("internal_launches", at_next_use, sum([await self._count_schedules(account) for account in accounts]), at_ms=committed)
        # Ended as ``end-sessions`` ends them, so a page that cached the old
        # role signs in again, and in the limit's own transaction, so a
        # command that dies after the commit leaves none open -- but only when
        # something changed: the deployer re-applies its record every pass,
        # and a person whose limit already held must not be signed out every
        # time it does.
        if changed:
            clock.stopped("sessions", ACTION_ENDED, ended, at_ms=committed)
        else:
            clock.stopped("sessions", ACTION_ALREADY_LIMITED, len(accounts), at_ms=committed)
        # Only work that still carries a role above the limit: whatever the
        # person starts afterwards runs at the limited role and is theirs to
        # finish, however often the deployer re-applies the limit.
        running = {"role_read_by": list(self._role_readers), "role_above": role}
        runs: dict[str, Any] = {"runs_found": 0, "runs_cancelled": 0, "runs_finished_first": [], "runs_unconfirmed": [], "returncode": 0}
        if end_running_work:
            runs = await self._end_running_work(accounts, above=role)
            if runs["runs_unconfirmed"]:
                clock.not_stopped("running_work", ACTION_ENDED, runs["runs_found"], unconfirmed=True, confirmed_by=CONFIRMED_BY_RUN_STATUS, **running)
            else:
                clock.stopped("running_work", ACTION_ENDED, runs["runs_found"], confirmed_by=CONFIRMED_BY_RUN_STATUS, **running)
        else:
            clock.not_stopped("running_work", ACTION_LEFT_ALONE, len(await self._active_runs(accounts, above=role)), unconfirmed=False, **running)
        document: dict[str, Any] = {
            "command": "limit-role",
            "identity": {"issuer": issuer, "subject": subject},
            "role": role,
            "verdict": "limited" if recorded else "already_limited",
            "changed": changed,
            "accounts": [_account_document(await self._users.get_user_by_id(str(account.id)) or account) for account in accounts],
            **runs,
            **clock.document(),
        }
        if document["surfaces_unconfirmed"]:
            document["returncode"] = EXIT_UNCONFIRMED_RUNS
        document["note"] = self._limit_note(document, end_running_work=end_running_work)
        return document

    def _limit_note(self, document: dict[str, Any], *, end_running_work: bool) -> str:
        if not document["accounts"]:
            return "no account exists for this identity yet; the limit is recorded and the account a sign-in creates holds this role"
        note = "the role reads the limit at every request; personal access tokens keep working at the limited role; a sign-in whose claim says more stores the limit"
        note += "; sessions that held the old role sign in again" if document["changed"] else "; nothing had changed since the limit was set, so no session was ended"
        if end_running_work:
            return self._run_note(
                document,
                done=note + "; every run these accounts had executing with a role above the limit was cancelled",
                undone=note + "; the runs named in `runs_unconfirmed` had not reached a terminal status when the wait ran out; re-run this command to see whether they have since",
            )
        readers = document["surfaces"]["running_work"]["role_read_by"]
        if readers:
            return note + f"; a run already executing keeps the role it started with, and this deployment reads a run's role ({', '.join(readers)}): pass --end-running-work to cancel it"
        return note + "; a run already executing keeps the role it started with, which nothing in this deployment reads (pass --end-running-work to cancel it anyway)"

    async def lift_role_limit(self, identity: tuple[str, str], *, clock: SurfaceClock | None = None) -> dict[str, Any]:
        """Withdraw the limit. Nothing changes until a sign-in reads the role again."""
        clock = clock or SurfaceClock()
        issuer, subject = identity
        lifted = await self._users.lift_role_limit(issuer, subject)
        committed = clock.now_ms()
        accounts = await self._users.list_users_by_identity(issuer, subject)
        if lifted:
            clock.stopped("role_limit", ACTION_LIFTED, len(accounts), at_ms=committed)
        return {
            "command": "lift-role-limit",
            "identity": {"issuer": issuer, "subject": subject},
            "verdict": "lifted" if lifted else "no_limit",
            "changed": lifted,
            "accounts": [_account_document(account) for account in accounts],
            "note": (
                "the stored role stays where the limit held it until a sign-in reads the role again: the next sign-in, where roles follow the claim; "
                "where they come from the administrators' list, only a sign-in that changes the account's address"
                if lifted
                else "no role limit was held for this identity"
            ),
            "returncode": 0,
            **clock.document(),
        }

    @staticmethod
    def _run_note(document: dict[str, Any], *, done: str, undone: str) -> str:
        """One sentence about the runs, saying only what the rows showed.

        Deliberately not a diagnosis: a run still unwinding a cancelled tool
        call and a Gateway that is not answering produce the same unconfirmed
        row from here, and telling an operator to restart a healthy Gateway
        would take every other person's work down with it.
        """
        note = undone if document["runs_unconfirmed"] else done
        if document["runs_finished_first"]:
            note += "; the runs named in `runs_finished_first` completed on their own before the cancellation reached them, so their results were delivered"
        return note

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

    async def _end_running_work(self, accounts: list[User], *, above: str | None = None, relook: bool = False) -> dict[str, Any]:
        """Cancel every run these accounts have executing, then wait for them to stop.

        The cancellation is the durable request a person's own cancel makes,
        so the owning worker applies it through its normal abort and terminal
        handling -- the same path, whichever worker owns the run. This process
        holds the database and nothing else; it cannot reach into a worker,
        and it must not pretend a write is a stopped run.

        It takes every account the refusal covers, not just the one the
        deployer named. The refusal is keyed by the identity, so a subject with
        an account under each of two configured providers is refused on both;
        leaving one of those accounts' runs writing files would be the same
        defect this exists to close, one account over.

        So it waits and then says what it saw. A run that did not reach a
        terminal status inside the bound is named, not rounded down: an
        operator told "cancelled" while a sandbox command is still writing
        files has been told the wrong thing. A run that reached ``success``
        before the cancellation landed is named too, under
        ``runs_finished_first`` -- it stopped, but it also delivered its result
        into a thread after the person was removed, which the operator should
        hear rather than read as one more run cancelled.

        ``above`` narrows it to runs whose sealed role is above that role, or
        unknown: what a role limit ends is work still carrying the role it
        took away, not work the person started since.

        A request that authenticated just before a refusal or a limit
        committed can still insert a run after the first look. Nothing
        admitted after the commit can -- a refused owner's run is refused as
        it starts, and a limited one runs at the limit -- so one more look
        once the wait is over finds what arrived meanwhile and nothing the
        person started since. A later pass finds anything slower. ``relook``
        asks for that look; ``above`` implies it.
        """
        result: dict[str, Any] = {
            "runs_found": 0,
            "runs_cancelled": 0,
            "runs_finished_first": [],
            "runs_unconfirmed": [],
            "returncode": 0,
        }
        if self._runs is None:
            return result
        owners = {str(row["run_id"]): user_id for user_id, row in await self._active_runs(accounts, above=above)}
        if not owners:
            return result
        terminal = await self._cancel_and_wait(owners)
        if above is not None or relook:
            late = {str(row["run_id"]): user_id for user_id, row in await self._active_runs(accounts, above=above) if str(row["run_id"]) not in owners}
            if late:
                terminal.update(await self._cancel_and_wait(late))
                owners.update(late)
        result["runs_found"] = len(owners)
        result["runs_finished_first"] = sorted(run_id for run_id, status in terminal.items() if status == "success")
        result["runs_cancelled"] = len(terminal) - len(result["runs_finished_first"])
        result["runs_unconfirmed"] = sorted(set(owners) - set(terminal))
        result["returncode"] = EXIT_UNCONFIRMED_RUNS if result["runs_unconfirmed"] else 0
        return result

    async def _cancel_and_wait(self, owners: dict[str, str]) -> dict[str, str]:
        """Ask for each run's cancellation, then wait; returns the status each run that stopped reached."""
        for run_id, user_id in owners.items():
            try:
                await self._runs.request_cancel_compat(run_id, action="interrupt", user_id=user_id)  # type: ignore[union-attr]
            except Exception as exc:  # noqa: BLE001 - one run that refuses the request must not hide the others
                logger.warning("Failed to request cancellation of run %s: %s", run_id, exc)
        return await self._wait_for_terminal(owners)

    async def _wait_for_terminal(self, owners: dict[str, str]) -> dict[str, str]:
        """Poll until every named run is terminal or the wait runs out.

        Returns the status each run that stopped reached, so the caller can
        tell a run it ended from one that finished on its own first.
        """
        terminal: dict[str, str] = {}
        pending = list(owners)
        deadline = self._deadline if self._deadline is not None else time.monotonic() + max(0.0, self._wait_seconds)
        while True:
            still_running: list[str] = []
            for run_id in pending:
                try:
                    # Scoped to the owner: this command only ever waits on a
                    # run it named from that account's own active rows.
                    row = await self._runs.get(run_id, user_id=owners[run_id])
                except Exception as exc:  # noqa: BLE001 - a read that fails is not a stopped run
                    logger.warning("Failed to read run %s while waiting for it to stop: %s", run_id, exc)
                    still_running.append(run_id)
                    continue
                status = None if row is None else row.get("status")
                # A row that is gone cannot still be executing; a row whose
                # status is terminal has stopped. Anything else is pending.
                if row is None:
                    terminal[run_id] = "absent"
                elif status in TERMINAL_RUN_STATUSES:
                    terminal[run_id] = str(status)
                else:
                    still_running.append(run_id)
            pending = still_running
            if not pending or time.monotonic() >= deadline:
                return terminal
            await asyncio.sleep(min(RUN_WAIT_POLL_SECONDS, max(0.0, deadline - time.monotonic())))

    async def _revoke_tokens(self, account: User) -> int:
        if self._tokens is None:
            return 0
        revoked = 0
        for record in await self._tokens.list_for_user(str(account.id)):
            if record.get("revoked_at") is None and await self._tokens.revoke(str(record["id"]), str(account.id)):
                revoked += 1
        return revoked

    async def _count_live_tokens(self, accounts: list[User]) -> int:
        if self._tokens is None:
            return 0
        return sum([sum(1 for record in await self._tokens.list_for_user(str(account.id)) if record.get("revoked_at") is None) for account in accounts])

    async def _active_runs(self, accounts: list[User], *, above: str | None = None) -> list[tuple[str, dict[str, Any]]]:
        """Each non-terminal run these accounts own, with its owner; with ``above``, only those sealed above that role."""
        if self._runs is None:
            return []
        found: list[tuple[str, dict[str, Any]]] = []
        for account in accounts:
            for row in await self._runs.list_active_by_user(str(account.id)):
                if row.get("run_id") and (above is None or _sealed_role_above(row, above)):
                    found.append((str(account.id), row))
        return found

    async def _count_schedules(self, account: User) -> int:
        if self._schedules is None:
            return 0
        tasks = await self._schedules.list_by_user(str(account.id))
        return sum(1 for task in tasks if task.get("status") in ACTIVE_SCHEDULE_STATUSES)


# ── Running it inside the deployment ────────────────────────────────────


def deployment_options(config: Any) -> dict[str, Any]:
    """What the command reads from the deployment it runs in: what reads a run's role, and whether first-boot setup opens without an administrator."""
    from app.gateway.auth.mode import sign_on_only

    return {"role_readers": run_role_readers(config), "setup_opens_without_admin": not sign_on_only()}


async def _run(
    command: str,
    *,
    issuer: str | None,
    subject: str | None,
    email: str | None,
    end_running_work: bool = False,
    wait_seconds: float = DEFAULT_RUN_WAIT_SECONDS,
    role: str = LIMIT_ROLES[0],
) -> dict[str, Any]:
    from deerflow.config import get_app_config
    from deerflow.persistence.engine import close_engine, get_session_factory, init_engine_from_config
    from deerflow.persistence.personal_access_tokens import PersonalAccessTokenRepository
    from deerflow.persistence.refusal_sweeps import RefusalSweepRepository
    from deerflow.persistence.run import RunRepository
    from deerflow.persistence.scheduled_tasks import ScheduledTaskRepository
    from deerflow.runtime.tenant_identity import TenantIdentityV1

    config = get_app_config()
    if config.database.backend == "memory":
        raise CommandError("the memory database backend keeps no accounts between processes; this command needs config.database on sqlite or postgres")
    # The same tenant the Gateway resolves at construction: the token store
    # and the run store both filter rows by it, so a run this command cancels
    # is one this deployment owns.
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
            runs=RunRepository(session_factory, tenant=tenant),
            sweeps=RefusalSweepRepository(session_factory),
            wait_seconds=wait_seconds,
            **deployment_options(config),
        )
        return await command_runner.run(command, issuer=issuer, subject=subject, email=email, end_running_work=end_running_work, role=role)
    finally:
        await close_engine()


class _Refusal(Exception):
    """A command line this command will not run; the document says why."""


class _ArgumentParser(argparse.ArgumentParser):
    """Answers a malformed command line with the one document and exit 1, like every other refusal.

    argparse's own answer is usage text on stderr and exit 2, which a caller
    that reads 2 as "done, but a run is unconfirmed" would misread.
    """

    def error(self, message: str) -> None:  # type: ignore[override]
        raise _Refusal(message)


#: The options that take a value: the word after one is never the command.
_VALUED_OPTIONS = ("--issuer", "--subject", "--email", "--role", "--wait-seconds")


def _command_word(argv: list[str]) -> str | None:
    """The command a refused command line named, for its refusal document: never a flag's value."""
    words = iter(argv)
    for word in words:
        if word in _VALUED_OPTIONS:
            next(words, None)
        elif word in COMMANDS:
            return word
    return None


def main(argv: list[str] | None = None) -> int:
    parser = _ArgumentParser(prog="python -m app.gateway.auth.accounts", description="Turn one account off or on, end its sessions, limit its role, release its address, or list every account. Not a network route.")
    parser.add_argument("command", choices=COMMANDS)
    parser.add_argument("--issuer", help="the identity provider's issuer URL, as configured")
    parser.add_argument("--subject", help="the person's subject at that issuer")
    parser.add_argument("--email", help="convenience: the account's email, when it resolves to exactly one provider account")
    parser.add_argument(
        "--end-running-work",
        action="store_true",
        help="end-sessions and limit-role only: also cancel the runs the account has executing (disable always does)",
    )
    parser.add_argument("--role", default=None, help=f"limit-role only: the highest role the identity may hold, one of {', '.join(LIMIT_ROLES)} (default {LIMIT_ROLES[0]})")
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=DEFAULT_RUN_WAIT_SECONDS,
        help=(
            f"the one bound on the command's waits -- for cancelled runs to reach a terminal status, and for every Gateway process to record closing "
            f"the account's connections -- before it reports them unconfirmed (default {DEFAULT_RUN_WAIT_SECONDS:g}; a Gateway looks about once a second)"
        ),
    )
    try:
        args = parser.parse_args(argv)
    except _Refusal as exc:
        command = _command_word(argv if argv is not None else sys.argv[1:])
        print(json.dumps({"command": command, "error": str(exc)}, sort_keys=True), flush=True)
        return 1
    if args.end_running_work and args.command not in ENDS_WORK_ON_REQUEST:
        print(json.dumps({"command": args.command, "error": "--end-running-work belongs to end-sessions and limit-role; disable always ends the account's running work"}, sort_keys=True), flush=True)
        return 1
    if args.role is not None and args.command != "limit-role":
        print(json.dumps({"command": args.command, "error": "--role belongs to limit-role"}, sort_keys=True), flush=True)
        return 1
    if args.wait_seconds < 0:
        print(json.dumps({"command": args.command, "error": "--wait-seconds cannot be negative"}, sort_keys=True), flush=True)
        return 1
    try:
        document = asyncio.run(
            _run(
                args.command,
                issuer=args.issuer,
                subject=args.subject,
                email=args.email,
                end_running_work=args.end_running_work,
                wait_seconds=args.wait_seconds,
                role=args.role or LIMIT_ROLES[0],
            )
        )
    except CommandError as exc:
        print(json.dumps({"command": args.command, "error": str(exc)}, sort_keys=True), flush=True)
        return 1
    except Exception as exc:  # noqa: BLE001 - the document is the answer, whatever failed
        print(json.dumps({"command": args.command, "error": f"{type(exc).__name__}: {exc}"}, sort_keys=True), flush=True)
        return 1
    print(json.dumps(document, sort_keys=True), flush=True)
    # A run this command could not confirm stopped is a failure the exit
    # status has to carry, not a detail buried in the document -- but its own
    # status, because the refusal *was* recorded and a runbook must not read
    # this as "the command did nothing".
    return int(document.get("returncode", 0))


if __name__ == "__main__":
    sys.exit(main())
