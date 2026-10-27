"""Opaque, one-startup authority for a fixed undispatched restart notice.

The UUID is correlation only. Actual lifetime, cohort membership, original
adapter and current SQL custody authorize the one atomic closure writer.
"""

from __future__ import annotations

import asyncio
import json
import math
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING
from uuid import uuid4

from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from app.telegram_bots.inbox import PublicationRecord
    from app.telegram_bots.links import BotLink, PublicationGuard

_ISSUER = object()
_LINK_FIELDS = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
_SCOPE_FIELDS = ("owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation")


def _same_link(left, right) -> bool:
    return all(type(getattr(left, name, None)) is type(getattr(right, name)) and getattr(left, name, None) == getattr(right, name) for name in _LINK_FIELDS)


def _identity(record):
    from app.telegram_bots.inbox import publication_identity

    return json.dumps(publication_identity(record), sort_keys=True, separators=(",", ":"), allow_nan=False)


class RestartRecoveryTicket:
    """Only an issuing epoch can create an accepted ticket."""

    def __init__(self, key=None, *, epoch=None, record=None, link=None, channel=None):
        if key is not _ISSUER:
            raise ValueError("restart-recovery-unavailable") from None
        self._epoch, self._record = epoch, record
        self._link, self._channel = deepcopy(link), channel
        self._retired = False

    @property
    def startup_id(self) -> str:
        return self._epoch._startup_id


class StartupRecoveryEpoch:
    """One fixed cohort captured before held/create/send admission opens."""

    def __init__(self, key=None, *, host=None, records=(), deadline=None, phase=None):
        if key is not _ISSUER:
            raise ValueError("restart-recovery-unavailable") from None
        self._host, self._phase, self._client = host, phase, host.thread_client
        self._endpoint_id = self._client.endpoint_id
        self._records, self._deadline = tuple(records), deadline
        self._loop = asyncio.get_running_loop()
        self._clock = self._loop.time
        self._startup_id = uuid4().hex
        self._active = True
        self._members = {(record.turn_id, record.publication_id): _identity(record) for record in records}
        self._tickets: dict[tuple[str, str], RestartRecoveryTicket] = {}

    @property
    def records(self) -> tuple[PublicationRecord, ...]:
        return self._records

    @property
    def deadline(self) -> float:
        return self._deadline

    def retire(self) -> None:
        self._active = False

    def check_current(self) -> None:
        """Check the original startup lifetime before a new service leaf."""
        self._check()

    def _check(self) -> None:
        host = self._host
        if not (
            self._active
            and asyncio.get_running_loop() is self._loop
            and self._clock() < self._deadline
            and host._recovery_epoch is self
            and host._recovery_phase is self._phase
            and host._copy_decision is False
            and host._cohort_complete is True
            and host._ready is True
            and host._send_ready is False
            and host._stopping is False
            and host._stopped is False
            and host._cause is None
            and host.thread_client is self._client
            and self._client.endpoint_id == self._endpoint_id
        ):
            self.retire()
            raise ValueError("restart-recovery-unavailable") from None

    async def capture(self, record: PublicationRecord, current_link: BotLink) -> RestartRecoveryAttempt:
        from app.telegram_bots.links import BotLink

        self._check()
        try:
            key = (record.turn_id, record.publication_id)
            if self._members.get(key) != _identity(record) or type(current_link) is not BotLink:
                raise ValueError
            scope = record.scope
            if (
                current_link.id != scope.link_id
                or any(type(getattr(current_link, name)) is not type(getattr(scope, name)) or getattr(current_link, name) != getattr(scope, name) for name in _SCOPE_FIELDS)
                or scope.endpoint_id != self._endpoint_id
            ):
                raise ValueError
            ticket = self._tickets.get(key)
            if ticket is None:
                channel = self._host.channel(current_link.id)
                ticket = RestartRecoveryTicket(_ISSUER, epoch=self, record=record, link=current_link, channel=channel)
                self._tickets[key] = ticket
            check_restart_ticket(ticket, record)
            guard = _FixedGuard(ticket)
            await guard.prepare()
            return RestartRecoveryAttempt(ticket, guard)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ValueError("restart-recovery-unavailable") from None

    async def prepare_receipt_read(self, record: PublicationRecord) -> RestartReceiptReadAttempt:
        """Fresh metadata-read custody, without renewing the closure ticket."""
        try:
            ticket = self._tickets.get((record.turn_id, record.publication_id))

            def current() -> bool:
                _check_original(ticket, record, writing=False)
                return True

            current()
            owner = await await_drained(self._host.authority.prepare_live_owner(ticket._link.owner_user_id))
            current()
            actual = await await_drained(self._host.links.get(ticket._link.id))
            current()
            if actual is None or not _same_link(actual, ticket._link) or actual.state != "connected" or actual.health != "ok":
                raise ValueError
            target = await await_drained(self._host.authority.target(actual))
            current()
            if target != "ok":
                raise ValueError
            return RestartReceiptReadAttempt(deepcopy(ticket._link), owner, current)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise ValueError("restart-recovery-unavailable") from None


def check_restart_ticket(ticket: RestartRecoveryTicket, record: PublicationRecord) -> None:
    """Validate original host authority; marker/run remain SQL observations."""
    _check_original(ticket, record, writing=True)


def _check_original(ticket, record, *, writing: bool) -> None:
    try:
        if type(ticket) is not RestartRecoveryTicket or writing and ticket._retired:
            raise ValueError
        epoch = ticket._epoch
        if type(epoch) is not StartupRecoveryEpoch:
            raise ValueError
        epoch._check()
        key = (record.turn_id, record.publication_id)
        channel, link = ticket._channel, ticket._link
        if not (
            epoch._tickets.get(key) is ticket
            and epoch._members.get(key) == _identity(record) == _identity(ticket._record)
            and epoch._host.channel(link.id) is channel
            and channel is not None
            and _same_link(channel.link, link)
            and type(channel.transport_revision) is int
            and channel.transport_revision == link.token_revision
            and channel.application.running is True
            and channel.link.state == "connected"
            and channel.link.health == "ok"
        ):
            ticket._retired = True
            raise ValueError
    except Exception:
        raise ValueError("restart-recovery-unavailable") from None


@dataclass(frozen=True)
class RestartRecoveryAttempt:
    ticket: RestartRecoveryTicket
    fixed_guard: PublicationGuard


@dataclass(frozen=True)
class RestartReceiptReadAttempt:
    captured_link: BotLink
    check_live_owner: Callable
    check_current: Callable[[], bool]


class _FixedGuard:
    def __init__(self, ticket: RestartRecoveryTicket):
        self._ticket, self._borrowed = ticket, None

    @property
    def captured_link(self) -> BotLink:
        return deepcopy(self._ticket._link)

    def _current(self) -> bool:
        check_restart_ticket(self._ticket, self._ticket._record)
        return True

    async def prepare(self) -> None:
        ticket, epoch = self._ticket, self._ticket._epoch
        try:
            self._current()
            owner = await await_drained(epoch._host.authority.prepare_live_owner(ticket._link.owner_user_id))
            self._current()
            actual = await await_drained(epoch._host.links.update_if_current(ticket._link.id, token_revision=ticket._link.token_revision))
            self._current()
            if actual is None or not _same_link(actual, ticket._link) or actual.state != "connected" or actual.health != "ok":
                raise ValueError
            target = await await_drained(epoch._host.authority.target(actual))
            self._current()
            if target != "ok":
                raise ValueError
            self._borrowed = epoch._host.links.publication_guard(ticket._link, check_live_owner=owner, check_attempt_current=self._current)
        except BaseException:
            ticket._retired = True
            self._borrowed = None
            raise

    async def check(self, writer):
        self._current()
        if self._borrowed is None:
            raise ValueError("restart-recovery-unavailable") from None
        return await self._borrowed.check(writer)


def _issue_startup_epoch(host, records, *, deadline: float, phase) -> StartupRecoveryEpoch:
    """Called only by the actual Supervisor's retained pre-release startup."""
    from app.telegram_bots.inbox import PublicationRecord
    from app.telegram_bots.supervisor import TelegramSupervisor

    if not (
        type(host) is TelegramSupervisor
        and host._copy_task is asyncio.current_task()
        and phase is not None
        and host._recovery_phase is phase
        and host._recovery_epoch is None
        and host._copy_decision is False
        and host._cohort_complete is True
        and host._ready is True
        and host._send_ready is False
        and not host._stopping
        and not host._stopped
        and host._cause is None
        and type(deadline) in {int, float}
        and math.isfinite(deadline)
        and asyncio.get_running_loop().time() < deadline
        and deadline <= asyncio.get_running_loop().time() + 30
        and type(records) is tuple
        and len(records) <= 32
        and all(type(record) is PublicationRecord and not record.dispatch_started and record.run_id is None for record in records)
        and len({(record.turn_id, record.publication_id) for record in records}) == len(records)
    ):
        raise ValueError("restart-recovery-unavailable") from None
    epoch = StartupRecoveryEpoch(_ISSUER, host=host, records=records, deadline=deadline, phase=phase)
    host._recovery_epoch = epoch
    epoch._check()
    return epoch
