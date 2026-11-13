"""Durable one-shot drafting ownership, with explicit fallback after interruption."""

import asyncio
import logging
from dataclasses import dataclass
from datetime import timedelta
from uuid import uuid4

from pydantic import TypeAdapter, ValidationError
from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assign import DraftAssignment, draft_assignment
from app.telegram_bots.assignment_drafts import AssignmentDrafts, DraftProjection, draft_identity, draft_scope, origin_facts
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow, _basis
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _safe_json
from app.telegram_bots.settings import SettingsRepository
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_result_identity import digest
from deerflow.agent_instances.work_contract import Statement
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow
from deerflow.runtime.keyed_lock import AsyncKeyedLockTable
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DraftRequest:
    id: str
    phase: str
    words: str
    previous: DraftAssignment | None
    source: str
    claim_id: str | None
    created_at: object
    signature: str


def request_snapshot(link, row):
    try:
        body = thaw(row.body)
        if (
            any(getattr(row, key) != value for key, value in draft_scope(link).items())
            or row.state != "draft"
            or row.attempts != 0
            or row.first_attempt_at is not None
            or row.operation_ids
            or row.cancel_requested_at is not None
            or row.continuation is not None
            or row.stop_dispatch is not None
            or set(body) != {"version", "request", "source", "origin"}
            or type(body["version"]) is not int
            or body["version"] != 2
            or body["source"] not in {"command", "edit"}
        ):
            raise ValueError()
        request = body["request"]
        if (
            type(request) is not dict
            or set(request) != {"phase", "words", "previous", "claim_id"}
            or request["phase"] not in {"new", "extracting", "awaiting_done", "failed"}
            or type(request["words"]) is not str
            or not request["words"].strip()
            or request["phase"] == "new"
            and request["claim_id"] is not None
            or request["phase"] != "new"
            and (type(request["claim_id"]) is not str or len(request["claim_id"]) != 32 or any(c not in "0123456789abcdef" for c in request["claim_id"]))
        ):
            raise ValueError()
        previous = DraftAssignment.model_validate(request["previous"]) if request["previous"] is not None else None
        return DraftRequest(row.id, request["phase"], request["words"], previous, body["source"], request["claim_id"], aware(row.created_at), digest([row.id, draft_scope(link), body]))
    except (ValueError, KeyError, TypeError, AttributeError):
        raise ProductSourceRetired() from None


def fallback_phase(value):
    if value.previous is not None:
        return "failed"
    try:
        TypeAdapter(Statement).validate_python(value.words)
    except ValidationError:
        return "failed"
    return "awaiting_done"


class AssignmentDraftRequests:
    def __init__(self, acts):
        self.drafts = AssignmentDrafts(acts)
        self.acts, self.outbox, self.config = acts, acts.outbox, acts.outbox.config
        self.settings = SettingsRepository(self.outbox.sessions, self.config)
        self._locks, self._active, self._tasks = AsyncKeyedLockTable(), set(), set()
        self._stopping = False

    def _current(self):
        if self._stopping:
            raise ProductSourceRetired()

    def _age(self, source):
        if self.outbox.clock() >= source.created_at + timedelta(hours=self.config.callbacks.draft_ttl_hours):
            raise CallbackRefused("stale.draft")

    async def stop(self):
        self._stopping = True
        if self._tasks:
            await await_drained(asyncio.gather(*tuple(self._tasks), return_exceptions=True))

    async def request(self, link, inbox, *, words, previous=None, source="command", admission=None):
        self._current()
        if type(words) is not str or not words.strip():
            raise ValueError("assign-words-required")
        if len(words) > self.config.assign.max_chars:
            raise ValueError("assign-too-long")
        if previous is not None and type(previous) is not DraftAssignment or source not in {"command", "edit"}:
            raise ValueError("invalid-draft-request")
        task = asyncio.create_task(self._request(link, inbox, words=words, previous=previous, source=source, admission=admission))
        self._tasks.add(task)
        try:
            return await await_drained(task)
        finally:
            self._tasks.discard(task)

    async def _request(self, link, inbox, *, words, previous, source, admission):
        identity = draft_identity(link, inbox)
        async with self._locks.hold(identity):
            self._current()
            body = _safe_json({"version": 2, "request": {"phase": "new", "words": words, "previous": previous.model_dump(mode="json") if previous else None, "claim_id": None}, "source": source, "origin": origin_facts(inbox)})
            current = await prepare_attention(self.drafts, link)
            facts = DraftProjection(link, body["origin"], inbox=inbox)
            await current(projection=facts)
            async with self.outbox._writer(link.id) as (writer, _binding):
                if admission is not None:
                    await admission(writer)
                await current(session=writer, projection=facts)
                row = await writer.get(TelegramActRow, identity, with_for_update=True, populate_existing=True)
                if row is None:
                    now = self.outbox.clock()
                    row = TelegramActRow(id=identity, **draft_scope(link), body=body, state="draft", operation_ids=[], attempts=0, created_at=now, updated_at=now)
                    writer.add(row)
                    await writer.flush()
                    await self.outbox.inbox.route(inbox, kind="assignment_draft", target_id=identity, basis={"draft_id": identity}, operation_ids=[], session=writer)
                elif row.body.get("version") != 1:
                    value = request_snapshot(link, row)
                    if value.words != words or not same_body(row.body["request"]["previous"], body["request"]["previous"]) or value.source != source:
                        raise ValueError("draft-source-conflict")
                complete = row.body.get("version") == 1
                if admission is not None:
                    await admission(writer)
            if complete:
                return await self.drafts.get(link, identity)
            value = await self._read(link, identity)
            if value.phase != "new":
                return value
            claim = uuid4().hex
            value = await self._phase(link, identity, expected=value, phase="extracting", claim_id=claim, admission=admission)
            if value.phase != "extracting" or value.claim_id != claim:
                return value
            self._active.add(identity)
            try:

                async def before_dispatch():
                    self._current()
                    live = await self._read(link, identity)
                    if live.phase != "extracting" or live.claim_id != claim:
                        raise ProductSourceRetired()
                    if admission is not None:
                        async with self.outbox._writer(link.id) as (writer, _binding):
                            await admission(writer)

                settings = await self.settings.get(link.owner_user_id)
                if settings is None:
                    raise ProductSourceRetired()
                proposal = await draft_assignment(value.words, agent_name=facts.name, settings=settings, config=self.config, previous=value.previous, now=self.outbox.clock(), before_dispatch=before_dispatch)
                self._current()
                if proposal is None:
                    return await self._phase(link, identity, expected=value, phase=fallback_phase(value), claim_id=claim, admission=admission)
                return await self._complete(link, identity, expected=value, draft=proposal, source=source, admission=admission)
            except ProductSourceRetired:
                # A current recovery may have selected the manual fallback.
                # Its owned phase wins over a late provider result.
                return await self._value(link, identity)
            except ValidationError:
                return await self._phase(link, identity, expected=value, phase="failed", claim_id=claim, admission=admission)
            finally:
                self._active.discard(identity)

    async def _value(self, link, identity):
        async with self.outbox.sessions() as reader:
            row = await reader.get(TelegramActRow, identity)
            complete = row is not None and row.body.get("version") == 1
        return await self.drafts.get(link, identity) if complete else await self._read(link, identity)

    async def _read(self, link, identity):
        self._current()
        async with self.outbox.sessions() as reader:
            row = await reader.get(TelegramActRow, identity)
            value = request_snapshot(link, row)
            saved = _snapshot(row)
        self._age(value)
        current = await prepare_attention(self.drafts, link)
        await current(projection=RequestProjection(link, saved))
        self._age(value)
        return value

    async def _phase(self, link, identity, *, expected, phase, claim_id, admission=None):
        self._current()
        async with self.outbox.sessions() as reader:
            row = await reader.get(TelegramActRow, identity)
            value = request_snapshot(link, row)
            saved = _snapshot(row)
        if value.signature != expected.signature:
            return await self._read(link, identity)
        current = await prepare_attention(self.drafts, link)
        before = RequestProjection(link, saved)
        async with self.outbox._writer(link.id) as (writer, _binding):
            self._current()
            if admission is not None:
                await admission(writer)
            await current(session=writer, projection=before)
            row = await writer.get(TelegramActRow, identity, with_for_update=True, populate_existing=True)
            if request_snapshot(link, row).signature != expected.signature:
                raise ProductSourceRetired()
            self._age(expected)
            row.body = {**thaw(row.body), "request": {**thaw(row.body["request"]), "phase": phase, "claim_id": claim_id}}
            row.updated_at = self.outbox.clock()
            await writer.flush()
            await current(session=writer, projection=RequestProjection(link, _snapshot(row)))
            if admission is not None:
                await admission(writer)
            self._current()
            result = request_snapshot(link, row)
            self._age(result)
            return result

    async def _complete(self, link, identity, *, expected, draft, source, criteria=None, admission=None):
        async with self.outbox.sessions() as reader:
            row = await reader.get(TelegramActRow, identity)
            if row is not None and row.body.get("version") == 1:
                complete = True
            else:
                complete = False
        if complete:
            return await self.drafts.get(link, identity)
        async with self.outbox.sessions() as reader:
            row = await reader.get(TelegramActRow, identity)
            value = request_snapshot(link, row)
            origin = row.body["origin"]
            mark = await reader.get(TelegramInboundMarkRow, (origin["link_id"], origin["message_id"], origin["chat_id"]))
            inbox = InboxRow.from_row(mark) if mark else None
            body = thaw(row.body)
            saved = _snapshot(row)
        if value.signature != expected.signature:
            return await self._read(link, identity)
        if inbox is None or inbox.route_kind != "assignment_draft" or inbox.route_target_id != identity:
            raise ProductSourceRetired()
        own, install = await self.drafts.prepare_create(link, inbox, draft=draft, source=source, criteria=criteria)
        if own != identity:
            raise ProductSourceRetired()
        current = await prepare_attention(self.drafts, link)
        async with self.outbox._writer(link.id) as (writer, _binding):
            self._current()
            await current(session=writer, projection=RequestProjection(link, saved))
            if admission is not None:
                await admission(writer)
            if criteria is not None:
                await self.outbox.inbox.route(criteria, kind="assignment_criteria", target_id=identity, basis={"draft_id": identity}, operation_ids=[], session=writer)
            result = await install(writer, expected_request=body)
            row = await writer.get(TelegramActRow, identity, populate_existing=True)
            completed = _snapshot(row)
            if admission is not None:
                await admission(writer)
            await current(session=writer, projection=RequestProjection(link, completed, request_phase=value.phase))
            self._current()
            self.drafts._age(result)
            return result

    async def criteria(self, link, identity, inbox, *, text, admission=None):
        if type(text) is not str or not text.strip():
            raise ValueError("assign-criteria-required")
        value = await self._read(link, identity)
        if value.phase != "awaiting_done":
            raise ProductSourceRetired()
        result = DraftAssignment(objective=value.words, success_criteria=text)
        return await self._complete(link, identity, expected=value, draft=result, source="fallback", criteria=inbox, admission=admission)

    async def recover(self, link):
        self._current()
        async with self.outbox.sessions() as reader:
            rows = list(
                await reader.scalars(
                    select(TelegramActRow)
                    .where(
                        TelegramActRow.link_id == link.id, TelegramActRow.binding_generation == link.binding_generation, TelegramActRow.kind == "delegate", TelegramActRow.state == "draft", TelegramActRow.body["version"].as_integer() == 2
                    )
                    .order_by(TelegramActRow.created_at, TelegramActRow.id)
                )
            )
            identities = [row.id for row in rows]
        recovered = []
        for identity in identities:
            if identity in self._active:
                continue
            try:
                value = await self._read(link, identity)
                if value.phase == "extracting":
                    value = await self._phase(link, identity, expected=value, phase=fallback_phase(value), claim_id=value.claim_id)
                    recovered.append(value)
            except Exception as exc:
                logger.debug("Draft recovery deferred (%s)", type(exc).__name__)
                continue
        return tuple(recovered)


class RequestProjection(DraftProjection):
    def __init__(self, link, saved, *, request_phase=None):
        super().__init__(link, saved.body["origin"], saved=saved)
        self.request_phase = request_phase or saved.body["request"]["phase"]

    def check_with_permissions(self, rows, held):
        super().check_with_permissions(rows, held)
        mark = rows[0][self.column + 1]
        if (
            mark.route_kind != "assignment_draft"
            or mark.route_target_id != self.saved.id
            or mark.state == "interrupted"
            or not same_body(_basis(mark), {"draft_id": self.saved.id})
            or self.request_phase in {"new", "extracting"}
            and mark.route_complete
        ):
            raise ProductSourceRetired()
        if self.criteria_origin is not None:
            criterion = rows[0][self.criteria_column]
            if criterion.state == "interrupted" or criterion.route_kind != "assignment_criteria" or criterion.route_target_id != self.saved.id or not same_body(_basis(criterion), {"draft_id": self.saved.id}):
                raise ProductSourceRetired()
