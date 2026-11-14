"""Private, source-qualified assignment drafts; no Work or execution is created."""

from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assign import DraftAssignment, prepare_due_date
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.authority import AuthorityUnavailable
from app.telegram_bots.callbacks import CallbackRefused
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _safe_json
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body, thaw
from app.telegram_bots.work_result_identity import digest
from deerflow.agent_instances.contract import AgentDenied, DefinitionSnapshot
from deerflow.config.agents_config import AgentConfig
from deerflow.persistence.agent_instances.model import AgentDefinitionRevisionRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class AssignmentDraft:
    id: str
    state: str
    draft: DraftAssignment
    original: DraftAssignment
    source: str
    due_note: bool
    created_at: object
    signature: str


def origin_facts(inbox):
    return {**{key: getattr(inbox, key) for key in ("link_id", "chat_id", "message_id", "update_id", "binding_generation", "message_thread_id")}, "input_signature": digest(thaw(inbox.sanitized_input))}


def draft_scope(link):
    return dict(
        link_id=link.id,
        owner_user_id=link.owner_user_id,
        bot_id=link.bot_id,
        binding_generation=link.binding_generation,
        telegram_user_id=link.telegram_user_id,
        chat_id=link.chat_id,
        instance_id=link.agent_ref,
        kind="delegate",
        work_id=None,
        request_id=None,
        message_id=None,
    )


def draft_identity(link, inbox):
    """One private draft/request identity belongs to this actual input."""
    return digest([draft_scope(link), origin_facts(inbox), "assignment"])[:32]


def prepare_draft(draft, *, permissions, now):
    shown, note = prepare_due_date(draft, permissions=permissions, now=now)
    if note and draft.due_text:
        # Keep the exact deadline words; exceeding the statement contract needs
        # another draft rather than silently truncating the person's criteria.
        shown = DraftAssignment.model_validate({**shown.model_dump(mode="json"), "success_criteria": shown.success_criteria + "\n\n" + draft.due_text})
    return shown, note


def snapshot_draft(link, row):
    try:
        if row is None or any(getattr(row, key) != value for key, value in draft_scope(link).items()):
            raise ValueError()
        body = thaw(row.body)
        if (
            type(body) is not dict
            or set(body) not in ({"version", "draft", "original", "source", "due_note", "origin"}, {"version", "draft", "original", "source", "due_note", "origin", "criteria_origin"})
            or type(body["version"]) is not int
            or body["version"] != 1
            or body["source"] not in {"command", "tool", "edit", "fallback"}
            or type(body["due_note"]) is not bool
        ):
            raise ValueError()
        draft, original = DraftAssignment.model_validate(body["draft"]), DraftAssignment.model_validate(body["original"])
        if (
            row.state not in {"draft", "refused"}
            or row.attempts != 0
            or row.first_attempt_at is not None
            or row.stop_dispatch is not None
            or row.cancel_requested_at is not None
            or row.state == "draft"
            and not row.operation_ids
            and row.continuation is not None
        ):
            raise ValueError()
        return AssignmentDraft(row.id, row.state, draft, original, body["source"], body["due_note"], aware(row.created_at), digest([row.id, draft_scope(link), body, row.state, row.operation_ids]))
    except (ValueError, TypeError, KeyError, AttributeError):
        raise ProductSourceRetired() from None


class AssignmentDrafts:
    def __init__(self, acts):
        self.acts, self.outbox = acts, acts.outbox
        self.work, self.authority, self.links = acts.work, acts.authority, acts.links
        self.config = acts.outbox.config
        if acts.links is None or acts.work is None or acts.session_factory.kw.get("bind") is not self.outbox.sessions.kw.get("bind") or acts.links.session_factory.kw.get("bind") is not self.outbox.sessions.kw.get("bind"):
            raise ValueError("assignment-drafts-require-shared-storage")

    def _age(self, source):
        if source.state != "draft" or self.outbox.clock() >= source.created_at + timedelta(hours=self.config.callbacks.draft_ttl_hours):
            raise CallbackRefused("stale.draft")

    async def prepare_create(self, link, inbox, *, draft, source, criteria=None, projection=None):
        """Prepare providers off-writer; install in an owning or borrowed writer."""
        if type(draft) is not DraftAssignment or source not in {"command", "tool", "edit", "fallback"} or type(inbox) is not InboxRow:
            raise ValueError("invalid-assignment-draft")
        origin = origin_facts(inbox)
        identity = draft_identity(link, inbox)
        admission = await prepare_attention(self, link)
        if criteria is not None and (
            source != "fallback" or type(criteria) is not InboxRow or (criteria.link_id, criteria.chat_id, criteria.binding_generation, criteria.message_thread_id) != (link.id, link.chat_id, link.binding_generation, inbox.message_thread_id)
        ):
            raise ProductSourceRetired()
        if projection is not None and (not isinstance(projection, DraftProjection) or projection.link != link or not same_body(projection.origin, origin) or projection.inbox != inbox or projection.saved is not None or criteria is not None):
            raise ValueError("invalid-assignment-projection")
        facts = projection if projection is not None else DraftProjection(link, origin, inbox=inbox, criteria=criteria)
        await admission(projection=facts)
        shown, note = prepare_draft(draft, permissions=facts.permissions, now=self.outbox.clock())
        body = _safe_json({"version": 1, "draft": shown.model_dump(mode="json"), "original": draft.model_dump(mode="json"), "source": source, "due_note": note, "origin": origin})
        if criteria is not None:
            body = {**body, "criteria_origin": origin_facts(criteria)}
        owner = await self.authority.prepare_live_owner(link.owner_user_id)

        async def install(writer, *, expected_request=None):
            await admission(session=writer, projection=facts)
            row = await writer.get(TelegramActRow, identity, with_for_update=True, populate_existing=True)
            if row is None:
                if expected_request is not None:
                    raise ProductSourceRetired()
                now = self.outbox.clock()
                row = TelegramActRow(id=identity, **draft_scope(link), body=body, state="draft", operation_ids=[], attempts=0, created_at=now, updated_at=now)
                writer.add(row)
                await writer.flush()
            elif expected_request is not None:
                if (
                    row.state != "draft"
                    or row.attempts != 0
                    or row.first_attempt_at is not None
                    or row.operation_ids
                    or any(getattr(row, key) != value for key, value in draft_scope(link).items())
                    or not same_body(row.body, expected_request)
                ):
                    raise ProductSourceRetired()
                row.body, row.updated_at = body, self.outbox.clock()
                await writer.flush()
            elif any(not same_body(row.body.get(key), body[key]) for key in ("original", "source", "origin")):
                raise ValueError("draft-source-conflict")
            result = snapshot_draft(link, row)
            self._age(result)
            await admission(session=writer, projection=facts)
            await owner(link.owner_user_id, writer)
            await admission(session=writer, projection=facts)
            self._age(result)
            return result

        return identity, install

    async def create(self, link, inbox, *, draft, source):
        async def perform():
            _identity, install = await self.prepare_create(link, inbox, draft=draft, source=source)
            async with self.outbox._writer(link.id) as (writer, _binding):
                return await install(writer)

        return await await_drained(perform())

    async def get(self, link, identity):
        async def perform():
            async with self.outbox.sessions() as reader:
                row = await reader.get(TelegramActRow, identity)
                source = snapshot_draft(link, row)
                origin = row.body["origin"]
                expected = _snapshot(row)
            self._age(source)
            admission = await prepare_attention(self, link)
            await admission(projection=DraftProjection(link, origin, saved=expected))
            self._age(source)
            return source

        return await await_drained(perform())


class DraftProjection:
    """Canonical adopted policy and exact original input after provider waits."""

    def __init__(self, link, origin, *, inbox=None, saved=None, criteria=None, require_work_policy=True):
        self.link, self.origin, self.inbox, self.saved = link, origin, inbox, saved
        self.require_work_policy = require_work_policy
        self.permissions = None
        self.name = None
        self.criteria_origin = origin_facts(criteria) if criteria is not None else saved.body.get("criteria_origin") if saved is not None else None

    def extend(self, query):
        from deerflow.persistence.agent_instances.model import AgentInstanceRow

        self.column = len(query.column_descriptions)
        definition, mark, draft = aliased(AgentDefinitionRevisionRow), aliased(TelegramInboundMarkRow), aliased(TelegramActRow)
        query = (
            query.add_columns(definition, mark)
            .join(definition, definition.revision == AgentInstanceRow.definition_revision)
            .join(mark, and_(mark.link_id == self.origin["link_id"], mark.chat_id == self.origin["chat_id"], mark.message_id == self.origin["message_id"]))
        )
        if self.saved is not None:
            query = query.add_columns(draft).join(draft, draft.id == self.saved.id)
        if self.criteria_origin is not None:
            criterion = aliased(TelegramInboundMarkRow)
            self.criteria_column = len(query.column_descriptions)
            query = query.add_columns(criterion).join(criterion, and_(criterion.link_id == self.criteria_origin["link_id"], criterion.chat_id == self.criteria_origin["chat_id"], criterion.message_id == self.criteria_origin["message_id"]))
        return query

    def check_with_permissions(self, rows, held):
        instance, permissions = rows[0][1:3]
        self.name = instance.name
        definition, mark = rows[0][self.column : self.column + 2]
        current = InboxRow.from_row(mark)
        if (
            not same_body(origin_facts(current), self.origin)
            or self.inbox is not None
            and not same_body(current.sanitized_input, self.inbox.sanitized_input)
            or (current.link_id, current.chat_id, current.binding_generation) != (self.link.id, self.link.chat_id, self.link.binding_generation)
        ):
            raise AuthorityUnavailable()
        snapshot = DefinitionSnapshot.capture(owner_id=definition.owner_id, config=definition.config, soul=definition.soul)
        policy = AgentConfig.model_validate(definition.config).work_policy
        if snapshot.revision != instance.definition_revision:
            raise AuthorityUnavailable()
        if self.require_work_policy and (policy is None or not policy.enabled):
            raise AgentDenied("Assignments are unavailable", reason="work-off")
        if self.permissions is not None and self.permissions != permissions:
            raise AuthorityUnavailable()
        self.permissions = permissions
        if self.criteria_origin is not None:
            criterion = InboxRow.from_row(rows[0][self.criteria_column])
            if not same_body(origin_facts(criterion), self.criteria_origin) or (criterion.link_id, criterion.chat_id, criterion.binding_generation, criterion.message_thread_id) != (
                self.link.id,
                self.link.chat_id,
                self.link.binding_generation,
                current.message_thread_id,
            ):
                raise AuthorityUnavailable()
        if self.saved is not None:
            row = rows[0][self.column + 2]
            if (
                any(getattr(row, key) != getattr(self.saved, key) for key in _ACT_SCOPE)
                or not same_body(row.body, self.saved.body)
                or row.state != self.saved.state
                or row.attempts != self.saved.attempts
                or row.first_attempt_at != self.saved.first_attempt_at
                or row.cancel_requested_at != self.saved.cancel_requested_at
                or not same_body(row.operation_ids, self.saved.operation_ids)
                or not same_body(row.continuation, self.saved.continuation)
                or not same_body(row.stop_dispatch, self.saved.stop_dispatch)
            ):
                raise ProductSourceRetired()
