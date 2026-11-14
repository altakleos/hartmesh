"""Private assignment cards join their exact draft and input at the SDK fence."""

from dataclasses import asdict
from datetime import datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import and_, false
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.assignment_drafts import DraftProjection, snapshot_draft
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body, thaw
from app.telegram_bots.work_details import WorkDetailProjection
from app.telegram_bots.work_news_sources import WorkNewsRead
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_sources import WorkViewReader
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def private_facts(row):
    result = {key: thaw(getattr(row, key)) for key in ("id", *_ACT_SCOPE, "body", "state", "reason", "operation_ids", "attempts", "result", "continuation", "stop_dispatch")}
    result.update({key: aware(getattr(row, key)).isoformat() if getattr(row, key) is not None else None for key in ("created_at", "updated_at", "first_attempt_at", "cancel_requested_at")})
    return result


def private_snapshot(value):
    return SimpleNamespace(**{**value, **{key: datetime.fromisoformat(value[key]) if value[key] is not None else None for key in ("created_at", "updated_at", "first_attempt_at", "cancel_requested_at")}})


def assignment_intent(item):
    intent = item.payload.get("assignment_intent")
    if type(intent) is not dict or set(intent) != {"version", "draft_id"} or type(intent["version"]) is not int or intent["version"] != 1 or type(intent["draft_id"]) is not str or len(intent["draft_id"]) != 32:
        raise ProductSourceRetired()
    return intent


def private_source(link, root, *, instance, permissions, policy, held, clock, config, journal=None, detail=None, transcripts=()):
    draft = snapshot_draft(link, root)
    if root.cancel_requested_at is not None:
        raise ProductSourceRetired()
    if root.state == "draft" and not root.operation_ids and root.continuation is not None:
        raise ProductSourceRetired()
    phase = "draft"
    if draft.state == "refused":
        phase = {"discarded": "discarded", "replaced": "replaced", "answered-here": "answering", "draft-expired": "expired"}.get(root.reason)
        if phase is None:
            raise ProductSourceRetired()
    expires = draft.created_at + timedelta(hours=config.callbacks.draft_ttl_hours)
    if phase == "draft" and clock.now() >= expires:
        phase = "expired"
    due = draft.draft.due_at.astimezone(clock.zone).strftime("%Y-%m-%d %H:%M") if draft.draft.due_at is not None else None
    data = {
        "root": private_facts(root),
        "draft_id": root.id,
        "phase": phase,
        "draft": draft.draft.model_dump(mode="json"),
        "source": draft.source,
        "due_note": draft.due_note,
        "expires_at": expires.isoformat(),
        "timezone": clock.settings.timezone,
        "can_save": "agents:write" in held,
        "can_start": {"agents:write", "runs:create", "threads:write"}.issubset(held),
    }
    values = {"name": instance.name, "due": due}
    if phase == "draft" and transcripts:
        values["heard"] = list(transcripts)
    if root.operation_ids:
        from app.telegram_bots.assignment_journals import action_facts, event_facts, journal_state

        if journal is None:
            raise ProductSourceRetired()
        parent, child, original_event, child_event = journal
        phase, notice, receipt = journal_state(link, root, parent, child, original_event, child_event, now=clock.now(), config=config)
        data.update(phase=phase, notice=notice, act=action_facts(parent), child=action_facts(child), original_event=event_facts(original_event), child_event=event_facts(child_event))
        if child is not None:
            data["due_note"] = True
            data["draft"] = {"objective": child.body["objective"], "success_criteria": child.body["success_criteria"], "due_at": None, "due_text": parent.continuation["delegate_source"]["due_text"]}
            values["due"] = None
        if phase == "saved":
            if detail is None or detail.record["id"] != receipt["id"]:
                raise ProductSourceRetired()

            def date(value):
                return datetime.fromisoformat(value).astimezone(clock.zone).strftime("%Y-%m-%d %H:%M") if value else None

            data.update(detail=asdict(detail), detail_dates={"date": date(detail.record["created_at"]), "due": date(detail.record["due_at"])})
    data = thaw(data)
    return WorkNewsRead(None, "assignment.card", values, instance.custody != "personal" and policy == "protected", data, digest([link.id, link.binding_generation, root.id, root.body["origin"]]), digest([data, values, permissions]))


class AssignmentReader(WorkViewReader):
    def __init__(self, acts):
        super().__init__(work=acts.work, authority=acts.authority, links=acts.links, clock=acts.outbox.clock)
        self.acts, self.config = acts, acts.outbox.config

    async def read(self, link, item):
        async def perform():
            intent = assignment_intent(item)
            async with self.links.session_factory() as reader:
                root = await reader.get(TelegramActRow, intent["draft_id"])
                snapshot_draft(link, root)
                captured = _snapshot(root)
                journal = None
                detail_id = None
                if captured.operation_ids:
                    from sqlalchemy import select

                    from app.telegram_bots.assignment_journals import journal_state

                    parent = await reader.get(TelegramActRow, captured.operation_ids[0]) if len(captured.operation_ids) == 1 else None
                    if parent is None:
                        raise ProductSourceRetired()
                    from app.telegram_bots.assignment_lineage import read_family

                    await read_family(reader, captured.id, parent)
                    from app.telegram_bots.work_delegate_actions import delegation_children

                    children = delegation_children(parent)
                    child = await reader.get(TelegramActRow, children[0]) if len(children) == 1 else None
                    original_event = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.instance_id == link.agent_ref, AgentWorkEventRow.operation_id == parent.id))
                    child_event = await reader.scalar(select(AgentWorkEventRow).where(AgentWorkEventRow.instance_id == link.agent_ref, AgentWorkEventRow.operation_id == child.id)) if child else None
                    journal = (_snapshot(parent), _snapshot(child) if child else None, original_event, child_event)
                    _phase, _notice, receipt = journal_state(link, captured, *journal, now=self.clock(), config=self.config)
                    detail_id = receipt["id"] if receipt is not None else None
                else:
                    from app.telegram_bots.assignment_lineage import require_fresh_draft

                    await require_fresh_draft(reader, link, captured.id)
            clock = await self.prepare_clock(link, item)
            detail = await self.detail(link, detail_id) if detail_id is not None else None
            projection = AssignmentProjection(self, link, item, root=captured, journal=journal, detail=detail, clock=clock)
            current = await prepare_attention(self, link)
            await current(projection=projection)
            return projection.source

        return await await_drained(perform())


class AssignmentProjection:
    def delivery_key(self, root):
        from app.telegram_bots.assignment_proposal_records import assignment_key

        return assignment_key(root, self.item.payload)

    def __init__(self, reader, link, item, source=None, *, root=None, journal=None, detail=None, clock):
        self.reader, self.link, self.item, self.expected, self.clock = reader, link, item, source, clock
        if source is not None:
            root = private_snapshot(source.work["root"])
            if source.work.get("act") is not None:
                from app.telegram_bots.assignment_journals import journal_rows

                parent, child = journal_rows(source.work)
                journal = (parent, child, None, None)
            if source.work.get("detail") is not None:
                from app.telegram_bots.work_view_sources import detail_read

                detail = detail_read(source.work["detail"])
        if root is None:
            raise ProductSourceRetired()
        snapshot_draft(link, root)
        self.root = root
        self.journal, self.detail = journal, detail
        self.consent = None
        self.family = None
        self.fresh = None
        if journal is not None:
            from app.telegram_bots.assignment_consent import DelegationConsent
            from app.telegram_bots.assignment_lineage import DelegateFamily

            self.consent = DelegationConsent(link, journal[0])
            self.family = DelegateFamily(root.id, journal[0].id)
        else:
            from app.telegram_bots.assignment_lineage import FreshDraft

            self.fresh = FreshDraft(root.id, item.id)
        self.detail_projection = WorkDetailProjection(reader, link, detail.record, detail.request, reusable=detail.can_reuse_conversation, expected=detail) if detail is not None else None
        self.projection = DraftProjection(link, root.body["origin"], saved=root)
        from app.telegram_bots.assignment_proposal_records import ProposalInputs, proposal_primary

        self.proposal = ProposalInputs(link, item) if proposal_primary(item.payload) else None
        self.source = None

    def extend(self, query):
        result = self.projection.extend(query)
        if self.proposal is not None:
            result = self.proposal.extend(result)
        if self.detail_projection is not None:
            result = self.detail_projection.extend(result)
        self.journal_column = len(result.column_descriptions)
        parent, child, event, child_event = aliased(TelegramActRow), aliased(TelegramActRow), aliased(AgentWorkEventRow), aliased(AgentWorkEventRow)
        parent_id = self.journal[0].id if self.journal else None
        child_id = self.journal[1].id if self.journal and self.journal[1] is not None else None
        result = (
            result.add_columns(parent, child, event, child_event)
            .outerjoin(parent, parent.id == parent_id if parent_id else false())
            .outerjoin(child, child.id == child_id if child_id else false())
            .outerjoin(event, and_(event.instance_id == self.link.agent_ref, event.operation_id == parent_id) if parent_id else false())
            .outerjoin(child_event, and_(child_event.instance_id == self.link.agent_ref, child_event.operation_id == child_id) if child_id else false())
        )
        if self.consent is not None:
            result = self.family.extend(result)
            result = self.consent.extend(result)
        self.delivery_column = len(result.column_descriptions)
        delivery, settings = TelegramOutboxRow, aliased(TelegramPersonSettingsRow)
        result = result.add_columns(delivery, settings).join(delivery, delivery.id == self.item.id).join(settings, settings.owner_user_id == self.link.owner_user_id)
        return self.fresh.extend(result) if self.fresh is not None else result

    def check(self, rows):
        raise ProductSourceRetired()

    def check_with_permissions(self, rows, held):
        self.projection.check_with_permissions(rows, held)
        if self.proposal is not None:
            self.proposal.check(rows)
        detail = None
        if self.detail_projection is not None:
            self.detail_projection.check_with_permissions(rows, held)
            detail = self.detail_projection.source
        root = rows[0][self.projection.column + 2]
        if not same_body(private_facts(root), private_facts(self.root)):
            raise ProductSourceRetired()
        delivery, settings = rows[0][self.delivery_column : self.delivery_column + 2]
        if self.fresh is not None:
            self.fresh.check(rows, delivery)
        intent = assignment_intent(self.item)
        if (
            _item(delivery) != self.item
            or intent["draft_id"] != root.id
            or delivery.dedupe_key != self.delivery_key(root)
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.class_, delivery.work_id, delivery.request_id, delivery.event_id)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, "reply", None, None, None)
            or delivery.message_thread_id != root.body["origin"]["message_thread_id"]
            or settings.timezone != self.clock.settings.timezone
        ):
            raise ProductSourceRetired()
        journal = None
        if self.journal is not None:
            from app.telegram_bots.assignment_journals import action_facts, event_facts

            parent, child, event, child_event = rows[0][self.journal_column : self.journal_column + 4]
            if parent is None or not same_body(action_facts(parent), action_facts(self.journal[0])) or not same_body(action_facts(child), action_facts(self.journal[1])):
                raise ProductSourceRetired()
            if self.expected is not None and (not same_body(event_facts(event), self.expected.work["original_event"]) or not same_body(event_facts(child_event), self.expected.work["child_event"])):
                raise ProductSourceRetired()
            journal = (_snapshot(parent), _snapshot(child) if child else None, event, child_event)
            self.family.check(rows, parent)
            self.consent.check(rows, root, journal[0], journal[1])
        from app.telegram_bots.assignment_drafts import draft_input_basis
        from app.telegram_bots.assignment_voice_inputs import voice_basis

        originals = [rows[0][self.projection.column + 1]]
        if self.projection.criteria_origin is not None:
            originals.append(rows[0][self.projection.criteria_column])
        transcripts = []
        for mark in originals:
            if voice_basis(mark) is not None:
                draft_input_basis(mark)
                transcripts.append(mark.sanitized_input["text"])
        self.source = private_source(self.link, root, instance=rows[0][1], permissions=rows[0][2], policy=rows[0][3], held=held, clock=self.clock, config=self.reader.config, journal=journal, detail=detail, transcripts=transcripts)
        if self.expected is not None and not same_body(self.source.__dict__, self.expected.__dict__):
            raise ProductSourceRetired()
