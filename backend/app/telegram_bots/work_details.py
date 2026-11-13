"""Current source facts for complete Work details; no publication or execution."""

from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attention import AttentionReader
from app.telegram_bots.attention_authority import current_attention, prepare_attention
from app.telegram_bots.authority import AccessOff, AuthorityUnavailable, _PreparedLiveOwner
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_start_retention import closed_start
from deerflow.agent_instances.contract import AgentPermission, DefinitionSnapshot
from deerflow.agent_instances.conversations import AgentConversations
from deerflow.agent_instances.human_input import HumanInput
from deerflow.agent_instances.work import _date
from deerflow.config.agents_config import AgentConfig
from deerflow.persistence.agent_instances.human_input import HumanInputRequestRow
from deerflow.persistence.agent_instances.model import AgentConversationRow, AgentDefinitionRevisionRow, AgentInstanceGrantRow, AgentInstanceRow, AgentLifecycleRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow
from deerflow.persistence.thread_meta.model import ThreadMetaRow
from deerflow.persistence.user.access import DisabledIdentityRow
from deerflow.persistence.user.model import UserRow
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained

_WORK = ("id", "instance_id", "creator_id", "objective", "success_criteria", "revision", "assignment_revision", "definition_revision", "status", "progress", "due_at", "created_at", "updated_at")
_ATTEMPT = ("id", "status", "assignment_revision", "instance_generation", "thread_id", "run_id", "activation_id", "requester_id")
_REQUEST = ("id", "work_id", "instance_id", "purpose", "recipient_id", "state", "revision", "request_revision", "assignment_revision", "basis_id", "basis_revision", "question")


def _fields(value, names):
    if value is None:
        return None
    result = {name: value.get(name) if type(value) is dict else getattr(value, name) for name in names}
    return {name: _date(item) if isinstance(item, datetime) else deepcopy(item) for name, item in result.items()}


@dataclass(frozen=True)
class WorkDetailRead:
    record: dict
    request: dict | None
    person: str | None
    recipient: str | None
    permissions: int
    held: tuple[str, ...]
    protected: bool
    lifecycle_pending: bool
    can_reuse_conversation: bool
    signature: str


class WorkDetailsReader(AttentionReader):
    async def detail(self, link, work_id, *, ignore_activation=None):
        async def perform():
            basis = await self._current(link)
            actor = PrincipalRef("human", link.owner_user_id)
            record = await self.work.get(actor=actor, instance_id=link.agent_ref, work_id=work_id)
            request = await HumanInput(self.work).get(actor=actor, request_id=record["human_input_request_id"], can_write="agents:write" in basis[0]) if record["human_input_request_id"] else None
            thread_id = (record.get("attempt") or {}).get("thread_id")
            reusable = await AgentConversations(self.work.agents).allowed(actor=actor, thread_id=thread_id, permission=AgentPermission.USE | AgentPermission.INSPECT) if thread_id else False
            projection = WorkDetailProjection(self, link, record, request, reusable=reusable, ignore_activation=ignore_activation)
            await current_attention(self, link, projection=projection)
            return projection.source

        return await await_drained(perform())

    async def prepare_detail(self, link, source, *, ignore_activation=None):
        """Prepare off-writer, then qualify this exact detail in a borrowed read."""
        admission = await prepare_attention(self, link)
        projection = WorkDetailProjection(self, link, source.record, source.request, reusable=source.can_reuse_conversation, expected=source, ignore_activation=ignore_activation)

        async def check(*, session=None):
            return await admission(session=session, projection=projection)

        return check


class WorkDetailProjection:
    """Assignment, recipient, lifecycle and conversation facts after provider waits."""

    def __init__(self, reader, link, record, request, *, reusable=False, expected=None, ignore_activation=None):
        from app.gateway.deps import bind_user_repo
        from deerflow.config.app_config import peek_loaded_app_config

        self.reader, self.link, self.ignore_activation = reader, link, ignore_activation
        self.record, self.request, self.reusable = deepcopy(record), deepcopy(request), reusable
        self.config = peek_loaded_app_config()
        if self.config is None:
            raise AuthorityUnavailable()
        users = self.users = bind_user_repo(reader.links.session_factory)
        self.recipient_owner = _PreparedLiveOwner(request["recipient_id"] if request else None, "", self.config.auth.local.enabled is False, users, reader.links.session_factory.kw["bind"].sync_engine)
        self.source, self.expected = None, deepcopy(expected)

    def extend(self, query, *, work_ids=None):
        self.column = len(query.column_descriptions)
        work, attempt, latest, request = aliased(AgentWorkRow), aliased(AgentWorkAttemptRow), aliased(AgentWorkAttemptRow), aliased(HumanInputRequestRow)
        definition, creator, person, grant, disabled, conversation, thread = (
            aliased(AgentDefinitionRevisionRow),
            aliased(UserRow),
            aliased(UserRow),
            aliased(AgentInstanceGrantRow),
            aliased(DisabledIdentityRow),
            aliased(AgentConversationRow),
            aliased(ThreadMetaRow),
        )
        current_attempt = select(latest.id).where(latest.work_id == work.id).order_by(latest.created_at.desc(), latest.id.desc()).limit(1).correlate(work).scalar_subquery()
        custodian, custodian_disabled = aliased(UserRow), aliased(DisabledIdentityRow)
        action, binding = aliased(TelegramActRow), aliased(TelegramBotLinkRow)
        waiting = (
            select(action.id)
            .join(binding, binding.id == action.link_id)
            .where(
                action.work_id == work.id,
                action.instance_id == work.instance_id,
                action.owner_user_id == self.link.owner_user_id,
                action.kind == "activate",
                action.state.in_(("pending", "waiting", "uncertain")),
                action.id != self.ignore_activation if self.ignore_activation is not None else True,
                binding.binding_generation == action.binding_generation,
                binding.owner_user_id == action.owner_user_id,
            )
            .order_by(action.created_at, action.id)
            .limit(1)
            .correlate(work)
            .scalar_subquery()
        )
        marker, journal, marker_binding, receipt_attempt = aliased(TelegramCallbackRow), aliased(TelegramActRow), aliased(TelegramBotLinkRow), aliased(AgentWorkAttemptRow)
        pending = select(AgentLifecycleRow.operation_id).where(AgentLifecycleRow.instance_id == AgentInstanceRow.id, AgentLifecycleRow.phase == "pending").correlate(AgentInstanceRow).exists()
        return (
            query.add_columns(
                work, attempt, request, definition, creator.email, person, grant.permissions, disabled, conversation, thread.thread_id, pending, custodian, custodian_disabled, waiting, marker, journal, marker_binding, receipt_attempt
            )
            .join(work, and_(work.id.in_(work_ids) if work_ids is not None else work.id == self.record["id"], work.instance_id == TelegramBotLinkRow.agent_ref))
            .outerjoin(attempt, attempt.id == current_attempt)
            .outerjoin(request, and_(request.work_id == work.id, request.instance_id == work.instance_id, request.state != "closed"))
            .join(definition, definition.revision == AgentInstanceRow.definition_revision)
            .outerjoin(creator, creator.id == work.creator_id)
            .outerjoin(person, person.id == request.recipient_id)
            .outerjoin(grant, and_(grant.instance_id == work.instance_id, grant.user_id == person.id))
            .outerjoin(disabled, or_(disabled.subject == person.id, disabled.subject == person.oauth_id))
            .outerjoin(conversation, and_(conversation.thread_id == attempt.thread_id, conversation.instance_id == work.instance_id))
            .outerjoin(thread, thread.thread_id == conversation.thread_id)
            .outerjoin(custodian, custodian.id == AgentInstanceRow.owner_id)
            .outerjoin(custodian_disabled, or_(custodian_disabled.subject == custodian.id, custodian_disabled.subject == custodian.oauth_id))
            .outerjoin(marker_binding, and_(marker_binding.owner_user_id == self.link.owner_user_id, marker_binding.agent_ref == work.instance_id))
            .outerjoin(
                marker,
                and_(
                    marker.link_id == marker_binding.id,
                    marker.binding_generation == marker_binding.binding_generation,
                    marker.chat_id == marker_binding.chat_id,
                    marker.kind == "work_start",
                    marker.target_kind == "work",
                    marker.target_id == work.id,
                    marker.payload["work_action_id"].as_string().is_not(None),
                ),
            )
            .outerjoin(journal, journal.id == marker.payload["work_action_id"].as_string())
            .outerjoin(receipt_attempt, and_(receipt_attempt.work_id == work.id, receipt_attempt.activation_id == journal.id))
        )

    def check_with_permissions(self, rows, held):
        from deerflow.config.app_config import peek_loaded_app_config

        if peek_loaded_app_config() is not self.config:
            raise AuthorityUnavailable()
        instance, permissions, policy = rows[0][1:4]
        work, attempt, request, definition, creator, person, recipient_grant, _disabled, conversation, thread_id, pending = rows[0][self.column : self.column + 11]
        if _fields(work, _WORK) != _fields(self.record, _WORK) or _fields(attempt, _ATTEMPT) != _fields(self.record.get("attempt"), _ATTEMPT) or _fields(request, _REQUEST) != _fields(self.request, _REQUEST):
            raise AuthorityUnavailable()
        recipient_live = False
        if person is not None:
            try:
                self.recipient_owner.check_snapshot(person, [row[self.column + 7] for row in rows])
                recipient_live = True
            except AccessOff:
                pass
        captured_definition = DefinitionSnapshot.capture(owner_id=definition.owner_id, config=definition.config, soul=definition.soul)
        if captured_definition.revision != instance.definition_revision:
            raise AuthorityUnavailable()
        policy_config = AgentConfig.model_validate(definition.config).work_policy
        pending_activation = rows[0][self.column + 13]
        for row in rows:
            marker, journal, binding, original_attempt = row[self.column + 14 : self.column + 18]
            if marker is not None and journal is not None and marker.id == journal.id == self.ignore_activation:
                continue
            if marker is not None and not closed_start(marker, journal, binding, original_attempt):
                pending_activation = pending_activation or marker.id
        record = _fields(work, _WORK)
        record.update(
            attempt=_fields(attempt, _ATTEMPT),
            work_enabled=bool(policy_config and policy_config.enabled),
            execution_available=self.record["execution_available"],
            needs_mandate_reconciliation=work.definition_revision != instance.definition_revision,
            activation_pending=pending_activation,
        )
        current_request = None
        if request is not None and HumanInput._source_current(request, work):
            required = 3 if request.purpose == "information" else 6
            eligible = recipient_live and type(recipient_grant) is int and 1 <= recipient_grant <= 7 and recipient_grant & required == required
            current_request = {
                **_fields(request, _REQUEST),
                "can_respond": "agents:write" in held and permissions & 3 == 3,
                "can_manage": "agents:write" in held and permissions & 6 == 6,
                "needs_routing": not eligible,
            }
        custodian_live = instance.custody == "company"
        if instance.custody == "personal":
            custodian = rows[0][self.column + 11]
            owner = _PreparedLiveOwner(instance.owner_id, "", self.config.auth.local.enabled is False, self.users, self.reader.links.session_factory.kw["bind"].sync_engine)
            try:
                owner.check_snapshot(custodian, [row[self.column + 12] for row in rows])
                custodian_live = True
            except AccessOff:
                pass
        can_reuse = conversation is not None and thread_id is not None and custodian_live
        if can_reuse != self.reusable:
            raise AuthorityUnavailable()
        source = WorkDetailRead(record, current_request, creator, person.email if person else None, permissions, tuple(sorted(held)), instance.custody != "personal" and policy == "protected", pending, can_reuse, "")
        self.source = WorkDetailRead(**{**asdict(source), "signature": digest(asdict(source))})
        if self.expected is not None and not same_body(asdict(self.source), asdict(self.expected)):
            raise AuthorityUnavailable()
