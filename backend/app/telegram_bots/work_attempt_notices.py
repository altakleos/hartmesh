"""Terminal Work notices bind settlement, activation and exact run metadata."""

from sqlalchemy import exists, select
from sqlalchemy.orm import aliased

from app.telegram_bots.attempt_text import FILES_REASONS
from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.result_notice_sources import fields
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_event_source import event_signature
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.agent_instances.model import AgentInstanceRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.run.model import RunRow
from deerflow.persistence.user.access import issuer_key
from deerflow.persistence.user.model import UserRow


class AttemptNotice:
    def __init__(self, link, item):
        self.link, self.item = link, item

    def extend(self, query, activation):
        self.column = len(query.column_descriptions)
        event, attempt = AgentWorkEventRow, AgentWorkAttemptRow
        cause, supervisor = aliased(event), aliased(UserRow)
        intervened = exists(select(cause.id).where(cause.work_id == event.work_id, cause.revision > activation.revision, cause.revision < event.revision, cause.action.in_(("stop", "edit", "cancel"))).correlate(event, activation))
        return query.add_columns(RunRow, intervened, supervisor.email).outerjoin(RunRow, RunRow.run_id == attempt.run_id).outerjoin(supervisor, supervisor.id == AgentInstanceRow.supervisor_id)

    def qualify(self, rows, event, work, instance, attempt, activation, intent):
        run, intervened, supervisor = rows[0][self.column : self.column + 3]
        key = intent["key"]
        contained = key == "interrupted"
        expected_status = {"interrupted": "failed", "attempt.failed": "failed", "attempt.no_result": "succeeded", "attempt.stopped_restart": "cancelled"}[key]
        if (
            attempt is None
            or activation is None
            or (attempt.work_id, attempt.activation_id, attempt.requester_id, attempt.status) != (work.id, activation.operation_id, self.link.owner_user_id, expected_status)
            or attempt.settled_at is None
            or (activation.work_id, activation.instance_id, activation.action) != (work.id, instance.id, "activate")
            or contained
            and (event.actor_kind != "host" or not event.actor_id)
        ):
            raise ProductSourceRetired()
        # Keep the ordinary settlement actor independent of the host's
        # containment identity; neither may be supplied by a human command.
        if not contained and (event.actor_kind, event.actor_id) != ("nonhuman", instance.principal_id):
            raise ProductSourceRetired()
        expected = {"attempt_id": attempt.id, "run_id": attempt.run_id, "status": expected_status}
        if contained:
            expected.update(reason=attempt.contained_reason, attachment_id=attempt.attachment_id)
        if not same_body(event.request, expected) or not contained and attempt.run_id is None:
            raise ProductSourceRetired()
        if run is not None and (
            type(run.metadata_json) is not dict
            or run.operation_kind != "run"
            or (run.run_id, run.thread_id, run.user_id, run.idempotency_key) != (attempt.run_id, attempt.thread_id, attempt.requester_id, "agent-work:" + attempt.id)
            or (run.metadata_json.get("agent_instance_id"), run.metadata_json.get("agent_work_id"), run.metadata_json.get("agent_work_attempt_id")) != (instance.id, work.id, attempt.id)
            or not contained
            and run.status not in {"attempt.failed": {"error", "timeout"}, "attempt.no_result": {"success"}, "attempt.stopped_restart": {"interrupted", "error", "timeout", "success"}}[key]
        ):
            raise ProductSourceRetired()
        if key == "attempt.stopped_restart" and intervened or key == "attempt.no_result" and (event.result.get("status") != "open" or event.result.get("outcome") is not None or event.result.get("blocker") is not None):
            raise ProductSourceRetired()
        reason = attempt.contained_reason if contained else run.stop_reason if run is not None and key == "attempt.failed" else None
        manager = bool(rows[0][2] & 4)
        user = rows[0][5]
        limited = any(row[7] is not None and row[7].role != "admin" and (not user.oauth_issuer or row[7].issuer == issuer_key(user.oauth_issuer)) for row in rows) if user.oauth_id and user.oauth_provider else False
        values = {
            "name": instance.name,
            "reason": reason,
            "supervisor": supervisor,
            "reader_is_manager": manager,
            "reader_is_supervisor": instance.supervisor_id == self.link.owner_user_id,
            "reader_is_administrator": user.system_role == "admin" and not limited,
            "progress": event.result.get("progress", ""),
            "thread_id": attempt.thread_id if key == "attempt.no_result" else None,
            "open_files": contained and reason in FILES_REASONS or key == "attempt.failed" and reason == "files-full" and manager,
            "excerpt": None,
        }
        signature = digest(
            [
                event_signature(event, None, intent),
                fields(attempt, ("id", "work_id", "activation_id", "requester_id", "run_id", "thread_id", "status", "contained_reason", "attachment_id")),
                fields(run, ("run_id", "thread_id", "user_id", "status", "stop_reason", "idempotency_key", "metadata_json")),
            ]
        )
        return signature, values
