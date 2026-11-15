"""The real original Start source and child relation share final admission SQL."""

from collections.abc import Mapping

from sqlalchemy import String, cast, or_
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_continuation_sources import ActivationParentProjection
from app.telegram_bots.work_start_conversations import context_deadline, require_context_child
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


class ContextParentProjection:
    def __init__(self, child, *, for_effect=True, explicit=False):
        self.child = child
        self.parent = None
        self.for_effect = for_effect
        self.explicit = explicit

    async def prepare(self, acts, link):
        self.clock = acts.outbox.clock
        marker = (self.child.continuation or {}).get("activation_context_parent")
        if not isinstance(marker, Mapping) or type(marker.get("root_id")) is not str:
            raise ValueError("invalid-activation-context-parent")
        async with acts.session_factory() as reader:
            root = await reader.get(TelegramActRow, marker["root_id"])
            if root is None:
                raise ValueError("missing-activation-context-parent")
            self.root = _snapshot(root)
            require_context_child(self.root, self.child)
        self.parent = ActivationParentProjection(self.root, for_effect=self.for_effect, effective_child=self.child)
        await self.parent.prepare(acts, link)
        return self

    def extend(self, query):
        root, attempt, event = aliased(TelegramActRow), aliased(AgentWorkAttemptRow), aliased(AgentWorkEventRow)
        self.column = len(query.column_descriptions)
        result = (
            query.add_columns(root, attempt.id, event.id)
            .outerjoin(root, or_(root.id == self.root.id, root.continuation["activation_context"]["child_id"].as_string() == self.child.id, cast(root.operation_ids, String).like('%"' + self.child.id + '"%')))
            .outerjoin(attempt, attempt.activation_id == root.id)
            .outerjoin(event, event.operation_id == root.id)
        )
        child_attempt, child_event = aliased(AgentWorkAttemptRow), aliased(AgentWorkEventRow)
        self.operation_column = len(result.column_descriptions)
        result = result.add_columns(child_attempt, child_event).outerjoin(child_attempt, child_attempt.activation_id == self.child.id).outerjoin(child_event, child_event.operation_id == self.child.id)
        result = self.parent.extend(result)
        self.origin_parent_column = self.parent.column
        return result

    def check(self, rows, actual_child, held):
        roots = {row[self.column].id: row[self.column] for row in rows if row[self.column] is not None}
        if len(roots) != 1:
            raise ValueError("invalid-activation-context-parent")
        root = next(iter(roots.values()))
        require_context_child(_snapshot(root), _snapshot(actual_child))
        if (
            root.state not in {"refused", "done", "uncertain"}
            or self.for_effect
            and (root.state not in {"refused", "uncertain"} or root.reason != root.continuation["activation_context"]["reason"] or root.cancel_requested_at is not None)
            or not same_body(root.body, self.root.body)
            or not same_body(root.continuation, self.root.continuation)
            or any(row[self.column + 1] is not None or row[self.column + 2] is not None for row in rows)
        ):
            raise ValueError("invalid-activation-context-admission")
        from app.telegram_bots.work_start_conversations import exact_runless_promise, qualify_child_operation

        attempts = {value.id: value for row in rows if (value := row[self.operation_column]) is not None}
        events = {value.id: value for row in rows if (value := row[self.operation_column + 1]) is not None}
        if len(attempts) > 1 or len(events) > 1:
            raise ValueError("ambiguous-activation-context-operation")
        event, attempt = next(iter(events.values()), None), next(iter(attempts.values()), None)
        qualify_child_operation(_snapshot(actual_child), event, attempt, allow_absent=True)
        if self.for_effect and self.clock() >= context_deadline(self.root) and not (self.explicit and exact_runless_promise(_snapshot(actual_child), event, attempt, deadline=context_deadline(self.root))):
            raise ValueError("activation-context-deadline")
        self.parent.check(rows, root, held)
