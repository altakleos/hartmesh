"""A refused Stop cannot lose its exact original attempt or child ownership."""

from datetime import timedelta

from sqlalchemy import and_
from sqlalchemy.orm import aliased

from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired
from app.telegram_bots.work_detail_cards import full_detail
from app.telegram_bots.work_saved_refusals import SavedStartProof
from app.telegram_bots.work_status_control_origins import carrier_source, status_record
from app.telegram_bots.work_stop_controls import displayed_stop
from app.telegram_bots.work_stop_refusals import StopRefusalProof, refused_stop
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow
from deerflow.persistence.user.model import UserRow


class SavedStopProof(SavedStartProof):
    required_routes = frozenset({"agents:write", "runs:cancel"})

    @staticmethod
    def eligible(root, effective):
        return root.id == effective.id and refused_stop(root)

    def __init__(self, owner, link, item, root, effective, origin, callback, parent, primary):
        super().__init__(owner, link, item, root, effective, origin, callback, parent, primary)
        original, _work_id, _attempt_id = displayed_stop(link, parent, callback)
        source = carrier_source(original)
        record = status_record(source) if source["key"].startswith("status.") else full_detail(source)["record"]
        shown = record.get("attempt") or {}
        if (record.get("id"), shown.get("id")) != (root.work_id, root.body["attempt_id"]) or type(shown.get("requester_id")) is not str:
            raise ProductSourceRetired()
        self.stop_requester, self.original_id = shown["requester_id"], shown["id"]
        self.work_id, self.original_scope = root.work_id, root.stop_dispatch
        self.no_effect = StopRefusalProof(root)

    def extend(self, query):
        query = self.no_effect.extend(super().extend(query))
        self.stop_column = len(query.column_descriptions)
        native, work, requester = aliased(AgentWorkAttemptRow), aliased(AgentWorkRow), aliased(UserRow)
        return (
            query.add_columns(native, work, requester.email)
            .join(native, native.id == self.original_id)
            .join(work, and_(work.id == native.work_id, work.instance_id == self.link.agent_ref))
            .outerjoin(requester, requester.id == native.requester_id)
        )

    def check_source(self, rows, root, effective, canonical, parent, primary, native, event):
        super().check_source(rows, root, effective, canonical, parent, primary, native, event)
        self.no_effect.check(rows, root)
        self.check_original(rows)

    def check_original(self, rows):
        original, work, email = rows[0][self.stop_column : self.stop_column + 3]
        if (
            (original.id, original.work_id, original.requester_id, work.instance_id) != (self.original_id, self.work_id, self.stop_requester, self.link.agent_ref)
            or original.status not in {"starting", "running"}
            or original.thread_id != self.original_scope["thread_id"]
            or self.original_scope["run_id"] is not None
            and original.run_id != self.original_scope["run_id"]
        ):
            raise ProductSourceRetired()
        if self.metadata is not None and self.metadata.key == "stop.not_yours" and email != self.metadata.values["person"]:
            raise DeliveryRetry(timedelta(seconds=1))


class SavedCommandStopProof(SavedStopProof):
    """A real typed Stop uses its actual Inbox selection without a callback."""

    def __init__(self, owner, link, item, root, native, mode):
        from app.telegram_bots.work_execution_readiness import readiness_signature

        self.owner, self.link, self.item = owner, link, item
        self.root_id = self.effective_id = root.id
        self.signature = readiness_signature(root, root)
        self.origin, self.group_id, self.parent_id = mode.proof.origin, None, item.id
        self.primary_id = self.primary_signature = None
        self.metadata = self.captured = self.silent = self.now = None
        self.stop_requester, self.original_id = native.requester_id, native.id
        self.work_id, self.original_scope = root.work_id, root.stop_dispatch
        self.no_effect, self.command_mode = StopRefusalProof(root), mode

    @classmethod
    async def prepare(cls, owner, link, item, root, reader):
        from app.telegram_bots.work_command_stop_origins import CommandStopModeProof

        if not cls.eligible(root, root):
            return None
        mode = CommandStopModeProof(link, root, clock=owner.host.outbox.clock, for_effect=False)
        await mode.proof.read(reader)
        native = await reader.get(AgentWorkAttemptRow, root.body["attempt_id"])
        if native is None:
            raise ProductSourceRetired()
        return cls(owner, link, item, root, native, mode)

    def extend(self, query):
        return self.command_mode.extend(super().extend(query))

    def check_source(self, rows, root, effective, canonical, parent, primary, native, event):
        from app.telegram_bots.work_actions import same_body
        from app.telegram_bots.work_execution_readiness import readiness_signature
        from app.telegram_bots.work_execution_sources import require_execution_scope

        require_execution_scope(self.link, root)
        if (
            not self.eligible(root, effective)
            or readiness_signature(root, effective) != self.signature
            or native is not False
            or event is not False
            or (canonical.link_id, canonical.owner_user_id, canonical.binding_generation, canonical.chat_id, canonical.instance_id, canonical.work_id, canonical.message_thread_id, canonical.class_)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, root.work_id, self.command_mode.proof.topic, "reply")
            or canonical.dedupe_key != "notice:work-execution:" + root.id
            or not same_body(canonical.payload.get("work_execution_intent"), {"version": 1, "act_id": root.id, "origin": self.origin})
        ):
            raise ProductSourceRetired()
        self.command_mode.check(rows, root)
        self.no_effect.check(rows, root)
        self.check_original(rows)
