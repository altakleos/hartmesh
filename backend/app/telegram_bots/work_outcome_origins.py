"""Actual outcome acknowledgements and canonical primary receipts at activation."""

import hashlib
import json
from types import SimpleNamespace

from sqlalchemy import and_, false
from sqlalchemy.orm import aliased

from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import _ACT_SCOPE, same_body
from app.telegram_bots.work_outcome_delivery import acknowledged_parts, materialize
from app.telegram_bots.work_outcome_proof import native_proof, outcome_key
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.work import AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramActRow


def is_outcome_carrier(parent):
    return parent is not None and ("work_outcome_basis" in parent.payload or parent.dedupe_key.startswith("notice:work-outcome:") or any(part.get("source_ref", {}).get("key") == "work.action.outcome" for part in parent.parts))


def acknowledged_outcome(link, parent, version, *, readonly_start_refresh=False):
    try:
        item = _item(parent)
        basis = item.payload["work_outcome_basis"]
        data = basis["context"]
        proof = data["origin_proof"]
        act = SimpleNamespace(**proof["act"])
        if (
            type(version) is not int
            or version != 0
            or data["work_controls"] != 1
            or type(data["work_controls"]) is not int
            or act.state != "done"
            or act.kind not in {"respond", "decide", "changes_requested"}
            or basis.get("routing") is not None
            or basis.get("comment") is not None
            or (item.link_id, item.owner_user_id, item.chat_id, item.binding_generation, item.instance_id, item.work_id, item.request_id, item.class_)
            != (link.id, link.owner_user_id, link.chat_id, link.binding_generation, link.agent_ref, act.work_id, act.request_id, "reply")
            or (act.link_id, act.owner_user_id, act.bot_id, act.binding_generation, act.telegram_user_id, act.chat_id, act.instance_id)
            != (link.id, link.owner_user_id, link.bot_id, link.binding_generation, link.telegram_user_id, link.chat_id, link.agent_ref)
            or data["work_id"] != act.work_id
            or data["request_id"] != act.request_id
            or data["detail"]["record"]["id"] != act.work_id
            or not same_body(data["act"], {key: proof["act"][key] for key in data["act"]})
            or basis["destination"] != {"chat_id": item.chat_id, "message_thread_id": item.message_thread_id}
            or outcome_key(item.dedupe_key) != (act.id, hashlib.sha256(json.dumps(basis, sort_keys=True).encode()).hexdigest()[:32])
        ):
            raise ProductSourceRetired()
        parts, keyboard, _ = materialize(link, data, locale=basis["locale"], timezone=basis["timezone"], product="")
        from app.telegram_bots.callbacks import start_refresh_keyboard

        matches = start_refresh_keyboard(basis["keyboard"], keyboard) if readonly_start_refresh else same_body(keyboard, basis["keyboard"])
        if not same_body(parts, basis["parts"]) or not matches or not same_body(basis["keyboard"], item.payload["product_keyboard_basis"]):
            raise ProductSourceRetired()
        acknowledged_parts(item, parts, edit=basis.get("edit") is not None)
        return item
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ProductSourceRetired() from None


def outcome_source(original):
    data = original.payload["work_outcome_basis"]["context"]
    return {
        "work_id": data["work_id"],
        "key": "outcome." + data["act"]["kind"],
        "values": {},
        "protected": data["protected"],
        "work": {"detail": data["detail"], "detail_dates": data["detail_dates"], "origin_proof": data["origin_proof"]},
        "event_signature": digest(data["origin_proof"]),
        "signature": digest(data),
    }


class OutcomeOriginProjection:
    def __init__(self, link, original):
        self.link = link
        self.proof = original.payload["work_outcome_basis"]["context"]["origin_proof"]

    def extend(self, query):
        primary = self.proof["act"]
        act, effect, answer = aliased(TelegramActRow), aliased(AgentWorkEventRow), aliased(HumanInputResponseRow)
        event = aliased(HumanInputEventRow if primary["kind"] == "respond" else AgentWorkEventRow)
        self.column = len(query.column_descriptions)
        return (
            query.add_columns(act, event, effect, answer)
            .outerjoin(act, act.id == primary["id"])
            .outerjoin(event, and_(event.instance_id == self.link.agent_ref, event.operation_id == primary["id"]))
            .outerjoin(effect, and_(effect.instance_id == self.link.agent_ref, effect.operation_id == primary["id"]))
            .outerjoin(answer, answer.id == self.proof["answer"]["id"] if primary["kind"] == "respond" else false())
        )

    def check_with_permissions(self, rows, held):
        act, event, effect, answer = rows[0][self.column : self.column + 4]
        if act is None or event is None or effect is None or self.proof["act"]["kind"] == "respond" and answer is None:
            raise ProductSourceRetired()
        if any(getattr(act, key) != self.proof["act"][key] for key in _ACT_SCOPE) or not same_body(native_proof(act, event, effect, answer), self.proof):
            raise ProductSourceRetired()
