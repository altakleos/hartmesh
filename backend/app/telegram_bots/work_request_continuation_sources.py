"""Original request confirmation and primary receipt in child admission SQL."""

from sqlalchemy import and_, false
from sqlalchemy.orm import aliased

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.callbacks import _validate_rows
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_continuation_state import continuation_state, require_child
from app.telegram_bots.work_control_admission import command
from app.telegram_bots.work_delegate_actions import DELEGATION_RECEIPT_FIELDS
from app.telegram_bots.work_interaction_delivery import acknowledged_source
from app.telegram_bots.work_request_continuations import outcome_signature, request_activation, request_input, request_signature
from deerflow.agent_instances.work import UNRESOLVED
from deerflow.persistence.agent_instances.human_input import HumanInputEventRow, HumanInputResponseRow
from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkEventRow
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


class RequestParentProof:
    def __init__(self, acts, link, parent, child, *, for_effect):
        self.acts, self.link, self.parent, self.child, self.for_effect = acts, link, parent, child, for_effect
        self.source = request_activation(parent)

    def extend(self, query, parent):
        self.column = len(query.column_descriptions)
        callback, anchor, member, outcome, event, response, promise = (
            aliased(TelegramCallbackRow),
            aliased(TelegramOutboxRow),
            aliased(TelegramCallbackRow),
            aliased(TelegramOutboxRow),
            aliased(AgentWorkEventRow),
            aliased(HumanInputEventRow),
            aliased(AgentWorkAttemptRow),
        )
        origin = self.source["origin"]
        result = (
            query.add_columns(callback, anchor, member, outcome, event, response, promise)
            .outerjoin(callback, callback.id == origin["callback_id"])
            .outerjoin(anchor, anchor.id == origin["anchor_outbox_id"])
            .outerjoin(member, member.group_id == callback.group_id)
            .outerjoin(outcome, outcome.id == self.source["outcome_id"])
            .outerjoin(event, and_(event.instance_id == parent.instance_id, event.operation_id == parent.id))
            .outerjoin(response, and_(response.instance_id == parent.instance_id, response.operation_id == parent.id))
            .outerjoin(promise, and_(promise.work_id == self.child.work_id, promise.activation_id == self.child.id))
        )
        self.input_column = len(result.column_descriptions)
        mark, question, control, answer = aliased(TelegramInboundMarkRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), aliased(HumanInputResponseRow)
        identity = self.source["input"]
        result = (
            result.add_columns(mark, question, control, answer)
            .outerjoin(mark, and_(mark.link_id == identity["link_id"], mark.chat_id == identity["chat_id"], mark.message_id == identity["message_id"]) if identity is not None else false())
            .outerjoin(question, question.id == mark.route_basis["work_answer"]["source_outbox_id"].as_string())
            .outerjoin(control, control.group_id == mark.route_basis["work_answer"]["group_id"].as_string())
            .outerjoin(answer, answer.id == response.receipt["response_id"].as_string())
        )
        typed_prompt, typed_parent = aliased(TelegramCallbackRow), aliased(TelegramOutboxRow)
        self.typed_column = len(result.column_descriptions)
        return (
            result.add_columns(typed_prompt, typed_parent)
            .outerjoin(typed_prompt, typed_prompt.id == mark.route_basis["work_input"]["prompt_id"].as_string())
            .outerjoin(typed_parent, typed_parent.id == mark.route_basis["work_input"]["source_outbox_id"].as_string())
        )

    def check(self, rows, parent, child, held):
        parent, child = _snapshot(parent), _snapshot(child)
        require_child(parent, child)
        actual = request_activation(parent)
        state = continuation_state(parent)
        if not same_body(actual, self.source) or parent.state != "done" or list(parent.operation_ids) != [child.id] or (state["work_id"], state["assignment_revision"]) != (parent.work_id, actual["assignment_revision"]):
            raise ValueError("invalid-request-parent")
        callback, anchor, _member, outcome, event, response, promise = rows[0][self.column : self.column + 7]
        exact_promise = (
            promise is not None
            and promise.run_id is None
            and promise.status in UNRESOLVED
            and (promise.work_id, promise.activation_id, promise.requester_id, promise.thread_id) == (child.work_id, child.id, child.owner_user_id, child.body["thread_id"])
            and same_body(promise.request, child.body)
        )
        if self.for_effect:
            if (
                parent.cancel_requested_at is not None
                or parent.kind == "decide"
                and rows[0][0].acts_paused_since is not None
                or not (state["phase"] == "child_planned" or state["phase"] == "refused" and exact_promise)
                or not {"agents:write", "runs:create"} <= held
            ):
                raise ValueError("invalid-request-parent")
        elif state["phase"] not in {"child_planned", "done", "refused", "cancelled"}:
            raise ValueError("invalid-request-parent")
        target = (None, None, None) if parent.kind == "respond" else (self.link.agent_ref, parent.work_id, parent.request_id)
        if (
            callback is None
            or anchor is None
            or outcome is None
            or (anchor.link_id, anchor.owner_user_id, anchor.binding_generation, anchor.chat_id, anchor.instance_id, anchor.work_id, anchor.request_id)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, *target)
            or (callback.id, callback.kind, callback.message_thread_id) != (parent.id, "decide_confirm_resume" if parent.kind == "decide" else "voice_save_resume", anchor.message_thread_id)
            or (callback.link_id, callback.binding_generation, callback.chat_id) != (self.link.id, self.link.binding_generation, self.link.chat_id)
            or actual["origin"]["presentation_version"] != callback.payload.get("presentation_version")
            or request_signature(anchor) != actual["origin"]["signature"]
            or outcome_signature(outcome) != actual["outcome_signature"]
            or (outcome.link_id, outcome.owner_user_id, outcome.binding_generation, outcome.chat_id, outcome.message_thread_id, outcome.instance_id, outcome.work_id, outcome.request_id, outcome.class_)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, anchor.message_thread_id, self.link.agent_ref, parent.work_id, parent.request_id, "reply")
        ):
            raise ValueError("invalid-request-consent")
        members = list({value.id: value for row in rows if (value := row[self.column + 2]) is not None}.values())
        _validate_rows(anchor, members)
        if not members or any(
            member.payload.get("work_action_id") != parent.id
            or member.consumed_at is None
            or member.closed_reason != "work-done"
            or member.message_id != callback.message_id
            or (member.link_id, member.binding_generation, member.chat_id, member.message_thread_id) != (self.link.id, self.link.binding_generation, self.link.chat_id, anchor.message_thread_id)
            for member in members
        ):
            raise ValueError("invalid-request-consent")
        mark, question, _question_control, answer = rows[0][self.input_column : self.input_column + 4]
        identity = actual["input"]
        if identity is not None:
            if (
                mark is None
                or mark.route_kind != "work_input"
                or mark.route_target_id != parent.request_id
                or (mark.link_id, mark.chat_id, mark.binding_generation, mark.message_thread_id) != (self.link.id, self.link.chat_id, self.link.binding_generation, anchor.message_thread_id)
                or not same_body(request_input(mark, kind=identity["kind"]), identity)
            ):
                raise ValueError("invalid-request-input")
        if parent.kind == "decide":
            source = acknowledged_source(self.link, anchor)
            saved_input = anchor.payload["work_request_basis"].get("input")
            if (identity is None) != (saved_input is None):
                raise ValueError("invalid-request-input")
            if identity is not None:
                from app.telegram_bots.work_input_previews import check_input_origin, input_identity

                if not same_body(input_identity(mark), saved_input):
                    raise ValueError("invalid-request-input")
                prompt, original = rows[0][self.typed_column : self.typed_column + 2]
                check_input_origin(
                    self.link, original, [prompt] if prompt is not None else [], mark, False, anchor.payload["work_request_basis"], source, draft_ttl_hours=self.acts.outbox.config.callbacks.draft_ttl_hours, receipt_owned=True
                )
            kind, body = command(self.acts, callback, source)
            if kind != parent.kind or not same_body(body.model_dump(mode="json", exclude_unset=True), parent.body):
                raise ValueError("invalid-request-consent")
        else:
            self.check_voice(rows, parent, callback, anchor, members, mark, question)
        shown = outcome.payload["work_outcome_basis"]["context"]
        if (shown["work_id"], shown["request_id"], shown["act"]["id"], shown["act"]["kind"], shown["act"]["state"], shown["act"]["attempts"]) != (
            parent.work_id,
            parent.request_id,
            parent.id,
            parent.kind,
            "done",
            parent.attempts,
        ) or not same_body(shown["act"]["body"], parent.body):
            raise ValueError("invalid-request-delivery")
        qualify_request_primary(parent, actual, event, response, answer)

    def check_voice(self, rows, parent, callback, anchor, members, mark, question):
        from app.telegram_bots.work_outcome_delivery import acknowledged_parts
        from app.telegram_bots.work_untied_delivery import answer_data, answer_input, answer_keyboard, answer_source_group, answer_source_input, notice_recipes

        data = answer_data(mark)
        basis = anchor.payload.get("product_basis", {})
        if (
            data["version"] != 2
            or data.get("resume") is not True
            or data["purpose"] != "answer"
            or (data["notice_id"], data["work_id"], data["request_id"], data["text"], data["seen"]["assignment_revision"]) != (anchor.id, parent.work_id, parent.request_id, parent.body["text"], self.source["assignment_revision"])
            or basis.get("origins") != [[mark.link_id, mark.message_id, mark.chat_id]]
            or basis.get("finish_route") is not False
        ):
            raise ValueError("invalid-voice-consent")
        if mark.operation_ids != [parent.id] or mark.route_basis.get("work_action_id") != parent.id or not answer_input(mark, data):
            raise ValueError("invalid-voice-input")
        if question is None or (question.link_id, question.owner_user_id, question.binding_generation, question.chat_id, question.message_thread_id, question.instance_id, question.work_id, question.request_id) != (
            self.link.id,
            self.link.owner_user_id,
            self.link.binding_generation,
            self.link.chat_id,
            anchor.message_thread_id,
            self.link.agent_ref,
            parent.work_id,
            parent.request_id,
        ):
            raise ValueError("invalid-voice-source")
        source = acknowledged_source(self.link, question)
        controls = list({value.id: value for row in rows if (value := row[self.input_column + 2]) is not None}.values())
        answer_source_group(question, controls, data)
        answer_source_input(question, mark, data)
        acknowledged_parts(anchor, notice_recipes(anchor, data, mark.message_id))
        cleared = all("transcript" not in member.payload for member in members)
        if not same_body(anchor.payload["product_keyboard_basis"], answer_keyboard(data, mark.message_id, cleared=cleared)) or (source.request["purpose"], source.request["id"], source.work["id"]) != (
            "information",
            parent.request_id,
            parent.work_id,
        ):
            raise ValueError("invalid-voice-consent")
        if (
            (callback.payload.get("work_id"), callback.payload.get("assignment_revision"), callback.payload.get("request_revision")) != (parent.work_id, self.source["assignment_revision"], parent.body["expected_request_revision"])
            or not cleared
            and callback.payload.get("transcript") != parent.body["text"]
        ):
            raise ValueError("invalid-voice-consent")


def qualify_request_primary(parent, actual, event, response, answer):
    """Immutable Native receipts shared by admission and settled-history expiry."""
    if (
        event is None
        or (event.instance_id, event.work_id, event.operation_id, event.actor_kind, event.actor_id, event.action)
        != (parent.instance_id, parent.work_id, parent.id, "human", parent.owner_user_id, "decide" if parent.kind == "decide" else "human_response")
        or not same_body(event.request, parent.body)
        or event.assignment_revision != actual["assignment_revision"]
        or type(event.result) is not dict
        or any(type(event.result.get(key)) is not int for key in ("revision", "assignment_revision"))
        or (event.result.get("id"), event.result.get("instance_id"), event.result.get("revision"), event.result.get("assignment_revision")) != (parent.work_id, parent.instance_id, event.revision, event.assignment_revision)
    ):
        raise ValueError("invalid-request-primary")
    if parent.kind == "decide":
        if response is None or any(not same_body(event.result.get(key), parent.result.get(key)) for key in DELEGATION_RECEIPT_FIELDS) or event.assignment_revision != actual["assignment_revision"]:
            raise ValueError("invalid-request-primary")
        expected = {
            "operation_id": parent.id,
            "request_id": parent.request_id,
            "revision": parent.body["request_basis"]["revision"] + 1,
            "request_revision": parent.body["request_basis"]["request_revision"],
            "actor_id": parent.owner_user_id,
            "actor_kind": "human",
            "response_id": None,
        }
        if (
            (response.instance_id, response.request_id, response.operation_id, response.actor_kind, response.actor_id, response.action) != (parent.instance_id, parent.request_id, parent.id, "human", parent.owner_user_id, "decide")
            or not same_body(response.request, parent.body)
            or not same_body(response.receipt, expected)
        ):
            raise ValueError("invalid-request-primary")
    else:
        if (
            response is None
            or (response.instance_id, response.request_id, response.operation_id, response.actor_kind, response.actor_id, response.action) != (parent.instance_id, parent.request_id, parent.id, "human", parent.owner_user_id, "respond")
            or not same_body(response.request, parent.body)
            or not same_body(response.receipt, parent.result.get("receipt"))
        ):
            raise ValueError("invalid-request-primary")
        if (
            answer is None
            or (answer.request_id, answer.actor_id, answer.disposition, answer.text, answer.choice, answer.request_revision, answer.assignment_revision)
            != (parent.request_id, parent.owner_user_id, "supplied", parent.body["text"], None, parent.body["expected_request_revision"], actual["assignment_revision"])
            or not same_body(answer.sources, parent.body.get("sources", []))
        ):
            raise ValueError("invalid-request-primary")

        expected = {
            "operation_id": parent.id,
            "request_id": parent.request_id,
            "revision": response.revision,
            "request_revision": parent.body["expected_request_revision"],
            "actor_id": parent.owner_user_id,
            "actor_kind": "human",
            "response_id": answer.id,
        }
        if not same_body(response.receipt, expected):
            raise ValueError("invalid-request-primary")
