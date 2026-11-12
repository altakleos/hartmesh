"""Source-qualified complete Work request previews, without dispatch or mutation."""

from copy import deepcopy
from dataclasses import dataclass
from html import escape

from app.telegram_bots.attention import AttentionReader
from app.telegram_bots.authority import AuthorityUnavailable
from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.render import render, render_plain
from app.telegram_bots.split import split_html, visible_text
from deerflow.agent_instances.contract import AgentConflict, AgentDenied
from deerflow.agent_instances.human_input import HumanInput
from deerflow.spaces.contract import PrincipalRef
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class WorkRequestRead:
    request: dict
    work: dict
    responses: tuple[dict, ...]
    seen: dict
    people: dict[str, str]
    protected: bool


@dataclass(frozen=True)
class WorkRequestPresentation:
    parts: tuple[dict, ...]
    keyboard: tuple[tuple[dict, ...], ...]


def seen_basis(record, request, responses):
    return dict(
        work_revision=record["revision"],
        assignment_revision=record["assignment_revision"],
        request_id=request["id"],
        revision=request["revision"],
        request_revision=request["request_revision"],
        response_ids=sorted(row["id"] for row in responses),
        outcome_id=record.get("outcome_id"),
        evidence_revision=(record.get("outcome") or {}).get("evidence_revision"),
        blocker_id=(record.get("blocker") or {}).get("id"),
        blocker_revision=(record.get("blocker") or {}).get("revision"),
    )


class WorkRequestReader(AttentionReader):
    async def read_request(self, link, request_id):
        return await await_drained(self._read_request(link, request_id))

    async def _read_request(self, link, request_id):
        from app.gateway.deps import bind_user_repo

        authority = await self._current(link)
        actor = PrincipalRef("human", link.owner_user_id)
        human = HumanInput(self.work)
        writable = "agents:write" in authority[0]
        request = await human.get(actor=actor, request_id=request_id, can_write=writable)
        if request["instance_id"] != link.agent_ref:
            raise AgentDenied("The request does not belong to this bot")
        record = await self.work.get(actor=actor, instance_id=link.agent_ref, work_id=request["work_id"])
        responses, offset = [], 0
        while True:
            page = await human.responses(actor=actor, request_id=request_id, limit=100, offset=offset)
            responses.extend(page["responses"])
            if not page["has_more"]:
                break
            if not page["responses"] or len(responses) >= 200:
                raise AuthorityUnavailable()
            offset += len(page["responses"])
        if len({row["id"] for row in responses}) != len(responses):
            raise AuthorityUnavailable()
        users = bind_user_repo(self.links.session_factory)
        people = {}
        for identity in {row["actor_id"] for row in responses if row.get("actor_id")}:
            user = await users.get_user_by_id(identity)
            if user is not None:
                people[identity] = user.email
        current_request = await human.get(actor=actor, request_id=request_id, can_write=writable)
        current_record = await self.work.get(actor=actor, instance_id=link.agent_ref, work_id=request["work_id"])
        if request != current_request or record != current_record:
            raise AgentConflict("Review the current request and all responses")
        if await self._current(link) != authority:
            raise AuthorityUnavailable()
        return WorkRequestRead(request, record, tuple(responses), seen_basis(record, request, responses), people, authority[1][1] == "protected")


def present_request(source, *, locale, concealed=False, outbox_id=None, files_above=()):
    """Full text precedes action controls; a concealed teaser conveys no basis."""
    request, record = source.request, source.work
    purpose = request["purpose"]
    if purpose not in {"information", "decision", "review"}:
        raise ValueError("unsupported-work-request")
    if concealed:
        if type(outbox_id) is not str or len(outbox_id) != 32 or any(ch not in "0123456789abcdef" for ch in outbox_id):
            raise ValueError("concealed-source-required")
        key = {"information": "concealed.question", "decision": "concealed.decision", "review": "concealed.review"}[purpose]
        html = render(key, locale)
        keyboard = [[{"label": "label.show", "kind": "reveal", "target_kind": "outbox", "target_id": outbox_id, "payload": {"outbox_id": outbox_id}}]]
    else:
        values = dict(objective=record["objective"], name=request["instance_name"], question=request["question"], reason=request["reason"], expected_response=request["expected_response"])
        if purpose == "review":
            html = render("review.ready", locale, **values, statement=(record.get("outcome") or {}).get("statement", ""))
            html += "\n\n" + render("ask.info", locale, **values).partition("\n")[2]
        else:
            html = render("ask.info" if purpose == "information" else "ask.decision", locale, **values)
        choices = request.get("choices") or []
        if choices:
            html += "\n\n" + "\n".join(f"{index}. {escape(choice)}" for index, choice in enumerate(choices, 1))
        for response in source.responses:
            text = response.get("text") or response.get("choice") or ""
            if response.get("text") and response.get("choice") and response["choice"] != response["text"]:
                text = response["choice"] + "\n" + response["text"]
            if response["disposition"] in {"cannot_provide", "wrong_recipient"}:
                text = render_plain("label." + response["disposition"], locale) + ("\n" + text if text else "")
            html += "\n\n" + render("ask.comment_line", locale, person=source.people.get(response.get("actor_id"), render("label.colleague", locale)), text=text)
            for reference in response.get("sources") or []:
                if reference.get("path"):
                    html += "\n" + escape(reference["path"])
        keyboard = []
        payload = {"request_id": request["id"], "work_id": record["id"], "seen": deepcopy(source.seen)}

        def button(kind, label, **extra):
            return {"label": label, "kind": kind, "target_kind": "request", "target_id": request["id"], "payload": {**payload, **extra}}

        if request["state"] != "closed" and not request["needs_routing"]:
            if purpose == "information" and request["can_respond"]:
                for index, choice in enumerate(choices, 1):
                    keyboard.append([{**button("hi_choice", "label.choice", choice=choice), "label_values": {"label": choice if len(choice) <= 32 else str(index)}}])
                keyboard.append([button("hi_answer", "label.type_answer" if choices else "label.answer"), button("hi_cant_open", "label.cannot_answer")])
            elif purpose == "decision" and request["can_manage"]:
                for index, choice in enumerate(choices, 1):
                    keyboard.append([{**button("decide_choice", "label.choice", decision=choice), "label_values": {"label": choice if len(choice) <= 32 else str(index)}}])
                keyboard.append([button("decide_own", "label.decide_own")])
            elif purpose == "review" and request["can_manage"]:
                keyboard = [[button("accept_open", "label.accept")], [button("changes_open", "label.request_changes")]]
        if files_above:
            if purpose != "review" or any(type(name) is not str or not name for name in files_above):
                raise ValueError("invalid-result-file-names")
            html += "\n\n" + render("result.files_above", locale) + "\n" + "\n".join(escape(name) for name in files_above)
    chunks = split_html(html)
    if not chunks:
        raise ValueError("empty-work-preview")
    return WorkRequestPresentation(
        tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "protect_content": source.protected} for chunk in chunks),
        tuple(tuple(row) for row in canonical_keyboard(keyboard)),
    )
