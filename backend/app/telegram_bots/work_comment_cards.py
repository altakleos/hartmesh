"""A canonical saved comment owns a complete current request and fresh controls."""

from dataclasses import asdict

from app.telegram_bots.producer import ProductSourceRetired
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_request_source import same_displayed_basis
from app.telegram_bots.work_requests import WorkRequestRead, present_request, seen_basis


def read_comment(link, read):
    act, source = read.act, read.source
    request = source.request
    if (
        act.kind != "respond"
        or act.state != "done"
        or act.body.get("disposition", "supplied") != "supplied"
        or request["purpose"] not in {"decision", "review"}
        or request["state"] != "pending"
        or request["needs_routing"]
        or request["recipient_id"] != link.owner_user_id
        or not request["can_manage"]
    ):
        return None
    source_data = asdict(source)
    source_data["responses"] = list(source_data["responses"])
    return {"version": 1, "source": source_data, "response_id": read.receipt["receipt"]["response_id"]}


def frozen_comment(link, data, value):
    try:
        if type(value) is not dict or set(value) != {"version", "source", "response_id"} or type(value["version"]) is not int or value["version"] != 1:
            raise ValueError()
        source = WorkRequestRead(**{**value["source"], "responses": tuple(value["source"]["responses"])})
        act, request, work = data["act"], source.request, source.work
        response = next((row for row in source.responses if row["id"] == value["response_id"]), None)
        if (
            act["kind"] != "respond"
            or act["state"] != "done"
            or act["body"].get("disposition", "supplied") != "supplied"
            or request["purpose"] not in {"decision", "review"}
            or request["state"] != "pending"
            or request["needs_routing"]
            or request["recipient_id"] != link.owner_user_id
            or not request["can_manage"]
            or data["permissions"] & 6 != 6
            or (request["work_id"], request["instance_id"], work["instance_id"]) != (work["id"], link.agent_ref, link.agent_ref)
            or (work["id"], request["id"], work["revision"], request["revision"], source.protected, request["purpose"])
            != (data["work_id"], data["request_id"], data["work_revision"], data["request_revision"], data["protected"], data["purpose"])
            or type(source.protected) is not bool
            or not same_body(source.seen, seen_basis(work, request, source.responses))
            or response is None
            or response["actor_id"] != link.owner_user_id
            or response["disposition"] != "supplied"
            or response.get("text") != act["body"].get("text")
            or response.get("choice") != act["body"].get("choice")
            or response["request_revision"] != act["body"]["expected_request_revision"]
            or response["assignment_revision"] != act["body"]["expected_assignment_revision"]
        ):
            raise ValueError()
        return source
    except (KeyError, TypeError, ValueError):
        raise ProductSourceRetired() from None


def same_comment(link, data, left, right):
    if left is None or right is None:
        return left is right
    return left["response_id"] == right["response_id"] and same_displayed_basis(frozen_comment(link, data, left), frozen_comment(link, data, right))


def materialize_comment(link, data, value, *, locale):
    from app.telegram_bots.render import render
    from app.telegram_bots.split import split_html, visible_text

    source = frozen_comment(link, data, value)
    shown = present_request(source, locale=locale)
    html = render("decide.comment_saved", locale) + "\n\n" + "".join(part["html"] for part in shown.parts)
    chunks = split_html(html)
    keyboard = [list(line) for line in shown.keyboard]
    parts = [
        {
            "kind": "text",
            "html": chunk,
            "visible_text": visible_text(chunk),
            "protect_content": source.protected,
            "source_ref": {"key": "work.action.outcome", "values": {"act_id": data["act"]["id"], "part": index}},
            **({"stored_keyboard": True} if keyboard and index == len(chunks) - 1 else {}),
        }
        for index, chunk in enumerate(chunks)
    ]
    return parts, keyboard, None


def acknowledged_comment(link, parent):
    from app.telegram_bots.work_outcome_delivery import acknowledged_parts

    try:
        basis = parent.payload["work_outcome_basis"]
        if (
            type(basis) is not dict
            or set(basis) != {"version", "context", "locale", "timezone", "parts", "keyboard", "destination", "input", "origin", "comment"}
            or type(basis["version"]) is not int
            or basis["version"] != 5
            or parent.state != "sent"
            or parent.class_ != "reply"
            or (parent.link_id, parent.owner_user_id, parent.binding_generation, parent.chat_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id)
            or not same_body(basis["destination"], {"chat_id": link.chat_id, "message_thread_id": parent.message_thread_id})
            or type(parent.payload["presentation_version"]) is not int
            or parent.payload["presentation_version"] != 0
        ):
            raise ValueError()
        source = frozen_comment(link, basis["context"], basis["comment"])
        if (parent.instance_id, parent.work_id, parent.request_id) != (link.agent_ref, source.work["id"], source.request["id"]):
            raise ValueError()
        parts, keyboard, _ = materialize_comment(link, basis["context"], basis["comment"], locale=basis["locale"])
        if not same_body(parts, basis["parts"]) or not same_body(keyboard, basis["keyboard"]) or not same_body(keyboard, parent.payload["product_keyboard_basis"]):
            raise ValueError()
        acknowledged_parts(parent, parts)
        return source
    except (KeyError, TypeError, ValueError):
        raise ProductSourceRetired() from None
