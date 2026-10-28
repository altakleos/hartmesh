"""One qualified mixed initial offer; independent file and human-input custody."""

from copy import deepcopy


async def prepare_mixed_files(record, source, *, http_owner, guard, capacity, max_bytes, conversation_url=None):
    from pathlib import PurePosixPath

    from app.telegram_bots.human_input_source import check_qualified_request, mixed_file_paths
    from app.telegram_bots.links import prepare_publication_guard

    basis = check_qualified_request(source, record)
    paths = mixed_file_paths(record, basis)

    async def check():
        check_qualified_request(source, record)
        await prepare_publication_guard(source._guard)
        await prepare_publication_guard(guard)
        check_qualified_request(source, record)

    files = []
    for path in paths:
        await check()
        try:
            metadata = await http_owner.describe_artifact(record.scope.owner_user_id, record.scope.thread_id, path, before_dispatch=check)
            if metadata.path != path:
                raise ValueError
            files.append({"path": path, "filename": metadata.filename, "size": metadata.size, "mime_type": metadata.mime_type, "state": "ready"})
        except Exception:
            await check()
            files.append({"path": path, "filename": PurePosixPath(path).name, "size": None, "mime_type": "application/octet-stream", "state": "unavailable"})
    await check()
    return {"files": files, "capacity": capacity, "max_bytes": max_bytes, "conversation_url": conversation_url}


def prepare_mixed_reply(record, source_basis, *, parts, keyboard, inputs, file_plan):
    from app.telegram_bots.file_source import media_source
    from app.telegram_bots.files import plan_output_files
    from app.telegram_bots.human_input_source import freeze_human_input_parts, mixed_file_paths, validate_request_basis
    from app.telegram_bots.inbox import publication_identity, validate_publication_inputs
    from app.telegram_bots.producer import _safe_json

    request = validate_request_basis(record, source_basis)
    paths = mixed_file_paths(record, source_basis)
    validate_publication_inputs(record, inputs)
    if type(file_plan) is not dict or set(file_plan) != {"files", "capacity", "max_bytes", "conversation_url"} or type(file_plan["files"]) is not list or tuple(file.get("path") for file in file_plan["files"]) != paths:
        raise ValueError("human-input-source-conflict")
    ui, controls = freeze_human_input_parts(record, request, parts=parts, keyboard=keyboard)
    plan = plan_output_files(file_plan["files"], answer_visible="", capacity=file_plan["capacity"], max_bytes=file_plan["max_bytes"], caption_allowed=False)
    anchor = ui[0]["reply_to_message_id"]
    media = [{**deepcopy(template), "source_ref": media_source(template["files"]), "reply_to_message_id": anchor} for template in plan.parts]
    if file_plan["conversation_url"] is not None:
        from urllib.parse import quote, urlsplit

        from app.telegram_bots.producer import validate_url_button

        validate_url_button({"page": "web.conversation", "url": file_plan["conversation_url"]})
        parsed = urlsplit(file_plan["conversation_url"])
        route = "/workspace/" + ("agents/" + quote(record.scope.agent_ref, safe="") + "/" if record.scope.agent_kind == "custom_agent" else "") + "chats/" + quote(record.scope.thread_id, safe="")
        if not parsed.path.endswith(route) or parsed.query or parsed.fragment:
            raise ValueError("human-input-source-conflict")
    ordered = media + ui
    basis = {
        "version": 7,
        "publication": publication_identity(record),
        "outcome": {"kind": "human_input_files", "projection": deepcopy(source_basis["projection"]), "request_basis": deepcopy(source_basis)},
        "inputs": deepcopy(inputs),
        "presentation": None,
        "display": None,
        "text_disposition": "human_input",
        **deepcopy(file_plan),
        "parts": ordered,
        "keyboard": controls,
        "human_input_parts": ui,
        "finish_route": True,
    }
    return _safe_json(basis), deepcopy(ordered)


def validate_mixed_reply_basis(item, part=None):
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.file_source import validate_file_parts
    from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope
    from app.telegram_bots.model_source import VerifiedModelReplyBasis
    from app.telegram_bots.producer import OutboxItem, _canonical_primary

    try:
        if type(item) is not OutboxItem or type(item.payload) is not dict or any(key in item.payload for key in ("delivery_kind", "product_basis", "turn_outcome_basis")):
            raise ValueError
        basis = item.payload["reply_basis"]
        if (
            type(basis) is not dict
            or set(basis) != {"version", "publication", "outcome", "inputs", "presentation", "display", "text_disposition", "files", "capacity", "max_bytes", "conversation_url", "parts", "keyboard", "human_input_parts", "finish_route"}
            or type(basis["version"]) is not int
            or basis["version"] != 7
        ):
            raise ValueError
        outcome = basis["outcome"]
        if type(outcome) is not dict or set(outcome) != {"kind", "projection", "request_basis"} or outcome["kind"] != "human_input_files":
            raise ValueError
        output = RunOutputProjectionV2.model_validate(outcome["projection"])
        identity = basis["publication"]
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": True, "run_id": output.output.run_id})
        if _canonical_primary(outcome["projection"]) != _canonical_primary(outcome["request_basis"]["projection"]):
            raise ValueError
        file_plan = {key: basis[key] for key in ("files", "capacity", "max_bytes", "conversation_url")}
        expected, _ = prepare_mixed_reply(record, outcome["request_basis"], parts=basis["human_input_parts"], keyboard=basis["keyboard"], inputs=basis["inputs"], file_plan=file_plan)
        if (
            _canonical_primary(expected) != _canonical_primary(basis)
            or _canonical_primary(item.payload["source"]) != _canonical_primary(outcome)
            or _canonical_primary({"keyboard": item.payload["product_keyboard_basis"]}) != _canonical_primary({"keyboard": basis["keyboard"]})
        ):
            raise ValueError
        if (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.dedupe_key, item.class_) != (
            scope.link_id,
            scope.owner_user_id,
            scope.binding_generation,
            scope.chat_id,
            "reply:" + record.turn_id,
            "reply",
        ) or item.message_thread_id not in (None, scope.message_thread_id):
            raise ValueError
        validate_file_parts(item)
        if part is not None and not any(_canonical_primary(part) == _canonical_primary(value) for value in item.parts):
            raise ValueError
        return VerifiedModelReplyBasis(record, output, None, "human_input_files")
    except Exception:
        raise ValueError("human-input-source-conflict") from None


def human_input_parts(item):
    from app.telegram_bots.human_input_source import validate_human_input_reply_basis
    from app.telegram_bots.producer import _canonical_primary

    validate_human_input_reply_basis(item)
    basis = item.payload["reply_basis"]
    recipes = basis["human_input_parts"] if basis["version"] == 7 else basis["parts"]
    sources = {_canonical_primary(value["source_ref"]) for value in recipes}
    return tuple(value for value in item.parts if value["state"] != "retired" and _canonical_primary(value["source_ref"]) in sources)


def is_human_input_part(item, part):
    from app.telegram_bots.producer import _canonical_primary

    return any(_canonical_primary(value) == _canonical_primary(part) for value in human_input_parts(item))


def human_input_visible(item):
    """Only the exact final question ACK opens interaction, never a file ACK."""
    from app.telegram_bots.producer import _canonical_primary

    owned = human_input_parts(item)
    basis = item.payload["reply_basis"]
    recipes = basis["human_input_parts"] if basis["version"] == 7 else basis["parts"]
    final = _canonical_primary(recipes[-1]["source_ref"])
    visible = [part for part in owned if part["state"] == "sent" and part["message_ids"] and _canonical_primary(part["source_ref"]) == final]
    if not visible:
        return False
    last = visible[-1]
    index = next(index for index, value in enumerate(item.parts) if value["part_id"] == last["part_id"])
    return all(part["state"] in {"sent", "unavailable", "retired"} for part in item.parts[:index])
