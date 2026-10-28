"""Finite V6 custody for matched, complete terminal file inventories.

Inventory grants source identity; HEAD facts and persisted presentation recipes
only describe how the existing delivery worker may present that source.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import PurePosixPath


def qualify_file_output(record, output):
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope, publication_identity

    try:
        if type(record) is not PublicationRecord or type(output) is not RunOutputProjectionV2:
            raise ValueError
        identity = publication_identity(record)
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": record.dispatch_started, "run_id": record.run_id})
        output = RunOutputProjectionV2.model_validate(output.model_dump(mode="python"))
        result, inventory = output.output, output.inventory
        scope, channel = record.scope, result.channel_execution
        if (result.owner_user_id, result.thread_id, result.run_id, channel.publication_id, channel.assistant_id, channel.agent_name) != (
            scope.owner_user_id,
            scope.thread_id,
            record.run_id,
            record.publication_id,
            scope.assistant_id,
            scope.agent_name,
        ):
            raise ValueError
        if result.state != "complete" and not (result.state == "unavailable" and result.reason == "unsupported_output" and result.run_status == "success"):
            raise ValueError
        if inventory.state != "matched" or inventory.document.coverage != "complete" or type(inventory.document.version) is not int or inventory.document.version != 2 or not inventory.document.items:
            raise ValueError
        paths = []
        for item in inventory.document.items:
            if item.kind != "file":
                raise ValueError
            path = item.path
            parsed = PurePosixPath(path)
            if (
                not path.startswith("/mnt/user-data/outputs/")
                or str(parsed) != path
                or "\\" in path
                or any(ord(char) < 32 or ord(char) == 127 for char in path)
                or any(part in {".", ".."} for part in parsed.parts)
                or path in paths
                or not item.surfaces
                or not item.witnesses
            ):
                raise ValueError
            paths.append(path)
        return tuple(paths)
    except Exception:
        raise ValueError("publication-source-conflict") from None


@dataclass(frozen=True)
class PreparedFilePart:
    record: object
    files: tuple[dict, ...]
    caption_html: str | None
    caption_text: str | None
    unavailable: str | None


def media_source(files):
    return {"output_files": [{"path": file["path"], "sha256": None} for file in files]}


def prepare_file_reply(record, output, *, files, capacity, max_bytes, display=None, conversation_url=None):
    from app.telegram_bots.files import plan_output_files
    from app.telegram_bots.inbox import publication_identity
    from app.telegram_bots.model_source import prepare_display_reply
    from app.telegram_bots.producer import freeze_reply_presentation

    paths = qualify_file_output(record, output)
    if type(files) is not list or tuple(file.get("path") for file in files) != paths:
        raise ValueError("publication-source-conflict")
    result = output.output
    frozen = freeze_reply_presentation(result.source.text, text_sha256=result.output_evidence.text_sha256) if result.state == "complete" else None
    answer = "".join(chunk.visible_text for chunk in frozen.chunks) if frozen is not None else ""
    if display is not None and frozen is not None:
        prepare_display_reply(record, output, display=display, _file_output=True)
    if conversation_url is not None:
        from app.telegram_bots.producer import validate_url_button

        validate_url_button({"page": "web.conversation", "url": conversation_url})
        from urllib.parse import quote, urlsplit

        route = "/workspace/" + ("agents/" + quote(record.scope.agent_ref, safe="") + "/" if record.scope.agent_kind == "custom_agent" else "") + "chats/" + quote(record.scope.thread_id, safe="")
        parsed = urlsplit(conversation_url)
        if not parsed.path.endswith(route) or parsed.query or parsed.fragment:
            raise ValueError("publication-source-conflict")
    plan = plan_output_files(files, answer_visible=answer, capacity=capacity, max_bytes=max_bytes)
    parts = []
    anchor = record.origins[0].message_id
    if frozen is not None and not plan.caption:
        if display is not None:
            _, parts = prepare_display_reply(record, output, display=display, _file_output=True)
        else:
            parts = [
                {
                    "kind": "text",
                    "source_ref": {
                        "assistant_message_id": result.output_evidence.assistant_message_id,
                        "thread_id": record.scope.thread_id,
                        "run_id": record.run_id,
                        "range": {"basis": "telegram-visible-v1", "start": chunk.start, "end": chunk.end},
                    },
                    "html": chunk.html,
                    "visible_text": chunk.visible_text,
                    "reply_to_message_id": anchor,
                }
                for chunk in frozen.chunks
            ]
    for template in plan.parts:
        part = {**deepcopy(template), "source_ref": media_source(template["files"]), "reply_to_message_id": anchor}
        if plan.caption and frozen is not None:
            part.update(caption_html="".join(chunk.html for chunk in frozen.chunks), caption_text=answer)
        parts.append(part)
    basis = {
        "version": 6,
        "publication": publication_identity(record),
        "outcome": {"kind": "file_outputs", "projection": output.model_dump(mode="json")},
        "inputs": None,
        "presentation": frozen.to_json() if frozen is not None else None,
        "text_disposition": "verified_text" if frozen is not None else "none",
        "files": deepcopy(files),
        "capacity": capacity,
        "max_bytes": max_bytes,
        "display": display,
        "conversation_url": conversation_url,
        "parts": deepcopy(parts),
        "finish_route": True,
    }
    return basis, parts


_RECEIPTS = {
    "part_id",
    "presentation_version",
    "ordinal",
    "state",
    "message_ids",
    "sent_at",
    "dispatched_at",
    "protect_content",
    "sent_protect_content",
    "plain_fallback",
    "invalid_token_verification_pending",
    "callback_group_id",
    "unavailable_reason",
    "file_failure_key",
}


def clean_part(part):
    value = {key: deepcopy(child) for key, child in part.items() if key not in _RECEIPTS}
    if "output_files" in value.get("source_ref", {}):
        source = value["source_ref"]
        if set(source) != {"output_files"} or type(source["output_files"]) is not list:
            raise ValueError
        for file in source["output_files"]:
            if type(file) is not dict or set(file) != {"path", "sha256"}:
                raise ValueError
            digest = file["sha256"]
            if digest is not None and (type(digest) is not str or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest)):
                raise ValueError
            file["sha256"] = None
    return value


def validate_file_reply_basis(item, part=None):
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope, validate_publication_inputs
    from app.telegram_bots.model_source import VerifiedModelReplyBasis
    from app.telegram_bots.producer import OutboxItem, _canonical_primary

    try:
        if type(item) is not OutboxItem or type(item.payload) is not dict or "delivery_kind" in item.payload:
            raise ValueError
        basis = item.payload["reply_basis"]
        if (
            type(basis) is not dict
            or set(basis) != {"version", "publication", "outcome", "inputs", "presentation", "text_disposition", "files", "capacity", "max_bytes", "display", "conversation_url", "parts", "finish_route"}
            or type(basis["version"]) is not int
            or basis["version"] != 6
            or basis["finish_route"] is not True
        ):
            raise ValueError
        outcome = basis["outcome"]
        if type(outcome) is not dict or set(outcome) != {"kind", "projection"} or outcome["kind"] != "file_outputs":
            raise ValueError
        output = RunOutputProjectionV2.model_validate(outcome["projection"])
        identity = basis["publication"]
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": True, "run_id": output.output.run_id})
        if (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.dedupe_key, item.class_) != (
            scope.link_id,
            scope.owner_user_id,
            scope.binding_generation,
            scope.chat_id,
            "reply:" + record.turn_id,
            "reply",
        ) or item.message_thread_id not in (None, scope.message_thread_id):
            raise ValueError
        validate_publication_inputs(record, basis["inputs"])
        expected, _ = prepare_file_reply(record, output, files=basis["files"], capacity=basis["capacity"], max_bytes=basis["max_bytes"], display=basis["display"], conversation_url=basis["conversation_url"])
        expected["inputs"] = basis["inputs"]
        # Reply anchor is chosen from actual arrival ordering by the writer.
        anchor = basis["parts"][0]["reply_to_message_id"]
        if type(anchor) is not int or anchor not in {origin.message_id for origin in record.origins}:
            raise ValueError
        expected["parts"] = [{**value, "reply_to_message_id": anchor} for value in expected["parts"]]
        if _canonical_primary(basis) != _canonical_primary(expected) or _canonical_primary(item.payload["source"]) != _canonical_primary(outcome):
            raise ValueError
        validate_file_parts(item)
        if part is not None and (not any(_canonical_primary(part) == _canonical_primary(value) for value in item.parts)):
            raise ValueError
        return VerifiedModelReplyBasis(record, output, None, "file_outputs")
    except Exception:
        raise ValueError("publication-source-conflict") from None


def validate_file_parts(item):
    """Replay the finite server-written presentation ledger, retaining old ACKs."""
    from app.telegram_bots.producer import _canonical_primary

    payload = item.payload
    initial = payload["file_initial_ids"]
    if type(initial) is not list or len(initial) != len(payload["reply_basis"]["parts"]) or len(set(initial)) != len(initial):
        raise ValueError
    version = payload.get("presentation_version")
    if type(version) is not int or version < 0:
        raise ValueError
    for value in item.parts:
        if type(value) is not dict or type(value.get("part_id")) is not str or len(value["part_id"]) != 32 or any(char not in "0123456789abcdef" for char in value["part_id"]):
            raise ValueError
        if (
            type(value.get("presentation_version")) is not int
            or value["presentation_version"] != version
            or type(value.get("ordinal")) is not int
            or value["ordinal"] < 0
            or value.get("state") not in {"pending", "sending", "sent", "unavailable", "retired"}
        ):
            raise ValueError
        if type(value.get("message_ids")) is not list or any(type(identity) is not int or identity <= 0 for identity in value["message_ids"]) or value["state"] == "sent" and not value["message_ids"]:
            raise ValueError
        if value["state"] == "sent":
            expected_count = len(value["files"]) if value["kind"] == "album" else 1
            if len(value["message_ids"]) != expected_count or len(set(value["message_ids"])) != expected_count:
                raise ValueError
        if any(type(value[key]) is not bool for key in ("protect_content", "sent_protect_content", "plain_fallback", "url_button_fallback") if key in value):
            raise ValueError
        if "file_failure_key" in value and (value["state"] != "unavailable" or value["file_failure_key"] not in {"file.out_failed", "file.too_large_out"}):
            raise ValueError
    rows = {value["part_id"]: value for value in item.parts}
    if len(rows) != len(item.parts):
        raise ValueError
    expected = dict(zip(initial, payload["reply_basis"]["parts"], strict=True))
    for event in payload.get("file_events", []):
        if type(event) is not dict or set(event) != {"old", "new", "reason"} or event["old"] not in expected or type(event["new"]) is not list:
            raise ValueError
        parent = rows[event["old"]]
        new = [rows[identity] for identity in event["new"]]
        if not new or len(set(event["new"])) != len(new) or any(identity in expected for identity in event["new"]):
            raise ValueError
        if event["reason"] in {"photo-classification", "photo-rejected", "capacity"}:
            if parent["state"] != "retired":
                raise ValueError
            if _canonical_primary({"files": [file for value in new for file in value["files"]]}) != _canonical_primary({"files": parent["files"]}) or any(value["kind"] not in {"document", "photo", "album"} for value in new):
                raise ValueError
            for value in new:
                old_hashes = {file["path"]: file["sha256"] for file in parent["source_ref"]["output_files"]}
                if any(file["sha256"] != old_hashes[file["path"]] for file in value["source_ref"]["output_files"]):
                    raise ValueError
                recipe = {**clean_part(parent), "kind": value["kind"], "files": value["files"], "source_ref": media_source(value["files"])}
                if _canonical_primary(clean_part(value)) != _canonical_primary(recipe):
                    raise ValueError
        elif event["reason"] in {"edit-anchor-missing", "protection", "topic-missing"}:
            if parent["state"] != "retired" or len(new) != 1:
                raise ValueError
            recipe = {key: child for key, child in clean_part(parent).items() if key not in {"target_message_id", "target_protect_content", "shown_message_id"}}
            if recipe["kind"] == "edit":
                recipe["kind"] = "text"
            if _canonical_primary(clean_part(new[0])) != _canonical_primary(recipe):
                raise ValueError
            if parent.get("files") and new[0]["source_ref"] != parent["source_ref"]:
                raise ValueError
        elif event["reason"] == "file-retry":
            if parent["state"] != "unavailable" or len(new) != 1:
                raise ValueError
            value = new[0]
            if value["kind"] != "document" or len(value["files"]) != 1 or value["files"][0]["path"] not in {file["path"] for file in parent["files"]}:
                raise ValueError
            from app.telegram_bots.files import plan_output_files

            retry_plan = plan_output_files(value["files"], answer_visible="", capacity=payload["reply_basis"]["capacity"], max_bytes=payload["reply_basis"]["max_bytes"])
            recipe = {key: child for key, child in clean_part(parent).items() if key not in {"caption_html", "caption_text"}}
            recipe.update(kind="document", files=value["files"], unavailable=retry_plan.parts[0]["unavailable"], source_ref=media_source(value["files"]))
            if _canonical_primary(clean_part(value)) != _canonical_primary(recipe):
                raise ValueError
            old_hashes = {file["path"]: file["sha256"] for file in parent["source_ref"]["output_files"]}
            if any(file["sha256"] != old_hashes[file["path"]] and old_hashes[file["path"]] is not None for file in value["source_ref"]["output_files"]):
                raise ValueError
        elif event["reason"] == "unavailable":
            if parent["state"] != "unavailable":
                raise ValueError
            allowed = unavailable_parts(parent, payload["reply_basis"])
            if _canonical_primary({"parts": [clean_part(value) for value in new]}) != _canonical_primary({"parts": allowed}):
                raise ValueError
        else:
            raise ValueError
        for value in new:
            expected[value["part_id"]] = clean_part(value)
    if set(rows) != set(expected):
        raise ValueError
    for identity, recipe in expected.items():
        if _canonical_primary(clean_part(rows[identity])) != _canonical_primary(recipe):
            raise ValueError


def unavailable_parts(parent, basis):
    """The caption's verified answer is kept even if its file cannot be sent."""
    result = []
    if parent.get("caption_html"):
        output = basis["outcome"]["projection"]["output"]
        from app.telegram_bots.producer import FrozenReplyPresentation

        presentation = FrozenReplyPresentation.from_json(basis["presentation"])
        for chunk in presentation.chunks:
            result.append(
                {
                    "kind": "text",
                    "source_ref": {
                        "assistant_message_id": output["output_evidence"]["assistant_message_id"],
                        "thread_id": output["thread_id"],
                        "run_id": output["run_id"],
                        "range": {"basis": "telegram-visible-v1", "start": chunk.start, "end": chunk.end},
                    },
                    "html": chunk.html,
                    "visible_text": chunk.visible_text,
                    "reply_to_message_id": parent["reply_to_message_id"],
                }
            )
    for file in parent["files"]:
        key = parent.get("file_failure_key", "file.too_large_out" if parent["unavailable"] == "oversize" else "file.out_failed")
        if key not in {"file.out_failed", "file.too_large_out"}:
            raise ValueError
        result.append(
            {
                "kind": "text",
                "source_ref": {"key": key, "values": {"file": file["filename"]}},
                "reply_to_message_id": parent["reply_to_message_id"],
                "file_retry_source": {"thread_id": basis["publication"]["scope"]["thread_id"], "path": file["path"]} if key == "file.out_failed" else None,
                "conversation_url": basis["conversation_url"],
                **({"stored_keyboard": True} if key == "file.out_failed" or basis["conversation_url"] is not None else {}),
            }
        )
    return result


async def plan_file_reply(source, record, link, output, *, guard, shown=None):
    """The original read lifetime covers every HEAD and the final source seal."""
    from dataclasses import asdict

    from app.telegram_bots.inbox import publication_identity
    from app.telegram_bots.links import prepare_publication_guard
    from app.telegram_bots.stopped_preview import check_shown_preview, take_shown_preview

    try:
        paths = qualify_file_output(record, output)
    except ValueError:
        return None
    if source._outbox is None or source._http_owner is None:
        return None
    identity = publication_identity(record)
    if shown is None and source._file_output_active is not None and source._file_output_active(record, link):
        return None

    async def check():
        await prepare_publication_guard(guard)
        if shown is not None:
            check_shown_preview(shown, publication_identity=identity, run_id=record.run_id)
        elif source._file_output_active is not None and source._file_output_active(record, link):
            raise ValueError("publication-source-conflict")

    await check()
    existing = await source._outbox.read_display_primary(record, output=output, guard=guard)
    if existing is not None:
        return existing
    files = []
    for path in paths:
        await check()
        try:
            metadata = await source._http_owner.describe_artifact(record.scope.owner_user_id, record.scope.thread_id, path, before_dispatch=check)
            if metadata.path != path:
                raise ValueError("publication-source-conflict")
            files.append({"path": path, "filename": metadata.filename, "size": metadata.size, "mime_type": metadata.mime_type, "state": "ready"})
        except Exception:
            # Failure to read metadata cannot erase an independently verified
            # text answer. Lifetime failure is rechecked before any writer.
            await check()
            files.append({"path": path, "filename": PurePosixPath(path).name, "size": None, "mime_type": "application/octet-stream", "state": "unavailable"})
    display = None
    if shown is not None:
        saved = take_shown_preview(shown, publication_identity=identity, run_id=record.run_id)
        display = {"kind": "model_text", "stop_id": None, "segments": [asdict(segment) for segment in saved.segments], "waiting": False}
    import asyncio

    from app.telegram_bots.pages import page_address
    from deerflow.utils.file_io import await_drained

    conversation_url = await await_drained(asyncio.to_thread(page_address, "web.conversation", link, thread_id=record.scope.thread_id))
    basis, parts = prepare_file_reply(record, output, files=files, capacity=source._file_capacity, max_bytes=source._file_max_bytes, display=display, conversation_url=conversation_url)

    class FileGuard:
        captured_link = guard.captured_link

        async def prepare(self):
            await check()

        async def check(self, writer):
            result = await guard.check(writer)
            if shown is not None:
                check_shown_preview(shown, publication_identity=identity, run_id=record.run_id)
            elif source._file_output_active is not None and source._file_output_active(record, link):
                raise ValueError("publication-source-conflict")
            return result

    return await source._outbox.plan_file_publication_reply(record, basis=basis, parts=parts, guard=FileGuard(), shown=shown)


def file_warning_keyboard(part):
    from app.telegram_bots.callbacks import canonical_keyboard

    source = part.get("file_retry_source")
    key = part.get("source_ref", {}).get("key")
    if part.get("stored_keyboard") is not True or key not in {"file.out_failed", "file.too_large_out"}:
        raise ValueError("publication-source-conflict")
    keyboard = []
    if key == "file.out_failed":
        if type(source) is not dict or set(source) != {"thread_id", "path"}:
            raise ValueError("publication-source-conflict")
        keyboard.append([{"label": "label.send_again", "kind": "file_retry", "target_kind": "thread", "target_id": source["thread_id"], "payload": source}])
    if part.get("conversation_url") is not None:
        keyboard.append([{"label": "label.open_conversation", "url": part["conversation_url"]}])
    return canonical_keyboard(keyboard)
