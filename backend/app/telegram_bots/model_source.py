"""Strict model parts borrow the original publication's one HTTP owner."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from deerflow.runtime.runs.output_inventory import content_items, offered_pages
from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.inbox import PublicationRecord
    from app.telegram_bots.links import BotLink
    from app.telegram_bots.producer import FrozenReplyPresentation, OutboxItem, SendingPart


class ReadPublicationOutput(Protocol):
    async def __call__(self, record: PublicationRecord, current_link: BotLink) -> RunOutputProjectionV2 | None: ...


@dataclass(frozen=True)
class PreparedModelPart:
    html: str
    plain_text: str
    human_input: bool = False
    page_keys: tuple[str, ...] = ()


@dataclass(frozen=True)
class VerifiedModelReplyBasis:
    record: PublicationRecord
    output: RunOutputProjectionV2
    presentation: FrozenReplyPresentation | None
    kind: str = "model_text"
    stop_id: str | None = None


def prepare_verified_reply(record, output, *, reply_to_message_id=None):
    """Pure closed preparation; a DTO is consistency evidence, never admission."""
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope
    from app.telegram_bots.producer import _publication_identity, _safe_json, freeze_reply_presentation

    try:
        if type(record) is not PublicationRecord or type(output) is not RunOutputProjectionV2:
            raise ValueError
        identity = _publication_identity(record)
        # Revalidate dataclass construction/copy bypasses and strict nested data.
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": record.dispatch_started, "run_id": record.run_id})
        output = RunOutputProjectionV2.model_validate(output.model_dump(mode="python"))
        result, inventory = output.output, output.inventory
        if result.state != "complete" or inventory.state != "matched" or inventory.document.coverage != "complete" or content_items(inventory.document) != ():
            raise ValueError
        channel = result.channel_execution
        if (result.owner_user_id, result.thread_id, channel.publication_id, channel.assistant_id, channel.agent_name) != (scope.owner_user_id, scope.thread_id, record.publication_id, scope.assistant_id, scope.agent_name):
            raise ValueError
        if record.run_id is not None and record.run_id != result.run_id:
            raise ValueError
        anchor = record.origins[0].message_id if reply_to_message_id is None else reply_to_message_id
        if type(anchor) is not int or anchor not in {origin.message_id for origin in record.origins}:
            raise ValueError
        presentation = freeze_reply_presentation(result.source.text, text_sha256=result.output_evidence.text_sha256)
        parts = [
            {
                "kind": "text",
                "source_ref": {"assistant_message_id": result.output_evidence.assistant_message_id, "thread_id": result.thread_id, "run_id": result.run_id, "range": {"basis": "telegram-visible-v1", "start": chunk.start, "end": chunk.end}},
                "html": chunk.html,
                "visible_text": chunk.visible_text,
                "reply_to_message_id": anchor,
            }
            for chunk in presentation.chunks
        ]
        basis = _safe_json({"version": 2, "publication": identity, "outcome": {"kind": "model_text", "projection": output.model_dump(mode="json")}, "presentation": presentation.to_json(), "parts": parts, "finish_route": True})
        return basis, parts
    except Exception:
        raise ValueError("publication-source-conflict") from None


def validate_model_reply_basis(item: OutboxItem, part: dict | None = None) -> VerifiedModelReplyBasis:
    """Validate stored data only; the caller must still obtain fresh authority."""
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope
    from app.telegram_bots.producer import FrozenReplyPresentation, OutboxItem, _canonical_primary

    try:
        if type(item) is OutboxItem and type(item.payload) is dict and type(item.payload.get("reply_basis")) is dict and type(item.payload["reply_basis"].get("version")) is int and item.payload["reply_basis"]["version"] == 7:
            from app.telegram_bots.mixed_source import validate_mixed_reply_basis

            return validate_mixed_reply_basis(item, part)
        if type(item) is OutboxItem and type(item.payload) is dict and type(item.payload.get("reply_basis")) is dict and type(item.payload["reply_basis"].get("version")) is int and item.payload["reply_basis"]["version"] == 6:
            from app.telegram_bots.file_source import validate_file_reply_basis

            return validate_file_reply_basis(item, part)
        if type(item) is OutboxItem and type(item.payload) is dict and type(item.payload.get("reply_basis")) is dict and type(item.payload["reply_basis"].get("version")) is int and item.payload["reply_basis"]["version"] == 5:
            from app.telegram_bots.human_input_source import validate_human_input_reply_basis

            return validate_human_input_reply_basis(item, part)
        if type(item) is OutboxItem and type(item.payload) is dict and type(item.payload.get("reply_basis")) is dict and type(item.payload["reply_basis"].get("version")) is int and item.payload["reply_basis"]["version"] == 4:
            return _validate_display_reply_basis(item, part)
        if type(item) is not OutboxItem or type(item.payload) is not dict or "delivery_kind" in item.payload:
            raise ValueError
        basis = item.payload["reply_basis"]
        if type(basis) is not dict or set(basis) != {"version", "publication", "outcome", "presentation", "parts", "finish_route"} or type(basis["version"]) is not int or basis["version"] != 2 or basis["finish_route"] is not True:
            raise ValueError
        outcome = basis["outcome"]
        if type(outcome) is not dict or set(outcome) != {"kind", "projection"} or outcome["kind"] != "model_text":
            raise ValueError
        output = RunOutputProjectionV2.model_validate(outcome["projection"])
        identity = basis["publication"]
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": True, "run_id": output.output.run_id})
        if (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.dedupe_key, item.class_) != (scope.link_id, scope.owner_user_id, scope.binding_generation, scope.chat_id, "reply:" + record.turn_id, "reply"):
            raise ValueError
        if item.message_thread_id not in (scope.message_thread_id, None):
            raise ValueError
        anchor = basis["parts"][0]["reply_to_message_id"]
        expected, _ = prepare_verified_reply(record, output, reply_to_message_id=anchor)
        if _canonical_primary(basis) != _canonical_primary(expected) or _canonical_primary(item.payload["source"]) != _canonical_primary(outcome):
            raise ValueError
        # Versioned renderer verification compares only; stored parts are never
        # replaced by a new rendering or split when a retry is sent.
        presentation = FrozenReplyPresentation.from_json(basis["presentation"])
        allowed = {
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
            "url_button_fallback",
            "invalid_token_verification_pending",
            "callback_group_id",
            "unavailable_reason",
        }
        version = item.payload.get("presentation_version")
        if type(version) is not int or version < 0:
            raise ValueError

        def initial_part(value):
            if type(value) is not dict or value.get("callback_group_id") is not None:
                raise ValueError
            if any(type(value[key]) is not bool for key in ("plain_fallback", "protect_content", "sent_protect_content") if key in value):
                raise ValueError
            if "presentation_version" in value and (type(value["presentation_version"]) is not int or value["presentation_version"] < 0):
                raise ValueError
            if "ordinal" in value and (type(value["ordinal"]) is not int or value["ordinal"] < 0):
                raise ValueError
            return {key: child for key, child in value.items() if key not in allowed}

        active = [value for value in item.parts if value.get("presentation_version", version) == version]
        if _canonical_primary({"parts": [initial_part(value) for value in active]}) != _canonical_primary({"parts": basis["parts"]}):
            raise ValueError
        selected = [part] if part is not None else item.parts
        if not selected:
            raise ValueError
        for value in selected:
            clean = initial_part(value)
            if _canonical_primary(clean) not in {_canonical_primary(initial) for initial in basis["parts"]}:
                raise ValueError
        return VerifiedModelReplyBasis(record, output, presentation)
    except Exception:
        raise ValueError("publication-source-conflict") from None


def prepare_display_reply(record, output, *, display, reply_to_message_id=None, _file_output=False):
    """Recompute a closed source-slice presentation; no host authority is minted."""
    from html import escape

    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.inbox import PublicationRecord, publication_identity
    from app.telegram_bots.markdown import to_html
    from app.telegram_bots.producer import _safe_json
    from app.telegram_bots.split import visible_text
    from app.telegram_bots.stopped_preview import StoppedSegment, preview_source_slices
    from deerflow.runtime.runs.preview import text_digest

    try:
        if type(record) is not PublicationRecord or type(output) is not RunOutputProjectionV2 or type(display) is not dict or set(display) != {"kind", "stop_id", "segments", "waiting"}:
            raise ValueError
        output = RunOutputProjectionV2.model_validate(output.model_dump(mode="python"))
        anchor = record.origins[0].message_id if reply_to_message_id is None else reply_to_message_id
        if type(anchor) is not int or anchor not in {origin.message_id for origin in record.origins}:
            raise ValueError
        result = output.output
        if (
            (result.owner_user_id, result.thread_id, result.run_id, result.channel_execution.publication_id, result.channel_execution.assistant_id, result.channel_execution.agent_name)
            != (record.scope.owner_user_id, record.scope.thread_id, record.run_id, record.publication_id, record.scope.assistant_id, record.scope.agent_name)
            or type(display["segments"]) is not list
            or type(display["waiting"]) is not bool
        ):
            raise ValueError
        segments = tuple(StoppedSegment(**value) for value in display["segments"] if type(value) is dict)
        if len(segments) != len(display["segments"]) or len({value.part_id for value in segments}) != len(segments):
            raise ValueError
        segments = tuple(sorted(segments, key=lambda value: value.part_index))
        if len({value.part_index for value in segments}) != len(segments):
            raise ValueError
        stopped = display["kind"] == "stopped_preview"
        if stopped:
            if result.state != "unavailable" or result.reason != "cancelled" or type(display["stop_id"]) is not str or not display["stop_id"] or not display["stop_id"].isprintable():
                raise ValueError
        else:
            if display["kind"] != "model_text" or display["stop_id"] is not None or display["waiting"]:
                raise ValueError
            # Retain the exact complete-empty/no-admitted-view eligibility rules.
            if _file_output:
                from app.telegram_bots.file_source import qualify_file_output

                qualify_file_output(record, output)
            else:
                prepare_verified_reply(record, output)
        slices = []
        if stopped:
            for segment in segments:
                slices.append((segment.source_start, segment.source_end, segment.text, segment, False))
        else:
            text = result.source.text
            eligible = [segment for segment in segments if segment.message_id == result.output_evidence.assistant_message_id]
            cursor = 0
            for segment in eligible:
                if segment.source_start != cursor or text[segment.source_start : segment.source_end] != segment.text:
                    raise ValueError
                cursor = segment.source_end
            # Only the latest acknowledged edit is extended. Earlier acknowledged
            # prefix parts are durable receipts, not work to resend on relocation.
            retained = eligible[:-1] if eligible and eligible[-1].mode == "edit" else []
            for segment in retained:
                slices.append((segment.source_start, segment.source_end, segment.text, segment, True))
            start = retained[-1].source_end if retained else 0
            latest = eligible[-1] if eligible and eligible[-1].mode == "edit" else None
            for index, (begin, end, raw) in enumerate(preview_source_slices(text, start=start)):
                slices.append((begin, end, raw, latest if index == 0 else None, False))
        parts = []
        for index, (begin, end, raw, segment, retained) in enumerate(slices):
            if not raw:
                continue
            rendered = None if stopped else to_html(raw, partial=True)
            if rendered is not None and rendered.wide_tables:
                raise ValueError
            html = escape(raw) if stopped else rendered.html
            source = {"preview_sha256": text_digest(raw), "thread_id": record.scope.thread_id, "run_id": record.run_id, "range": {"basis": "telegram-source-slices-v1", "start": begin, "end": end}, "slice": index}
            value = {"kind": "edit" if segment is not None and segment.mode == "edit" else "text", "source_ref": source, "html": html, "visible_text": visible_text(html), "reply_to_message_id": anchor}
            if segment is not None and segment.mode == "edit":
                value.update(target_message_id=segment.anchor_message_id, target_protect_content=False)
            if retained:
                value["shown_message_id"] = segment.anchor_message_id
            parts.append(value)
        if stopped:
            parts.append({"kind": "text", "source_ref": {"key": "stop.answer", "values": {"written": bool(parts), "waiting": display["waiting"]}}, "reply_to_message_id": anchor})
        outcome = {"kind": display["kind"], "projection": output.model_dump(mode="json")}
        basis = {"version": 4, "publication": publication_identity(record), "outcome": outcome, "presentation": None, "display": display, "parts": parts, "finish_route": True}
        return _safe_json(basis), _safe_json({"parts": parts})["parts"]
    except Exception:
        raise ValueError("publication-source-conflict") from None


def _validate_display_reply_basis(item, part):
    from app.gateway.run_output import RunOutputProjectionV2
    from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope
    from app.telegram_bots.producer import _canonical_primary

    basis = item.payload["reply_basis"]
    if type(item.binding_generation) is not int or type(item.chat_id) is not int or "turn_outcome_basis" in item.payload or "input_owner_callback_id" in item.payload:
        raise ValueError
    if set(basis) != {"version", "publication", "outcome", "presentation", "display", "parts", "finish_route"} or basis["finish_route"] is not True or basis["presentation"] is not None or "delivery_kind" in item.payload:
        raise ValueError
    outcome = basis["outcome"]
    if type(outcome) is not dict or set(outcome) != {"kind", "projection"} or outcome["kind"] != basis["display"]["kind"]:
        raise ValueError
    output = RunOutputProjectionV2.model_validate(outcome["projection"])
    identity = basis["publication"]
    scope = PublicationScope(**identity["scope"])
    record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": True, "run_id": output.output.run_id})
    if (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.dedupe_key, item.class_) != (scope.link_id, scope.owner_user_id, scope.binding_generation, scope.chat_id, "reply:" + record.turn_id, "reply"):
        raise ValueError
    if item.message_thread_id is not None and (type(item.message_thread_id) is not int or item.message_thread_id != scope.message_thread_id):
        raise ValueError
    expected, _ = prepare_display_reply(record, output, display=basis["display"], reply_to_message_id=basis["parts"][0]["reply_to_message_id"])
    if _canonical_primary(expected) != _canonical_primary(basis) or _canonical_primary(item.payload["source"]) != _canonical_primary(outcome):
        raise ValueError
    receipts = {
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
        "url_button_fallback",
        "invalid_token_verification_pending",
        "callback_group_id",
        "unavailable_reason",
    }

    def clean(value):
        if type(value) is not dict or value.get("callback_group_id") is not None or any(type(value[key]) is not bool for key in ("plain_fallback", "protect_content", "sent_protect_content") if key in value):
            raise ValueError
        for key in ("presentation_version", "ordinal"):
            if key in value and (type(value[key]) is not int or value[key] < 0):
                raise ValueError
        return {key: child for key, child in value.items() if key not in receipts}

    allowed = {_canonical_primary(initial) for initial in basis["parts"]}
    if item.payload.get("replacement_reason") in {"edit-anchor-missing", "protection", "topic-missing"}:
        for initial in basis["parts"]:
            relocated = {key: value for key, value in initial.items() if key not in {"target_message_id", "target_protect_content", "shown_message_id"}}
            if relocated["kind"] == "edit":
                relocated["kind"] = "text"
            allowed.add(_canonical_primary(relocated))
    for value in item.parts + ([part] if part is not None else []):
        if _canonical_primary(clean(value)) not in allowed:
            raise ValueError
    version = item.payload.get("presentation_version")
    if not item.parts or type(version) is not int or version < 0:
        raise ValueError
    active = [value for value in item.parts if value.get("presentation_version") == version]
    active_sources = [_canonical_primary(value["source_ref"]) for value in active]
    if len(active_sources) != len(set(active_sources)):
        raise ValueError
    acknowledged_sources = {_canonical_primary(value["source_ref"]) for value in item.parts if value.get("state") == "sent"}
    required = {_canonical_primary(value["source_ref"]) for value in basis["parts"]}
    if required != set(active_sources) | acknowledged_sources:
        raise ValueError
    return VerifiedModelReplyBasis(record, output, None, outcome["kind"], basis["display"]["stop_id"])


def _page_keys(item, part, output):
    """A turn's page offers go under the last part that carries its answer."""
    inventory = output.inventory
    if not item.dedupe_key.startswith("reply:") or inventory.state != "matched" or inventory.document.coverage != "complete":
        return ()
    answering = [value for value in item.parts if value["state"] != "retired" and value["kind"] in {"text", "edit"} and value["source_ref"].get("key") in (None, "human_input.presentation")]
    return offered_pages(inventory.document) if answering and answering[-1]["part_id"] == part["part_id"] else ()


def _prepare_part(captured):
    from app.telegram_bots.links import BotLink
    from app.telegram_bots.producer import SendingPart

    if type(captured) is not SendingPart or type(captured.link) is not BotLink:
        raise ValueError("publication-source-conflict") from None
    private = deepcopy(captured)
    proof = validate_model_reply_basis(private.item, private.part)
    scope, link = proof.record.scope, private.link
    if (
        link.id != scope.link_id
        or any(getattr(link, field) != getattr(scope, field) for field in ("owner_user_id", "agent_kind", "agent_ref", "bot_id", "connection_id", "telegram_user_id", "chat_id", "binding_generation"))
        or private.token_revision != link.token_revision
    ):
        raise ValueError("publication-source-conflict") from None
    return private, proof


class ModelSource:
    def __init__(self, *, read_output: ReadPublicationOutput, human_input_source=None, outbox=None, http_owner=None, file_capacity=64 * 1024 * 1024, file_max_bytes=50_000_000, file_output_active=None):
        if not callable(read_output):
            raise TypeError("Model source requires its publication reader") from None
        self._read_output = read_output
        self._human_input_source = human_input_source
        self._outbox, self._http_owner = outbox, http_owner
        self._file_capacity, self._file_max_bytes = file_capacity, file_max_bytes
        self._file_output_active = file_output_active

    async def plan_file_reply(self, record, link, output, *, guard, shown=None):
        from app.telegram_bots.file_source import plan_file_reply

        return await plan_file_reply(self, record, link, output, guard=guard, shown=shown)

    async def retry_file_callback(self, link, row, *, callbacks, guard):
        from pathlib import PurePosixPath

        from app.telegram_bots.links import prepare_publication_guard

        saved = await self._outbox.read_file_retry(link, row.id, callbacks=callbacks, guard=guard)
        if saved is None:
            return False
        record, file = saved

        async def check():
            await prepare_publication_guard(guard)

        await check()
        try:
            metadata = await self._http_owner.describe_artifact(link.owner_user_id, record.scope.thread_id, file["path"], before_dispatch=check)
            if metadata.path != file["path"]:
                raise ValueError
            descriptor = {"path": metadata.path, "filename": metadata.filename, "size": metadata.size, "mime_type": metadata.mime_type, "state": "ready"}
        except Exception:
            await check()
            descriptor = {"path": file["path"], "filename": PurePosixPath(file["path"]).name, "size": None, "mime_type": "application/octet-stream", "state": "unavailable"}
        return await self._outbox.retry_file_callback(link, row.id, metadata=descriptor, callbacks=callbacks, guard=guard)

    async def read_file_part(self, captured):
        from app.telegram_bots.file_source import PreparedFilePart
        from app.telegram_bots.producer import DeliveryClaimRetired, _canonical_primary

        try:
            private, proof = await await_drained(asyncio.to_thread(_prepare_part, captured))
            if proof.kind not in {"file_outputs", "human_input_files"} or private.part["kind"] not in {"document", "photo", "album"}:
                raise ValueError
            fresh = await self._read_output(proof.record, private.link)
            if fresh is None or _canonical_primary(fresh.model_dump(mode="json")) != _canonical_primary(proof.output.model_dump(mode="json")):
                raise ValueError
            return PreparedFilePart(proof.record, tuple(deepcopy(private.part["files"])), private.part.get("caption_html"), private.part.get("caption_text"), private.part["unavailable"])
        except Exception:
            raise DeliveryClaimRetired() from None

    async def read_part(self, captured: SendingPart) -> PreparedModelPart:
        from app.telegram_bots.producer import DeliveryClaimRetired

        try:
            private, proof = await await_drained(asyncio.to_thread(_prepare_part, captured))
            if proof.kind == "human_input_files" and private.part["source_ref"].get("key") != "human_input.presentation":
                # File warnings retain their own source-verified controls;
                # they do not acquire question/form authority or currency.
                from app.telegram_bots.producer import _canonical_primary

                fresh = await self._read_output(proof.record, private.link)
                if fresh is None or _canonical_primary(fresh.model_dump(mode="json")) != _canonical_primary(proof.output.model_dump(mode="json")) or private.part["source_ref"].get("key") not in {"file.out_failed", "file.too_large_out"}:
                    raise ValueError
                return None
            if proof.kind in {"human_input", "human_input_files"}:
                from app.telegram_bots.human_input_source import check_qualified_request
                from app.telegram_bots.producer import _canonical_primary

                if self._human_input_source is None:
                    raise ValueError
                qualified = await self._human_input_source.read_request(proof.record, private.link)
                if qualified is None or _canonical_primary(check_qualified_request(qualified, proof.record)) != _canonical_primary(private.item.payload["reply_basis"]["outcome"]["request_basis"]):
                    raise ValueError
                return PreparedModelPart(private.part["html"], private.part["visible_text"], True, _page_keys(private.item, private.part, proof.output))
            fresh = await self._read_output(proof.record, private.link)

            def compare():
                from app.telegram_bots.producer import _canonical_primary

                if fresh is None:
                    raise ValueError
                if proof.kind == "file_outputs":
                    if _canonical_primary(fresh.model_dump(mode="json")) != _canonical_primary(proof.output.model_dump(mode="json")) or private.part["kind"] not in {"text", "edit"}:
                        raise ValueError
                    if "key" in private.part["source_ref"]:
                        return None
                    return PreparedModelPart(private.part["html"], private.part["visible_text"], page_keys=_page_keys(private.item, private.part, proof.output))
                if private.item.payload["reply_basis"]["version"] == 4:
                    candidate, _ = prepare_display_reply(proof.record, fresh, display=private.item.payload["reply_basis"]["display"], reply_to_message_id=private.item.payload["reply_basis"]["parts"][0]["reply_to_message_id"])
                else:
                    candidate, _ = prepare_verified_reply(proof.record, fresh, reply_to_message_id=private.part["reply_to_message_id"])
                if _canonical_primary(candidate) != _canonical_primary(private.item.payload["reply_basis"]):
                    raise ValueError
                if proof.kind == "stopped_preview" and private.part["source_ref"].get("key") == "stop.answer":
                    return None
                return PreparedModelPart(private.part["html"], private.part["visible_text"], page_keys=_page_keys(private.item, private.part, proof.output))

            return await await_drained(asyncio.to_thread(compare))
        except Exception:
            raise DeliveryClaimRetired() from None
