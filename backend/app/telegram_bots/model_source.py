"""Strict model parts borrow the original publication's one HTTP owner."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

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


@dataclass(frozen=True)
class VerifiedModelReplyBasis:
    record: PublicationRecord
    output: RunOutputProjectionV2
    presentation: FrozenReplyPresentation


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
        if result.state != "complete" or inventory.state != "matched" or inventory.document.coverage != "complete" or inventory.document.items != ():
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
    def __init__(self, *, read_output: ReadPublicationOutput):
        if not callable(read_output):
            raise TypeError("Model source requires its publication reader") from None
        self._read_output = read_output

    async def read_part(self, captured: SendingPart) -> PreparedModelPart:
        from app.telegram_bots.producer import DeliveryClaimRetired

        try:
            private, proof = await await_drained(asyncio.to_thread(_prepare_part, captured))
            fresh = await self._read_output(proof.record, private.link)

            def compare():
                from app.telegram_bots.producer import _canonical_primary

                if fresh is None:
                    raise ValueError
                candidate, _ = prepare_verified_reply(proof.record, fresh, reply_to_message_id=private.part["reply_to_message_id"])
                if _canonical_primary(candidate) != _canonical_primary(private.item.payload["reply_basis"]):
                    raise ValueError
                return PreparedModelPart(private.part["html"], private.part["visible_text"])

            return await await_drained(asyncio.to_thread(compare))
        except Exception:
            raise DeliveryClaimRetired() from None
