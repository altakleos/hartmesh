"""Reply-only durable delivery. No models, runs, transport or startup are owned here.

Writers lock the binding, then its delivery, inbox identities and callback groups.
No SQL transaction crosses a transport/authority call. Every worker write is
fenced by process, admission and binding; time never transfers a live claim.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import random
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Literal, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

from sqlalchemy import or_, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError

from app.telegram_bots.adapter import TOKEN_BODY
from app.telegram_bots.api_logging import log_call_failure
from app.telegram_bots.config import TelegramBotsConfig
from app.telegram_bots.links import BotLink
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow
from deerflow.utils.file_io import await_drained

if TYPE_CHECKING:
    from app.gateway.run_output import RunOutputProjection, RunOutputProjectionV2
    from app.telegram_bots.inbox import PublicationRecord, RestartClosure
    from app.telegram_bots.links import PublicationGuard

OUTBOX_SEND_CONCURRENCY = 8
DELIVERY_RETRY_FIRST_SECONDS = 5
DELIVERY_RETRY_CAP_SECONDS = 300
ESSENTIAL_DELIVERY_CLASSES = frozenset({"reply", "question", "result", "interrupt", "progress", "service", "reminder"})
_KINDS = frozenset({"text", "document", "photo", "album", "edit", "card"})
_FINAL_PARTS = frozenset({"sent", "unavailable", "retired"})
_STATUS_KEYS = frozenset({"access.agent_off", "agent.paused", "agent.retired", "unavailable.fix", "unavailable.off", "unavailable.paused", "unavailable.moved", "unavailable.files_unchecked", "refused.interrupted", "custom_agent.deleted"})
_URL_SOURCES = {
    "file.too_large_in": ("web.conversation", "label.open_conversation"),
    "web.memory": ("web.memory", "label.open_memory"),
    "custom_agent.deleted": ("web.telegram", "label.open_telegram"),
    "chat.restarted": ("web.conversation", "label.open_conversation"),
}
_TRANSPORT_RECEIPTS = frozenset({"sent_at", "dispatched_at", "unavailable_reason", "invalid_token_verification_pending", "url_button_fallback", "plain_fallback"})
_PART_IDENTITY = frozenset({"part_id", "presentation_version", "ordinal", "state", "message_ids"})
_SKIP_REASONS = frozenset({"no-access", "superseded", "link-not-connected", "disabled", "policy-off", "expired"})
_LIVE_CLAIMS: set[tuple[int, str, str]] = set()
_TOKEN = re.compile(TOKEN_BODY)
_FORBIDDEN_KEYS = frozenset({"token", "bot_token", "access_token", "raw_update", "update", "audio", "audio_bytes", "download_url", "temporary_url", "bytes"})


def validate_url_button(value: dict) -> dict:
    """Accept a closed server-owned navigation control, without callback authority."""
    if not isinstance(value, dict) or set(value) != {"page", "url"} or not isinstance(value["page"], str) or value["page"] not in {entry[0] for entry in _URL_SOURCES.values()} or not isinstance(value["url"], str):
        raise ValueError("invalid-url-button")
    address = value["url"]
    if any(ord(character) <= 32 or ord(character) == 127 for character in address) or "\\" in address:
        raise ValueError("invalid-url-button")
    try:
        parsed = urlsplit(address)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username is not None or parsed.password is not None:
            raise ValueError("invalid-url-button")
        _ = parsed.port
    except ValueError:
        raise ValueError("invalid-url-button") from None
    return {"page": value["page"], "url": address}


def url_button_label(source: dict, button: dict) -> str:
    """Derive the label from an exact fixed source/page pair at plan and send."""
    button = validate_url_button(button)
    if not isinstance(source, dict) or set(source) != {"key", "values"} or not isinstance(source["key"], str) or not isinstance(source["values"], dict):
        raise ValueError("invalid-url-button")
    expected = _URL_SOURCES.get(source["key"])
    if expected is None or expected[0] != button["page"]:
        raise ValueError("invalid-url-button")
    return expected[1]


def _status_parts(parts: list[dict]) -> list[dict]:
    """Discard caller receipts before saving the immutable fixed-notice basis."""
    if not isinstance(parts, list) or not parts:
        raise ValueError("invalid-status-notice")
    result = []
    for supplied in parts:
        if not isinstance(supplied, dict):
            raise ValueError("invalid-status-notice")
        part = _safe_json({key: value for key, value in supplied.items() if key not in _TRANSPORT_RECEIPTS | _PART_IDENTITY})
        source = part.get("source_ref", {})
        if (
            part.get("kind") != "text"
            or not isinstance(source, dict)
            or set(source) != {"key", "values"}
            or not isinstance(source["key"], str)
            or source["key"] not in _STATUS_KEYS
            or not isinstance(source["values"], dict)
            or set(part) - {"kind", "source_ref", "silent", "url_button"}
            or "silent" in part
            and type(part["silent"]) is not bool
            or any(not isinstance(value, (str, bool, int, type(None))) for value in source["values"].values())
        ):
            raise ValueError("invalid-status-notice")
        if "url_button" in part:
            url_button_label(source, part["url_button"])
        result.append(part)
    return result


def _safe_json(value: dict) -> dict:
    def valid(item, depth=0):
        if depth > 16:
            return False
        if item is None or type(item) in {bool, int}:
            return True
        if type(item) is float:
            return math.isfinite(item)
        if isinstance(item, str):
            return _TOKEN.search(item) is None
        if isinstance(item, list):
            return all(valid(child, depth + 1) for child in item)
        return isinstance(item, dict) and all(isinstance(key, str) and key not in _FORBIDDEN_KEYS and valid(key, depth + 1) and valid(child, depth + 1) for key, child in item.items())

    if not isinstance(value, dict) or not valid(value):
        raise ValueError("unsafe-delivery-source")
    try:
        return json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError, OverflowError):
        raise ValueError("unsafe-delivery-source") from None


def _now() -> datetime:
    return datetime.now(UTC)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


@dataclass(frozen=True)
class OutboxItem:
    """Detached delivery basis, safe to pass across an awaited policy check."""

    id: str
    link_id: str
    owner_user_id: str
    binding_generation: int
    dedupe_key: str
    class_: str
    payload: dict
    parts: list[dict]
    chat_id: int | None
    message_thread_id: int | None
    attempts: int = 0
    silent: bool = False


_TURN_OUTCOME_KEYS = frozenset(
    {
        "error.generic",
        "busy.cancelled",
        "busy.gave_up",
        "chat.restarted",
        "chat.files_full",
        "refused.interrupted",
        "refused.restarting",
        "refused.files_busy",
        "start.ws_failed",
        "busy.queued",
        "busy.thread",
        "busy.starting_elsewhere",
        "busy.two_conversations",
    }
)
INPUT_OUTCOME_KEYS = (_TURN_OUTCOME_KEYS - {"busy.queued", "busy.thread", "busy.starting_elsewhere", "busy.two_conversations"}) | {"custom_agent.deleted", "busy.followup_full"}


@dataclass(frozen=True)
class PublicationOutcomeBasis:
    record: PublicationRecord
    run_id: str | None
    kind: str
    source_ref: dict
    stop_id: str | None = None


def _prepare_publication_outcome(record, *, run, creation_refusal, source_ref):
    from app.channels.execution import ChannelExecutionScope
    from app.telegram_bots.errors import classify_creation_refusal, classify_run

    if (run is None) == (creation_refusal is None) or type(source_ref) is not dict or set(source_ref) != {"key", "values"}:
        raise ValueError("publication-outcome-conflict") from None
    scope = record.scope
    execution = ChannelExecutionScope(record.publication_id, scope.owner_user_id, scope.connection_id, str(scope.chat_id), scope.thread_id, scope.endpoint_id, scope.assistant_id, scope.agent_name)
    classified = classify_run(run, execution) if run is not None else classify_creation_refusal(creation_refusal, execution)
    if classified is None or source_ref["key"] != classified.text_key and not (classified.retry_later and source_ref["key"] in {"busy.gave_up", "busy.cancelled"}) or source_ref["key"] in {"busy.queued", "busy.thread"}:
        raise ValueError("publication-outcome-conflict") from None
    proof = {"kind": "run", "row": dict(run.row)} if run is not None else {"kind": "creation_refusal", "scope": asdict(execution)}
    return _safe_json({"version": 1, "publication": _publication_identity(record), "proof": proof, "source_ref": source_ref, "finish_route": True})


def validate_publication_outcome(item: OutboxItem) -> PublicationOutcomeBasis:
    """Read only the closed server-authored fixed outcome, never model wording."""
    from types import MappingProxyType

    from app.channels.execution import ChannelExecutionScope
    from app.channels.execution_client import ValidatedRun
    from app.gateway.channel_execution import receipt_from_run_kwargs
    from app.telegram_bots.errors import classify_run
    from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope

    try:
        basis = item.payload["turn_outcome_basis"]
        if type(basis) is not dict or set(basis) != {"version", "publication", "proof", "source_ref", "finish_route"} or type(basis["version"]) is not int or basis["version"] != 1 or basis["finish_route"] is not True:
            raise ValueError
        identity, proof, source = basis["publication"], basis["proof"], basis["source_ref"]
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(
            **{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": proof.get("kind") != "before_dispatch_stop", "run_id": proof.get("row", {}).get("run_id")}
        )
        if (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.dedupe_key, item.class_) != (scope.link_id, scope.owner_user_id, scope.binding_generation, scope.chat_id, "reply:" + record.turn_id, "reply"):
            raise ValueError
        execution = ChannelExecutionScope(record.publication_id, scope.owner_user_id, scope.connection_id, str(scope.chat_id), scope.thread_id, scope.endpoint_id, scope.assistant_id, scope.agent_name)
        if proof.get("kind") == "run" and set(proof) == {"kind", "row"}:
            run = ValidatedRun(MappingProxyType(proof["row"]), receipt_from_run_kwargs(proof["row"].get("kwargs")))
            classified = classify_run(run, execution)
            if classified is None or source["key"] != classified.text_key and not (classified.retry_later and source["key"] in {"busy.gave_up", "busy.cancelled"}):
                raise ValueError
        elif proof.get("kind") == "creation_refusal" and set(proof) == {"kind", "scope"}:
            if _canonical_primary(proof["scope"]) != _canonical_primary(asdict(execution)) or source["key"] not in {"busy.gave_up", "busy.cancelled"}:
                raise ValueError
        elif proof.get("kind") == "before_dispatch_stop" and set(proof) == {"kind", "stop_id"}:
            if type(proof["stop_id"]) is not str or not proof["stop_id"] or len(proof["stop_id"]) > 128 or not proof["stop_id"].isprintable() or source != {"key": "busy.cancelled", "values": {}}:
                raise ValueError
        else:
            raise ValueError
        if type(source) is not dict or set(source) != {"key", "values"} or source["key"] not in _TURN_OUTCOME_KEYS or source["key"] in {"busy.queued", "busy.thread"}:
            raise ValueError
        if _canonical_primary({"parts": item.payload["product_basis"]["parts"]}) != _canonical_primary({"parts": [{"kind": "text", "source_ref": source}]}):
            raise ValueError
        if "delivery_kind" in item.payload or "reply_basis" in item.payload or type(item.binding_generation) is not int or type(item.chat_id) is not int:
            raise ValueError
        return PublicationOutcomeBasis(record, record.run_id, proof["kind"], source, proof.get("stop_id"))
    except Exception:
        raise ValueError("publication-outcome-conflict") from None


def _item(row: TelegramOutboxRow) -> OutboxItem:
    return OutboxItem(**{name: deepcopy(getattr(row, name)) for name in OutboxItem.__dataclass_fields__})


def _same_fixed_source(left, right, depth: int = 0) -> bool:
    """Compare the small fixed-notice echo without JSON bool/int coercion."""
    if depth > 16 or type(left) is not type(right):
        return False
    if type(left) is dict:
        return left.keys() == right.keys() and all(_same_fixed_source(left[key], right[key], depth + 1) for key in left)
    return type(left) in {str, int, bool, type(None)} and left == right


def uses_model_source(item: OutboxItem) -> bool:
    """A fixed-looking part cannot downgrade a saved model primary's source."""
    source = item.payload.get("source")
    if type(source) is dict and source.get("kind") == "model_text":
        return True
    if type(source) is dict and source.get("kind") == "restart_before_dispatch":
        try:
            validate_restart_reply_basis(item)
            return False
        except ValueError:
            return True
    if "reply_basis" not in item.payload:
        return False
    basis = item.payload["reply_basis"]
    if type(basis) is not dict or type(basis.get("version")) is not int or basis["version"] != 1:
        return True
    outcome = basis.get("outcome")
    if type(outcome) is not dict or outcome.get("kind") not in {"sealed_failed", "sealed_cancelled"}:
        return True
    if not _same_fixed_source(source, outcome):
        return True
    projection = outcome.get("projection")
    reason = "failed" if outcome["kind"] == "sealed_failed" else "cancelled"
    return not (
        type(projection) is dict
        and type(projection.get("version")) is int
        and projection["version"] == 1
        and projection.get("state") == "unavailable"
        and projection.get("reason") == reason
        and type(projection.get("output_evidence")) is dict
        and projection["output_evidence"].get("kind") == "unavailable"
        and projection["output_evidence"].get("reason") == reason
    )


@dataclass(frozen=True)
class RestartReplyBasis:
    record: PublicationRecord
    closure: RestartClosure


def validate_restart_reply_basis(item: OutboxItem, part: dict | None = None) -> RestartReplyBasis:
    """Closed fixed basis only; no run proof or status-repair privilege."""
    from app.telegram_bots.inbox import PublicationOrigin, PublicationRecord, PublicationScope, RestartClosure, publication_identity

    try:
        if type(item) is not OutboxItem or type(item.payload) is not dict or "delivery_kind" in item.payload:
            raise ValueError
        basis = item.payload["reply_basis"]
        if (
            type(basis) is not dict
            or set(basis) != {"version", "publication", "outcome", "presentation", "parts", "finish_route"}
            or type(basis["version"]) is not int
            or basis["version"] != 3
            or basis["presentation"] is not None
            or basis["finish_route"] is not True
        ):
            raise ValueError
        identity = basis["publication"]
        scope = PublicationScope(**identity["scope"])
        record = PublicationRecord(**{**identity, "scope": scope, "origins": tuple(PublicationOrigin(**origin) for origin in identity["origins"]), "dispatch_started": False, "run_id": None})
        if _canonical_primary(identity) != _canonical_primary(publication_identity(record)):
            raise ValueError
        outcome = basis["outcome"]
        if (
            type(outcome) is not dict
            or set(outcome) != {"kind", "closure", "source_ref"}
            or outcome["kind"] != "restart_before_dispatch"
            or _canonical_primary(outcome["source_ref"]) != _canonical_primary({"key": "chat.restarted", "values": {}})
        ):
            raise ValueError
        fact = RestartClosure.from_json(outcome["closure"])
        if (fact.publication_id, fact.input_version, fact.input_sha256) != (record.publication_id, record.input_version, record.input_sha256):
            raise ValueError
        if _canonical_primary(item.payload["source"]) != _canonical_primary(outcome):
            raise ValueError
        if (
            (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.dedupe_key, item.class_) != (scope.link_id, scope.owner_user_id, scope.binding_generation, scope.chat_id, "reply:" + record.turn_id, "reply")
            or item.message_thread_id is not None
            and (type(item.message_thread_id) is not int or item.message_thread_id <= 0)
            or item.message_thread_id not in (scope.message_thread_id, None)
        ):
            raise ValueError
        planned = basis["parts"]
        if type(planned) is not list or len(planned) != 1:
            raise ValueError
        initial = planned[0]
        if (
            type(initial) is not dict
            or set(initial) - {"kind", "source_ref", "reply_to_message_id", "url_button"}
            or initial.get("kind") != "text"
            or _canonical_primary(initial.get("source_ref")) != _canonical_primary(outcome["source_ref"])
            or type(initial.get("reply_to_message_id")) is not int
            or initial["reply_to_message_id"] not in {origin.message_id for origin in record.origins}
        ):
            raise ValueError
        if "url_button" in initial:
            url_button_label(initial["source_ref"], initial["url_button"])
        version = item.payload.get("presentation_version")
        if type(version) is not int or version < 0 or type(item.parts) is not list:
            raise ValueError
        allowed = _PART_IDENTITY | _TRANSPORT_RECEIPTS | {"protect_content", "sent_protect_content", "callback_group_id"}

        def clean(value):
            if type(value) is not dict or value.get("callback_group_id") is not None:
                raise ValueError
            if any(type(value[key]) is not bool for key in ("protect_content", "sent_protect_content", "plain_fallback", "url_button_fallback") if key in value):
                raise ValueError
            if any(type(value[key]) is not int or value[key] < 0 for key in ("presentation_version", "ordinal") if key in value):
                raise ValueError
            return {key: child for key, child in value.items() if key not in allowed}

        active = [value for value in item.parts if type(value) is dict and value.get("presentation_version") == version]
        if len(active) != 1 or _canonical_primary(clean(active[0])) != _canonical_primary(initial):
            raise ValueError
        selected = [part] if part is not None else item.parts
        if not selected or any(_canonical_primary(clean(value)) != _canonical_primary(initial) for value in selected):
            raise ValueError
        return RestartReplyBasis(record, fact)
    except Exception:
        raise ValueError("publication-source-conflict") from None


@dataclass(frozen=True)
class SendingPart:
    """One detached, currently admitted raw transport attempt."""

    link: BotLink
    item: OutboxItem
    part: dict
    claim_owner: str
    claim_id: str
    token_revision: int


class DeliveryClaimRetired(Exception):
    """The transport admission no longer authorizes a request; no payload fault."""


class PreviewExpired(DeliveryClaimRetired):
    """An actual host view ended; only its strict temporary plan may retire."""


@dataclass(frozen=True)
class DeliveryEligibility:
    decision: str
    reason: str | None = None
    protect_content: bool = False

    @classmethod
    def allow(cls, *, protect_content: bool = False) -> DeliveryEligibility:
        return cls("allow", protect_content=protect_content)

    @classmethod
    def wait(cls) -> DeliveryEligibility:
        return cls("wait")

    @classmethod
    def skip(cls, reason: str) -> DeliveryEligibility:
        if reason not in _SKIP_REASONS:
            raise ValueError("invalid-delivery-skip")
        return cls("skip", reason)


@dataclass(frozen=True)
class PartReceipt:
    message_ids: tuple[int, ...]
    protect_content: bool = False


class DeliverPart(Protocol):
    async def __call__(self, *, link_id: str, outbox_id: str, part_id: str, claim_owner: str, claim_id: str) -> list[int] | PartReceipt: ...


class DeliveryRetry(Exception):
    """A server wait, including long limiter refusals; never an attempt."""

    def __init__(self, retry_after: float | timedelta) -> None:
        super().__init__("delivery-retry")
        self.retry_after = retry_after.total_seconds() if isinstance(retry_after, timedelta) else max(0, retry_after)


class TransientDeliveryError(Exception):
    pass


class PermanentDeliveryError(Exception):
    def __init__(self, reason: str = "invalid-payload") -> None:
        # Never persist transport descriptions, credentials or payloads.
        super().__init__("invalid-payload")
        self.reason = "invalid-payload"


class SourceUnavailable(Exception):
    """An authorized file attempt needs an explicit saved product-text fallback.

    The file layer owns byte reads/hashes. This seam only accepts a structured
    fixed source reference and never substitutes other bytes or a fresh answer.
    """

    def __init__(self, *, source_ref: dict, reason: str = "source-unavailable") -> None:
        if reason not in {"source-unavailable", "source-changed"} or set(source_ref) != {"key", "values"} or source_ref["key"] not in {"file.out_failed", "file.too_large_out"} or not isinstance(source_ref["values"], dict):
            raise ValueError("invalid-unavailable-source-notice")
        if set(source_ref["values"]) - {"file", "product"} or any(not isinstance(value, str) or len(value) > 4096 for value in source_ref["values"].values()):
            raise ValueError("invalid-unavailable-source-notice")
        super().__init__(reason)
        self.source_ref, self.reason = _safe_json(source_ref), reason


def _presentation_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _digest(value: object) -> bool:
    return type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None


@dataclass(frozen=True)
class FrozenReplyChunk:
    html: str
    visible_text: str
    visible_sha256: str
    start: int
    end: int

    def __post_init__(self) -> None:
        from app.telegram_bots.split import utf16_units, visible_text

        try:
            if (
                type(self.html) is not str
                or type(self.visible_text) is not str
                or not self.visible_text
                or type(self.start) is not int
                or type(self.end) is not int
                or self.start < 0
                or self.end - self.start != len(self.visible_text)
                or not _digest(self.visible_sha256)
                or _presentation_digest(self.visible_text) != self.visible_sha256
                or visible_text(self.html) != self.visible_text
                or utf16_units(self.visible_text) > 4096
            ):
                raise ValueError
        except Exception:
            raise ValueError("invalid-publication-presentation") from None


@dataclass(frozen=True)
class FrozenReplyPresentation:
    renderer_schema: Literal["telegram-html-v1"]
    text_sha256: str
    visible_sha256: str
    chunks: tuple[FrozenReplyChunk, ...]

    def __post_init__(self) -> None:
        try:
            if self.renderer_schema != "telegram-html-v1" or type(self.renderer_schema) is not str or not _digest(self.text_sha256) or not _digest(self.visible_sha256) or type(self.chunks) is not tuple or not self.chunks:
                raise ValueError
            cursor = 0
            for chunk in self.chunks:
                if type(chunk) is not FrozenReplyChunk:
                    raise ValueError
                FrozenReplyChunk(**asdict(chunk))
                if chunk.start != cursor:
                    raise ValueError
                cursor = chunk.end
            if _presentation_digest("".join(chunk.visible_text for chunk in self.chunks)) != self.visible_sha256:
                raise ValueError
        except Exception:
            raise ValueError("invalid-publication-presentation") from None

    def to_json(self) -> dict:
        return {"renderer_schema": self.renderer_schema, "text_sha256": self.text_sha256, "visible_sha256": self.visible_sha256, "chunks": [asdict(chunk) for chunk in self.chunks]}

    @classmethod
    def from_json(cls, value: object) -> FrozenReplyPresentation:
        try:
            if type(value) is not dict or set(value) != {"renderer_schema", "text_sha256", "visible_sha256", "chunks"} or type(value["chunks"]) is not list:
                raise ValueError
            chunks = []
            for chunk in value["chunks"]:
                if type(chunk) is not dict or set(chunk) != {"html", "visible_text", "visible_sha256", "start", "end"}:
                    raise ValueError
                chunks.append(FrozenReplyChunk(**chunk))
            return cls(value["renderer_schema"], value["text_sha256"], value["visible_sha256"], tuple(chunks))
        except Exception:
            raise ValueError("invalid-publication-presentation") from None


def freeze_reply_presentation(text: str, *, text_sha256: str) -> FrozenReplyPresentation:
    """Freeze the existing renderer's visible basis, retaining the original digest."""
    from app.telegram_bots.markdown import to_html
    from app.telegram_bots.split import split_html, visible_text

    try:
        if type(text) is not str or not text or not _digest(text_sha256) or _presentation_digest(text) != text_sha256:
            raise ValueError
        rendered = to_html(text)
        if rendered.wide_tables:
            raise ValueError
        chunks = []
        cursor = 0
        for html in split_html(rendered.html):
            visible = visible_text(html)
            chunks.append(FrozenReplyChunk(html, visible, _presentation_digest(visible), cursor, cursor + len(visible)))
            cursor += len(visible)
        if "".join(chunk.visible_text for chunk in chunks) != rendered.plain:
            raise ValueError
        return FrozenReplyPresentation("telegram-html-v1", text_sha256, _presentation_digest(rendered.plain), tuple(chunks))
    except Exception:
        raise ValueError("invalid-publication-presentation") from None


def _detached_output(value: RunOutputProjection) -> RunOutputProjection:
    from app.gateway.run_output import RunOutputProjection

    try:
        if type(value) is not RunOutputProjection:
            raise ValueError
        return RunOutputProjection.model_validate(value.model_dump(mode="python"))
    except Exception:
        raise ValueError("publication-source-conflict") from None


@dataclass(frozen=True)
class PrimaryTurnNotice:
    kind: Literal["sealed_failed", "sealed_cancelled"]
    output: RunOutputProjection

    def __post_init__(self) -> None:
        if type(self.kind) is not str or self.kind not in {"sealed_failed", "sealed_cancelled"}:
            raise ValueError("publication-source-conflict")
        output = _detached_output(self.output)
        reason = "failed" if self.kind == "sealed_failed" else "cancelled"
        if output.state != "unavailable" or output.reason != reason or output.output_evidence.kind != "unavailable":
            raise ValueError("publication-source-conflict")
        object.__setattr__(self, "output", output)


def _publication_identity(record: PublicationRecord) -> dict:
    from app.telegram_bots.inbox import PublicationRecord

    if type(record) is not PublicationRecord:
        raise ValueError("publication-source-conflict")
    result = asdict(record)
    result.pop("dispatch_started")
    result.pop("run_id")
    if result.get("owner_callback_id") is None:
        result.pop("owner_callback_id", None)
    # Origins are immutable tuples in the host DTO and JSON arrays on disk.
    result["origins"] = [asdict(origin) for origin in record.origins]
    return _safe_json(result)


def _canonical_primary(value: dict) -> str:
    return json.dumps(_safe_json(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _guard_publication_link(record, guard):
    try:
        link = guard.captured_link
        if (
            type(link) is not BotLink
            or link.id != record.scope.link_id
            or any(getattr(link, name) != getattr(record.scope, name) for name in ("owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation"))
        ):
            raise ValueError
        return link
    except Exception:
        raise ValueError("publication-source-conflict") from None


def _prepare_publication_reply(record: PublicationRecord, output: RunOutputProjection | None, notice: PrimaryTurnNotice | None) -> tuple[dict, list[dict]]:
    identity = _publication_identity(record)
    scope = identity["scope"]
    if notice is None:
        output = _detached_output(output)
        if output.state != "complete":
            raise ValueError("publication-source-conflict")
        presentation = freeze_reply_presentation(output.source.text, text_sha256=output.output_evidence.text_sha256)
        parts = [
            {
                "kind": "text",
                "source_ref": {"assistant_message_id": output.output_evidence.assistant_message_id, "thread_id": output.thread_id, "run_id": output.run_id, "range": {"basis": "telegram-visible-v1", "start": chunk.start, "end": chunk.end}},
                "html": chunk.html,
                "visible_text": chunk.visible_text,
            }
            for chunk in presentation.chunks
        ]
        outcome = {"kind": "model_text", "projection": output.model_dump(mode="json", exclude={"source"})}
        frozen = presentation.to_json()
    else:
        if type(notice) is not PrimaryTurnNotice:
            raise ValueError("publication-source-conflict")
        notice = PrimaryTurnNotice(notice.kind, notice.output)
        output = notice.output
        key = {"sealed_failed": "error.generic", "sealed_cancelled": "run.cancelled"}[notice.kind]
        parts = [{"kind": "text", "source_ref": {"key": key, "values": {}}}]
        outcome = {"kind": notice.kind, "projection": output.model_dump(mode="json"), "source_ref": parts[0]["source_ref"]}
        frozen = None
    if output is not None and (output.owner_user_id, output.thread_id, output.channel_execution.publication_id, output.channel_execution.assistant_id, output.channel_execution.agent_name) != (
        scope["owner_user_id"],
        scope["thread_id"],
        identity["publication_id"],
        scope["assistant_id"],
        scope["agent_name"],
    ):
        raise ValueError("publication-source-conflict")
    basis = _safe_json({"version": 1, "publication": identity, "outcome": outcome, "presentation": frozen, "parts": parts, "finish_route": True})
    return basis, parts


class Outbox:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession], *, inbox=None, config: TelegramBotsConfig | None = None, clock: Callable[[], datetime] = _now) -> None:
        from app.telegram_bots.inbox import InboxRepository

        self.sessions = session_factory
        self.inbox = inbox if inbox is not None else InboxRepository(session_factory)
        self.config = config or TelegramBotsConfig()
        self.clock = clock

    async def get(self, link, outbox_id, *, session=None):
        async def read(reader):
            row = await reader.get(TelegramOutboxRow, outbox_id, populate_existing=True)
            if row is None or (row.link_id, row.owner_user_id, row.chat_id, row.binding_generation) != (link.id, link.owner_user_id, link.chat_id, link.binding_generation):
                return None
            return _item(row)

        if session is not None:
            return await read(session)
        async with self.sessions() as reader:
            return await read(reader)

    async def list_human_inputs(self, link, *, topic, session=None):
        topic = None if topic in (None, 1) else topic

        async def read(reader):
            active_control = (
                select(TelegramCallbackRow.id)
                .where(
                    TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == TelegramOutboxRow.id,
                    TelegramCallbackRow.link_id == link.id,
                    TelegramCallbackRow.chat_id == link.chat_id,
                    TelegramCallbackRow.message_thread_id == topic,
                    TelegramCallbackRow.binding_generation == link.binding_generation,
                    TelegramCallbackRow.consumed_at.is_(None),
                    TelegramCallbackRow.kind != "file_retry",
                )
                .exists()
            )
            rows = list(
                await reader.scalars(
                    select(TelegramOutboxRow)
                    .where(
                        TelegramOutboxRow.link_id == link.id,
                        TelegramOutboxRow.owner_user_id == link.owner_user_id,
                        TelegramOutboxRow.chat_id == link.chat_id,
                        TelegramOutboxRow.binding_generation == link.binding_generation,
                        TelegramOutboxRow.message_thread_id == topic,
                        TelegramOutboxRow.payload["reply_basis"]["version"].as_integer().in_([5, 7]),
                        active_control,
                    )
                    .order_by(TelegramOutboxRow.created_at.desc(), TelegramOutboxRow.id.desc())
                    .limit(32)
                )
            )
            return tuple(_item(row) for row in rows)

        if session is not None:
            return await read(session)
        async with self.sessions() as reader:
            return await read(reader)

    async def plan_human_input_reply(self, record, source, *, parts, keyboard, callbacks, guard, session=None, file_plan=None):
        return await self._plan_human_input(record, source, parts=parts, keyboard=keyboard, callbacks=callbacks, guard=guard, step_id=None, session=session, file_plan=file_plan)

    async def plan_human_input_step(self, record, source, *, step_id, parts, keyboard, callbacks, guard, session=None):
        return await self._plan_human_input(record, source, parts=parts, keyboard=keyboard, callbacks=callbacks, guard=guard, step_id=step_id, session=session)

    async def _plan_human_input(self, record, source, *, parts, keyboard, callbacks, guard, step_id, session, file_plan=None):
        from app.telegram_bots.human_input_source import check_qualified_request, human_input_step_key, prepare_human_input_reply, validate_human_input_reply_basis
        from app.telegram_bots.links import prepare_publication_guard

        private_source = check_qualified_request(source, record)
        mixed = private_source["version"] == 2 and step_id is None
        if mixed != (file_plan is not None):
            raise ValueError("human-input-source-conflict")
        record = source.record
        link = _guard_publication_link(record, guard)
        parts, keyboard = deepcopy(parts), deepcopy(keyboard)
        dedupe = "reply:" + record.turn_id if step_id is None else human_input_step_key(record, step_id)

        async def check(writer):
            check_qualified_request(source, record)
            await source._guard.check(writer)
            result = await guard.check(writer)
            check_qualified_request(source, record)
            return result

        async def save():
            if session is None:
                await prepare_publication_guard(guard)
                await prepare_publication_guard(source._guard)
            async with self._writer(link.id, session=session) as (writer, _):
                await check(writer)
                parent = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id).with_for_update().execution_options(populate_existing=True))
                if parent is None and step_id is None:
                    fresh, _rows = await self.inbox.lock_publication(record, session=writer)
                    inputs = await self.inbox.snapshot_publication_inputs(fresh, session=writer)
                else:
                    if parent is None:
                        raise ValueError("human-input-source-conflict") from None
                    previous = await await_drained(asyncio.to_thread(validate_human_input_reply_basis, _item(parent)))
                    if _canonical_primary(_publication_identity(previous.record)) != _canonical_primary(_publication_identity(record)) or _canonical_primary(parent.payload["reply_basis"]["outcome"]["request_basis"]) != _canonical_primary(
                        private_source
                    ):
                        raise ValueError("human-input-source-conflict") from None
                    if private_source["version"] == 2 and step_id is not None:
                        from app.telegram_bots.mixed_source import human_input_visible, validate_mixed_reply_basis

                        await await_drained(asyncio.to_thread(validate_mixed_reply_basis, _item(parent)))
                        if not await await_drained(asyncio.to_thread(human_input_visible, _item(parent))):
                            raise ValueError("human-input-source-conflict")
                    fresh, inputs = record, deepcopy(parent.payload["reply_basis"]["inputs"])
                if mixed:
                    from app.telegram_bots.mixed_source import prepare_mixed_reply

                    basis, prepared = await await_drained(asyncio.to_thread(prepare_mixed_reply, fresh, private_source, parts=parts, keyboard=keyboard, inputs=inputs, file_plan=file_plan))
                else:
                    basis, prepared = await await_drained(asyncio.to_thread(prepare_human_input_reply, fresh, private_source, parts=parts, keyboard=keyboard, inputs=inputs, step_id=step_id))
                existing = (
                    parent
                    if step_id is None
                    else await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == dedupe).with_for_update().execution_options(populate_existing=True))
                )
                if existing is not None:
                    if _canonical_primary(existing.payload.get("reply_basis")) != _canonical_primary(basis):
                        raise ValueError("human-input-source-conflict") from None
                    identity = existing.id
                else:
                    identity, created = await self._add(
                        link,
                        dedupe_key=dedupe,
                        class_="reply",
                        payload={"source": basis["outcome"], "reply_basis": basis, "product_keyboard_basis": basis["keyboard"], "parts": prepared},
                        session=writer,
                        message_thread_id=record.scope.message_thread_id,
                    )
                    if not created:
                        raise ValueError("human-input-source-conflict") from None
                    if mixed:
                        saved = await writer.get(TelegramOutboxRow, identity)
                        saved.payload = {**saved.payload, "file_initial_ids": [value["part_id"] for value in saved.parts], "file_events": []}
                    if basis["keyboard"]:
                        group = await callbacks.write_group(
                            link,
                            basis["keyboard"],
                            anchor_outbox_id=identity,
                            presentation_version=0,
                            chat_id=link.chat_id,
                            message_id=prepared[-1].get("target_message_id") if prepared[-1]["kind"] == "edit" else None,
                            message_thread_id=record.scope.message_thread_id,
                            session=writer,
                        )
                        saved = await writer.get(TelegramOutboxRow, identity)
                        changed = deepcopy(saved.parts)
                        changed[-1]["callback_group_id"] = group.group_id
                        saved.parts = changed
                if step_id is None and existing is None:
                    await self.inbox.seal_publication(fresh, outbox_id=identity, assistant_message_id=None, session=writer)
                await writer.flush()
                await check(writer)
                return identity

        return await await_drained(save())

    @asynccontextmanager
    async def _writer(self, link_id: str, *, session: AsyncSession | None = None) -> AsyncIterator[tuple[AsyncSession, TelegramBotLinkRow | None]]:
        if session is not None:
            if not session.in_transaction():
                raise ValueError("caller-owned-transaction-required")
            if session.get_bind().dialect.name == "sqlite":
                # sqlite3 legacy mode can leave session.begin() without a native
                # transaction until its first write. Reserve that caller's
                # writer before reading authority, without another checkout or
                # commit. An already reserved writer simply retains its lock.
                await session.execute(update(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link_id).values(id=TelegramBotLinkRow.id))
            link = await session.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link_id).with_for_update().execution_options(populate_existing=True))
            yield session, link
            return
        async with self.sessions() as owned, owned.begin():
            if owned.get_bind().dialect.name == "sqlite":
                await owned.execute(text("BEGIN IMMEDIATE"))
            link = await owned.scalar(select(TelegramBotLinkRow).where(TelegramBotLinkRow.id == link_id).with_for_update().execution_options(populate_existing=True))
            yield owned, link

    @staticmethod
    def _binding(row: TelegramBotLinkRow | None, link: BotLink) -> bool:
        return row is not None and row.owner_user_id == link.owner_user_id and row.binding_generation == link.binding_generation and row.chat_id == link.chat_id

    @staticmethod
    def _parts(parts: list[dict], version: int) -> list[dict]:
        if not isinstance(parts, list) or not parts:
            raise ValueError("empty-delivery-plan")
        frozen = []
        for index, supplied in enumerate(parts):
            if index < len(parts) - 1 and supplied.get("callback_group_id"):
                raise ValueError("invalid-delivery-controls")
            if supplied.get("kind") not in _KINDS or not isinstance(supplied.get("source_ref"), dict) or not supplied["source_ref"]:
                raise ValueError("invalid-delivery-part")
            part = _safe_json(supplied)
            for receipt_field in ("sent_at", "dispatched_at", "unavailable_reason", "invalid_token_verification_pending", "url_button_fallback"):
                part.pop(receipt_field, None)
            source = part["source_ref"]
            if set(source) == {"key", "values"}:
                if not isinstance(source["key"], str) or not isinstance(source["values"], dict):
                    raise ValueError("invalid-delivery-source")
            elif "assistant_message_id" in source:
                if set(source) - {"assistant_message_id", "thread_id", "run_id", "range", "start", "end"}:
                    raise ValueError("invalid-delivery-source")
            elif set(source) == {"output_files"}:
                if part["kind"] not in {"document", "photo", "album"} or type(source["output_files"]) is not list or not source["output_files"]:
                    raise ValueError("invalid-delivery-source")
                for file in source["output_files"]:
                    if type(file) is not dict or set(file) != {"path", "sha256"} or type(file["path"]) is not str or not file["path"].startswith("/mnt/user-data/outputs/") or file["sha256"] is not None and not _digest(file["sha256"]):
                        raise ValueError("invalid-delivery-source")
            elif "preview_sha256" in source:
                if set(source) != {"preview_sha256", "thread_id", "run_id", "range", "slice"}:
                    raise ValueError("invalid-delivery-source")
            elif not {"space_id", "path"} <= set(source) or set(source) - {"space_id", "path", "sha256"}:
                raise ValueError("invalid-delivery-source")
            if "url_button" in part:
                if part["kind"] != "text" or part.get("callback_group_id"):
                    raise ValueError("invalid-url-button")
                url_button_label(source, part["url_button"])
                part["url_button"] = validate_url_button(part["url_button"])
            part.update(part_id=uuid4().hex, presentation_version=version, ordinal=index, state="pending", message_ids=[])
            part.setdefault("callback_group_id", None)
            frozen.append(part)
        return frozen

    async def _add(
        self,
        link: BotLink,
        *,
        dedupe_key: str,
        class_: str,
        payload: dict,
        not_before: datetime | None = None,
        session: AsyncSession | None = None,
        server_status: bool = False,
        check_attempt_current: Callable[[], bool] | None = None,
        **targets,
    ) -> tuple[str, bool]:
        if class_ != "reply":
            raise ValueError("reply-only-outbox")
        if payload.get("delivery_kind") == "status_notice" and not server_status:
            raise ValueError("reserved-status-notice")
        if not dedupe_key or len(dedupe_key) > 96 or _TOKEN.search(dedupe_key):
            raise ValueError("invalid-delivery-key")
        allowed = {"instance_id", "work_id", "request_id", "event_id", "chat_id", "message_thread_id", "silent", "inbox_rows", "finish_route"}
        if set(targets) - allowed:
            raise ValueError("invalid-delivery-target")
        if targets.get("chat_id", link.chat_id) != link.chat_id:
            raise ValueError("invalid-delivery-target")
        async with self._writer(link.id, session=session) as (writer, binding):
            if not self._binding(binding, link) or binding.state != "connected":
                raise ValueError("link-not-connected")
            existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == dedupe_key).with_for_update())
            if check_attempt_current is not None and check_attempt_current() is not True:
                raise ValueError("stale-status-attempt")
            if existing is not None:
                if existing.binding_generation != link.binding_generation:
                    raise ValueError("delivery-generation-mismatch")
                return existing.id, False
            frozen_payload = _safe_json(payload)
            parts = self._parts(frozen_payload.pop("parts", []), 0)
            frozen_payload["presentation_version"] = 0
            now = self.clock()
            row = TelegramOutboxRow(
                id=uuid4().hex,
                link_id=link.id,
                owner_user_id=link.owner_user_id,
                binding_generation=link.binding_generation,
                dedupe_key=dedupe_key,
                class_=class_,
                payload=frozen_payload,
                parts=parts,
                state="pending",
                not_before=not_before or now,
                chat_id=link.chat_id,
                message_thread_id=targets.pop("message_thread_id", None),
                created_at=now,
                **{key: value for key, value in targets.items() if key not in {"chat_id", "inbox_rows", "finish_route"}},
            )
            writer.add(row)
            await writer.flush()
            if targets.get("inbox_rows"):
                await self.inbox.register_delivery(targets["inbox_rows"], outbox_id=row.id, finish=targets.get("finish_route", False), session=writer)
            return row.id, True

    async def add(self, link: BotLink, *, dedupe_key: str, class_: str, payload: dict, not_before: datetime | None = None, session: AsyncSession | None = None, **targets) -> bool:
        return (await self._add(link, dedupe_key=dedupe_key, class_=class_, payload=payload, not_before=not_before, session=session, **targets))[1]

    async def plan_product(self, link: BotLink, *, source_id: str, parts: list[dict], inbox_rows: Sequence = (), finish_route: bool = False, message_thread_id: int | None = None, session: AsyncSession | None = None) -> str:
        """Save a fixed product delivery, retaining its exact origin and purpose."""
        if not isinstance(source_id, str) or not source_id:
            raise ValueError("invalid-delivery-key")
        if type(finish_route) is not bool:
            raise ValueError("invalid-product-completion")
        # Transport receipts never become caller-controlled replay basis. The
        # same sanitized parts are used for both the basis and saved plan.
        if not isinstance(parts, list) or not parts:
            raise ValueError("empty-delivery-plan")
        parts = [_safe_json(supplied) for supplied in parts]
        for supplied in parts:
            supplied.pop("url_button_fallback", None)
        origins = sorted((row.link_id, row.message_id, row.chat_id) for row in inbox_rows)
        if len(set(origins)) != len(origins) or any(row.link_id != link.id or row.binding_generation != link.binding_generation or row.chat_id != link.chat_id for row in inbox_rows):
            raise ValueError("product-source-conflict")
        topic = message_thread_id if message_thread_id is not None else inbox_rows[0].message_thread_id if inbox_rows else None
        if topic is not None and (type(topic) is not int or topic <= 0):
            raise ValueError("invalid-delivery-target")
        if topic == 1:
            topic = None
        basis = _safe_json({"origins": [list(value) for value in origins], "chat_id": link.chat_id, "message_thread_id": topic, "finish_route": finish_route, "parts": parts})
        if any(part.get("kind") not in {"text", "edit"} or set(part.get("source_ref", {})) != {"key", "values"} for part in parts):
            raise ValueError("invalid-product-source")
        async with self._writer(link.id, session=session) as (writer, binding):
            if not self._binding(binding, link) or binding.state != "connected":
                raise ValueError("link-not-connected")
            existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == f"notice:{source_id}").with_for_update())
            if existing is not None:
                if (
                    existing.binding_generation != link.binding_generation
                    or existing.owner_user_id != link.owner_user_id
                    or existing.chat_id != link.chat_id
                    or json.dumps(existing.payload.get("product_basis"), sort_keys=True) != json.dumps(basis, sort_keys=True)
                ):
                    raise ValueError("product-source-conflict")
                identity = existing.id
            else:
                identity, _ = await self._add(link, dedupe_key=f"notice:{source_id}", class_="reply", payload={"product_basis": basis, "parts": parts}, message_thread_id=topic, session=writer)
            if inbox_rows:
                await self.inbox.register_delivery(inbox_rows, outbox_id=identity, finish=finish_route, session=writer)
            return identity

    async def plan_product_keyboard(
        self,
        link: BotLink,
        *,
        source_id: str,
        parts: list[dict],
        keyboard,
        callbacks,
        guard: PublicationGuard,
        inbox_rows: Sequence = (),
        finish_route: bool = False,
        message_thread_id: int | None = None,
        session: AsyncSession | None = None,
    ) -> str:
        return await self._plan_product_keyboard(
            link, source_id=source_id, parts=parts, keyboard=keyboard, callbacks=callbacks, guard=guard, inbox_rows=inbox_rows, finish_route=finish_route, message_thread_id=message_thread_id, session=session
        )

    async def _plan_product_keyboard(
        self,
        link: BotLink,
        *,
        source_id: str,
        parts: list[dict],
        keyboard,
        callbacks,
        guard: PublicationGuard,
        inbox_rows: Sequence = (),
        finish_route: bool = False,
        message_thread_id: int | None = None,
        session: AsyncSession | None = None,
        _extra=None,
        _dedupe=None,
        _attach=None,
        _url_button=None,
    ) -> str:
        """Commit fixed presentation and its random controls as one replayable plan."""
        from app.telegram_bots.callbacks import canonical_keyboard
        from app.telegram_bots.links import prepare_publication_guard

        controls = canonical_keyboard(keyboard)
        dedupe = _dedupe or f"notice:{source_id}"
        extra = _safe_json(_extra or {})
        if not isinstance(source_id, str) or not source_id or type(finish_route) is not bool or not isinstance(parts, list) or not parts:
            raise ValueError("invalid-product-source")
        parts = [_safe_json(part) for part in parts]
        if any(part.get("kind") not in {"text", "edit"} or set(part.get("source_ref", {})) != {"key", "values"} or {"callback_group_id", "stored_keyboard", "url_button"}.intersection(part) for part in parts):
            raise ValueError("invalid-product-source")
        if _url_button is not None:
            button = validate_url_button(_url_button)
            url_button_label(parts[-1]["source_ref"], button)
            parts[-1]["url_button"] = button
        origins = sorted((row.link_id, row.message_id, row.chat_id) for row in inbox_rows)
        topic = message_thread_id if message_thread_id is not None else inbox_rows[0].message_thread_id if inbox_rows else None
        if topic is not None and (type(topic) is not int or topic <= 0):
            raise ValueError("invalid-delivery-target")
        if topic == 1:
            topic = None
        if len(set(origins)) != len(origins) or any(
            row.link_id != link.id or row.binding_generation != link.binding_generation or row.chat_id != link.chat_id or (None if row.message_thread_id == 1 else row.message_thread_id) != topic for row in inbox_rows
        ):
            raise ValueError("product-source-conflict")
        basis = _safe_json({"origins": [list(value) for value in origins], "chat_id": link.chat_id, "message_thread_id": topic, "finish_route": finish_route, "parts": parts})
        scope = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
        captured = guard.captured_link
        if any(getattr(captured, name) != getattr(link, name) for name in scope):
            raise ValueError("product-source-conflict")
        if session is not None:
            bind = self.sessions.kw.get("bind")
            if bind is None or not session.in_transaction() or not session.is_active or session.get_bind() is not getattr(bind, "sync_engine", bind):
                raise ValueError("caller-owned-transaction-required")

        async def save():
            # A borrowed writer's owner already prepared this guard before
            # taking SQL ownership. Never reach provider/config preparation
            # through a second checkout while that writer is held.
            if session is None:
                await prepare_publication_guard(guard)
            async with self._writer(link.id, session=session) as (writer, _binding):
                current = await guard.check(writer)
                if any(getattr(current, name) != getattr(link, name) for name in scope):
                    raise ValueError("product-source-conflict")
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == dedupe).with_for_update())
                if existing is not None:
                    if (
                        existing.binding_generation != link.binding_generation
                        or existing.owner_user_id != link.owner_user_id
                        or existing.chat_id != link.chat_id
                        or _canonical_primary(existing.payload.get("product_basis")) != _canonical_primary(basis)
                        or _canonical_primary({"keyboard": existing.payload.get("product_keyboard_basis")}) != _canonical_primary({"keyboard": controls})
                        or any(_canonical_primary({"value": existing.payload.get(key)}) != _canonical_primary({"value": value}) for key, value in extra.items())
                    ):
                        raise ValueError("product-source-conflict")
                    identity = existing.id
                else:
                    saved_parts = deepcopy(parts)
                    saved_parts[-1]["stored_keyboard"] = True
                    identity, _ = await self._add(
                        link,
                        dedupe_key=dedupe,
                        class_="reply",
                        payload={"product_basis": basis, "product_keyboard_basis": controls, **extra, "parts": saved_parts},
                        message_thread_id=topic,
                        session=writer,
                    )
                    parent = await writer.get(TelegramOutboxRow, identity)
                    if controls:
                        group = await callbacks.write_group(
                            link,
                            controls,
                            anchor_outbox_id=identity,
                            presentation_version=0,
                            chat_id=link.chat_id,
                            message_id=parts[-1].get("target_message_id") if parts[-1]["kind"] == "edit" else None,
                            message_thread_id=topic,
                            session=writer,
                        )
                        saved = deepcopy(parent.parts)
                        saved[-1]["callback_group_id"] = group.group_id
                        parent.parts = saved
                if inbox_rows:
                    await self.inbox.register_delivery(inbox_rows, outbox_id=identity, finish=finish_route, session=writer)
                if _attach is not None:
                    await _attach(writer, identity)
                await writer.flush()
                # Original host/current-custody fence is the last await before
                # owned commit; a borrowed caller retains its final guard too.
                await guard.check(writer)
                return identity

        return await await_drained(save())

    async def plan_inbound_file_notice(self, record, file, *, source_ref, callbacks, guard, retry=False, url_button=None):
        """Freeze an auxiliary file failure over actual input, never its final."""
        from app.telegram_bots.inbox import file_notice_inputs, publication_identity, publication_message_parts, validate_chat_file

        if (
            type(source_ref) is not dict
            or set(source_ref) != {"key", "values"}
            or source_ref["key"] not in {"file.not_saved", "file.own_full", "file.too_large_in", "file.own_preparing", "file.own_no_seat", "file.own_unchecked", "file.own_unavailable"}
            or type(retry) is not bool
            or retry
            and source_ref["key"] != "file.not_saved"
        ):
            raise ValueError("file-notice-conflict") from None
        file = validate_chat_file(file)
        if source_ref["values"].get("file") != file.get("filename"):
            raise ValueError("file-notice-conflict") from None
        link = guard.captured_link
        part = {"kind": "text", "source_ref": deepcopy(source_ref), "reply_to_message_id": file["message_id"]}
        if url_button is not None:
            url_button = validate_url_button(url_button)
            url_button_label(source_ref, url_button)
        dedupe = f"file-input:{record.publication_id}:{file['message_id']}:{source_ref['key']}"
        async with callbacks.writer(link, guard) as writer:
            fresh, origins = await self.inbox.lock_publication(record, session=writer)
            receipt = await self.inbox.snapshot_publication_inputs(fresh, session=writer)
            text, _kwargs, files = publication_message_parts(origins, submission=receipt["submission"])
            if file not in files:
                raise ValueError("file-notice-conflict") from None
            basis = {"version": 1, "publication": publication_identity(fresh), "inputs": receipt, "file": deepcopy(file), "text": text, "files": files}
            existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == dedupe).with_for_update())
            if existing is not None:
                old_source, _old_origins, old_basis = file_notice_inputs(existing.payload)
                if publication_identity(old_source) != publication_identity(fresh) or old_basis["file"] != file:
                    raise ValueError("file-notice-conflict") from None
                basis = old_basis
            keyboard = [[{"label": "label.send_again", "kind": "file_retry", "target_kind": "thread", "target_id": fresh.scope.thread_id, "payload": {"message_id": origins[0].message_id, "text": text, "files": files}}]] if retry else []
            return await self._plan_product_keyboard(
                link,
                source_id=dedupe,
                parts=[part],
                keyboard=keyboard,
                callbacks=callbacks,
                guard=guard,
                message_thread_id=fresh.scope.message_thread_id,
                session=writer,
                _extra={"inbound_file_basis": basis},
                _dedupe=dedupe,
                _url_button=url_button,
            )

    async def plan_input_outcome(self, link, *, source_id, source_ref, keyboard, callbacks, guard, inbox_rows=(), input_owner_callback_id=None):
        if bool(inbox_rows) == (input_owner_callback_id is not None) or type(source_ref) is not dict or set(source_ref) != {"key", "values"} or source_ref["key"] not in INPUT_OUTCOME_KEYS:
            raise ValueError("input-outcome-conflict") from None
        if input_owner_callback_id is not None and (type(input_owner_callback_id) is not str or re.fullmatch(r"[0-9a-f]{32}", input_owner_callback_id) is None):
            raise ValueError("input-outcome-conflict") from None
        topic = None
        if input_owner_callback_id is not None:
            stage = await self.inbox.read_callback_input(link, input_owner_callback_id, guard=guard)
            if stage.record is not None or not stage.origins:
                raise ValueError("input-outcome-conflict") from None
            topic = stage.origins[0].message_thread_id

        async def attach(writer, identity):
            if input_owner_callback_id is not None:
                await self.inbox.attach_input_outcome(link, input_owner_callback_id, outbox_id=identity, session=writer)

        return await self._plan_product_keyboard(
            link,
            source_id=source_id,
            parts=[{"kind": "text", "source_ref": source_ref}],
            keyboard=keyboard,
            callbacks=callbacks,
            guard=guard,
            inbox_rows=inbox_rows,
            finish_route=bool(inbox_rows),
            message_thread_id=topic,
            _extra={"input_owner_callback_id": input_owner_callback_id} if input_owner_callback_id is not None else {},
            _attach=attach,
        )

    async def _locked_stream_placeholder(self, writer, link, outbox_id):
        from app.telegram_bots.callbacks import CALLBACK_METADATA_KEYS

        row = await writer.get(TelegramOutboxRow, outbox_id, with_for_update=True, populate_existing=True)
        if (
            row is None
            or (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.class_) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, "reply")
            or re.fullmatch(r"notice:chat-stream:[0-9a-f]{32}:[0-9]+", row.dedupe_key) is None
        ):
            raise ValueError("stream-placeholder-conflict") from None
        basis = row.payload.get("product_basis")
        controls = row.payload.get("product_keyboard_basis")
        if type(basis) is not dict or basis.get("finish_route") is not False or type(basis.get("parts")) is not list or len(basis["parts"]) != 1:
            raise ValueError("stream-placeholder-conflict") from None
        initial = basis["parts"][0]
        if initial.get("kind") != "text" or initial.get("source_ref") != {"key": "answer.thinking", "values": {}} or set(initial) - {"kind", "source_ref", "reply_to_message_id", "protect_content"}:
            raise ValueError("stream-placeholder-conflict") from None
        if "protect_content" in initial and type(initial["protect_content"]) is not bool:
            raise ValueError("stream-placeholder-conflict") from None
        if type(controls) is not list or len(controls) != 1 or type(controls[0]) is not list or len(controls[0]) != 1:
            raise ValueError("stream-placeholder-conflict") from None
        button = controls[0][0]
        if (
            type(button) is not dict
            or set(button) != {"label", "kind", "target_kind", "target_id", "payload", "expires_at"}
            or (button["label"], button["kind"], button["target_kind"], button["payload"], button["expires_at"]) != ("label.stop", "stop_answer", "draft", {}, None)
        ):
            raise ValueError("stream-placeholder-conflict") from None
        version = row.payload.get("presentation_version")
        active = [part for part in row.parts if part.get("presentation_version") == version]
        if type(version) is not int or version < 0 or len(active) != 1 or active[0].get("stored_keyboard") is not True:
            raise ValueError("stream-placeholder-conflict") from None
        groups = {part.get("callback_group_id") for part in row.parts} - {None}
        if len(groups) != 1:
            raise ValueError("stream-placeholder-conflict") from None
        buttons = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups)).order_by(TelegramCallbackRow.id).with_for_update()))
        if len(buttons) != 1:
            raise ValueError("stream-placeholder-conflict") from None
        saved = buttons[0]
        if (
            (saved.link_id, saved.chat_id, saved.binding_generation, saved.kind, saved.target_kind, saved.target_id) != (link.id, link.chat_id, link.binding_generation, "stop_answer", "draft", button["target_id"])
            or saved.payload.get("anchor_outbox_id") != row.id
            or {key: value for key, value in saved.payload.items() if key not in CALLBACK_METADATA_KEYS}
        ):
            raise ValueError("stream-placeholder-conflict") from None
        return row, saved

    async def retire_stream_placeholder(self, link, *, outbox_id, guard):
        """Retire only an unclaimed exact Thinking plan; a live claim must settle."""
        from app.telegram_bots.links import prepare_publication_guard

        scope = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")
        if type(link) is not BotLink or any(getattr(guard.captured_link, name) != getattr(link, name) for name in scope):
            raise ValueError("stream-placeholder-conflict") from None

        async def perform():
            await prepare_publication_guard(guard)
            async with self._writer(link.id) as (writer, _):
                current = await guard.check(writer)
                if any(getattr(current, name) != getattr(link, name) for name in scope):
                    raise ValueError("stream-placeholder-conflict") from None
                row, saved = await self._locked_stream_placeholder(writer, link, outbox_id)
                if row.state == "sending" or row.claim_id is not None or row.claim_owner is not None:
                    result = "sending"
                elif row.state == "sent":
                    result = "sent"
                elif row.state == "pending" or row.state == "skipped" and row.skip_reason == "superseded":
                    row.state, row.skip_reason, row.closed_at = "skipped", "superseded", self.clock()
                    parts = deepcopy(row.parts)
                    for part in parts:
                        if part["state"] not in _FINAL_PARTS:
                            part["state"] = "retired"
                    row.parts = parts
                    if saved.consumed_at is None:
                        saved.consumed_at, saved.closed_reason = self.clock(), "superseded"
                    result = "retired"
                else:
                    raise ValueError("stream-placeholder-conflict") from None
                await writer.flush()
                if result == "retired":
                    await self.inbox.settle_deliveries(link_id=link.id, session=writer)
                await guard.check(writer)
                return result

        return await await_drained(perform())

    async def plan_publication_outcome(self, record, *, run=None, creation_refusal=None, source_ref, keyboard, callbacks, guard):
        link = _guard_publication_link(record, guard)
        basis = await await_drained(asyncio.to_thread(_prepare_publication_outcome, record, run=run, creation_refusal=creation_refusal, source_ref=source_ref))

        async def attach(writer, identity):
            await self.inbox.seal_publication_outcome(record, outbox_id=identity, session=writer)

        return await self._plan_product_keyboard(
            link,
            source_id="turn-outcome:" + record.publication_id,
            parts=[{"kind": "text", "source_ref": source_ref}],
            keyboard=keyboard,
            callbacks=callbacks,
            guard=guard,
            message_thread_id=record.scope.message_thread_id,
            _extra={"turn_outcome_basis": basis},
            _dedupe="reply:" + record.turn_id,
            _attach=attach,
        )

    async def plan_cancelled_before_dispatch(self, record, *, stop_id, guard, callbacks, keyboard=()):
        link = _guard_publication_link(record, guard)
        if type(stop_id) is not str or not stop_id or len(stop_id) > 128 or not stop_id.isprintable() or record.run_id is not None:
            raise ValueError("publication-outcome-conflict") from None
        source = {"key": "busy.cancelled", "values": {}}
        basis = _safe_json({"version": 1, "publication": _publication_identity(record), "proof": {"kind": "before_dispatch_stop", "stop_id": stop_id}, "source_ref": source, "finish_route": True})

        async def attach(writer, identity):
            await self.inbox.seal_cancelled_before_dispatch(record, stop_id=stop_id, outbox_id=identity, session=writer)

        return await self._plan_product_keyboard(
            link,
            source_id="before-dispatch-stop:" + record.publication_id,
            parts=[{"kind": "text", "source_ref": source}],
            keyboard=keyboard,
            callbacks=callbacks,
            guard=guard,
            message_thread_id=record.scope.message_thread_id,
            _extra={"turn_outcome_basis": basis},
            _dedupe="reply:" + record.turn_id,
            _attach=attach,
        )

    @staticmethod
    def _sending(binding: TelegramBotLinkRow | None, row: TelegramOutboxRow | None, *, link_id: str, part_id: str, claim_owner: str, claim_id: str, token_revision: int, preparing: bool = True) -> SendingPart:
        if type(token_revision) is not int or token_revision < 0 or not isinstance(claim_owner, str) or not claim_owner or not isinstance(claim_id, str) or not claim_id:
            raise DeliveryClaimRetired()
        if binding is None or row is None or row.link_id != link_id or not Outbox._fenced(row, generation=binding.binding_generation, owner=claim_owner, claim=claim_id):
            raise DeliveryClaimRetired()
        if binding.token_revision != token_revision or row.owner_user_id != binding.owner_user_id or row.chat_id != binding.chat_id or preparing and binding.state != "connected":
            raise DeliveryClaimRetired()
        status = row.class_ == "reply" and row.payload.get("delivery_kind") == "status_notice" and all(part["kind"] == "text" and part["source_ref"].get("key") in _STATUS_KEYS and not part.get("callback_group_id") for part in row.parts)
        if preparing and binding.health != "ok" and not (binding.health == "agent_unavailable" and status):
            raise DeliveryClaimRetired()
        version = row.payload.get("presentation_version", 0)
        required = [part for part in row.parts if part["presentation_version"] == version and part["state"] not in _FINAL_PARTS]
        if not required or required[0]["part_id"] != part_id or required[0]["state"] != "sending" or any(part["state"] == "sending" for part in required[1:]):
            raise DeliveryClaimRetired()
        return SendingPart(BotLink.from_row(binding), _item(row), deepcopy(required[0]), claim_owner, claim_id, token_revision)

    async def capture_sending_part(self, *, link_id: str, outbox_id: str, part_id: str, claim_owner: str, claim_id: str, token_revision: int) -> SendingPart:
        """Read the exact part under its fences, closing SQL before transport."""
        async with self._writer(link_id) as (session, binding):
            row = await session.get(TelegramOutboxRow, outbox_id, with_for_update=True)
            return self._sending(binding, row, link_id=link_id, part_id=part_id, claim_owner=claim_owner, claim_id=claim_id, token_revision=token_revision)

    async def _file_retry_locked(self, link, callback_id, callbacks, writer):
        from app.telegram_bots.callbacks import _RESERVED
        from app.telegram_bots.file_source import validate_media_reply_basis

        parent, rows, selected = await callbacks._locked(writer, link, callback_id)
        proof = validate_media_reply_basis(_item(parent))
        payload = {key: value for key, value in selected.payload.items() if key not in _RESERVED}
        if (
            selected.kind != "file_retry"
            or type(payload) is not dict
            or set(payload) != {"thread_id", "path"}
            or payload["thread_id"] != proof.record.scope.thread_id
            or selected.target_kind != "thread"
            or selected.target_id != payload["thread_id"]
        ):
            raise ValueError("publication-source-conflict")
        warning = next((part for part in parent.parts if part.get("callback_group_id") == selected.group_id and part.get("file_retry_source") == payload), None)
        if warning is None:
            raise ValueError("publication-source-conflict")
        event = next((event for event in parent.payload["file_events"] if warning["part_id"] in event["new"] and event["reason"] == "unavailable"), None)
        if event is None:
            raise ValueError("publication-source-conflict")
        original = next(part for part in parent.parts if part["part_id"] == event["old"])
        file = next(file for file in original["files"] if file["path"] == payload["path"])
        return parent, rows, selected, proof.record, original, deepcopy(file)

    async def read_file_retry(self, link, callback_id, *, callbacks, guard):
        async with callbacks.writer(link, guard) as writer:
            parent, _rows, selected, record, _original, file = await self._file_retry_locked(link, callback_id, callbacks, writer)
            if selected.consumed_at is not None:
                return None
            callbacks._usable(selected)
            return record, file

    async def retry_file_callback(self, link, callback_id, *, metadata, callbacks, guard):
        """Append one delivery-only file attempt; never reserve a model input."""
        from app.telegram_bots.file_source import clean_part, media_source, validate_media_reply_basis

        async with callbacks.writer(link, guard) as writer:
            parent, rows, selected, record, original, file = await self._file_retry_locked(link, callback_id, callbacks, writer)
            if selected.consumed_at is not None:
                return False
            callbacks._usable(selected)
            if parent.state == "sending":
                return False
            if type(metadata) is not dict or set(metadata) != {"path", "filename", "size", "mime_type", "state"} or metadata["path"] != file["path"]:
                raise ValueError("publication-source-conflict")
            from app.telegram_bots.files import plan_output_files

            # Validate HEAD facts as presentation data, never authority.
            retry_plan = plan_output_files([metadata], answer_visible="", capacity=parent.payload["reply_basis"]["capacity"], max_bytes=parent.payload["reply_basis"]["max_bytes"])
            recipe = {key: value for key, value in clean_part(original).items() if key not in {"caption_html", "caption_text"}}
            recipe.update(kind="document", files=[deepcopy(metadata)], unavailable=retry_plan.parts[0]["unavailable"], source_ref=media_source([metadata]))
            digest = next(member["sha256"] for member in original["source_ref"]["output_files"] if member["path"] == file["path"])
            recipe["source_ref"]["output_files"][0]["sha256"] = digest
            fresh = self._parts([recipe], parent.payload.get("presentation_version", 0))
            fresh[0]["ordinal"] = len(parent.parts)
            parent.parts = [*deepcopy(parent.parts), *fresh]
            parent.payload = {**parent.payload, "file_events": [*parent.payload["file_events"], {"old": original["part_id"], "new": [fresh[0]["part_id"]], "reason": "file-retry"}]}
            validate_media_reply_basis(_item(parent))
            parent.state, parent.not_before, parent.failed_reason, parent.closed_at = "pending", self.clock(), None, None
            parent.claim_owner = parent.claim_id = parent.claimed_at = None
            warning = next(
                (part for part in parent.parts if part.get("callback_group_id") == selected.group_id and part["state"] == "sent" and part["message_ids"] and (selected.message_id is None or selected.message_id in part["message_ids"])), None
            )
            if warning is None:
                raise ValueError("publication-source-conflict")
            # Retire the visible controls in the same durable unit as the new
            # delivery; the existing sender owns this metadata-only edit.
            await self._add(
                link,
                dedupe_key="file-retry-cleanup:" + selected.id,
                class_="reply",
                payload={"kind": "keyboard_cleanup", "parts": [{"kind": "edit", "source_ref": warning["source_ref"], "remove_keyboard": True, "target_message_id": warning["message_ids"][-1]}]},
                session=writer,
                message_thread_id=parent.message_thread_id,
            )
            for row in rows:
                if row.consumed_at is None:
                    row.consumed_at, row.closed_reason = self.clock(), "done"
            return True

    async def capture_caption_cleanup(self, *, link_id, outbox_id, part_id, claim_owner, claim_id, token_revision):
        """Capture ACK-owned preview cleanup without reopening media delivery."""
        from app.telegram_bots.file_source import validate_media_reply_basis

        async with self._writer(link_id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, outbox_id, with_for_update=True)
            if binding is None or not self._fenced(row, generation=binding.binding_generation, owner=claim_owner, claim=claim_id) or binding.token_revision != token_revision or binding.state != "connected" or binding.health != "ok":
                raise DeliveryClaimRetired()
            item = _item(row)
            validate_media_reply_basis(item)
            disposition = row.payload.get("caption_cleanup")
            selected = next((part for part in item.parts if part["part_id"] == part_id), None)
            display = item.payload["reply_basis"]["display"]
            ids = sorted({segment["anchor_message_id"] for segment in (display or {}).get("segments", []) if segment["mode"] == "edit"})
            if disposition != {"part_id": part_id, "message_ids": ids, "state": "pending"} or not ids or selected is None or selected["state"] != "sent" or not selected.get("caption_html") or not selected["message_ids"]:
                raise DeliveryClaimRetired()
            return SendingPart(BotLink.from_row(binding), item, deepcopy(selected), claim_owner, claim_id, token_revision)

    async def record_file_hashes(self, captured: SendingPart, hashes: dict) -> bool:
        """Bind first authorized bytes before API dispatch; never replace a hash."""
        if type(hashes) is not dict or not hashes or any(type(path) is not str or not _digest(digest) for path, digest in hashes.items()):
            raise ValueError("invalid-file-hashes")
        async with self._writer(captured.link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
            try:
                current = self._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
                from app.telegram_bots.file_source import validate_media_reply_basis

                validate_media_reply_basis(current.item, current.part)
            except (ValueError, DeliveryClaimRetired):
                return False
            if _canonical_primary(current.part) != _canonical_primary(captured.part):
                return False
            files = current.part["source_ref"]["output_files"]
            if not set(hashes) <= {file["path"] for file in files} or any(file["sha256"] not in (None, hashes[file["path"]]) for file in files if file["path"] in hashes):
                return False
            parts = deepcopy(row.parts)
            selected = next(value for value in parts if value["part_id"] == current.part["part_id"])
            selected["source_ref"] = {"output_files": [{"path": file["path"], "sha256": hashes.get(file["path"], file["sha256"])} for file in files]}
            row.parts = parts
            return True

    async def replace_output_parts(self, captured: SendingPart, templates, *, reason: str) -> bool:
        """Retire one unsent source and persist explicit ordered same-file parts."""
        from app.telegram_bots.file_source import clean_part, media_source, validate_media_reply_basis

        if reason not in {"photo-classification", "photo-rejected", "capacity"} or type(templates) not in {list, tuple} or not templates:
            raise ValueError("invalid-file-replacement")
        templates = deepcopy(list(templates))
        async with self._writer(captured.link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
            try:
                current = self._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
                validate_media_reply_basis(current.item, current.part)
            except (ValueError, DeliveryClaimRetired):
                return False
            if _canonical_primary(current.part) != _canonical_primary(captured.part):
                return False
            if any(
                type(value) is not dict
                or set(value) != {"kind", "files", "unavailable"}
                or value["kind"] not in {"document", "photo", "album"}
                or type(value["files"]) is not list
                or not value["files"]
                or value["unavailable"] != current.part["unavailable"]
                for value in templates
            ):
                raise ValueError("invalid-file-replacement")
            if _canonical_primary({"files": [file for value in templates for file in value["files"]]}) != _canonical_primary({"files": current.part["files"]}):
                raise ValueError("invalid-file-replacement")
            recipes = []
            hashes = {file["path"]: file["sha256"] for file in current.part["source_ref"]["output_files"]}
            for template in templates:
                if template["kind"] in {"document", "photo"} and len(template["files"]) != 1 or template["kind"] == "album" and not 2 <= len(template["files"]) <= 10:
                    raise ValueError("invalid-file-replacement")
                recipe = {**clean_part(current.part), **template, "source_ref": media_source(template["files"])}
                for file in recipe["source_ref"]["output_files"]:
                    file["sha256"] = hashes[file["path"]]
                recipes.append(recipe)
            parts = deepcopy(row.parts)
            position = next(index for index, value in enumerate(parts) if value["part_id"] == current.part["part_id"])
            parts[position]["state"] = "retired"
            fresh = self._parts(recipes, current.part["presentation_version"])
            parts[position + 1 : position + 1] = fresh
            for index, value in enumerate(parts):
                value["ordinal"] = index
            row.parts = parts
            event = {"old": current.part["part_id"], "new": [value["part_id"] for value in fresh], "reason": reason}
            row.payload = {**row.payload, "file_events": [*row.payload.get("file_events", []), event]}
            validate_media_reply_basis(_item(row))
            row.state, row.not_before = "pending", self.clock()
            row.claim_owner = row.claim_id = row.claimed_at = None
            return True

    async def fallback_to_plain(self, captured: SendingPart) -> bool:
        """Persist one same-source plain fallback; another worker must send it."""
        async with self._writer(captured.link.id) as (session, binding):
            row = await session.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
            try:
                current = self._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision, preparing=False)
            except DeliveryClaimRetired:
                return False
            caption = False
            if current.item.payload.get("reply_basis", {}).get("version") in {6, 7} and current.part["kind"] == "document" and current.part.get("caption_html"):
                from app.telegram_bots.file_source import validate_media_reply_basis

                try:
                    validate_media_reply_basis(current.item, current.part)
                    caption = True
                except ValueError:
                    return False
            if (
                current.part.get("plain_fallback")
                or current.part["kind"] not in {"text", "edit"}
                and not caption
                or current.part["presentation_version"] != captured.part["presentation_version"]
                or current.part["source_ref"] != captured.part["source_ref"]
            ):
                return False
            parts = deepcopy(row.parts)
            selected = next(part for part in parts if part["part_id"] == current.part["part_id"])
            selected.update(plain_fallback=True, state="pending")
            row.parts, row.state, row.not_before = parts, "pending", self.clock()
            row.claim_owner = row.claim_id = row.claimed_at = None
            return True

    async def fallback_url_button(self, captured: SendingPart) -> bool:
        """Journal a refused address without replacing its source or sending again."""
        async with self._writer(captured.link.id) as (session, binding):
            row = await session.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
            try:
                current = self._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision, preparing=False)
            except DeliveryClaimRetired:
                return False
            stored_urls = False
            if current.part.get("stored_keyboard") is True and current.item.payload.get("reply_basis", {}).get("version") in {6, 7}:
                from app.telegram_bots.file_source import file_warning_keyboard, validate_media_reply_basis

                try:
                    validate_media_reply_basis(current.item, current.part)
                    keyboard = file_warning_keyboard(current.part)
                    stored_urls = any("url" in button for line in keyboard for button in line)
                except ValueError:
                    return False
            elif current.part.get("stored_keyboard") is True and set(current.part.get("source_ref", {})) == {"key", "values"} and not uses_model_source(current.item):
                from app.telegram_bots.callbacks import canonical_keyboard

                try:
                    keyboard = canonical_keyboard(current.item.payload.get("product_keyboard_basis"))
                except ValueError:
                    return False
                stored_urls = any("url" in button for line in keyboard for button in line)
            scope = ("owner_user_id", "agent_kind", "agent_ref", "bot_id", "connection_id", "binding_generation", "telegram_user_id", "chat_id", "token_revision")
            if (
                current.part != captured.part
                or current.part.get("url_button_fallback")
                or current.part["kind"] != "text"
                or not ("url_button" in current.part or stored_urls)
                or current.item.message_thread_id != captured.item.message_thread_id
                or any(getattr(current.link, field) != getattr(captured.link, field) for field in scope)
            ):
                return False
            parts = deepcopy(row.parts)
            selected = next(part for part in parts if part["part_id"] == current.part["part_id"])
            selected.update(url_button_fallback=True, state="pending")
            row.parts, row.state, row.not_before = parts, "pending", self.clock()
            row.claim_owner = row.claim_id = row.claimed_at = None
            return True

    async def defer_token_verification(self, captured: SendingPart, *, retry_after_seconds: float | None, http_status: int | None = None) -> bool:
        """Retain an observed token failure until its original call is classified.

        A server deadline releases the claim. Without one, the caller raises its
        transient failure and the existing worker owns attempt/backoff updates.
        """
        not_before = None
        if retry_after_seconds is not None:
            if type(retry_after_seconds) not in {int, float} or not math.isfinite(retry_after_seconds) or retry_after_seconds < 0:
                raise ValueError("invalid-verification-delay")
            try:
                not_before = self.clock() + timedelta(seconds=retry_after_seconds)
            except (OverflowError, ValueError):
                raise ValueError("invalid-verification-delay") from None
        if http_status is not None and (type(http_status) is not int or http_status not in {401, 404}):
            raise ValueError("invalid-verification-status")
        async with self._writer(captured.link.id) as (session, binding):
            row = await session.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
            try:
                current = self._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision, preparing=False)
            except DeliveryClaimRetired:
                return False
            if current.part["presentation_version"] != captured.part["presentation_version"] or json.dumps(current.part["source_ref"], sort_keys=True) != json.dumps(captured.part["source_ref"], sort_keys=True):
                return False
            parts = deepcopy(row.parts)
            selected = next(part for part in parts if part["part_id"] == current.part["part_id"])
            selected["invalid_token_verification_pending"] = {"token_revision": captured.token_revision, "http_status": http_status, "failure_logged": True}
            if not_before is not None:
                selected["state"] = "pending"
                row.state, row.not_before = "pending", not_before
                row.claim_owner = row.claim_id = row.claimed_at = None
            row.parts = parts
            return True

    async def plan_reply(self, link: BotLink, *, turn_id: str, source: dict, parts: list[dict], inbox_rows: Sequence, session: AsyncSession | None = None) -> str:
        source = _safe_json(source)
        if "delivery_kind" in source:
            raise ValueError("reserved-status-notice")
        async with self._writer(link.id, session=session) as (writer, binding):
            # Lock the exact existing delivery before its input group. A late
            # final from a previous publication cannot seal the current retry.
            await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == f"reply:{turn_id}").with_for_update())
            marks = list(
                await writer.scalars(
                    select(TelegramInboundMarkRow)
                    .where(TelegramInboundMarkRow.link_id == link.id, TelegramInboundMarkRow.turn_id == turn_id)
                    .order_by(TelegramInboundMarkRow.message_id, TelegramInboundMarkRow.chat_id)
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
            )
            identities = {(row.link_id, row.message_id, row.chat_id) for row in inbox_rows}
            if not marks or len(identities) != len(inbox_rows) or identities != {(row.link_id, row.message_id, row.chat_id) for row in marks}:
                raise ValueError("reply-source-conflict")
            for mark in marks:
                publication = mark.route_basis.get("current_publication_id")
                entry = next((entry for entry in mark.route_basis.get("publications", []) if entry.get("publication_id") == publication), None)
                if (
                    mark.thread_id is None
                    or mark.run_id is None
                    or source.get("thread_id") != mark.thread_id
                    or source.get("run_id") != mark.run_id
                    or publication is None
                    or source.get("publication_id") != publication
                    or entry is None
                    or entry.get("run_id") != mark.run_id
                    or not isinstance(source.get("assistant_message_id"), str)
                    or not source["assistant_message_id"]
                    or mark.assistant_message_id not in (None, source["assistant_message_id"])
                    or "message_ids" in source
                    and set(source["message_ids"]) != {row.message_id for row in marks}
                ):
                    raise ValueError("reply-source-conflict")
            identity, _ = await self._add(link, dedupe_key=f"reply:{turn_id}", class_="reply", payload={"source": deepcopy(source), "parts": parts}, session=writer, message_thread_id=inbox_rows[0].message_thread_id if inbox_rows else None)
            await self.inbox.register_delivery(inbox_rows, outbox_id=identity, primary=True, finish=True, session=writer)
            for mark in marks:
                mark.assistant_message_id = source["assistant_message_id"]
            return identity

    async def plan_publication_reply(self, record: PublicationRecord, *, output: RunOutputProjection, guard: PublicationGuard) -> str:
        """Plan proved text only; this dormant writer performs no source HTTP or send."""
        basis, parts = await await_drained(asyncio.to_thread(_prepare_publication_reply, record, output, None))
        return await await_drained(self._write_publication_primary(record, guard=guard, basis=basis, parts=parts))

    async def plan_publication_notice(self, record: PublicationRecord, *, notice: PrimaryTurnNotice, guard: PublicationGuard) -> str:
        """Seal one trusted fixed outcome, without inventing an assistant identity."""
        basis, parts = await await_drained(asyncio.to_thread(_prepare_publication_reply, record, None, notice))
        return await await_drained(self._write_publication_primary(record, guard=guard, basis=basis, parts=parts))

    async def plan_verified_publication_reply(self, record: PublicationRecord, *, output: RunOutputProjectionV2, guard: PublicationGuard) -> str:
        """Plan strict complete-empty V2 text; legacy bases are never adopted."""

        def prepare():
            from app.telegram_bots.model_source import prepare_verified_reply

            return prepare_verified_reply(record, output)

        basis, parts = await await_drained(asyncio.to_thread(prepare))
        return await await_drained(self._write_publication_primary(record, guard=guard, basis=basis, parts=parts))

    async def plan_file_publication_reply(self, record, *, basis, parts, guard, shown=None):
        from app.telegram_bots.links import prepare_publication_guard
        from app.telegram_bots.stopped_preview import check_shown_preview

        basis, parts = deepcopy(basis), deepcopy(parts)
        if basis.get("display") is None:
            if shown is not None:
                raise ValueError("publication-source-conflict")
            return await await_drained(self._write_publication_primary(record, guard=guard, basis=basis, parts=parts))
        identity = _publication_identity(record)

        def check():
            try:
                saved = check_shown_preview(shown, publication_identity=identity, run_id=record.run_id)
                display = {"kind": "model_text", "stop_id": None, "segments": [asdict(segment) for segment in saved.segments], "waiting": False}
                if _canonical_primary(display) != _canonical_primary(basis["display"]):
                    raise ValueError
            except Exception:
                raise ValueError("publication-source-conflict") from None

        check()

        class ShownGuard:
            captured_link = guard.captured_link

            async def prepare(self):
                check()
                await prepare_publication_guard(guard)
                check()

            async def check(self, writer):
                check()
                current = await guard.check(writer)
                check()
                return current

        return await await_drained(self._write_publication_primary(record, guard=ShownGuard(), basis=basis, parts=parts))

    async def plan_streamed_publication_reply(self, record, *, output, shown, guard):
        return await self._plan_display(record, output=output, capability=shown, guard=guard, stopped=False, waiting=False)

    async def plan_stopped_publication_reply(self, record, *, output, preview, guard, waiting=False):
        return await self._plan_display(record, output=output, capability=preview, guard=guard, stopped=True, waiting=waiting)

    async def read_display_primary(self, record, *, output, guard):
        """Confirm an exact source without changing its primary or input."""
        from app.telegram_bots.links import prepare_publication_guard
        from app.telegram_bots.model_source import validate_model_reply_basis

        if (
            type(guard.captured_link) is not BotLink
            or any(getattr(guard.captured_link, name) != getattr(record.scope, name) for name in ("owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation"))
            or guard.captured_link.id != record.scope.link_id
        ):
            raise ValueError("publication-source-conflict") from None

        async def perform():
            await prepare_publication_guard(guard)
            async with self._writer(record.scope.link_id) as (writer, _):
                await guard.check(writer)
                existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == record.scope.link_id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id).with_for_update())
                if existing is None:
                    await guard.check(writer)
                    return None
                fresh, _ = await self.inbox.lock_publication(record, session=writer)
                proof = await await_drained(asyncio.to_thread(validate_model_reply_basis, _item(existing)))
                if _canonical_primary(_publication_identity(proof.record)) != _canonical_primary(_publication_identity(fresh)) or _canonical_primary(proof.output.model_dump(mode="json")) != _canonical_primary(
                    output.model_dump(mode="json")
                ):
                    raise ValueError("publication-source-conflict") from None
                await guard.check(writer)
                return existing.id

        return await await_drained(perform())

    async def _plan_display(self, record, *, output, capability, guard, stopped, waiting):
        from app.telegram_bots.inbox import publication_identity
        from app.telegram_bots.model_source import prepare_display_reply
        from app.telegram_bots.stopped_preview import check_shown_preview, check_stopped_preview, take_shown_preview, take_stopped_preview

        identity = publication_identity(record)
        take = take_stopped_preview if stopped else take_shown_preview
        check = check_stopped_preview if stopped else check_shown_preview
        data = take(capability, publication_identity=identity, run_id=record.run_id)
        display = {"kind": "stopped_preview" if stopped else "model_text", "stop_id": data.stop_id if stopped else None, "segments": [asdict(segment) for segment in data.segments], "waiting": waiting}
        basis, parts = await await_drained(asyncio.to_thread(prepare_display_reply, record, output, display=display))

        class DisplayGuard:
            captured_link = guard.captured_link

            async def prepare(self):
                from app.telegram_bots.links import prepare_publication_guard

                check(capability, publication_identity=identity, run_id=record.run_id)
                await prepare_publication_guard(guard)
                check(capability, publication_identity=identity, run_id=record.run_id)

            async def check(self, writer):
                check(capability, publication_identity=identity, run_id=record.run_id)
                current = await guard.check(writer)
                check(capability, publication_identity=identity, run_id=record.run_id)
                return current

        return await await_drained(self._write_publication_primary(record, guard=DisplayGuard(), basis=basis, parts=parts))

    async def _write_publication_primary(self, record: PublicationRecord, *, guard: PublicationGuard, basis: dict, parts: list[dict]) -> str:
        scope = basis["publication"]["scope"]
        link = guard.captured_link
        fields = ("owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation")
        if type(link) is not BotLink or link.id != scope["link_id"] or any(getattr(link, name) != scope[name] for name in fields):
            raise ValueError("publication-source-conflict")
        dedupe = f"reply:{basis['publication']['turn_id']}"
        if basis["version"] in {2, 4, 6}:
            from app.telegram_bots.links import prepare_publication_guard

            await prepare_publication_guard(guard)
        async with self._writer(link.id) as (writer, _):
            await guard.check(writer)
            existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == dedupe).with_for_update().execution_options(populate_existing=True))
            fresh, rows = await self.inbox.lock_publication(record, session=writer)
            if basis["version"] in {2, 4, 6}:
                # The host DTO sorts origin identities, not arrival order.
                # Resolve the original reply anchor from the locked saved rows.
                anchor = min(rows, key=lambda row: (row.accepted_at, row.update_id, row.message_id)).message_id
                parts = [{**part, "reply_to_message_id": anchor} for part in parts]
                basis = {**basis, "parts": parts}
            if _canonical_primary(_publication_identity(fresh)) != _canonical_primary(basis["publication"]):
                raise ValueError("publication-source-conflict")
            if basis["version"] == 6:
                basis = {**basis, "inputs": await self.inbox.snapshot_publication_inputs(fresh, session=writer)}
            outcome = basis["outcome"]
            projection = outcome["projection"]
            if basis["version"] in {2, 4, 6}:
                projection = projection["output"]
            if fresh.dispatch_started is not True or fresh.run_id is None or fresh.run_id != projection["run_id"]:
                raise ValueError("publication-source-conflict")
            assistant = projection["output_evidence"]["assistant_message_id"] if outcome["kind"] == "model_text" or basis["version"] == 6 and projection["state"] == "complete" else None
            if existing is not None:
                if existing.owner_user_id != scope["owner_user_id"] or existing.binding_generation != scope["binding_generation"] or existing.chat_id != scope["chat_id"] or type(existing.payload) is not dict:
                    raise ValueError("publication-source-conflict")
                saved = existing.payload.get("reply_basis")
                # Equality to the closed freshly validated basis also rejects
                # extra/missing keys and JSON bool/int substitutions on replay.
                if type(saved) is not dict or _canonical_primary(saved) != _canonical_primary(basis) or _canonical_primary(existing.payload.get("source")) != _canonical_primary(outcome):
                    raise ValueError("publication-source-conflict")
                if saved["presentation"] is not None:
                    FrozenReplyPresentation.from_json(saved["presentation"])
                identity = existing.id
            else:
                identity, created = await self._add(link, dedupe_key=dedupe, class_="reply", payload={"source": outcome, "reply_basis": basis, "parts": parts}, session=writer, message_thread_id=scope["message_thread_id"])
                if not created:
                    raise ValueError("publication-source-conflict")
                if basis["version"] == 6:
                    saved_row = await writer.get(TelegramOutboxRow, identity)
                    saved_row.payload = {**saved_row.payload, "file_initial_ids": [part["part_id"] for part in saved_row.parts], "file_events": []}
                if basis["version"] in {4, 6}:
                    saved_row = await writer.get(TelegramOutboxRow, identity)
                    saved_parts = deepcopy(saved_row.parts)
                    for part in saved_parts:
                        if "shown_message_id" in part:
                            part.update(state="sent", message_ids=[part["shown_message_id"]], sent_at=self.clock().isoformat())
                    saved_row.parts = saved_parts
            await self.inbox.seal_publication(fresh, outbox_id=identity, assistant_message_id=assistant, session=writer)
            await writer.flush()
            # The complete owning unit retains commit/session cleanup. No SQL
            # work follows this guard except the admitted transaction's commit.
            await guard.check(writer)
            return identity

    async def plan_restart_before_dispatch(self, record: PublicationRecord, *, ticket, guard: PublicationGuard) -> str:
        """One startup-only closed primary; no POST, source read or ticket retry."""
        from app.telegram_bots.inbox import RestartClosure, publication_identity
        from app.telegram_bots.links import prepare_publication_guard
        from app.telegram_bots.recovery_authority import check_restart_ticket

        check_restart_ticket(ticket, record)
        captured = guard.captured_link
        fields = ("owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation")
        if type(captured) is not BotLink or captured.id != record.scope.link_id or any(getattr(captured, name) != getattr(record.scope, name) for name in fields):
            raise ValueError("publication-source-conflict") from None

        def prepare():
            from app.telegram_bots.pages import page_address

            fact = RestartClosure(1, "restart_before_dispatch", ticket.startup_id, record.publication_id, record.input_version, record.input_sha256)
            outcome = {"kind": "restart_before_dispatch", "closure": asdict(fact), "source_ref": {"key": "chat.restarted", "values": {}}}
            part = {"kind": "text", "source_ref": outcome["source_ref"]}
            address = page_address("web.conversation", captured, thread_id=record.scope.thread_id)
            if address is not None:
                part["url_button"] = {"page": "web.conversation", "url": address}
            return outcome, part

        outcome, initial = await await_drained(asyncio.to_thread(prepare))

        async def perform():
            check_restart_ticket(ticket, record)
            await prepare_publication_guard(guard)
            async with self._writer(record.scope.link_id) as (writer, _):
                await guard.check(writer)
                existing = await writer.scalar(
                    select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == record.scope.link_id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id).with_for_update().execution_options(populate_existing=True)
                )
                fresh, rows = await self.inbox.lock_restart_publication(record, session=writer)
                anchor = min(rows, key=lambda row: (row.accepted_at, row.update_id, row.message_id)).message_id
                parts = [{**initial, "reply_to_message_id": anchor}]
                basis = {"version": 3, "publication": publication_identity(fresh), "outcome": outcome, "presentation": None, "parts": parts, "finish_route": True}
                closed = any("publication_recovery" in row.route_basis for row in rows)
                if existing is None:
                    if closed:
                        raise ValueError("publication-source-conflict") from None
                    identity, created = await self._add(
                        captured, dedupe_key="reply:" + fresh.turn_id, class_="reply", payload={"source": outcome, "reply_basis": basis, "parts": parts}, session=writer, message_thread_id=fresh.scope.message_thread_id
                    )
                    if not created:
                        raise ValueError("publication-source-conflict") from None
                else:
                    if not closed:
                        raise ValueError("publication-source-conflict") from None
                    proof = await await_drained(asyncio.to_thread(validate_restart_reply_basis, _item(existing)))
                    if proof.closure != RestartClosure.from_json(outcome["closure"]) or _canonical_primary(existing.payload["reply_basis"]) != _canonical_primary(basis):
                        raise ValueError("publication-source-conflict") from None
                    identity = existing.id
                await self.inbox.seal_restart_before_dispatch(fresh, outbox_id=identity, ticket=ticket, session=writer)
                await writer.flush()
                await guard.check(writer)
                check_restart_ticket(ticket, fresh)
                return identity

        return await await_drained(perform())

    async def plan_status_notice(
        self, link: BotLink, *, source_id: str, parts: list[dict], inbox_rows: Sequence = (), finish_route: bool = False, session: AsyncSession | None = None, check_attempt_current: Callable[[], bool] | None = None
    ) -> str:
        """Server handler path only; untrusted outbound metadata cannot set this tag."""
        if not isinstance(source_id, str) or not source_id:
            raise ValueError("invalid-delivery-key")
        if type(finish_route) is not bool:
            raise ValueError("invalid-status-completion")
        parts = _status_parts(parts)
        origins = sorted((row.link_id, row.message_id, row.chat_id) for row in inbox_rows)
        if len(set(origins)) != len(origins) or any(row.link_id != link.id or row.binding_generation != link.binding_generation or row.chat_id != link.chat_id for row in inbox_rows):
            raise ValueError("status-source-conflict")
        topic = inbox_rows[0].message_thread_id if inbox_rows else None
        if topic is not None and (type(topic) is not int or topic <= 0):
            raise ValueError("invalid-delivery-target")
        if topic == 1:
            topic = None
        scope = {name: getattr(link, name) for name in ("owner_user_id", "agent_kind", "agent_ref", "bot_id", "connection_id", "telegram_user_id", "binding_generation", "chat_id")}
        basis = _safe_json({"scope": scope, "origins": [list(value) for value in origins], "message_thread_id": topic, "finish_route": finish_route, "parts": parts})
        async with self._writer(link.id, session=session) as (writer, binding):
            if not self._binding(binding, link) or binding.state != "connected" or binding.token_revision != link.token_revision or any(getattr(binding, name) != value for name, value in scope.items()):
                raise ValueError("link-not-connected")
            existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == f"notice:{source_id}").with_for_update())
            if check_attempt_current is not None and check_attempt_current() is not True:
                raise ValueError("stale-status-attempt")
            if existing is not None:
                if (
                    existing.binding_generation != link.binding_generation
                    or existing.owner_user_id != link.owner_user_id
                    or existing.chat_id != link.chat_id
                    or json.dumps(existing.payload.get("status_basis"), sort_keys=True) != json.dumps(basis, sort_keys=True)
                ):
                    raise ValueError("status-source-conflict")
                identity = existing.id
            else:
                identity, _ = await self._add(
                    link,
                    dedupe_key=f"notice:{source_id}",
                    class_="reply",
                    payload={"delivery_kind": "status_notice", "status_basis": basis, "parts": parts},
                    server_status=True,
                    session=writer,
                    message_thread_id=topic,
                    check_attempt_current=check_attempt_current,
                )
            if inbox_rows:
                await self.inbox.register_delivery(inbox_rows, outbox_id=identity, finish=finish_route, session=writer)
            if check_attempt_current is not None and check_attempt_current() is not True:
                raise ValueError("stale-status-attempt")
            return identity

    async def retry(self, link: BotLink, outbox_id: str, *, session: AsyncSession | None = None) -> None:
        if session is not None:
            bind = self.sessions.kw.get("bind")
            if bind is None or not session.in_transaction() or not session.is_active or session.get_bind() is not getattr(bind, "sync_engine", bind):
                raise ValueError("caller-owned-transaction-required")
        async with self._writer(link.id, session=session) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, outbox_id, with_for_update=True)
            if not self._binding(binding, link) or row is None or row.link_id != link.id or row.binding_generation != link.binding_generation:
                raise ValueError("delivery-generation-mismatch")
            if row.state == "sending" or row.state in {"sent", "skipped"}:
                return
            row.state, row.not_before, row.failed_reason, row.closed_at = "pending", self.clock(), None, None
            row.claim_owner = row.claim_id = row.claimed_at = None

    @staticmethod
    def _fenced(row: TelegramOutboxRow | None, *, generation: int, owner: str, claim: str) -> bool:
        return row is not None and row.state == "sending" and row.binding_generation == generation and row.claim_owner == owner and row.claim_id == claim

    async def _retarget_pending_prompts(self, writer, parent, replacements):
        from app.telegram_bots.callbacks import _visibility

        prompt_rows = await writer.scalars(
            select(TelegramCallbackRow)
            .where(
                TelegramCallbackRow.kind == "prompt",
                TelegramCallbackRow.link_id == parent.link_id,
                TelegramCallbackRow.binding_generation == parent.binding_generation,
                TelegramCallbackRow.chat_id == parent.chat_id,
                TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == parent.id,
            )
            .with_for_update()
        )
        for prompt in prompt_rows:
            if prompt.consumed_at is None and "prompt_successor" not in prompt.payload and "prompt_visibility" in prompt.payload:
                visibility = _visibility(prompt.payload["prompt_visibility"])
                if visibility["state"] == "pending" and visibility["part_id"] in replacements:
                    prompt.payload = {**prompt.payload, "prompt_visibility": {**visibility, "part_id": replacements[visibility["part_id"]]}, "presentation_version": parent.payload["presentation_version"], "presentation_pending": False}
                    prompt.message_thread_id = parent.message_thread_id

    async def replace_presentation(self, link: BotLink, outbox_id: str, *, expected_version: int, claim_owner: str, claim_id: str, reason: str, parts: list[dict], message_thread_id: int | None) -> None:
        if reason not in {"edit-anchor-missing", "protection", "topic-missing"}:
            raise ValueError("invalid-presentation-reason")
        async with self._writer(link.id) as (session, binding):
            row = await session.get(TelegramOutboxRow, outbox_id, with_for_update=True)
            if not self._binding(binding, link) or not self._fenced(row, generation=link.binding_generation, owner=claim_owner, claim=claim_id) or row.link_id != link.id or row.payload.get("presentation_version", 0) != expected_version:
                return
            if row.payload.get("reply_basis", {}).get("version") in {6, 7}:
                from app.telegram_bots.file_source import validate_media_reply_basis

                validate_media_reply_basis(_item(row))
                previous = deepcopy(row.parts)
                fresh, events = [], []
                for original in previous:
                    if original["state"] in _FINAL_PARTS:
                        continue
                    recipe = {key: deepcopy(value) for key, value in original.items() if key not in _TRANSPORT_RECEIPTS | _PART_IDENTITY | {"target_message_id", "target_protect_content", "shown_message_id"}}
                    if recipe["kind"] == "edit":
                        recipe["kind"] = "text"
                    created = self._parts([recipe], expected_version)[0]
                    original["state"] = "retired"
                    fresh.append(created)
                    events.append({"old": original["part_id"], "new": [created["part_id"]], "reason": reason})
                if not fresh:
                    return
                row.parts = previous + fresh
                for index, value in enumerate(row.parts):
                    value["ordinal"] = index
                row.payload = {**row.payload, "file_events": [*row.payload["file_events"], *events], "replacement_reason": reason}
                row.message_thread_id, row.state, row.not_before = message_thread_id, "pending", self.clock()
                row.claim_owner = row.claim_id = row.claimed_at = None
                groups = {value.get("callback_group_id") for value in fresh} - {None}
                callbacks = await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id.in_(groups)).order_by(TelegramCallbackRow.id).with_for_update())
                for callback in callbacks:
                    if callback.consumed_at is None:
                        payload = deepcopy(callback.payload)
                        if callback.message_id is not None:
                            history = {"message_id": callback.message_id, "message_thread_id": callback.message_thread_id, "presentation_version": expected_version}
                            payload["presentation_history"] = [*payload.get("presentation_history", []), history]
                        callback.payload, callback.message_id, callback.message_thread_id = payload, None, message_thread_id
                if row.payload["reply_basis"]["version"] == 7:
                    await self._retarget_pending_prompts(session, row, {event["old"]: event["new"][0] for event in events})
                validate_media_reply_basis(_item(row))
                return
            # The canonical sources and action groups are immutable across relocation.
            old_sources = [{key: value for key, value in part["source_ref"].items() if key not in {"range", "start", "end"}} for part in row.parts]
            groups = {part.get("callback_group_id") for part in row.parts}
            for part in parts:
                basis = {key: value for key, value in part.get("source_ref", {}).items() if key not in {"range", "start", "end"}}
                if basis not in old_sources or part.get("callback_group_id") not in groups:
                    raise ValueError("changed-presentation-basis")
            version = expected_version + 1
            previous = deepcopy(row.parts)
            unsettled = [deepcopy(part) for part in previous if part["presentation_version"] == expected_version and part["state"] not in _FINAL_PARTS]
            for part in previous:
                if part["state"] not in _FINAL_PARTS:
                    part["state"] = "retired"
            fresh = self._parts(parts, version)
            row.parts = previous + fresh
            row.payload = {**row.payload, "presentation_version": version, "replacement_reason": reason}
            row.message_thread_id, row.state, row.not_before = message_thread_id, "pending", self.clock()
            row.claim_owner = row.claim_id = row.claimed_at = None
            mixed_step = row.payload.get("reply_basis", {}).get("version") == 5 and row.payload["reply_basis"]["outcome"]["request_basis"]["version"] == 2
            if mixed_step:
                replacements = {}
                for original in unsettled:
                    matching = [value for value in fresh if _canonical_primary(value["source_ref"]) == _canonical_primary(original["source_ref"])]
                    if len(matching) != 1:
                        raise ValueError("changed-presentation-basis")
                    replacements[original["part_id"]] = matching[0]["part_id"]
                await self._retarget_pending_prompts(session, row, replacements)
            callbacks = await session.scalars(
                select(TelegramCallbackRow)
                .where(TelegramCallbackRow.link_id == link.id, TelegramCallbackRow.binding_generation == link.binding_generation, TelegramCallbackRow.group_id.in_(groups - {None}))
                .order_by(TelegramCallbackRow.id)
                .with_for_update()
            )
            for callback in callbacks:
                if callback.consumed_at is None:
                    callback.payload = {**callback.payload, "presentation_pending": not mixed_step, "presentation_version": version, "anchor_outbox_id": row.id}
                    if mixed_step:
                        callback.message_id, callback.message_thread_id = None, message_thread_id


class OutboxSender:
    def __init__(
        self,
        outbox: Outbox,
        *,
        run_id: str,
        deliver_part: DeliverPart,
        eligibility: Callable[[BotLink, OutboxItem, dict | None], Awaitable[DeliveryEligibility]],
        concurrency: int = OUTBOX_SEND_CONCURRENCY,
        jitter: Callable[[], float] = random.random,
        cleanup_caption=None,
    ) -> None:
        if not run_id or len(run_id) > 32 or not 1 <= concurrency <= OUTBOX_SEND_CONCURRENCY:
            raise ValueError("invalid-delivery-worker-settings")
        self.outbox, self.run_id, self.deliver_part, self.eligibility = outbox, run_id, deliver_part, eligibility
        self.concurrency, self.jitter = concurrency, jitter
        self.cleanup_caption = cleanup_caption
        self._workers: dict[str, tuple[str, str, asyncio.Task]] = {}
        self._generations: dict[str, int] = {}
        self._callbacks: set[asyncio.Task] = set()
        self._release_tasks: dict[str, asyncio.Task] = {}
        self._retirements: dict[str, asyncio.Task] = {}
        self._stop_task: asyncio.Task | None = None
        self._tick_lock = asyncio.Lock()
        self._recovered, self._stopping = False, False
        self._last_link: str | None = None

    async def recover_prior_claims(self) -> int:
        if self._workers:
            raise RuntimeError("recovery-after-workers-started")
        count = await self._recover_claims(current_process=False)
        self._recovered = True
        return count

    async def _recover_claims(self, *, current_process: bool) -> int:
        # An absent live admission is an explicit process fact, never an age
        # heuristic. Revisit finished own claims whose cleanup SQL failed.
        ownership = TelegramOutboxRow.claim_owner == self.run_id if current_process else or_(TelegramOutboxRow.claim_owner != self.run_id, TelegramOutboxRow.claim_owner.is_(None))
        async with self.outbox.sessions() as session:
            links = list(await session.scalars(select(TelegramOutboxRow.link_id).where(TelegramOutboxRow.state == "sending", ownership).distinct()))
        count = 0
        for identity in sorted(links):
            async with self.outbox._writer(identity) as (session, binding):
                claimed = await session.scalars(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == identity, TelegramOutboxRow.state == "sending", ownership).with_for_update())
                for row in claimed:
                    if any(owner == row.claim_owner and claim == row.claim_id for _, owner, claim in _LIVE_CLAIMS):
                        continue
                    self._pending(row)
                    count += 1
        return count

    @staticmethod
    def _pending(row: TelegramOutboxRow) -> None:
        row.state = "pending"
        row.claim_owner = row.claim_id = row.claimed_at = None
        parts = deepcopy(row.parts)
        for part in parts:
            if part["state"] == "sending":
                part["state"] = "pending"
        row.parts = parts

    def _structural(self, binding: BotLink | None, row: OutboxItem) -> DeliveryEligibility | None:
        if binding is None or binding.binding_generation != row.binding_generation or binding.owner_user_id != row.owner_user_id or binding.chat_id != row.chat_id:
            return DeliveryEligibility.skip("link-not-connected")
        if binding.state == "blocked":
            return DeliveryEligibility.wait()
        if binding.state != "connected":
            return DeliveryEligibility.skip("link-not-connected")
        if binding.health == "policy_off":
            return DeliveryEligibility.skip("policy-off")
        if binding.health == "agent_unavailable":
            valid = row.class_ == "reply" and row.payload.get("delivery_kind") == "status_notice" and all(part["kind"] == "text" and part["source_ref"].get("key") in _STATUS_KEYS and not part.get("callback_group_id") for part in row.parts)
            if not valid:
                return DeliveryEligibility.wait()
        elif binding.health != "ok":
            return DeliveryEligibility.wait()
        return None

    async def _eligibility(self, binding: BotLink | None, row: OutboxItem, part: dict | None) -> DeliveryEligibility:
        # Current owner/access/policy is required even when transport is waiting.
        if binding is None:
            return DeliveryEligibility.skip("link-not-connected")
        if row.payload.get("kind") == "delivery_failure_notice":
            async with self.outbox.sessions() as session:
                original = await session.get(TelegramOutboxRow, row.payload.get("repair_outbox_id"))
                if original is None or original.binding_generation != row.binding_generation or original.state in {"sent", "skipped"}:
                    return DeliveryEligibility.skip("superseded")
        decision = await self.eligibility(binding, row, deepcopy(part))
        if not isinstance(decision, DeliveryEligibility) or decision.decision not in {"allow", "wait", "skip"} or decision.decision == "skip" and decision.reason not in _SKIP_REASONS:
            raise ValueError("invalid-delivery-eligibility")
        if decision.decision != "allow":
            return decision
        return self._structural(binding, row) or decision

    async def _snapshot(self, link_id: str, row_id: str) -> tuple[BotLink | None, OutboxItem | None]:
        async with self.outbox.sessions() as session:
            link = await session.get(TelegramBotLinkRow, link_id)
            row = await session.get(TelegramOutboxRow, row_id)
            return BotLink.from_row(link) if link else None, _item(row) if row else None

    @asynccontextmanager
    async def _claim_writer(self, link_id: str, claim: str):
        try:
            async with self.outbox._writer(link_id) as values:
                yield values
        except BaseException:
            # A failed SQL commit never reserves an in-memory worker slot.
            _LIVE_CLAIMS.discard((id(self.outbox.sessions), self.run_id, claim))
            raise

    async def tick(self) -> int:
        async with self._tick_lock:
            if self._stopping:
                return 0
            if not self._recovered:
                await self.recover_prior_claims()
            await self._recover_claims(current_process=True)
            free = min(self.concurrency - len(self._workers), OUTBOX_SEND_CONCURRENCY - len(_LIVE_CLAIMS))
            if free <= 0:
                return 0
            async with self.outbox.sessions() as session:
                due = list(
                    (
                        await session.execute(
                            select(TelegramOutboxRow.id, TelegramOutboxRow.link_id, TelegramOutboxRow.created_at)
                            .where(TelegramOutboxRow.class_ == "reply", TelegramOutboxRow.state == "pending", TelegramOutboxRow.not_before <= self.outbox.clock())
                            .order_by(TelegramOutboxRow.created_at, TelegramOutboxRow.id)
                        )
                    ).all()
                )
            links = list(dict.fromkeys(row.link_id for row in due))
            if self._last_link in links:
                offset = links.index(self._last_link) + 1
                links = links[offset:] + links[:offset]
            count = 0
            for link_id in links:
                if count >= free:
                    break
                if link_id in self._workers or link_id in self._retirements:
                    continue
                # Oldest due reply per link. A waiting row must not block a later
                # fixed repair/status notice on the same link.
                for candidate in (row for row in due if row.link_id == link_id):
                    binding, row = await self._snapshot(link_id, candidate.id)
                    if row is None:
                        continue
                    decision = await self._eligibility(binding, row, None)
                    claim = uuid4().hex
                    async with self._claim_writer(link_id, claim) as (session, current):
                        saved = await session.get(TelegramOutboxRow, row.id, with_for_update=True)
                        if saved is None or saved.state != "pending" or _aware(saved.not_before) > self.outbox.clock():
                            continue
                        # An authority check never authorizes a replaced binding/token.
                        if binding is not None and (
                            current is None or current.token_revision != binding.token_revision or current.binding_generation != binding.binding_generation or current.health != binding.health or current.state != binding.state
                        ):
                            continue
                        if decision.decision == "skip":
                            await self._skip(session, saved, decision.reason)
                            continue
                        if decision.decision == "wait":
                            continue
                        busy = await session.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link_id, TelegramOutboxRow.state == "sending").limit(1))
                        if busy is not None:
                            break
                        if len(_LIVE_CLAIMS) >= OUTBOX_SEND_CONCURRENCY:
                            break
                        saved.state, saved.claimed_at, saved.claim_owner, saved.claim_id = "sending", self.outbox.clock(), self.run_id, claim
                        _LIVE_CLAIMS.add((id(self.outbox.sessions), self.run_id, claim))
                    task = asyncio.create_task(self._run(link_id, row.id, row.binding_generation, claim))
                    self._generations[claim] = row.binding_generation
                    self._workers[link_id] = row.id, claim, task
                    task.add_done_callback(lambda done, link=link_id, item=row.id, generation=row.binding_generation, admission=claim: self._worker_done(link, item, generation, admission, done))
                    self._last_link = link_id
                    count += 1
                    break
            return count

    async def _skip(self, session: AsyncSession, row: TelegramOutboxRow, reason: str) -> None:
        row.state, row.skip_reason, row.closed_at = "skipped", reason, self.outbox.clock()
        row.claim_owner = row.claim_id = row.claimed_at = None
        await session.flush()
        await self.outbox.inbox.settle_deliveries(link_id=row.link_id, session=session)

    async def _run(self, link_id: str, row_id: str, generation: int, claim: str) -> None:
        while True:
            if self._stopping:
                return
            binding, row = await self._snapshot(link_id, row_id)
            if row is None:
                return
            version = row.payload.get("presentation_version", 0)
            part = next((part for part in row.parts if part["presentation_version"] == version and part["state"] not in _FINAL_PARTS), None)
            decision = await self._eligibility(binding, row, part)
            if self._stopping:
                return
            if decision.decision == "allow" and row.payload.get("caption_cleanup", {}).get("state") == "pending":
                await self._cleanup_caption(link_id, row_id, generation, claim, row.payload["caption_cleanup"]["part_id"])
                return
            if decision.decision == "allow" and part is not None and part["kind"] == "edit" and not part.get("remove_keyboard") and decision.protect_content and not part.get("target_protect_content", False):
                await self._replace(binding, row, claim, reason="protection")
                return
            async with self.outbox._writer(link_id) as (session, current):
                saved = await session.get(TelegramOutboxRow, row_id, with_for_update=True)
                if not self.outbox._fenced(saved, generation=generation, owner=self.run_id, claim=claim):
                    return
                structural = self._structural(BotLink.from_row(current) if current else None, _item(saved))
                if structural is not None and (decision.decision == "allow" or structural.decision == "skip"):
                    decision = structural
                if binding is not None and current is not None and binding.token_revision != current.token_revision:
                    return
                if decision.decision == "skip":
                    if decision.reason == "superseded" and part is not None and saved.payload.get("reply_basis", {}).get("version") == 7:
                        from app.telegram_bots.mixed_source import human_input_parts, is_human_input_part

                        if await await_drained(asyncio.to_thread(is_human_input_part, _item(saved), part)):
                            owned = await await_drained(asyncio.to_thread(human_input_parts, _item(saved)))
                            ids = {value["part_id"] for value in owned}
                            parts = deepcopy(saved.parts)
                            for value in parts:
                                if value["part_id"] in ids and value["state"] not in _FINAL_PARTS:
                                    value["state"] = "retired"
                            saved.parts = parts
                            if all(value["state"] in _FINAL_PARTS for value in parts):
                                await self._complete(session, saved)
                                return
                            continue
                    await self._skip(session, saved, decision.reason)
                    return
                if decision.decision == "wait":
                    self._pending(saved)
                    return
                if part is None:
                    await self._complete(session, saved)
                    return
                # Pacing belongs to the destination, including other deliveries
                # and ambiguous attempts, and survives replacement/restart.
                plans = await session.scalars(select(TelegramOutboxRow.parts).where(TelegramOutboxRow.link_id == link_id, TelegramOutboxRow.binding_generation == generation))
                last = max((datetime.fromisoformat(stamp) for parts in plans for value in parts for stamp in [value.get("dispatched_at") or value.get("sent_at")] if stamp), default=None)
                if last is not None and last + timedelta(seconds=1) > self.outbox.clock():
                    self._pending(saved)
                    saved.not_before = last + timedelta(seconds=1)
                    return
                parts = deepcopy(saved.parts)
                sending = next(value for value in parts if value["part_id"] == part["part_id"])
                sending["state"] = "sending"
                sending["protect_content"] = bool(sending.get("protect_content") or decision.protect_content)
                sending["dispatched_at"] = self.outbox.clock().isoformat()
                saved.parts = parts
                required_protection = sending["protect_content"]
            if self._stopping:
                return
            try:
                receipt = await self.deliver_part(link_id=link_id, outbox_id=row_id, part_id=part["part_id"], claim_owner=self.run_id, claim_id=claim)
                if isinstance(receipt, PartReceipt):
                    if receipt.protect_content != required_protection:
                        raise PermanentDeliveryError()
                    message_ids = list(receipt.message_ids)
                else:
                    message_ids = receipt
                if not isinstance(message_ids, list) or not message_ids or any(type(value) is not int or value <= 0 for value in message_ids):
                    raise PermanentDeliveryError()
            except asyncio.CancelledError:
                raise
            except PreviewExpired:
                await self._expire_preview(link_id, row_id, generation, claim)
                return
            except DeliveryClaimRetired:
                return
            except Exception as error:
                if isinstance(error, SourceUnavailable):
                    await self._unavailable(link_id, row_id, generation, claim, part["part_id"], error)
                    return
                if isinstance(error, BadRequest):
                    description = str(error).lower()  # in-memory classification only
                    missing = any(value in description for value in ("message to edit not found", "message can't be edited", "message cannot be edited"))
                    topic_missing = "message thread not found" in description
                    if (missing or topic_missing) and row.payload.get("kind") != "keyboard_cleanup":
                        await self._replace(binding, row, claim, reason="topic-missing" if topic_missing else "edit-anchor-missing")
                        return
                await self._failure(link_id, row_id, generation, claim, error)
                return
            async with self.outbox._writer(link_id) as (session, current):
                saved = await session.get(TelegramOutboxRow, row_id, with_for_update=True)
                if not self.outbox._fenced(saved, generation=generation, owner=self.run_id, claim=claim):
                    return
                if current is None or current.binding_generation != generation:
                    return
                parts = deepcopy(saved.parts)
                acknowledged = next(value for value in parts if value["part_id"] == part["part_id"])
                acknowledged.pop("invalid_token_verification_pending", None)
                acknowledged.update(state="sent", message_ids=message_ids, protect_content=required_protection, sent_at=self.outbox.clock().isoformat())
                saved.parts = parts
                if saved.payload.get("reply_basis", {}).get("version") in {5, 7}:
                    from app.telegram_bots.callbacks import CallbackRepository, Prompts

                    await Prompts(CallbackRepository(self.outbox.sessions, config=self.outbox.config, clock=self.outbox.clock)).acknowledge_part(saved, acknowledged, session=session)
                if acknowledged.get("caption_html") and saved.payload.get("reply_basis", {}).get("version") in {6, 7}:
                    display = saved.payload["reply_basis"]["display"]
                    ids = sorted({segment["anchor_message_id"] for segment in (display or {}).get("segments", []) if segment["mode"] == "edit"})
                    if ids:
                        saved.payload = {**saved.payload, "caption_cleanup": {"part_id": acknowledged["part_id"], "message_ids": ids, "state": "pending"}}
                if all(value["state"] in _FINAL_PARTS for value in parts if value["presentation_version"] == version):
                    await self._complete(session, saved)
                    return

    async def _cleanup_caption(self, link_id, row_id, generation, claim, part_id):
        done = False
        unavailable = False
        retry_after = timedelta(seconds=1)
        if self.cleanup_caption is not None and not self._stopping:
            try:
                result = await self.cleanup_caption(link_id=link_id, outbox_id=row_id, part_id=part_id, claim_owner=self.run_id, claim_id=claim)
                done = result is True
                unavailable = type(result) is str and result == "unavailable"
            except asyncio.CancelledError:
                raise
            except DeliveryRetry as error:
                retry_after = timedelta(seconds=error.retry_after)
            except Exception:
                done = False
        async with self.outbox._writer(link_id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, row_id, with_for_update=True)
            if binding is None or not self.outbox._fenced(row, generation=generation, owner=self.run_id, claim=claim):
                return
            disposition = row.payload.get("caption_cleanup")
            if type(disposition) is not dict or disposition.get("part_id") != part_id or disposition.get("state") != "pending":
                return
            if done or unavailable:
                row.payload = {**row.payload, "caption_cleanup": {**disposition, "state": "unavailable" if unavailable else "done"}}
                await self._complete(writer, row)
            else:
                self._pending(row)
                row.not_before = max(_aware(row.not_before), self.outbox.clock() + retry_after)

    async def _expire_preview(self, link_id, row_id, generation, claim):
        async with self.outbox._writer(link_id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, row_id, with_for_update=True)
            if binding is None or not self.outbox._fenced(row, generation=generation, owner=self.run_id, claim=claim):
                return
            row, saved = await self.outbox._locked_stream_placeholder(writer, BotLink.from_row(binding), row_id)
            row.state, row.skip_reason, row.closed_at = "skipped", "superseded", self.outbox.clock()
            row.claim_owner = row.claim_id = row.claimed_at = None
            parts = deepcopy(row.parts)
            for part in parts:
                if part["state"] not in _FINAL_PARTS:
                    part["state"] = "retired"
            row.parts = parts
            if saved.consumed_at is None:
                saved.consumed_at, saved.closed_reason = self.outbox.clock(), "superseded"
            await writer.flush()
            await self.outbox.inbox.settle_deliveries(link_id=link_id, session=writer)

    async def _replace(self, binding: BotLink, row: OutboxItem, claim: str, *, reason: str) -> None:
        version = row.payload.get("presentation_version", 0)
        parts = []
        for part in row.parts:
            basis = row.payload.get("reply_basis", {})
            display = type(basis) is dict and (basis.get("version") == 4 or basis.get("version") == 5 and basis.get("outcome", {}).get("request_basis", {}).get("version") == 2)
            if part["presentation_version"] != version or (display or part["kind"] in {"document", "photo", "album"}) and part["state"] in {"sent", "unavailable"}:
                continue
            replacement = {
                key: deepcopy(value)
                for key, value in part.items()
                if key not in {"part_id", "presentation_version", "ordinal", "state", "message_ids", "sent_at", "dispatched_at", "target_message_id", "target_protect_content", "shown_message_id"}
            }
            if replacement["kind"] == "edit":
                replacement["kind"] = "text"
            parts.append(replacement)
        if parts:
            await self.outbox.replace_presentation(
                binding, row.id, expected_version=version, claim_owner=self.run_id, claim_id=claim, reason=reason, parts=parts, message_thread_id=None if reason == "topic-missing" else row.message_thread_id
            )

    async def _unavailable(self, link_id: str, row_id: str, generation: int, claim: str, part_id: str, error: SourceUnavailable) -> None:
        async with self.outbox._writer(link_id) as (session, binding):
            row = await session.get(TelegramOutboxRow, row_id, with_for_update=True)
            if not self.outbox._fenced(row, generation=generation, owner=self.run_id, claim=claim) or binding is None or binding.binding_generation != generation:
                return
            version = row.payload.get("presentation_version", 0)
            parts = deepcopy(row.parts)
            position = next((index for index, part in enumerate(parts) if part["part_id"] == part_id and part["presentation_version"] == version and part["kind"] in {"document", "photo", "album"} and part["state"] == "sending"), None)
            if position is None:
                raise ValueError("invalid-unavailable-source-part")
            unavailable = parts[position]
            unavailable.update(state="unavailable", unavailable_reason=error.reason)
            if row.payload.get("reply_basis", {}).get("version") in {6, 7}:
                from app.telegram_bots.callbacks import CallbackRepository
                from app.telegram_bots.file_source import file_warning_keyboard, unavailable_parts, validate_media_reply_basis

                # This source-specific disposition preserves a caption's text
                # before the failure notices and binds every retry to its path.
                unavailable["file_failure_key"] = error.source_ref["key"]
                recipes = unavailable_parts(unavailable, row.payload["reply_basis"])
                fresh = []
                for recipe in recipes:
                    fresh.extend(Outbox._parts([recipe], version))
                parts[position + 1 : position + 1] = fresh
                for index, value in enumerate(parts):
                    value["ordinal"] = index
                row.parts = parts
                row.payload = {**row.payload, "file_events": [*row.payload.get("file_events", []), {"old": part_id, "new": [value["part_id"] for value in fresh], "reason": "unavailable"}]}
                validate_media_reply_basis(_item(row))
                callbacks = CallbackRepository(self.outbox.sessions, config=self.outbox.config, clock=self.outbox.clock)
                for value in fresh:
                    if value.get("stored_keyboard"):
                        group = await callbacks.write_group(
                            BotLink.from_row(binding), file_warning_keyboard(value), anchor_outbox_id=row.id, presentation_version=version, chat_id=row.chat_id, message_id=None, message_thread_id=row.message_thread_id, session=session
                        )
                        value["callback_group_id"] = group.group_id
                row.parts = parts
                self._pending(row)
                return
            group = unavailable.get("callback_group_id")
            flags = {key: unavailable[key] for key in ("silent", "protect_content", "reply_to_message_id") if key in unavailable}
            replacement = Outbox._parts([{"kind": "text", "source_ref": error.source_ref, "replaces_part_id": part_id, "callback_group_id": group, **flags}], version)[0]
            # The failed file never acquired a visible anchor. Its existing
            # controls belong to the final visible fallback, with the same
            # callback rows and immutable operation basis.
            unavailable["callback_group_id"] = None
            replacement["ordinal"] = unavailable["ordinal"] + 1
            for part in parts[position + 1 :]:
                if part["presentation_version"] == version:
                    part["ordinal"] += 1
            parts.insert(position + 1, replacement)
            row.parts = parts
            self._pending(row)

    async def _complete(self, session: AsyncSession, row: TelegramOutboxRow) -> None:
        if row.payload.get("caption_cleanup", {}).get("state") == "pending":
            self._pending(row)
            return
        version = row.payload.get("presentation_version", 0)
        current_parts = [part for part in row.parts if part["presentation_version"] == version]
        if not current_parts or any(part["state"] not in _FINAL_PARTS for part in current_parts):
            return
        now = self.outbox.clock()
        row.state, row.sent_at, row.closed_at = "sent", now, now
        row.degraded_at = row.failed_reason = None
        row.claim_owner = row.claim_id = row.claimed_at = None
        acknowledged = [value for part in current_parts for value in part["message_ids"]]
        row.message_id = acknowledged[-1] if acknowledged else None
        await session.flush()
        await self.outbox.inbox.settle_deliveries(link_id=row.link_id, session=session)
        groups = {part.get("callback_group_id") for part in current_parts} - {None}
        callbacks = list(
            await session.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.link_id == row.link_id, TelegramCallbackRow.binding_generation == row.binding_generation).order_by(TelegramCallbackRow.id).with_for_update())
        )
        for callback in callbacks:
            if callback.consumed_at is not None:
                continue
            if callback.group_id in groups and row.message_id is not None:
                anchor = row.message_id
                if row.payload.get("reply_basis", {}).get("version") in {6, 7}:
                    own = next((part for part in current_parts if part.get("callback_group_id") == callback.group_id and part["state"] == "sent" and part["message_ids"]), None)
                    if own is None:
                        continue
                    anchor = own["message_ids"][-1]
                payload = deepcopy(callback.payload)
                if payload.get("presentation_pending"):
                    previous = {"message_id": callback.message_id, "message_thread_id": callback.message_thread_id, "presentation_version": version - 1}
                    history = list(payload.get("presentation_history", []))
                    historical_ids = {identity for part in row.parts if part["state"] == "sent" and part["presentation_version"] < version for identity in part["message_ids"]}
                    if previous["message_id"] in historical_ids and previous not in history:
                        history.append(previous)
                    payload["presentation_history"] = history
                payload.update(presentation_pending=False, presentation_version=version, anchor_outbox_id=row.id)
                callback.payload, callback.message_id, callback.message_thread_id = payload, anchor, row.message_thread_id
            if callback.kind == "delivery_retry" and callback.target_kind == "outbox" and callback.target_id == row.id:
                callback.consumed_at, callback.closed_reason = now, "done"
                if callback.message_id is not None:
                    # Replayable cleanup only. This row cannot execute the action.
                    cleanup_key = f"notice:repair-cleanup:{row.id}:{callback.group_id}"
                    exists = await session.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == row.link_id, TelegramOutboxRow.dedupe_key == cleanup_key))
                    if exists is None:
                        session.add(
                            TelegramOutboxRow(
                                id=uuid4().hex,
                                link_id=row.link_id,
                                owner_user_id=row.owner_user_id,
                                binding_generation=row.binding_generation,
                                dedupe_key=cleanup_key,
                                class_="reply",
                                payload={"kind": "keyboard_cleanup", "presentation_version": 0, "target_message_id": callback.message_id},
                                parts=Outbox._parts([{"kind": "edit", "source_ref": {"key": "delivery.failed", "values": {}}, "remove_keyboard": True, "target_message_id": callback.message_id}], 0),
                                state="pending",
                                not_before=now,
                                chat_id=row.chat_id,
                                message_thread_id=callback.message_thread_id,
                                created_at=now,
                            )
                        )

    async def _failure(self, link_id: str, row_id: str, generation: int, claim: str, error: Exception) -> None:
        async with self.outbox._writer(link_id) as (session, binding):
            row = await session.get(TelegramOutboxRow, row_id, with_for_update=True)
            if not self.outbox._fenced(row, generation=generation, owner=self.run_id, claim=claim):
                return
            now = self.outbox.clock()
            if isinstance(error, Forbidden) and binding is not None and binding.state == "blocked" and binding.owner_user_id == row.owner_user_id and binding.binding_generation == row.binding_generation and binding.chat_id == row.chat_id:
                # The captured bound send already committed the block. Its
                # structural hold owns the wait, rather than transient backoff.
                # Reconnect leaves unrelated 429/pacing deadlines untouched.
                self._pending(row)
                row.not_before = now
                return
            if isinstance(error, (DeliveryRetry, RetryAfter)) or isinstance(error, TelegramError) and isinstance(getattr(error, "retry_after", None), (int, float, timedelta)):
                delay = error.retry_after
                seconds = delay.total_seconds() if isinstance(delay, timedelta) else float(delay)
                self._pending(row)
                row.not_before = now + timedelta(seconds=max(1, seconds))
                return
            if isinstance(error, (PermanentDeliveryError, BadRequest)):
                if isinstance(error, BadRequest) and row.payload.get("kind") == "keyboard_cleanup" and any(value in str(error).lower() for value in ("message to edit not found", "message can't be edited", "message cannot be edited")):
                    await self._skip(session, row, "superseded")
                    return
                part = next((part for part in row.parts if part["state"] == "sending"), {})
                method = {"text": "sendMessage", "card": "sendMessage", "document": "sendDocument", "photo": "sendPhoto", "album": "sendMediaGroup", "edit": "editMessageText"}.get(part.get("kind"), "sendMessage")
                if part.get("remove_keyboard"):
                    method = "editMessageReplyMarkup"
                if getattr(error, "_hartmesh_telegram_failure_logged", False) is not True:
                    log_call_failure(
                        link_id=link_id,
                        bot_id=binding.bot_id if binding is not None else None,
                        method=method,
                        error=error,
                        attempts=row.attempts + 1,
                        outbox_id=row.id,
                        part_id=part.get("part_id"),
                        class_=row.class_,
                        claim_owner=self.run_id,
                        claim_id=claim,
                        failed_reason="invalid-payload",
                    )
                self._pending(row)
                parts = deepcopy(row.parts)
                for value in parts:
                    value.pop("invalid_token_verification_pending", None)
                row.parts = parts
                row.state, row.failed_reason, row.closed_at = "failed", "invalid-payload", now
                failure_version = row.payload.get("failure_version", 0) + 1
                row.payload = {**row.payload, "failure_version": failure_version}
                # The safe recovery message never recursively produces another.
                if row.payload.get("kind") == "delivery_failure_notice":
                    return
                if binding is None or binding.state != "connected" or (binding.owner_user_id, binding.binding_generation, binding.chat_id) != (row.owner_user_id, generation, row.chat_id):
                    return
                from app.telegram_bots.callbacks import CallbackRefused, CallbackRepository

                source = {"kind": "text", "source_ref": {"key": "delivery.failed", "values": {}}}
                # Preserve the original failed-delivery journal even if its
                # account/connection custody has since ended. Only construction
                # of the now-undeliverable old-scope notice is rolled back.
                try:
                    async with session.begin_nested():
                        notice = TelegramOutboxRow(
                            id=uuid4().hex,
                            link_id=link_id,
                            owner_user_id=row.owner_user_id,
                            binding_generation=generation,
                            dedupe_key=f"delivery-failed:{row_id}:{failure_version}",
                            class_="reply",
                            payload={
                                "kind": "delivery_failure_notice",
                                "repair_outbox_id": row_id,
                                "presentation_version": 0,
                                "product_basis": {"origins": [], "chat_id": row.chat_id, "message_thread_id": row.message_thread_id, "finish_route": False, "parts": [source]},
                            },
                            parts=Outbox._parts([{**source, "stored_keyboard": True}], 0),
                            state="pending",
                            not_before=now,
                            chat_id=row.chat_id,
                            message_thread_id=row.message_thread_id,
                            created_at=now,
                        )
                        session.add(notice)
                        await session.flush()
                        group = await CallbackRepository(self.outbox.sessions, config=self.outbox.config, clock=self.outbox.clock).write_repair_group(
                            BotLink.from_row(binding), failed_outbox_id=row.id, anchor_outbox_id=notice.id, session=session
                        )
                        notice.parts = [{**part, "callback_group_id": group.group_id} for part in notice.parts]
                except CallbackRefused:
                    return
                return
            # Unknown local errors retain delivery too. Do not persist exception
            # text or turn temporary transport/storage failures into payload faults.
            self._pending(row)
            row.attempts += 1
            exponent = min(row.attempts - 1, 16)
            delay = min(DELIVERY_RETRY_CAP_SECONDS, DELIVERY_RETRY_FIRST_SECONDS * 2**exponent)
            delay = min(DELIVERY_RETRY_CAP_SECONDS, delay * (1 + max(0, min(1, self.jitter())) * 0.2))
            row.not_before = now + timedelta(seconds=delay)
            if row.attempts >= self.outbox.config.outbox.max_attempts and row.degraded_at is None:
                row.degraded_at = now

    def _worker_done(self, link_id: str, row_id: str, generation: int, claim: str, task: asyncio.Task) -> None:
        if not task.cancelled():
            task.exception()  # retrieve unexpected exceptions; never log payloads
        callback = asyncio.create_task(self._release(link_id, row_id, generation, claim, task))
        self._callbacks.add(callback)
        self._release_tasks[claim] = callback
        callback.add_done_callback(lambda done, admission=claim: self._release_done(admission, done))

    def _release_done(self, claim: str, task: asyncio.Task) -> None:
        self._callbacks.discard(task)
        if self._release_tasks.get(claim) is task:
            self._release_tasks.pop(claim, None)
        if not task.cancelled():
            task.exception()  # failed cleanup is recovered by the next tick

    async def _release(self, link_id: str, row_id: str, generation: int, claim: str, task: asyncio.Task) -> None:
        try:
            async with self.outbox._writer(link_id) as (session, binding):
                row = await session.get(TelegramOutboxRow, row_id, with_for_update=True)
                if self.outbox._fenced(row, generation=generation, owner=self.run_id, claim=claim):
                    self._pending(row)
        finally:
            _LIVE_CLAIMS.discard((id(self.outbox.sessions), self.run_id, claim))
            self._generations.pop(claim, None)
            if self._workers.get(link_id) == (row_id, claim, task):
                self._workers.pop(link_id, None)

    async def drain(self) -> None:
        while self._workers or self._callbacks or self._retirements:
            tasks = [value[2] for value in self._workers.values()] + list(self._callbacks) + list(self._retirements.values())
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(0)

    async def retire_link(self, link_id: str) -> None:
        """Invalidate and drain only this link's currently owned admission.

        The cleanup task remains owned after a caller is cancelled. A tick
        cannot admit a new worker for that link until the cleanup finishes;
        stop/drain also await these retained retirement tasks.
        """
        if not isinstance(link_id, str) or not link_id or len(link_id) > 32:
            raise ValueError("invalid-link-id")
        task = self._retirements.get(link_id)
        if task is None:
            task = asyncio.create_task(self._retire_link(link_id))
            self._retirements[link_id] = task
            task.add_done_callback(lambda done: self._retirement_done(link_id, done))
        await asyncio.shield(task)

    def _retirement_done(self, link_id: str, task: asyncio.Task) -> None:
        if self._retirements.get(link_id) is task:
            self._retirements.pop(link_id, None)
        if not task.cancelled():
            task.exception()

    async def _retire_link(self, link_id: str) -> None:
        async with self._tick_lock:
            worker = self._workers.get(link_id)
            if worker is None:
                return
            row_id, claim, task = worker
            generation = self._generations[claim]
            # Commit the receipt fence before cancellation, which a transport
            # is allowed to catch while its already-dispatched call finishes.
            async with self.outbox._writer(link_id) as (session, binding):
                row = await session.get(TelegramOutboxRow, row_id, with_for_update=True)
                if self.outbox._fenced(row, generation=generation, owner=self.run_id, claim=claim):
                    self._pending(row)
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        while self._workers.get(link_id) == worker:
            release = self._release_tasks.get(claim)
            if release is not None:
                await asyncio.gather(release, return_exceptions=True)
            else:
                await asyncio.sleep(0)  # worker completion schedules its release

    async def stop(self) -> None:
        self._stopping = True
        if self._stop_task is None or self._stop_task.done():
            self._stop_task = asyncio.create_task(self._stop_workers())
        task = self._stop_task

        async def wait_owned() -> None:
            await asyncio.shield(task)

        # A cancelled shutdown caller must not proceed to close SDK clients
        # while an already-dispatched worker still owns one of those clients.
        await await_drained(wait_owned())

    async def _stop_workers(self) -> None:
        failure = None
        async with self._tick_lock:
            # Invalidate before cancelling: cancellation may be swallowed by
            # transport. A tick already in admission must finish before this
            # snapshot, so no worker can appear after shutdown drained it.
            workers = [(link_id, row_id, claim, task, self._generations[claim]) for link_id, (row_id, claim, task) in self._workers.items()]
            for link_id, row_id, claim, task, generation in workers:
                try:
                    async with self.outbox._writer(link_id) as (session, binding):
                        row = await session.get(TelegramOutboxRow, row_id, with_for_update=True)
                        if self.outbox._fenced(row, generation=generation, owner=self.run_id, claim=claim):
                            self._pending(row)
                except Exception as error:
                    if failure is None:
                        failure = error
                finally:
                    task.cancel()
        await self.drain()
        if failure is not None:
            raise failure
