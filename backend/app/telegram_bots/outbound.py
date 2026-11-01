"""Claimed output-file delivery owned by the existing outbox sender attempt."""

from __future__ import annotations

import asyncio
import hashlib
import logging

from telegram.error import BadRequest, Forbidden, InvalidToken, NetworkError

from app.telegram_bots.adapter import LongRetryAfter, SendContext
from app.telegram_bots.files import OutputBatch, classify_output_part, classify_output_photo, repartition_output_part
from app.telegram_bots.outbound_upload import _forget_exception
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryEligibility, DeliveryRetry, PermanentDeliveryError, SourceUnavailable
from app.telegram_bots.settings import silent_now
from deerflow.utils.file_io import await_drained

logger = logging.getLogger(__name__)


def _logged(error):
    error._hartmesh_telegram_failure_logged = True
    return error


def _hash_files(files):
    data = None
    try:
        result = {}
        for path, data in files:
            result[path] = hashlib.sha256(data).hexdigest()
        return result
    finally:
        files = data = None


def _unavailable(prepared, *, reason="source-unavailable", oversize=False):
    name = ", ".join(member["filename"] for member in prepared.files)[:4096]
    return SourceUnavailable(source_ref={"key": "file.too_large_out" if oversize else "file.out_failed", "values": {"file": name}}, reason=reason)


async def send_file_part(channel, saved, capture) -> list[int]:
    """Read only the saved source as its recipient, then upload charged bytes."""
    from app.telegram_bots.outbound_upload import upload_output

    services, expected = channel._services, saved
    source, owner = services.model_source, services.thread_client
    if source is None or owner is None:
        raise DeliveryClaimRetired()
    prepared = await source.read_file_part(saved)
    part = saved.part
    original_link = saved.link

    def unchanged(captured):
        # The link row's stamps move once a minute, and a stamp is no reason to give up a read file.
        return captured.part == expected.part and captured.item == expected.item and channel._same_scope(original_link, captured.link)

    async def current():
        if not channel._callback_current(original_link):
            raise DeliveryClaimRetired()
        latest = await services.outbox.capture_sending_part(**capture)
        if not unchanged(latest):
            raise DeliveryClaimRetired()
        verdict = await services.eligibility(latest.link, latest.item, latest.part)
        if not isinstance(verdict, DeliveryEligibility) or verdict.decision != "allow" or verdict.protect_content and not latest.part.get("protect_content", False):
            raise DeliveryClaimRetired()
        if await source.read_file_part(latest) != prepared or not channel._callback_current(original_link):
            raise DeliveryClaimRetired()
        # The fresh read may have waited behind another claim/link transition.
        final = await services.outbox.capture_sending_part(**capture)
        if not unchanged(final) or not channel._callback_current(original_link):
            raise DeliveryClaimRetired()

    async def dispatch_current():
        # Thread/run rights do not substitute for a current Space/seat read.
        # Limiter waits and short retries must reacquire the same artifact door.
        await current()
        try:
            for member in prepared.files:
                metadata = await owner.describe_artifact(original_link.owner_user_id, prepared.record.scope.thread_id, member["path"], before_dispatch=current)
                if (metadata.path, metadata.filename, metadata.size, metadata.mime_type) != (member["path"], member["filename"], member["size"], member["mime_type"]):
                    raise DeliveryClaimRetired()
        except DeliveryClaimRetired:
            raise
        except Exception as error:
            _forget_exception(error)
            raise DeliveryClaimRetired() from None
        await current()

    await current()
    if prepared.unavailable is not None:
        raise _unavailable(prepared, oversize=prepared.unavailable == "oversize")
    metadata = []
    failure = outcome = None
    upload_started = False
    reservation = batch = data = buffers = hash_inputs = None
    try:
        for member in prepared.files:
            described = await owner.describe_artifact(original_link.owner_user_id, prepared.record.scope.thread_id, member["path"], before_dispatch=current)
            if (described.path, described.filename, described.size, described.mime_type) != (member["path"], member["filename"], member["size"], member["mime_type"]):
                failure = "source-changed"
                break
            metadata.append(described)
        if failure is None:
            ceilings = tuple(member.size for member in metadata)
            if any(size > services.config.files.max_outbound_bytes for size in ceilings):
                failure = "oversize"
            elif sum(ceilings) > services.file_bytes_budget.capacity:
                # A restart may lower the fixed configured capacity after the
                # original plan. Persist smaller groups before opening bytes.
                template = {"kind": part["kind"], "files": list(prepared.files), "unavailable": None}
                replacements = repartition_output_part(template, capacity=services.file_bytes_budget.capacity)
                await current()
                await services.outbox.replace_output_parts(expected, replacements, reason="capacity")
                failure = "replanned"
            else:
                reservation = await services.file_bytes_budget.reserve_batch(ceilings, timeout=services.config.files.retry_max_seconds)
                batch = OutputBatch(reservation)
                await current()
                for index, member in enumerate(metadata):
                    closed = batch.begin_transport(index)
                    try:
                        data = await owner.read_artifact_capped(original_link.owner_user_id, prepared.record.scope.thread_id, member.path, ceiling=member.size, before_dispatch=current, on_failed_close=closed)
                        await batch.keep(index, data)
                        data = None
                        await current()
                    finally:
                        data = None
                        if not owner.retains_failed_close(closed):
                            await await_drained(closed())
                    hash_inputs = ((member.path, batch.data(index)),)

                    def hash_current():
                        # Executor call arguments must not retain bytes after
                        # its Future completes. Drain a cleared shared cell.
                        nonlocal hash_inputs
                        try:
                            return _hash_files(hash_inputs)
                        finally:
                            hash_inputs = None

                    hashes = await await_drained(asyncio.to_thread(hash_current))
                    if not await services.outbox.record_file_hashes(expected, hashes):
                        failure = "source-changed"
                        break
                    expected = await services.outbox.capture_sending_part(**capture)
                    await current()
                if failure is None:
                    if part["kind"] in {"photo", "album"}:
                        photos = []
                        for index, member in enumerate(metadata):
                            photos.append(await classify_output_photo(member.filename, batch.data(index)))
                        template = {"kind": part["kind"], "files": list(prepared.files), "unavailable": None}
                        replacements = classify_output_part(template, photos)
                        if replacements != (template,):
                            await services.outbox.replace_output_parts(expected, replacements, reason="photo-classification")
                            failure = "replanned"
                    if failure is None:
                        settings = await services.settings.get(original_link.owner_user_id)
                        if settings is None:
                            raise PermanentDeliveryError("missing-settings")
                        silent = saved.item.silent or bool(part.get("silent", False)) or await asyncio.to_thread(silent_now, original_link, settings, services.config, channel._now())
                        await current()
                        buffers = tuple((member.filename, batch.data(index)) for index, member in enumerate(metadata))
                        upload_started = True
                        outcome = await upload_output(
                            channel.bot,
                            kind=part["kind"],
                            files=buffers,
                            chat_id=saved.item.chat_id,
                            message_thread_id=saved.item.message_thread_id,
                            reply_to_message_id=part.get("reply_to_message_id"),
                            silent=silent,
                            protect_content=bool(part.get("protect_content", False)),
                            caption=prepared.caption_text if part.get("plain_fallback") else prepared.caption_html,
                            plain=bool(part.get("plain_fallback")),
                            before_dispatch=dispatch_current,
                        )
                        buffers = None
    except (DeliveryClaimRetired, PermanentDeliveryError):
        raise
    except asyncio.CancelledError as error:
        _forget_exception(error)
        raise
    except Exception as error:
        # Artifact 403/404 and transport failures are file-source outcomes;
        # they must never masquerade as Telegram blocking or token rejection.
        _forget_exception(error)
        failure = "upload-ambiguous" if upload_started else "source-unavailable"
    finally:
        data = buffers = hash_inputs = None
        if batch is not None:
            await await_drained(batch.aclose())
        elif reservation is not None:
            await await_drained(reservation.aclose())
    if failure == "replanned":
        raise DeliveryClaimRetired()
    if failure == "upload-ambiguous":
        raise NetworkError("File acknowledgement unavailable")
    if failure is not None and part["kind"] == "album":
        # A member's unavailable bytes must not suppress its healthy siblings.
        # Keep original order/hashes and retry each member independently through
        # the same source door; already-ACKed parts remain untouched.
        await current()
        template = {"kind": "album", "files": list(prepared.files), "unavailable": None}
        replacements = classify_output_part(template, [False] * len(prepared.files))
        await services.outbox.replace_output_parts(expected, replacements, reason="photo-classification")
        raise DeliveryClaimRetired()
    if failure is not None:
        raise _unavailable(prepared, reason="source-changed" if failure == "source-changed" else "source-unavailable", oversize=failure == "oversize")
    if outcome is None:
        raise PermanentDeliveryError("invalid-output-upload")
    if outcome.failure is None:
        channel._chat_message_sent(saved.item.chat_id, saved.item.message_thread_id)
        await channel._after_send_success()
        return list(outcome.message_ids)
    channel._failure()
    method = {"document": "sendDocument", "photo": "sendPhoto", "album": "sendMediaGroup"}[part["kind"]]
    # The upload leaf has dropped SDK request/body/exception custody. Rebuild
    # only a fixed diagnostic for the existing structured log contract.
    if outcome.failure == "retry":
        diagnostic = LongRetryAfter(outcome.retry_after)
    elif outcome.failure == "token":
        diagnostic = InvalidToken("Unauthorized" if outcome.http_status == 401 else "Not Found" if outcome.http_status == 404 else "Token verification required")
    elif outcome.failure == "forbidden":
        diagnostic = Forbidden("File send refused")
    elif outcome.failure in {"payload", "entities", "topic", "anchor"}:
        diagnostic = BadRequest("can't parse entities" if outcome.failure == "entities" else "message thread not found" if outcome.failure == "topic" else "File payload refused")
    else:
        diagnostic = NetworkError("File delivery unavailable")
    channel._log_part_failure(method, diagnostic, expected)
    context = SendContext(original_link.id, saved.item.chat_id, original_link.binding_generation, channel.transport_revision, "bound_message")
    if outcome.failure == "retry":
        await channel._send_refused_by_limit()
        raise DeliveryRetry(outcome.retry_after)
    if outcome.failure == "forbidden":
        await channel._forbidden(context)
        raise _logged(Forbidden("Bound file delivery refused"))
    if outcome.failure == "token":
        verdict = await channel.verify_token()
        if verdict.status == "unverified":
            await channel._defer_invalid_token(expected, verdict, http_status=outcome.http_status)
        if verdict.status == "valid":
            raise _logged(PermanentDeliveryError("invalid-payload"))
        raise DeliveryClaimRetired()
    if outcome.failure == "entities" and not part.get("plain_fallback"):
        await services.outbox.fallback_to_plain(expected)
        raise DeliveryClaimRetired()
    if outcome.failure == "topic":
        raise _logged(BadRequest("message thread not found"))
    if outcome.failure == "payload" and part["kind"] in {"photo", "album"}:
        template = {"kind": part["kind"], "files": list(prepared.files), "unavailable": None}
        replacements = classify_output_part(template, [False] * len(prepared.files))
        await services.outbox.replace_output_parts(expected, replacements, reason="photo-rejected")
        raise DeliveryClaimRetired()
    if outcome.failure == "ack":
        raise _logged(NetworkError("File acknowledgement unavailable"))
    if outcome.failure == "network":
        raise _logged(NetworkError("File delivery unavailable"))
    raise _unavailable(prepared)


async def cleanup_caption(channel, capture) -> bool | str:
    """Delete only saved previews after their captioned document was ACKed."""
    from telegram.error import InvalidToken, RetryAfter, TelegramError

    from app.telegram_bots.adapter import LiveRequestFence, LongRetryAfter

    services = channel._services
    saved = await services.outbox.capture_caption_cleanup(**capture)
    ids = tuple(saved.item.payload["caption_cleanup"]["message_ids"])
    prepared = await services.model_source.read_file_part(saved)

    def unchanged(captured):
        return captured.part == saved.part and captured.item == saved.item and channel._same_scope(saved.link, captured.link)

    async def current():
        if not channel._callback_current(saved.link):
            raise DeliveryClaimRetired()
        latest = await services.outbox.capture_caption_cleanup(**capture)
        if not unchanged(latest):
            raise DeliveryClaimRetired()
        verdict = await services.eligibility(latest.link, latest.item, latest.part)
        if not isinstance(verdict, DeliveryEligibility) or verdict.decision != "allow":
            raise DeliveryClaimRetired()
        if await services.model_source.read_file_part(latest) != prepared or not channel._callback_current(saved.link):
            raise DeliveryClaimRetired()
        if not unchanged(await services.outbox.capture_caption_cleanup(**capture)):
            raise DeliveryClaimRetired()

    for message_id in ids:
        await current()
        try:
            await channel.bot.delete_message(chat_id=saved.item.chat_id, message_id=message_id, rate_limit_args=LiveRequestFence(current))
        except BadRequest as error:
            if "message to delete not found" not in str(error).casefold():
                logger.info("telegram_bots preview_cleanup_unavailable link=%s", saved.link.id)
                return "unavailable"
        except (LongRetryAfter, RetryAfter) as error:
            seconds = error.seconds if isinstance(error, LongRetryAfter) else error._retry_after.total_seconds()
            raise DeliveryRetry(seconds) from None
        except InvalidToken:
            await channel.verify_token()
            return False
        except Forbidden:
            # Optional preview deletion cannot block a working message sender
            # or keep its acknowledged file/input pending forever.
            logger.info("telegram_bots preview_cleanup_unavailable link=%s", saved.link.id)
            return "unavailable"
        except TelegramError:
            return False
    return True
